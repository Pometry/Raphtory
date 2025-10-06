use super::{
    edges::{edge_entry::EdgeStorageEntry, unlocked::UnlockedEdges},
    nodes::node_entry::NodeStorageEntry,
};
use crate::{
    graph::{
        edges::edges::{EdgesStorage, EdgesStorageRef},
        locked::LockedGraph,
        nodes::{nodes::NodesStorage, nodes_ref::NodesStorageEntry},
    },
    mutation::MutationError,
};
use db4_graph::TemporalGraph;
use raphtory_api::core::entities::{properties::meta::Meta, LayerIds, LayerVariants, EID, VID};
use raphtory_core::entities::{nodes::node_ref::NodeRef, properties::graph_meta::GraphMeta};
use std::{fmt::Debug, iter, sync::Arc};
use thiserror::Error;

#[derive(Clone, Debug)]
pub enum GraphStorage {
    Mem(LockedGraph),
    Unlocked(Arc<TemporalGraph>),
}

#[derive(Error, Debug)]
pub enum Immutable {
    #[error("The graph is locked and cannot be mutated")]
    ReadLockedImmutable,
}

impl From<TemporalGraph> for GraphStorage {
    fn from(value: TemporalGraph) -> Self {
        Self::Unlocked(Arc::new(value))
    }
}

impl Default for GraphStorage {
    fn default() -> Self {
        GraphStorage::Unlocked(Arc::new(TemporalGraph::default()))
    }
}

impl std::fmt::Display for GraphStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Graph(num_nodes={}, num_edges={})",
            self.unfiltered_num_nodes(),
            self.unfiltered_num_edges(),
        )
    }
}

impl GraphStorage {
    /// Check if two storage instances point at the same underlying storage
    pub fn ptr_eq(&self, other: &Self) -> bool {
        match self {
            GraphStorage::Mem(LockedGraph {
                graph: this_graph, ..
            })
            | GraphStorage::Unlocked(this_graph) => match other {
                GraphStorage::Mem(LockedGraph {
                    graph: other_graph, ..
                })
                | GraphStorage::Unlocked(other_graph) => Arc::ptr_eq(this_graph, other_graph),
            },
        }
    }

    pub fn mutable(&self) -> Result<&Arc<TemporalGraph>, MutationError> {
        match self {
            GraphStorage::Mem(_) => Err(Immutable::ReadLockedImmutable)?,
            GraphStorage::Unlocked(graph) => Ok(graph),
        }
    }

    #[inline(always)]
    pub fn is_immutable(&self) -> bool {
        match self {
            GraphStorage::Mem(_) => true,
            GraphStorage::Unlocked(_) => false,
        }
    }

    #[inline(always)]
    pub fn lock(&self) -> Self {
        match self {
            GraphStorage::Unlocked(storage) => {
                let locked = LockedGraph::new(storage.clone());
                GraphStorage::Mem(locked)
            }
            _ => self.clone(),
        }
    }

    pub fn disk_storage_enabled(&self) -> bool {
        match self {
            GraphStorage::Mem(graph) => graph.graph.disk_storage_enabled(),
            GraphStorage::Unlocked(graph) => graph.disk_storage_enabled(),
        }
    }

    #[inline(always)]
    pub fn nodes(&self) -> NodesStorageEntry<'_> {
        match self {
            GraphStorage::Mem(storage) => NodesStorageEntry::Mem(&storage.nodes),
            GraphStorage::Unlocked(storage) => {
                NodesStorageEntry::Unlocked(storage.storage().nodes().locked())
            }
        }
    }

    #[inline(always)]
    pub fn internalise_node(&self, v: NodeRef) -> Option<VID> {
        match v {
            NodeRef::Internal(vid) => Some(vid),
            node_ref => match self {
                GraphStorage::Mem(locked) => locked.graph.resolve_node_ref(node_ref),
                GraphStorage::Unlocked(unlocked) => unlocked.resolve_node_ref(node_ref),
            },
        }
    }

    #[inline(always)]
    pub fn unfiltered_num_nodes(&self) -> usize {
        match self {
            GraphStorage::Mem(storage) => storage.graph.internal_num_nodes(),
            GraphStorage::Unlocked(storage) => storage.internal_num_nodes(),
        }
    }

    #[inline(always)]
    pub fn unfiltered_num_edges(&self) -> usize {
        match self {
            GraphStorage::Mem(storage) => storage.graph.internal_num_edges(),
            GraphStorage::Unlocked(storage) => storage.internal_num_edges(),
        }
    }

    #[inline(always)]
    pub fn unfiltered_num_layers(&self) -> usize {
        match self {
            GraphStorage::Mem(storage) => storage.graph.num_layers(),
            GraphStorage::Unlocked(storage) => storage.num_layers(),
        }
    }

    #[inline(always)]
    pub fn core_nodes(&self) -> NodesStorage {
        match self {
            GraphStorage::Mem(storage) => NodesStorage::new(storage.nodes.clone()),
            GraphStorage::Unlocked(storage) => {
                NodesStorage::new(storage.read_locked().nodes.clone())
            }
        }
    }

    #[inline(always)]
    pub fn core_node<'a>(&'a self, vid: VID) -> NodeStorageEntry<'a> {
        match self {
            GraphStorage::Mem(storage) => NodeStorageEntry::Mem(storage.nodes.node_ref(vid)),
            GraphStorage::Unlocked(storage) => {
                NodeStorageEntry::Unlocked(storage.storage().nodes().node(vid))
            }
        }
    }

    #[inline(always)]
    pub fn edges(&self) -> EdgesStorageRef<'_> {
        match self {
            GraphStorage::Mem(storage) => EdgesStorageRef::Mem(&storage.edges),
            GraphStorage::Unlocked(storage) => {
                EdgesStorageRef::Unlocked(UnlockedEdges(storage.storage()))
            }
        }
    }

    #[inline(always)]
    pub fn owned_edges(&self) -> EdgesStorage {
        match self {
            GraphStorage::Mem(storage) => EdgesStorage::new(storage.edges.clone()),
            GraphStorage::Unlocked(storage) => {
                EdgesStorage::new(storage.storage().edges().locked().into())
            }
        }
    }

    #[inline(always)]
    pub fn edge_entry(&self, eid: EID) -> EdgeStorageEntry<'_> {
        match self {
            GraphStorage::Mem(storage) => EdgeStorageEntry::Mem(storage.edges.edge_ref(eid)),
            GraphStorage::Unlocked(storage) => {
                EdgeStorageEntry::Unlocked(storage.storage().edges().edge(eid))
            }
        }
    }

    pub fn layer_ids_iter(&self, layer_ids: &LayerIds) -> impl Iterator<Item = usize> {
        match layer_ids {
            LayerIds::None => LayerVariants::None(iter::empty()),
            LayerIds::All => LayerVariants::All(1..=self.unfiltered_num_layers()),
            LayerIds::One(id) => LayerVariants::One(iter::once(*id)),
            LayerIds::Multiple(ids) => LayerVariants::Multiple(ids.clone().into_iter()),
        }
    }

    pub fn unfiltered_layer_ids(&self) -> impl Iterator<Item = usize> {
        1..=self.unfiltered_num_layers()
    }

    pub fn node_meta(&self) -> &Meta {
        match self {
            GraphStorage::Mem(storage) => storage.graph.node_meta(),
            GraphStorage::Unlocked(storage) => storage.node_meta(),
        }
    }

    pub fn edge_meta(&self) -> &Meta {
        match self {
            GraphStorage::Mem(storage) => storage.graph.edge_meta(),
            GraphStorage::Unlocked(storage) => storage.edge_meta(),
        }
    }

    pub fn graph_meta(&self) -> &GraphMeta {
        match self {
            GraphStorage::Mem(storage) => storage.graph.graph_meta(),
            GraphStorage::Unlocked(storage) => storage.graph_meta(),
        }
    }
}
