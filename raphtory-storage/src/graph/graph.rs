use super::{
    edges::{edge_entry::EdgeStorageEntry, unlocked::UnlockedEdges},
    nodes::node_entry::NodeStorageEntry,
};
use crate::graph::{
    edges::edges::{EdgesStorage, EdgesStorageRef},
    locked::LockedGraph,
    nodes::{nodes::NodesStorage, nodes_ref::NodesStorageEntry},
};
use db4_graph::TemporalGraph;
use raphtory_api::core::entities::{properties::meta::Meta, LayerIds, LayerVariants, EID, VID};
use raphtory_core::entities::{nodes::node_ref::NodeRef, properties::graph_meta::GraphMeta};
use std::{fmt::Debug, iter, sync::Arc};
use thiserror::Error;

#[cfg(feature = "storage")]
use crate::disk::{
    storage_interface::{
        edges::DiskEdges, edges_ref::DiskEdgesRef, node::DiskNode, nodes::DiskNodesOwned,
        nodes_ref::DiskNodesRef,
    },
    DiskGraphStorage,
};
use crate::mutation::MutationError;

#[derive(Clone, Debug)]
pub enum GraphStorage {
    Mem(LockedGraph),
    Unlocked(Arc<TemporalGraph>),
    #[cfg(feature = "storage")]
    Disk(Arc<DiskGraphStorage>),
}

#[derive(Error, Debug)]
pub enum Immutable {
    #[error("The graph is locked and cannot be mutated")]
    ReadLockedImmutable,
    #[cfg(feature = "storage")]
    #[error("DiskGraph cannot be mutated")]
    DiskGraphImmutable,
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
                #[cfg(feature = "storage")]
                _ => false,
            },
            #[cfg(feature = "storage")]
            GraphStorage::Disk(this_graph) => match other {
                GraphStorage::Disk(other_graph) => Arc::ptr_eq(this_graph, other_graph),
                _ => false,
            },
        }
    }

    pub fn mutable(&self) -> Result<&Arc<TemporalGraph>, MutationError> {
        match self {
            GraphStorage::Mem(_) => Err(Immutable::ReadLockedImmutable)?,
            GraphStorage::Unlocked(graph) => Ok(graph),
            #[cfg(feature = "storage")]
            GraphStorage::Disk(_) => Err(Immutable::DiskGraphImmutable)?,
        }
    }

    #[inline(always)]
    pub fn is_immutable(&self) -> bool {
        match self {
            GraphStorage::Mem(_) => true,
            GraphStorage::Unlocked(_) => false,
            #[cfg(feature = "storage")]
            GraphStorage::Disk(_) => true,
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

    #[inline(always)]
    pub fn nodes(&self) -> NodesStorageEntry {
        match self {
            GraphStorage::Mem(storage) => NodesStorageEntry::Mem(&storage.nodes),
            GraphStorage::Unlocked(storage) => {
                NodesStorageEntry::Unlocked(storage.storage().nodes().locked())
            }
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => {
                NodesStorageEntry::Disk(DiskNodesRef::new(&storage.inner))
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
                #[cfg(feature = "storage")]
                GraphStorage::Disk(storage) => match v {
                    NodeRef::External(id) => storage.inner.find_node(id),
                    _ => unreachable!("VID is handled above!"),
                },
            },
        }
    }

    #[inline(always)]
    pub fn unfiltered_num_nodes(&self) -> usize {
        match self {
            GraphStorage::Mem(storage) => storage.graph.internal_num_nodes(),
            GraphStorage::Unlocked(storage) => storage.internal_num_nodes(),
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => storage.inner.num_nodes(),
        }
    }

    #[inline(always)]
    pub fn unfiltered_num_edges(&self) -> usize {
        match self {
            GraphStorage::Mem(storage) => storage.graph.internal_num_edges(),
            GraphStorage::Unlocked(storage) => storage.internal_num_edges(),
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => storage.inner.count_edges(),
        }
    }

    #[inline(always)]
    pub fn unfiltered_num_layers(&self) -> usize {
        match self {
            GraphStorage::Mem(storage) => storage.graph.num_layers(),
            GraphStorage::Unlocked(storage) => storage.num_layers(),
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => storage.inner.layers().len(),
        }
    }

    #[inline(always)]
    pub fn core_nodes(&self) -> NodesStorage {
        match self {
            GraphStorage::Mem(storage) => NodesStorage::new(storage.nodes.clone()),
            GraphStorage::Unlocked(storage) => {
                NodesStorage::new(storage.read_locked().nodes.clone())
            }
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => {
                NodesStorage::Disk(DiskNodesOwned::new(storage.inner.clone()))
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
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => {
                NodeStorageEntry::Disk(DiskNode::new(&storage.inner, vid))
            }
        }
    }

    #[inline(always)]
    pub fn edges(&self) -> EdgesStorageRef {
        match self {
            GraphStorage::Mem(storage) => EdgesStorageRef::Mem(&storage.edges),
            GraphStorage::Unlocked(storage) => {
                EdgesStorageRef::Unlocked(UnlockedEdges(storage.storage()))
            }
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => EdgesStorageRef::Disk(DiskEdgesRef::new(&storage.inner)),
        }
    }

    #[inline(always)]
    pub fn owned_edges(&self) -> EdgesStorage {
        match self {
            GraphStorage::Mem(storage) => EdgesStorage::new(storage.edges.clone()),
            GraphStorage::Unlocked(storage) => {
                EdgesStorage::new(storage.storage().edges().locked().into())
            }
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => EdgesStorage::Disk(DiskEdges::new(storage)),
        }
    }

    #[inline(always)]
    pub fn edge_entry(&self, eid: EID) -> EdgeStorageEntry {
        match self {
            GraphStorage::Mem(storage) => EdgeStorageEntry::Mem(storage.edges.edge_ref(eid)),
            GraphStorage::Unlocked(storage) => {
                EdgeStorageEntry::Unlocked(storage.storage().edges().edge(eid))
            }
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => EdgeStorageEntry::Disk(storage.inner.edge(eid)),
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
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => storage.node_meta(),
        }
    }

    pub fn edge_meta(&self) -> &Meta {
        match self {
            GraphStorage::Mem(storage) => storage.graph.edge_meta(),
            GraphStorage::Unlocked(storage) => storage.edge_meta(),
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => storage.edge_meta(),
        }
    }

    pub fn graph_meta(&self) -> &GraphMeta {
        match self {
            GraphStorage::Mem(storage) => storage.graph.graph_meta(),
            GraphStorage::Unlocked(storage) => storage.graph_meta(),
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => storage.graph_meta(),
        }
    }
}
