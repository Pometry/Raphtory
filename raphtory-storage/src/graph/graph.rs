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
use itertools::Either;
use raphtory_api::core::entities::{
    properties::meta::Meta, LayerId, LayerIds, LayerVariants, EID, VID,
};
use raphtory_core::entities::{edges::edge_ref::EdgeRef, nodes::node_ref::NodeRef};
use std::{fmt::Debug, iter, path::Path, sync::Arc};
use storage::{
    api::nodes::{NodeSegmentOps, PropPredicate},
    error::StorageError,
    pages::SegmentCounts,
    persist::strategy::PersistenceStrategy,
    state::StateIndex,
    Extension, GIDResolver, GraphPropEntry,
};
use thiserror::Error;

pub use storage::api::nodes::PropPredicate as NodePropPredicate;

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

impl From<Arc<TemporalGraph>> for GraphStorage {
    fn from(value: Arc<TemporalGraph>) -> Self {
        Self::Unlocked(value)
    }
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
            self.unfiltered_num_nodes(&LayerIds::All),
            self.unfiltered_num_edges(&LayerIds::All),
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

    pub fn flush(&self) -> Result<(), StorageError> {
        match self {
            GraphStorage::Mem(graph) => graph.flush(),
            GraphStorage::Unlocked(graph) => graph.flush(),
        }
    }

    pub fn vacuum(&self) -> Result<(), StorageError> {
        match self {
            GraphStorage::Mem(graph) => graph.vacuum(),
            GraphStorage::Unlocked(graph) => graph.vacuum(),
        }
    }

    pub fn disk_storage_path(&self) -> Option<&Path> {
        match self {
            GraphStorage::Mem(graph) => graph.graph.disk_storage_path(),
            GraphStorage::Unlocked(graph) => graph.disk_storage_path(),
        }
    }

    pub fn logical_to_physical(&self) -> &GIDResolver {
        match self {
            GraphStorage::Mem(graph) => &graph.graph.logical_to_physical,
            GraphStorage::Unlocked(graph) => &graph.logical_to_physical,
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

    pub fn num_node_segments(&self) -> usize {
        match self {
            GraphStorage::Mem(storage) => storage.graph.storage().nodes().num_segments(),
            GraphStorage::Unlocked(storage) => storage.storage().nodes().num_segments(),
        }
    }

    pub fn num_edge_segments(&self) -> usize {
        match self {
            GraphStorage::Mem(storage) => storage.graph.storage().edges().num_segments(),
            GraphStorage::Unlocked(storage) => storage.storage().edges().num_segments(),
        }
    }

    fn temporal_graph(&self) -> &TemporalGraph {
        match self {
            GraphStorage::Mem(storage) => &storage.graph,
            GraphStorage::Unlocked(storage) => storage,
        }
    }

    /// Resolve a node property predicate to a candidate VID superset using the
    /// storage backend's secondary indexes, if it has them for this property.
    /// `metadata` selects the metadata prop-id space over the temporal one.
    /// `None` means the predicate cannot be served and callers should scan.
    /// Candidates may include non-matching nodes — callers must still verify.
    pub fn node_prop_candidates(
        &self,
        prop_id: usize,
        metadata: bool,
        predicate: &PropPredicate,
    ) -> Option<Vec<VID>> {
        let nodes = self.temporal_graph().storage().nodes();
        let max_segment_len = nodes.max_segment_len();
        let mut vids = Vec::new();
        for segment in nodes.segments_iter() {
            let candidates = segment.node_prop_candidates(prop_id, metadata, predicate)?;
            let segment_id = segment.segment_id();
            vids.extend(
                candidates
                    .rows
                    .into_iter()
                    .map(|pos| pos.as_vid(segment_id, max_segment_len)),
            );
        }
        Some(vids)
    }

    /// Ask the storage backend to build any missing secondary property
    /// indexes. A no-op for backends without index support. Segments are
    /// independent, so they build in parallel.
    pub fn build_node_prop_index(&self) -> Result<(), StorageError> {
        use rayon::iter::ParallelIterator;
        let nodes = self.temporal_graph().storage().nodes();
        nodes
            .segments_par_iter()
            .try_for_each(|segment| segment.build_prop_index())
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
    pub fn unfiltered_num_nodes(&self, layer_ids: &LayerIds) -> usize {
        match self {
            GraphStorage::Mem(storage) => storage.graph.internal_num_nodes(layer_ids),
            GraphStorage::Unlocked(storage) => storage.internal_num_nodes(layer_ids),
        }
    }

    #[inline(always)]
    pub fn unfiltered_num_edges(&self, layer_ids: &LayerIds) -> usize {
        match self {
            GraphStorage::Mem(storage) => storage.graph.internal_num_edges(layer_ids),
            GraphStorage::Unlocked(storage) => storage.internal_num_edges(layer_ids),
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

    /// Try to get a node that may not be initialised yet
    pub fn try_core_node<'a>(&'a self, vid: VID) -> Option<NodeStorageEntry<'a>> {
        match self {
            GraphStorage::Mem(storage) => {
                storage.nodes.try_node_ref(vid).map(NodeStorageEntry::Mem)
            }
            GraphStorage::Unlocked(storage) => storage
                .storage()
                .nodes()
                .try_node(vid)
                .map(NodeStorageEntry::Unlocked),
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
    pub fn edge_entry(&self, eid: Either<EID, EdgeRef>) -> EdgeStorageEntry<'_> {
        match self {
            GraphStorage::Mem(storage) => EdgeStorageEntry::Mem(storage.edges.edge_ref(eid)),
            GraphStorage::Unlocked(storage) => {
                EdgeStorageEntry::Unlocked(storage.storage().edges().edge(eid))
            }
        }
    }

    /// Acquired a locked, read-only view of graph properties / metadata.
    #[inline(always)]
    pub fn graph_entry(&self) -> GraphPropEntry<'_> {
        match self {
            GraphStorage::Mem(storage) => storage.graph.storage().graph_props().graph_entry(),
            GraphStorage::Unlocked(storage) => storage.storage().graph_props().graph_entry(),
        }
    }

    pub fn layer_ids_iter(&self, layer_ids: &LayerIds) -> impl Iterator<Item = LayerId> {
        match layer_ids {
            LayerIds::None => LayerVariants::None(iter::empty()),
            LayerIds::All => LayerVariants::All((1..=self.unfiltered_num_layers()).map(LayerId)),
            LayerIds::One(id) => LayerVariants::One(iter::once(*id)),
            LayerIds::Multiple(ids) => LayerVariants::Multiple(ids.clone().into_iter()),
        }
    }

    pub fn unfiltered_layer_ids(&self) -> impl Iterator<Item = LayerId> {
        (1..=self.unfiltered_num_layers()).map(LayerId)
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

    pub fn graph_props_meta(&self) -> &Meta {
        match self {
            GraphStorage::Mem(storage) => storage.graph.graph_props_meta(),
            GraphStorage::Unlocked(storage) => storage.graph_props_meta(),
        }
    }

    pub fn extension(&self) -> &Extension {
        match self {
            GraphStorage::Mem(storage) => storage.graph.extension(),
            GraphStorage::Unlocked(storage) => storage.extension(),
        }
    }

    pub fn total_allocated_memory(&self) -> usize {
        self.extension().estimated_size()
    }

    pub fn node_segment_counts(&self) -> SegmentCounts<VID> {
        match self {
            GraphStorage::Mem(storage) => storage.nodes.segment_counts(),
            GraphStorage::Unlocked(storage) => storage.storage().node_segment_counts(),
        }
    }

    pub fn node_state_index(&self) -> StateIndex<VID> {
        self.node_segment_counts().into()
    }

    pub fn edge_segment_counts(&self) -> SegmentCounts<EID> {
        match self {
            GraphStorage::Mem(storage) => storage.edges.segment_counts(),
            GraphStorage::Unlocked(storage) => storage.storage().edge_segment_counts(),
        }
    }
}
