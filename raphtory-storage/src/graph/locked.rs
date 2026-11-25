use db4_graph::TemporalGraph;
use raphtory_api::{core::entities::properties::meta::ReadLockedMetaPresence, ArcRwLockReadGuard};
use std::sync::{Arc, OnceLock};
use storage::{
    api::node_type_index::NodeTypeIndexOps, segments::node_type_index::MemNodeTypeIndex, Extension,
    ReadLockedEdges, ReadLockedNodes,
};

/// A fully locked, read-only graph.
#[derive(Debug)]
pub struct ReadLockedGraph {
    // Must be locked before nodes.
    node_type_index: Arc<ArcRwLockReadGuard<MemNodeTypeIndex>>,
    nodes: Arc<ReadLockedNodes<Extension>>,
    edges: Arc<ReadLockedEdges<Extension>>,
    /// Presence bitsets, read-locked only on first use.
    node_presence: Arc<OnceLock<ReadLockedMetaPresence>>,
    edge_presence: Arc<OnceLock<ReadLockedMetaPresence>>,
    pub graph: Arc<TemporalGraph>,
}

impl ReadLockedGraph {
    pub fn new(graph: Arc<TemporalGraph>) -> Self {
        let storage = graph.storage();
        let node_type_index = Arc::new(storage.node_type_index().head_shared_arc());
        let nodes = Arc::new(storage.nodes().locked());
        let edges = Arc::new(storage.edges().locked());

        Self {
            node_type_index,
            nodes,
            edges,
            node_presence: Default::default(),
            edge_presence: Default::default(),
            graph,
        }
    }

    pub(crate) fn nodes(&self) -> &Arc<ReadLockedNodes<Extension>> {
        &self.nodes
    }

    pub(crate) fn edges(&self) -> &Arc<ReadLockedEdges<Extension>> {
        &self.edges
    }

    pub(crate) fn node_presence(&self) -> &ReadLockedMetaPresence {
        self.node_presence
            .get_or_init(|| self.graph.node_meta().read_lock_layer_presence())
    }

    pub(crate) fn edge_presence(&self) -> &ReadLockedMetaPresence {
        self.edge_presence
            .get_or_init(|| self.graph.edge_meta().read_lock_layer_presence())
    }
}

impl Clone for ReadLockedGraph {
    fn clone(&self) -> Self {
        ReadLockedGraph {
            node_type_index: self.node_type_index.clone(),
            nodes: self.nodes.clone(),
            edges: self.edges.clone(),
            node_presence: self.node_presence.clone(),
            edge_presence: self.edge_presence.clone(),
            graph: self.graph.clone(),
        }
    }
}
