use db4_graph::TemporalGraph;
use raphtory_api::core::entities::properties::meta::MetaLayerPresence;
use std::sync::{Arc, OnceLock};
use storage::{Extension, ReadLockedEdges, ReadLockedNodes};

/// A fully locked, read-only graph.
#[derive(Debug)]
pub struct ReadLockedGraph {
    pub(crate) nodes: Arc<ReadLockedNodes<Extension>>,
    pub(crate) edges: Arc<ReadLockedEdges<Extension>>,
    /// Presence bitsets, read-locked on first use and held until the last clone is dropped.
    node_presence: Arc<OnceLock<MetaLayerPresence>>,
    edge_presence: Arc<OnceLock<MetaLayerPresence>>,
    pub graph: Arc<TemporalGraph>,
}

impl ReadLockedGraph {
    pub fn new(graph: Arc<TemporalGraph>) -> Self {
        let nodes = Arc::new(graph.storage().nodes().locked());
        let edges = Arc::new(graph.storage().edges().locked());
        Self {
            nodes,
            edges,
            node_presence: Default::default(),
            edge_presence: Default::default(),
            graph,
        }
    }

    pub(crate) fn node_presence(&self) -> &MetaLayerPresence {
        self.node_presence
            .get_or_init(|| self.graph.node_meta().read_lock_layer_presence())
    }

    pub(crate) fn edge_presence(&self) -> &MetaLayerPresence {
        self.edge_presence
            .get_or_init(|| self.graph.edge_meta().read_lock_layer_presence())
    }
}

impl Clone for ReadLockedGraph {
    fn clone(&self) -> Self {
        ReadLockedGraph {
            nodes: self.nodes.clone(),
            edges: self.edges.clone(),
            node_presence: self.node_presence.clone(),
            edge_presence: self.edge_presence.clone(),
            graph: self.graph.clone(),
        }
    }
}
