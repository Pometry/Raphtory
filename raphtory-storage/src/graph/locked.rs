use db4_graph::TemporalGraph;
use std::sync::Arc;
use storage::{Extension, ReadLockedEdges, ReadLockedNodes};

/// A fully locked, read-only graph.
#[derive(Debug)]
pub struct ReadLockedGraph {
    pub(crate) nodes: Arc<ReadLockedNodes<Extension>>,
    pub(crate) edges: Arc<ReadLockedEdges<Extension>>,
    pub graph: Arc<TemporalGraph>,
}

impl ReadLockedGraph {
    pub fn new(graph: Arc<TemporalGraph>) -> Self {
        let nodes = Arc::new(graph.storage().nodes().locked());
        let edges = Arc::new(graph.storage().edges().locked());
        Self {
            nodes,
            edges,
            graph,
        }
    }
}

impl Clone for ReadLockedGraph {
    fn clone(&self) -> Self {
        ReadLockedGraph {
            nodes: self.nodes.clone(),
            edges: self.edges.clone(),
            graph: self.graph.clone(),
        }
    }
}
