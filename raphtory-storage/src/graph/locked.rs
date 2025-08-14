use db4_graph::TemporalGraph;
use std::sync::Arc;
use storage::{Extension, ReadLockedEdges, ReadLockedNodes};

#[derive(Debug)]
pub struct LockedGraph {
    pub(crate) nodes: Arc<ReadLockedNodes<Extension>>,
    pub(crate) edges: Arc<ReadLockedEdges<Extension>>,
    pub graph: Arc<TemporalGraph>,
}

impl LockedGraph {
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

impl Clone for LockedGraph {
    fn clone(&self) -> Self {
        LockedGraph {
            nodes: self.nodes.clone(),
            edges: self.edges.clone(),
            graph: self.graph.clone(),
        }
    }
}
