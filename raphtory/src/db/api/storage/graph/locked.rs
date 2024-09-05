use std::sync::Arc;

use crate::core::{
    entities::{graph::tgraph::TemporalGraph, nodes::node_store::NodeStore, VID},
    storage::{raw_edges::LockedEdges, ReadLockedStorage},
};

#[derive(Debug)]
pub struct LockedGraph {
    pub(crate) nodes: Arc<ReadLockedStorage<NodeStore, VID>>,
    pub(crate) edges: Arc<LockedEdges>,
    pub(crate) graph: Arc<TemporalGraph>,
}

impl<'de> serde::Deserialize<'de> for LockedGraph {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        TemporalGraph::deserialize(deserializer).map(|graph| LockedGraph::new(Arc::new(graph)))
    }
}

impl serde::Serialize for LockedGraph {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.graph.serialize(serializer)
    }
}

impl LockedGraph {
    pub fn new(graph: Arc<TemporalGraph>) -> Self {
        let nodes = Arc::new(graph.storage.nodes_read_lock());
        let edges = Arc::new(graph.storage.edges_read_lock());
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
