use db4_graph::TemporalGraph;
use raphtory_api::core::{
    entities::{GidRef, VID},
    storage::dict_mapper::MaybeNew,
};
use raphtory_core::{
    entities::graph::logical_to_physical::InvalidNodeId,
    storage::{raw_edges::WriteLockedEdges, WriteLockedNodes},
};
use std::sync::Arc;
use storage::{pages::locked::{edges::WriteLockedEdgePages, nodes::WriteLockedNodePages}, Extension, ReadLockedEdges, ReadLockedNodes};

#[derive(Debug)]
pub struct LockedGraph {
    pub(crate) nodes: Arc<ReadLockedNodes<Extension>>,
    pub(crate) edges: Arc<ReadLockedEdges<Extension>>,
    pub graph: Arc<TemporalGraph>,
}

// impl<'de> serde::Deserialize<'de> for LockedGraph {
//     fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
//     where
//         D: serde::Deserializer<'de>,
//     {
//         TemporalGraph::deserialize(deserializer).map(|graph| LockedGraph::new(Arc::new(graph)))
//     }
// }
//
// impl serde::Serialize for LockedGraph {
//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
//     where
//         S: serde::Serializer,
//     {
//         self.graph.serialize(serializer)
//     }
// }

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

