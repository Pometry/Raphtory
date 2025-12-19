use crate::{
    db::api::view::internal::GraphView,
    prelude::GraphViewOps,
    serialise::{GraphFolder, GraphPaths},
};
use raphtory_api::GraphType;
use serde::{Deserialize, Serialize};

#[derive(PartialEq, Serialize, Deserialize, Debug)]
pub struct GraphMetadata {
    pub node_count: usize,
    pub edge_count: usize,
    pub graph_type: GraphType,
    pub is_diskgraph: bool,
}

impl GraphMetadata {
    pub fn from_graph<G: GraphView>(graph: G) -> Self {
        let node_count = graph.count_nodes();
        let edge_count = graph.count_edges();
        let graph_type = graph.graph_type();
        let is_diskgraph = graph.disk_storage_enabled().is_some();
        Self {
            node_count,
            edge_count,
            graph_type,
            is_diskgraph,
        }
    }
}

pub fn assert_metadata_correct<'graph>(folder: &GraphFolder, graph: &impl GraphViewOps<'graph>) {
    let metadata = folder.read_metadata().unwrap();
    assert_eq!(metadata.node_count, graph.count_nodes());
    assert_eq!(metadata.edge_count, graph.count_edges());
    assert_eq!(metadata.graph_type, graph.graph_type());
}
