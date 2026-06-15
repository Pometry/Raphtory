use crate::{
    db::api::view::internal::GraphView,
    prelude::GraphViewOps,
    serialise::{GraphFolder, GraphPaths},
};
use raphtory_api::core::storage::graph_folder::GraphMetadata;

/// Build the [`GraphMetadata`] summary for a graph
pub fn build_graph_metadata(graph: impl GraphView) -> GraphMetadata {
    GraphMetadata {
        node_count: graph.count_nodes(),
        edge_count: graph.count_edges(),
        graph_type: graph.graph_type(),
        is_diskgraph: graph.disk_storage_path().is_some(),
    }
}

pub fn assert_metadata_correct<'graph>(folder: &GraphFolder, graph: &impl GraphViewOps<'graph>) {
    let metadata = folder.read_metadata().unwrap();
    assert_eq!(metadata.node_count, graph.count_nodes());
    assert_eq!(metadata.edge_count, graph.count_edges());
    assert_eq!(metadata.graph_type, graph.graph_type());
}
