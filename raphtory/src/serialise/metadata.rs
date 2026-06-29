use crate::{
    db::api::view::internal::GraphView,
    errors::GraphError,
    prelude::{GraphViewOps, ParquetEncoder},
};
use raphtory_api::core::storage::graph_folder::{
    make_path_pointer, GraphFolder, GraphMetadata, GraphPaths, InnerGraphFolder, Metadata,
    GRAPH_META_PATH, GRAPH_PATH,
};

/// Build the [`GraphMetadata`] summary for a graph
pub fn build_graph_metadata(graph: impl GraphView) -> GraphMetadata {
    GraphMetadata {
        node_count: graph.count_nodes(),
        edge_count: graph.count_edges(),
        graph_type: graph.graph_type(),
        is_diskgraph: graph.disk_storage_path().is_some(),
    }
}

/// Encode `graph`'s data into a fresh directory inside `folder` and atomically point the folder's
/// metadata at it, deleting any previously-stored graph data.
pub fn replace_graph_in_folder(
    folder: &InnerGraphFolder,
    graph: impl ParquetEncoder + GraphView + std::fmt::Debug,
) -> Result<(), GraphError> {
    let data_path = folder.as_ref();
    let new_relative_graph_path = make_path_pointer(data_path, GRAPH_META_PATH, GRAPH_PATH)?;
    graph.encode_parquet(data_path.join(&new_relative_graph_path))?;
    let meta = Metadata {
        path: new_relative_graph_path,
        meta: build_graph_metadata(&graph),
    };
    folder.replace_graph_path(meta)?;
    Ok(())
}

pub fn assert_metadata_correct<'graph>(folder: &GraphFolder, graph: &impl GraphViewOps<'graph>) {
    let metadata = folder.read_metadata().unwrap();
    assert_eq!(metadata.node_count, graph.count_nodes());
    assert_eq!(metadata.edge_count, graph.count_edges());
    assert_eq!(metadata.graph_type, graph.graph_type());
}
