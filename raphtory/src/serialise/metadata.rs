use crate::{
    db::api::view::internal::GraphView,
    prelude::GraphViewOps,
    serialise::{GraphFolder, GraphPaths, Metadata, GRAPH_META_PATH},
};
use raphtory_api::GraphType;
use serde::{Deserialize, Serialize};
use std::{fs, fs::File, path::Path};
use storage::error::StorageError;

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
        let is_diskgraph = graph.disk_storage_path().is_some();
        Self {
            node_count,
            edge_count,
            graph_type,
            is_diskgraph,
        }
    }
}

/// Refresh the `.meta` file for a disk-backed graph by writing the current
/// node/edge counts and graph type.
/// `disk_graph_path` is the inner graph directory: `<root>/<data_dir>/<graph_dir>`.
pub fn write_disk_graph_metadata(
    disk_graph_path: &Path,
    graph: impl GraphView,
) -> Result<(), StorageError> {
    let Some(data_folder) = disk_graph_path.parent() else {
        return Ok(());
    };
    if !data_folder.is_dir() {
        return Ok(());
    }
    let Some(graph_path_name) = disk_graph_path.file_name().and_then(|s| s.to_str()) else {
        return Ok(());
    };

    let metadata = GraphMetadata::from_graph(graph);
    let meta = Metadata {
        path: graph_path_name.to_string(),
        meta: metadata,
    };
    let tmp_path = data_folder.join(".tmp");
    {
        let tmp_file = File::create(&tmp_path)?;
        serde_json::to_writer(&tmp_file, &meta)?;
        tmp_file.sync_all()?;
    }
    fs::rename(tmp_path, data_folder.join(GRAPH_META_PATH))?;
    Ok(())
}

pub fn assert_metadata_correct<'graph>(folder: &GraphFolder, graph: &impl GraphViewOps<'graph>) {
    let metadata = folder.read_metadata().unwrap();
    assert_eq!(metadata.node_count, graph.count_nodes());
    assert_eq!(metadata.edge_count, graph.count_edges());
    assert_eq!(metadata.graph_type, graph.graph_type());
}

#[cfg(test)]
mod tests {
    use crate::{
        prelude::*,
        serialise::{GraphFolder, GraphPaths},
    };

    #[test]
    fn flush_updates_disk_graph_metadata_counts() {
        use storage::{persist::strategy::PersistenceStrategy, Extension};
        if !<Extension as PersistenceStrategy>::disk_storage_enabled() {
            println!("Disk storage is disabled");
            return;
        } else {
            println!("Disk storage is enabled, running test");
        }

        let tmp = tempfile::TempDir::new().unwrap();
        let folder = GraphFolder::from(tmp.path().join("g"));

        let graph = Graph::new_at_path(&folder).unwrap();

        // Before any writes, counts are 0.
        let meta = folder.read_metadata().unwrap();
        assert_eq!(meta.node_count, 0);
        assert_eq!(meta.edge_count, 0);
        assert!(meta.is_diskgraph);

        graph.add_node(0, "a", NO_PROPS, None, None).unwrap();
        graph.add_node(0, "b", NO_PROPS, None, None).unwrap();
        graph.add_edge(1, "a", "b", NO_PROPS, None).unwrap();

        // Metadata file is stale until flush.
        graph.flush().unwrap();

        let meta = folder.read_metadata().unwrap();
        assert_eq!(meta.node_count, graph.count_nodes());
        assert_eq!(meta.edge_count, graph.count_edges());
        assert_eq!(meta.node_count, 2);
        assert_eq!(meta.edge_count, 1);
        assert!(meta.is_diskgraph);
    }
}
