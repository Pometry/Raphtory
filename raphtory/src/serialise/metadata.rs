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

/// Update the node/edge counts in an existing `.meta` file
pub fn refresh_disk_graph_metadata(
    disk_graph_path: &Path,
    node_count: usize,
    edge_count: usize,
) -> Result<(), StorageError> {
    let Some(data_folder) = disk_graph_path.parent() else {
        return Ok(());
    };
    if !data_folder.is_dir() {
        return Ok(());
    }

    let meta_path = data_folder.join(GRAPH_META_PATH);
    let json = match fs::read_to_string(&meta_path) {
        Ok(json) => json,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err.into()),
    };

    let mut meta: Metadata = serde_json::from_str(&json)?;
    if meta.meta.node_count == node_count && meta.meta.edge_count == edge_count {
        return Ok(());
    }
    meta.meta.node_count = node_count;
    meta.meta.edge_count = edge_count;

    // write to disk
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

        // Explicitly flush the graph to update the .meta file
        graph.flush().unwrap();

        let meta = folder.read_metadata().unwrap();
        assert_eq!(meta.node_count, graph.count_nodes());
        assert_eq!(meta.edge_count, graph.count_edges());
        assert_eq!(meta.node_count, 2);
        assert_eq!(meta.edge_count, 1);
        assert!(meta.is_diskgraph);
    }

    #[test]
    fn drop_updates_disk_graph_metadata_counts() {
        use storage::{persist::strategy::PersistenceStrategy, Extension};
        if !<Extension as PersistenceStrategy>::disk_storage_enabled() {
            return;
        }

        let tmp = tempfile::TempDir::new().unwrap();
        let folder = GraphFolder::from(tmp.path().join("g"));

        {
            let graph = Graph::new_at_path(&folder).unwrap();
            graph.add_node(0, "a", NO_PROPS, None, None).unwrap();
            graph.add_node(0, "b", NO_PROPS, None, None).unwrap();
            graph.add_node(0, "c", NO_PROPS, None, None).unwrap();
            graph.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
            graph.add_edge(1, "b", "c", NO_PROPS, None).unwrap();
            // No explicit flush - rely on Drop to refresh `.meta`.
        }

        let meta = folder.read_metadata().unwrap();
        assert_eq!(meta.node_count, 3);
        assert_eq!(meta.edge_count, 2);
        assert!(meta.is_diskgraph);
    }
}
