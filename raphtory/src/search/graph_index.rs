use crate::{
    core::{
        entities::{EID, VID},
        storage::timeindex::TimeIndexEntry,
        utils::errors::GraphError,
    },
    db::api::{storage::graph::storage_ops::GraphStorage, view::IndexSpec},
    prelude::*,
    search::{edge_index::EdgeIndex, node_index::NodeIndex, searcher::Searcher},
};
use parking_lot::RwLock;
use raphtory_api::core::storage::dict_mapper::MaybeNew;
use std::{
    ffi::OsStr,
    fmt::{Debug, Formatter},
    fs,
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
};
use tempfile::TempDir;
use uuid::Uuid;
use walkdir::WalkDir;
use zip::{write::FileOptions, ZipArchive, ZipWriter};

#[derive(Clone)]
pub struct GraphIndex {
    pub(crate) node_index: NodeIndex,
    pub(crate) edge_index: EdgeIndex,
    pub path: Option<Arc<TempDir>>, // If path is None, index is created in-memory
    pub index_spec: Arc<RwLock<IndexSpec>>,
}

impl Debug for GraphIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphIndex")
            .field("node_index", &self.node_index)
            .field("edge_index", &self.edge_index)
            .field("path", &self.path.as_ref().map(|p| p.path()))
            .field("index_spec", &self.index_spec)
            .finish()
    }
}

impl GraphIndex {
    fn copy_dir_recursive(source: &Path, destination: &Path) -> Result<(), GraphError> {
        for entry in WalkDir::new(source) {
            let entry = entry.map_err(|e| {
                GraphError::IOErrorMsg(format!("Failed to read directory entry: {}", e))
            })?;

            let entry_path = entry.path();

            if entry_path.starts_with(destination) {
                continue;
            }

            let relative_path = entry_path.strip_prefix(source).map_err(|e| {
                GraphError::IOErrorMsg(format!(
                    "Failed to determine relative path during copy: {}",
                    e
                ))
            })?;

            let dest_path = destination.join(relative_path);

            if entry_path.is_dir() {
                fs::create_dir_all(&dest_path).map_err(|e| {
                    GraphError::IOErrorMsg(format!(
                        "Failed to create directory {}: {}",
                        dest_path.display(),
                        e
                    ))
                })?;
            } else if entry_path.is_file() {
                if let Some(parent) = dest_path.parent() {
                    fs::create_dir_all(parent).map_err(|e| {
                        GraphError::IOErrorMsg(format!(
                            "Failed to create parent directory {}: {}",
                            parent.display(),
                            e
                        ))
                    })?;
                }

                fs::copy(entry_path, &dest_path).map_err(|e| {
                    GraphError::IOErrorMsg(format!(
                        "Failed to copy file {} to {}: {}",
                        entry_path.display(),
                        dest_path.display(),
                        e
                    ))
                })?;
            }
        }

        Ok(())
    }

    fn unzip_index(source: &Path, destination: &Path) -> Result<(), GraphError> {
        let file = File::open(source)?;
        let mut archive = ZipArchive::new(file)?;

        for i in 0..archive.len() {
            let mut entry = archive.by_index(i)?;
            let entry_path = Path::new(entry.name());

            // Check if the first component is "index"
            if entry_path.components().next().map(|c| c.as_os_str()) != Some(OsStr::new("index")) {
                continue;
            }

            // Strip "index" from the path
            let rel_path = entry_path.strip_prefix("index").map_err(|e| {
                GraphError::IOErrorMsg(format!("Failed to strip 'index' prefix: {}", e))
            })?;

            let out_path = destination.join(rel_path);

            if let Some(parent) = out_path.parent() {
                fs::create_dir_all(parent)?;
            }

            let mut outfile = File::create(&out_path)?;
            std::io::copy(&mut entry, &mut outfile)?;
        }

        Ok(())
    }

    pub fn load_from_path(path: &PathBuf) -> Result<Self, GraphError> {
        let tmp_path = TempDir::new_in(path)?;
        let path = path.join("index");
        let path = path.as_path();
        if path.is_file() {
            GraphIndex::unzip_index(path, tmp_path.path())?;
        } else {
            GraphIndex::copy_dir_recursive(path, tmp_path.path())?;
        }

        let node_index = NodeIndex::load_from_path(&tmp_path.path().join("nodes"))?;
        let edge_index = EdgeIndex::load_from_path(&tmp_path.path().join("edges"))?;
        let path = Some(Arc::new(tmp_path));

        let index_spec = IndexSpec {
            node_const_props: node_index.resolve_const_props(),
            node_temp_props: node_index.resolve_temp_props(),
            edge_const_props: edge_index.resolve_const_props(),
            edge_temp_props: edge_index.resolve_temp_props(),
        };

        Ok(GraphIndex {
            node_index: node_index.clone(),
            edge_index: edge_index.clone(),
            path,
            index_spec: Arc::new(RwLock::new(index_spec)),
        })
    }

    fn get_node_index_path(path: &Option<Arc<TempDir>>) -> Option<PathBuf> {
        path.as_ref().map(|d| d.path().join("nodes"))
    }

    fn get_edge_index_path(path: &Option<Arc<TempDir>>) -> Option<PathBuf> {
        path.as_ref().map(|d| d.path().join("edges"))
    }

    pub fn create(
        graph: &GraphStorage,
        create_in_ram: bool,
        cached_graph_path: Option<&Path>,
    ) -> Result<Self, GraphError> {
        let dir = if !create_in_ram {
            let temp_dir = match cached_graph_path {
                // Creates index in a temp dir within cache graph dir.
                // The intention is to avoid creating index in a tmp dir that could be on another file system.
                Some(path) => TempDir::new_in(path)?,
                None => TempDir::new()?,
            };
            Some(Arc::new(temp_dir))
        } else {
            None
        };

        let index_spec = IndexSpec::default();

        let path = GraphIndex::get_node_index_path(&dir);
        let node_index = NodeIndex::new(&path)?;
        node_index.index_nodes(graph, path, &index_spec)?;

        let path = GraphIndex::get_edge_index_path(&dir);
        let edge_index = EdgeIndex::new(&path)?;
        edge_index.index_edges(graph, path, &index_spec)?;

        Ok(GraphIndex {
            node_index,
            edge_index,
            path: dir,
            index_spec: Arc::new(RwLock::new(index_spec)),
        })
    }

    pub fn update(&self, graph: &GraphStorage, index_spec: IndexSpec) -> Result<(), GraphError> {
        let mut existing_spec = self.index_spec.write();

        if let Some(diff_spec) = IndexSpec::diff(&*existing_spec, &index_spec) {
            let path = GraphIndex::get_node_index_path(&self.path);
            self.node_index.index_nodes(graph, path, &diff_spec)?;
            // self.node_index.print()?;

            let path = GraphIndex::get_edge_index_path(&self.path);
            self.edge_index.index_edges(graph, path, &diff_spec)?;
            // self.edge_index.print()?;

            *existing_spec = IndexSpec::union(&*existing_spec, &diff_spec);
        }

        Ok(())
    }

    pub fn searcher(&self) -> Searcher {
        Searcher::new(self)
    }

    #[allow(dead_code)]
    // Useful for debugging
    pub fn print(&self) -> Result<(), GraphError> {
        self.node_index.print()?;
        self.edge_index.print()?;
        Ok(())
    }

    pub(crate) fn persist_to_disk(&self, path: &Path) -> Result<(), GraphError> {
        let source_path = self.path.as_ref().ok_or(GraphError::GraphIndexIsMissing)?;

        let temp_path = &path.with_extension(format!("tmp-{}", Uuid::new_v4()));

        GraphIndex::copy_dir_recursive(source_path.path(), temp_path)?;

        // Always overwrite the existing graph index when persisting, since the in-memory
        // working index may have newer updates. The persisted index is decoupled from the
        // active one, and changes remain in memory unless explicitly saved.
        // This behavior mirrors how the in-memory graph works — updates are not persisted
        // unless manually saved, except when using the cached view (see db/graph/views/cached_view).
        if path.exists() {
            fs::remove_dir_all(path)
                .map_err(|_e| GraphError::FailedToRemoveExistingGraphIndex(path.to_path_buf()))?;
        }

        fs::rename(temp_path, path).map_err(|e| {
            GraphError::IOErrorMsg(format!("Failed to rename temp index folder: {}", e))
        })?;

        Ok(())
    }

    pub(crate) fn persist_to_disk_zip(&self, path: &Path) -> Result<(), GraphError> {
        let index_path = &path.join("index");

        let source_path = self.path.as_ref().ok_or(GraphError::GraphIndexIsMissing)?;

        if index_path.exists() {
            fs::remove_dir_all(index_path)
                .map_err(|_e| GraphError::FailedToRemoveExistingGraphIndex(index_path.clone()))?;
        }

        let file = File::options().read(true).write(true).open(path)?;

        let mut zip = ZipWriter::new_append(file)?;

        for entry in WalkDir::new(&source_path.path())
            .into_iter()
            .filter_map(Result::ok)
            .filter(|e| e.path().is_file())
        {
            let rel_path = entry
                .path()
                .strip_prefix(source_path.path())
                .map_err(|e| GraphError::IOErrorMsg(format!("Failed to strip path: {}", e)))?;

            let zip_entry_name = PathBuf::from("index")
                .join(rel_path)
                .to_string_lossy()
                .into_owned();
            zip.start_file::<_, ()>(zip_entry_name, FileOptions::default())
                .map_err(|e| {
                    GraphError::IOErrorMsg(format!("Failed to start zip file entry: {}", e))
                })?;

            let mut f = File::open(entry.path())
                .map_err(|e| GraphError::IOErrorMsg(format!("Failed to open index file: {}", e)))?;

            std::io::copy(&mut f, &mut zip).map_err(|e| {
                GraphError::IOErrorMsg(format!("Failed to write zip content: {}", e))
            })?;
        }

        zip.finish()
            .map_err(|e| GraphError::IOErrorMsg(format!("Failed to finalize zip: {}", e)))?;

        Ok(())
    }

    pub(crate) fn add_node_update(
        &self,
        graph: &GraphStorage,
        t: TimeIndexEntry,
        v: MaybeNew<VID>,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        self.node_index.add_node_update(graph, t, v, props)?;
        Ok(())
    }

    pub(crate) fn add_node_constant_properties(
        &self,
        node_id: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        self.node_index
            .add_node_constant_properties(node_id, props)?;
        Ok(())
    }

    pub(crate) fn update_node_constant_properties(
        &self,
        node_id: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        self.node_index
            .update_node_constant_properties(node_id, props)
    }

    pub(crate) fn add_edge_update(
        &self,
        graph: &GraphStorage,
        edge_id: MaybeNew<EID>,
        t: TimeIndexEntry,
        layer: usize,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        self.edge_index
            .add_edge_update(graph, edge_id, t, layer, props)
    }

    pub(crate) fn add_edge_constant_properties(
        &self,
        edge_id: EID,
        layer: usize,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        self.edge_index
            .add_edge_constant_properties(edge_id, layer, props)
    }

    pub(crate) fn update_edge_constant_properties(
        &self,
        edge_id: EID,
        layer: usize,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        self.edge_index
            .update_edge_constant_properties(edge_id, layer, props)
    }
}

#[cfg(test)]
mod graph_index_test {
    use crate::{
        db::{api::view::SearchableGraphOps, graph::views::filter::model::PropertyFilterOps},
        prelude::{AdditionOps, EdgeViewOps, Graph, GraphViewOps, NodeViewOps, PropertyFilter},
    };

    #[cfg(feature = "search")]
    use crate::db::graph::assertions::{search_edges, search_nodes};
    use crate::prelude::IndexMutationOps;

    fn init_nodes_graph(graph: Graph) -> Graph {
        graph
            .add_node(1, 1, [("p1", 1), ("p2", 2)], Some("fire_nation"))
            .unwrap();
        graph
            .add_node(2, 1, [("p6", 6)], Some("fire_nation"))
            .unwrap();
        graph
            .add_node(2, 2, [("p4", 5)], Some("fire_nation"))
            .unwrap();
        graph
            .add_node(3, 3, [("p2", 4), ("p3", 3)], Some("water_tribe"))
            .unwrap();
        graph
    }

    fn init_edges_graph(graph: Graph) -> Graph {
        graph
            .add_edge(1, 1, 2, [("p1", 1), ("p2", 2)], None)
            .unwrap();
        graph.add_edge(2, 1, 2, [("p6", 6)], None).unwrap();
        graph.add_edge(2, 2, 3, [("p4", 5)], None).unwrap();
        graph
            .add_edge(3, 3, 4, [("p2", 4), ("p3", 3)], None)
            .unwrap();
        graph
    }

    #[test]
    fn test_if_bulk_load_create_graph_index_is_ok() {
        let graph = Graph::new();
        let graph = init_nodes_graph(graph);

        assert_eq!(graph.count_nodes(), 3);

        let _ = graph.create_index_in_ram().unwrap();
    }

    #[test]
    fn test_if_adding_nodes_to_existing_graph_index_is_ok() {
        let graph = Graph::new();
        let _ = graph.create_index_in_ram().unwrap();

        let graph = init_nodes_graph(graph);

        assert_eq!(graph.count_nodes(), 3);
    }

    #[test]
    fn test_if_adding_edges_to_existing_graph_index_is_ok() {
        let graph = Graph::new();
        // Creates graph index
        let _ = graph.create_index_in_ram().unwrap();

        let graph = init_edges_graph(graph);

        assert_eq!(graph.count_edges(), 3);
    }

    #[test]
    fn test_node_const_property_graph_index_is_ok() {
        let graph = Graph::new();
        let graph = init_nodes_graph(graph);
        graph.create_index_in_ram().unwrap();
        graph
            .node(1)
            .unwrap()
            .add_constant_properties([("x", 1u64)])
            .unwrap();

        let filter = PropertyFilter::property("x").constant().eq(1u64);
        assert_eq!(search_nodes(&graph, filter.clone()), vec!["1"]);

        graph
            .node(1)
            .unwrap()
            .update_constant_properties([("x", 2u64)])
            .unwrap();
        let filter = PropertyFilter::property("x").constant().eq(1u64);
        assert_eq!(search_nodes(&graph, filter.clone()), Vec::<&str>::new());

        graph
            .node(1)
            .unwrap()
            .update_constant_properties([("x", 2u64)])
            .unwrap();
        let filter = PropertyFilter::property("x").constant().eq(2u64);
        assert_eq!(search_nodes(&graph, filter.clone()), vec!["1"]);
    }

    #[test]
    fn test_edge_const_property_graph_index_is_ok() {
        let graph = Graph::new();
        let graph = init_edges_graph(graph);
        graph.create_index_in_ram().unwrap();
        graph
            .edge(1, 2)
            .unwrap()
            .add_constant_properties([("x", 1u64)], None)
            .unwrap();

        let filter = PropertyFilter::property("x").constant().eq(1u64);
        assert_eq!(search_edges(&graph, filter.clone()), vec!["1->2"]);

        graph
            .edge(1, 2)
            .unwrap()
            .update_constant_properties([("x", 2u64)], None)
            .unwrap();
        let filter = PropertyFilter::property("x").constant().eq(1u64);
        assert_eq!(search_edges(&graph, filter.clone()), Vec::<&str>::new());

        graph
            .edge(1, 2)
            .unwrap()
            .update_constant_properties([("x", 2u64)], None)
            .unwrap();
        let filter = PropertyFilter::property("x").constant().eq(2u64);
        assert_eq!(search_edges(&graph, filter.clone()), vec!["1->2"]);
    }
}
