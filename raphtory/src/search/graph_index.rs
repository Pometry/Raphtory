use crate::{
    core::{
        entities::{EID, VID},
        storage::timeindex::TimeIndexEntry,
        utils::errors::GraphError,
    },
    db::api::{storage::graph::storage_ops::GraphStorage, view::internal::InternalStorageOps},
    prelude::*,
    search::{
        edge_index::EdgeIndex, fields, node_index::NodeIndex, property_index::PropertyIndex,
        searcher::Searcher,
    },
};
use raphtory_api::core::{storage::dict_mapper::MaybeNew, PropType};
use std::{
    fmt::{Debug, Formatter},
    fs,
    fs::File,
    path::{Path, PathBuf},
};
use tantivy::schema::{FAST, INDEXED, STORED};
use tempfile::TempDir;
use uuid::Uuid;
use walkdir::WalkDir;
use zip::{write::FileOptions, ZipArchive, ZipWriter};

#[derive(Clone)]
pub struct GraphIndex {
    pub(crate) node_index: NodeIndex,
    pub(crate) edge_index: EdgeIndex,
    pub path: Option<PathBuf>, // If path is None, index is created in-memory
}

impl Debug for GraphIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphIndex")
            .field("node_index", &self.node_index)
            .field("edge_index", &self.edge_index)
            .finish()
    }
}

impl GraphIndex {
    fn copy_dir_recursive(source: &PathBuf, destination: &PathBuf) -> Result<(), GraphError> {
        for entry in WalkDir::new(source) {
            let entry = entry.map_err(|e| {
                GraphError::IOErrorMsg(format!("Failed to read directory entry: {}", e))
            })?;

            let relative_path = entry.path().strip_prefix(source).map_err(|e| {
                GraphError::IOErrorMsg(format!(
                    "Failed to determine relative path during copy: {}",
                    e
                ))
            })?;
            let dest_path = destination.join(relative_path);

            if entry.path().is_dir() {
                fs::create_dir_all(&dest_path).map_err(|e| {
                    GraphError::IOErrorMsg(format!(
                        "Failed to create directory {}: {}",
                        dest_path.display(),
                        e
                    ))
                })?;
            } else if entry.path().is_file() {
                if let Some(parent) = dest_path.parent() {
                    fs::create_dir_all(parent).map_err(|e| {
                        GraphError::IOErrorMsg(format!(
                            "Failed to create parent directory {}: {}",
                            parent.display(),
                            e
                        ))
                    })?;
                }

                fs::copy(entry.path(), &dest_path).map_err(|e| {
                    GraphError::IOErrorMsg(format!(
                        "Failed to copy file {} to {}: {}",
                        entry.path().display(),
                        dest_path.display(),
                        e
                    ))
                })?;
            }
        }

        Ok(())
    }

    fn unzip_index(source: &PathBuf, destination: &PathBuf) -> Result<(), GraphError> {
        let file = File::open(source)?;
        let mut archive = ZipArchive::new(file)?;

        for i in 0..archive.len() {
            let mut entry = archive.by_index(i)?;
            let name = entry.name().to_string();
            if !name.starts_with("index/") {
                continue;
            }

            let rel_path = Path::new(&name)
                .strip_prefix("index/")
                .map_err(|e| GraphError::IOErrorMsg(format!("Failed to strip 'index/': {}", e)))?;

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
        let tmp_path = &TempDir::new()?.path().to_path_buf();
        if path.is_file() {
            GraphIndex::unzip_index(path, tmp_path)?;
        } else {
            GraphIndex::copy_dir_recursive(path, tmp_path)?;
        }

        let node_index = NodeIndex::load_from_path(&tmp_path.join("nodes"))?;
        let edge_index = EdgeIndex::load_from_path(&tmp_path.join("edges"))?;
        let path = Some(tmp_path.clone());

        Ok(GraphIndex {
            node_index,
            edge_index,
            path,
        })
    }

    pub fn create_from_graph(
        graph: &GraphStorage,
        create_in_ram: bool,
    ) -> Result<Self, GraphError> {
        let path = if !create_in_ram {
            Some(TempDir::new()?.path().to_path_buf())
        } else {
            None
        };
        let node_index = NodeIndex::index_nodes(graph, &path)?;
        // node_index.print()?;

        let edge_index = EdgeIndex::index_edges(graph, &path)?;
        // edge_index.print()?;

        Ok(GraphIndex {
            node_index,
            edge_index,
            path,
        })
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

    pub(crate) fn persist_to_disk(&self, path: &PathBuf) -> Result<(), GraphError> {
        let source_path = self.path.as_ref().ok_or(GraphError::GraphIndexIsMissing)?;

        let temp_path = &path.with_extension(format!("tmp-{}", Uuid::new_v4()));

        GraphIndex::copy_dir_recursive(source_path, temp_path)?;

        // Always overwrite the existing graph index when persisting, since the in-memory
        // working index may have newer updates. The persisted index is decoupled from the
        // active one, and changes remain in memory unless explicitly saved.
        // This behavior mirrors how the in-memory graph works — updates are not persisted
        // unless manually saved, except when using the cached view (see db/graph/views/cached_view).
        if path.exists() {
            fs::remove_dir_all(path)
                .map_err(|_e| GraphError::FailedToRemoveExistingGraphIndex(path.clone()))?;
        }

        fs::rename(temp_path, path).map_err(|e| {
            GraphError::IOErrorMsg(format!("Failed to rename temp index folder: {}", e))
        })?;

        Ok(())
    }

    pub(crate) fn persist_to_disk_zip(&self, path: &PathBuf) -> Result<(), GraphError> {
        let index_path = &path.join("index");

        let source_path = self.path.as_ref().ok_or(GraphError::GraphIndexIsMissing)?;

        if index_path.exists() {
            fs::remove_dir_all(index_path)
                .map_err(|_e| GraphError::FailedToRemoveExistingGraphIndex(index_path.clone()))?;
        }

        let file = File::options().read(true).write(true).open(path)?;

        let mut zip = ZipWriter::new_append(file)?;

        for entry in WalkDir::new(&source_path)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|e| e.path().is_file())
        {
            let rel_path = entry
                .path()
                .strip_prefix(source_path)
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
        self.node_index.add_node_update(graph, t, v, props)
    }

    pub(crate) fn add_node_constant_properties(
        &self,
        graph: &GraphStorage,
        node_id: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        self.node_index
            .add_node_constant_properties(graph, node_id, props)
    }

    pub(crate) fn update_node_constant_properties(
        &self,
        graph: &GraphStorage,
        node_id: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        self.node_index
            .update_node_constant_properties(graph, node_id, props)
    }

    pub(crate) fn add_edge_update(
        &self,
        graph: &GraphStorage,
        edge_id: MaybeNew<EID>,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        layer: usize,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        self.edge_index
            .add_edge_update(graph, edge_id, t, src, dst, layer, props)
    }

    pub(crate) fn add_edge_constant_properties(
        &self,
        graph: &GraphStorage,
        edge_id: EID,
        layer: usize,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        self.edge_index
            .add_edge_constant_properties(graph, edge_id, layer, props)
    }

    pub(crate) fn update_edge_constant_properties(
        &self,
        graph: &GraphStorage,
        edge_id: EID,
        layer: usize,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        self.edge_index
            .update_edge_constant_properties(graph, edge_id, layer, props)
    }

    pub(crate) fn create_edge_property_index(
        &self,
        prop_id: MaybeNew<usize>,
        prop_name: &str,
        prop_type: &PropType,
        is_static: bool, // Const or Temporal Property
    ) -> Result<(), GraphError> {
        let edge_index_path = &self.path.as_deref().map(|p| p.join("edges"));
        self.edge_index.entity_index.create_property_index(
            prop_id,
            prop_name,
            prop_type,
            is_static,
            |schema| {
                schema.add_u64_field(fields::EDGE_ID, INDEXED | FAST | STORED);
                schema.add_u64_field(fields::LAYER_ID, INDEXED | FAST | STORED);
            },
            |schema| {
                schema.add_i64_field(fields::TIME, INDEXED | FAST | STORED);
                schema.add_u64_field(fields::SECONDARY_TIME, INDEXED | FAST | STORED);
                schema.add_u64_field(fields::EDGE_ID, INDEXED | FAST | STORED);
                schema.add_u64_field(fields::LAYER_ID, INDEXED | FAST | STORED);
            },
            PropertyIndex::new_edge_property,
            edge_index_path,
        )
    }

    pub(crate) fn create_node_property_index(
        &self,
        prop_id: MaybeNew<usize>,
        prop_name: &str,
        prop_type: &PropType,
        is_static: bool, // Const or Temporal Property
    ) -> Result<(), GraphError> {
        let node_index_path = self.path.as_deref().map(|p| p.join("nodes"));
        self.node_index.entity_index.create_property_index(
            prop_id,
            prop_name,
            prop_type,
            is_static,
            |schema| {
                schema.add_u64_field(fields::NODE_ID, INDEXED | FAST | STORED);
            },
            |schema| {
                schema.add_i64_field(fields::TIME, INDEXED | FAST | STORED);
                schema.add_u64_field(fields::SECONDARY_TIME, INDEXED | FAST | STORED);
                schema.add_u64_field(fields::NODE_ID, INDEXED | FAST | STORED);
            },
            PropertyIndex::new_node_property,
            &node_index_path,
        )
    }
}

#[cfg(test)]
mod graph_index_test {
    use crate::{
        db::{api::view::SearchableGraphOps, graph::views::filter::model::PropertyFilterOps},
        prelude::{AdditionOps, EdgeViewOps, Graph, GraphViewOps, NodeViewOps, PropertyFilter},
    };

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
            .add_edge(1, 1, 2, [("p1", 1), ("p2", 2)], Some("fire_nation"))
            .unwrap();
        graph
            .add_edge(2, 1, 2, [("p6", 6)], Some("fire_nation"))
            .unwrap();
        graph
            .add_edge(2, 2, 3, [("p4", 5)], Some("fire_nation"))
            .unwrap();
        graph
            .add_edge(3, 3, 4, [("p2", 4), ("p3", 3)], Some("water_tribe"))
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
        let res = graph.search_nodes(filter, 20, 0).unwrap();
        let res = res.iter().map(|n| n.name()).collect::<Vec<_>>();
        assert_eq!(res, vec!["1"]);

        graph
            .node(1)
            .unwrap()
            .update_constant_properties([("x", 2u64)])
            .unwrap();
        let filter = PropertyFilter::property("x").constant().eq(1u64);
        let res = graph.search_nodes(filter, 20, 0).unwrap();
        let res = res.iter().map(|n| n.name()).collect::<Vec<_>>();
        assert_eq!(res, Vec::<&str>::new());

        graph
            .node(1)
            .unwrap()
            .update_constant_properties([("x", 2u64)])
            .unwrap();
        let filter = PropertyFilter::property("x").constant().eq(2u64);
        let res = graph.search_nodes(filter, 20, 0).unwrap();
        let res = res.iter().map(|n| n.name()).collect::<Vec<_>>();
        assert_eq!(res, vec!["1"]);
    }

    #[test]
    fn test_edge_const_property_graph_index_is_ok() {
        let graph = Graph::new();
        let graph = init_edges_graph(graph);
        graph.create_index_in_ram().unwrap();
        graph
            .edge(1, 2)
            .unwrap()
            .add_constant_properties([("x", 1u64)], Some("fire_nation"))
            .unwrap();

        let filter = PropertyFilter::property("x").constant().eq(1u64);
        let res = graph.search_edges(filter, 20, 0).unwrap();
        let res = res
            .iter()
            .map(|e| format!("{}->{}", e.src().name(), e.dst().name()))
            .collect::<Vec<_>>();
        assert_eq!(res, vec!["1->2"]);

        graph
            .edge(1, 2)
            .unwrap()
            .update_constant_properties([("x", 2u64)], Some("fire_nation"))
            .unwrap();
        let filter = PropertyFilter::property("x").constant().eq(1u64);
        let res = graph.search_edges(filter, 20, 0).unwrap();
        let res = res
            .iter()
            .map(|e| format!("{}->{}", e.src().name(), e.dst().name()))
            .collect::<Vec<_>>();
        assert_eq!(res, Vec::<&str>::new());

        graph
            .edge(1, 2)
            .unwrap()
            .update_constant_properties([("x", 2u64)], Some("fire_nation"))
            .unwrap();
        let filter = PropertyFilter::property("x").constant().eq(2u64);
        let res = graph.search_edges(filter, 20, 0).unwrap();
        let res = res
            .iter()
            .map(|e| format!("{}->{}", e.src().name(), e.dst().name()))
            .collect::<Vec<_>>();
        assert_eq!(res, vec!["1->2"]);
    }
}
