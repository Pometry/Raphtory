use crate::{
    core::{
        entities::{EID, VID},
        storage::timeindex::EventTime,
    },
    db::api::view::IndexSpec,
    errors::GraphError,
    prelude::*,
    search::{edge_index::EdgeIndex, node_index::NodeIndex, searcher::Searcher},
    serialise::{GraphFolder, GraphPaths, InnerGraphFolder, INDEX_PATH},
};
use parking_lot::RwLock;
use raphtory_api::core::storage::dict_mapper::MaybeNew;
use raphtory_storage::graph::graph::GraphStorage;
use std::{
    ffi::OsStr,
    fmt::Debug,
    fs,
    fs::File,
    io::{Seek, Write},
    ops::Deref,
    path::{Path, PathBuf},
    sync::Arc,
};
use tempfile::TempDir;
use uuid::Uuid;
use walkdir::WalkDir;
use zip::{
    write::{FileOptions, SimpleFileOptions},
    ZipArchive, ZipWriter,
};
use raphtory_api::core::entities::LayerId;

#[derive(Clone)]
pub struct Index {
    pub(crate) node_index: NodeIndex,
    pub(crate) edge_index: EdgeIndex,
}

impl Index {
    pub fn print(&self) -> Result<(), GraphError> {
        self.node_index.print()?;
        self.edge_index.print()?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct ImmutableGraphIndex {
    pub(crate) index: Index,
    pub(crate) path: Arc<InnerGraphFolder>,
    pub index_spec: Arc<IndexSpec>,
}

#[derive(Clone)]
pub struct MutableGraphIndex {
    pub(crate) index: Index,
    pub(crate) path: Option<Arc<TempDir>>,
    pub index_spec: Arc<RwLock<IndexSpec>>,
}

impl MutableGraphIndex {
    pub fn update(&self, graph: &GraphStorage, index_spec: IndexSpec) -> Result<(), GraphError> {
        let mut existing_spec = self.index_spec.write();

        if let Some(diff_spec) = IndexSpec::diff(&*existing_spec, &index_spec) {
            let path = get_node_index_path(&self.path);
            self.index
                .node_index
                .index_nodes_props(graph, path, &diff_spec)?;

            let path = get_edge_index_path(&self.path);
            self.index
                .edge_index
                .index_edges_props(graph, path, &diff_spec)?;

            // self.index.print()?;

            *existing_spec = IndexSpec::union(&*existing_spec, &diff_spec);
        }

        Ok(())
    }

    pub(crate) fn add_new_node(
        &self,
        node_id: VID,
        name: String,
        node_type: Option<&str>,
    ) -> Result<(), GraphError> {
        self.index.node_index.add_new_node(node_id, name, node_type)
    }

    pub(crate) fn add_node_update(
        &self,
        t: EventTime,
        v: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        self.index.node_index.add_node_update(t, v, props)?;
        Ok(())
    }

    pub(crate) fn add_node_metadata(
        &self,
        node_id: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        self.index.node_index.add_node_metadata(node_id, props)?;
        Ok(())
    }

    pub(crate) fn update_node_metadata(
        &self,
        node_id: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        self.index.node_index.update_node_metadata(node_id, props)
    }

    pub(crate) fn add_edge_update(
        &self,
        graph: &GraphStorage,
        edge_id: MaybeNew<EID>,
        t: EventTime,
        layer: LayerId,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        self.index
            .edge_index
            .add_edge_update(graph, edge_id, t, layer, props)
    }

    pub(crate) fn add_edge_metadata(
        &self,
        edge_id: EID,
        layer: LayerId,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        self.index
            .edge_index
            .add_edge_metadata(edge_id, layer, props)
    }

    pub(crate) fn update_edge_metadata(
        &self,
        edge_id: EID,
        layer: LayerId,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        self.index
            .edge_index
            .update_edge_metadata(edge_id, layer, props)
    }
}

#[derive(Clone, Default)]
pub enum GraphIndex {
    #[default]
    Empty,
    Immutable(ImmutableGraphIndex),
    Mutable(MutableGraphIndex),
}

impl Debug for GraphIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GraphIndex::Immutable(i) => f
                .debug_struct("GraphIndex::Immutable")
                .field("node_index", &i.index.node_index)
                .field("edge_index", &i.index.edge_index)
                .field("path", &i.path)
                .field("index_spec", &i.index_spec)
                .finish(),
            GraphIndex::Mutable(m) => f
                .debug_struct("GraphIndex::Mutable")
                .field("node_index", &m.index.node_index)
                .field("edge_index", &m.index.edge_index)
                .field("path", &m.path.as_ref().map(|p| p.path()))
                .field("index_spec", &m.index_spec)
                .finish(),
            GraphIndex::Empty => f.debug_struct("GraphIndex::Empty").finish(),
        }
    }
}

impl GraphIndex {
    pub fn create(
        graph: &GraphStorage,
        create_in_ram: bool,
        cached_graph_path: Option<GraphFolder>,
    ) -> Result<Self, GraphError> {
        let dir = if !create_in_ram {
            let temp_dir = match cached_graph_path {
                // Creates index in a temp dir within cache graph dir.
                // The intention is to avoid creating index in a tmp dir that could be on another file system.
                Some(path) => TempDir::new_in(path.root())?,
                None => TempDir::new()?,
            };

            Some(Arc::new(temp_dir))
        } else {
            None
        };

        let index_spec = IndexSpec::default();

        let path = get_node_index_path(&dir);
        let node_index = NodeIndex::new(&path)?;
        node_index.index_nodes_fields(graph)?;

        let path = get_edge_index_path(&dir);
        let edge_index = EdgeIndex::new(&path)?;
        edge_index.index_edges_fields(graph)?;

        Ok(GraphIndex::Mutable(MutableGraphIndex {
            index: Index {
                node_index,
                edge_index,
            },
            path: dir,
            index_spec: Arc::new(RwLock::new(index_spec)),
        }))
    }

    pub fn load_from_path(path: &GraphFolder) -> Result<GraphIndex, GraphError> {
        if path.is_zip() {
            let index_path = TempDir::new()?;
            unzip_index(&path.root(), index_path.path())?;

            let (index, index_spec) = load_indexes(index_path.path())?;

            Ok(GraphIndex::Mutable(MutableGraphIndex {
                index,
                path: Some(Arc::new(index_path)),
                index_spec: Arc::new(RwLock::new(index_spec)),
            }))
        } else {
            let index_path = path.index_path()?;
            let (index, index_spec) = load_indexes(index_path.as_path())?;

            Ok(GraphIndex::Immutable(ImmutableGraphIndex {
                index,
                path: Arc::new(path.data_path()?),
                index_spec: Arc::new(index_spec),
            }))
        }
    }

    pub(crate) fn persist_to_disk(&self, path: &impl GraphPaths) -> Result<(), GraphError> {
        let source_path = self.path().ok_or(GraphError::CannotPersistRamIndex)?;
        let path = path.index_path()?;
        if source_path != path {
            copy_dir_recursive(&source_path, &path)?;
        }
        Ok(())
    }

    pub(crate) fn persist_to_disk_zip<W: Write + Seek>(
        &self,
        writer: &mut ZipWriter<W>,
        prefix: &str,
    ) -> Result<(), GraphError> {
        let source_path = self.path().ok_or(GraphError::CannotPersistRamIndex)?;
        for entry in WalkDir::new(&source_path)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|e| e.path().is_file())
        {
            let rel_path = entry.path().strip_prefix(&source_path)?;

            let zip_entry_name = Path::new(prefix).join(rel_path);
            writer.start_file_from_path(zip_entry_name, SimpleFileOptions::default())?;

            let mut f = File::open(entry.path())?;

            std::io::copy(&mut f, writer)?;
        }
        Ok(())
    }

    pub fn make_mutable_if_needed(&mut self) -> Result<(), GraphError> {
        if let GraphIndex::Immutable(immutable) = self {
            let temp_dir = TempDir::new_in(immutable.path.as_ref())?;
            let temp_path = temp_dir.path();

            copy_dir_recursive(&immutable.path.index_path(), temp_path)?;

            let node_index = NodeIndex::load_from_path(&temp_path.join("nodes"))?;
            let edge_index = EdgeIndex::load_from_path(&temp_path.join("edges"))?;

            let index_spec = immutable.index_spec.clone();

            *self = GraphIndex::Mutable(MutableGraphIndex {
                index: Index {
                    node_index,
                    edge_index,
                },
                path: Some(Arc::new(temp_dir)),
                index_spec: Arc::new(RwLock::new(index_spec.deref().clone())),
            });
        }
        Ok(())
    }

    pub fn is_immutable(&self) -> bool {
        matches!(self, GraphIndex::Immutable(_))
    }

    pub fn index(&self) -> Option<&Index> {
        match self {
            GraphIndex::Immutable(i) => Some(&i.index),
            GraphIndex::Mutable(m) => Some(&m.index),
            GraphIndex::Empty => None,
        }
    }

    pub fn path(&self) -> Option<PathBuf> {
        match self {
            GraphIndex::Immutable(i) => Some(i.path.index_path()),
            GraphIndex::Mutable(m) => m.path.as_ref().map(|p| p.path().to_path_buf()),
            GraphIndex::Empty => None,
        }
    }

    pub fn index_spec(&self) -> IndexSpec {
        match self {
            GraphIndex::Immutable(i) => i.index_spec.deref().clone(),
            GraphIndex::Mutable(m) => m.index_spec.read_recursive().deref().clone(),
            GraphIndex::Empty => IndexSpec::default(),
        }
    }

    pub fn is_indexed(&self) -> bool {
        !matches!(self, GraphIndex::Empty)
    }

    pub fn searcher(&self) -> Option<Searcher<'_>> {
        self.index().map(Searcher::new)
    }
}

fn get_node_index_path(path: &Option<Arc<TempDir>>) -> Option<PathBuf> {
    path.as_ref().map(|p| p.path().join("nodes"))
}

fn get_edge_index_path(path: &Option<Arc<TempDir>>) -> Option<PathBuf> {
    path.as_ref().map(|p| p.path().join("edges"))
}

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

fn load_indexes(index_path: &Path) -> Result<(Index, IndexSpec), GraphError> {
    let node_index = NodeIndex::load_from_path(&index_path.join("nodes"))?;
    let edge_index = EdgeIndex::load_from_path(&index_path.join("edges"))?;

    let index_spec = IndexSpec {
        node_metadata: node_index.resolve_metadata(),
        node_properties: node_index.resolve_properties(),
        edge_metadata: edge_index.resolve_metadata(),
        edge_properties: edge_index.resolve_properties(),
    };

    Ok((
        Index {
            node_index,
            edge_index,
        },
        index_spec,
    ))
}

#[cfg(test)]
mod graph_index_test {
    use crate::prelude::{AdditionOps, Graph, GraphViewOps};

    use crate::db::graph::views::filter::model::{
        edge_filter::EdgeFilter, node_filter::NodeFilter, property_filter::ops::PropertyFilterOps,
        PropertyFilterFactory,
    };
    #[cfg(feature = "search")]
    use crate::{
        db::graph::assertions::{search_edges, search_nodes},
        prelude::IndexMutationOps,
    };

    fn init_nodes_graph(graph: Graph) -> Graph {
        graph
            .add_node(1, 1, [("p1", 1), ("p2", 2)], Some("fire_nation"), None)
            .unwrap();
        graph
            .add_node(2, 1, [("p6", 6)], Some("fire_nation"), None)
            .unwrap();
        graph
            .add_node(2, 2, [("p4", 5)], Some("fire_nation"), None)
            .unwrap();
        graph
            .add_node(3, 3, [("p2", 4), ("p3", 3)], Some("water_tribe"), None)
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

        graph.create_index_in_ram().unwrap();
    }

    #[test]
    fn test_if_adding_nodes_to_existing_graph_index_is_ok() {
        let graph = Graph::new();
        graph.create_index_in_ram().unwrap();

        let graph = init_nodes_graph(graph);

        assert_eq!(graph.count_nodes(), 3);
    }

    #[test]
    fn test_if_adding_edges_to_existing_graph_index_is_ok() {
        let graph = Graph::new();
        // Creates graph index
        graph.create_index_in_ram().unwrap();

        let graph = init_edges_graph(graph);

        assert_eq!(graph.count_edges(), 3);
    }

    #[test]
    fn test_node_metadata_graph_index_is_ok() {
        let graph = Graph::new();
        let graph = init_nodes_graph(graph);
        graph.create_index_in_ram().unwrap();
        graph.node(1).unwrap().add_metadata([("x", 1u64)]).unwrap();

        let filter = NodeFilter.metadata("x").eq(1u64);
        assert_eq!(search_nodes(&graph, filter.clone()), vec!["1"]);

        graph
            .node(1)
            .unwrap()
            .update_metadata([("x", 2u64)])
            .unwrap();
        let filter = NodeFilter.metadata("x").eq(1u64);
        assert_eq!(search_nodes(&graph, filter.clone()), Vec::<&str>::new());

        graph
            .node(1)
            .unwrap()
            .update_metadata([("x", 2u64)])
            .unwrap();
        let filter = NodeFilter.metadata("x").eq(2u64);
        assert_eq!(search_nodes(&graph, filter.clone()), vec!["1"]);
    }

    #[test]
    fn test_edge_metadata_graph_index_is_ok() {
        let graph = Graph::new();
        let graph = init_edges_graph(graph);
        graph.create_index_in_ram().unwrap();
        graph
            .edge(1, 2)
            .unwrap()
            .add_metadata([("x", 1u64)], None)
            .unwrap();

        let filter = EdgeFilter.metadata("x").eq(1u64);
        assert_eq!(search_edges(&graph, filter.clone()), vec!["1->2"]);

        graph
            .edge(1, 2)
            .unwrap()
            .update_metadata([("x", 2u64)], None)
            .unwrap();
        let filter = EdgeFilter.metadata("x").eq(1u64);
        assert_eq!(search_edges(&graph, filter.clone()), Vec::<&str>::new());

        graph
            .edge(1, 2)
            .unwrap()
            .update_metadata([("x", 2u64)], None)
            .unwrap();
        let filter = EdgeFilter.metadata("x").eq(2u64);
        assert_eq!(search_edges(&graph, filter.clone()), vec!["1->2"]);
    }
}
