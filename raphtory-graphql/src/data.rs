use crate::{
    model::{algorithms::global_plugins::GlobalPlugins, create_dirs_if_not_present, GqlGraphType},
    server_config::AppConfig,
};
use moka::sync::Cache;
#[cfg(feature = "storage")]
use raphtory::disk_graph::DiskGraphStorage;
use raphtory::{
    core::{
        entities::nodes::node_ref::AsNodeRef,
        utils::errors::{
            GraphError,
            InvalidPathReason::{
                self, BackslashError, CurDirNotAllowed, DoubleForwardSlash, ParentDirNotAllowed,
                PathDoesNotExist, PathIsDirectory, PathNotParsable, PathNotUTF8, RootNotAllowed,
                SymlinkNotAllowed,
            },
        },
    },
    db::{
        api::{
            mutation::internal::InheritMutationOps,
            view::{internal::Static, Base, InheritViewOps, MaterializedGraph, StaticGraphViewOps},
        },
        graph::{edge::EdgeView, node::NodeView, views::deletion_graph::PersistentGraph},
    },
    prelude::*,
    search::IndexedGraph,
    vectors::{
        embedding_cache::EmbeddingCache, template::DocumentTemplate, vectorisable::Vectorisable,
        vectorised_graph::VectorisedGraph, EmbeddingFunction,
    },
};
use std::{
    collections::HashMap,
    fs,
    path::{Component, Path, PathBuf, StripPrefixError},
    sync::Arc,
};
use walkdir::WalkDir;

#[derive(Clone)]
pub(crate) struct ExistingGraphFolder {
    pub(crate) user_facing_path: PathBuf,
    pub(crate) base_path: PathBuf,
    pub(crate) graph_path: PathBuf,
    pub(crate) vectors_path: Option<PathBuf>,
}

const VECTORS_FILE_NAME: &str = "vectors";

struct VectorsPath(PathBuf);

impl VectorsPath {
    fn path(&self) -> &Path {
        &self.0
    }
}

impl ExistingGraphFolder {
    fn try_from(base_path: PathBuf, relative_path: &str) -> Result<Self, GraphError> {
        let graph_folder = ValidGraphFolder::try_from(base_path, relative_path)?;
        if graph_folder.graph_path.exists() {
            let vectors_path = if graph_folder.vectors_path.exists() {
                Some(graph_folder.vectors_path)
            } else {
                None
            };
            Ok(Self {
                user_facing_path: graph_folder.user_facing_path,
                base_path: graph_folder.base_path,
                graph_path: graph_folder.graph_path,
                vectors_path,
            })
        } else {
            // TODO: review if it is ok using GraphError::GraphNotFound instead of PathDoesNotExist here
            Err(GraphError::GraphNotFound(
                graph_folder.user_facing_path.clone(),
            ))
        }
    }

    pub(crate) fn get_vector_path_to_write(&self) -> VectorsPath {
        VectorsPath(self.base_path.join(VECTORS_FILE_NAME))
    }

    pub(crate) fn get_graph_name(&self) -> Result<String, GraphError> {
        let path = &self.user_facing_path;
        let last_component: Component = path
            .components()
            .last()
            .ok_or_else(|| GraphError::from(PathNotParsable(path.clone())))?;
        match last_component {
            Component::Normal(value) => value
                .to_str()
                .map(|s| s.to_string())
                .ok_or(GraphError::from(PathNotParsable(path.clone()))),
            Component::Prefix(_)
            | Component::RootDir
            | Component::CurDir
            | Component::ParentDir => Err(GraphError::from(PathNotParsable(path.clone()))),
        }
    }
}

pub(crate) struct ValidGraphFolder {
    // TODO: make this not public so that we guarantee calling try_from is the only way of creating this struct
    pub(crate) user_facing_path: PathBuf,
    pub(crate) base_path: PathBuf,
    pub(crate) graph_path: PathBuf, // TODO: maybe these two can always be inferred from base_path
    pub(crate) vectors_path: PathBuf,
}

impl ValidGraphFolder {
    fn try_from(base_path: PathBuf, relative_path: &str) -> Result<Self, InvalidPathReason> {
        let user_facing_path = PathBuf::from(relative_path);
        // check for errors in the path
        //additionally ban any backslash
        if relative_path.contains(r"\") {
            return Err(BackslashError(user_facing_path));
        }
        if relative_path.contains(r"//") {
            return Err(DoubleForwardSlash(user_facing_path));
        }

        let mut full_path = base_path;
        // fail if any component is a Prefix (C://), tries to access root,
        // tries to access a parent dir or is a symlink which could break out of the working dir
        for component in user_facing_path.components() {
            match component {
                Component::Prefix(_) => return Err(RootNotAllowed(user_facing_path)),
                Component::RootDir => return Err(RootNotAllowed(user_facing_path)),
                Component::CurDir => return Err(CurDirNotAllowed(user_facing_path)),
                Component::ParentDir => return Err(ParentDirNotAllowed(user_facing_path)),
                Component::Normal(component) => {
                    //check for symlinks
                    full_path.push(component);
                    if full_path.is_symlink() {
                        return Err(SymlinkNotAllowed(user_facing_path));
                    }
                }
            }
        }
        Ok(Self {
            user_facing_path,
            base_path: full_path.clone(),
            graph_path: full_path.join("graph"),
            vectors_path: full_path.join(VECTORS_FILE_NAME),
        })
    }

    pub(crate) fn get_vector_path_to_write(&self) -> VectorsPath {
        VectorsPath(self.vectors_path.clone())
    }
}

#[derive(Clone)]
pub struct GraphWithVectors {
    pub graph: IndexedGraph<MaterializedGraph>,
    pub vectors: Option<VectorisedGraph<MaterializedGraph>>,
}

// TODO: I don't think Im using these anywhere
// FIXME: need to also write to disk!!!!!!!!!
impl GraphWithVectors {
    pub(crate) async fn update_node_embeddings<T: AsNodeRef>(&self, node: T) {
        if let Some(vectors) = &self.vectors {
            vectors.update_node(node).await
        }
    }

    pub(crate) async fn update_edge_embeddings<T: AsNodeRef>(&self, src: T, dst: T) {
        if let Some(vectors) = &self.vectors {
            vectors.update_edge(src, dst).await
        }
    }
}

// TODO: review why this doesnt work
impl Base for GraphWithVectors {
    type Base = MaterializedGraph;

    #[inline]
    fn base(&self) -> &Self::Base {
        &self.graph.graph
    }
}

impl Static for GraphWithVectors {}

impl InheritViewOps for GraphWithVectors {}
impl InheritMutationOps for GraphWithVectors {}
// impl InheritDeletionOps for GraphWithVectors {}

// TODO: implement this traits for GraphWithVectors but using the new folder format ?!?!?!?
// or implement maybe CacheOps so I can also take care of writing to the vectors file???
// I don't think I can because that is async... not trivial at least
// impl StableEncode for GraphWithVectors {
//     fn encode_to_proto(&self) -> ProtoGraph {
//         self.graph.encode_to_proto()
//     }
// }

// impl StableDecode for GraphWithVectors {
//     fn decode_from_proto(graph: &ProtoGraph) -> Result<Self, GraphError> {
//         let inner = G::decode_from_proto(graph)?;
//         let indexed = Self::from_graph(&inner)?;
//         Ok(indexed)
//     }
// }

// impl InternalCache for GraphWithVectors {
//     fn init_cache(&self, path: impl AsRef<Path>) -> Result<(), GraphError> {
//         self.graph.init_cache(path)
//     }

//     fn get_cache(&self) -> Option<&GraphWriter> {
//         self.graph.get_cache()
//     }
// }

// impl CacheOps for GraphWithVectors {
//     fn cache(&self, path: impl AsRef<Path>) -> Result<(), GraphError> {
//         self.graph.cache(path)
//     }

//     fn write_updates(&self) -> Result<(), GraphError> {
//         todo!()
//     }

//     fn load_cached(path: impl AsRef<Path>) -> Result<Self, GraphError> {
//         todo!()
//     }
// }

pub(crate) trait UpdateEmbeddings {
    async fn update_embeddings(&self);
}

impl UpdateEmbeddings for NodeView<GraphWithVectors> {
    async fn update_embeddings(&self) {
        self.graph.update_node_embeddings(self.name()).await
    }
}

impl UpdateEmbeddings for EdgeView<GraphWithVectors> {
    async fn update_embeddings(&self) {
        self.graph
            .update_edge_embeddings(self.src().name(), self.dst().name())
            .await
    }
}

#[derive(Clone)]
pub struct EmbeddingConf {
    pub(crate) function: Arc<dyn EmbeddingFunction>,
    pub(crate) cache: Arc<Option<EmbeddingCache>>, // FIXME: no need for this to be Option
    pub(crate) global_template: Option<DocumentTemplate>,
    pub(crate) individual_templates: HashMap<PathBuf, DocumentTemplate>,
}

pub struct GraphEmbeddingConf {
    function: Arc<dyn EmbeddingFunction>,
    cache: Arc<Option<EmbeddingCache>>,
    template: DocumentTemplate,
}

impl EmbeddingConf {
    fn resolve_for_graph(&self, graph: &Path) -> Option<GraphEmbeddingConf> {
        let template = self
            .individual_templates
            .get(graph)
            .or(self.global_template.as_ref())?;
        Some(GraphEmbeddingConf {
            function: self.function.clone(),
            cache: self.cache.clone(),
            template: template.clone(),
        })
    }
}

pub struct Data {
    work_dir: PathBuf,
    cache: Cache<PathBuf, GraphWithVectors>,
    pub(crate) embedding_conf: Option<EmbeddingConf>,
}

impl Data {
    pub fn new(work_dir: &Path, configs: &AppConfig) -> Self {
        let cache_configs = &configs.cache;

        let cache = Cache::<PathBuf, GraphWithVectors>::builder()
            .max_capacity(cache_configs.capacity)
            .time_to_idle(std::time::Duration::from_secs(cache_configs.tti_seconds))
            .eviction_listener(|_, value, _| {
                value
                    .graph
                    .write_updates()
                    .unwrap_or_else(|err| println!("Write on eviction failed: {err:?}"))
                // FIXME: don't have currently a way to know which embedding updates are pending
            })
            .build();

        Self {
            work_dir: work_dir.to_path_buf(),
            cache,
            embedding_conf: Default::default(),
        }
    }

    // TODO: make this return GraphWithVectors and remove the other one
    pub fn get_graph(
        &self,
        path: &str,
    ) -> Result<IndexedGraph<MaterializedGraph>, Arc<GraphError>> {
        self.get_graph_with_vectors(path).map(|graph| graph.graph)
    }

    pub fn get_graph_with_vectors(&self, path: &str) -> Result<GraphWithVectors, Arc<GraphError>> {
        let graph_folder = ExistingGraphFolder::try_from(self.work_dir.clone(), path)?;
        self.cache
            .try_get_with(path.into(), || self.read_graph_from_path(&graph_folder))
    }

    pub async fn insert_graph(
        &self,
        path: &str,
        graph: MaterializedGraph,
    ) -> Result<(), GraphError> {
        // TODO: replace ValidGraphFolder with ValidNonExistingGraphFolder !!!!!!!!!
        // or even a NewGraphFolder, so that we try to create the graph file and if that is sucessful
        // we can write to it and its guaranteed to me atomic
        let folder = ValidGraphFolder::try_from(self.work_dir.clone(), path)?;
        match ExistingGraphFolder::try_from(self.work_dir.clone(), path) {
            Ok(_) => Err(GraphError::GraphNameAlreadyExists(folder.user_facing_path)),
            Err(_) => {
                fs::create_dir_all(&folder.base_path)?;
                graph.cache(&folder.graph_path)?;

                let vectors = self
                    .vectorise_and_store(graph.clone(), &folder.get_vector_path_to_write())
                    .await;

                let graph_with_vectors = GraphWithVectors {
                    graph: graph.into(),
                    vectors,
                };

                self.cache.insert(path.into(), graph_with_vectors);
                Ok(())
            }
        }
    }

    pub fn delete_graph(&self, path: &str) -> Result<(), GraphError> {
        let graph_folder = ExistingGraphFolder::try_from(self.work_dir.clone(), path)?;
        // TODO: double check that paths actuallt refers to a graph dir, not any arbitrary dir
        fs::remove_dir_all(graph_folder.base_path)?;
        self.cache.remove(&PathBuf::from(path));
        Ok(())
    }

    fn get_embedding_conf(&self, graph: &Path) -> Option<GraphEmbeddingConf> {
        self.embedding_conf
            .as_ref()
            .and_then(|conf| conf.resolve_for_graph(graph))
    }

    async fn vectorise_and_store(
        &self,
        graph: MaterializedGraph,
        path: &VectorsPath,
    ) -> Option<VectorisedGraph<MaterializedGraph>> {
        let conf = self.get_embedding_conf(path.path())?;
        let vectors = graph
            .vectorise_with_cache(
                Box::new(conf.function.clone()),
                conf.cache.clone(),
                true,
                conf.template.clone(),
                true, // verbose
            )
            .await;

        vectors.write_to_path(path.path());
        Some(vectors)
    }

    pub(crate) async fn vectorise_all_graphs_that_are_not(&self) {
        for folder in self.get_all_graph_folders() {
            if folder.vectors_path.is_none() {
                if let Ok(graph) = self.read_graph_from_path(&folder) {
                    self.vectorise_and_store(graph.graph.graph, &folder.get_vector_path_to_write())
                        .await;
                }
            }
        }
    }

    // TODO: return iter
    pub fn get_all_graph_folders(&self) -> Vec<ExistingGraphFolder> {
        let base_path = self.work_dir.clone();
        WalkDir::new(&self.work_dir)
            .into_iter()
            .filter_map(|e| {
                let entry = e.ok()?;
                let path = entry.path();
                let relative = self.get_relative_path(path).ok()?;
                let relative_str = relative.to_str()?; // optential UTF8 error here
                let folder = ExistingGraphFolder::try_from(base_path.clone(), relative_str).ok()?;
                Some(folder)
            })
            .collect()
    }

    fn get_relative_path(&self, path: &Path) -> Result<PathBuf, StripPrefixError> {
        Ok(path.strip_prefix(&self.work_dir)?.to_path_buf())
    }

    fn read_graph_from_path(
        &self,
        folder: &ExistingGraphFolder,
    ) -> Result<GraphWithVectors, GraphError> {
        let path = &folder.graph_path;
        let graph = if path.is_dir() {
            if is_disk_graph_dir(path) {
                get_disk_graph_from_path(path)?.ok_or(GraphError::DiskGraphNotFound)?
            } else {
                Err(PathIsDirectory(path.clone()))?
            }
        } else {
            let graph = MaterializedGraph::load_cached(path)?;
            IndexedGraph::from_graph(&graph)?
        };

        let vectors = self.read_vectors_from_path(graph.graph.clone(), &folder);

        println!("Graph loaded = {}", path.display());
        Ok(GraphWithVectors { graph, vectors })
    }

    fn read_vectors_from_path(
        &self,
        source: MaterializedGraph,
        folder: &ExistingGraphFolder,
    ) -> Option<VectorisedGraph<MaterializedGraph>> {
        let conf = self.get_embedding_conf(&folder.user_facing_path)?;
        let vectors_path = folder.vectors_path.as_ref()?;
        VectorisedGraph::read_from_path(&vectors_path, source, conf.function, conf.cache)
    }

    pub(crate) fn get_global_plugins(&self) -> GlobalPlugins {
        let graphs = self
            .get_all_graph_folders()
            .into_iter()
            .filter_map(|folder| {
                Some((
                    folder.user_facing_path.clone().to_str()?.to_owned(),
                    self.read_graph_from_path(&folder).ok()?.vectors?,
                ))
            })
            .collect::<HashMap<_, _>>();
        GlobalPlugins {
            graphs: graphs.into(),
        }
    }
}

#[cfg(feature = "storage")]
fn copy_dir_recursive(source_dir: &Path, target_dir: &Path) -> Result<(), GraphError> {
    if !target_dir.exists() {
        fs::create_dir_all(target_dir)?;
    }

    for entry in fs::read_dir(source_dir)? {
        let entry = entry?;
        let entry_path = entry.path();
        let target_path = target_dir.join(entry.file_name());

        if entry_path.is_dir() {
            copy_dir_recursive(&entry_path, &target_path)?;
        } else {
            fs::copy(&entry_path, &target_path)?;
        }
    }

    Ok(())
}

#[cfg(feature = "storage")]
fn load_disk_graph_from_path(
    path_on_server: &Path,
    target_path: &Path,
    overwrite: bool,
) -> Result<Option<PathBuf>, GraphError> {
    let _ = load_disk_graph(path_on_server)?;
    if target_path.exists() {
        if overwrite {
            fs::remove_dir_all(&target_path)?;
            copy_dir_recursive(path_on_server, &target_path)?;
            println!("Disk Graph loaded = {}", target_path.display());
        } else {
            return Err(GraphError::GraphNameAlreadyExists(target_path.to_path_buf()).into());
        }
    } else {
        copy_dir_recursive(path_on_server, &target_path)?;
        println!("Disk Graph loaded = {}", target_path.display());
    }
    Ok(Some(target_path.to_path_buf()))
}

#[cfg(feature = "storage")]
fn get_disk_graph_from_path(
    path: &Path,
) -> Result<Option<IndexedGraph<MaterializedGraph>>, GraphError> {
    let graph = load_disk_graph(path)?;
    println!("Disk Graph loaded = {}", path.display());
    Ok(Some(IndexedGraph::from_graph(&graph.into())?))
}

#[cfg(not(feature = "storage"))]
fn get_disk_graph_from_path(
    _path: &Path,
) -> Result<Option<IndexedGraph<MaterializedGraph>>, GraphError> {
    Ok(None)
}

// We are loading all the graphs in the work dir for vectorized APIs

fn is_disk_graph_dir(path: &Path) -> bool {
    // Check if the directory contains files specific to disk_graph graphs
    let files = fs::read_dir(path).unwrap();
    let mut has_disk_graph_files = false;
    for file in files {
        let file_name = file.unwrap().file_name().into_string().unwrap();
        if file_name.ends_with(".ipc") {
            has_disk_graph_files = true;
            break;
        }
    }
    has_disk_graph_files
}

#[cfg(feature = "storage")]
fn load_disk_graph(path: &Path) -> Result<MaterializedGraph, GraphError> {
    let disk_graph = DiskGraphStorage::load_from_dir(path)
        .map_err(|e| GraphError::LoadFailure(e.to_string()))?;
    let graph: MaterializedGraph = disk_graph.into_graph().into(); // TODO: We currently have no way to identify disk graphs as MaterializedGraphs
    Ok(graph)
}

#[allow(unused_variables)]
#[cfg(not(feature = "storage"))]
fn _load_disk_graph(_path: &Path) -> Result<MaterializedGraph, GraphError> {
    unimplemented!("Storage feature not enabled, cannot load from disk graph")
}

#[cfg(test)]
pub(crate) mod data_tests {
    use crate::{
        data::Data,
        server_config::{AppConfig, AppConfigBuilder},
    };
    use itertools::Itertools;
    use raphtory::{db::api::view::MaterializedGraph, prelude::*};
    use std::{
        collections::HashMap,
        fs,
        fs::File,
        io,
        path::{Path, PathBuf},
    };

    #[cfg(feature = "storage")]
    use crate::data::copy_dir_recursive;
    use raphtory::core::utils::errors::{GraphError, InvalidPathReason};
    #[cfg(feature = "storage")]
    use raphtory::{
        db::api::storage::graph::storage_ops::GraphStorage, db::api::view::internal::CoreGraphOps,
        disk_graph::DiskGraphStorage,
    };
    #[cfg(feature = "storage")]
    use std::{thread, time::Duration};

    use super::ValidGraphFolder;

    fn get_maybe_relative_path(work_dir: &Path, path: PathBuf) -> Option<String> {
        let relative_path = match path.strip_prefix(work_dir) {
            Ok(relative_path) => relative_path,
            Err(_) => return None, // Skip paths that cannot be stripped
        };

        let parent_path = relative_path.parent().unwrap_or(Path::new(""));
        if let Some(parent_str) = parent_path.to_str() {
            if parent_str.is_empty() {
                None
            } else {
                Some(parent_str.to_string())
            }
        } else {
            None
        }
    }

    // This function creates files that mimic disk graph for tests
    fn create_ipc_files_in_dir(dir_path: &Path) -> io::Result<()> {
        if !dir_path.exists() {
            fs::create_dir_all(dir_path)?;
        }

        let file_paths = ["file1.ipc", "file2.txt", "file3.ipc"];

        for &file_name in &file_paths {
            let file_path = dir_path.join(file_name);
            File::create(file_path)?;
        }

        Ok(())
    }

    // TODO: review, I think this fails if any of the keys has a slash (a nested path)
    pub(crate) fn save_graphs_to_work_dir(
        work_dir: &Path,
        graphs: &HashMap<String, MaterializedGraph>,
    ) -> Result<(), GraphError> {
        for (name, graph) in graphs.into_iter() {
            let data = Data::new(work_dir, &AppConfig::default());
            let folder = ValidGraphFolder::try_from(data.work_dir, name)?;

            #[cfg(feature = "storage")]
            if let GraphStorage::Disk(dg) = graph.core_graph() {
                let disk_graph_path = dg.graph_dir();
                #[cfg(feature = "storage")]
                copy_dir_recursive(disk_graph_path, &full_path)?;
            } else {
                graph.encode(&full_path)?;
            }

            #[cfg(not(feature = "storage"))]
            {
                graph.encode(&folder.graph_path)?;
            }
        }
        Ok(())
    }

    #[test]
    #[cfg(feature = "storage")]
    fn test_get_disk_graph_from_path() {
        let tmp_graph_dir = tempfile::tempdir().unwrap();

        let graph = Graph::new();
        graph
            .add_edge(0, 1, 2, [("name", "test_e1")], None)
            .unwrap();
        graph
            .add_edge(0, 1, 3, [("name", "test_e2")], None)
            .unwrap();
        let graph_path = tmp_graph_dir.path().join("test_dg");
        let _ = DiskGraphStorage::from_graph(&graph, &graph_path).unwrap();

        let res = read_graph_from_path(&graph_path).unwrap();
        assert_eq!(res.graph.into_events().unwrap().count_edges(), 2);

        // Dir path doesn't exists
        let res = read_graph_from_path(&tmp_graph_dir.path().join("test_dg1"));
        assert!(res.is_err());
        if let Err(err) = res {
            assert!(err.to_string().contains("Invalid path"));
        }

        // Dir path exists but is not a disk graph path
        let tmp_graph_dir = tempfile::tempdir().unwrap();
        let res = read_graph_from_path(&tmp_graph_dir.path());
        assert!(res.is_err());
        if let Err(err) = res {
            assert!(err.to_string().contains("Invalid path"));
        }
    }

    fn list_top_level_files_and_dirs(path: &Path) -> io::Result<Vec<String>> {
        let mut entries_vec = Vec::new();
        let entries = fs::read_dir(path)?;

        for entry in entries {
            let entry = entry?;
            let entry_path = entry.path();

            if let Some(file_name) = entry_path.file_name() {
                if let Some(file_str) = file_name.to_str() {
                    entries_vec.push(file_str.to_string());
                }
            }
        }

        Ok(entries_vec)
    }

    #[test]
    #[cfg(feature = "storage")]
    fn test_save_graphs_to_work_dir() {
        let tmp_graph_dir = tempfile::tempdir().unwrap();
        let tmp_work_dir = tempfile::tempdir().unwrap();

        let graph = Graph::new();
        graph.add_constant_properties([("name", "test_g")]).unwrap();
        graph
            .add_edge(0, 1, 2, [("name", "test_e1")], None)
            .unwrap();
        graph
            .add_edge(0, 1, 3, [("name", "test_e2")], None)
            .unwrap();

        let graph2 = DiskGraphStorage::from_graph(&graph, &tmp_graph_dir.path().join("test_dg"))
            .unwrap()
            .into_graph()
            .into();

        let graph: MaterializedGraph = graph.into();
        let graphs = HashMap::from([
            ("test_g".to_string(), graph),
            ("test_dg".to_string(), graph2),
        ]);

        save_graphs_to_work_dir(&tmp_work_dir.path(), &graphs).unwrap();

        let mut graphs = list_top_level_files_and_dirs(&tmp_work_dir.path()).unwrap();
        graphs.sort();
        assert_eq!(graphs, vec!["test_dg", "test_g"]);
    }

    #[test]
    #[cfg(feature = "storage")]
    fn test_eviction() {
        let tmp_work_dir = tempfile::tempdir().unwrap();

        let graph = Graph::new();
        graph
            .add_edge(0, 1, 2, [("name", "test_e1")], None)
            .unwrap();
        graph
            .add_edge(0, 1, 3, [("name", "test_e2")], None)
            .unwrap();

        graph.encode(&tmp_work_dir.path().join("test_g")).unwrap();
        let _ = DiskGraphStorage::from_graph(&graph, &tmp_work_dir.path().join("test_dg")).unwrap();
        graph.encode(&tmp_work_dir.path().join("test_g2")).unwrap();

        let configs = AppConfigBuilder::new()
            .with_cache_capacity(1)
            .with_cache_tti_seconds(2)
            .build();

        let data = Data::new(tmp_work_dir.path(), &configs);

        assert!(!data.graphs.contains_key(&PathBuf::from("test_dg")));
        assert!(!data.graphs.contains_key(&PathBuf::from("test_g")));

        // Test size based eviction
        let _ = data.get_graph(Path::new("test_dg"));
        assert!(data.graphs.contains_key(&PathBuf::from("test_dg")));
        assert!(!data.graphs.contains_key(&PathBuf::from("test_g")));

        let _ = data.get_graph(Path::new("test_g"));
        assert!(data.graphs.contains_key(&PathBuf::from("test_g")));

        thread::sleep(Duration::from_secs(3));
        assert!(!data.graphs.contains_key(&PathBuf::from("test_dg")));
        assert!(!data.graphs.contains_key(&PathBuf::from("test_g")));
    }

    #[test]
    fn test_get_graph_paths() {
        let temp_dir = tempfile::tempdir().unwrap();
        let work_dir = temp_dir.path();
        let g0_path = work_dir.join("g0");
        let g1_path = work_dir.join("g1");
        let g2_path = work_dir
            .join("shivam")
            .join("investigations")
            .join("2024-12-22")
            .join("g2");
        let g3_path = work_dir.join("shivam").join("investigations").join("g3"); // Graph
        let g4_path = work_dir.join("shivam").join("investigations").join("g4"); // Disk graph dir
        let g5_path = work_dir.join("shivam").join("investigations").join("g5"); // Empty dir

        fs::create_dir_all(
            &work_dir
                .join("shivam")
                .join("investigations")
                .join("2024-12-22"),
        )
        .unwrap();
        fs::create_dir_all(&g4_path).unwrap();
        create_ipc_files_in_dir(&g4_path).unwrap();
        fs::create_dir_all(&g5_path).unwrap();

        File::create(&g0_path).unwrap();
        File::create(&g1_path).unwrap();
        File::create(&g2_path).unwrap();
        File::create(&g3_path).unwrap();

        let configs = AppConfigBuilder::new()
            .with_cache_capacity(1)
            .with_cache_tti_seconds(2)
            .build();

        let data = Data::new(work_dir, &configs);

        let paths = data
            .get_all_graph_folders()
            .into_iter()
            .map(|folder| folder.base_path)
            .collect_vec();

        assert_eq!(paths.len(), 5);
        assert!(paths.contains(&g0_path));
        assert!(paths.contains(&g1_path));
        assert!(paths.contains(&g2_path));
        assert!(paths.contains(&g3_path));
        assert!(paths.contains(&g4_path));
        assert!(!paths.contains(&g5_path)); // Empty dir are ignored

        assert_eq!(get_maybe_relative_path(work_dir, g0_path), None);
        assert_eq!(get_maybe_relative_path(work_dir, g1_path), None);
        let expected = Path::new("shivam")
            .join("investigations")
            .join("2024-12-22");
        assert_eq!(
            get_maybe_relative_path(work_dir, g2_path),
            Some(expected.display().to_string())
        );
        let expected = Path::new("shivam").join("investigations");
        assert_eq!(
            get_maybe_relative_path(work_dir, g3_path),
            Some(expected.display().to_string())
        );
        assert_eq!(
            get_maybe_relative_path(work_dir, g4_path),
            Some(expected.display().to_string())
        );
    }
}
