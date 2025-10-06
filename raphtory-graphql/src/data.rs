use crate::{
    config::app_config::AppConfig,
    graph::GraphWithVectors,
    model::blocking_io,
    paths::{valid_path, ExistingGraphFolder, ValidGraphFolder},
};
use itertools::Itertools;
use moka::future::Cache;
use raphtory::{
    db::api::view::{internal::InternalStorageOps, MaterializedGraph},
    errors::{GraphError, InvalidPathReason},
    prelude::StableEncode,
    vectors::{
        cache::VectorCache, template::DocumentTemplate, vectorisable::Vectorisable,
        vectorised_graph::VectorisedGraph,
    },
};
use std::{
    collections::HashMap,
    io::{Read, Seek},
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::fs;
use tracing::warn;
use walkdir::WalkDir;

#[derive(Clone)]
pub struct EmbeddingConf {
    pub(crate) cache: VectorCache,
    pub(crate) global_template: Option<DocumentTemplate>,
    pub(crate) individual_templates: HashMap<PathBuf, DocumentTemplate>,
}

pub(crate) fn get_relative_path(
    work_dir: PathBuf,
    path: &Path,
    namespace: bool,
) -> Result<String, InvalidPathReason> {
    let path_buf = path.strip_prefix(work_dir.clone())?.to_path_buf();
    let components = path_buf
        .components()
        .into_iter()
        .map(|c| {
            c.as_os_str()
                .to_str()
                .ok_or(InvalidPathReason::NonUTFCharacters)
        })
        .collect::<Result<Vec<_>, _>>()?;
    //a safe unwrap as checking above
    let path_str = components.into_iter().join("/");
    valid_path(work_dir, &path_str, namespace)?;
    Ok(path_str)
}

#[derive(Clone)]
pub struct Data {
    pub(crate) work_dir: PathBuf,
    cache: Cache<PathBuf, GraphWithVectors>,
    pub(crate) create_index: bool,
    pub(crate) embedding_conf: Option<EmbeddingConf>,
}

impl Data {
    pub fn new(work_dir: &Path, configs: &AppConfig) -> Self {
        let cache_configs = &configs.cache;

        let cache = Cache::<PathBuf, GraphWithVectors>::builder()
            .max_capacity(cache_configs.capacity)
            .time_to_idle(std::time::Duration::from_secs(cache_configs.tti_seconds))
            .eviction_listener(|_, graph, _| {
                // On eviction, serialize graphs that don't have underlying persistence.
                // FIXME: don't have currently a way to know which embedding updates are pending
                if !graph.graph.disk_storage_enabled() {
                    if let Some(folder) = graph.folder.get() {
                        let _ = folder
                            .clear()
                            .map_err(|e| warn!("Error clearing graph folder on eviction: {e}"));

                        let _ = graph
                            .graph
                            .encode(folder.clone())
                            .map_err(|e| warn!("Error serializing graph on eviction: {e}"));
                    }
                }
            })
            .build();

        #[cfg(feature = "search")]
        let create_index = configs.index.create_index;
        #[cfg(not(feature = "search"))]
        let create_index = false;

        Self {
            work_dir: work_dir.to_path_buf(),
            cache,
            create_index,
            embedding_conf: Default::default(),
        }
    }

    pub async fn get_graph(
        &self,
        path: &str,
    ) -> Result<(GraphWithVectors, ExistingGraphFolder), Arc<GraphError>> {
        let graph_folder = ExistingGraphFolder::try_from(self.work_dir.clone(), path)?;
        let graph_folder_clone = graph_folder.clone();
        self.cache
            .try_get_with(path.into(), self.read_graph_from_folder(graph_folder_clone))
            .await
            .map(|graph| (graph, graph_folder))
    }

    pub async fn has_graph(&self, path: &str) -> bool {
        ExistingGraphFolder::try_from(self.work_dir.clone(), path).is_ok()
    }

    pub fn validate_path_for_insert(&self, path: &str) -> Result<ValidGraphFolder, GraphError> {
        let folder = ValidGraphFolder::try_from(self.work_dir.clone(), path)?;

        match ExistingGraphFolder::try_from(self.work_dir.clone(), path) {
            Ok(_) => Err(GraphError::GraphNameAlreadyExists(folder.to_error_path())),
            Err(_) => Ok(folder),
        }
    }

    pub async fn insert_graph(
        &self,
        folder: ValidGraphFolder,
        graph: MaterializedGraph,
    ) -> Result<(), GraphError> {
        let path = folder.get_original_path_str();
        let graph_clone = graph.clone();
        let folder_clone = folder.clone();

        blocking_io(move || {
            // Graphs with underlying storage persistence already write data to disk.
            // They just need to write metadata, primarily to infer the graph type.
            // Graphs without persistence are encoded to the folder.
            if graph_clone.disk_storage_enabled() {
                folder_clone.write_metadata(&graph_clone)?;
            } else {
                graph_clone.encode(folder_clone.clone())?;
            }

            Ok::<(), GraphError>(())
        }).await?;

        let vectors = self.vectorise(graph.clone(), &folder).await;
        let graph = GraphWithVectors::new(graph, vectors);

        let folder_for_init = folder.clone();

        graph
            .folder
            .get_or_try_init(|| Ok::<_, GraphError>(folder_for_init.into()))?;

        self.cache.insert(path.into(), graph).await;

        Ok(())
    }

    /// Insert a graph serialized from a graph folder.
    pub async fn insert_graph_as_bytes<R: Read + Seek>(
        &self,
        folder: ValidGraphFolder,
        bytes: R,
    ) -> Result<(), GraphError> {
        let path = folder.get_original_path_str();
        folder.unzip_to_folder(bytes)?;

        // Can't use '?' directly as get_graph returns Arc<GraphError>
        self.get_graph(&path)
            .await
            .map_err(|e| GraphError::IOErrorMsg(e.to_string()))?;

        Ok(())
    }

    pub async fn delete_graph(&self, path: &str) -> Result<(), GraphError> {
        let graph_folder = ExistingGraphFolder::try_from(self.work_dir.clone(), path)?;
        fs::remove_dir_all(graph_folder.get_base_path()).await?;
        self.cache.remove(&PathBuf::from(path)).await;
        Ok(())
    }

    fn resolve_template(&self, graph: &Path) -> Option<&DocumentTemplate> {
        let conf = self.embedding_conf.as_ref()?;
        conf.individual_templates
            .get(graph)
            .or(conf.global_template.as_ref())
    }

    async fn vectorise_with_template(
        &self,
        graph: MaterializedGraph,
        folder: &ValidGraphFolder,
        template: &DocumentTemplate,
    ) -> Option<VectorisedGraph<MaterializedGraph>> {
        let conf = self.embedding_conf.as_ref()?;
        let vectors = graph
            .vectorise(
                conf.cache.clone(),
                template.clone(),
                Some(&folder.get_vectors_path()),
                true, // verbose
            )
            .await;
        match vectors {
            Ok(vectors) => Some(vectors),
            Err(error) => {
                let name = folder.get_original_path_str();
                warn!("An error occurred when trying to vectorise graph {name}: {error}");
                None
            }
        }
    }

    async fn vectorise(
        &self,
        graph: MaterializedGraph,
        folder: &ValidGraphFolder,
    ) -> Option<VectorisedGraph<MaterializedGraph>> {
        let template = self.resolve_template(folder.get_original_path())?;
        self.vectorise_with_template(graph, folder, template).await
    }

    async fn vectorise_folder(&self, folder: &ExistingGraphFolder) -> Option<()> {
        // it's important that we check if there is a valid template set for this graph path
        // before actually loading the graph, otherwise we are loading the graph for no reason
        let template = self.resolve_template(folder.get_original_path())?;
        let graph = self
            .read_graph_from_folder(folder.clone())
            .await
            .ok()?
            .graph;
        self.vectorise_with_template(graph, folder, template).await;
        Some(())
    }

    pub(crate) async fn vectorise_all_graphs_that_are_not(&self) -> Result<(), GraphError> {
        for folder in self.get_all_graph_folders() {
            if !folder.get_vectors_path().exists() {
                self.vectorise_folder(&folder).await;
            }
        }
        Ok(())
    }

    // TODO: return iter
    pub fn get_all_graph_folders(&self) -> Vec<ExistingGraphFolder> {
        let base_path = self.work_dir.clone();
        WalkDir::new(&self.work_dir)
            .into_iter()
            .filter_map(|e| {
                let entry = e.ok()?;
                let path = entry.path();
                let relative = get_relative_path(base_path.clone(), path, false).ok()?;
                let folder = ExistingGraphFolder::try_from(base_path.clone(), &relative).ok()?;
                Some(folder)
            })
            .collect()
    }

    async fn read_graph_from_folder(
        &self,
        folder: ExistingGraphFolder,
    ) -> Result<GraphWithVectors, GraphError> {
        let cache = self.embedding_conf.as_ref().map(|conf| conf.cache.clone());
        let create_index = self.create_index;
        blocking_io(move || GraphWithVectors::read_from_folder(&folder, cache, create_index)).await
    }
}

impl Drop for Data {
    fn drop(&mut self) {
        // On drop, serialize graphs that don't have underlying persistence.
        for (_, graph) in self.cache.iter() {
            if !graph.graph.disk_storage_enabled() {
                if let Some(folder) = graph.folder.get() {
                    let _ = folder
                        .clear()
                        .map_err(|e| warn!("Error clearing graph folder on drop: {e}"));

                    let _ = graph
                        .graph
                        .encode(folder.clone())
                        .map_err(|e| warn!("Error serializing graph on drop: {e}"));
                }
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod data_tests {
    use super::ValidGraphFolder;
    use crate::{
        config::app_config::{AppConfig, AppConfigBuilder},
        data::Data,
    };
    use itertools::Itertools;
    use raphtory::{
        db::api::view::MaterializedGraph,
        errors::GraphError,
        prelude::*,
        serialise::{GraphFolder, GRAPH_PATH},
    };
    use std::{collections::HashMap, fs, fs::File, io, path::Path, time::Duration};
    use tokio::time::sleep;

    // TODO: Change this to work for new diskgraph
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

    fn create_graph_folder(path: &Path) {
        // Use empty graph to create folder structure
        let graph = Graph::new();
        graph.encode(path).unwrap();
    }

    pub(crate) fn save_graphs_to_work_dir(
        work_dir: &Path,
        graphs: &HashMap<String, MaterializedGraph>,
    ) -> Result<(), GraphError> {
        for (name, graph) in graphs.into_iter() {
            let data = Data::new(work_dir, &AppConfig::default());
            let folder = ValidGraphFolder::try_from(data.work_dir.clone(), name)?;
            graph.encode(folder)?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_save_graphs_to_work_dir() {
        let tmp_work_dir = tempfile::tempdir().unwrap();

        let graph = Graph::new();
        graph.add_metadata([("name", "test_g")]).unwrap();
        graph
            .add_edge(0, 1, 2, [("name", "test_e1")], None)
            .unwrap();
        graph
            .add_edge(0, 1, 3, [("name", "test_e2")], None)
            .unwrap();

        let graph: MaterializedGraph = graph.into();

        let mut graphs = HashMap::new();

        graphs.insert("test_g".to_string(), graph);

        save_graphs_to_work_dir(tmp_work_dir.path(), &graphs).unwrap();

        let data = Data::new(tmp_work_dir.path(), &Default::default());

        for graph in graphs.keys() {
            assert!(data.get_graph(graph).await.is_ok(), "could not get {graph}")
        }
    }

    #[tokio::test]
    async fn test_eviction() {
        let tmp_work_dir = tempfile::tempdir().unwrap();

        let graph = Graph::new();
        graph
            .add_edge(0, 1, 2, [("name", "test_e1")], None)
            .unwrap();
        graph
            .add_edge(0, 1, 3, [("name", "test_e2")], None)
            .unwrap();

        graph.encode(&tmp_work_dir.path().join("test_g")).unwrap();
        graph.encode(&tmp_work_dir.path().join("test_g2")).unwrap();

        let configs = AppConfigBuilder::new()
            .with_cache_capacity(1)
            .with_cache_tti_seconds(2)
            .build();

        let data = Data::new(tmp_work_dir.path(), &configs);

        assert!(!data.cache.contains_key(Path::new("test_g")));
        assert!(!data.cache.contains_key(Path::new("test_g2")));

        // Test size based eviction
        data.get_graph("test_g2").await.unwrap();
        assert!(data.cache.contains_key(Path::new("test_g2")));
        assert!(!data.cache.contains_key(Path::new("test_g")));

        data.get_graph("test_g").await.unwrap(); // wait for any eviction
        data.cache.run_pending_tasks().await;
        assert_eq!(data.cache.iter().count(), 1);

        sleep(Duration::from_secs(3)).await;
        assert!(!data.cache.contains_key(Path::new("test_g")));
        assert!(!data.cache.contains_key(Path::new("test_g2")));
    }

    #[tokio::test]
    async fn test_get_graph_paths() {
        let temp_dir = tempfile::tempdir().unwrap();
        let work_dir = temp_dir.path();

        let g0_path = work_dir.join("g0");
        let g1_path = work_dir.join("g1");
        let g2_path = work_dir.join("shivam/investigations/2024-12-22/g2");
        let g3_path = work_dir.join("shivam/investigations/g3"); // Graph
        let g4_path = work_dir.join("shivam/investigations/g4"); // Disk graph dir
        let g5_path = work_dir.join("shivam/investigations/g5"); // Empty dir
        let g6_path = work_dir.join("shivam/investigations/g6"); // File that is not a graph

        create_graph_folder(&g0_path);
        create_graph_folder(&g1_path);
        create_graph_folder(&g2_path);
        create_graph_folder(&g3_path);
        create_graph_folder(&g4_path);

        // Simulate disk graph
        create_ipc_files_in_dir(&g4_path.join(GRAPH_PATH)).unwrap();

        // Empty, non-graph folder
        fs::create_dir_all(&g5_path).unwrap();

        // Simulate non-graph folder with random files
        fs::create_dir_all(&g6_path).unwrap();
        fs::write(g6_path.join("random-file"), "some-random-content").unwrap();

        let configs = AppConfigBuilder::new()
            .with_cache_capacity(1)
            .with_cache_tti_seconds(2)
            .build();

        let data = Data::new(work_dir, &configs);

        let paths = data
            .get_all_graph_folders()
            .into_iter()
            .map(|folder| folder.get_base_path().to_path_buf())
            .collect_vec();

        assert_eq!(paths.len(), 5);
        assert!(paths.contains(&g0_path));
        assert!(paths.contains(&g1_path));
        assert!(paths.contains(&g2_path));
        assert!(paths.contains(&g3_path));
        assert!(paths.contains(&g4_path));
        assert!(!paths.contains(&g5_path)); // Empty folder is ignored
        assert!(!paths.contains(&g6_path)); // Non-graph folder is ignored

        assert!(data
            .get_graph("shivam/investigations/2024-12-22/g2")
            .await
            .is_ok());

        assert!(data.get_graph("some/random/path").await.is_err());
    }
}
