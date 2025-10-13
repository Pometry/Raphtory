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
            .eviction_listener(|_, graph, cause| {
                // The eviction listener gets called any time a graph is removed from the cache,
                // not just when it is evicted. Only serialize on evictions.
                if !cause.was_evicted() {
                    return;
                }

                // On eviction, serialize graphs that don't have underlying storage.
                // FIXME: don't have currently a way to know which embedding updates are pending
                if !graph.graph.disk_storage_enabled() && graph.is_dirty() {
                    if let Err(e) = Self::encode_graph_to_disk(graph.clone()) {
                        warn!("Error encoding graph to disk on eviction: {e}");
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

    pub fn validate_path_for_insert(
        &self,
        path: &str,
        overwrite: bool,
    ) -> Result<ValidGraphFolder, GraphError> {
        let folder = ValidGraphFolder::try_from(self.work_dir.clone(), path)?;

        match ExistingGraphFolder::try_from(self.work_dir.clone(), path) {
            Ok(_) => {
                if overwrite {
                    Ok(folder)
                } else {
                    Err(GraphError::GraphNameAlreadyExists(folder.to_error_path()))
                }
            }
            Err(_) => Ok(folder),
        }
    }

    pub async fn insert_graph(
        &self,
        folder: ValidGraphFolder,
        graph: MaterializedGraph,
    ) -> Result<(), GraphError> {
        let vectors = self.vectorise(graph.clone(), &folder).await;
        let graph = GraphWithVectors::new(graph, vectors, folder.clone().into());

        let graph_clone = graph.clone();
        let folder_clone = folder.clone();

        blocking_io(move || {
            // Graphs with underlying storage already write data to disk.
            // They just need to write metadata, primarily to infer the graph type.
            // Graphs without storage are encoded to the folder.
            if graph_clone.disk_storage_enabled() {
                folder_clone.write_metadata(&graph_clone)?;
            } else {
                Self::encode_graph_to_disk(graph_clone)?;
            }

            Ok::<(), GraphError>(())
        })
        .await?;

        let path = folder.get_original_path_str();
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

        let existing_folder = ExistingGraphFolder::try_from(self.work_dir.clone(), path)?;
        self.vectorise_folder(&existing_folder).await;

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

    /// Serializes a graph to disk, overwriting any existing data in its folder.
    fn encode_graph_to_disk(graph: GraphWithVectors) -> Result<(), GraphError> {
        let folder_path = graph.folder.get_base_path();

        // Create a backup of the existing folder
        if folder_path.exists() {
            let bak_path = folder_path.with_extension("bak");

            // Remove any old backups
            if bak_path.exists() {
                std::fs::remove_dir_all(&bak_path)?;
            }

            std::fs::rename(&folder_path, &bak_path)?;
        }

        // Serialize the graph to the original folder path
        graph.graph.encode(&folder_path)?;

        // Delete the backup on success
        let bak_path = folder_path.with_extension("bak");

        if bak_path.exists() {
            std::fs::remove_dir_all(&bak_path)?;
        }

        Ok(())
    }
}

impl Drop for Data {
    fn drop(&mut self) {
        // On drop, serialize graphs that don't have underlying storage.
        for (_, graph) in self.cache.iter() {
            if !graph.graph.disk_storage_enabled() && graph.is_dirty() {
                if let Err(e) = Self::encode_graph_to_disk(graph.clone()) {
                    warn!("Error encoding graph to disk on drop: {e}");
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
    use raphtory::{db::api::view::MaterializedGraph, errors::GraphError, prelude::*};
    use std::{collections::HashMap, fs, path::Path, time::Duration};
    use tokio::time::sleep;

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

    #[tokio::test]
    async fn test_drop_skips_write_when_graph_is_not_dirty() {
        let tmp_work_dir = tempfile::tempdir().unwrap();

        // Create two graphs and save them to disk
        let graph1 = Graph::new();
        graph1
            .add_edge(0, 1, 2, [("name", "test_e1")], None)
            .unwrap();
        graph1
            .add_edge(0, 1, 3, [("name", "test_e2")], None)
            .unwrap();

        let graph2 = Graph::new();
        graph2
            .add_edge(0, 2, 3, [("name", "test_e3")], None)
            .unwrap();
        graph2
            .add_edge(0, 2, 4, [("name", "test_e4")], None)
            .unwrap();

        let graph1_path = tmp_work_dir.path().join("test_graph1");
        let graph2_path = tmp_work_dir.path().join("test_graph2");
        graph1.encode(&graph1_path).unwrap();
        graph2.encode(&graph2_path).unwrap();

        // Record modification times before any operations
        let graph1_metadata = fs::metadata(&graph1_path).unwrap();
        let graph2_metadata = fs::metadata(&graph2_path).unwrap();
        let graph1_original_time = graph1_metadata.modified().unwrap();
        let graph2_original_time = graph2_metadata.modified().unwrap();

        let configs = AppConfigBuilder::new()
            .with_cache_capacity(10)
            .with_cache_tti_seconds(300)
            .build();

        let data = Data::new(tmp_work_dir.path(), &configs);

        let (loaded_graph1, _) = data.get_graph("test_graph1").await.unwrap();
        let (loaded_graph2, _) = data.get_graph("test_graph2").await.unwrap();

        assert!(!loaded_graph1.is_dirty(), "Graph1 should not be dirty when loaded from disk");
        assert!(!loaded_graph2.is_dirty(), "Graph2 should not be dirty when loaded from disk");

        // Modify only graph1 to make it dirty
        loaded_graph1.set_dirty(true);
        assert!(loaded_graph1.is_dirty(), "Graph1 should be dirty after modification");

        // Drop the Data instance - this should trigger serialization
        drop(data);

        // Check modification times after drop
        let graph1_metadata_after = fs::metadata(&graph1_path).unwrap();
        let graph2_metadata_after = fs::metadata(&graph2_path).unwrap();
        let graph1_modified_time = graph1_metadata_after.modified().unwrap();
        let graph2_modified_time = graph2_metadata_after.modified().unwrap();

        // Graph1 (dirty) modification time should be different
        assert_ne!(
            graph1_original_time,
            graph1_modified_time,
            "Graph1 (dirty) should have been written to disk on drop"
        );

        // Graph2 (not dirty) modification time should be the same
        assert_eq!(
            graph2_original_time,
            graph2_modified_time,
            "Graph2 (not dirty) should not have been written to disk on drop"
        );
    }

    #[tokio::test]
    async fn test_eviction_skips_write_when_graph_is_not_dirty() {
        let tmp_work_dir = tempfile::tempdir().unwrap();

        // Create two graphs and save them to disk
        let graph1 = Graph::new();
        graph1
            .add_edge(0, 1, 2, [("name", "test_e1")], None)
            .unwrap();
        graph1
            .add_edge(0, 1, 3, [("name", "test_e2")], None)
            .unwrap();

        let graph2 = Graph::new();
        graph2
            .add_edge(0, 2, 3, [("name", "test_e3")], None)
            .unwrap();
        graph2
            .add_edge(0, 2, 4, [("name", "test_e4")], None)
            .unwrap();

        let graph1_path = tmp_work_dir.path().join("test_graph1");
        let graph2_path = tmp_work_dir.path().join("test_graph2");
        graph1.encode(&graph1_path).unwrap();
        graph2.encode(&graph2_path).unwrap();

        // Record modification times before any operations
        let graph1_metadata = fs::metadata(&graph1_path).unwrap();
        let graph2_metadata = fs::metadata(&graph2_path).unwrap();
        let graph1_original_time = graph1_metadata.modified().unwrap();
        let graph2_original_time = graph2_metadata.modified().unwrap();

        // Create cache with time to idle 3 seconds to force eviction
        let configs = AppConfigBuilder::new()
            .with_cache_capacity(10)
            .with_cache_tti_seconds(3)
            .build();

        let data = Data::new(tmp_work_dir.path(), &configs);

        // Load first graph
        let (loaded_graph1, _) = data.get_graph("test_graph1").await.unwrap();
        assert!(!loaded_graph1.is_dirty(), "Graph1 should not be dirty when loaded from disk");

        // Modify graph1 to make it dirty
        loaded_graph1.set_dirty(true);
        assert!(loaded_graph1.is_dirty(), "Graph1 should be dirty after modification");

        // Load second graph
        println!("Loading second graph");
        let (loaded_graph2, _) = data.get_graph("test_graph2").await.unwrap();
        assert!(!loaded_graph2.is_dirty(), "Graph2 should not be dirty when loaded from disk");

        // Sleep to trigger eviction
        sleep(Duration::from_secs(3)).await;
        data.cache.run_pending_tasks().await;

        // Check modification times after eviction
        let graph1_metadata_after = fs::metadata(&graph1_path).unwrap();
        let graph2_metadata_after = fs::metadata(&graph2_path).unwrap();
        let graph1_modified_time = graph1_metadata_after.modified().unwrap();
        let graph2_modified_time = graph2_metadata_after.modified().unwrap();

        // Graph1 (dirty) modification time should be different
        assert_ne!(
            graph1_original_time,
            graph1_modified_time,
            "Graph1 (dirty) should have been written to disk on eviction"
        );

        // Graph2 (not dirty) modification time should be the same
        assert_eq!(
            graph2_original_time,
            graph2_modified_time,
            "Graph2 (not dirty) should not have been written to disk on eviction"
        );
    }
}
