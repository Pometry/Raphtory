use crate::{
    config::app_config::AppConfig,
    graph::GraphWithVectors,
    model::blocking_io,
    paths::{
        mark_dirty, valid_path, valid_relative_graph_path, ExistingGraphFolder,
        InternalPathValidationError, PathValidationError, ValidGraphFolder, WithPath,
        WriteableGraphFolder,
    },
    rayon::blocking_compute,
    GQLError,
    GQLError::Insertion,
};
use futures_util::FutureExt;
use itertools::{fold, Itertools};
use moka::future::Cache;
use raphtory::{
    db::api::view::{internal::InternalStorageOps, MaterializedGraph},
    errors::{GraphError, InvalidPathReason},
    prelude::StableEncode,
    serialise::{GraphFolder, META_PATH},
    vectors::{
        cache::VectorCache, template::DocumentTemplate, vectorisable::Vectorisable,
        vectorised_graph::VectorisedGraph,
    },
};
use std::{
    collections::HashMap,
    fs,
    fs::File,
    io,
    io::{ErrorKind, Read, Seek},
    path::{Path, PathBuf},
    sync::Arc,
};
use tempfile::{spooled_tempfile_in, tempfile_in, NamedTempFile};
use tracing::{error, warn};
use walkdir::WalkDir;

pub const DIRTY_PATH: &'static str = ".dirty";

#[derive(Clone)]
pub struct EmbeddingConf {
    pub(crate) cache: VectorCache,
    pub(crate) global_template: Option<DocumentTemplate>,
    pub(crate) individual_templates: HashMap<PathBuf, DocumentTemplate>,
}

#[derive(thiserror::Error, Debug)]
enum MutationErrorInner {
    #[error(transparent)]
    GraphError(#[from] GraphError),
    #[error(transparent)]
    IO(#[from] io::Error),
    #[error(transparent)]
    InvalidInternal(#[from] InternalPathValidationError),
}

#[derive(thiserror::Error, Debug)]
pub enum InsertionError {
    #[error("Failed to insert graph {graph}: {error}")]
    Insertion {
        graph: String,
        error: MutationErrorInner,
    },
    #[error(transparent)]
    PathValidation(#[from] PathValidationError),
    #[error("Failed to insert graph {graph}: {error}")]
    GraphError { graph: String, error: GraphError },
}

impl InsertionError {
    pub fn from_inner(graph: &str, error: MutationErrorInner) -> Self {
        InsertionError::Insertion {
            graph: graph.to_string(),
            error,
        }
    }

    pub fn from_graph_err(graph: &str, error: GraphError) -> Self {
        InsertionError::GraphError {
            graph: graph.to_string(),
            error,
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum DeletionError {
    #[error("Failed to delete graph {graph}: {error}")]
    Insertion {
        graph: String,
        error: MutationErrorInner,
    },
    #[error(transparent)]
    PathValidation(#[from] PathValidationError),
}

#[derive(thiserror::Error, Debug)]
pub enum MoveError {
    #[error("Failed to move graph: {0}")]
    Insertion(#[from] InsertionError),
    #[error("Failed to move graph: {0}")]
    Deletion(#[from] DeletionError),
}

impl DeletionError {
    fn from_inner(graph: &str, error: MutationErrorInner) -> Self {
        DeletionError::Insertion {
            graph: graph.to_string(),
            error,
        }
    }
}

/// Get relative path as String joined with `"/"` for use with the validation methods.
/// The path is not validated here!
pub(crate) fn get_relative_path(
    work_dir: &Path,
    path: &Path,
) -> Result<String, InternalPathValidationError> {
    let relative = path.strip_prefix(work_dir)?;
    let mut path_str = String::new();
    let mut components = relative.components().map(|component| {
        component
            .as_os_str()
            .to_str()
            .ok_or(InternalPathValidationError::NonUTFCharacters)
    });
    if let Some(first) = components.next() {
        path_str.push_str(first?);
    }
    for component in components {
        path_str.push('/');
        path_str.push_str(component?);
    }
    Ok(path_str)
}

pub struct Data {
    pub(crate) work_dir: PathBuf,
    cache: Cache<String, GraphWithVectors>,
    pub(crate) create_index: bool,
    pub(crate) embedding_conf: Option<EmbeddingConf>,
}

impl Data {
    pub fn new(work_dir: &Path, configs: &AppConfig) -> Self {
        let cache_configs = &configs.cache;

        let cache = Cache::<String, GraphWithVectors>::builder()
            .max_capacity(cache_configs.capacity)
            .time_to_idle(std::time::Duration::from_secs(cache_configs.tti_seconds))
            .async_eviction_listener(|_, graph, cause| {
                // The eviction listener gets called any time a graph is removed from the cache,
                // not just when it is evicted. Only serialize on evictions.
                async move {
                    if !cause.was_evicted() {
                        return;
                    }
                    if let Err(e) =
                        blocking_compute(move || graph.folder.write_graph_data(graph.graph)).await
                    {
                        error!("Error encoding graph to disk on eviction: {e}");
                    }
                }
                .boxed()
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

    pub fn validate_path_for_insert(
        &self,
        path: &str,
        overwrite: bool,
    ) -> Result<WriteableGraphFolder, PathValidationError> {
        if overwrite {
            WriteableGraphFolder::try_existing_or_new(self.work_dir.clone(), path)
        } else {
            WriteableGraphFolder::try_new(self.work_dir.clone(), path)
        }
    }

    pub async fn get_graph(&self, path: &str) -> Result<GraphWithVectors, Arc<GQLError>> {
        self.cache
            .try_get_with(path.into(), self.read_graph_from_disk(path))
            .await
    }

    pub fn has_graph(&self, path: &str) -> bool {
        self.cache.contains_key(path)
            || ExistingGraphFolder::try_from(self.work_dir.clone(), path).is_ok()
    }

    pub async fn insert_graph(
        &self,
        writeable_folder: WriteableGraphFolder,
        graph: MaterializedGraph,
    ) -> Result<(), InsertionError> {
        let vectors = self.vectorise(graph.clone(), &writeable_folder).await;
        let graph = blocking_compute(move || {
            writeable_folder.write_graph_data(graph.clone())?;
            let folder = writeable_folder.finish()?;
            let graph = GraphWithVectors::new(graph, vectors, folder.as_existing()?);
            Ok::<_, InsertionError>(graph)
        })
        .await?;

        self.cache.insert(graph.folder.local_path(), graph).await;
        Ok(())
    }

    /// Insert a graph serialized from a graph folder.
    pub async fn insert_graph_as_bytes<R: Read + Seek + Send + 'static>(
        &self,
        folder: WriteableGraphFolder,
        bytes: R,
    ) -> Result<(), InsertionError> {
        let folder_clone = folder.clone();
        blocking_io(move || {
            folder_clone
                .data_path()
                .unzip_to_folder(bytes)
                .map_err(|err| {
                    InsertionError::from_graph_err(folder_clone.get_original_path_str(), err)
                })
        })
        .await?;
        self.vectorise_folder(folder.as_existing()?).await;
        blocking_io(move || folder.finish()).await?;
        Ok(())
    }

    async fn delete_graph_inner(
        &self,
        graph_folder: ExistingGraphFolder,
    ) -> Result<(), MutationErrorInner> {
        blocking_io(move || {
            let dirty_file = mark_dirty(graph_folder.path())?;
            fs::remove_dir_all(graph_folder.path())?;
            fs::remove_file(dirty_file)?;
            Ok::<_, MutationErrorInner>(())
        })
        .await?;
        Ok(())
    }

    pub async fn delete_graph(&self, path: &str) -> Result<(), DeletionError> {
        let graph_folder = ExistingGraphFolder::try_from(self.work_dir.clone(), path)?;
        self.delete_graph_inner(graph_folder)
            .await
            .map_err(|err| DeletionError::from_inner(path, err))?;
        self.cache.remove(path).await;
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
                Some(&folder.get_vectors_path().ok()?),
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

    async fn vectorise_folder(&self, folder: ExistingGraphFolder) -> Option<()> {
        // it's important that we check if there is a valid template set for this graph path
        // before actually loading the graph, otherwise we are loading the graph for no reason
        let template = self.resolve_template(folder.get_original_path())?;
        let graph = self
            .read_graph_from_disk_inner(folder.clone())
            .await
            .ok()?
            .graph;
        self.vectorise_with_template(graph, &folder, template).await;
        Some(())
    }

    pub(crate) async fn vectorise_all_graphs_that_are_not(&self) -> Result<(), GraphError> {
        for folder in self.get_all_graph_folders() {
            if !folder.data_path().get_vectors_path()?.exists() {
                self.vectorise_folder(folder).await;
            }
        }
        Ok(())
    }

    // TODO: return iter
    pub fn get_all_graph_folders(&self) -> impl Iterator<Item = ExistingGraphFolder> {
        let base_path = self.work_dir.clone();
        WalkDir::new(&self.work_dir)
            .into_iter()
            .filter_map(move |e| {
                let entry = e.ok()?;
                let path = entry.path();
                let relative = get_relative_path(&base_path, path).ok()?;
                let folder = ExistingGraphFolder::try_from(base_path.clone(), &relative).ok()?;
                Some(folder)
            })
    }

    async fn read_graph_from_disk_inner(
        &self,
        folder: ExistingGraphFolder,
    ) -> Result<GraphWithVectors, GQLError> {
        let cache = self.embedding_conf.as_ref().map(|conf| conf.cache.clone());
        let create_index = self.create_index;
        Ok(
            blocking_io(move || GraphWithVectors::read_from_folder(&folder, cache, create_index))
                .await?,
        )
    }

    async fn read_graph_from_disk(&self, path: &str) -> Result<GraphWithVectors, GQLError> {
        let folder = ExistingGraphFolder::try_from(self.work_dir.clone(), path)?;
        self.read_graph_from_disk_inner(folder).await
    }
}

impl Drop for Data {
    fn drop(&mut self) {
        // On drop, serialize graphs that don't have underlying storage.
        for (_, graph) in self.cache.iter() {
            if let Err(e) = graph.folder.write_graph_data(graph.graph) {
                error!("Error encoding graph to disk on drop: {e}");
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod data_tests {
    use super::InsertionError;
    use crate::{config::app_config::AppConfigBuilder, data::Data};
    use itertools::Itertools;
    use raphtory::{
        db::api::view::{internal::InternalStorageOps, MaterializedGraph},
        prelude::*,
    };
    use std::{collections::HashMap, fs, path::Path, time::Duration};
    use tokio::time::sleep;

    fn create_graph_folder(path: &Path) {
        // Use empty graph to create folder structure
        let graph = Graph::new();
        graph.encode(path).unwrap();
    }

    pub(crate) async fn save_graphs_to_work_dir(
        data: &Data,
        graphs: &HashMap<String, MaterializedGraph>,
    ) -> Result<(), InsertionError> {
        for (name, graph) in graphs.into_iter() {
            let folder = data.validate_path_for_insert(name, true)?;
            data.insert_graph(folder, graph.clone()).await?;
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
        let data = Data::new(tmp_work_dir.path(), &Default::default());

        save_graphs_to_work_dir(&data, &graphs).await.unwrap();

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

        assert!(!data.cache.contains_key("test_g"));
        assert!(!data.cache.contains_key("test_g2"));

        // Test size based eviction
        data.get_graph("test_g2").await.unwrap();
        assert!(data.cache.contains_key("test_g2"));
        assert!(!data.cache.contains_key("test_g"));

        data.get_graph("test_g").await.unwrap(); // wait for any eviction
        data.cache.run_pending_tasks().await;
        assert_eq!(data.cache.iter().count(), 1);

        sleep(Duration::from_secs(3)).await;
        assert!(!data.cache.contains_key("test_g"));
        assert!(!data.cache.contains_key("test_g2"));
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
            .map(|folder| folder.0.data_path().root().to_path_buf())
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

        let loaded_graph1 = data.get_graph("test_graph1").await.unwrap();
        let loaded_graph2 = data.get_graph("test_graph2").await.unwrap();

        // TODO: This test doesn't work with disk storage right now, make sure modification dates actually update correctly!
        if loaded_graph1.graph.disk_storage_enabled() {
            assert!(
                !loaded_graph1.is_dirty(),
                "Graph1 should not be dirty when loaded from disk"
            );
            assert!(
                !loaded_graph2.is_dirty(),
                "Graph2 should not be dirty when loaded from disk"
            );

            // Modify only graph1 to make it dirty
            loaded_graph1.set_dirty(true);
            assert!(
                loaded_graph1.is_dirty(),
                "Graph1 should be dirty after modification"
            );

            // Drop the Data instance - this should trigger serialization
            drop(data);

            // Check modification times after drop
            let graph1_metadata_after = fs::metadata(&graph1_path).unwrap();
            let graph2_metadata_after = fs::metadata(&graph2_path).unwrap();
            let graph1_modified_time = graph1_metadata_after.modified().unwrap();
            let graph2_modified_time = graph2_metadata_after.modified().unwrap();

            // Graph1 (dirty) modification time should be different
            assert_ne!(
                graph1_original_time, graph1_modified_time,
                "Graph1 (dirty) should have been written to disk on drop"
            );

            // Graph2 (not dirty) modification time should be the same
            assert_eq!(
                graph2_original_time, graph2_modified_time,
                "Graph2 (not dirty) should not have been written to disk on drop"
            );
        }
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
        let loaded_graph1 = data.get_graph("test_graph1").await.unwrap();
        assert!(
            !loaded_graph1.is_dirty(),
            "Graph1 should not be dirty when loaded from disk"
        );

        // Modify graph1 to make it dirty
        loaded_graph1.set_dirty(true);
        assert!(
            loaded_graph1.is_dirty(),
            "Graph1 should be dirty after modification"
        );

        // Load second graph
        println!("Loading second graph");
        let loaded_graph2 = data.get_graph("test_graph2").await.unwrap();
        assert!(
            !loaded_graph2.is_dirty(),
            "Graph2 should not be dirty when loaded from disk"
        );

        // Sleep to trigger eviction
        sleep(Duration::from_secs(3)).await;
        data.cache.run_pending_tasks().await;

        // TODO: This test doesn't work with disk storage right now, make sure modification dates actually update correctly!
        if loaded_graph1.graph.disk_storage_enabled() {
            // Check modification times after eviction
            let graph1_metadata_after = fs::metadata(&graph1_path).unwrap();
            let graph2_metadata_after = fs::metadata(&graph2_path).unwrap();
            let graph1_modified_time = graph1_metadata_after.modified().unwrap();
            let graph2_modified_time = graph2_metadata_after.modified().unwrap();

            // Graph1 (dirty) modification time should be different
            assert_ne!(
                graph1_original_time, graph1_modified_time,
                "Graph1 (dirty) should have been written to disk on eviction"
            );

            // Graph2 (not dirty) modification time should be the same
            assert_eq!(
                graph2_original_time, graph2_modified_time,
                "Graph2 (not dirty) should not have been written to disk on eviction"
            );
        }
    }
}
