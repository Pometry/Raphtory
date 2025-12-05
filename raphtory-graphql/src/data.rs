use crate::{
    config::app_config::AppConfig,
    graph::GraphWithVectors,
    model::blocking_io,
    paths::{valid_path, ExistingGraphFolder, ValidGraphFolder},
};
use itertools::Itertools;
use moka::future::Cache;
use raphtory::{
    db::api::view::MaterializedGraph,
    errors::{GraphError, GraphResult, InvalidPathReason},
    prelude::CacheOps,
    vectors::{
        cache::{CachedEmbeddingModel, VectorCache},
        template::DocumentTemplate,
        vectorisable::Vectorisable,
        vectorised_graph::VectorisedGraph,
    },
};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::fs;
use tracing::{error, warn};
use walkdir::WalkDir;

// #[derive(Clone)]
// pub struct EmbeddingConf {
//     pub(crate) cache: VectorCache,
//     // pub(crate) global_template: Option<DocumentTemplate>,
//     // pub(crate) individual_templates: HashMap<PathBuf, DocumentTemplate>,
// }

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
    pub(crate) work_dir: PathBuf, // TODO: move this to config?
    cache: Cache<PathBuf, GraphWithVectors>,
    pub(crate) create_index: bool, // TODO: move this to config?
    pub(crate) vector_cache: VectorCache,
}

impl Data {
    pub async fn new(work_dir: &Path, configs: &AppConfig) -> GraphResult<Self> {
        let cache_configs = &configs.cache;

        let cache = Cache::<PathBuf, GraphWithVectors>::builder()
            .max_capacity(cache_configs.capacity)
            .time_to_idle(std::time::Duration::from_secs(cache_configs.tti_seconds))
            .eviction_listener(|_, graph, _| {
                graph
                    .write_updates()
                    .unwrap_or_else(|err| error!("Write on eviction failed: {err:?}"))
                // FIXME: don't have currently a way to know which embedding updates are pending
            })
            .build();

        #[cfg(feature = "search")]
        let create_index = configs.index.create_index;
        #[cfg(not(feature = "search"))]
        let create_index = false;

        // TODO: make vector feature optional?

        Ok(Self {
            work_dir: work_dir.to_path_buf(),
            cache,
            create_index,
            vector_cache: VectorCache::on_disk(&work_dir.join(".vector-cache")).await?, // FIXME: need to disable graph names starting with a dot
        })
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
            Ok(_) => Err(GraphError::GraphNameAlreadyExists(folder.to_error_path())),
            Err(_) => {
                fs::create_dir_all(folder.get_base_path()).await?;
                let folder_clone = folder.clone();
                let graph_clone = graph.clone();
                blocking_io(move || graph_clone.cache(folder_clone)).await?;
                // let vectors = self.vectorise(graph.clone(), &folder).await;
                let graph = GraphWithVectors::new(graph, None);
                graph
                    .folder
                    .get_or_try_init(|| Ok::<_, GraphError>(folder.into()))?;
                self.cache.insert(path.into(), graph).await;
                Ok(())
            }
        }
    }

    pub async fn delete_graph(&self, path: &str) -> Result<(), GraphError> {
        let graph_folder = ExistingGraphFolder::try_from(self.work_dir.clone(), path)?;
        fs::remove_dir_all(graph_folder.get_base_path()).await?;
        self.cache.remove(&PathBuf::from(path)).await;
        Ok(())
    }

    async fn vectorise_with_template(
        &self,
        graph: MaterializedGraph,
        folder: &ValidGraphFolder,
        template: &DocumentTemplate,
        model: CachedEmbeddingModel,
    ) -> Option<VectorisedGraph<MaterializedGraph>> {
        let vectors = graph
            .vectorise(
                model,
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

    pub(crate) async fn vectorise_folder(
        &self,
        folder: &ExistingGraphFolder,
        template: &DocumentTemplate,
        model: CachedEmbeddingModel,
    ) -> GraphResult<()> {
        let graph = self.read_graph_from_folder(folder.clone()).await?.graph;
        self.vectorise_with_template(graph, folder, template, model)
            .await;
        self.cache
            .remove(&folder.get_original_path().to_path_buf())
            .await;
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
        let create_index = self.create_index;
        GraphWithVectors::read_from_folder(&folder, &self.vector_cache, create_index).await
        // FIXME: I need some blocking_io inside of GraphWithVectors::read_from_folder
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
    use std::{collections::HashMap, fs, fs::File, io, path::Path, time::Duration};
    use tokio::time::sleep;

    #[cfg(feature = "storage")]
    use raphtory_storage::{core_ops::CoreGraphOps, graph::graph::GraphStorage};

    #[cfg(feature = "storage")]
    fn copy_dir_recursive(source_dir: &Path, target_dir: &Path) -> Result<(), GraphError> {
        fs::create_dir_all(target_dir)?;
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
        fs::create_dir_all(path).unwrap();
        File::create(path.join(".raph")).unwrap();
        File::create(path.join("graph")).unwrap();
    }

    pub(crate) async fn save_graphs_to_work_dir(
        work_dir: &Path,
        graphs: &HashMap<String, MaterializedGraph>,
    ) -> Result<(), GraphError> {
        for (name, graph) in graphs.into_iter() {
            let data = Data::new(work_dir, &AppConfig::default()).await?;
            let folder = ValidGraphFolder::try_from(data.work_dir, name)?;

            #[cfg(feature = "storage")]
            if let GraphStorage::Disk(dg) = graph.core_graph() {
                let disk_graph_path = dg.graph_dir();
                copy_dir_recursive(disk_graph_path, &folder.get_graph_path())?;
                File::create(folder.get_meta_path())?;
            } else {
                graph.encode(folder)?;
            }

            #[cfg(not(feature = "storage"))]
            graph.encode(folder)?;
        }
        Ok(())
    }

    #[tokio::test]
    #[cfg(feature = "storage")]
    async fn test_get_disk_graph_from_path() {
        let tmp_graph_dir = tempfile::tempdir().unwrap();

        let graph = Graph::new();
        graph
            .add_edge(0, 1, 2, [("name", "test_e1")], None)
            .unwrap();
        graph
            .add_edge(0, 1, 3, [("name", "test_e2")], None)
            .unwrap();

        let base_path = tmp_graph_dir.path().to_owned();
        let graph_path = base_path.join("test_dg");
        fs::create_dir(&graph_path).unwrap();
        File::create(graph_path.join(".raph")).unwrap();
        let _ = DiskGraphStorage::from_graph(&graph, &graph_path.join("graph")).unwrap();

        let data = Data::new(&base_path, &Default::default());
        let res = data.get_graph("test_dg").await.unwrap().0;
        assert_eq!(res.graph.into_events().unwrap().count_edges(), 2);

        // Dir path doesn't exists
        let res = data.get_graph("test_dg1").await;
        assert!(res.is_err());
        if let Err(err) = res {
            assert!(err.to_string().contains("Graph not found"));
        }

        // Dir path exists but is not a disk graph path
        // let tmp_graph_dir = tempfile::tempdir().unwrap();
        // let res = read_graph_from_path(base_path, "");
        let res = data.get_graph("").await;
        assert!(res.is_err());
        if let Err(err) = res {
            assert!(err.to_string().contains("Graph not found"));
        }
    }

    #[tokio::test]
    async fn test_save_graphs_to_work_dir() {
        let tmp_graph_dir = tempfile::tempdir().unwrap();
        let tmp_work_dir = tempfile::tempdir().unwrap();

        let graph = Graph::new();
        graph.add_metadata([("name", "test_g")]).unwrap();
        graph
            .add_edge(0, 1, 2, [("name", "test_e1")], None)
            .unwrap();
        graph
            .add_edge(0, 1, 3, [("name", "test_e2")], None)
            .unwrap();

        #[cfg(feature = "storage")]
        let graph2: MaterializedGraph = graph
            .persist_as_disk_graph(tmp_graph_dir.path())
            .unwrap()
            .into();

        let graph: MaterializedGraph = graph.into();

        let mut graphs = HashMap::new();

        graphs.insert("test_g".to_string(), graph);

        #[cfg(feature = "storage")]
        graphs.insert("test_dg".to_string(), graph2);

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

        fs::create_dir_all(&g4_path.join("graph")).unwrap();
        File::create(g4_path.join(".raph")).unwrap();
        create_ipc_files_in_dir(&g4_path.join("graph")).unwrap();

        fs::create_dir_all(&g5_path).unwrap();

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
        assert!(!paths.contains(&g5_path)); // Empty dir is ignored

        assert!(data
            .get_graph("shivam/investigations/2024-12-22/g2")
            .await
            .is_ok());
        assert!(data.get_graph("some/random/path").await.is_err());
    }
}
