use crate::{
    config::app_config::AppConfig,
    graph::GraphWithVectors,
    model::plugins::query_plugin::QueryPlugin,
    paths::{ExistingGraphFolder, ValidGraphFolder},
};
use moka::sync::Cache;
use raphtory::{
    core::utils::errors::{GraphError, GraphResult},
    db::api::view::MaterializedGraph,
    vectors::{
        embedding_cache::EmbeddingCache, embeddings::openai_embedding, template::DocumentTemplate,
        vectorisable::Vectorisable, vectorised_graph::VectorisedGraph, Embedding,
        EmbeddingFunction,
    },
};
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf, StripPrefixError},
    sync::Arc,
};
use tracing::{error, warn};
use walkdir::WalkDir;

#[derive(Clone)]
pub struct EmbeddingConf {
    pub(crate) function: Arc<dyn EmbeddingFunction>,
    pub(crate) cache: Arc<Option<EmbeddingCache>>, // FIXME: no need for this to be Option
    pub(crate) global_template: Option<DocumentTemplate>,
    pub(crate) individual_templates: HashMap<PathBuf, DocumentTemplate>,
}

#[derive(Clone)]
pub struct Data {
    pub(crate) work_dir: PathBuf,
    cache: Cache<PathBuf, GraphWithVectors>,
    pub(crate) index: bool,
    pub(crate) embedding_conf: Option<EmbeddingConf>,
}

impl Data {
    pub fn new(work_dir: &Path, configs: &AppConfig) -> Self {
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

        Self {
            work_dir: work_dir.to_path_buf(),
            cache,
            index: true,
            embedding_conf: Default::default(),
        }
    }

    pub fn get_graph(
        &self,
        path: &str,
    ) -> Result<(GraphWithVectors, ExistingGraphFolder), Arc<GraphError>> {
        let graph_folder = ExistingGraphFolder::try_from(self.work_dir.clone(), path)?;
        self.cache
            .try_get_with(path.into(), || self.read_graph_from_folder(&graph_folder))
            .map(|graph| (graph, graph_folder))
    }

    pub async fn insert_graph(
        &self,
        path: &str,
        graph: MaterializedGraph,
    ) -> Result<(), GraphError> {
        let folder = ValidGraphFolder::try_from(self.work_dir.clone(), path)?;
        let vectors = self.vectorise(graph.clone(), &folder).await;
        let index = self.index.then(|| graph.clone().into());
        let graph = GraphWithVectors::new(graph, index, vectors);
        self.insert_graph_with_vectors(path, graph)
    }

    pub fn insert_graph_with_vectors(
        &self,
        path: &str,
        graph: GraphWithVectors,
    ) -> Result<(), GraphError> {
        // TODO: replace ValidGraphFolder with ValidNonExistingGraphFolder !!!!!!!!!
        // or even a NewGraphFolder, so that we try to create the graph file and if that is sucessful
        // we can write to it and its guaranteed to me atomic
        let folder = ValidGraphFolder::try_from(self.work_dir.clone(), path)?;
        match ExistingGraphFolder::try_from(self.work_dir.clone(), path) {
            Ok(_) => Err(GraphError::GraphNameAlreadyExists(folder.to_error_path())),
            Err(_) => {
                fs::create_dir_all(folder.get_base_path())?;
                graph.cache(folder)?;
                self.cache.insert(path.into(), graph);
                Ok(())
            }
        }
    }

    pub fn delete_graph(&self, path: &str) -> Result<(), GraphError> {
        let graph_folder = ExistingGraphFolder::try_from(self.work_dir.clone(), path)?;
        fs::remove_dir_all(graph_folder.get_base_path())?;
        self.cache.remove(&PathBuf::from(path));
        Ok(())
    }

    pub async fn embed_query(&self, query: String) -> GraphResult<Embedding> {
        let embedding_function = self
            .embedding_conf
            .as_ref()
            .map(|conf| conf.function.clone());
        let embedding = if let Some(embedding_function) = embedding_function {
            embedding_function.call(vec![query]).await?.remove(0)
        } else {
            openai_embedding(vec![query]).await?.remove(0)
        };
        Ok(embedding)
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
                Box::new(conf.function.clone()),
                conf.cache.clone(),
                true, // overwrite
                template.clone(),
                Some(folder.get_original_path_str().to_owned()),
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

    async fn vectorise_folder(
        &self,
        folder: &ExistingGraphFolder,
    ) -> Option<VectorisedGraph<MaterializedGraph>> {
        // it's important that we check if there is a valid template set for this graph path
        // before actually loading the graph, otherwise we are loading the graph for no reason
        let template = self.resolve_template(folder.get_original_path())?;
        let graph = self.read_graph_from_folder(folder).ok()?.graph;
        self.vectorise_with_template(graph, folder, template).await
    }

    pub(crate) async fn vectorise_all_graphs_that_are_not(&self) -> Result<(), GraphError> {
        for folder in self.get_all_graph_folders() {
            if !folder.get_vectors_path().exists() {
                let vectors = self.vectorise_folder(&folder).await;
                if let Some(vectors) = vectors {
                    vectors.write_to_path(&folder.get_vectors_path())?;
                }
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
                let relative = self.get_relative_path(path).ok()?;
                let relative_str = relative.to_str()?; // potential UTF8 error here
                let cleaned = relative_str.replace(r"\", "/");
                let folder = ExistingGraphFolder::try_from(base_path.clone(), &cleaned).ok()?;
                Some(folder)
            })
            .collect()
    }

    fn get_relative_path(&self, path: &Path) -> Result<PathBuf, StripPrefixError> {
        Ok(path.strip_prefix(&self.work_dir)?.to_path_buf())
    }

    pub(crate) fn get_global_plugins(&self) -> QueryPlugin {
        let graphs = self
            .get_all_graph_folders()
            .into_iter()
            .filter_map(|folder| {
                Some((
                    folder.get_original_path_str().to_owned(),
                    self.read_graph_from_folder(&folder).ok()?.vectors?,
                ))
            })
            .collect::<HashMap<_, _>>();
        QueryPlugin {
            graphs: graphs.into(),
        }
    }

    fn read_graph_from_folder(
        &self,
        folder: &ExistingGraphFolder,
    ) -> Result<GraphWithVectors, GraphError> {
        let embedding = self
            .embedding_conf
            .as_ref()
            .map(|conf| conf.function.clone())
            .unwrap_or(Arc::new(openai_embedding));
        let cache = self
            .embedding_conf
            .as_ref()
            .map(|conf| conf.cache.clone())
            .unwrap_or(Arc::new(None));

        GraphWithVectors::read_from_folder(folder, self.index, embedding, cache)
    }
}

#[cfg(test)]
pub(crate) mod data_tests {
    use crate::{
        config::app_config::{AppConfig, AppConfigBuilder},
        data::Data,
    };
    use itertools::Itertools;
    use raphtory::{core::utils::errors::GraphError, db::api::view::MaterializedGraph, prelude::*};
    use std::{
        collections::HashMap,
        fs,
        fs::File,
        io,
        path::{Path, PathBuf},
    };

    #[cfg(feature = "storage")]
    use raphtory::{
        db::api::storage::graph::storage_ops::GraphStorage, db::api::view::internal::CoreGraphOps,
        disk_graph::DiskGraphStorage,
    };
    #[cfg(feature = "storage")]
    use std::{thread, time::Duration};

    use super::ValidGraphFolder;

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
                copy_dir_recursive(disk_graph_path, &folder.get_graph_path())?;
            } else {
                graph.encode(folder)?;
            }

            #[cfg(not(feature = "storage"))]
            graph.encode(folder)?;
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

        let base_path = tmp_graph_dir.path().to_owned();
        let graph_path = base_path.join("test_dg");
        fs::create_dir(&graph_path).unwrap();
        File::create(graph_path.join(".raph")).unwrap();
        let _ = DiskGraphStorage::from_graph(&graph, &graph_path.join("graph")).unwrap();

        let data = Data::new(&base_path, &Default::default());
        let res = data.get_graph("test_dg").unwrap().0;
        assert_eq!(res.graph.into_events().unwrap().count_edges(), 2);

        // Dir path doesn't exists
        let res = data.get_graph("test_dg1");
        assert!(res.is_err());
        if let Err(err) = res {
            assert!(err.to_string().contains("Graph not found"));
        }

        // Dir path exists but is not a disk graph path
        // let tmp_graph_dir = tempfile::tempdir().unwrap();
        // let res = read_graph_from_path(base_path, "");
        let res = data.get_graph("");
        assert!(res.is_err());
        if let Err(err) = res {
            assert!(err.to_string().contains("Graph not found"));
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

    #[cfg(feature = "storage")]
    #[test]
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

        let disk_graph_path = tmp_work_dir.path().join("test_dg");
        fs::create_dir(&disk_graph_path).unwrap();
        File::create(disk_graph_path.join(".raph")).unwrap();
        let _ = DiskGraphStorage::from_graph(&graph, disk_graph_path.join("graph")).unwrap();

        graph.encode(&tmp_work_dir.path().join("test_g2")).unwrap();

        let configs = AppConfigBuilder::new()
            .with_cache_capacity(1)
            .with_cache_tti_seconds(2)
            .build();

        let data = Data::new(tmp_work_dir.path(), &configs);

        assert!(!data.cache.contains_key(&PathBuf::from("test_dg")));
        assert!(!data.cache.contains_key(&PathBuf::from("test_g")));

        // Test size based eviction
        let _ = data.get_graph("test_dg");
        assert!(data.cache.contains_key(&PathBuf::from("test_dg")));
        assert!(!data.cache.contains_key(&PathBuf::from("test_g")));

        let _ = data.get_graph("test_g");
        assert!(data.cache.contains_key(&PathBuf::from("test_g")));

        thread::sleep(Duration::from_secs(3));
        assert!(!data.cache.contains_key(&PathBuf::from("test_dg")));
        assert!(!data.cache.contains_key(&PathBuf::from("test_g")));
    }

    fn create_graph_folder(path: &Path) {
        fs::create_dir_all(path).unwrap();
        File::create(path.join(".raph")).unwrap();
        File::create(path.join("graph")).unwrap();
    }

    #[test]
    fn test_get_graph_paths() {
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
            .is_ok());
        assert!(data.get_graph("some/random/path").is_err());
    }
}
