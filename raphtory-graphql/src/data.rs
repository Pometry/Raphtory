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
    errors::{GraphError, InvalidPathReason},
    prelude::CacheOps,
    vectors::{
        cache::VectorCache, template::DocumentTemplate, vectorisable::Vectorisable,
        vectorised_graph::VectorisedGraph,
    },
};
use raphtory_api::core::storage::{FxDashMap, dashmap::mapref::entry::Entry};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::fs;
use tracing::{error, warn};
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
    loaded_graphs: FxDashMap<PathBuf, GraphWithVectors>,
    pub(crate) create_index: bool,
    pub(crate) embedding_conf: Option<EmbeddingConf>,
}

impl Data {
    pub fn new(work_dir: &Path, configs: &AppConfig) -> Self {
        #[cfg(feature = "search")]
        let create_index = configs.index.create_index;
        #[cfg(not(feature = "search"))]
        let create_index = false;

        Self {
            work_dir: work_dir.to_path_buf(),
            loaded_graphs: FxDashMap::default(),
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
        let entry = self.loaded_graphs.entry(path.into());

        match entry {
            Entry::Occupied(entry) => Ok((entry.get().clone(), graph_folder)),
            Entry::Vacant(entry) => {
                let graph = self.read_graph_from_folder(graph_folder_clone).await?;

                entry.insert(graph.clone());
                Ok((graph, graph_folder))
            }
        }
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
                let vectors = self.vectorise(graph.clone(), &folder).await;
                let graph = GraphWithVectors::new(graph, vectors);
                graph
                    .folder
                    .get_or_try_init(|| Ok::<_, GraphError>(folder.into()))?;
                self.loaded_graphs.insert(path.into(), graph);
                Ok(())
            }
        }
    }

    pub async fn delete_graph(&self, path: &str) -> Result<(), GraphError> {
        let graph_folder = ExistingGraphFolder::try_from(self.work_dir.clone(), path)?;
        fs::remove_dir_all(graph_folder.get_base_path()).await?;
        self.loaded_graphs.remove(&PathBuf::from(path));
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

#[cfg(test)]
pub(crate) mod data_tests {
    use super::ValidGraphFolder;
    use crate::{
        config::app_config::{AppConfig, AppConfigBuilder},
        data::Data,
    };
    use itertools::Itertools;
    use raphtory::{db::api::view::MaterializedGraph, errors::GraphError, prelude::*};
    use std::{collections::HashMap, fs, fs::File, io, path::Path};

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

    pub(crate) fn save_graphs_to_work_dir(
        work_dir: &Path,
        graphs: &HashMap<String, MaterializedGraph>,
    ) -> Result<(), GraphError> {
        for (name, graph) in graphs.into_iter() {
            let data = Data::new(work_dir, &AppConfig::default());
            let folder = ValidGraphFolder::try_from(data.work_dir, name)?;
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

        let configs = AppConfigBuilder::new().build();

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
