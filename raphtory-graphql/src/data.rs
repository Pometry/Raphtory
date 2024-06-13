use crate::{
    model::algorithms::global_plugins::GlobalPlugins,
    server_config::{load_config, CacheConfig},
};
use async_graphql::Error;
use dynamic_graphql::Result;
use itertools::Itertools;
use moka::sync::{Cache, CacheBuilder};
#[cfg(feature = "storage")]
use raphtory::disk_graph::graph_impl::DiskGraph;
use raphtory::{
    core::Prop,
    db::api::view::MaterializedGraph,
    prelude::{GraphViewOps, PropUnwrap, PropertyAdditionOps},
    search::IndexedGraph,
};
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};
use walkdir::WalkDir;

pub struct Data {
    pub(crate) work_dir: String,
    pub(crate) graphs: Arc<Cache<String, IndexedGraph<MaterializedGraph>>>,
    pub(crate) global_plugins: GlobalPlugins,
}

impl Data {
    pub fn new(
        work_dir: &Path,
        maybe_graphs: Option<HashMap<String, MaterializedGraph>>,
        maybe_graph_paths: Option<Vec<PathBuf>>,
        maybe_cache_config: Option<CacheConfig>,
    ) -> Self {
        let cache_configs = if maybe_cache_config.is_some() {
            maybe_cache_config.unwrap()
        } else {
            let app_config = load_config().expect("Failed to load config file");
            app_config.cache
        };

        let graphs_cache_builder = CacheBuilder::new(cache_configs.capacity)
            .time_to_idle(std::time::Duration::from_secs(cache_configs.tti_seconds));

        let graphs_cache: Arc<Cache<String, IndexedGraph<MaterializedGraph>>> =
            Arc::new(graphs_cache_builder.build());

        save_graphs_to_work_dir(work_dir, &maybe_graphs.unwrap_or_default())
            .expect("Failed to save graphs to work dir");

        load_graphs_from_paths(work_dir, maybe_graph_paths.unwrap_or_default())
            .expect("Failed to save graph paths to work dir");

        Self {
            work_dir: work_dir.to_string_lossy().into_owned(),
            graphs: graphs_cache,
            global_plugins: GlobalPlugins::default(),
        }
    }

    pub fn get_graph(&self, name: &str) -> Result<Option<IndexedGraph<MaterializedGraph>>> {
        let name = name.to_string();
        let path = Path::new(&self.work_dir).join(name.as_str());
        if !path.exists() {
            Ok(None)
        } else {
            match self.graphs.get(&name) {
                Some(graph) => Ok(Some(graph.clone())),
                None => match get_graph_from_path(Path::new(&self.work_dir), &path)? {
                    Some((_, graph)) => Ok(Some(self.graphs.get_with(name, || graph))),
                    None => Ok(None),
                },
            }
        }
    }

    pub fn get_graphs(&self) -> Result<Vec<(String, IndexedGraph<MaterializedGraph>)>> {
        Ok(get_graphs_from_work_dir(Path::new(&self.work_dir))?
            .into_iter()
            .collect_vec())
    }

    #[allow(dead_code)]
    // TODO: use this for loading both regular and vectorised graphs
    #[allow(dead_code)]
    pub fn generic_load_from_file<T, F>(path: &str, loader: F) -> impl Iterator<Item = T>
    where
        F: Fn(&Path) -> T + 'static,
    {
        WalkDir::new(path)
            .into_iter()
            .filter_map(|e| {
                let entry = e.ok()?;
                let path = entry.path();
                let filename = path.file_name().and_then(|name| name.to_str())?;
                (path.is_file() && !filename.starts_with('.')).then_some(entry)
            })
            .map(move |entry| {
                let path = entry.path();
                let path_string = path.display().to_string();
                println!("loading from {path_string}");
                loader(path)
            })
    }
}

fn load_graph_from_path(work_dir: &Path, path: &Path) -> Result<String> {
    println!("loading graph from {}", path.display());
    let result: Result<(String, MaterializedGraph), Error> = if path.is_dir() {
        println!("Disk Graph loaded = {}", path.display());
        if is_disk_graph_dir(path) {
            load_disk_graph(path)
        } else {
            Err(Error::from("Not a disk graph directory"))
        }
    } else {
        println!("Graph loaded = {}", path.display());
        load_bincode_graph(work_dir, path)
    };

    let (name, graph) = result?;
    let path = Path::new(work_dir).join(name.as_str());

    graph.save_to_file(path)?;

    Ok(name)
}

// The default behaviour is to just override the existing graphs.
// Always returns list of newly loaded graphs and not all the graphs available in the work dir.
pub fn load_graphs_from_path(work_dir: &Path, path: &Path) -> Result<Vec<String>> {
    println!("loading graphs from {}", path.display());
    let entries = fs::read_dir(path).unwrap();
    entries
        .map(|entry| {
            let path = entry?.path();
            load_graph_from_path(work_dir, &path)
        })
        .collect()
}

fn load_graphs_from_paths(work_dir: &Path, paths: Vec<PathBuf>) -> Result<Vec<String>> {
    paths
        .iter()
        .map(|path| load_graph_from_path(work_dir, path))
        .collect()
}

fn get_graph_from_path(
    work_dir: &Path,
    path: &Path,
) -> Result<Option<(String, IndexedGraph<MaterializedGraph>)>, Error> {
    if !path.exists() {
        return Ok(None);
    }
    if path.is_dir() {
        println!("Disk Graph loaded = {}", path.display());
        if is_disk_graph_dir(path) {
            let (name, graph) = load_disk_graph(path)?;
            Ok(Some((name, IndexedGraph::from_graph(&graph.into())?)))
        } else {
            Ok(None)
        }
    } else {
        println!("Graph loaded = {}", path.display());
        let (name, graph) = load_bincode_graph(work_dir, path)?;
        Ok(Some((name, IndexedGraph::from_graph(&graph.into())?)))
    }
}

pub(crate) fn get_graphs_from_work_dir(
    work_dir: &Path,
) -> Result<HashMap<String, IndexedGraph<MaterializedGraph>>> {
    fn get_graph_paths(work_dir: &Path) -> Vec<PathBuf> {
        let mut paths = Vec::new();
        if let Ok(entries) = fs::read_dir(work_dir) {
            for entry in entries {
                if let Ok(entry) = entry {
                    paths.push(entry.path());
                }
            }
        }
        paths
    }

    let mut graphs = HashMap::new();
    for path in get_graph_paths(work_dir) {
        if let Some((name, graph)) = get_graph_from_path(work_dir, &path)? {
            graphs.insert(name, graph);
        }
    }

    Ok(graphs)
}

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

fn get_graph_name(path: &Path, graph: &MaterializedGraph) -> String {
    graph
        .properties()
        .get("name")
        .into_str()
        .map(|v| v.to_string())
        .unwrap_or_else(|| path.file_name().unwrap().to_str().unwrap().to_owned())
}

fn save_graphs_to_work_dir(
    work_dir: &Path,
    graphs: &HashMap<String, MaterializedGraph>,
) -> Result<()> {
    for (name, graph) in graphs {
        let path = work_dir.join(&name);
        graph.save_to_file(&path)?;
    }
    Ok(())
}

fn load_bincode_graph(work_dir: &Path, path: &Path) -> Result<(String, MaterializedGraph)> {
    let graph = MaterializedGraph::load_from_file(path, false)?;
    let name = get_graph_name(path, &graph);
    let path = Path::new(work_dir).join(name.as_str());
    graph.update_constant_properties([(
        "path".to_string(),
        Prop::str(path.display().to_string().clone()),
    )])?;
    Ok((name, graph))
}

#[cfg(feature = "storage")]
fn load_disk_graph(path: &Path) -> Result<(String, MaterializedGraph)> {
    let disk_graph = DiskGraph::load_from_dir(path)?;
    let graph: MaterializedGraph = disk_graph.into();
    let graph_name = get_graph_name(path, &graph);

    Ok((graph_name, graph))
}

#[allow(unused_variables)]
#[cfg(not(feature = "storage"))]
fn load_disk_graph(path: &Path) -> Result<(String, MaterializedGraph)> {
    unimplemented!("Storage feature not enabled, cannot load from disk graph")
}

#[cfg(test)]
mod data_tests {
    use crate::data::{
        get_graph_from_path, get_graphs_from_work_dir, load_graph_from_path, load_graphs_from_path,
        load_graphs_from_paths,
    };
    use itertools::Itertools;
    use raphtory::prelude::{AdditionOps, Graph, GraphViewOps};

    #[test]
    fn test_load_graph_from_path() {
        let tmp_graph_dir = tempfile::tempdir().unwrap();
        let tmp_work_dir = tempfile::tempdir().unwrap();

        let graph = Graph::new();
        graph
            .add_edge(0, 1, 2, [("name", "test_e1")], None)
            .unwrap();
        graph
            .add_edge(0, 1, 3, [("name", "test_e2")], None)
            .unwrap();
        let graph_path = tmp_graph_dir.path().join("test_g");
        graph.save_to_file(&graph_path).unwrap();

        let res = load_graph_from_path(tmp_work_dir.path(), &graph_path).unwrap();
        assert_eq!(res, "test_g");

        let graph = Graph::load_from_file(tmp_work_dir.path().join("test_g"), false).unwrap();
        assert_eq!(graph.count_edges(), 2);
    }

    #[test]
    fn test_load_graphs_from_path() {
        let tmp_graph_dir = tempfile::tempdir().unwrap();
        let tmp_work_dir = tempfile::tempdir().unwrap();

        let graph = Graph::new();
        graph
            .add_edge(0, 1, 2, [("name", "test_e1")], None)
            .unwrap();
        graph
            .add_edge(0, 1, 3, [("name", "test_e2")], None)
            .unwrap();
        let graph_path = tmp_graph_dir.path().join("test_g1");
        graph.save_to_file(&graph_path).unwrap();

        let graph = Graph::new();
        graph
            .add_edge(0, 1, 2, [("name", "test_e1")], None)
            .unwrap();
        graph
            .add_edge(0, 2, 3, [("name", "test_e2")], None)
            .unwrap();
        graph
            .add_edge(0, 2, 4, [("name", "test_e3")], None)
            .unwrap();
        let graph_path = tmp_graph_dir.path().join("test_g2");
        graph.save_to_file(&graph_path).unwrap();

        let res = load_graphs_from_path(tmp_work_dir.path(), tmp_graph_dir.path()).unwrap();
        assert_eq!(res, vec!["test_g2", "test_g1"]);

        let graph = Graph::load_from_file(tmp_work_dir.path().join("test_g1"), false).unwrap();
        assert_eq!(graph.count_edges(), 2);

        let graph = Graph::load_from_file(tmp_work_dir.path().join("test_g2"), false).unwrap();
        assert_eq!(graph.count_edges(), 3);

        // Test if graph is overwritten while load from path
        let graph = Graph::new();
        graph
            .add_edge(0, 1, 2, [("name", "test_e1")], None)
            .unwrap();
        graph
            .add_edge(0, 2, 3, [("name", "test_e2")], None)
            .unwrap();
        graph
            .add_edge(0, 2, 4, [("name", "test_e3")], None)
            .unwrap();
        graph
            .add_edge(0, 2, 5, [("name", "test_e4")], None)
            .unwrap();
        let graph_path = tmp_graph_dir.path().join("test_g2");
        graph.save_to_file(&graph_path).unwrap();

        let res = load_graphs_from_path(tmp_work_dir.path(), tmp_graph_dir.path()).unwrap();
        assert_eq!(res, vec!["test_g2", "test_g1"]);

        let graph = Graph::load_from_file(tmp_work_dir.path().join("test_g2"), false).unwrap();
        assert_eq!(graph.count_edges(), 4);
    }

    #[test]
    fn test_load_graphs_from_paths() {
        let tmp_graph_dir = tempfile::tempdir().unwrap();
        let tmp_work_dir = tempfile::tempdir().unwrap();

        let graph = Graph::new();
        graph
            .add_edge(0, 1, 2, [("name", "test_e1")], None)
            .unwrap();
        graph
            .add_edge(0, 1, 3, [("name", "test_e2")], None)
            .unwrap();
        let graph_path1 = tmp_graph_dir.path().join("test_g1");
        graph.save_to_file(&graph_path1).unwrap();

        let graph = Graph::new();
        graph
            .add_edge(0, 1, 2, [("name", "test_e1")], None)
            .unwrap();
        graph
            .add_edge(0, 2, 3, [("name", "test_e2")], None)
            .unwrap();
        graph
            .add_edge(0, 2, 4, [("name", "test_e3")], None)
            .unwrap();
        let graph_path2 = tmp_graph_dir.path().join("test_g2");
        graph.save_to_file(&graph_path2).unwrap();

        let res =
            load_graphs_from_paths(tmp_work_dir.path(), vec![graph_path1, graph_path2]).unwrap();
        assert_eq!(res, vec!["test_g1", "test_g2"]);

        let graph = Graph::load_from_file(tmp_work_dir.path().join("test_g1"), false).unwrap();
        assert_eq!(graph.count_edges(), 2);

        let graph = Graph::load_from_file(tmp_work_dir.path().join("test_g2"), false).unwrap();
        assert_eq!(graph.count_edges(), 3);
    }

    #[test]
    fn test_get_graph_from_path() {
        let tmp_graph_dir = tempfile::tempdir().unwrap();
        let tmp_work_dir = tempfile::tempdir().unwrap();

        let graph = Graph::new();
        graph
            .add_edge(0, 1, 2, [("name", "test_e1")], None)
            .unwrap();
        graph
            .add_edge(0, 1, 3, [("name", "test_e2")], None)
            .unwrap();
        let graph_path = tmp_graph_dir.path().join("test_g1");
        graph.save_to_file(&graph_path).unwrap();

        let res = get_graph_from_path(tmp_work_dir.path(), &graph_path).unwrap();

        assert!(res.is_some());
        let res = res.unwrap();
        assert_eq!(res.0, "test_g1");
        assert_eq!(res.1.graph.into_events().unwrap().count_edges(), 2);

        let res = get_graph_from_path(tmp_work_dir.path(), &tmp_graph_dir.path().join("test_g2"))
            .unwrap();
        assert!(res.is_none());

        // TODO: Add tests for disk graph
    }

    #[test]
    fn test_get_graphs_from_work_dir() {
        let tmp_graph_dir = tempfile::tempdir().unwrap();
        let tmp_work_dir = tempfile::tempdir().unwrap();

        let graph = Graph::new();
        graph
            .add_edge(0, 1, 2, [("name", "test_e1")], None)
            .unwrap();
        graph
            .add_edge(0, 1, 3, [("name", "test_e2")], None)
            .unwrap();
        let graph_path = tmp_graph_dir.path().join("test_g1");
        graph.save_to_file(&graph_path).unwrap();

        let graph = Graph::new();
        graph
            .add_edge(0, 1, 2, [("name", "test_e1")], None)
            .unwrap();
        graph
            .add_edge(0, 2, 3, [("name", "test_e2")], None)
            .unwrap();
        graph
            .add_edge(0, 2, 4, [("name", "test_e3")], None)
            .unwrap();
        let graph_path = tmp_graph_dir.path().join("test_g2");
        graph.save_to_file(&graph_path).unwrap();

        let res = load_graphs_from_path(tmp_work_dir.path(), tmp_graph_dir.path()).unwrap();
        assert_eq!(res, vec!["test_g2", "test_g1"]);

        let res = get_graphs_from_work_dir(tmp_work_dir.path()).unwrap();
        assert_eq!(res.len(), 2);
        let mut names = res.keys().collect_vec();
        names.sort();
        assert_eq!(names, vec!["test_g1", "test_g2"]);
    }

    // TODO: Add cache eviction tests
}
