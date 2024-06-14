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

        let graphs_cache_builder = Cache::builder()
            .max_capacity(cache_configs.capacity)
            .time_to_idle(std::time::Duration::from_secs(cache_configs.tti_seconds))
            .build();

        let graphs_cache: Arc<Cache<String, IndexedGraph<MaterializedGraph>>> =
            Arc::new(graphs_cache_builder);

        save_graphs_to_work_dir(work_dir, &maybe_graphs.unwrap_or_default())
            .expect("Failed to save graphs to work dir");

        load_graphs_from_paths(work_dir, maybe_graph_paths.unwrap_or_default(), true)
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

fn copy_dir_recursive(source_dir: &Path, target_dir: &Path) -> Result<()> {
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
    work_dir: &Path,
    path: &Path,
    overwrite: bool,
) -> Result<Option<String>> {
    let (name, graph) = load_disk_graph(path)?;
    let graph_dir = &graph.into_disk_graph().unwrap().graph_dir;
    let target_dir = &Path::new(work_dir).join(name.as_str());
    if target_dir.exists() {
        if overwrite {
            fs::remove_dir_all(target_dir)?;
            copy_dir_recursive(graph_dir, target_dir)?;
            println!("Disk Graph loaded = {}", path.display());
        }
    } else {
        copy_dir_recursive(graph_dir, target_dir)?;
        println!("Disk Graph loaded = {}", path.display());
    }
    Ok(Some(name))
}

#[cfg(not(feature = "storage"))]
fn load_disk_graph_from_path(
    work_dir: &Path,
    path: &Path,
    overwrite: bool,
) -> Result<Option<String>> {
    Ok(None)
}

// If overwrite is false, graphs with names that already exist in the work directory will be silently ignored!
fn load_graph_from_path(work_dir: &Path, path: &Path, overwrite: bool) -> Result<Option<String>> {
    if !path.exists() {
        return Ok(None);
    }
    println!("Loading graph from {}", path.display());
    if path.is_dir() {
        if is_disk_graph_dir(path) {
            load_disk_graph_from_path(work_dir, path, overwrite)
        } else {
            Ok(None)
        }
    } else {
        let (name, graph) = load_bincode_graph(work_dir, path)?;
        let path = Path::new(work_dir).join(name.as_str());
        if path.exists() {
            if overwrite {
                fs::remove_file(&path)?;
                graph.save_to_file(&path)?;
                println!("Graph loaded = {}", path.display());
            }
        } else {
            graph.save_to_file(&path)?;
            println!("Graph loaded = {}", path.display());
        }
        Ok(Some(name))
    }
}

// The default behaviour is to just override the existing graphs.
// Always returns list of newly loaded graphs and not all the graphs available in the work dir.
pub fn load_graphs_from_path(work_dir: &Path, path: &Path, overwrite: bool) -> Result<Vec<String>> {
    println!("loading graphs from {}", path.display());
    let entries = fs::read_dir(path).unwrap();
    entries
        .filter_map(|entry| {
            let path = entry.unwrap().path();
            load_graph_from_path(work_dir, &path, overwrite).transpose()
        })
        .collect()
}

fn load_graphs_from_paths(
    work_dir: &Path,
    paths: Vec<PathBuf>,
    overwrite: bool,
) -> Result<Vec<String>> {
    paths
        .iter()
        .filter_map(|path| load_graph_from_path(work_dir, path, overwrite).transpose())
        .collect()
}

#[cfg(feature = "storage")]
fn get_disk_graph_from_path(
    path: &Path,
) -> Result<Option<(String, IndexedGraph<MaterializedGraph>)>, Error> {
    let (name, graph) = load_disk_graph(path)?;
    println!("Disk Graph loaded = {}", path.display());
    Ok(Some((name, IndexedGraph::from_graph(&graph.into())?)))
}

#[cfg(not(feature = "storage"))]
fn get_disk_graph_from_path(
    path: &Path,
) -> Result<Option<(String, IndexedGraph<MaterializedGraph>)>, Error> {
    Ok(None)
}

fn get_graph_from_path(
    work_dir: &Path,
    path: &Path,
) -> Result<Option<(String, IndexedGraph<MaterializedGraph>)>, Error> {
    if !path.exists() {
        return Ok(None);
    }
    if path.is_dir() {
        if is_disk_graph_dir(path) {
            get_disk_graph_from_path(path)
        } else {
            Ok(None)
        }
    } else {
        let (name, graph) = load_bincode_graph(work_dir, path)?;
        println!("Graph loaded = {}", path.display());
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
    use crate::{
        data::{
            get_graph_from_path, get_graphs_from_work_dir, load_graph_from_path,
            load_graphs_from_path, load_graphs_from_paths, Data,
        },
        server_config::CacheConfig,
    };
    use itertools::Itertools;
    use moka::sync::Cache;
    #[cfg(feature = "storage")]
    use raphtory::disk_graph::graph_impl::DiskGraph;
    use raphtory::prelude::{AdditionOps, Graph, GraphViewOps, PropertyAdditionOps};
    use std::{fs, io, path::Path, thread, time::Duration};

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

    // This function creates files that mimic disk graph for tests
    fn create_ipc_files_in_dir(dir_path: &Path) -> io::Result<()> {
        if !dir_path.exists() {
            fs::create_dir_all(dir_path)?;
        }

        let file_paths = ["file1.ipc", "file2.txt", "file3.ipc"];

        for &file_name in &file_paths {
            let file_path = dir_path.join(file_name);
            fs::File::create(file_path)?;
        }

        Ok(())
    }

    #[test]
    #[cfg(not(feature = "storage"))]
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

        let res = load_graph_from_path(tmp_work_dir.path(), &graph_path, true)
            .unwrap()
            .unwrap();
        assert_eq!(res, "test_g");

        let graph = Graph::load_from_file(tmp_work_dir.path().join("test_g"), false).unwrap();
        assert_eq!(graph.count_edges(), 2);

        // Dir path doesn't exists
        let res = load_graph_from_path(
            tmp_work_dir.path(),
            &tmp_graph_dir.path().join("test_dg1"),
            true,
        )
        .unwrap();
        assert!(res.is_none());

        // Dir path exists but is not a disk graph path
        let tmp_graph_dir = tempfile::tempdir().unwrap();
        let res = load_graph_from_path(tmp_work_dir.path(), &tmp_graph_dir.path(), true).unwrap();
        assert!(res.is_none());

        // Dir path exists and is a disk graph path but storage feature is disabled
        let graph_path = tmp_graph_dir.path().join("test_dg");
        create_ipc_files_in_dir(&graph_path).unwrap();
        let res = load_graph_from_path(tmp_work_dir.path(), &graph_path, true).unwrap();
        assert!(res.is_none());
    }

    #[test]
    #[cfg(feature = "storage")]
    fn test_load_disk_graph_from_path() {
        let tmp_graph_dir = tempfile::tempdir().unwrap();
        let tmp_work_dir = tempfile::tempdir().unwrap();

        let graph = Graph::new();
        graph
            .add_edge(0, 1, 2, [("name", "test_e1")], None)
            .unwrap();
        graph
            .add_edge(0, 1, 3, [("name", "test_e2")], None)
            .unwrap();

        let graph_dir = tmp_graph_dir.path().join("test_dg");
        let _ = DiskGraph::from_graph(&graph, &graph_dir).unwrap();

        let res = load_graph_from_path(tmp_work_dir.path(), &graph_dir, true)
            .unwrap()
            .unwrap();
        assert_eq!(res, "test_dg");

        let graph = DiskGraph::load_from_dir(tmp_work_dir.path().join("test_dg")).unwrap();
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

        let res = load_graphs_from_path(tmp_work_dir.path(), tmp_graph_dir.path(), true).unwrap();
        assert_eq!(res, vec!["test_g2", "test_g1"]);

        let graph = Graph::load_from_file(tmp_work_dir.path().join("test_g1"), false).unwrap();
        assert_eq!(graph.count_edges(), 2);

        let graph = Graph::load_from_file(tmp_work_dir.path().join("test_g2"), false).unwrap();
        assert_eq!(graph.count_edges(), 3);

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

        // Test overwrite false
        let res = load_graphs_from_path(tmp_work_dir.path(), tmp_graph_dir.path(), false).unwrap();
        assert_eq!(res, vec!["test_g2", "test_g1"]);

        let graph = Graph::load_from_file(tmp_work_dir.path().join("test_g2"), false).unwrap();
        assert_eq!(graph.count_edges(), 3);

        // Test overwrite true
        let res = load_graphs_from_path(tmp_work_dir.path(), tmp_graph_dir.path(), true).unwrap();
        assert_eq!(res, vec!["test_g2", "test_g1"]);

        let graph = Graph::load_from_file(tmp_work_dir.path().join("test_g2"), false).unwrap();
        assert_eq!(graph.count_edges(), 4);
    }

    #[test]
    #[cfg(feature = "storage")]
    fn test_load_disk_graphs_from_path() {
        let tmp_graph_dir = tempfile::tempdir().unwrap();
        let tmp_work_dir = tempfile::tempdir().unwrap();

        let graph = Graph::new();
        graph
            .add_edge(0, 1, 2, [("name", "test_e1")], None)
            .unwrap();
        graph
            .add_edge(0, 1, 3, [("name", "test_e2")], None)
            .unwrap();
        let graph_dir = tmp_graph_dir.path().join("test_dg1");
        let _ = DiskGraph::from_graph(&graph, &graph_dir).unwrap();

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
        let graph_dir = tmp_graph_dir.path().join("test_dg2");
        let _ = DiskGraph::from_graph(&graph, &graph_dir).unwrap();

        let res = load_graphs_from_path(tmp_work_dir.path(), tmp_graph_dir.path(), true).unwrap();
        assert_eq!(res, vec!["test_dg2", "test_dg1"]);

        let graph = DiskGraph::load_from_dir(tmp_work_dir.path().join("test_dg1")).unwrap();
        assert_eq!(graph.count_edges(), 2);

        let graph = DiskGraph::load_from_dir(tmp_work_dir.path().join("test_dg2")).unwrap();
        assert_eq!(graph.count_edges(), 3);

        // Test if graph is overwritten while load from path
        let graph = Graph::new();
        graph
            .add_constant_properties([("name", "test_dg2")])
            .unwrap();
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

        // You can't do this! Writing to a non-empty dir is not allowed when creating disk graph.
        // let graph_dir = tmp_graph_dir.path().join("test_dg2");
        // let _ = DiskGraph::from_graph(&graph, &graph_dir).unwrap();

        let tmp_graph_dir = tempfile::tempdir().unwrap();
        let graph_dir = tmp_graph_dir.path().join("test_dg2");
        let _ = DiskGraph::from_graph(&graph, &graph_dir).unwrap();

        let tmp_work_dir = tmp_work_dir.path();
        let graph = DiskGraph::load_from_dir(&graph_dir).unwrap();
        assert_eq!(graph.count_edges(), 4);

        // Test overwrite false
        let res = load_graphs_from_path(tmp_work_dir, tmp_graph_dir.path(), false).unwrap();
        assert_eq!(res, vec!["test_dg2"]);

        let graphs_paths = list_top_level_files_and_dirs(tmp_work_dir).unwrap();
        assert_eq!(graphs_paths, ["test_dg2", "test_dg1"]);

        let graph = DiskGraph::load_from_dir(tmp_work_dir.join("test_dg2")).unwrap();
        assert_eq!(graph.count_edges(), 3);

        // Test overwrite true
        let res = load_graphs_from_path(tmp_work_dir, tmp_graph_dir.path(), true).unwrap();
        assert_eq!(res, vec!["test_dg2"]);

        let graphs_paths = list_top_level_files_and_dirs(tmp_work_dir).unwrap();
        assert_eq!(graphs_paths, ["test_dg2", "test_dg1"]);

        let graph = DiskGraph::load_from_dir(tmp_work_dir.join("test_dg2")).unwrap();
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

        let res = load_graphs_from_paths(tmp_work_dir.path(), vec![graph_path1, graph_path2], true)
            .unwrap();
        assert_eq!(res, vec!["test_g1", "test_g2"]);

        let graph = Graph::load_from_file(tmp_work_dir.path().join("test_g1"), false).unwrap();
        assert_eq!(graph.count_edges(), 2);

        let graph = Graph::load_from_file(tmp_work_dir.path().join("test_g2"), false).unwrap();
        assert_eq!(graph.count_edges(), 3);
    }

    #[test]
    #[cfg(feature = "storage")]
    fn test_load_disk_graphs_from_paths() {
        let tmp_graph_dir = tempfile::tempdir().unwrap();
        let tmp_work_dir = tempfile::tempdir().unwrap();

        let graph = Graph::new();
        graph
            .add_edge(0, 1, 2, [("name", "test_e1")], None)
            .unwrap();
        graph
            .add_edge(0, 1, 3, [("name", "test_e2")], None)
            .unwrap();
        let graph_path1 = tmp_graph_dir.path().join("test_dg1");
        let _ = DiskGraph::from_graph(&graph, &graph_path1).unwrap();

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
        let graph_path2 = tmp_graph_dir.path().join("test_dg2");
        let _ = DiskGraph::from_graph(&graph, &graph_path2).unwrap();

        let res = load_graphs_from_paths(tmp_work_dir.path(), vec![graph_path1, graph_path2], true)
            .unwrap();
        assert_eq!(res, vec!["test_dg1", "test_dg2"]);

        let graph = DiskGraph::load_from_dir(tmp_work_dir.path().join("test_dg1")).unwrap();
        assert_eq!(graph.count_edges(), 2);

        let graph = DiskGraph::load_from_dir(tmp_work_dir.path().join("test_dg2")).unwrap();
        assert_eq!(graph.count_edges(), 3);
    }

    #[test]
    #[cfg(not(feature = "storage"))]
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

        // Dir path doesn't exists
        let res = get_graph_from_path(tmp_work_dir.path(), &tmp_graph_dir.path().join("test_dg1"))
            .unwrap();
        assert!(res.is_none());

        // Dir path exists but is not a disk graph path
        let tmp_graph_dir = tempfile::tempdir().unwrap();
        let res = get_graph_from_path(tmp_work_dir.path(), &tmp_graph_dir.path()).unwrap();
        assert!(res.is_none());

        // Dir path exists and is a disk graph path but storage feature is disabled
        let graph_path = tmp_graph_dir.path().join("test_dg");
        create_ipc_files_in_dir(&graph_path).unwrap();
        let res = get_graph_from_path(tmp_work_dir.path(), &graph_path).unwrap();
        assert!(res.is_none());
    }

    #[test]
    #[cfg(feature = "storage")]
    fn test_get_disk_graph_from_path() {
        let tmp_graph_dir = tempfile::tempdir().unwrap();
        let tmp_work_dir = tempfile::tempdir().unwrap();

        let graph = Graph::new();
        graph
            .add_edge(0, 1, 2, [("name", "test_e1")], None)
            .unwrap();
        graph
            .add_edge(0, 1, 3, [("name", "test_e2")], None)
            .unwrap();
        let graph_path = tmp_graph_dir.path().join("test_dg");
        let _ = DiskGraph::from_graph(&graph, &graph_path).unwrap();

        let res = get_graph_from_path(tmp_work_dir.path(), &graph_path).unwrap();

        assert!(res.is_some());
        let res = res.unwrap();
        assert_eq!(res.0, "test_dg");
        assert_eq!(res.1.graph.into_disk_graph().unwrap().count_edges(), 2);

        // Dir path doesn't exists
        let res = get_graph_from_path(tmp_work_dir.path(), &tmp_graph_dir.path().join("test_dg1"))
            .unwrap();
        assert!(res.is_none());

        // Dir path exists but is not a disk graph path
        let tmp_graph_dir = tempfile::tempdir().unwrap();
        let res = get_graph_from_path(tmp_work_dir.path(), &tmp_graph_dir.path()).unwrap();
        assert!(res.is_none());
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

        let res = load_graphs_from_path(tmp_work_dir.path(), tmp_graph_dir.path(), true).unwrap();
        assert_eq!(res, vec!["test_g2", "test_g1"]);

        let res = get_graphs_from_work_dir(tmp_work_dir.path()).unwrap();
        assert_eq!(res.len(), 2);
        let mut names = res.keys().collect_vec();
        names.sort();
        assert_eq!(names, vec!["test_g1", "test_g2"]);
    }

    #[test]
    #[cfg(feature = "storage")]
    fn test_get_disk_graphs_from_work_dir() {
        let tmp_graph_dir = tempfile::tempdir().unwrap();
        let tmp_work_dir = tempfile::tempdir().unwrap();

        let graph = Graph::new();
        graph
            .add_edge(0, 1, 2, [("name", "test_e1")], None)
            .unwrap();
        graph
            .add_edge(0, 1, 3, [("name", "test_e2")], None)
            .unwrap();
        let graph_path = tmp_graph_dir.path().join("test_dg1");
        let _ = DiskGraph::from_graph(&graph, &graph_path).unwrap();

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
        let graph_path = tmp_graph_dir.path().join("test_dg2");
        let _ = DiskGraph::from_graph(&graph, &graph_path).unwrap();

        let res = load_graphs_from_path(tmp_work_dir.path(), tmp_graph_dir.path(), true).unwrap();
        assert_eq!(res, vec!["test_dg2", "test_dg1"]);

        let res = get_graphs_from_work_dir(tmp_work_dir.path()).unwrap();
        assert_eq!(res.len(), 2);
        let mut names = res.keys().collect_vec();
        names.sort();
        assert_eq!(names, vec!["test_dg1", "test_dg2"]);
    }

    #[test]
    #[cfg(feature = "storage")]
    fn test_eviction() {
        let tmp_graph_dir = tempfile::tempdir().unwrap();
        let tmp_work_dir = tempfile::tempdir().unwrap();

        let graph = Graph::new();
        graph
            .add_edge(0, 1, 2, [("name", "test_e1")], None)
            .unwrap();
        graph
            .add_edge(0, 1, 3, [("name", "test_e2")], None)
            .unwrap();

        let graph_path1 = tmp_graph_dir.path().join("test_g");
        graph.save_to_file(&graph_path1).unwrap();

        let graph_path2 = tmp_graph_dir.path().join("test_dg");
        let _ = DiskGraph::from_graph(&graph, &graph_path2).unwrap();

        let graph_path3 = tmp_graph_dir.path().join("test_g2");
        graph.save_to_file(&graph_path3).unwrap();

        let data = Data::new(
            tmp_work_dir.path(),
            None,
            Some(vec![graph_path1, graph_path2, graph_path3]),
            Some(CacheConfig {
                capacity: 1,
                tti_seconds: 2,
            }),
        );

        assert!(!data.graphs.contains_key("test_dg"));
        assert!(!data.graphs.contains_key("test_g"));

        // Test size based eviction
        let _ = data.get_graph("test_dg");
        assert!(data.graphs.contains_key("test_dg"));
        assert!(!data.graphs.contains_key("test_g"));

        let _ = data.get_graph("test_g");
        // data.graphs.iter().for_each(|(k, _)| println!("{}", k));
        // assert!(!data.graphs.contains_key("test_dg"));
        assert!(data.graphs.contains_key("test_g"));

        thread::sleep(Duration::from_secs(3));
        assert!(!data.graphs.contains_key("test_dg"));
        assert!(!data.graphs.contains_key("test_g"));
    }
}
