use crate::{
    model::{
        algorithms::global_plugins::GlobalPlugins, construct_graph_full_path,
        create_dirs_if_not_present,
    },
    server_config::AppConfig,
};
use async_graphql::Error;
use dynamic_graphql::Result;
use moka::sync::Cache;
#[cfg(feature = "storage")]
use raphtory::disk_graph::DiskGraphStorage;
use raphtory::{
    core::utils::errors::GraphError,
    db::api::{
        storage::storage_ops::GraphStorage,
        view::{internal::CoreGraphOps, MaterializedGraph},
    },
    search::IndexedGraph,
};
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};
use walkdir::WalkDir;

pub struct Data {
    pub(crate) work_dir: PathBuf,
    pub(crate) graphs: Cache<PathBuf, IndexedGraph<MaterializedGraph>>,
    pub(crate) global_plugins: GlobalPlugins,
}

impl Data {
    pub fn new(work_dir: &Path, configs: &AppConfig) -> Self {
        let cache_configs = &configs.cache;

        let graphs_cache_builder = Cache::builder()
            .max_capacity(cache_configs.capacity)
            .time_to_idle(std::time::Duration::from_secs(cache_configs.tti_seconds))
            .build();

        let graphs_cache: Cache<PathBuf, IndexedGraph<MaterializedGraph>> = graphs_cache_builder;

        Self {
            work_dir: work_dir.to_path_buf(),
            graphs: graphs_cache,
            global_plugins: GlobalPlugins::default(),
        }
    }

    pub fn get_graph(&self, path: &Path) -> Result<IndexedGraph<MaterializedGraph>> {
        let full_path = construct_graph_full_path(&self.work_dir, path)?;
        if !full_path.exists() {
            return Err(GraphError::GraphNotFound(path.to_path_buf()).into());
        } else {
            match self.graphs.get(&path.to_path_buf()) {
                Some(graph) => Ok(graph.clone()),
                None => {
                    let (_, graph) = get_graph_from_path(&full_path)?;
                    Ok(self.graphs.get_with(path.to_path_buf(), || graph))
                }
            }
        }
    }

    pub fn get_graph_names_paths(&self) -> Result<(Vec<String>, Vec<PathBuf>)> {
        let mut names = vec![];
        let mut paths = vec![];
        for path in get_graph_paths(&self.work_dir) {
            let (name, _) = get_graph_from_path(&path)?;
            names.push(name);
            paths.push(get_relative_path(&self.work_dir, path)?);
        }

        Ok((names, paths))
    }

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

#[cfg(feature = "storage")]
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
    path_on_server: &Path,
    target_path: &Path,
    overwrite: bool,
) -> Result<Option<PathBuf>> {
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

#[cfg(not(feature = "storage"))]
fn load_disk_graph_from_path(
    _path_on_server: &Path,
    _target_path: &Path,
    _overwrite: bool,
) -> Result<Option<PathBuf>> {
    Ok(None)
}

pub fn load_graph_from_path(
    work_dir: &Path,
    path_on_server: &Path,
    namespace: &Option<String>,
    overwrite: bool,
) -> Result<PathBuf> {
    if !path_on_server.exists() {
        return Err(GraphError::InvalidPath(path_on_server.to_path_buf()).into());
    }
    println!("Loading graph from {}", path_on_server.display());
    let target_path = get_target_path(work_dir, path_on_server, namespace)?;
    if path_on_server.is_dir() {
        if is_disk_graph_dir(&path_on_server) {
            load_disk_graph_from_path(&path_on_server, &target_path, overwrite)?
                .ok_or(GraphError::DiskGraphNotFound.into())
        } else {
            Err(GraphError::InvalidPath(path_on_server.to_path_buf()).into())
        }
    } else {
        let (_, graph) = load_bincode_graph(&path_on_server)?;
        if target_path.exists() {
            if overwrite {
                fs::remove_file(&target_path)?;
                graph.save_to_file(&target_path)?;
                println!("Graph loaded = {}", target_path.display());
            } else {
                return Err(GraphError::GraphNameAlreadyExists(target_path.to_path_buf()).into());
            }
        } else {
            create_dirs_if_not_present(&target_path)?;
            graph.save_to_file(&target_path)?;
            println!("Graph loaded = {}", target_path.display());
        }
        Ok(target_path)
    }
}

fn get_target_path(work_dir: &Path, path: &Path, namespace: &Option<String>) -> Result<PathBuf> {
    let graph_name = get_graph_name(path)?;
    let target_dir = if let Some(namespace) = namespace {
        construct_graph_full_path(&work_dir, &Path::new(namespace).join(graph_name))?
    } else {
        construct_graph_full_path(&work_dir, Path::new(&graph_name))?
    };
    Ok(target_dir)
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
    _path: &Path,
) -> Result<Option<(String, IndexedGraph<MaterializedGraph>)>, Error> {
    Ok(None)
}

fn get_graph_from_path(path: &Path) -> Result<(String, IndexedGraph<MaterializedGraph>), Error> {
    if !path.exists() {
        return Err(GraphError::InvalidPath(path.to_path_buf()).into());
    }
    if path.is_dir() {
        if is_disk_graph_dir(path) {
            get_disk_graph_from_path(path)?.ok_or(GraphError::DiskGraphNotFound.into())
        } else {
            return Err(GraphError::InvalidPath(path.to_path_buf()).into());
        }
    } else {
        let (name, graph) = load_bincode_graph(path)?;
        println!("Graph loaded = {}", path.display());
        Ok((name, IndexedGraph::from_graph(&graph.into())?))
    }
}

// We are loading all the graphs in the work dir for vectorized APIs
pub(crate) fn get_graphs_from_work_dir(
    work_dir: &Path,
) -> Result<HashMap<String, IndexedGraph<MaterializedGraph>>> {
    let mut graphs = HashMap::new();
    for path in get_graph_paths(work_dir) {
        let (name, graph) = get_graph_from_path(&path)?;
        graphs.insert(name, graph);
    }
    Ok(graphs)
}

fn get_relative_path(work_dir: &Path, path: PathBuf) -> Result<PathBuf> {
    Ok(path.strip_prefix(work_dir)?.to_path_buf())
}

fn get_graph_paths(work_dir: &Path) -> Vec<PathBuf> {
    fn traverse_directory(dir: &Path, paths: &mut Vec<PathBuf>) {
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries {
                if let Ok(entry) = entry {
                    let path = entry.path();
                    if path.is_dir() {
                        if is_disk_graph_dir(&path) {
                            paths.push(path);
                        } else {
                            traverse_directory(&path, paths);
                        }
                    } else if path.is_file() {
                        paths.push(path);
                    }
                }
            }
        }
    }

    let mut paths = Vec::new();
    traverse_directory(work_dir, &mut paths);
    paths
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

pub(crate) fn get_graph_name(path: &Path) -> Result<String, &'static str> {
    path.file_name()
        .and_then(|os_str| os_str.to_str())
        .map(|str_slice| str_slice.to_string())
        .ok_or("No file name found in the path or invalid UTF-8")
}

#[cfg(test)]
pub(crate) fn save_graphs_to_work_dir(
    work_dir: &Path,
    graphs: &HashMap<String, MaterializedGraph>,
) -> Result<()> {
    for (name, graph) in graphs.into_iter() {
        let full_path = construct_graph_full_path(&work_dir, Path::new(name))?;
        if let GraphStorage::Disk(dg) = graph.core_graph() {
            let disk_graph_path = dg.graph_dir();
            #[cfg(feature = "storage")]
            copy_dir_recursive(disk_graph_path, &full_path)?;
        } else {
            graph.save_to_path(&full_path)?;
        }
    }
    Ok(())
}

pub(crate) fn load_bincode_graph(path: &Path) -> Result<(String, MaterializedGraph)> {
    let graph = MaterializedGraph::load_from_file(path, false)?;
    let name = get_graph_name(path)?;
    Ok((name, graph))
}

#[cfg(feature = "storage")]
fn load_disk_graph(path: &Path) -> Result<(String, MaterializedGraph)> {
    let disk_graph = DiskGraphStorage::load_from_dir(path)?;
    let graph: MaterializedGraph = disk_graph.into_graph().into(); // TODO: We currently have no way to identify disk graphs as MaterializedGraphs
    let graph_name = get_graph_name(path)?;
    Ok((graph_name, graph))
}

#[allow(unused_variables)]
#[cfg(not(feature = "storage"))]
fn _load_disk_graph(_path: &Path) -> Result<(String, MaterializedGraph)> {
    unimplemented!("Storage feature not enabled, cannot load from disk graph")
}

#[cfg(test)]
mod data_tests {
    use crate::{
        data::{
            get_graph_from_path, get_graph_paths, load_graph_from_path, save_graphs_to_work_dir,
            Data,
        },
        server_config::AppConfigBuilder,
    };
    #[cfg(feature = "storage")]
    use raphtory::disk_graph::DiskGraphStorage;
    use raphtory::{
        db::api::view::MaterializedGraph,
        prelude::{PropertyAdditionOps, *},
    };
    use std::{
        collections::HashMap,
        fs,
        fs::File,
        io,
        path::{Path, PathBuf},
        thread,
        time::Duration,
    };

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

    #[allow(dead_code)]
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
            File::create(file_path)?;
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

        let res = load_graph_from_path(tmp_work_dir.path(), &graph_path, &None, true).unwrap();
        assert_eq!(res, tmp_work_dir.path().join("test_g"));

        let graph = Graph::load_from_file(tmp_work_dir.path().join("test_g"), false).unwrap();
        assert_eq!(graph.count_edges(), 2);

        // Test directory path doesn't exist
        let result = std::panic::catch_unwind(|| {
            load_graph_from_path(
                tmp_work_dir.path(),
                &tmp_graph_dir.path().join("test_dg1"),
                &None,
                true,
            )
            .unwrap();
        });

        // Assert that it panicked with the expected message
        assert!(result.is_err());
        if let Err(err) = result {
            let panic_message = err
                .downcast_ref::<String>()
                .expect("Expected a String panic message");
            assert!(
                panic_message.contains("Invalid path:"),
                "Unexpected panic message: {}",
                panic_message
            );
            assert!(
                panic_message.contains("test_dg1"),
                "Unexpected panic message: {}",
                panic_message
            );
        }

        // Dir path exists but is not a disk graph path
        let result = std::panic::catch_unwind(|| {
            load_graph_from_path(tmp_work_dir.path(), &tmp_graph_dir.path(), &None, true).unwrap();
        });

        // Assert that it panicked with the expected message
        assert!(result.is_err());
        if let Err(err) = result {
            let panic_message = err
                .downcast_ref::<String>()
                .expect("Expected a String panic message");
            assert!(
                panic_message.contains("Invalid path:"),
                "Unexpected panic message: {}",
                panic_message
            );
        }

        // Dir path exists and is a disk graph path but storage feature is disabled
        let graph_path = tmp_graph_dir.path().join("test_dg");
        create_ipc_files_in_dir(&graph_path).unwrap();
        let res = load_graph_from_path(tmp_work_dir.path(), &graph_path, &None, true);
        assert!(res.is_err());
        if let Err(err) = res {
            assert!(err.message.contains("Disk graph not found"));
        }
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
        let _ = DiskGraphStorage::from_graph(&graph, &graph_dir).unwrap();

        let res = load_graph_from_path(tmp_work_dir.path(), &graph_dir, &None, true)
            .unwrap()
            .display()
            .to_string();
        assert_eq!(res, "test_dg");

        let graph = DiskGraphStorage::load_from_dir(tmp_work_dir.path().join("test_dg"))
            .unwrap()
            .into_graph();
        assert_eq!(graph.count_edges(), 2);
    }

    #[test]
    #[cfg(not(feature = "storage"))]
    fn test_get_graph_from_path() {
        let tmp_graph_dir = tempfile::tempdir().unwrap();

        let graph = Graph::new();
        graph
            .add_edge(0, 1, 2, [("name", "test_e1")], None)
            .unwrap();
        graph
            .add_edge(0, 1, 3, [("name", "test_e2")], None)
            .unwrap();
        let graph_path = tmp_graph_dir.path().join("test_g1");
        graph.save_to_file(&graph_path).unwrap();

        let res = get_graph_from_path(&graph_path).unwrap();
        assert_eq!(res.0, "test_g1");
        assert_eq!(res.1.graph.into_events().unwrap().count_edges(), 2);

        let res = get_graph_from_path(&tmp_graph_dir.path().join("test_g2"));
        assert!(res.is_err());
        if let Err(err) = res {
            assert!(err.message.contains("Invalid path"));
        }

        // Dir path doesn't exists
        let res = get_graph_from_path(&tmp_graph_dir.path().join("test_dg1"));
        assert!(res.is_err());
        if let Err(err) = res {
            assert!(err.message.contains("Invalid path"));
        }

        // Dir path exists but is not a disk graph path
        let tmp_graph_dir = tempfile::tempdir().unwrap();
        let res = get_graph_from_path(&tmp_graph_dir.path());
        assert!(res.is_err());
        if let Err(err) = res {
            assert!(err.message.contains("Invalid path"));
        }

        // Dir path exists and is a disk graph path but storage feature is disabled
        let graph_path = tmp_graph_dir.path().join("test_dg");
        create_ipc_files_in_dir(&graph_path).unwrap();
        let res = get_graph_from_path(&graph_path);
        assert!(res.is_err());
        if let Err(err) = res {
            assert!(err.message.contains("Disk graph not found"));
        }
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
        let _ = DiskGraphStorage::from_graph(&graph, &graph_path).unwrap();

        let res = get_graph_from_path(&graph_path).unwrap();
        assert_eq!(res.0, "test_dg");
        assert_eq!(res.1.graph.into_events().unwrap().count_edges(), 2);

        // Dir path doesn't exists
        let res = get_graph_from_path(&tmp_graph_dir.path().join("test_dg1"));
        assert!(res.is_err());
        if let Err(err) = res {
            assert!(err.message.contains("Invalid path"));
        }

        // Dir path exists but is not a disk graph path
        let tmp_graph_dir = tempfile::tempdir().unwrap();
        let res = get_graph_from_path(&tmp_graph_dir.path());
        assert!(res.is_err());
        if let Err(err) = res {
            assert!(err.message.contains("Invalid path"));
        }
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

        let graphs = list_top_level_files_and_dirs(&tmp_work_dir.path()).unwrap();
        assert_eq!(graphs, vec!["test_g", "test_dg"]);
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
        let _ = DiskGraphStorage::from_graph(&graph, &graph_path2).unwrap();

        let graph_path3 = tmp_graph_dir.path().join("test_g2");
        graph.save_to_file(&graph_path3).unwrap();

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

        let paths = get_graph_paths(work_dir);

        assert_eq!(paths.len(), 5);
        assert!(paths.contains(&g0_path));
        assert!(paths.contains(&g1_path));
        assert!(paths.contains(&g2_path));
        assert!(paths.contains(&g3_path));
        assert!(paths.contains(&g4_path));
        assert!(!paths.contains(&g5_path)); // Empty dir are ignored

        assert_eq!(get_maybe_relative_path(work_dir, g0_path), None);
        assert_eq!(get_maybe_relative_path(work_dir, g1_path), None);
        assert_eq!(
            get_maybe_relative_path(work_dir, g2_path),
            Some("shivam/investigations/2024-12-22".to_string())
        );
        assert_eq!(
            get_maybe_relative_path(work_dir, g3_path),
            Some("shivam/investigations".to_string())
        );
        assert_eq!(
            get_maybe_relative_path(work_dir, g4_path),
            Some("shivam/investigations".to_string())
        );
    }
}
