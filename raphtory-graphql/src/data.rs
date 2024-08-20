use crate::{model::algorithms::global_plugins::GlobalPlugins, server_config::AppConfig};
use moka::sync::Cache;
#[cfg(feature = "storage")]
use raphtory::disk_graph::DiskGraphStorage;
use raphtory::{
    core::utils::errors::{
        GraphError, InvalidPathReason,
        InvalidPathReason::{
            BackslashError, CurDirNotAllowed, DoubleForwardSlash, ParentDirNotAllowed,
            PathDoesNotExist, PathIsDirectory, PathNotParsable, PathNotUTF8, RootNotAllowed,
            SymlinkNotAllowed,
        },
    },
    db::api::view::MaterializedGraph,
    search::IndexedGraph,
};
use std::{
    collections::HashMap,
    fs,
    path::{Component, Path, PathBuf, StripPrefixError},
    sync::Arc,
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

    pub fn get_graph(
        &self,
        path: &Path,
    ) -> Result<IndexedGraph<MaterializedGraph>, Arc<GraphError>> {
        let full_path = self
            .construct_graph_full_path(path)
            .map_err(|e| GraphError::from(e))?;
        if !full_path.exists() {
            return Err(GraphError::GraphNotFound(path.to_path_buf()).into());
        } else {
            self.graphs
                .try_get_with(path.to_path_buf(), || get_graph_from_path(&full_path))
        }
    }

    pub fn get_graph_names_paths(&self) -> Result<Vec<PathBuf>, GraphError> {
        let mut paths = vec![];
        for path in self.get_graph_paths() {
            //TODO don't load the graphs here
            let _ = get_graph_from_path(&path)?;
            match self.get_relative_path(&path) {
                Ok(p) => paths.push(p),
                Err(_) => return Err(GraphError::from(InvalidPathReason::StripPrefixError(path))), //Possibly just ignore but log?
            }
        }
        Ok(paths)
    }

    pub fn get_graphs_from_work_dir(
        &self,
    ) -> Result<HashMap<String, IndexedGraph<MaterializedGraph>>, GraphError> {
        let mut graphs = HashMap::new();
        for path in self.get_graph_paths() {
            let graph = get_graph_from_path(&path)?;
            graphs.insert(
                self.get_relative_path(&path)
                    .map_err(|_| GraphError::from(InvalidPathReason::StripPrefixError(path)))?
                    .display()
                    .to_string(),
                graph,
            );
        }
        Ok(graphs)
    }

    fn get_graph_paths(&self) -> Vec<PathBuf> {
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
        traverse_directory(&self.work_dir, &mut paths);
        paths
    }

    fn get_relative_path(&self, path: &PathBuf) -> Result<PathBuf, StripPrefixError> {
        Ok(path.strip_prefix(&self.work_dir)?.to_path_buf())
    }

    pub fn construct_graph_full_path(&self, path: &Path) -> Result<PathBuf, InvalidPathReason> {
        // check for errors in the path
        //additionally ban any backslash
        let path_str = match path.to_str() {
            None => {
                return Err(PathNotUTF8(path.to_path_buf()));
            }
            Some(str) => str,
        };
        if path_str.contains(r"\") {
            return Err(BackslashError(path.to_path_buf()));
        }
        if path_str.contains(r"//") {
            return Err(DoubleForwardSlash(path.to_path_buf()));
        }

        let mut full_path = self.work_dir.to_path_buf();
        // fail if any component is a Prefix (C://), tries to access root,
        // tries to access a parent dir or is a symlink which could break out of the working dir
        for component in path.components() {
            match component {
                Component::Prefix(_) => return Err(RootNotAllowed(path.to_path_buf())),
                Component::RootDir => return Err(RootNotAllowed(path.to_path_buf())),
                Component::CurDir => return Err(CurDirNotAllowed(path.to_path_buf())),
                Component::ParentDir => return Err(ParentDirNotAllowed(path.to_path_buf())),
                Component::Normal(component) => {
                    //check for symlinks
                    full_path.push(component);
                    if full_path.is_symlink() {
                        return Err(SymlinkNotAllowed(path.to_path_buf()));
                    }
                }
            }
        }
        return Ok(full_path);
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

fn get_graph_from_path(path: &Path) -> Result<IndexedGraph<MaterializedGraph>, GraphError> {
    if !path.exists() {
        return Err(PathDoesNotExist(path.to_path_buf()).into());
    }
    if path.is_dir() {
        if is_disk_graph_dir(path) {
            get_disk_graph_from_path(path)?.ok_or(GraphError::DiskGraphNotFound.into())
        } else {
            return Err(PathIsDirectory(path.to_path_buf()).into());
        }
    } else {
        let graph = load_bincode_graph(path)?;
        println!("Graph loaded = {}", path.display());
        Ok(IndexedGraph::from_graph(&graph.into())?)
    }
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

pub(crate) fn get_graph_name(path: &Path) -> Result<String, GraphError> {
    match path.file_stem() {
        None => {
            let last_component: Component = path
                .components()
                .last()
                .ok_or_else(|| GraphError::from(PathNotParsable(path.to_path_buf())))?;
            match last_component {
                Component::Prefix(_) => Err(GraphError::from(PathNotParsable(path.to_path_buf()))),
                Component::RootDir => Err(GraphError::from(PathNotParsable(path.to_path_buf()))),
                Component::CurDir => Err(GraphError::from(PathNotParsable(path.to_path_buf()))),
                Component::ParentDir => Err(GraphError::from(PathNotParsable(path.to_path_buf()))),
                Component::Normal(value) => value
                    .to_str()
                    .map(|s| s.to_string())
                    .ok_or(GraphError::from(PathNotParsable(path.to_path_buf()))),
            }
        }
        Some(value) => value
            .to_str()
            .map(|s| s.to_string())
            .ok_or(GraphError::from(PathNotParsable(path.to_path_buf()))),
    } //should not happen, but means we always get a name
}

pub(crate) fn load_bincode_graph(path: &Path) -> Result<MaterializedGraph, GraphError> {
    let graph = MaterializedGraph::load_from_file(path, false)?;
    Ok(graph)
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
        data::{get_graph_from_path, Data},
        server_config::{AppConfig, AppConfigBuilder},
    };
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
        db::api::storage::storage_ops::GraphStorage, db::api::view::internal::CoreGraphOps,
        disk_graph::DiskGraphStorage,
    };
    #[cfg(feature = "storage")]
    use std::{thread, time::Duration};

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

    pub(crate) fn save_graphs_to_work_dir(
        work_dir: &Path,
        graphs: &HashMap<String, MaterializedGraph>,
    ) -> Result<(), GraphError> {
        for (name, graph) in graphs.into_iter() {
            let data = Data::new(work_dir, &AppConfig::default());
            let full_path = data.construct_graph_full_path(Path::new(name))?;

            #[cfg(feature = "storage")]
            if let GraphStorage::Disk(dg) = graph.core_graph() {
                let disk_graph_path = dg.graph_dir();
                #[cfg(feature = "storage")]
                copy_dir_recursive(disk_graph_path, &full_path)?;
            } else {
                graph.save_to_path(&full_path)?;
            }

            #[cfg(not(feature = "storage"))]
            {
                graph.save_to_path(&full_path)?;
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

        let res = get_graph_from_path(&graph_path).unwrap();
        assert_eq!(res.graph.into_events().unwrap().count_edges(), 2);

        // Dir path doesn't exists
        let res = get_graph_from_path(&tmp_graph_dir.path().join("test_dg1"));
        assert!(res.is_err());
        if let Err(err) = res {
            assert!(err.to_string().contains("Invalid path"));
        }

        // Dir path exists but is not a disk graph path
        let tmp_graph_dir = tempfile::tempdir().unwrap();
        let res = get_graph_from_path(&tmp_graph_dir.path());
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

        graph
            .save_to_file(&tmp_work_dir.path().join("test_g"))
            .unwrap();
        let _ = DiskGraphStorage::from_graph(&graph, &tmp_work_dir.path().join("test_dg")).unwrap();
        graph
            .save_to_file(&tmp_work_dir.path().join("test_g2"))
            .unwrap();

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

    #[cfg(any(target_os = "macos", target_os = "linux"))]
    #[test]
    fn test_invalid_utf8_failure() {
        use std::{ffi::OsString, os::unix::ffi::OsStringExt};
        let invalid_bytes = vec![0xFF, 0xFE, b'/', b'p', b'a', b't', b'h'];
        let string = OsString::from_vec(invalid_bytes);
        let invalid_path = Path::new(&string);
        let data = Data::new(&tempfile::tempdir().unwrap().path(), &AppConfig::default());
        match data.construct_graph_full_path(invalid_path) {
            Err(e) => match e {
                InvalidPathReason::PathNotUTF8(_) => {}
                _ => panic!("Expected InvalidPathReason::PathNotUTF8, but got something else"),
            },
            Ok(_) => {
                panic!("Expected InvalidPathReason::PathNotUTF8, but got something else")
            }
        }
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn test_invalid_utf16_failure() {
        use std::{ffi::OsString, os::windows::ffi::OsStringExt, path::Path};

        let invalid_utf16: Vec<u16> = vec![0xD800, 0xD801]; // Invalid UTF-16 data
        let string = OsString::from_wide(&invalid_utf16);
        let invalid_path = Path::new(&string);
        let data = Data::new(&tempfile::tempdir().unwrap().path(), &AppConfig::default());
        match data.construct_graph_full_path(invalid_path) {
            Err(e) => match e {
                InvalidPathReason::PathNotUTF8(_) => {}
                _ => panic!("Expected InvalidPathReason::PathNotUTF8, but got something else"),
            },
            Ok(_) => {
                panic!("Expected InvalidPathReason::PathNotUTF8, but got something else")
            }
        }
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

        let paths = data.get_graph_paths();

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
