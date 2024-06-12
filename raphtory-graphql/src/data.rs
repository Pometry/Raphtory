use crate::server_config::{load_config, CacheConfig};
use async_graphql::Error;
use dynamic_graphql::Result;
use moka::sync::{Cache, CacheBuilder};
#[cfg(feature = "storage")]
use raphtory::disk_graph::graph_impl::DiskGraph;
use raphtory::{
    core::Prop,
    db::api::view::MaterializedGraph,
    prelude::{GraphViewOps, PropUnwrap, PropertyAdditionOps},
    search::IndexedGraph,
    vectors::vectorised_graph::DynamicVectorisedGraph,
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
    pub(crate) vector_stores: Arc<Cache<String, DynamicVectorisedGraph>>,
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
            .time_to_live(std::time::Duration::from_secs(cache_configs.ttl_seconds))
            .time_to_idle(std::time::Duration::from_secs(cache_configs.tti_seconds));

        let vector_stores_cache_builder = CacheBuilder::new(cache_configs.capacity)
            .time_to_live(std::time::Duration::from_secs(cache_configs.ttl_seconds))
            .time_to_idle(std::time::Duration::from_secs(cache_configs.tti_seconds));

        let graphs_cache: Arc<Cache<String, IndexedGraph<MaterializedGraph>>> =
            Arc::new(graphs_cache_builder.build());
        let vector_stores_cache = Arc::new(vector_stores_cache_builder.build());

        save_graphs_to_work_dir(work_dir, &maybe_graphs.unwrap_or_default())
            .expect("Failed to save graphs to work dir");

        load_graphs_from_paths(work_dir, maybe_graph_paths.unwrap_or_default())
            .expect("Failed to save graph paths to work dir");

        Self {
            work_dir: work_dir.to_string_lossy().into_owned(),
            graphs: graphs_cache,
            vector_stores: vector_stores_cache,
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
                None => {
                    let result: Result<Option<IndexedGraph<MaterializedGraph>>, Error> =
                        if path.is_dir() {
                            println!("Disk Graph loaded = {}", path.display());
                            if is_disk_graph_dir(&path) {
                                let (_, graph) = load_disk_graph(&path)?;
                                Ok(Some(IndexedGraph::from_graph(&graph.into())?))
                            } else {
                                Ok(None)
                            }
                        } else {
                            println!("Graph loaded = {}", path.display());
                            let (_, graph) = load_bincode_graph(&path)?;
                            Ok(Some(IndexedGraph::from_graph(&graph.into())?))
                        };

                    match result? {
                        Some(graph) => Ok(Some(self.graphs.get_with(name, || graph))),
                        None => Ok(None),
                    }
                }
            }
        }
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
        if is_disk_graph_dir(&path) {
            load_disk_graph(&path)
        } else {
            Err(Error::from("Not a disk graph directory"))
        }
    } else {
        println!("Graph loaded = {}", path.display());
        load_bincode_graph(&path)
    };

    let (name, graph) = result?;
    let path = Path::new(work_dir).join(name.as_str());

    graph.save_to_file(path)?;

    Ok(name)
}

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

fn add_to_map(
    graphs: &mut HashMap<String, MaterializedGraph>,
    graph_name: &str,
    graph: MaterializedGraph,
) {
    if let Some(old_graph) = graphs.insert(graph_name.to_string(), graph) {
        let old_path = old_graph.properties().get("path").unwrap_str();
        let name = old_graph.properties().get("name").unwrap_str();
        panic!(
            "Graph with name {} defined multiple times, first file: {}, second file: {}",
            name, old_path, graph_name
        );
    }
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

fn load_bincode_graph(path: &Path) -> Result<(String, MaterializedGraph)> {
    let path_string = path.display().to_string();
    let graph = MaterializedGraph::load_from_file(path, false)?;
    let graph_name = get_graph_name(path, &graph);
    graph.update_constant_properties([("path".to_string(), Prop::str(path_string.clone()))])?;
    Ok((graph_name, graph))
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
