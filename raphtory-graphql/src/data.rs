use raphtory::{
    db::api::view::internal::{DynamicGraph, IntoDynamic},
    prelude::{Graph, GraphViewOps},
    search::IndexedGraph,
};
use std::{
    collections::{HashMap, HashSet},
    path::Path,
};
use walkdir::WalkDir;

pub(crate) struct Data {
    pub(crate) graphs: HashMap<String, IndexedGraph<DynamicGraph>>,
}

impl Data {
    pub fn from_map(graphs: HashMap<String, DynamicGraph>) -> Self {
        let graphs = Self::convert_graphs(graphs);
        Self { graphs }
    }

    pub fn from_directory(directory_path: &str) -> Self {
        let graphs = Self::load_from_file(directory_path);
        Self { graphs }
    }

    pub fn from_map_and_directory(
        graphs: HashMap<String, DynamicGraph>,
        directory_path: &str,
    ) -> Self {
        let graphs = Self::convert_graphs(graphs);
        let mut graphs_from_files = Self::load_from_file(directory_path);
        graphs_from_files.extend(graphs);
        Self {
            graphs: graphs_from_files,
        }
    }

    fn convert_graphs(
        graphs: HashMap<String, DynamicGraph>,
    ) -> HashMap<String, IndexedGraph<DynamicGraph>> {
        graphs
            .into_iter()
            .map(|(name, g)| {
                (
                    name,
                    IndexedGraph::from_graph(&g).expect("Unable to index graph"),
                )
            })
            .collect()
    }

    fn load_from_file(path: &str) -> HashMap<String, IndexedGraph<DynamicGraph>> {
        let mut valid_paths = HashSet::<String>::new();

        for entry in WalkDir::new(path).into_iter().filter_map(|e| e.ok()) {
            let path = entry.path();
            let path_string = path.display().to_string();
            let filename = path.file_name().and_then(|name| name.to_str());
            if let Some(filename) = filename {
                if path.is_file() && !filename.starts_with('.') {
                    valid_paths.insert(path_string);
                }
            }
        }

        let mut graphs_loaded: Vec<String> = vec![];
        let mut is_graph_already_loaded = |graph_name: String| {
            if graphs_loaded.contains(&graph_name) {
                panic!("Graph by name {} is already loaded", graph_name);
            } else {
                graphs_loaded.push(graph_name);
            }
        };

        let graphs: HashMap<String, IndexedGraph<DynamicGraph>> = valid_paths
            .into_iter()
            .map(|path| {
                println!("loading graph from {path}");
                let graph = Graph::load_from_file(&path).expect("Unable to load from graph");
                let maybe_graph_name = graph.properties().get("name");

                return match maybe_graph_name {
                    None => {
                        let graph_name = Path::new(&path).file_name().unwrap().to_str().unwrap();
                        is_graph_already_loaded(graph_name.to_string());
                        (graph_name.to_string(), graph)
                    }
                    Some(graph_name) => {
                        is_graph_already_loaded(graph_name.to_string());
                        (graph_name.to_string(), graph)
                    }
                };
            })
            .map(|(name, g)| {
                (
                    name,
                    IndexedGraph::from_graph(&g.into_dynamic()).expect("Unable to index graph"),
                )
            })
            .collect();
        graphs
    }
}
