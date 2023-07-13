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
    pub fn load(directory_path: &str) -> Self {
        let mut valid_paths = HashSet::<String>::new();

        for entry in WalkDir::new(directory_path)
            .into_iter()
            .filter_map(|e| e.ok())
        {
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

        Self { graphs }
    }
}
