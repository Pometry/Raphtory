use itertools::Itertools;
use parking_lot::RwLock;
use raphtory::{
    core::Prop,
    db::{
        api::view::internal::DynamicGraph,
        graph::{edge::EdgeView, vertex::VertexView},
    },
    prelude::{Graph, GraphViewOps, PropertyAdditionOps},
    search::IndexedGraph,
    vectors::{
        document_template::DocumentTemplate, vectorized_graph::VectorizedGraph, DocumentInput,
    },
};
use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::Arc,
};
use walkdir::WalkDir;
pub(crate) type DynamicTemplate = Arc<dyn DocumentTemplate<Graph>>;

#[derive(Default)]
pub(crate) struct Data {
    pub(crate) graphs: RwLock<HashMap<String, IndexedGraph<Graph>>>,
    pub(crate) vector_stores: RwLock<HashMap<String, VectorizedGraph<Graph, DynamicTemplate>>>,
}

impl Data {
    pub fn from_map(graphs: HashMap<String, Graph>) -> Self {
        let graphs = RwLock::new(Self::convert_graphs(graphs));
        let vector_stores = RwLock::new(HashMap::new());
        Self {
            graphs,
            vector_stores,
        }
    }

    pub fn from_directory(directory_path: &str) -> Self {
        let graphs = RwLock::new(Self::load_from_file(directory_path));
        let vector_stores = RwLock::new(HashMap::new());
        Self {
            graphs,
            vector_stores,
        }
    }

    pub fn from_map_and_directory(graphs: HashMap<String, Graph>, directory_path: &str) -> Self {
        let mut graphs = Self::convert_graphs(graphs);
        graphs.extend(Self::load_from_file(directory_path));
        let graphs = RwLock::new(graphs);
        let vector_stores = RwLock::new(HashMap::new());
        Self {
            graphs,
            vector_stores,
        }
    }

    fn convert_graphs(graphs: HashMap<String, Graph>) -> HashMap<String, IndexedGraph<Graph>> {
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

    pub fn load_from_file(path: &str) -> HashMap<String, IndexedGraph<Graph>> {
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

        let graphs: HashMap<String, IndexedGraph<Graph>> = valid_paths
            .into_iter()
            .map(|path| {
                println!("loading graph from {path}");
                let graph = Graph::load_from_file(&path).expect("Unable to load from graph");
                graph
                    .add_constant_properties([("path".to_string(), Prop::str(path.clone()))])
                    .expect("Failed to add static property");
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
                    IndexedGraph::from_graph(&g).expect("Unable to index graph"),
                )
            })
            .collect();
        graphs
    }
}
