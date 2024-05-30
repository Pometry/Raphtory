use parking_lot::RwLock;
use raphtory::{
    arrow::graph_impl::ArrowGraph,
    core::Prop,
    db::api::view::MaterializedGraph,
    prelude::{GraphViewOps, PropUnwrap, PropertyAdditionOps},
    search::IndexedGraph,
    vectors::vectorised_graph::DynamicVectorisedGraph,
};
use std::{collections::HashMap, fs, path::Path, sync::Arc};
use walkdir::WalkDir;

#[derive(Default)]
pub struct Data {
    pub(crate) graphs: Arc<RwLock<HashMap<String, IndexedGraph<MaterializedGraph>>>>,
    pub(crate) vector_stores: Arc<RwLock<HashMap<String, DynamicVectorisedGraph>>>,
}

impl Data {
    pub fn from_map<G: Into<MaterializedGraph>>(graphs: HashMap<String, G>) -> Self {
        let graphs = Arc::new(RwLock::new(Self::convert_graphs(graphs)));
        let vector_stores = Arc::new(RwLock::new(HashMap::new()));
        Self {
            graphs,
            vector_stores,
        }
    }

    pub fn from_directory(directory_path: &str) -> Self {
        let graphs = Arc::new(RwLock::new(Self::load_from_file(directory_path)));
        let vector_stores = Arc::new(RwLock::new(HashMap::new()));
        Self {
            graphs,
            vector_stores,
        }
    }

    pub fn from_map_and_directory<G: Into<MaterializedGraph>>(
        graphs: HashMap<String, G>,
        directory_path: &str,
    ) -> Self {
        let mut graphs = Self::convert_graphs(graphs);
        graphs.extend(Self::load_from_file(directory_path));
        let graphs = Arc::new(RwLock::new(graphs));
        let vector_stores = Arc::new(RwLock::new(HashMap::new()));
        Self {
            graphs,
            vector_stores,
        }
    }

    fn convert_graphs<G: Into<MaterializedGraph>>(
        graphs: HashMap<String, G>,
    ) -> HashMap<String, IndexedGraph<MaterializedGraph>> {
        graphs
            .into_iter()
            .map(|(name, g)| {
                (
                    name,
                    IndexedGraph::from_graph(&g.into()).expect("Unable to index graph"),
                )
            })
            .collect()
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

    pub fn load_from_file(path: &str) -> HashMap<String, IndexedGraph<MaterializedGraph>> {
        fn get_graph_name(path: &Path, graph: &MaterializedGraph) -> String {
            graph
                .properties()
                .get("name")
                .into_str()
                .map(|v| v.to_string())
                .unwrap_or_else(|| path.file_name().unwrap().to_str().unwrap().to_owned())
        }

        fn is_arrow_graph_dir(path: &Path) -> bool {
            // Check if the directory contains files specific to arrow graphs
            let files = fs::read_dir(path).unwrap();
            let mut has_arrow_files = false;
            for file in files {
                let file_name = file.unwrap().file_name().into_string().unwrap();
                if file_name.ends_with(".ipc") {
                    has_arrow_files = true;
                    break;
                }
            }
            has_arrow_files
        }

        fn load_bincode_graph(path: &Path) -> (String, MaterializedGraph) {
            let path_string = path.display().to_string();
            let graph =
                MaterializedGraph::load_from_file(path, false).expect("Unable to load from graph");
            let graph_name = get_graph_name(path, &graph);
            graph
                .update_constant_properties([("path".to_string(), Prop::str(path_string.clone()))])
                .expect("Failed to add static property");

            (graph_name, graph)
        }

        fn load_arrow_graph(path: &Path) -> (String, MaterializedGraph) {
            let arrow_graph =
                ArrowGraph::load_from_dir(path).expect("Unable to load from arrow graph");
            let graph: MaterializedGraph = arrow_graph.into();
            let graph_name = get_graph_name(path, &graph);

            (graph_name, graph)
        }

        fn add_to_graphs(
            graphs: &mut HashMap<String, IndexedGraph<MaterializedGraph>>,
            graph_name: &str,
            graph: &MaterializedGraph,
        ) {
            if let Some(old_graph) = graphs.insert(
                graph_name.to_string(),
                IndexedGraph::from_graph(graph).expect("Unable to index graph"),
            ) {
                let old_path = old_graph.properties().get("path").unwrap_str();
                let name = old_graph.properties().get("name").unwrap_str();
                panic!(
                    "Graph with name {} defined multiple times, first file: {}, second file: {}",
                    name, old_path, graph_name
                );
            }
        }

        let mut graphs: HashMap<String, IndexedGraph<MaterializedGraph>> = HashMap::default();

        for entry in fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                println!("Arrow Graph loaded = {}", path.display());
                if is_arrow_graph_dir(&path) {
                    if let (graph_name, graph) = load_arrow_graph(&path) {
                        add_to_graphs(&mut graphs, &graph_name, &graph);
                    }
                }
            } else {
                println!("Graph loaded = {}", path.display());
                if let (graph_name, graph) = load_bincode_graph(&path) {
                    add_to_graphs(&mut graphs, &graph_name, &graph);
                }
            }
        }

        graphs
    }
}
