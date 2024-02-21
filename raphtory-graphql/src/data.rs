use parking_lot::RwLock;
use raphtory::{
    core::Prop,
    db::api::view::MaterializedGraph,
    prelude::{GraphViewOps, PropUnwrap, PropertyAdditionOps},
    search::IndexedGraph,
    vectors::vectorised_graph::DynamicVectorisedGraph,
};
use std::{collections::HashMap, path::Path, sync::Arc};
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

    pub fn from_directory(directory_path: &str, force: bool) -> Self {
        let graphs = Arc::new(RwLock::new(Self::load_from_file(directory_path, force)));
        let vector_stores = Arc::new(RwLock::new(HashMap::new()));
        Self {
            graphs,
            vector_stores,
        }
    }

    pub fn from_map_and_directory<G: Into<MaterializedGraph>>(
        graphs: HashMap<String, G>,
        directory_path: &str,
        force_graph_load: bool,
    ) -> Self {
        let mut graphs = Self::convert_graphs(graphs);
        graphs.extend(Self::load_from_file(directory_path, force_graph_load));
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

    pub fn load_from_file(
        path: &str,
        force: bool,
    ) -> HashMap<String, IndexedGraph<MaterializedGraph>> {
        let valid_entries = WalkDir::new(path).into_iter().filter_map(|e| {
            let entry = e.ok()?;
            let path = entry.path();
            let filename = path.file_name().and_then(|name| name.to_str())?;
            (path.is_file() && !filename.starts_with('.')).then_some(entry)
        });

        let mut graphs: HashMap<String, IndexedGraph<MaterializedGraph>> = HashMap::default();

        for entry in valid_entries {
            let path = entry.path();
            let path_string = path.display().to_string();
            println!("loading graph from {path_string}");
            let graph =
                MaterializedGraph::load_from_file(path, force).expect("Unable to load from graph");
            let graph_name = graph
                .properties()
                .get("name")
                .into_str()
                .map(|v| v.to_string())
                .unwrap_or_else(|| path.file_name().unwrap().to_str().unwrap().to_owned());
            graph
                .update_constant_properties([("path".to_string(), Prop::str(path_string.clone()))])
                .expect("Failed to add static property");

            if let Some(old_graph) = graphs.insert(
                graph_name,
                IndexedGraph::from_graph(&graph).expect("Unable to index graph"),
            ) {
                // insertion returns the old value if the entry already existed
                let old_path = old_graph.properties().get("path").unwrap_str();
                let name = old_graph.properties().get("name").unwrap_str();
                panic!("Graph with name {name} defined multiple times, first file: {old_path}, second file: {path_string}")
            }
        }
        graphs
    }
}
