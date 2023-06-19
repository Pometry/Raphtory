use raphtory::db::graph::Graph;
use raphtory::db::view_api::internal::CoreGraphOps;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use walkdir::WalkDir;

pub(crate) struct Data {
    pub(crate) graphs: HashMap<String, Graph>,
}

impl Data {
    pub fn load(directory_path: &str) -> Self {
        let mut valid_paths = HashSet::<String>::new();

        for entry in WalkDir::new(directory_path)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            let p = entry.path().display().to_string();
            if p.contains("graphdb_nr_shards") {
                valid_paths.insert(p.strip_suffix("graphdb_nr_shards").unwrap().to_string());
            }
        }

        let graphs: HashMap<String, Graph> = valid_paths
            .into_iter()
            .map(|path| {
                let graph = Graph::load_from_file(&path).expect("Unable to load from graph");
                let maybe_graph_name = graph.static_prop("name");
                return match maybe_graph_name {
                    None => (
                        Path::new(&path)
                            .file_name()
                            .unwrap()
                            .to_str()
                            .unwrap()
                            .to_string(),
                        graph,
                    ),
                    Some(graph_name) => (graph_name.to_string(), graph),
                };
            })
            .collect();

        Self { graphs }
    }
}
