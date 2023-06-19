use raphtory::db::graph::Graph;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

pub(crate) struct Data {
    pub(crate) graphs: HashMap<String, Graph>,
}

impl Data {
    pub fn load(directory_path: &str) -> Self {
        let paths = fs::read_dir(directory_path).unwrap_or_else(|_| {
            panic!("path '{directory_path}' doesn't exist or it is not a directory")
        });

        let graphs = paths
            .filter_map(|entry| {
                let path: PathBuf = entry.unwrap().path();
                if path.is_dir() {
                    let graph = Graph::load_from_file(&path).ok()?;
                    let filename = path.file_name()?.to_str()?.to_string();
                    Some((filename, graph))
                } else {
                    None
                }
            })
            .collect();

        Self { graphs }
    }
}
