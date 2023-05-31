use raphtory::db::graph::Graph;
use std::collections::HashMap;
use std::fs;

pub struct Data {
    pub(crate) graphs: HashMap<String, Graph>,
}

impl Data {
    pub fn new(graphs: HashMap<String, Graph>) -> Self {
        Self { graphs }
    }

    pub fn load(directory_path: &str) -> Self {
        let paths = fs::read_dir(directory_path).unwrap_or_else(|_| {
            panic!("path '{directory_path}' doesn't exist or it is not a directory")
        });

        let graphs = paths
            .filter_map(|entry| {
                let path = entry.unwrap().path();
                let graph = Graph::load_from_file(&path).ok()?;
                let filename = path.file_name()?.to_str()?.to_string();

                Some((filename, graph))
            })
            .collect();

        Self { graphs }
    }
}

#[cfg(test)]
mod graphql_test {
    use super::*;

    use std::any::Any;

    #[test]
    fn data_id() {
        let graph = Graph::new(1);
        let graphs = HashMap::from([("lotr".to_string(), graph)]);
        let data = Data::new(graphs);
        dbg!(&data.type_id());
        dbg!((&data).type_id());
        dbg!(data.type_id());
    }
}
