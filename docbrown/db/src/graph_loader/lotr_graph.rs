use fetch_data::FetchDataError;
use serde::Deserialize;
use std::path::PathBuf;
use crate::graph_loader::{fetch_file, GraphDB, CsvLoader};
use docbrown_core::{Prop, utils};

#[derive(Deserialize, std::fmt::Debug)]
pub struct Lotr {
    src_id: String,
    dst_id: String,
    time: i64,
}

pub fn lotr_file() -> Result<PathBuf, FetchDataError> {
    fetch_file(
        "lotr.csv",
        "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv",
        "c37083a5018827d06de3b884bea8275301d5ef6137dfae6256b793bffb05d033",
    )
}

pub fn lotr_graph(shards: usize) -> GraphDB {
    let graph = {
        let g = GraphDB::new(shards);

        CsvLoader::new(&lotr_file().unwrap())
            .load_into_graph(&g, |lotr: Lotr, g: &GraphDB| {
                let src_id = utils::calculate_hash(&lotr.src_id);
                let dst_id = utils::calculate_hash(&lotr.dst_id);
                let time = lotr.time; 

                g.add_vertex(
                    src_id,
                    time,
                    &vec![("name".to_string(), Prop::Str("Character".to_string()))],
                );
                g.add_vertex(
                    src_id,
                    time,
                    &vec![("name".to_string(), Prop::Str("Character".to_string()))],
                );
                g.add_edge(
                    src_id,
                    dst_id,
                    time,
                    &vec![(
                        "name".to_string(),
                        Prop::Str("Character Co-occurrence".to_string()),
                    )],
                );
            })
            .expect("Failed to load graph from CSV data files");
        g
    };
    graph
}
