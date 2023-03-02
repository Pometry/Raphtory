use serde::Deserialize;
use std::path::PathBuf;
use crate::{graph_loader::{fetch_file, CsvLoader}, graph::Graph};
use docbrown_core::{Prop, utils};

#[derive(Deserialize, std::fmt::Debug)]
pub struct Lotr {
    src_id: String,
    dst_id: String,
    time: i64,
}

pub fn lotr_file() -> Result<PathBuf, Box<dyn std::error::Error>> {
    fetch_file(
        "lotr.csv",
        "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv",
    )
}

pub fn lotr_graph(shards: usize) -> Graph {
    let graph = {
        let g = Graph::new(shards);

        CsvLoader::new(&lotr_file().unwrap())
            .load_into_graph(&g, |lotr: Lotr, g: &Graph| {
                let src_id = utils::calculate_hash(&lotr.src_id);
                let dst_id = utils::calculate_hash(&lotr.dst_id);
                let time = lotr.time; 

                g.add_vertex(
                    time,
                    src_id,
                    &vec![("name".to_string(), Prop::Str("Character".to_string()))],
                );
                g.add_vertex(
                    time,
                    src_id,
                    &vec![("name".to_string(), Prop::Str("Character".to_string()))],
                );
                g.add_edge(
                    time,
                    src_id,
                    dst_id,
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
