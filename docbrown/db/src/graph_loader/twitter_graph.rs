use fetch_data::FetchDataError;
use serde::Deserialize;
use std::path::PathBuf;
use crate::graph_loader::{fetch_file, GraphDB, CsvLoader};
use docbrown_core::{Prop, utils};

#[derive(Deserialize, std::fmt::Debug)]
pub struct Twitter {
    src_id: String,
    dst_id: String,
}

pub fn twitter_file() -> Result<PathBuf, FetchDataError> {
    fetch_file(
        "twitter.csv",
        "https://raw.githubusercontent.com/Raphtory/Data/main/snap-twitter.csv",
        "b9cbdf68086c0c6b1501efa2e5ac6b1b0d9e069ad9215cebeba244e6e623c1bb",
    )
}

pub fn twitter_graph(shards: usize) -> GraphDB {
    let graph = {
        let g = GraphDB::new(shards);

        CsvLoader::new(&twitter_file().unwrap())
            .set_delimiter(" ")
            .load_into_graph(&g, |twitter: Twitter, g: &GraphDB| {
                let src_id = utils::calculate_hash(&twitter.src_id);
                let dst_id = utils::calculate_hash(&twitter.dst_id);
                let time = 1;

                g.add_vertex(
                    src_id,
                    time,
                    &vec![("name".to_string(), Prop::Str("User".to_string()))],
                );
                g.add_vertex(
                    src_id,
                    time,
                    &vec![("name".to_string(), Prop::Str("User".to_string()))],
                );
                g.add_edge(
                    src_id,
                    dst_id,
                    time,
                    &vec![("name".to_string(), Prop::Str("Tweet".to_string()))],
                );
            })
            .expect("Failed to load graph from CSV data files");
        g
    };

    graph
}