use crate::{
    graph::Graph,
    graph_loader::{fetch_file, CsvLoader},
};
use serde::Deserialize;
use std::path::PathBuf;

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
        600,
    )
}

pub fn lotr_graph(shards: usize) -> Graph {
    let graph = {
        let g = Graph::new(shards);

        CsvLoader::new(&lotr_file().unwrap())
            .load_into_graph(&g, |lotr: Lotr, g: &Graph| {
                let src_id = &lotr.src_id;
                let dst_id = &lotr.dst_id;
                let time = lotr.time;

                g.add_vertex(time, src_id.clone(), &vec![])
                    .map_err(|err| println!("{:?}", err))
                    .ok();
                g.add_vertex(time, dst_id.clone(), &vec![])
                    .map_err(|err| println!("{:?}", err))
                    .ok();
                g.add_edge(time, src_id.clone(), dst_id.clone(), &vec![]);
            })
            .expect("Failed to load graph from CSV data files");
        g
    };
    graph
}
