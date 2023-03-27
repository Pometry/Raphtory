use crate::{
    graph::Graph,
    graph_loader::{fetch_file, CsvLoader},
};

use serde::Deserialize;
use std::path::PathBuf;

#[derive(Deserialize, std::fmt::Debug)]
pub struct TEdge {
    src_id: u64,
    dst_id: u64,
    time: i64,
}

pub fn sx_superuser_file() -> Result<PathBuf, Box<dyn std::error::Error>> {
    fetch_file(
        "sx-superuser.txt.gz",
        "https://snap.stanford.edu/data/sx-superuser.txt.gz",
        600,
    )
}

pub fn sx_superuser_graph(shards: usize) -> Result<Graph, Box<dyn std::error::Error>> {
    let graph = Graph::new(shards);
    CsvLoader::new(sx_superuser_file()?)
        .set_delimiter(" ")
        .load_into_graph(&graph, |edge: TEdge, g: &Graph| {
            g.add_edge(edge.time, edge.src_id, edge.dst_id, &vec![]);
        })?;

    Ok(graph)
}

#[cfg(test)]
mod sx_superuser_test {
    use crate::graph_loader::sx_superuser_graph::{sx_superuser_file, sx_superuser_graph};

    #[test]
    fn test_download_works() {
        let file = sx_superuser_file().unwrap();
        assert!(file.is_file())
    }

    #[test]
    fn test_graph_loading_works() {
        let graph = sx_superuser_graph(2).unwrap();
    }
}
