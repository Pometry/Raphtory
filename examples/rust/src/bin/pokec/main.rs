use std::time::Instant;

use raphtory::{
    algorithms::pagerank::{self, unweighted_page_rank},
    db::{graph::Graph, view_api::GraphViewOps},
};
use raphtory_io::graph_loader::source::csv_loader::CsvLoader;
use rayon::vec;
use serde::Deserialize;

#[derive(Deserialize, std::fmt::Debug)]
struct Edge {
    src: u64,
    dst: u64,
}

fn main() {
    let shards = 16;
    let g = Graph::new(shards);
    let now = Instant::now();

    CsvLoader::new("/home/murariuf/Offline/pokec/soc-pokec-relationships.txt.gz")
        .set_delimiter("\t")
        .set_header(false)
        .load_into_graph(&g, |e: Edge, g| {
            g.add_edge(0, e.src, e.dst, &vec![], None)
                .expect("Failed to add edge");
        })
        .expect("Failed to load graph from encoded data files");

    println!(
        "Loaded graph from encoded data files {} with {} vertices, {} edges which took {} seconds",
        "/home/murariuf/Offline/pokec/soc-pokec-relationships.txt.gz",
        g.num_vertices(),
        g.num_edges(),
        now.elapsed().as_secs()
    );

    let now = Instant::now();

    unweighted_page_rank(&g,100, None , Some(0.00001));

    println!("PageRank took {} seconds", now.elapsed().as_secs());
}
