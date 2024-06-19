use raphtory::graph_loader::source::csv_loader::CsvLoader;
use raphtory::prelude::*;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
struct Edge {
    src: u64,
    dst: u64,
}

fn main() {
    let path = std::env::args().nth(1).expect("No path provided");
    let graph = Graph::new();
    let now = std::time::Instant::now();
    CsvLoader::new(path)
        .set_delimiter(" ")
        .set_header(false)
        .load_into_graph::<_, Edge, _>(&graph, |edge, graph| {
            graph.add_edge(0, edge.src, edge.dst, NO_PROPS, None).expect("Failed to add edge");
            if graph.count_edges() % 10000 == 0 {
                println!("Edges loaded: {}", graph.count_edges());
            }
        })
        .expect("Failed to load graph");

    println!("Time taken to load graph: {:?}", now.elapsed());
}
