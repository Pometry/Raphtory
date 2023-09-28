use raphtory::{
    core::{entities::{VID, LayerIds}, Direction},
    db::api::view::internal::{CoreGraphOps, GraphOps},
    graph_loader::source::csv_loader::CsvLoader,
    prelude::*,
};
use raphtory_storage::arrow::col_graph2::TempColGraphFragment;
use serde::Deserialize;
use rayon::prelude::*;

#[derive(Deserialize, Debug)]
pub struct Edge {
    _unknown0: i64,
    _unknown1: i64,
    _unknown2: i64,
    src: u64,
    dst: u64,
    time: i64,
    _unknown3: u64,
    amount_usd: u64,
}

fn main() {
    let csv_bay_path = std::env::args()
        .nth(1)
        .expect("please supply a file to read");

    let parque_bay_path = std::env::args()
        .nth(2)
        .expect("please supply a file to read");

    let graph_dir = std::env::args()
        .nth(3)
        .expect("please supply a graph output directory");

    let rap_graph = Graph::new();

    CsvLoader::new(csv_bay_path)
        .load_into_graph(&rap_graph, |sent: Edge, g: &Graph| {
            let src = sent.src;
            let dst = sent.dst;
            let time = sent.time;

            g.add_edge(
                time,
                src,
                dst,
                [("amount".to_owned(), Prop::U64(sent.amount_usd))],
                None,
            )
            .unwrap();
        })
        .expect("failed to load graph");

    println!("Graph num_vertices {}", rap_graph.unfiltered_num_vertices());

    let chunk_size = 1 << 18;

    let graph = TempColGraphFragment::from_sorted_parquet_edge_list(
        parque_bay_path,
        "source",
        "destination",
        "time",
        chunk_size,
        graph_dir,
    )
    .expect("failed to load from parquet");

    println!("TCGraph num_vertices {}", graph.num_vertices());

    let now = std::time::Instant::now();

    let count:usize = (0..graph.num_vertices()).into_par_iter().map(|a| {
        let mut count = 0;
        for (_, b) in graph.edges(VID(a), Direction::OUT) {
            for (_, _ ) in graph.edges(b, Direction::OUT) {
                count +=1;
            }
        }
        count
    }).sum();

    println!("Temporal Columnar graph");
    println!("2 Hop path count: {}", count);
    println!("2 Hop path count took {} seconds", now.elapsed().as_secs());

    let now = std::time::Instant::now();

    let count:usize = (0..rap_graph.count_vertices()).into_par_iter().map(|a| {
        let mut count = 0;
        for b in rap_graph.neighbours(VID(a), Direction::OUT, LayerIds::All, None) {
            for _ in rap_graph.neighbours(b, Direction::OUT, LayerIds::All, None) {
                count +=1;
            }
        }
        count
    }).sum();

    println!("Raphtory graph");
    println!("2 Hop path count: {}", count);
    println!("2 Hop path count took {} seconds", now.elapsed().as_secs());
}
