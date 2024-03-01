use std::collections::HashMap;
use raphtory_storage::lanl::*;
use raphtory_storage::lanl::loader::{load_from_dir, load_from_parquet};

fn main() {
    let graph_dir = "/Users/shivamkapoor/Official/target";
    let layernames_parquet_dirs: HashMap<&str, &str> = HashMap::from([
        ("netflow", "/Users/shivamkapoor/Official/data/netflowsorted/nft_sorted"),
        ("events_1v", "/Users/shivamkapoor/Official/data/netflowsorted/v1_sorted"),
        ("events_2v", "/Users/shivamkapoor/Official/data/netflowsorted/v2_sorted")
    ]);

    let graph = match load_from_dir(graph_dir) {
        Ok(g) => g,
        Err(e) => {
            println!("Failed to load the graph from the directory. Attempting to load from parquet files: {}", e);
            load_from_parquet(graph_dir, layernames_parquet_dirs).expect("Failed to load the graph from parquet files")
        }
    };

    println!("Node count {}", graph.num_nodes());
    println!("Edge count {}", graph.num_edges(0));
    println!("Earliest time {}", graph.earliest());
    println!("Latest time {}", graph.latest());

    measure("Query 1", || query1::run(&graph).unwrap());
    measure("Query 2", || query2::run(&graph).unwrap());
    measure("Query 3", || query3::run(&graph).unwrap());
    measure("Query 3b", || query3b::run(&graph).unwrap());
    // # measure("Query 3c", || query3c::run(&graph).unwrap());
    measure("Query 4", || query4::run2(&graph).unwrap());
}
