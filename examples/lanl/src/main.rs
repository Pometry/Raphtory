use raphtory::{
    arrow::graph_impl::{ArrowGraph, ParquetLayerCols},
    prelude::GraphViewOps,
};
use raphtory_storage::lanl::*;
use std::num::NonZeroUsize;

fn main() {
    let graph_dir: &str = "target";
    let layer_parquet_cols: Vec<ParquetLayerCols> = vec![
        ParquetLayerCols {
            parquet_dir: "data/netflowsorted/nft_sorted",
            layer: "netflow",
            src_col: "src",
            src_hash_col: "src_hash",
            dst_col: "dst",
            dst_hash_col: "dst_hash",
            time_col: "epoch_time",
        },
        ParquetLayerCols {
            parquet_dir: "data/netflowsorted/v1_sorted",
            layer: "events_1v",
            src_col: "src",
            src_hash_col: "src_hash",
            dst_col: "dst",
            dst_hash_col: "dst_hash",
            time_col: "epoch_time",
        },
        ParquetLayerCols {
            parquet_dir: "data/netflowsorted/v2_sorted",
            layer: "events_2v",
            src_col: "src",
            src_hash_col: "src_hash",
            dst_col: "dst",
            dst_hash_col: "dst_hash",
            time_col: "epoch_time",
        },
    ];

    let graph = match ArrowGraph::load_from_dir(graph_dir) {
        Ok(g) => g,
        Err(e) => {
            println!("Failed to load the graph from the directory. Attempting to load from parquet files: {}", e);

            let num_threads = std::thread::available_parallelism()
                .unwrap_or(NonZeroUsize::new(1).unwrap())
                .into();
            let chunk_size = 268_435_456;
            let t_props_chunk_size = chunk_size / 8;
            let read_chunk_size = Some(4_000_000);
            let concurrent_files = Some(1);

            ArrowGraph::load_from_parquets(
                graph_dir,
                layer_parquet_cols,
                chunk_size,
                t_props_chunk_size,
                read_chunk_size,
                concurrent_files,
                num_threads,
            )
            .expect("Failed to load the graph from parquet files")
        }
    };

    println!("Node count {}", graph.count_nodes());
    println!("Edge count {}", graph.count_edges());
    println!("Earliest time {}", graph.earliest());
    println!("Latest time {}", graph.latest());

    measure("Query 1", || query1::run(&graph).unwrap(), false);
    measure("Query 2", || query2::run(&graph).unwrap(), false);
    measure("Query 3", || query3::run(&graph).unwrap(), false);
    measure("Query 3b", || query3b::run(&graph).unwrap(), false);
    // # measure("Query 3c", || query3c::run(&graph).unwrap(), false);
    measure("Query 4", || query4::run2(&graph).unwrap(), false);
}
