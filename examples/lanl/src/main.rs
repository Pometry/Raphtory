use raphtory::{
    algorithms::{
        centrality::pagerank::unweighted_page_rank, components::weakly_connected_components,
    },
    arrow::graph_impl::{ArrowGraph, ParquetLayerCols},
    prelude::{LayerOps, *},
};
use raphtory_arrow::algorithms::connected_components;
use raphtory_storage::lanl::{exfiltration, *};
use std::{env, num::NonZeroUsize, vec};

fn main() {
    // Retrieve named parameters from environment variables
    let resources_dir =
        env::var("resources_dir").expect("Environment variable 'resources_dir' not set");
    let target_dir = env::var("target_dir").expect("Environment variable 'target_dir' not set");

    // Set default values and then try to override them with environment variables if they exist
    let chunk_size: usize = env::var("chunk_size")
        .unwrap_or_else(|_| "268435456".to_string())
        .parse()
        .expect("Invalid value for 'chunk_size'");
    let t_props_chunk_size: usize = env::var("t_props_chunk_size")
        .unwrap_or_else(|_| "20000000".to_string())
        .parse()
        .expect("Invalid value for 't_props_chunk_size'");
    let read_chunk_size: usize = env::var("read_chunk_size")
        .unwrap_or_else(|_| "4000000".to_string())
        .parse()
        .expect("Invalid value for 'read_chunk_size'");
    let concurrent_files: usize = env::var("concurrent_files")
        .unwrap_or_else(|_| "1".to_string())
        .parse()
        .expect("Invalid value for 'concurrent_files'");
    let num_threads: usize = env::var("num_threads")
        .unwrap_or_else(|_| NonZeroUsize::new(1).unwrap().to_string())
        .parse()
        .expect("Invalid value for 'num_threads'");

    println!("Resources directory: {}", resources_dir);
    println!("Target directory: {}", target_dir);
    println!("Chunk Size: {}", chunk_size);
    println!("T Props Chunk Size: {}", t_props_chunk_size);
    println!("Read Chunk Size: {}", read_chunk_size);
    println!("Concurrent Files: {}", concurrent_files);
    println!("Num Threads: {}", num_threads);

    let graph_dir: &str = &target_dir.to_string();

    let parquet_dirs = vec![
        format!("{}/netflowsorted/nft_sorted", resources_dir),
        format!("{}/netflowsorted/v1_sorted", resources_dir),
        format!("{}/netflowsorted/v2_sorted", resources_dir),
    ];

    let layer_parquet_cols: Vec<ParquetLayerCols> = vec![
        ParquetLayerCols {
            parquet_dir: &parquet_dirs[0],
            layer: "netflow",
            src_col: "src",
            dst_col: "dst",
            time_col: "epoch_time",
        },
        ParquetLayerCols {
            parquet_dir: &parquet_dirs[1],
            layer: "events_1v",
            src_col: "src",
            dst_col: "dst",
            time_col: "epoch_time",
        },
        ParquetLayerCols {
            parquet_dir: &parquet_dirs[2],
            layer: "events_2v",
            src_col: "src",
            dst_col: "dst",
            time_col: "epoch_time",
        },
    ];

    let graph = match measure_without_print_results("Graph load from dir", || {
        ArrowGraph::load_from_dir(graph_dir)
    }) {
        Ok(g) => g,
        Err(e) => {
            println!("Failed to load saved graph. Attempting to load from parquet files...");
            measure_without_print_results("Graph load from parquets", || {
                ArrowGraph::load_from_parquets(
                    graph_dir,
                    layer_parquet_cols,
                    None,
                    chunk_size,
                    t_props_chunk_size,
                    Some(read_chunk_size),
                    Some(concurrent_files),
                    num_threads,
                )
            })
            .expect("Failed to load the graph from parquet files")
        }
    };

    println!("Node count = {}", graph.count_nodes());
    println!("Edge count = {}", graph.count_edges());
    println!("Earliest time = {}", graph.earliest_time().unwrap());
    println!("Latest time = {}", graph.latest_time().unwrap());

    measure_with_print_results("Query 1", || query1::run(graph.as_ref()).unwrap());
    measure_with_print_results("Query 2", || query2::run(graph.as_ref()).unwrap());
    measure_with_print_results("Query 3", || query3::run(graph.as_ref()).unwrap());
    measure_with_print_results("Query 3b", || query3b::run(graph.as_ref()).unwrap());
    // # measure_with_print_results("Query 3c", || query3c::run(graph.as_ref()).unwrap());
    measure_with_print_results("Query 4", || query4::run2(graph.as_ref()).unwrap());

    measure_without_print_results("CC", || {
        connected_components::connected_components(graph.as_ref())
    });
    measure_without_print_results("Weakly CC", || {
        weakly_connected_components(&graph.valid_layers("netflow"), 20, None)
    });
    measure_without_print_results("Page Rank", || {
        unweighted_page_rank(
            &graph.valid_layers("netflow"),
            Some(100),
            Some(1),
            None,
            true,
            None,
        )
    });
    measure_with_print_results("Exfilteration Query 1", || {
        exfiltration::query1::run(graph.as_ref())
    });
    measure_with_print_results("Exfilteration Count Query Total", || {
        exfiltration::count::query_total(graph.as_ref(), 30)
    });
    measure_with_print_results("Exfilteration List Query Count", || {
        exfiltration::list::query_count(graph.as_ref(), 30)
    });
}
