use raphtory::{
    algorithms::{
        centrality::pagerank::unweighted_page_rank, components::weakly_connected_components,
    },
    arrow::{
        graph_impl::{ArrowGraph, ParquetLayerCols},
        query::{ast::Query, executors::rayon2, state::NoState, ForwardState, NodeSource},
    },
    core::entities::VID,
    prelude::GraphViewOps,
};
use raphtory_arrow::algorithms::connected_components;
use raphtory_storage::lanl::*;
use std::{any::Any, env, io::Write, num::NonZeroUsize, vec};

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

    let parquet_dirs = vec![format!("{}", resources_dir)];

    let layer_parquet_cols: Vec<ParquetLayerCols> = vec![ParquetLayerCols {
        parquet_dir: &parquet_dirs[0],
        layer: "default",
        src_col: "src",
        dst_col: "dst",
        time_col: "time",
    }];

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

    fn iterate_all_edges(g: &ArrowGraph) {
        for e in g.edges() {
            e.edge.src().0;
        }
    }

    measure_without_print_results("Iterate All Edges", || iterate_all_edges(&graph));

    measure_without_print_results("CC", || {
        connected_components::connected_components(graph.as_ref())
    });
    measure_without_print_results("Weakly CC", || {
        weakly_connected_components(&graph, 20, None)
    });
    measure_without_print_results("Page Rank", || {
        unweighted_page_rank(&graph, Some(100), Some(1), None, true, None)
    });

    measure_without_print_results("Static Multi Hop Query (1 node)", || {
        static_multi_hop_single_node_query(&graph)
    });
    measure_without_print_results("Static Multi Hop Query (100 node)", || {
        static_multi_hop_multi_node_query(&graph)
    });
    measure_without_print_results("Temporal Multi Hop Query (1 node)", || {
        multi_hop_single_node_query(&graph)
    });
    measure_without_print_results("Temporal Multi Hop Query (100 node)", || {
        multi_hop_multi_node_query(&graph)
    });
}

fn static_multi_hop_single_node_query(graph: &ArrowGraph) {
    let nodes = vec![VID(3000)];

    let (sender, receiver) = std::sync::mpsc::channel();
    let query = Query::new().out("default").out("default").channel([sender]);

    let result = rayon2::execute::<NoState>(query, NodeSource::NodeIds(nodes), &graph, |_| NoState);
    assert!(result.is_ok());

    let _ = receiver.into_iter().collect::<Vec<_>>();
}

fn static_multi_hop_multi_node_query(graph: &ArrowGraph) {
    let nodes: Vec<VID> = (0..100).map(VID).collect();

    let (sender, receiver) = std::sync::mpsc::channel();
    let query = Query::new()
        .out_limit("default", 100)
        .out_limit("default", 100)
        .out_limit("default", 100)
        .out_limit("default", 100)
        .channel([sender]);

    let result = rayon2::execute::<NoState>(query, NodeSource::NodeIds(nodes), &graph, |_| {
        NoState::new()
    });
    assert!(result.is_ok());

    match receiver.recv() {
        Ok((_, vid)) => {
            println!("VID {}", vid.0);
        }
        Err(e) => {
            println!("Error receiving result: {:?}", e);
        }
    }
}

fn multi_hop_single_node_query(graph: &ArrowGraph) {
    let nodes = vec![VID(0)];

    let query: Query<ForwardState> = Query::new()
        .out_limit("default", 100)
        .out_limit("default", 100)
        .out_limit("default", 100)
        .out_limit("default", 100)
        .path("hop", |mut writer, state: ForwardState| {
            serde_json::to_writer(&mut writer, &state.path).unwrap();
            write!(writer, "\n").unwrap();
        });

    let result =
        rayon2::execute::<ForwardState>(query, NodeSource::NodeIds(nodes), graph, |node| {
            let earliest = node.earliest();
            ForwardState::at_time(node, earliest, 100)
        });

    assert!(result.is_ok());
}

fn multi_hop_multi_node_query(graph: &ArrowGraph) {
    let nodes: Vec<VID> = (0..100).map(VID).collect();

    let query: Query<ForwardState> = Query::new()
        .out_limit("default", 100)
        .out_limit("default", 100)
        .out_limit("default", 100)
        .out_limit("default", 100)
        .path("hop", |mut writer, state: ForwardState| {
            serde_json::to_writer(&mut writer, &state.path).unwrap();
            write!(writer, "\n").unwrap();
        });

    let result =
        rayon2::execute::<ForwardState>(query, NodeSource::NodeIds(nodes), graph, |node| {
            let earliest = node.earliest();
            ForwardState::at_time(node, earliest, 100)
        });

    assert!(result.is_ok());
}
