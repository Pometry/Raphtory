//! Times read workloads on a graph saved on disk: adjacency scans, node and edge time
//! scans, and a few algorithms. Each workload runs twice: the first run pages the data in
//! (cold, unless the OS cache already holds it), the second runs on a warm cache.
//!
//! Usage: graph_algos <graph_dir> [workload ...]
use raphtory::{
    algorithms::{
        centrality::pagerank::page_rank,
        components::{strongly_connected_components, weakly_connected_components},
        cores::k_core::k_core_set,
        motifs::triangle_count::triangle_count,
    },
    prelude::*,
};
use rayon::prelude::*;
use std::{hint::black_box, path::PathBuf, time::Instant};

const WORKLOADS: [&str; 8] = [
    "degree",
    "node_edge_history",
    "edge_explode",
    "page_rank",
    "wcc",
    "scc",
    "k_core",
    "triangle_count",
];

fn run(graph: &Graph, workload: &str) -> String {
    match workload {
        // adjacency lists of every node
        "degree" => {
            let total: usize = graph.nodes().par_iter().map(|n| n.degree()).sum();
            format!("sum degree {total}")
        }
        // node additions (the per-node edge event times)
        "node_edge_history" => {
            let total: usize = graph
                .nodes()
                .par_iter()
                .map(|n| n.edge_history_count())
                .sum();
            format!("edge events seen from nodes {total}")
        }
        // the time index of every edge, reached through each node's out edges
        "edge_explode" => {
            let total: usize = graph
                .nodes()
                .par_iter()
                .map(|n| n.out_edges().explode().into_iter().count())
                .sum();
            format!("exploded edges {total}")
        }
        "page_rank" => {
            let ranks = page_rank(graph, None, Some(20), None, None, true, None);
            format!("ranked {} nodes", black_box(ranks).len())
        }
        "wcc" => {
            let components = weakly_connected_components(graph);
            format!("labelled {} nodes", black_box(components).len())
        }
        "scc" => {
            let components = strongly_connected_components(graph);
            format!("labelled {} nodes", black_box(components).len())
        }
        "k_core" => {
            let core = k_core_set(graph, 3, usize::MAX, None);
            format!("3-core has {} nodes", core.len())
        }
        "triangle_count" => format!("{} triangles", triangle_count(graph, None)),
        other => panic!("unknown workload {other}; expected one of {WORKLOADS:?}"),
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let graph_path = args
        .next()
        .map(PathBuf::from)
        .unwrap_or_else(|| panic!("Usage: graph_algos <graph_dir> [workload ...]"));
    let mut workloads: Vec<String> = args.collect();
    if workloads.is_empty() {
        workloads = WORKLOADS.iter().map(|w| w.to_string()).collect();
    }

    let now = Instant::now();
    let graph = Graph::load(&graph_path).unwrap();
    println!(
        "ALGO open {:.2}s ({} nodes, {} edges)",
        now.elapsed().as_secs_f64(),
        graph.count_nodes(),
        graph.count_edges()
    );

    for workload in &workloads {
        for pass in ["cold", "warm"] {
            let now = Instant::now();
            let result = run(&graph, workload);
            println!(
                "ALGO {workload} {pass} {:.2}s ({result})",
                now.elapsed().as_secs_f64()
            );
        }
    }
}
