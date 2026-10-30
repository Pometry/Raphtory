//! Times read workloads on a graph saved on disk: adjacency scans, node and edge time
//! scans, and a few algorithms. Each workload runs twice: the first run pages the data in
//! (cold, unless the OS cache already holds it), the second runs on a warm cache.
//!
//! The workloads run on the whole graph, then on windows holding the last 1/5, 1/3 and 1/2 of
//! its dated edge events: by count, not by time range, and ignoring the events at the
//! earliest time (most of SNB's events are static relations at time 0, which would otherwise
//! put the whole graph in the 1/3 and 1/2 windows). Windowed views also read event times to
//! decide what is in the window.
//!
//! Usage: graph_algos <graph_dir> [--no-windows] [workload ...]
//! With no workloads it runs all of `DEFAULT_WORKLOADS`; `triangle_count` only runs when named
//! (about 110s on SNB-10).
use raphtory::{
    algorithms::{
        centrality::pagerank::page_rank,
        components::{strongly_connected_components, weakly_connected_components},
        cores::k_core::k_core_set,
        motifs::triangle_count::triangle_count,
    },
    core::storage::timeindex::AsTime,
    db::api::view::StaticGraphViewOps,
    prelude::*,
};
use rayon::prelude::*;
use std::{hint::black_box, path::PathBuf, time::Instant};

const DEFAULT_WORKLOADS: [&str; 7] = [
    "degree",
    "node_edge_history",
    "edge_explode",
    "page_rank",
    "wcc",
    "scc",
    "k_core",
];

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

fn run<G: StaticGraphViewOps>(graph: &G, workload: &str) -> String {
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
            // capped like page_rank: on ETH, peeling to convergence takes hours
            let core = k_core_set(graph, 3, 20, None);
            format!("3-core has {} nodes", core.len())
        }
        "triangle_count" => format!("{} triangles", triangle_count(graph, None)),
        other => panic!("unknown workload {other}; expected one of {WORKLOADS:?}"),
    }
}

fn main() {
    // strongly_connected_components recurses once per node on a path (Tarjan)
    std::thread::Builder::new()
        .stack_size(4 << 30)
        .spawn(run_all)
        .unwrap()
        .join()
        .unwrap();
}

fn run_all() {
    let mut args = std::env::args().skip(1);
    let graph_path = args
        .next()
        .map(PathBuf::from)
        .unwrap_or_else(|| panic!("Usage: graph_algos <graph_dir> [--no-windows] [workload ...]"));
    let mut workloads: Vec<String> = args.collect();
    let windows = !workloads.iter().any(|w| w == "--no-windows");
    workloads.retain(|w| w != "--no-windows");
    if workloads.is_empty() {
        workloads = DEFAULT_WORKLOADS.iter().map(|w| w.to_string()).collect();
    }

    let now = Instant::now();
    let graph = Graph::load(&graph_path).unwrap();
    println!(
        "ALGO open {:.2}s ({} nodes, {} edges)",
        now.elapsed().as_secs_f64(),
        graph.count_nodes(),
        graph.count_edges()
    );

    run_workloads("full", &graph, &workloads);
    if !windows {
        return;
    }

    let now = Instant::now();
    let earliest = graph.earliest_time().unwrap().t();
    let mut times: Vec<i64> = graph
        .nodes()
        .par_iter()
        .flat_map_iter(|n| {
            n.out_edges()
                .explode()
                .into_iter()
                .filter_map(|e| e.time().ok().map(|t| t.t()))
                .filter(|&t| t > earliest)
                .collect::<Vec<_>>()
        })
        .collect();
    let latest = graph.latest_time().unwrap().t();
    println!(
        "ALGO collected {} edge event times after {earliest} for the windows in {:.2}s",
        times.len(),
        now.elapsed().as_secs_f64()
    );
    for (name, fraction) in [("last_1/5", 5), ("last_1/3", 3), ("last_1/2", 2)] {
        let rank = times.len() - times.len() / fraction;
        let (_, start, _) = times.select_nth_unstable(rank);
        let start = *start;
        let window = graph.window(start, latest + 1);
        println!(
            "ALGO {name} window [{start}, {}] ({} nodes, {} edges)",
            latest + 1,
            window.count_nodes(),
            window.count_edges()
        );
        run_workloads(name, &window, &workloads);
    }
}

fn run_workloads<G: StaticGraphViewOps>(view: &str, graph: &G, workloads: &[String]) {
    for workload in workloads {
        for pass in ["cold", "warm"] {
            let now = Instant::now();
            let result = run(graph, workload);
            println!(
                "ALGO {view} {workload} {pass} {:.2}s ({result})",
                now.elapsed().as_secs_f64()
            );
        }
    }
}
