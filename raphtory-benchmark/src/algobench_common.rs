#![allow(dead_code)]

// Shared infrastructure for the algobench_* benchmark binaries (see benches/algobench_*.rs).
//
// Benchmarks are split across binaries by algorithm speed/complexity (fast / medium / slow)
// so that a run of the fast or medium tier isn't held hostage by a handful of expensive
// algorithms, and so slow algorithms can run against a smaller graph to keep wall-clock time
// reasonable. Only `algobench_views` benchmarks the graph/subgraph/layered/filtered view
// variants (on a representative algorithm from each speed tier); every other binary only
// benchmarks the plain graph.
//
// The underlying random_attachment graphs are expensive to build, so each variant
// (plain / weighted / typed, large / tiny) is constructed once per process and cached;
// every benchmark reuses the cached graph (cheap `Arc` clone) and only builds a cheap view
// (subgraph/filter/layer) on top of it.

use criterion::{Criterion, SamplingMode};
use raphtory::{
    db::{
        api::view::{Filter, StaticGraphViewOps},
        graph::views::{
            filter::model::{
                degree_filter::DegreeFilterFactory, property_filter::ops::PropertyFilterOps,
            },
            node_subgraph::NodeSubgraph,
        },
    },
    graphgen::random_attachment::random_attachment,
    prelude::*,
};
use std::{hint::black_box, sync::OnceLock};

pub fn graph_benchmark_with_setup<G, BuildGraph, Setup, Run, SetupData, Output>(
    c: &mut Criterion,
    name: &str,
    measurement_secs: u64,
    sample_size: usize,
    build_graph: BuildGraph,
    setup: Setup,
    mut run: Run,
) where
    G: StaticGraphViewOps,
    BuildGraph: FnOnce() -> G,
    Setup: Fn(&G) -> SetupData,
    Run: FnMut(&G, &SetupData) -> Output,
{
    let mut group = c.benchmark_group(name);
    let graph = build_graph();
    let setup_data = setup(&graph);

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(measurement_secs));
    group.sample_size(sample_size);
    group.bench_function(name, |b| {
        b.iter(|| {
            let result = run(&graph, &setup_data);
            black_box(result);
        });
    });
    group.finish();
}

pub fn graph_benchmark<G, BuildGraph, Run, Output>(
    c: &mut Criterion,
    name: &str,
    measurement_secs: u64,
    sample_size: usize,
    build_graph: BuildGraph,
    run: Run,
) where
    G: StaticGraphViewOps,
    BuildGraph: FnOnce() -> G,
    Run: FnMut(&G, &()) -> Output,
{
    graph_benchmark_with_setup(
        c,
        name,
        measurement_secs,
        sample_size,
        build_graph,
        |_| (),
        run,
    )
}

pub fn simple_benchmark<Run, Output>(
    c: &mut Criterion,
    name: &str,
    measurement_secs: u64,
    sample_size: usize,
    mut run: Run,
) where
    Run: FnMut() -> Output,
{
    let mut group = c.benchmark_group(name);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(measurement_secs));
    group.sample_size(sample_size);
    group.bench_function(name, |b| {
        b.iter(|| {
            let result = run();
            black_box(result);
        });
    });
    group.finish()
}

pub fn first_node_id<G: StaticGraphViewOps>(graph: &G) -> GID {
    graph
        .nodes()
        .id()
        .iter_values()
        .next()
        .expect("graph has nodes")
}

// Large graph (5000 nodes) - used by the fast/medium tiers and as the base for the
// representative fast/medium/trivial algorithms in algobench_views.

pub fn build_large_random_attachment_graph() -> Graph {
    let graph = Graph::new();
    let seed: [u8; 32] = [1; 32];
    random_attachment(&graph, 5000, 4, Some(seed));
    graph
}

pub fn large_random_attachment_graph() -> Graph {
    static GRAPH: OnceLock<Graph> = OnceLock::new();
    GRAPH.get_or_init(build_large_random_attachment_graph).clone()
}

pub fn large_random_attachment_subgraph() -> NodeSubgraph<Graph> {
    let graph = large_random_attachment_graph();
    let subgraph = graph.subgraph(graph.nodes());
    subgraph
}

pub fn large_random_attachment_filtered() -> impl StaticGraphViewOps {
    large_random_attachment_graph()
        .filter(NodeFilter.degree().ge(0u64))
        .unwrap()
}

pub fn large_random_attachment_layered() -> impl StaticGraphViewOps {
    let graph = large_random_attachment_graph();
    graph.default_layer()
}

pub fn build_large_weighted_random_attachment_graph() -> Graph {
    let graph = build_large_random_attachment_graph();
    let ids = graph.nodes().id().iter_values().collect::<Vec<_>>();
    if let (Some(src), Some(dst)) = (ids.first(), ids.get(1)) {
        graph
            .add_edge(0, src.clone(), dst.clone(), [("weight", 1.0f64)], None)
            .expect("unable to add weighted edge");
    }
    graph
}

pub fn large_weighted_random_attachment_graph() -> Graph {
    static GRAPH: OnceLock<Graph> = OnceLock::new();
    GRAPH
        .get_or_init(build_large_weighted_random_attachment_graph)
        .clone()
}

pub fn build_large_typed_random_attachment_graph() -> Graph {
    let graph = build_large_random_attachment_graph();
    let ids = graph.nodes().id().iter_values().collect::<Vec<_>>();
    for id in ids {
        graph
            .add_node(0, id, NO_PROPS, Some("Right"), None)
            .expect("unable to set node type");
    }
    graph
}

pub fn large_typed_random_attachment_graph() -> Graph {
    static GRAPH: OnceLock<Graph> = OnceLock::new();
    GRAPH
        .get_or_init(build_large_typed_random_attachment_graph)
        .clone()
}

// Medium graph (1500 nodes) - dedicated to algobench_medium, distinct from the large
// (5000 node) graph so that binary isn't just running the fast/slow tiers' graph at a
// different set of algorithms.

pub fn build_medium_random_attachment_graph() -> Graph {
    let graph = Graph::new();
    let seed: [u8; 32] = [1; 32];
    random_attachment(&graph, 1500, 4, Some(seed));
    graph
}

pub fn medium_random_attachment_graph() -> Graph {
    static GRAPH: OnceLock<Graph> = OnceLock::new();
    GRAPH
        .get_or_init(build_medium_random_attachment_graph)
        .clone()
}

pub fn medium_random_attachment_subgraph() -> NodeSubgraph<Graph> {
    let graph = medium_random_attachment_graph();
    let subgraph = graph.subgraph(graph.nodes());
    subgraph
}

pub fn medium_random_attachment_filtered() -> impl StaticGraphViewOps {
    medium_random_attachment_graph()
        .filter(NodeFilter.degree().ge(0u64))
        .unwrap()
}

pub fn medium_random_attachment_layered() -> impl StaticGraphViewOps {
    let graph = medium_random_attachment_graph();
    graph.default_layer()
}

pub fn build_medium_weighted_random_attachment_graph() -> Graph {
    let graph = build_medium_random_attachment_graph();
    let ids = graph.nodes().id().iter_values().collect::<Vec<_>>();
    if let (Some(src), Some(dst)) = (ids.first(), ids.get(1)) {
        graph
            .add_edge(0, src.clone(), dst.clone(), [("weight", 1.0f64)], None)
            .expect("unable to add weighted edge");
    }
    graph
}

pub fn medium_weighted_random_attachment_graph() -> Graph {
    static GRAPH: OnceLock<Graph> = OnceLock::new();
    GRAPH
        .get_or_init(build_medium_weighted_random_attachment_graph)
        .clone()
}

pub fn build_medium_typed_random_attachment_graph() -> Graph {
    let graph = build_medium_random_attachment_graph();
    let ids = graph.nodes().id().iter_values().collect::<Vec<_>>();
    for id in ids {
        graph
            .add_node(0, id, NO_PROPS, Some("Right"), None)
            .expect("unable to set node type");
    }
    graph
}

pub fn medium_typed_random_attachment_graph() -> Graph {
    static GRAPH: OnceLock<Graph> = OnceLock::new();
    GRAPH
        .get_or_init(build_medium_typed_random_attachment_graph)
        .clone()
}

// Tiny graph (100 nodes) - dedicated to algorithms too expensive to run at the large
// graph's 5000-node scale (components, betweenness, temporal rich club, matching, layout).

pub fn build_tiny_random_attachment_graph() -> Graph {
    let graph = Graph::new();
    let seed: [u8; 32] = [1; 32];
    random_attachment(&graph, 100, 4, Some(seed));
    graph
}

pub fn tiny_random_attachment_graph() -> Graph {
    static GRAPH: OnceLock<Graph> = OnceLock::new();
    GRAPH.get_or_init(build_tiny_random_attachment_graph).clone()
}

pub fn tiny_random_attachment_subgraph() -> NodeSubgraph<Graph> {
    let graph = tiny_random_attachment_graph();
    let subgraph = graph.subgraph(graph.nodes());
    subgraph
}

pub fn tiny_random_attachment_filtered() -> impl StaticGraphViewOps {
    tiny_random_attachment_graph()
        .filter(NodeFilter.degree().ge(0u64))
        .unwrap()
}

pub fn tiny_random_attachment_layered() -> impl StaticGraphViewOps {
    let graph = tiny_random_attachment_graph();
    graph.default_layer()
}
