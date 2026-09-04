use criterion::{Criterion, SamplingMode};
use raphtory::{
    db::{
        api::view::{Filter, StaticGraphViewOps},
        graph::views::node_subgraph::NodeSubgraph,
    },
    graphgen::random_attachment::random_attachment,
    prelude::*,
};
use std::sync::OnceLock;

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
            run(&graph, &setup_data);
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

pub fn first_node_id<G: StaticGraphViewOps>(graph: &G) -> GID {
    graph
        .nodes()
        .id()
        .iter_values()
        .next()
        .expect("graph has nodes")
}

// graph constructors

pub fn build_large_random_attachment_graph() -> Graph {
    let graph = Graph::new();
    let seed: [u8; 32] = [1; 32];
    random_attachment(&graph, 5000, 4, Some(seed));
    graph
}

pub fn large_random_attachment_graph() -> Graph {
    static GRAPH: OnceLock<Graph> = OnceLock::new();
    GRAPH
        .get_or_init(build_large_random_attachment_graph)
        .clone()
}

pub fn large_random_attachment_subgraph() -> NodeSubgraph<Graph> {
    let graph = large_random_attachment_graph();
    let subgraph = graph.subgraph(graph.nodes());
    subgraph
}

pub fn large_random_attachment_filtered() -> impl StaticGraphViewOps {
    large_random_attachment_graph()
        .filter(NodeFilter.degree().ge(1u64))
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

pub fn build_tiny_random_attachment_graph() -> Graph {
    let graph = Graph::new();
    let seed: [u8; 32] = [1; 32];
    random_attachment(&graph, 100, 4, Some(seed));
    graph
}

pub fn tiny_random_attachment_graph() -> Graph {
    static GRAPH: OnceLock<Graph> = OnceLock::new();
    GRAPH
        .get_or_init(build_tiny_random_attachment_graph)
        .clone()
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
