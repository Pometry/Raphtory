use criterion::measurement::WallTime;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkGroup, Criterion};
use rand::{Rng, SeedableRng};
use rand_distr::{Distribution, Normal};
use raphtory::db::api::properties::internal::InternalTemporalPropertyViewOps;
use raphtory::storage::core_ops::CoreGraphOps;
use raphtory::{graph_loader::lotr_graph::lotr_graph, prelude::*};
use raphtory_api::core::storage::timeindex::AsTime;

const PROP_NAME: &str = "bench_prop";
const RNG_SEED: u64 = 42;

/// Sample a time from a normal distribution centered at mid, clamped to [earliest, latest]
fn sample_time(rng: &mut impl rand::Rng, earliest: i64, latest: i64) -> i64 {
    let mid = earliest + (latest - earliest) / 2;
    let std_dev = ((latest - earliest) / 6) as f64; // ~99.7% within range
    let normal = Normal::new(mid as f64, std_dev).unwrap();
    let sample = normal.sample(rng) as i64;
    sample.clamp(earliest, latest)
}

/// Setup graph with temporal properties on nodes and edges
/// Properties are added at times sampled from a normal distribution centered at midpoint
/// Returns (graph, property_id_for_nodes, property_id_for_edges, earliest_time, latest_time)
fn setup_graph_with_props() -> (Graph, usize, usize, i64, i64) {
    let graph = lotr_graph();

    let earliest = graph.earliest_time().expect("graph should have time").t();
    let latest = graph.latest_time().expect("graph should have time").t();

    let mut rng = rand::rngs::StdRng::seed_from_u64(RNG_SEED);

    // Add temporal property to all nodes at times from normal distribution
    let nodes = graph.nodes().id().collect::<Vec<_>>();
    for node in nodes {
        let t = sample_time(&mut rng, earliest, latest);
        let node = graph.node(node).expect("node should exist");
        node.add_updates(t, [(PROP_NAME, Prop::I32(42))], None)
            .expect("failed to add node property");
    }

    // Add temporal property to all edges at times from normal distribution
    let edges = graph.edges().id().collect::<Vec<_>>();
    for (src, dst) in edges {
        let t = sample_time(&mut rng, earliest, latest);
        let edge = graph.edge(src, dst).expect("edge should exist");
        edge.add_updates(t, [(PROP_NAME, Prop::I32(42))], None)
            .expect("failed to add edge property");
    }

    let node_prop_id = graph
        .node_meta()
        .temporal_prop_mapper()
        .get_id(PROP_NAME)
        .expect("node property should exist");

    let edge_prop_id = graph
        .edge_meta()
        .temporal_prop_mapper()
        .get_id(PROP_NAME)
        .expect("edge property should exist");

    (graph, node_prop_id, edge_prop_id, earliest, latest)
}

/// Setup layered graph with temporal properties
/// Properties are added at times sampled from a normal distribution centered at midpoint
/// Returns (graph, node_prop_id, edge_prop_id, earliest, latest, layer_names)
fn setup_layered_graph_with_props(
    num_layers: usize,
) -> (Graph, usize, usize, i64, i64, Vec<String>) {
    let graph = lotr_graph();

    let layer_names: Vec<String> = (0..num_layers).map(|i| format!("layer_{}", i)).collect();

    // Copy edges to all layers
    let mut edges = vec![];
    for edge in graph.edges() {
        edges.push(edge.edge);
    }

    let earliest = graph.earliest_time().expect("graph should have time").t();
    let latest = graph.latest_time().expect("graph should have time").t();

    let mut rng = rand::rngs::StdRng::seed_from_u64(RNG_SEED);

    // Add temporal property to all nodes at times from normal distribution
    for node in graph.nodes().id().collect::<Vec<_>>() {
        let t = sample_time(&mut rng, earliest, latest);
        let node = graph.node(node).expect("node should exist");
        let layer = format!("layer_{}", rng.random_range(0..num_layers));
        node.add_updates(t, [(PROP_NAME, Prop::I32(42))], Some(layer.as_str()))
            .expect("failed to add node property");
    }

    // Add temporal property to all edges at times from normal distribution, on all layers
    for edge in &edges {
        let t = sample_time(&mut rng, earliest, latest);
        let edge = graph
            .edge(edge.src(), edge.dst())
            .expect("edge should exist");
        let layer = format!("layer_{}", rng.random_range(0..num_layers));
        edge.add_updates(t, [(PROP_NAME, Prop::I32(42))], Some(layer.as_str()))
            .expect("failed to add edge property");
    }

    let node_prop_id = graph
        .node_meta()
        .temporal_prop_mapper()
        .get_id(PROP_NAME)
        .expect("node property should exist");

    let edge_prop_id = graph
        .edge_meta()
        .temporal_prop_mapper()
        .get_id(PROP_NAME)
        .expect("edge property should exist");

    (
        graph,
        node_prop_id,
        edge_prop_id,
        earliest,
        latest,
        layer_names,
    )
}

/// Calculate window bounds for a given coverage percentage
/// coverage: 0.1 = 10%, 0.5 = 50%, 1.0 = 100%
fn window_for_coverage(earliest: i64, latest: i64, coverage: f64) -> (i64, i64) {
    let range = latest - earliest;
    let window_size = (range as f64 * coverage) as i64;
    let mid = earliest + range / 2;
    let start = mid - window_size / 2;
    let end = mid + window_size / 2 + 1; // +1 because window end is exclusive
    (start, end)
}

/// Window that ends before the property updates (most will return None)
fn window_before_prop(earliest: i64, latest: i64) -> (i64, i64) {
    let mid = earliest + (latest - earliest) / 2;
    let std_dev = (latest - earliest) / 6;
    // Window ends 2 std devs before mid (covers ~2.5% of distribution)
    (earliest, mid - 2 * std_dev)
}

/// Window that contains most property updates (most will return Some)
fn window_contains_prop(earliest: i64, latest: i64) -> (i64, i64) {
    let mid = earliest + (latest - earliest) / 2;
    let std_dev = (latest - earliest) / 6;
    // Window covers +/- 1 std dev from mid (~68% of distribution)
    (mid - std_dev, mid + std_dev + 1)
}

/// Window that starts after most property updates (most will return None for event semantics)
fn window_after_prop(earliest: i64, latest: i64) -> (i64, i64) {
    let mid = earliest + (latest - earliest) / 2;
    let std_dev = (latest - earliest) / 6;
    // Window starts 2 std devs after mid (covers ~2.5% of distribution)
    (mid + 2 * std_dev, latest + 1)
}

fn bench_node_temporal_value(
    group: &mut BenchmarkGroup<WallTime>,
    name: &str,
    graph: &Graph,
    prop_id: usize,
    window: Option<(i64, i64)>,
) {
    let nodes: Vec<_> = graph.nodes().collect();

    match window {
        Some((start, end)) => {
            let windowed = graph.window(start, end);
            let windowed_nodes: Vec<_> = windowed.nodes().collect();
            group.bench_function(name, |b| {
                b.iter(|| {
                    for node in &windowed_nodes {
                        black_box(node.temporal_value(prop_id));
                    }
                })
            });
        }
        None => {
            group.bench_function(name, |b| {
                b.iter(|| {
                    for node in &nodes {
                        black_box(node.temporal_value(prop_id));
                    }
                })
            });
        }
    }
}

fn bench_edge_temporal_value(
    group: &mut BenchmarkGroup<WallTime>,
    name: &str,
    graph: &Graph,
    prop_id: usize,
    window: Option<(i64, i64)>,
    layers: Option<&[String]>,
) {
    match (window, layers) {
        (Some((start, end)), Some(layer_names)) => {
            let layer_refs: Vec<&str> = layer_names.iter().map(|s| s.as_str()).collect();
            let view = graph.window(start, end).layers(layer_refs).unwrap();
            let edges: Vec<_> = view.edges().collect();
            group.bench_function(name, |b| {
                b.iter(|| {
                    for edge in &edges {
                        black_box(edge.temporal_value(prop_id));
                    }
                })
            });
        }
        (Some((start, end)), None) => {
            let windowed = graph.window(start, end);
            let edges: Vec<_> = windowed.edges().collect();
            group.bench_function(name, |b| {
                b.iter(|| {
                    for edge in &edges {
                        black_box(edge.temporal_value(prop_id));
                    }
                })
            });
        }
        (None, Some(layer_names)) => {
            let layer_refs: Vec<&str> = layer_names.iter().map(|s| s.as_str()).collect();
            let view = graph.layers(layer_refs).unwrap();
            let edges: Vec<_> = view.edges().collect();
            group.bench_function(name, |b| {
                b.iter(|| {
                    for edge in &edges {
                        black_box(edge.temporal_value(prop_id));
                    }
                })
            });
        }
        (None, None) => {
            let edges: Vec<_> = graph.edges().collect();
            group.bench_function(name, |b| {
                b.iter(|| {
                    for edge in &edges {
                        black_box(edge.temporal_value(prop_id));
                    }
                })
            });
        }
    }
}

fn temporal_props_benchmarks(c: &mut Criterion) {
    eprintln!("=== temporal_props_benchmarks STARTING ===");

    // ========== Basic graph (no layers) ==========
    eprintln!("Setting up graph with props...");
    let (graph, node_prop_id, edge_prop_id, earliest, latest) = setup_graph_with_props();
    eprintln!(
        "Graph setup complete: {} nodes, {} edges",
        graph.count_nodes(),
        graph.count_edges()
    );

    // --- Node benchmarks ---
    eprintln!("Starting node benchmarks...");
    let mut node_group = c.benchmark_group("node_temporal_value");

    // No window (100% coverage, returns Some)
    bench_node_temporal_value(
        &mut node_group,
        "no_window_some",
        &graph,
        node_prop_id,
        None,
    );

    // Window coverage benchmarks (all return Some since they include midpoint)
    for coverage in [0.1, 0.5, 1.0] {
        let (start, end) = window_for_coverage(earliest, latest, coverage);
        let name = format!("window_{}pct_some", (coverage * 100.0) as u32);
        bench_node_temporal_value(
            &mut node_group,
            &name,
            &graph,
            node_prop_id,
            Some((start, end)),
        );
    }

    // Window position benchmarks
    let (start, end) = window_before_prop(earliest, latest);
    bench_node_temporal_value(
        &mut node_group,
        "window_before_none",
        &graph,
        node_prop_id,
        Some((start, end)),
    );

    let (start, end) = window_contains_prop(earliest, latest);
    bench_node_temporal_value(
        &mut node_group,
        "window_contains_some",
        &graph,
        node_prop_id,
        Some((start, end)),
    );

    let (start, end) = window_after_prop(earliest, latest);
    bench_node_temporal_value(
        &mut node_group,
        "window_after_none",
        &graph,
        node_prop_id,
        Some((start, end)),
    );

    node_group.finish();
    eprintln!("Node benchmarks complete.");

    // --- Edge benchmarks (no layers) ---
    eprintln!("Starting edge benchmarks...");
    let mut edge_group = c.benchmark_group("edge_temporal_value");

    // No window (100% coverage, returns Some)
    bench_edge_temporal_value(
        &mut edge_group,
        "no_window_some",
        &graph,
        edge_prop_id,
        None,
        None,
    );

    // Window coverage benchmarks
    for coverage in [0.1, 0.5, 1.0] {
        let (start, end) = window_for_coverage(earliest, latest, coverage);
        let name = format!("window_{}pct_some", (coverage * 100.0) as u32);
        bench_edge_temporal_value(
            &mut edge_group,
            &name,
            &graph,
            edge_prop_id,
            Some((start, end)),
            None,
        );
    }

    // Window position benchmarks
    let (start, end) = window_before_prop(earliest, latest);
    bench_edge_temporal_value(
        &mut edge_group,
        "window_before_none",
        &graph,
        edge_prop_id,
        Some((start, end)),
        None,
    );

    let (start, end) = window_contains_prop(earliest, latest);
    bench_edge_temporal_value(
        &mut edge_group,
        "window_contains_some",
        &graph,
        edge_prop_id,
        Some((start, end)),
        None,
    );

    let (start, end) = window_after_prop(earliest, latest);
    bench_edge_temporal_value(
        &mut edge_group,
        "window_after_none",
        &graph,
        edge_prop_id,
        Some((start, end)),
        None,
    );

    edge_group.finish();
    eprintln!("Edge benchmarks complete.");

    // ========== Layered graph benchmarks ==========
    for num_layers in [1, 2] {
        eprintln!("Setting up layered graph with {} layers...", num_layers);
        let (layered_graph, _node_prop_id, edge_prop_id, earliest, latest, layer_names) =
            setup_layered_graph_with_props(num_layers);
        eprintln!(
            "Layered graph setup complete: {} nodes, {} edges",
            layered_graph.count_nodes(),
            layered_graph.count_edges()
        );

        let mut layered_group =
            c.benchmark_group(format!("edge_temporal_value_{}layers", num_layers));

        // No window, with layer filter
        bench_edge_temporal_value(
            &mut layered_group,
            "no_window_some",
            &layered_graph,
            edge_prop_id,
            None,
            Some(&layer_names),
        );

        // Window coverage with layers
        for coverage in [0.1, 0.5, 1.0] {
            let (start, end) = window_for_coverage(earliest, latest, coverage);
            let name = format!("window_{}pct_some", (coverage * 100.0) as u32);
            bench_edge_temporal_value(
                &mut layered_group,
                &name,
                &layered_graph,
                edge_prop_id,
                Some((start, end)),
                Some(&layer_names),
            );
        }

        // Window position with layers
        let (start, end) = window_before_prop(earliest, latest);
        bench_edge_temporal_value(
            &mut layered_group,
            "window_before_none",
            &layered_graph,
            edge_prop_id,
            Some((start, end)),
            Some(&layer_names),
        );

        let (start, end) = window_contains_prop(earliest, latest);
        bench_edge_temporal_value(
            &mut layered_group,
            "window_contains_some",
            &layered_graph,
            edge_prop_id,
            Some((start, end)),
            Some(&layer_names),
        );

        let (start, end) = window_after_prop(earliest, latest);
        bench_edge_temporal_value(
            &mut layered_group,
            "window_after_none",
            &layered_graph,
            edge_prop_id,
            Some((start, end)),
            Some(&layer_names),
        );

        layered_group.finish();
    }
}

criterion_group!(benches, temporal_props_benchmarks);
criterion_main!(benches);
