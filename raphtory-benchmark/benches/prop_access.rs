//! Property access: the cost of reading one property of a node, an edge or the
//! graph, for temporal properties and for metadata.
//!
//! The storage measured depends on the workspace the bench is built in:
//!   open_source    built from `Raphtory/`: the open-source in-memory storage
//!   disk           built from the `v4-storage` root: a disk graph as ingestion
//!                  leaves it, with each segment spread over several components
//!   disk_vacuumed  the same disk graph after `vacuum()`, which compacts every node
//!                  and edge segment into one component; graph properties are
//!                  left as they are, so graph reads should match `disk`
//!
//! Run the same command in both workspaces and compare the results by group:
//!   cd Raphtory && cargo bench -p raphtory-benchmark --bench prop_access
//!   cargo bench -p raphtory-benchmark --bench prop_access
//!
//! The data is written in `ROUNDS` rounds, every round updating the temporal
//! property of every node, edge and the graph. The disk graph is flushed after
//! each round, so each round lands in its own disk component until the default
//! merge policy compacts them: after the last round every segment holds three.
//! Metadata is written in the first round only, so before vacuum it sits in the
//! oldest component.
//!
//! Each read is timed over a fixed random sample of nodes and edges; throughput
//! is reported in reads per second.

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use rand::{rngs::StdRng, RngExt, SeedableRng};
use raphtory::{
    db::api::storage::storage::{Extension, PersistenceStrategy},
    prelude::*,
};
use std::hint::black_box;
use tempfile::TempDir;

const NUM_NODES: u64 = 100_000;
const OUT_DEGREE: u64 = 5;
/// Edge `k` of `src` goes to `src + k * STRIDE`; `OUT_DEGREE * STRIDE < NUM_NODES`
/// keeps every target distinct and different from `src`.
const STRIDE: u64 = 7_919;
const ROUNDS: i64 = 5;
const UPDATES_PER_ROUND: i64 = 2;
/// Every entity has one temporal update at each time in `0..NUM_TIMES`.
const NUM_TIMES: i64 = ROUNDS * UPDATES_PER_ROUND;
/// End of the window for windowed reads, halfway through the history.
const MID: i64 = NUM_TIMES / 2;
const SAMPLE_SIZE: usize = 10_000;
const SEED: u64 = 42;

const TEMPORAL: &str = "temporal";
const METADATA: &str = "metadata";

struct Fixture {
    name: &'static str,
    graph: Graph,
    // After `graph`, so the graph is dropped before its directory is removed.
    _dir: Option<TempDir>,
}

fn out_edge(src: u64, k: u64) -> (u64, u64) {
    (src, (src + k * STRIDE) % NUM_NODES)
}

fn out_edges(src: u64) -> impl Iterator<Item = (u64, u64)> {
    (1..=OUT_DEGREE).map(move |k| out_edge(src, k))
}

/// Written in a fixed order so every graph gets the same node and edge ids.
fn write_round(graph: &Graph, round: i64) {
    for t in round * UPDATES_PER_ROUND..(round + 1) * UPDATES_PER_ROUND {
        for node in 0..NUM_NODES {
            graph
                .add_node(t, node, [(TEMPORAL, Prop::I64(t))], None, None)
                .unwrap();
        }
        for (src, dst) in (0..NUM_NODES).flat_map(out_edges) {
            graph
                .add_edge(t, src, dst, [(TEMPORAL, Prop::I64(t))], None)
                .unwrap();
        }
        graph.add_properties(t, [(TEMPORAL, Prop::I64(t))]).unwrap();
    }

    if round == 0 {
        for node in 0..NUM_NODES {
            graph
                .node(node)
                .unwrap()
                .add_metadata([(METADATA, Prop::I64(node as i64))])
                .unwrap();
        }
        for (src, dst) in (0..NUM_NODES).flat_map(out_edges) {
            graph
                .edge(src, dst)
                .unwrap()
                .add_metadata([(METADATA, Prop::I64(dst as i64))], None)
                .unwrap();
        }
        graph.add_metadata([(METADATA, Prop::I64(0))]).unwrap();
    }
}

fn open_source_graph() -> Fixture {
    let graph = Graph::new();
    for round in 0..ROUNDS {
        write_round(&graph, round);
    }
    Fixture {
        name: "open_source",
        graph,
        _dir: None,
    }
}

/// Synchronous flushes keep the component layout the same from run to run.
fn disk_args() -> Args {
    serde_json::from_value(serde_json::json!({
        "persistence": {
            "wal_enabled": false,
            "fsync_on_flush": false,
            "bg_flush": false,
        }
    }))
    .unwrap()
}

fn disk_graph(vacuum: bool) -> Fixture {
    let dir = TempDir::new().unwrap();
    let graph = Graph::new_at_path_with_config(dir.path(), disk_args()).unwrap();
    for round in 0..ROUNDS {
        write_round(&graph, round);
        graph.flush().unwrap();
    }
    if vacuum {
        graph.vacuum().unwrap();
    }
    Fixture {
        name: if vacuum { "disk_vacuumed" } else { "disk" },
        graph,
        _dir: Some(dir),
    }
}

/// Fail before benchmarking a graph that does not read back what was written,
/// e.g. one that lost data in vacuum.
fn check(graph: &Graph) {
    let latest = Some(Prop::I64(NUM_TIMES - 1));
    let windowed = Some(Prop::I64(MID - 1));
    let history = Some(NUM_TIMES as usize);
    let (src, dst) = out_edge(NUM_NODES - 1, OUT_DEGREE);

    let node = graph.node(src).unwrap();
    assert_eq!(node.properties().get(TEMPORAL), latest);
    assert_eq!(
        graph
            .before(MID)
            .node(src)
            .unwrap()
            .properties()
            .get(TEMPORAL),
        windowed
    );
    assert_eq!(
        node.properties()
            .temporal()
            .get(TEMPORAL)
            .map(|p| p.iter().count()),
        history
    );
    assert_eq!(node.metadata().get(METADATA), Some(Prop::I64(src as i64)));

    let edge = graph.edge(src, dst).unwrap();
    assert_eq!(edge.properties().get(TEMPORAL), latest);
    assert_eq!(
        graph
            .before(MID)
            .edge(src, dst)
            .unwrap()
            .properties()
            .get(TEMPORAL),
        windowed
    );
    assert_eq!(
        edge.properties()
            .temporal()
            .get(TEMPORAL)
            .map(|p| p.iter().count()),
        history
    );
    assert_eq!(edge.metadata().get(METADATA), Some(Prop::I64(dst as i64)));

    assert_eq!(graph.properties().get(TEMPORAL), latest);
    assert_eq!(graph.before(MID).properties().get(TEMPORAL), windowed);
    assert_eq!(
        graph
            .properties()
            .temporal()
            .get(TEMPORAL)
            .map(|p| p.iter().count()),
        history
    );
    assert_eq!(graph.metadata().get(METADATA), Some(Prop::I64(0)));
}

/// One group per kind of read, with one benchmark per storage variant in it.
/// `prepare` builds the entities to read from a fixture's graph outside the
/// measurement; `read` performs `reads` property reads on them.
fn bench_reads<T>(
    c: &mut Criterion,
    group_name: &str,
    fixtures: &[Fixture],
    reads: usize,
    prepare: impl Fn(&Graph) -> T,
    read: impl Fn(&T),
) {
    let mut group = c.benchmark_group(group_name);
    group.throughput(Throughput::Elements(reads as u64));
    for fixture in fixtures {
        let input = prepare(&fixture.graph);
        group.bench_function(fixture.name, |b| b.iter(|| read(&input)));
    }
    group.finish();
}

fn prop_access(c: &mut Criterion) {
    let fixtures = if Extension::disk_storage_enabled() {
        vec![disk_graph(false), disk_graph(true)]
    } else {
        vec![open_source_graph()]
    };
    for fixture in &fixtures {
        check(&fixture.graph);
    }

    let mut rng = StdRng::seed_from_u64(SEED);
    let node_ids: Vec<u64> = (0..SAMPLE_SIZE)
        .map(|_| rng.random_range(0..NUM_NODES))
        .collect();
    let edge_ids: Vec<(u64, u64)> = (0..SAMPLE_SIZE)
        .map(|_| {
            out_edge(
                rng.random_range(0..NUM_NODES),
                rng.random_range(1..=OUT_DEGREE),
            )
        })
        .collect();

    let nodes = |g: &Graph| -> Vec<_> { node_ids.iter().map(|&id| g.node(id).unwrap()).collect() };
    let windowed_nodes = |g: &Graph| -> Vec<_> {
        let g = g.before(MID);
        node_ids.iter().map(|&id| g.node(id).unwrap()).collect()
    };
    let edges = |g: &Graph| -> Vec<_> {
        edge_ids
            .iter()
            .map(|&(src, dst)| g.edge(src, dst).unwrap())
            .collect()
    };
    let windowed_edges = |g: &Graph| -> Vec<_> {
        let g = g.before(MID);
        edge_ids
            .iter()
            .map(|&(src, dst)| g.edge(src, dst).unwrap())
            .collect()
    };

    bench_reads(
        c,
        "node_temporal_latest",
        &fixtures,
        SAMPLE_SIZE,
        nodes,
        |nodes| {
            for node in nodes {
                black_box(node.properties().get(TEMPORAL));
            }
        },
    );
    bench_reads(
        c,
        "node_temporal_latest_windowed",
        &fixtures,
        SAMPLE_SIZE,
        windowed_nodes,
        |nodes| {
            for node in nodes {
                black_box(node.properties().get(TEMPORAL));
            }
        },
    );
    bench_reads(
        c,
        "node_temporal_history",
        &fixtures,
        SAMPLE_SIZE,
        nodes,
        |nodes| {
            for node in nodes {
                black_box(
                    node.properties()
                        .temporal()
                        .get(TEMPORAL)
                        .map(|p| p.iter().count()),
                );
            }
        },
    );
    bench_reads(c, "node_metadata", &fixtures, SAMPLE_SIZE, nodes, |nodes| {
        for node in nodes {
            black_box(node.metadata().get(METADATA));
        }
    });

    bench_reads(
        c,
        "edge_temporal_latest",
        &fixtures,
        SAMPLE_SIZE,
        edges,
        |edges| {
            for edge in edges {
                black_box(edge.properties().get(TEMPORAL));
            }
        },
    );
    bench_reads(
        c,
        "edge_temporal_latest_windowed",
        &fixtures,
        SAMPLE_SIZE,
        windowed_edges,
        |edges| {
            for edge in edges {
                black_box(edge.properties().get(TEMPORAL));
            }
        },
    );
    bench_reads(
        c,
        "edge_temporal_history",
        &fixtures,
        SAMPLE_SIZE,
        edges,
        |edges| {
            for edge in edges {
                black_box(
                    edge.properties()
                        .temporal()
                        .get(TEMPORAL)
                        .map(|p| p.iter().count()),
                );
            }
        },
    );
    bench_reads(c, "edge_metadata", &fixtures, SAMPLE_SIZE, edges, |edges| {
        for edge in edges {
            black_box(edge.metadata().get(METADATA));
        }
    });

    bench_reads(
        c,
        "graph_temporal_latest",
        &fixtures,
        1,
        Graph::clone,
        |g| {
            black_box(g.properties().get(TEMPORAL));
        },
    );
    bench_reads(
        c,
        "graph_temporal_latest_windowed",
        &fixtures,
        1,
        |g| g.before(MID),
        |g| {
            black_box(g.properties().get(TEMPORAL));
        },
    );
    bench_reads(
        c,
        "graph_temporal_history",
        &fixtures,
        1,
        Graph::clone,
        |g| {
            black_box(
                g.properties()
                    .temporal()
                    .get(TEMPORAL)
                    .map(|p| p.iter().count()),
            );
        },
    );
    bench_reads(c, "graph_metadata", &fixtures, 1, Graph::clone, |g| {
        black_box(g.metadata().get(METADATA));
    });
}

criterion_group!(benches, prop_access);
criterion_main!(benches);
