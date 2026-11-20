//! Property access: the cost of reading one property of a node, an edge or the
//! graph, for temporal properties and for metadata, for each property type in
//! [`PROP_KINDS`].
//!
//! The storage measured:
//!   open_source    a graph created without a path, built from `Raphtory/`: all
//!                  data stays in mutable in-memory segments
//!   in_memory      the same, built from the `v4-storage` root: past half its
//!                  memory budget, segments are frozen into immutable in-memory
//!                  components, as on disk but never written out
//!   disk           a disk graph as ingestion leaves it, with each segment spread
//!                  over several components; built from the `v4-storage` root only
//!   disk_vacuumed  the same disk graph after `vacuum()`, which compacts every node
//!                  and edge segment into one component; graph properties are
//!                  left as they are, so graph reads should match `disk`
//!
//! Run the same command in both workspaces and compare the results by group:
//!   cd Raphtory && cargo bench -p raphtory-benchmark --bench prop_access
//!   cargo bench -p raphtory-benchmark --bench prop_access
//!
//! Benchmarks are named `<read>/<property type>/<storage>`.
//! The data is written in `ROUNDS` rounds, every round updating every temporal
//! property of every node, edge and the graph. The default merge policy compacts the components
//! when the graph is flushed; after the last round, every node and edge segment holds three.
//! Metadata is written in the first round only, so before vacuum it sits in the
//! oldest component.
//!
//! Each read is timed over a fixed random sample of nodes and edges; throughput
//! is reported in reads per second.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
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
const LIST_LEN: i64 = 4;

/// A property type, written under its own keys as a temporal property and as
/// metadata.
struct PropKind {
    name: &'static str,
    temporal_key: &'static str,
    metadata_key: &'static str,
    /// The value. For temporal properties, it is associated with the update time.
    /// For metadata, it is associated with an id.
    prop_value: fn(i64) -> Prop,
}

const PROP_KINDS: [PropKind; 4] = [
    PropKind {
        name: "i64_prop",
        temporal_key: "temporal_i64",
        metadata_key: "metadata_i64",
        prop_value: Prop::I64,
    },
    PropKind {
        name: "str_prop",
        temporal_key: "temporal_str",
        metadata_key: "metadata_str",
        prop_value: str_value,
    },
    PropKind {
        name: "list_prop",
        temporal_key: "temporal_list",
        metadata_key: "metadata_list",
        prop_value: list_value,
    },
    PropKind {
        name: "map_prop",
        temporal_key: "temporal_map",
        metadata_key: "metadata_map",
        prop_value: map_value,
    },
];

fn str_value(x: i64) -> Prop {
    Prop::str(format!("value {x}"))
}

fn list_value(x: i64) -> Prop {
    Prop::list_unchecked(x..x + LIST_LEN)
}

fn map_value(x: i64) -> Prop {
    Prop::map([("int", Prop::I64(x)), ("str", str_value(x))])
}

/// Every temporal property at time `t`. Built afresh for each entity so no two
/// entities share an allocation.
fn temporal_props(t: i64) -> impl ExactSizeIterator<Item = (&'static str, Prop)> {
    PROP_KINDS
        .iter()
        .map(move |kind| (kind.temporal_key, (kind.prop_value)(t)))
}

fn metadata_props(x: i64) -> impl ExactSizeIterator<Item = (&'static str, Prop)> {
    PROP_KINDS
        .iter()
        .map(move |kind| (kind.metadata_key, (kind.prop_value)(x)))
}

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
                .add_node(t, node, temporal_props(t), None, None)
                .unwrap();
        }
        for (src, dst) in (0..NUM_NODES).flat_map(out_edges) {
            graph
                .add_edge(t, src, dst, temporal_props(t), None)
                .unwrap();
        }
        graph.add_properties(t, temporal_props(t)).unwrap();
    }

    if round == 0 {
        for node in 0..NUM_NODES {
            graph
                .node(node)
                .unwrap()
                .add_metadata(metadata_props(node as i64))
                .unwrap();
        }
        for (src, dst) in (0..NUM_NODES).flat_map(out_edges) {
            graph
                .edge(src, dst)
                .unwrap()
                .add_metadata(metadata_props(dst as i64), None)
                .unwrap();
        }
        graph.add_metadata(metadata_props(0)).unwrap();
    }
}

fn in_memory_graph() -> Fixture {
    let graph = Graph::new();
    for round in 0..ROUNDS {
        write_round(&graph, round);
    }
    Fixture {
        name: if Extension::disk_storage_enabled() {
            "in_memory"
        } else {
            "open_source"
        },
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
    let (src, dst) = out_edge(NUM_NODES - 1, OUT_DEGREE);
    let node = graph.node(src).unwrap();
    let edge = graph.edge(src, dst).unwrap();
    let windowed = graph.before(MID);
    let windowed_node = windowed.node(src).unwrap();
    let windowed_edge = windowed.edge(src, dst).unwrap();

    for kind in &PROP_KINDS {
        let key = kind.temporal_key;
        let latest = Some((kind.prop_value)(NUM_TIMES - 1));
        let windowed_latest = Some((kind.prop_value)(MID - 1));
        let history = Some(NUM_TIMES as usize);

        assert_eq!(node.properties().get(key), latest);
        assert_eq!(windowed_node.properties().get(key), windowed_latest);
        assert_eq!(
            node.properties()
                .temporal()
                .get(key)
                .map(|p| p.iter().count()),
            history
        );
        assert_eq!(
            node.metadata().get(kind.metadata_key),
            Some((kind.prop_value)(src as i64))
        );

        assert_eq!(edge.properties().get(key), latest);
        assert_eq!(windowed_edge.properties().get(key), windowed_latest);
        assert_eq!(
            edge.properties()
                .temporal()
                .get(key)
                .map(|p| p.iter().count()),
            history
        );
        assert_eq!(
            edge.metadata().get(kind.metadata_key),
            Some((kind.prop_value)(dst as i64))
        );

        assert_eq!(graph.properties().get(key), latest);
        assert_eq!(windowed.properties().get(key), windowed_latest);
        assert_eq!(
            graph
                .properties()
                .temporal()
                .get(key)
                .map(|p| p.iter().count()),
            history
        );
        assert_eq!(graph.metadata().get(kind.metadata_key), Some((kind.prop_value)(0)));
    }
}

/// One group per kind of read, with one benchmark per property type and storage
/// variant in it. `prepare` builds the entities to read from a fixture's graph
/// outside the measurement; `read` performs `reads` reads of one property type
/// on them.
fn bench_reads<T>(
    c: &mut Criterion,
    group_name: &str,
    fixtures: &[Fixture],
    reads: usize,
    prepare: impl Fn(&Graph) -> T,
    read: impl Fn(&T, &PropKind),
) {
    let inputs: Vec<_> = fixtures
        .iter()
        .map(|fixture| (fixture.name, prepare(&fixture.graph)))
        .collect();
    let mut group = c.benchmark_group(group_name);
    group.throughput(Throughput::Elements(reads as u64));
    for kind in &PROP_KINDS {
        for (name, input) in &inputs {
            group.bench_function(BenchmarkId::new(kind.name, name), |b| {
                b.iter(|| read(input, kind))
            });
        }
    }
    group.finish();
}

fn prop_access(c: &mut Criterion) {
    let fixtures = if Extension::disk_storage_enabled() {
        vec![in_memory_graph(), disk_graph(false), disk_graph(true)]
    } else {
        vec![in_memory_graph()]
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
        |nodes, kind| {
            for node in nodes {
                black_box(node.properties().get(kind.temporal_key));
            }
        },
    );
    bench_reads(
        c,
        "node_temporal_latest_windowed",
        &fixtures,
        SAMPLE_SIZE,
        windowed_nodes,
        |nodes, kind| {
            for node in nodes {
                black_box(node.properties().get(kind.temporal_key));
            }
        },
    );
    bench_reads(
        c,
        "node_temporal_history",
        &fixtures,
        SAMPLE_SIZE,
        nodes,
        |nodes, kind| {
            for node in nodes {
                if let Some(history) = node.properties().temporal().get(kind.temporal_key) {
                    for entry in history.iter() {
                        black_box(entry);
                    }
                }
            }
        },
    );
    bench_reads(
        c,
        "node_metadata",
        &fixtures,
        SAMPLE_SIZE,
        nodes,
        |nodes, kind| {
            for node in nodes {
                black_box(node.metadata().get(kind.metadata_key));
            }
        },
    );

    bench_reads(
        c,
        "edge_temporal_latest",
        &fixtures,
        SAMPLE_SIZE,
        edges,
        |edges, kind| {
            for edge in edges {
                black_box(edge.properties().get(kind.temporal_key));
            }
        },
    );
    bench_reads(
        c,
        "edge_temporal_latest_windowed",
        &fixtures,
        SAMPLE_SIZE,
        windowed_edges,
        |edges, kind| {
            for edge in edges {
                black_box(edge.properties().get(kind.temporal_key));
            }
        },
    );
    bench_reads(
        c,
        "edge_temporal_history",
        &fixtures,
        SAMPLE_SIZE,
        edges,
        |edges, kind| {
            for edge in edges {
                if let Some(history) = edge.properties().temporal().get(kind.temporal_key) {
                    for entry in history.iter() {
                        black_box(entry);
                    }
                }
            }
        },
    );
    bench_reads(
        c,
        "edge_metadata",
        &fixtures,
        SAMPLE_SIZE,
        edges,
        |edges, kind| {
            for edge in edges {
                black_box(edge.metadata().get(kind.metadata_key));
            }
        },
    );

    bench_reads(
        c,
        "graph_temporal_latest",
        &fixtures,
        1,
        Graph::clone,
        |g, kind| {
            black_box(g.properties().get(kind.temporal_key));
        },
    );
    bench_reads(
        c,
        "graph_temporal_latest_windowed",
        &fixtures,
        1,
        |g| g.before(MID),
        |g, kind| {
            black_box(g.properties().get(kind.temporal_key));
        },
    );
    bench_reads(
        c,
        "graph_temporal_history",
        &fixtures,
        1,
        Graph::clone,
        |g, kind| {
            if let Some(history) = g.properties().temporal().get(kind.temporal_key) {
                for entry in history.iter() {
                    black_box(entry);
                }
            }
        },
    );
    bench_reads(
        c,
        "graph_metadata",
        &fixtures,
        1,
        Graph::clone,
        |g, kind| {
            black_box(g.metadata().get(kind.metadata_key));
        },
    );
}

criterion_group!(benches, prop_access);
criterion_main!(benches);
