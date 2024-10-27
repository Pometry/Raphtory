#![allow(dead_code)]

use criterion::{
    black_box, measurement::WallTime, BatchSize, Bencher, BenchmarkGroup, BenchmarkId, Criterion,
};
use rand::{distributions::Uniform, seq::*, Rng, SeedableRng};
use raphtory::{db::api::view::StaticGraphViewOps, prelude::*};
use raphtory_api::core::utils::logging::global_info_logger;
use std::collections::HashSet;
use tempfile::TempDir;
use tracing::info;

fn make_index_gen() -> Box<dyn Iterator<Item = u64>> {
    let rng = rand::thread_rng();
    let range = Uniform::new(u64::MIN, u64::MAX);
    Box::new(rng.sample_iter(range))
}

fn make_time_gen() -> Box<dyn Iterator<Item = i64>> {
    let rng = rand::thread_rng();
    let range = Uniform::new(i64::MIN, i64::MAX);
    Box::new(rng.sample_iter(range))
}

pub fn bootstrap_graph(num_nodes: usize, as_gid: impl Fn(u64) -> GID) -> Graph {
    let graph = Graph::new();
    let mut indexes = make_index_gen();
    let mut times = make_time_gen();
    let num_edges = num_nodes / 2;
    for _ in 0..num_edges {
        let source = indexes.next().unwrap();
        let target = indexes.next().unwrap();
        let time = times.next().unwrap();
        graph
            .add_edge(time, as_gid(source), as_gid(target), NO_PROPS, None)
            .unwrap();
    }
    graph
}

pub fn bench<F>(
    group: &mut BenchmarkGroup<WallTime>,
    name: &str,
    parameter: Option<usize>,
    mut task: F,
) where
    F: FnMut(&mut Bencher<'_, WallTime>),
{
    match parameter {
        Some(parameter) => group.bench_with_input(
            BenchmarkId::new(name, parameter),
            &parameter,
            |b: &mut Bencher, _| task(b),
        ),
        None => group.bench_function(name, task),
    };
}

pub fn run_ingestion_benchmarks<F>(
    group: &mut BenchmarkGroup<WallTime>,
    mut make_graph: F,
    parameter: Option<usize>,
) where
    F: FnMut() -> Graph,
{
    let mut indexes = make_index_gen();
    let mut times = make_time_gen();
    let mut index_sample = || indexes.next().unwrap();
    let mut time_sample = || times.next().unwrap();

    bench(
        group,
        "existing node varying time",
        parameter,
        |b: &mut Bencher| {
            b.iter_batched_ref(
                || (make_graph(), time_sample()),
                |(g, t): &mut (Graph, i64)| g.add_node(*t, 0, NO_PROPS, None),
                BatchSize::SmallInput,
            )
        },
    );
    bench(
        group,
        "new node constant time",
        parameter,
        |b: &mut Bencher| {
            b.iter_batched_ref(
                || (make_graph(), index_sample()),
                |(g, v): &mut (Graph, u64)| g.add_node(0, *v, NO_PROPS, None),
                BatchSize::SmallInput,
            )
        },
    );
    bench(
        group,
        "existing edge varying time",
        parameter,
        |b: &mut Bencher| {
            b.iter_batched_ref(
                || (make_graph(), time_sample()),
                |(g, t)| g.add_edge(*t, 0, 0, NO_PROPS, None),
                BatchSize::SmallInput,
            )
        },
    );
    bench(
        group,
        "new edge constant time",
        parameter,
        |b: &mut Bencher| {
            b.iter_batched_ref(
                || (make_graph(), index_sample(), index_sample()),
                |(g, s, d)| g.add_edge(0, *s, *d, NO_PROPS, None),
                BatchSize::SmallInput,
            )
        },
    );
}

fn times(n: usize) -> impl Iterator {
    std::iter::repeat(()).take(n)
}

pub fn run_large_ingestion_benchmarks<F, F2>(
    group: &mut BenchmarkGroup<WallTime>,
    mut make_graph: F,
    mut make_graph_str: F2,
    parameter: Option<usize>,
) where
    F: FnMut() -> Graph,
    F2: FnMut() -> Graph,
{
    let updates = 1000;

    bench(
        group,
        "1k fixed edge updates with varying time",
        parameter,
        |b: &mut Bencher| {
            b.iter_batched_ref(
                || {
                    (
                        make_graph(),
                        make_time_gen().take(updates).collect::<Vec<i64>>(),
                    )
                },
                |(g, times)| {
                    for t in times.iter() {
                        g.add_edge(*t, 0, 0, NO_PROPS, None).unwrap();
                    }
                },
                BatchSize::SmallInput,
            )
        },
    );

    bench(
        group,
        "1k fixed edge updates with varying time and numeric string input",
        parameter,
        |b: &mut Bencher| {
            b.iter_batched_ref(
                || {
                    (
                        make_graph_str(),
                        make_time_gen().take(updates).collect::<Vec<i64>>(),
                    )
                },
                |(g, times)| {
                    for t in times.iter() {
                        g.add_edge(*t, "0", "0", NO_PROPS, None).unwrap();
                    }
                },
                BatchSize::SmallInput,
            )
        },
    );

    bench(
        group,
        "1k fixed edge updates with varying time and string input",
        parameter,
        |b: &mut Bencher| {
            b.iter_batched_ref(
                || {
                    (
                        make_graph_str(),
                        make_time_gen().take(updates).collect::<Vec<i64>>(),
                    )
                },
                |(g, times)| {
                    for t in times.iter() {
                        g.add_edge(*t, "test", "other", NO_PROPS, None).unwrap();
                    }
                },
                BatchSize::SmallInput,
            )
        },
    );

    bench(
        group,
        "1k random edge additions",
        parameter,
        |b: &mut Bencher| {
            b.iter_batched_ref(
                || {
                    (
                        make_graph(),
                        make_index_gen(),
                        make_index_gen(),
                        make_time_gen().take(updates).collect::<Vec<i64>>(),
                    )
                },
                |(g, src_gen, dst_gen, times)| {
                    for t in times.iter() {
                        g.add_edge(
                            *t,
                            src_gen.next().unwrap(),
                            dst_gen.next().unwrap(),
                            NO_PROPS,
                            None,
                        )
                        .unwrap();
                    }
                },
                BatchSize::SmallInput,
            )
        },
    );

    bench(
        group,
        "1k random edge additions with numeric string input",
        parameter,
        |b: &mut Bencher| {
            b.iter_batched_ref(
                || {
                    (
                        make_graph_str(),
                        make_index_gen().map(|v| v.to_string()),
                        make_index_gen().map(|v| v.to_string()),
                        make_time_gen().take(updates).collect::<Vec<i64>>(),
                    )
                },
                |(g, src_gen, dst_gen, times)| {
                    for t in times.iter() {
                        g.add_edge(
                            *t,
                            src_gen.next().unwrap(),
                            dst_gen.next().unwrap(),
                            NO_PROPS,
                            None,
                        )
                        .unwrap();
                    }
                },
                BatchSize::SmallInput,
            )
        },
    );
}

pub fn run_analysis_benchmarks<F, G>(
    group: &mut BenchmarkGroup<WallTime>,
    make_graph: F,
    parameter: Option<usize>,
) where
    F: Fn() -> G,
    G: StaticGraphViewOps,
{
    global_info_logger();
    let graph = make_graph();
    info!(
        "Num layers {:?}, node count: {}, edge_count: {}",
        graph.unique_layers().count(),
        graph.count_nodes(),
        graph.count_edges()
    );
    let edges: HashSet<(GID, GID)> = graph.edges().id().collect();

    let edges_t = graph
        .edges()
        .explode()
        .into_iter()
        .map(|e| (e.src().id(), e.dst().id(), e.time().expect("need time")))
        .collect::<Vec<_>>();

    let nodes: HashSet<GID> = graph.nodes().id().collect();

    bench(group, "num_edges", parameter, |b: &mut Bencher| {
        b.iter(|| graph.count_edges())
    });

    bench(group, "num_edges_temporal", parameter, |b: &mut Bencher| {
        b.iter(|| graph.count_temporal_edges())
    });

    bench(group, "has_edge_existing", parameter, |b: &mut Bencher| {
        let mut rng = rand::thread_rng();
        let (src, dst) = edges.iter().choose(&mut rng).expect("non-empty graph");
        b.iter(|| graph.has_edge(src, dst))
    });

    bench(
        group,
        "has_edge_nonexisting",
        parameter,
        |b: &mut Bencher| {
            let mut rng = rand::thread_rng();
            let edge = loop {
                let edge: (&GID, &GID) = (
                    nodes.iter().choose(&mut rng).expect("non-empty graph"),
                    nodes.iter().choose(&mut rng).expect("non-empty graph"),
                );
                if !edges.contains(&(edge.0.clone(), edge.1.clone())) {
                    break edge;
                }
            };
            b.iter(|| graph.has_edge(edge.0, edge.1))
        },
    );

    bench(group, "active edge", parameter, |b: &mut Bencher| {
        let mut rng = rand::thread_rng();
        let (edge, active_t) = edges_t
            .choose(&mut rng)
            .and_then(|(src, dst, t)| graph.edge(src, dst).map(|e| (e, *t)))
            .expect("active edge");
        b.iter(|| {
            edge.window(active_t.saturating_sub(5), active_t + 5)
                .explode_layers()
                .iter()
                .for_each(|e| {
                    black_box(e);
                });
        });
    });

    bench(group, "edge has layer", parameter, |b: &mut Bencher| {
        let mut rng = rand::thread_rng();
        let edge = edges
            .iter()
            .choose(&mut rng)
            .and_then(|(src, dst)| graph.edge(src, dst))
            .expect("active edge");

        let layers = graph.unique_layers().collect::<Vec<_>>();
        b.iter(|| {
            for name in layers.iter() {
                black_box(edge.has_layer(name));
            }
        });
    });

    bench(group, "num_nodes", parameter, |b: &mut Bencher| {
        b.iter(|| graph.count_nodes())
    });

    bench(group, "has_node_existing", parameter, |b: &mut Bencher| {
        let mut rng = rand::thread_rng();
        let v = nodes.iter().choose(&mut rng).expect("non-empty graph");
        b.iter(|| graph.has_node(v))
    });

    bench(
        group,
        "has_node_nonexisting",
        parameter,
        |b: &mut Bencher| {
            let mut rng = rand::thread_rng();
            let v: u64 = loop {
                let v: u64 = rng.gen();
                if !nodes.contains(&GID::U64(v)) {
                    break v;
                }
            };
            b.iter(|| graph.has_node(v))
        },
    );

    bench(group, "max_id", parameter, |b: &mut Bencher| {
        b.iter(|| graph.nodes().id().max())
    });

    bench(group, "max_degree", parameter, |b: &mut Bencher| {
        b.iter(|| graph.nodes().degree().max())
    });

    bench(group, "iterate nodes", parameter, |b: &mut Bencher| {
        b.iter(|| {
            for n in graph.nodes() {
                black_box(n);
            }
        })
    });

    bench(group, "iterate edges", parameter, |b: &mut Bencher| {
        b.iter(|| {
            for e in graph.edges() {
                black_box(e);
            }
        })
    });

    bench(
        group,
        "iterate_exploded_edges",
        parameter,
        |b: &mut Bencher| {
            b.iter(|| {
                for e in graph.edges() {
                    for ee in e.explode() {
                        black_box(ee);
                    }
                }
            })
        },
    );

    bench(
        group,
        "max_neighbour_degree",
        parameter,
        |b: &mut Bencher| {
            let v = graph
                .nodes()
                .into_iter()
                .next()
                .expect("graph should not be empty");
            b.iter(|| v.neighbours().degree().max())
        },
    );
}

pub fn run_materialize<F, G>(
    group: &mut BenchmarkGroup<WallTime>,
    make_graph: F,
    parameter: Option<usize>,
) where
    F: Fn() -> G,
    G: StaticGraphViewOps,
{
    let graph = make_graph();
    bench(group, "materialize", parameter, |b: &mut Bencher| {
        b.iter(|| {
            let mg = graph.materialize();
            black_box(mg)
        })
    });
}

pub fn run_graph_ops_benches(
    c: &mut Criterion,
    graph_name: &str,
    graph: Graph,
    layered_graph: Graph,
) {
    let mut graph_group = c.benchmark_group(graph_name);
    let make_graph = || graph.clone();
    run_analysis_benchmarks(&mut graph_group, make_graph, None);
    graph_group.finish();

    bench_materialise(&format!("{graph_name}_materialise"), c, make_graph);

    let group_name = format!("{graph_name}_window_100");
    let make_graph = || graph.window(i64::MIN, i64::MAX);
    let mut graph_window_group_100 = c.benchmark_group(group_name);
    // graph_window_group_100.sample_size(10);
    run_analysis_benchmarks(&mut graph_window_group_100, make_graph, None);
    graph_window_group_100.finish();

    bench_materialise(
        &format!("{graph_name}_window_100_materialise"),
        c,
        make_graph,
    );

    // graph windowed
    let group_name = format!("{graph_name}_window_10");
    let mut graph_window_group_10 = c.benchmark_group(group_name);
    let latest = graph.latest_time().expect("non-empty graph");
    let earliest = graph.earliest_time().expect("non-empty graph");
    let start = latest - (latest - earliest) / 10;
    // graph_window_group_10.sample_size(10);
    let make_graph = || graph.window(start, latest + 1);
    run_analysis_benchmarks(&mut graph_window_group_10, make_graph, None);
    graph_window_group_10.finish();
    bench_materialise(
        &format!("{graph_name}_window_10_materialise"),
        c,
        make_graph,
    );

    // subgraph
    let mut rng = rand::rngs::StdRng::seed_from_u64(73);
    let nodes = graph
        .nodes()
        .into_iter()
        .choose_multiple(&mut rng, graph.count_nodes() / 10)
        .into_iter()
        .map(|n| n.id())
        .collect::<Vec<_>>();
    let subgraph = graph.subgraph(nodes);
    let group_name = format!("{graph_name}_subgraph_10pc");
    let mut subgraph_10 = c.benchmark_group(group_name);
    // subgraph_10.sample_size(10);

    let make_graph = || subgraph.clone();
    run_analysis_benchmarks(&mut subgraph_10, make_graph, None);
    subgraph_10.finish();
    bench_materialise(
        &format!("{graph_name}_subgraph_10pc_materialise"),
        c,
        make_graph,
    );

    // subgraph windowed
    let group_name = format!("{graph_name}_subgraph_10pc_windowed");
    let mut subgraph_10_windowed = c.benchmark_group(group_name);

    let make_graph = || subgraph.window(start, latest + 1);
    run_analysis_benchmarks(&mut subgraph_10_windowed, make_graph, None);
    subgraph_10_windowed.finish();
    bench_materialise(
        &format!("{graph_name}_subgraph_10pc_windowed_materialise"),
        c,
        make_graph,
    );

    // layered graph windowed
    let graph = layered_graph;
    let group_name = format!("{graph_name}_window_50_layered");
    let mut graph_window_layered_group_50 = c.benchmark_group(group_name);
    let latest = graph.latest_time().expect("non-empty graph");
    let earliest = graph.earliest_time().expect("non-empty graph");
    let start = latest - (latest - earliest) / 2;
    graph_window_layered_group_50.sample_size(10);
    let make_graph = || {
        graph
            .window(start, latest + 1)
            .layers(["0", "1", "2", "3", "4"])
            .unwrap()
    };
    run_analysis_benchmarks(&mut graph_window_layered_group_50, make_graph, None);
    graph_window_layered_group_50.finish();
    bench_materialise(
        &format!("{graph_name}_window_50_layered_materialise"),
        c,
        make_graph,
    );

    let graph = graph.persistent_graph();

    let group_name = format!("{graph_name}_persistent_window_50_layered");
    let mut graph_window_layered_group_50 = c.benchmark_group(group_name);
    let latest = graph.latest_time().expect("non-empty graph");
    let earliest = graph.earliest_time().expect("non-empty graph");
    let start = latest - (latest - earliest) / 2;
    graph_window_layered_group_50.sample_size(10);
    let make_graph = || {
        graph
            .window(start, latest + 1)
            .layers(["0", "1", "2", "3", "4"])
            .unwrap()
    };
    run_analysis_benchmarks(&mut graph_window_layered_group_50, make_graph, None);
    graph_window_layered_group_50.finish();
    bench_materialise(
        &format!("{graph_name}_persistent_window_50_layered_materialise"),
        c,
        make_graph,
    );
}

pub fn run_proto_encode_benchmark(group: &mut BenchmarkGroup<WallTime>, graph: Graph) {
    println!("graph: {graph}");
    bench(group, "proto_encode", None, |b: &mut Bencher| {
        b.iter_batched(
            || TempDir::new().unwrap(),
            |f| graph.encode(f.path()).unwrap(),
            BatchSize::NumIterations(100),
        );
    });
}

pub fn run_proto_decode_benchmark(group: &mut BenchmarkGroup<WallTime>, graph: Graph) {
    println!("graph: {graph}");
    let f = TempDir::new().unwrap();
    graph.encode(f.path()).unwrap();
    bench(group, "proto_decode", None, |b| {
        b.iter(|| Graph::decode(f.path()).unwrap())
    })
}

pub fn bench_materialise<F, G>(name: &str, c: &mut Criterion, make_graph: F)
where
    F: Fn() -> G,
    G: StaticGraphViewOps,
{
    let mut mat_graph_group = c.benchmark_group(name);
    mat_graph_group.sample_size(10);
    run_materialize(&mut mat_graph_group, make_graph, None);
    mat_graph_group.finish();
}
