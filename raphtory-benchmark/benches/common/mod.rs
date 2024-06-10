#![allow(dead_code)]

use criterion::{
    black_box, measurement::WallTime, BatchSize, Bencher, BenchmarkGroup, BenchmarkId,
};
use rand::{distributions::Uniform, seq::*, Rng};
use raphtory::{core::entities::LayerIds, db::api::view::StaticGraphViewOps, prelude::*};
use std::collections::HashSet;

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

pub fn bootstrap_graph(num_nodes: usize) -> Graph {
    let graph = Graph::new();
    let mut indexes = make_index_gen();
    let mut times = make_time_gen();
    let num_edges = num_nodes / 2;
    for _ in 0..num_edges {
        let source = indexes.next().unwrap();
        let target = indexes.next().unwrap();
        let time = times.next().unwrap();
        graph
            .add_edge(time, source, target, NO_PROPS, None)
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

pub fn run_large_ingestion_benchmarks<F>(
    group: &mut BenchmarkGroup<WallTime>,
    mut make_graph: F,
    parameter: Option<usize>,
) where
    F: FnMut() -> Graph,
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
                        make_graph(),
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
                        make_graph(),
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
                        make_graph(),
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
    let graph = make_graph();
    println!(
        "Num layers {:?}, node count: {}, edge_count: {}",
        graph.unique_layers().count(),
        graph.count_nodes(),
        graph.count_edges()
    );
    let edges: HashSet<(u64, u64)> = graph
        .edges()
        .into_iter()
        .map(|e| (e.src().id(), e.dst().id()))
        .collect();

    let edges_t = graph
        .edges()
        .explode()
        .into_iter()
        .map(|e| (e.src().id(), e.dst().id(), e.time().expect("need time")))
        .collect::<Vec<_>>();

    let nodes: HashSet<u64> = graph.nodes().id().collect();

    bench(group, "num_edges", parameter, |b: &mut Bencher| {
        b.iter(|| graph.count_edges())
    });

    bench(group, "num_edges_temporal", parameter, |b: &mut Bencher| {
        b.iter(|| graph.count_temporal_edges())
    });

    bench(group, "has_edge_existing", parameter, |b: &mut Bencher| {
        let mut rng = rand::thread_rng();
        let edge = edges.iter().choose(&mut rng).expect("non-empty graph");
        b.iter(|| graph.has_edge(edge.0, edge.1))
    });

    bench(
        group,
        "has_edge_nonexisting",
        parameter,
        |b: &mut Bencher| {
            let mut rng = rand::thread_rng();
            let edge = loop {
                let edge: (u64, u64) = (
                    *nodes.iter().choose(&mut rng).expect("non-empty graph"),
                    *nodes.iter().choose(&mut rng).expect("non-empty graph"),
                );
                if !edges.contains(&edge) {
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
        let v = *nodes.iter().choose(&mut rng).expect("non-empty graph");
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
                if !nodes.contains(&v) {
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

    bench(group, "materialize", parameter, |b: &mut Bencher| {
        b.iter(|| {
            let mg = graph.materialize();
            black_box(mg)
        })
    })

    // Too noisy due to degree variability and confuses criterion
    // bench(
    //     group,
    //     "max_neighbour_degree",
    //     parameter,
    //     |b: &mut Bencher| {
    //         let mut rng = rand::thread_rng();
    //         let v = graph
    //             .node(*nodes.iter().choose(&mut rng).expect("non-empty graph"))
    //             .expect("existing node");
    //         b.iter(|| v.neighbours().degree().max())
    //     },
    // );
}
