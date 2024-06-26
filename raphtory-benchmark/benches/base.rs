use crate::common::{bootstrap_graph, run_large_ingestion_benchmarks};
use common::run_graph_ops_benches;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use raphtory::{graph_loader::lotr_graph::lotr_graph, prelude::*};

mod common;

pub fn base(c: &mut Criterion) {
    // let mut ingestion_group = c.benchmark_group("ingestion");
    // ingestion_group.throughput(Throughput::Elements(1));
    // run_ingestion_benchmarks(&mut ingestion_group, || bootstrap_graph(4, 10_000), None);
    // ingestion_group.finish();
    //
    // let mut analysis_group = c.benchmark_group("analysis");
    // run_analysis_benchmarks(&mut analysis_group, || lotr_graph(4), None);
    // analysis_group.finish();
    let mut large_group = c.benchmark_group("large");
    large_group.warm_up_time(std::time::Duration::from_secs(1));
    large_group.sample_size(10);
    large_group.throughput(Throughput::Elements(1_000));
    large_group.measurement_time(std::time::Duration::from_secs(3));
    // Make an option of None
    run_large_ingestion_benchmarks(&mut large_group, || bootstrap_graph(10000), None);
    large_group.finish();

    let graph = lotr_graph();

    let layered_graph = Graph::new();

    for layer in (0..10).map(|i| i.to_string()) {
        for edge in graph.edges() {
            for t in edge.history() {
                layered_graph
                    .add_edge(
                        t,
                        edge.src().name().clone(),
                        edge.dst().name().clone(),
                        NO_PROPS,
                        Some(&layer),
                    )
                    .expect("Error: Unable to add edge");
            }
        }
    }

    run_graph_ops_benches(c, "lotr", graph, layered_graph)
}

criterion_group!(benches, base);
criterion_main!(benches);
