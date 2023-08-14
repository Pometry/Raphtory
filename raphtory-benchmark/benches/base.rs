use crate::common::{bootstrap_graph, run_analysis_benchmarks, run_large_ingestion_benchmarks};
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use raphtory::{graph_loader::example::lotr_graph::lotr_graph, prelude::*};

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
    let mut graph_group = c.benchmark_group("lotr_graph");
    let graph = lotr_graph();
    run_analysis_benchmarks(&mut graph_group, || graph.clone(), None);
    graph_group.finish();
    let mut graph_window_group_100 = c.benchmark_group("lotr_graph_window_100");
    graph_window_group_100.sample_size(10);
    run_analysis_benchmarks(
        &mut graph_window_group_100,
        || graph.window(i64::MIN, i64::MAX),
        None,
    );
    graph_window_group_100.finish();
    let mut graph_window_group_10 = c.benchmark_group("lotr_graph_window_10");
    let latest = graph.end().expect("non-empty graph");
    let earliest = graph.start().expect("non-empty graph");
    let start = latest - (latest - earliest) / 10;
    graph_window_group_10.sample_size(10);
    run_analysis_benchmarks(
        &mut graph_window_group_10,
        || graph.window(start, latest + 1),
        None,
    );
    graph_window_group_10.finish();
}

criterion_group!(benches, base);
criterion_main!(benches);
