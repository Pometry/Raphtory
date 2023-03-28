use common::run_analysis_benchmarks;
use criterion::{
    criterion_group, criterion_main, AxisScale, Criterion, PlotConfiguration, Throughput,
};
use docbrown_db::graph_loader::sx_superuser_graph::sx_superuser_graph;
use docbrown_db::view_api::GraphViewOps;

mod common;

pub fn graph(c: &mut Criterion) {
    let mut graph_group = c.benchmark_group("analysis_graph");
    let graph = sx_superuser_graph(2).unwrap();
    run_analysis_benchmarks(&mut graph_group, || graph.clone(), None);
    graph_group.finish();
    let mut graph_window_group_100 = c.benchmark_group("analysis_graph_window_100");
    graph_window_group_100.sample_size(10);
    run_analysis_benchmarks(
        &mut graph_window_group_100,
        || graph.window(i64::MIN, i64::MAX),
        None,
    );
    graph_window_group_100.finish();
    let mut graph_window_group_10 = c.benchmark_group("analysis_graph_window_10");
    let latest = graph.latest_time().expect("non-empty graph").unwrap();
    let earliest = graph.earliest_time().expect("non-empty graph").unwrap();
    let start = latest - (latest - earliest) / 10;
    graph_window_group_10.sample_size(10);
    run_analysis_benchmarks(
        &mut graph_window_group_10,
        || graph.window(start, latest + 1),
        None,
    );
    graph_window_group_10.finish();
}

criterion_group!(benches, graph);
criterion_main!(benches);
