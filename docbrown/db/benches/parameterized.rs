use crate::common::bootstrap_graph;
use common::{run_analysis_benchmarks, run_ingestion_benchmarks};
use criterion::{
    criterion_group, criterion_main, AxisScale, Criterion, PlotConfiguration, Throughput,
};

mod common;

pub fn parameterized(c: &mut Criterion) {
    let vertices_exponents = 1..6;
    let shards = 1..10;

    let vertices = vertices_exponents.map(|exp| 10usize.pow(exp));
    let mut ingestion_group = c.benchmark_group("ingestion-num_vertices");
    ingestion_group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));
    for num_vertices in vertices.clone() {
        let make_graph = || bootstrap_graph(4, num_vertices);
        ingestion_group.throughput(Throughput::Elements(num_vertices as u64));
        run_ingestion_benchmarks(&mut ingestion_group, make_graph, Some(num_vertices));
    }
    ingestion_group.finish();
    let mut analysis_group = c.benchmark_group("analysis-num_vertices");
    analysis_group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));
    for num_vertices in vertices.clone() {
        let make_graph = || bootstrap_graph(4, num_vertices);
        run_analysis_benchmarks(&mut analysis_group, make_graph, Some(num_vertices))
    }
    analysis_group.finish();

    let mut ingestion_group = c.benchmark_group("ingestion-num_shards");
    for num_shards in shards.clone() {
        let make_graph = || bootstrap_graph(num_shards, 10_000);
        ingestion_group.throughput(Throughput::Elements(num_shards as u64));
        run_ingestion_benchmarks(&mut ingestion_group, make_graph, Some(num_shards));
    }
    ingestion_group.finish();
    let mut analysis_group = c.benchmark_group("analysis-num_shards");
    for num_shards in shards.clone() {
        let make_graph = || bootstrap_graph(num_shards, 10_000);
        run_analysis_benchmarks(&mut analysis_group, make_graph, Some(num_shards))
    }
    analysis_group.finish();
}

criterion_group!(benches, parameterized);
criterion_main!(benches);
