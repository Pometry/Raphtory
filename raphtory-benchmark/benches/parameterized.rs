use criterion::{
    criterion_group, criterion_main, AxisScale, Criterion, PlotConfiguration, Throughput,
};
use raphtory_api::core::entities::GID;
use raphtory_benchmark::common::{bootstrap_graph, run_large_ingestion_benchmarks};

pub fn parameterized(c: &mut Criterion) {
    let nodes_exponents = 1..6;

    let nodes = nodes_exponents.map(|exp| 10usize.pow(exp));
    let mut ingestion_group = c.benchmark_group("ingestion-num_nodes");
    ingestion_group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));
    for num_nodes in nodes {
        let make_graph = || bootstrap_graph(num_nodes, |id| GID::U64(id));
        let make_graph_str = || bootstrap_graph(num_nodes, |id| GID::Str(id.to_string()));
        ingestion_group.throughput(Throughput::Elements(num_nodes as u64));
        ingestion_group.sample_size(10);
        ingestion_group.warm_up_time(std::time::Duration::from_secs(1));
        run_large_ingestion_benchmarks(
            &mut ingestion_group,
            make_graph,
            make_graph_str,
            Some(num_nodes),
        );
    }
    ingestion_group.finish();
}

criterion_group!(benches, parameterized);
criterion_main!(benches);
