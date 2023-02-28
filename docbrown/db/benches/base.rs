use crate::common::{bootstrap_graph, run_large_ingestion_benchmarks};
use common::{run_analysis_benchmarks, run_ingestion_benchmarks};
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use docbrown_db::graph_loader::lotr_graph::{self, lotr_graph};

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
    run_large_ingestion_benchmarks(&mut large_group, || bootstrap_graph(4, 10000), None);
    large_group.finish();
}

criterion_group!(benches, base);
criterion_main!(benches);
