use crate::common::{bootstrap_graph, run_large_ingestion_benchmarks};
use common::{run_analysis_benchmarks, run_ingestion_benchmarks};
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use docbrown_db::data;

mod common;

pub fn base(c: &mut Criterion) {
    // let mut ingestion_group = c.benchmark_group("ingestion");
    // ingestion_group.throughput(Throughput::Elements(1));
    // run_ingestion_benchmarks(&mut ingestion_group, || bootstrap_graph(4, 10_000), None);
    // ingestion_group.finish();
    //
    // let mut analysis_group = c.benchmark_group("analysis");
    // run_analysis_benchmarks(&mut analysis_group, || data::lotr_graph(4), None);
    // analysis_group.finish();

    let mut large_group = c.benchmark_group("large");
    run_large_ingestion_benchmarks(&mut large_group, || bootstrap_graph(4, 0));
    large_group.finish();
}

criterion_group!(benches, base);
criterion_main!(benches);
