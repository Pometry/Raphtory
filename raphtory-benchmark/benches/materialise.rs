use criterion::{criterion_group, criterion_main, Criterion};
use raphtory::graph_loader::sx_superuser_graph::sx_superuser_graph;
use raphtory_benchmark::common::bench_materialise;

pub fn bench(c: &mut Criterion) {
    let graph = sx_superuser_graph().unwrap();
    bench_materialise("materialise", c, || graph.clone());
}

criterion_group!(benches, bench);
criterion_main!(benches);
