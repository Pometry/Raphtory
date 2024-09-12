use criterion::{criterion_group, criterion_main, Criterion};
use raphtory::graph_loader::sx_superuser_graph::sx_superuser_graph;
use raphtory_benchmark::common::run_proto_decode_benchmark;

fn bench(c: &mut Criterion) {
    let graph = sx_superuser_graph().unwrap();
    let mut group = c.benchmark_group("proto_sx_superuser");
    group.sample_size(10);
    run_proto_decode_benchmark(&mut group, graph);
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
