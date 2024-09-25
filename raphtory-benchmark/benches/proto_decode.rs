use criterion::{criterion_group, criterion_main, Criterion};
use raphtory::graph_loader::{lotr_graph::lotr_graph, sx_superuser_graph::sx_superuser_graph};
use raphtory_benchmark::common::run_proto_decode_benchmark;

fn bench(c: &mut Criterion) {
    let graph = sx_superuser_graph().unwrap();
    let mut group = c.benchmark_group("proto_sx_superuser");
    group.sample_size(10);
    run_proto_decode_benchmark(&mut group, graph);
    group.finish();

    let mut group = c.benchmark_group("proto_lotr");
    let graph = lotr_graph();
    group.sample_size(100);
    run_proto_decode_benchmark(&mut group, graph);
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
