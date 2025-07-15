use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use raphtory::prelude::{Graph, IndexMutationOps, StableDecode, StableEncode};
use raphtory_benchmark::graph_gen::raph_social::generate_graph;
use tempfile::TempDir;

fn bench_graph_init_index(c: &mut Criterion) {
    let graph = generate_graph(300, 500, 700, 1000);

    let mut group = c.benchmark_group("index_init");
    group.sample_size(100);

    group.bench_function(BenchmarkId::from_parameter("load_once"), |b| {
        graph.drop_index().unwrap();
        b.iter(|| graph.create_index().unwrap());
    });

    group.finish();
}

fn bench_graph_index_load(c: &mut Criterion) {
    let graph = generate_graph(300, 500, 700, 1000);

    graph.create_index().unwrap();
    let dir = TempDir::new().unwrap();
    let path = dir.path();
    graph.encode(path).unwrap();

    let mut group = c.benchmark_group("graph_index_load");
    group.sample_size(100);

    group.bench_function(BenchmarkId::from_parameter("load_once"), |b| {
        b.iter(|| Graph::decode(black_box(&path)).unwrap());
    });

    group.finish();
}

criterion_group!(benches, bench_graph_init_index, bench_graph_index_load);
criterion_main!(benches);
