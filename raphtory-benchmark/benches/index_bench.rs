use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use raphtory::{
    db::api::view::StaticGraphViewOps,
    prelude::{AdditionOps, Graph, IndexMutationOps, StableDecode, StableEncode},
};
use raphtory_api::core::entities::properties::prop::Prop;
use tempfile::TempDir;

fn init_graph<G: StaticGraphViewOps + AdditionOps>(graph: G) -> G {
    let node_data = vec![
        (6, "N1", 2u64, "air_nomad"),
        (7, "N1", 1u64, "air_nomad"),
        (6, "N2", 1u64, "water_tribe"),
        (7, "N2", 2u64, "water_tribe"),
        (8, "N3", 1u64, "air_nomad"),
        (9, "N4", 1u64, "air_nomad"),
        (5, "N5", 1u64, "air_nomad"),
        (6, "N5", 2u64, "air_nomad"),
        (5, "N6", 1u64, "fire_nation"),
        (6, "N6", 1u64, "fire_nation"),
        (3, "N7", 1u64, "air_nomad"),
        (5, "N7", 1u64, "air_nomad"),
        (3, "N8", 1u64, "fire_nation"),
        (4, "N8", 2u64, "fire_nation"),
    ];

    for (ts, name, value, kind) in node_data {
        graph
            .add_node(ts, name, [("p1", Prop::U64(value))], Some(kind))
            .unwrap();
    }

    graph
}

fn bench_graph_init_index(c: &mut Criterion) {
    let graph = init_graph(Graph::new());
    let mut group = c.benchmark_group("index_init");
    group.sample_size(1000);

    group.bench_function(BenchmarkId::from_parameter("load_once"), |b| {
        b.iter(|| graph.create_index().unwrap());
    });

    group.finish();
}

fn bench_graph_index_load(c: &mut Criterion) {
    let graph = init_graph(Graph::new());
    graph.create_index().unwrap();
    let dir = TempDir::new().unwrap();
    let path = dir.path();
    graph.encode(path).unwrap();

    let mut group = c.benchmark_group("graph_index_load");
    group.sample_size(10_000);

    group.bench_function(BenchmarkId::from_parameter("load_once"), |b| {
        b.iter(|| {
            let graph = Graph::decode(black_box(&path)).unwrap();
            black_box(graph);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_graph_index_load);
criterion_main!(benches);
