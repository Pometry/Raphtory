use criterion::{criterion_group, criterion_main, Criterion};
use raphtory::core::tgraph::vertices::input_vertex::InputVertex;
use raphtory::db::graph::Graph;

mod common;
use rand::distributions::{Alphanumeric, DistString};
use rand::{thread_rng, Rng};
use raphtory::db::api::mutation::AdditionOps;

fn random_string(n: usize) -> String {
    Alphanumeric.sample_string(&mut rand::thread_rng(), n)
}

pub fn graph(c: &mut Criterion) {
    let mut id_group = c.benchmark_group("input vertex");
    id_group.bench_function("string input", |bencher| {
        let src: String = random_string(16);
        bencher.iter(|| src.id())
    });

    id_group.bench_function("numeric string input", |bencher| {
        let id: u64 = thread_rng().gen();
        let id_str = id.to_string();
        bencher.iter(|| id_str.id())
    });

    id_group.bench_function("numeric input", |bencher| {
        let id: u64 = thread_rng().gen();
        bencher.iter(|| id.id())
    });

    id_group.finish();
    let mut graph_group = c.benchmark_group("edge_add");
    let mut g = Graph::new();
    graph_group.bench_function("string  input", |bencher| {
        let src: String = random_string(16);
        let dst: String = random_string(16);
        let t: i64 = thread_rng().gen();
        bencher.iter(|| g.add_edge(t, src.clone(), dst.clone(), [], None))
    });
    graph_group.finish();
}

criterion_group!(benches, graph);
criterion_main!(benches);
