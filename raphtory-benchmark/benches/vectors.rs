use criterion::{criterion_group, criterion_main, Criterion};

use raphtory_benchmark::common::vectors::{
    create_graph_for_vector_bench, gen_embedding_for_bench, vectorise_graph_for_bench,
};

fn bench_search_entities(c: &mut Criterion) {
    let g = create_graph_for_vector_bench(100_000);
    let v = vectorise_graph_for_bench(g);

    let query = gen_embedding_for_bench("0");
    c.bench_function("semantic_search_entities", |b| {
        b.iter(|| v.entities_by_similarity(&query, 10, None))
    });
}

criterion_group!(vector_benches, bench_search_entities,);
criterion_main!(vector_benches);
