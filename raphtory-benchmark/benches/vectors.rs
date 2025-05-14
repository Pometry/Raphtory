use std::hash::{DefaultHasher, Hash, Hasher};

use criterion::{criterion_group, criterion_main, Criterion};
use rand::{rngs::StdRng, Rng, SeedableRng};
use raphtory::{
    prelude::{AdditionOps, Graph, NO_PROPS},
    vectors::{template::DocumentTemplate, vectorisable::Vectorisable, Embedding, EmbeddingResult},
};
use tokio::runtime::Runtime;

fn gen_embedding(text: &str) -> Embedding {
    let mut hasher = DefaultHasher::new();
    text.hash(&mut hasher);
    let hash = hasher.finish();

    let mut rng: StdRng = SeedableRng::seed_from_u64(hash);
    (0..3072).map(|_| rng.gen()).collect()
}

async fn embedding_model(texts: Vec<String>) -> EmbeddingResult<Vec<Embedding>> {
    Ok(texts.iter().map(|text| gen_embedding(text)).collect())
}

fn bench_search_entities(c: &mut Criterion) {
    let graph = Graph::new();

    for id in 0..10_000 {
        graph.add_node(0, id, NO_PROPS, None).unwrap();
    }

    let rt = Runtime::new().unwrap();
    let v = rt.block_on(async {
        let embedding = Box::new(embedding_model);
        let template = DocumentTemplate {
            node_template: Some("{{name}}".to_owned()),
            edge_template: None,
        };
        graph
            .vectorise(embedding, None.into(), false, template, None, true)
            .await
            .unwrap()
    });

    let query = gen_embedding("0");

    println!("vectorised graph ready");

    c.bench_function("semantic_search_entities", |b| {
        b.iter(|| v.entities_by_similarity(&query, 10, None))
    });

    // let mut group = c.benchmark_group("bench_search_entities");

    // group.bench_function("search_api", |b| {
    //     b.iter_batched(
    //         || {
    //             let mut iter = node_names.iter().cloned().cycle();
    //             let random_name = iter.next().unwrap();
    //             NodeFilter::node_name().eq(random_name)
    //         },
    //         |random_filter| {
    //             graph.search_nodes(random_filter, 5, 0).unwrap();
    //         },
    //         BatchSize::SmallInput,
    //     )
    // });
}

criterion_group!(vector_benches, bench_search_entities);
criterion_main!(vector_benches);
