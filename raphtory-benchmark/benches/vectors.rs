use std::{
    hash::{DefaultHasher, Hash, Hasher},
    time::SystemTime,
};

use criterion::{criterion_group, criterion_main, Criterion};
use rand::{rngs::StdRng, Rng, SeedableRng};
use raphtory::{
    prelude::{AdditionOps, Graph, NO_PROPS},
    vectors::{
        cache::VectorCache, embeddings::EmbeddingResult, template::DocumentTemplate,
        vectorisable::Vectorisable, Embedding,
    },
};
use tokio::runtime::Runtime;

fn gen_embedding(text: &str) -> Embedding {
    let mut hasher = DefaultHasher::new();
    text.hash(&mut hasher);
    let hash = hasher.finish();

    let mut rng: StdRng = SeedableRng::seed_from_u64(hash);
    (0..1024).map(|_| rng.gen()).collect()
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
        let cache = VectorCache::in_memory(embedding_model).await;
        let template = DocumentTemplate {
            node_template: Some("{{name}}".to_owned()),
            edge_template: None,
        };
        graph.vectorise(cache, template, None, true).await.unwrap()
    });

    let query = gen_embedding("0");

    println!("vectorised graph ready");

    let result = v
        .entities_by_similarity(&query, 10, None)
        .get_documents_with_scores();
    dbg!(&result
        .iter()
        .map(|(doc, score)| (doc.content.to_owned(), score))
        .collect::<Vec<_>>());

    c.bench_function("semantic_search_entities", |b| {
        b.iter(|| v.entities_by_similarity(&query, 10, None))
    });
}

criterion_group!(vector_benches, bench_search_entities,);
criterion_main!(vector_benches);
