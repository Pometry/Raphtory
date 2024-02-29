use criterion::{criterion_group, criterion_main, Criterion};
use rand::{
    distributions::{Alphanumeric, DistString},
    thread_rng, Rng,
};
use raphtory::core::DocumentInput;
use raphtory::db::graph::views::deletion_graph::GraphWithDeletions;
use raphtory::vectors::{document_template::DocumentTemplate, vectorisable::Vectorisable};
use raphtory::{core::entities::nodes::input_node::InputNode, prelude::*, vectors::Embedding};
use std::path::PathBuf;
use tokio::runtime::Runtime;

mod common;

async fn random_embedding(texts: Vec<String>) -> Vec<Embedding> {
    let mut rng = thread_rng();
    texts
        .iter()
        .map(|_| (0..1536).map(|_| rng.gen()).collect())
        .collect()
}

struct EmptyTemplate;

impl DocumentTemplate<Graph> for EmptyTemplate {
    fn graph(&self, _graph: &Graph) -> Box<dyn Iterator<Item = DocumentInput>> {
        Box::new(std::iter::empty())
    }

    fn node(
        &self,
        _node: &raphtory::db::graph::node::NodeView<Graph>,
    ) -> Box<dyn Iterator<Item = DocumentInput>> {
        Box::new(std::iter::once("".into()))
    }

    fn edge(
        &self,
        _edge: &raphtory::db::graph::edge::EdgeView<Graph, Graph>,
    ) -> Box<dyn Iterator<Item = DocumentInput>> {
        Box::new(std::iter::once("".into()))
    }
}

pub fn vectors(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let g = Graph::new();
    for id in 0..500_000 {
        g.add_node(0, id, NO_PROPS, None).unwrap();
    }
    for id in 0..500_000 {
        g.add_edge(0, 0, id, NO_PROPS, None).unwrap();
    }
    let query = rt.block_on(random_embedding(vec!["".to_owned()])).remove(0);
    let cache_path = || Some(PathBuf::from("/tmp/raphtory/vector-bench"));

    let native_vectorised_graph = rt.block_on(g.vectorise_with_template(
        Box::new(random_embedding),
        cache_path(),
        true,
        EmptyTemplate,
        false, // use faiss
        false,
    ));
    c.bench_function("native-index", |b| {
        b.iter(|| native_vectorised_graph.append_by_similarity(&query, 1, None));
    });

    let faiss_vectorised_graph = rt.block_on(g.vectorise_with_template(
        Box::new(random_embedding),
        cache_path(),
        true,
        EmptyTemplate,
        true, // use faiss
        false,
    ));
    c.bench_function("faiss-index", |b| {
        b.iter(|| faiss_vectorised_graph.append_by_similarity(&query, 1, None));
    });
}

criterion_group!(benches, vectors);
criterion_main!(benches);
