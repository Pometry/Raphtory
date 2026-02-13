use std::hash::{DefaultHasher, Hash, Hasher};

use rand::{rngs::StdRng, Rng, SeedableRng};
use raphtory::{
    prelude::{AdditionOps, Graph, NO_PROPS},
    vectors::{
        cache::VectorCache, embeddings::EmbeddingResult, storage::OpenAIEmbeddings,
        template::DocumentTemplate, vectorisable::Vectorisable, vectorised_graph::VectorisedGraph,
        Embedding,
    },
};
use tokio::runtime::Runtime;

pub fn gen_embedding_for_bench(text: &str) -> Embedding {
    let mut hasher = DefaultHasher::new();
    text.hash(&mut hasher);
    let hash = hasher.finish();

    let mut rng: StdRng = SeedableRng::seed_from_u64(hash);
    (0..1536).map(|_| rng.gen()).collect()
}

async fn embedding_model(texts: Vec<String>) -> EmbeddingResult<Vec<Embedding>> {
    Ok(texts
        .iter()
        .map(|text| gen_embedding_for_bench(text))
        .collect())
}

pub fn create_graph_for_vector_bench(size: usize) -> Graph {
    let graph = Graph::new();
    for id in 0..size {
        graph.add_node(0, id as u64, NO_PROPS, None).unwrap();
    }
    graph
}

pub async fn vectorise_graph_for_bench_async(graph: Graph) -> VectorisedGraph<Graph> {
    let cache = VectorCache::in_memory();
    let model = cache
        .openai(OpenAIEmbeddings::new("whatever", "localhost://1783").into())
        .await
        .unwrap();
    let template = DocumentTemplate {
        node_template: Some("{{name}}".to_owned()),
        edge_template: None,
    };
    graph.vectorise(model, template, None, true).await.unwrap()
}

// TODO: remove this version
pub fn vectorise_graph_for_bench(graph: Graph) -> VectorisedGraph<Graph> {
    let rt = Runtime::new().unwrap();
    rt.block_on(vectorise_graph_for_bench_async(graph))
}
