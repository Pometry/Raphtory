use std::{
    hash::{DefaultHasher, Hash, Hasher},
    time::SystemTime,
};

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

fn create_graph(size: usize) -> Graph {
    let graph = Graph::new();
    for id in 0..size {
        graph.add_node(0, id as u64, NO_PROPS, None).unwrap();
    }
    graph
}

fn vectorise_graph(graph: Graph) {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
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
}

fn print_time(start: SystemTime, message: &str) {
    let duration = SystemTime::now().duration_since(start).unwrap().as_secs();
    println!("{message} - took {duration}s");
}

fn main() {
    for size in [10_000] {
        let graph = create_graph(size);
        let start = SystemTime::now();
        vectorise_graph(graph);
        print_time(start, &format!(">>> vectorise {}k", size / 1000));
    }
}
