use arrow_array::Float32Array;
use serde::{ser::SerializeSeq, Deserialize, Serialize, Serializer};

use crate::db::{
    api::view::StaticGraphViewOps,
    graph::{edge::EdgeView, node::NodeView},
};
#[cfg(feature = "python")]
use crate::python::types::repr::Repr;
use std::{future::Future, ops::Deref, pin::Pin};

pub mod cache;
pub mod custom;
pub mod datetimeformat;
pub mod embeddings;
mod entity_db;
mod entity_ref;
pub mod splitting;
pub mod storage; // TODO: re-export Embeddings instead of making this public
pub mod template;
mod utils;
mod vector_collection;
pub mod vector_selection;
pub mod vectorisable;
pub mod vectorised_graph;

#[derive(Debug, Clone, PartialEq)]
pub struct Embedding(Float32Array);

impl Serialize for Embedding {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for i in self.0.values().iter() {
            seq.serialize_element(i)?;
        }
        seq.end()
    }
}

impl<'a> Deserialize<'a> for Embedding {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'a>,
    {
        let vec: Vec<f32> = Deserialize::deserialize(deserializer)?;
        Ok(Embedding(Float32Array::from(vec)))
    }
}

impl Deref for Embedding {
    type Target = [f32];

    fn deref(&self) -> &Self::Target {
        self.0.values()
    }
}

impl Embedding {
    pub fn inner(&self) -> &Float32Array {
        &self.0
    }
}

impl From<Vec<f32>> for Embedding {
    fn from(vec: Vec<f32>) -> Self {
        Embedding(Float32Array::from(vec))
    }
}

impl From<&[f32]> for Embedding {
    fn from(slice: &[f32]) -> Self {
        Embedding(Float32Array::from(slice.to_vec()))
    }
}

impl<const N: usize> From<[f32; N]> for Embedding {
    fn from(array: [f32; N]) -> Self {
        Embedding(Float32Array::from(array.to_vec()))
    }
}

impl From<Float32Array> for Embedding {
    fn from(array: Float32Array) -> Self {
        Embedding(array)
    }
}

#[cfg(feature = "python")]
impl Repr for Embedding {
    fn repr(&self) -> String {
        format!("{:?}", &self.0.values())
    }
}

impl FromIterator<f32> for Embedding {
    fn from_iter<T: IntoIterator<Item = f32>>(iter: T) -> Self {
        let vec: Vec<f32> = iter.into_iter().collect();
        Embedding(Float32Array::from(vec))
    }
}

#[derive(Debug, Clone)]
pub enum DocumentEntity<G: StaticGraphViewOps> {
    Node(NodeView<'static, G>),
    Edge(EdgeView<G>),
}

#[derive(Debug, Clone)]
pub struct Document<G: StaticGraphViewOps> {
    pub entity: DocumentEntity<G>,
    pub content: String,
    pub embedding: Embedding,
}

pub struct VectorsQuery<T> {
    future: Pin<Box<dyn Future<Output = T> + Send>>,
}

impl<T: Send> VectorsQuery<T> {
    pub fn new(future: Pin<Box<dyn Future<Output = T> + Send>>) -> Self {
        Self { future }
    }

    pub async fn execute(self) -> T {
        self.future.await
    }
}

impl<T: Send + 'static> VectorsQuery<T> {
    pub fn resolved(resolved: T) -> Self {
        Self {
            future: Box::pin(async move { resolved }),
        }
    }
}

#[cfg(test)]
mod vector_tests {
    use crate::{
        prelude::*,
        vectors::{
            cache::{CachedEmbeddingModel, VectorCache},
            custom::serve_custom_embedding,
            embeddings::ModelConfig,
            storage::OpenAIEmbeddings,
            template::DocumentTemplate,
            vector_selection::noop_executor,
            vectorisable::Vectorisable,
        },
    };
    use raphtory_api::core::entities::properties::prop::Prop;
    use std::time::Duration;
    use tokio;

    const NO_PROPS: [(&str, Prop); 0] = [];

    fn fake_embedding(text: &str) -> Vec<f32> {
        println!("Creating fake embedding for {text}");
        vec![1.0, 0.0, 0.0]
    }

    async fn use_fake_model() -> CachedEmbeddingModel {
        tokio::spawn(async {
            let running = serve_custom_embedding(None, 3070, fake_embedding).await;
            running.wait().await;
        });
        tokio::time::sleep(Duration::from_secs(1)).await;
        VectorCache::in_memory()
            .openai(
                super::embeddings::ModelConfig::OpenAI(OpenAIEmbeddings::new(
                    "whatever",
                    "http://localhost:3070",
                ))
                .with_dimension(10),
            )
            .await
            .unwrap()
    }

    fn panicking_embedding(_text: &str) -> Vec<f32> {
        panic!("embedding function was called")
    }

    async fn use_panicking_model_config() -> OpenAIEmbeddings {
        tokio::spawn(async {
            serve_custom_embedding(None, 3071, panicking_embedding).await;
        });
        OpenAIEmbeddings::new("whatever", "http://localhost:3071")
    }

    fn custom_template() -> DocumentTemplate {
        DocumentTemplate {
            node_template: Some(
                "{{ name}} is a {{ node_type }} aged {{ properties.age }}".to_owned(),
            ),
            edge_template: Some(
                "{{ src.name }} appeared with {{ dst.name}} in lines: {{ history|join(', ') }}"
                    .to_owned(),
            ),
        }
    }

    // TODO: bring this back
    // the point of this test was double-checking when the same query comes in again,
    // the cached result is used intead of calling the model again
    // I need to find an alternative way
    // might be having an embedding model that always returns something to "raphtory"
    // but only returns some answer the first time it gets some text,  errors out the second time
    // Another option might be having a model that returns always the same embedding for "raphtory"
    // but increments for the rest of the texts
    // can then validate the answer didn't change, which means the cache was used
    // #[tokio::test]
    // async fn test_embedding_cache() {
    //     let template = custom_template();
    //     let g = Graph::new();
    //     g.add_node(0, "test", NO_PROPS, None).unwrap();

    //     let path = PathBuf::from("/tmp/raphtory/very/deep/path/embedding-cache-test");
    //     let _ = remove_dir_all(&path);
    //     let config = use_panicking_model_config().await;

    //     let cache = VectorCache::on_disk(&path).await.unwrap();
    //     let model = cache.openai(config).await.unwrap();
    //     g.vectorise(model, template.clone(), None, false)
    //         .await
    //         .unwrap();

    //     // the following uses the embeddings from the cache, so it doesn't call the panicking
    //     // embedding, which would make the test fail
    //     let cache = VectorCache::on_disk(&path).await.unwrap();
    //     let model = cache
    //         .cache_model(
    //             EmbeddingModel::custom(panicking_embedding)
    //                 .sampled()
    //                 .await
    //                 .unwrap(),
    //         )
    //         .await
    //         .unwrap();
    //     g.vectorise(model, template, None, false).await.unwrap();
    // }

    #[tokio::test]
    async fn test_empty_graph() {
        let template = custom_template();
        let g = Graph::new();
        let model = use_fake_model().await;
        let vectors = g.vectorise(model, template, None, false).await.unwrap();
        let embedding = vectors.embed_text("whatever").await.unwrap();
        let mut selection = vectors
            .entities_by_similarity(&embedding, 10, None)
            .execute()
            .await
            .unwrap();
        selection
            .expand_entities_by_similarity(&embedding, 10, None, noop_executor)
            .await
            .unwrap();
        selection.expand(2, None);
        let docs = selection.get_documents().await.unwrap();
        assert!(docs.is_empty())
    }

    #[test]
    fn test_node_into_doc() {
        let g = Graph::new();
        g.add_node(
            0,
            "Frodo",
            [("age".to_string(), Prop::str("30"))],
            Some("hobbit"),
        )
        .unwrap();

        let template = custom_template();
        let content: String = template.node(g.node("Frodo").unwrap()).unwrap();
        let expected_content = "Frodo is a hobbit aged 30";
        assert_eq!(&content, expected_content);
    }

    #[test]
    fn test_edge_into_doc() {
        let g = Graph::new();
        g.add_edge(0, "Frodo", "Gandalf", NO_PROPS, Some("talk to"))
            .unwrap();

        let template = custom_template();
        let content: String = template
            .edge(g.edge("Frodo", "Gandalf").unwrap().as_ref())
            .unwrap();
        let expected_content = "Frodo appeared with Gandalf in lines: 0";
        assert_eq!(&content, expected_content);
    }

    #[ignore = "this test needs an OpenAI API key to run"]
    #[tokio::test]
    async fn test_vector_store() {
        let template = custom_template();
        let g = Graph::new();
        g.add_node(
            0,
            "Gandalf",
            [
                ("type".to_string(), Prop::str("wizard")),
                ("age".to_string(), Prop::str("120")),
            ],
            None,
        )
        .unwrap();
        g.add_node(
            0,
            "Frodo",
            [
                ("type".to_string(), Prop::str("hobbit")),
                ("age".to_string(), Prop::str("30")),
            ],
            None,
        )
        .unwrap();
        g.add_edge(0, "Frodo", "Gandalf", NO_PROPS, Some("talk to"))
            .unwrap();
        g.add_node(
            2,
            "Aragorn",
            [
                ("type".to_string(), Prop::str("human")),
                ("age".to_string(), Prop::str("40")),
            ],
            None,
        )
        .unwrap();

        dotenv::dotenv().ok();

        let model = VectorCache::in_memory()
            .openai(
                ModelConfig::OpenAI(OpenAIEmbeddings::empty("text-embedding-3-small"))
                    .with_dimension(10),
            )
            .await
            .unwrap();

        let vectors = g.vectorise(model, template, None, false).await.unwrap();

        let query = "Find a magician".to_owned();
        let embedding = vectors.embed_text(query).await.unwrap();
        let docs = vectors
            .nodes_by_similarity(&embedding, 1, None)
            .execute()
            .await
            .unwrap()
            .get_documents()
            .await
            .unwrap();
        // TODO: use the ids instead in all of these cases
        assert!(docs[0].content.contains("Gandalf is a wizard"));

        let query = "Find a young person".to_owned();
        let embedding = vectors.embed_text(query).await.unwrap();
        let docs = vectors
            .nodes_by_similarity(&embedding, 1, None)
            .execute()
            .await
            .unwrap()
            .get_documents()
            .await
            .unwrap();
        assert!(docs[0].content.contains("Frodo is a hobbit")); // this fails when using gte-small

        // with window!
        let query = "Find a young person".to_owned();
        let embedding = vectors.embed_text(query).await.unwrap();
        let docs = vectors
            .nodes_by_similarity(&embedding, 1, Some((1, 3)))
            .execute()
            .await
            .unwrap()
            .get_documents()
            .await
            .unwrap();
        assert!(!docs[0].content.contains("Frodo is a hobbit")); // this fails when using gte-small

        let query = "Has anyone appeared with anyone else?".to_owned();
        let embedding = vectors.embed_text(query).await.unwrap();
        let docs = vectors
            .edges_by_similarity(&embedding, 1, None)
            .execute()
            .await
            .unwrap()
            .get_documents()
            .await
            .unwrap();
        assert!(docs[0].content.contains("Frodo appeared with Gandalf"));
    }
}
