use crate::db::{
    api::view::StaticGraphViewOps,
    graph::{edge::EdgeView, node::NodeView},
};
use std::sync::Arc;

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

pub type Embedding = Arc<[f32]>;

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

#[cfg(test)]
mod vector_tests {
    use super::embeddings::EmbeddingResult;
    use crate::{
        prelude::*,
        vectors::{
            cache::{CachedEmbeddingModel, VectorCache},
            custom::serve_custom_embedding,
            embeddings::EmbeddingModel,
            storage::OpenAIEmbeddings,
            template::DocumentTemplate,
            vectorisable::Vectorisable,
            Embedding,
        },
    };
    use itertools::Itertools;
    use raphtory_api::core::entities::properties::prop::Prop;
    use std::{fs::remove_dir_all, path::PathBuf, sync::Arc};
    use tokio;

    const NO_PROPS: [(&str, Prop); 0] = [];

    fn fake_embedding(text: &str) -> Vec<f32> {
        println!("Creating fake embedding for {text}");
        vec![1.0, 0.0, 0.0]
    }

    async fn use_fake_model() -> CachedEmbeddingModel {
        tokio::spawn(async {
            serve_custom_embedding("0.0.0.0:3070", fake_embedding).await;
        });
        VectorCache::in_memory()
            .openai(OpenAIEmbeddings {
                api_base: Some("http://localhost:3070/v1".to_owned()), // FIXME: the fact that I need to write /v1 is not ideal?
                ..Default::default()
            })
            .await
            .unwrap()
    }

    fn panicking_embedding(_text: &str) -> Vec<f32> {
        panic!("embedding function was called")
    }

    async fn use_panicking_model_config() -> OpenAIEmbeddings {
        tokio::spawn(async {
            serve_custom_embedding("0.0.0.0:3071", panicking_embedding).await;
        });
        OpenAIEmbeddings {
            api_base: Some("http://localhost:3071/v1".to_owned()), // FIXME: the fact that I need to write /v1 is not ideal?
            ..Default::default()
        }
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
            .await
            .unwrap();
        selection
            .expand_entities_by_similarity(&embedding, 10, None)
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
            .openai(OpenAIEmbeddings {
                model: "text-embedding-3-small".to_owned(),
                ..Default::default()
            })
            .await
            .unwrap();

        let vectors = g.vectorise(model, template, None, false).await.unwrap();

        let query = "Find a magician".to_owned();
        let embedding = vectors.embed_text(query).await.unwrap();
        let docs = vectors
            .nodes_by_similarity(&embedding, 1, None)
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
            .await
            .unwrap()
            .get_documents()
            .await
            .unwrap();
        assert!(docs[0].content.contains("Frodo appeared with Gandalf"));
    }
}
