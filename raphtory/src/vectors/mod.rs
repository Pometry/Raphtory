use crate::vectors::entity_id::EntityId;
use futures_util::future::BoxFuture;
use std::future::Future;

mod document_source;
pub mod embeddings;
mod entity_id;
pub mod graph_entity;
pub mod vectorizable;
pub mod vectorized_graph;

pub type Embedding = Vec<f32>;

pub enum Document {
    Node {
        name: String,
        content: String,
    },
    Edge {
        src: String,
        dst: String,
        content: String,
    },
}

pub trait DocumentOps {
    fn content(&self) -> &str;
    fn into_content(self) -> String;
}

impl DocumentOps for Document {
    fn content(&self) -> &str {
        match self {
            Document::Node { content, .. } => content,
            Document::Edge { content, .. } => content,
        }
    }
    fn into_content(self) -> String {
        match self {
            Document::Node { content, .. } => content,
            Document::Edge { content, .. } => content,
        }
    }
}

#[derive(Clone)]
pub(crate) struct EntityDocument {
    id: EntityId,
    content: String,
}

pub trait EmbeddingFunction: Send + Sync {
    fn call(&self, texts: Vec<String>) -> BoxFuture<'static, Vec<Embedding>>;
}

impl<T, F> EmbeddingFunction for T
where
    T: Fn(Vec<String>) -> F + Send + Sync,
    F: Future<Output = Vec<Embedding>> + Send + 'static,
{
    fn call(&self, texts: Vec<String>) -> BoxFuture<'static, Vec<Embedding>> {
        Box::pin(self(texts))
    }
}

#[cfg(test)]
mod vector_tests {
    use super::*;
    use crate::{
        core::Prop,
        db::graph::{edge::EdgeView, vertex::VertexView},
        prelude::{AdditionOps, EdgeViewOps, Graph, GraphViewOps, VertexViewOps},
        vectors::{
            document_source::DocumentSource, embeddings::openai_embedding,
            graph_entity::GraphEntity, vectorizable::Vectorizable,
        },
    };
    use dotenv::dotenv;
    use itertools::Itertools;
    use std::path::PathBuf;
    use tokio;

    const NO_PROPS: [(&str, Prop); 0] = [];

    fn format_time(time: i64) -> String {
        format!("line {time}")
    }

    fn node_template(vertex: &VertexView<Graph>) -> String {
        let name = vertex.name();
        let node_type = vertex.properties().get("type").unwrap().to_string();
        let property_list =
            vertex.generate_property_list(&format_time, vec!["type", "_id"], vec![]);
        format!("{name} is a {node_type} with the following details:\n{property_list}")
    }

    fn edge_template(edge: &EdgeView<Graph>) -> String {
        let src = edge.src().name();
        let dst = edge.dst().name();
        let lines = edge.history().iter().join(",");
        format!("{src} appeared with {dst} in lines: {lines}")
    }

    // TODO: test default templates

    #[ignore = "this test needs an OpenAI API key to run"]
    #[tokio::test]
    async fn test_empty_graph() {
        dotenv().ok();

        let g = Graph::new();
        let cache = PathBuf::from("/tmp/raphtory/vector-cache-lotr-test");
        let vectors = g.vectorize(Box::new(openai_embedding), &cache).await;
        let docs = vectors
            .similarity_search("whatever", 10, 0, 0, 20, None, None)
            .await;

        assert!(docs.is_empty())
    }

    #[test]
    fn test_node_into_doc() {
        let g = Graph::new();
        g.add_vertex(
            0,
            "Frodo",
            [
                ("type".to_string(), Prop::str("hobbit")),
                ("age".to_string(), Prop::str("30")),
            ],
        )
        .unwrap();

        let doc = g
            .vertex("Frodo")
            .unwrap()
            .generate_doc(&node_template)
            .content;
        let expected_doc = r###"Frodo is a hobbit with the following details:
earliest activity: line 0
latest activity: line 0
age: 30"###;
        assert_eq!(doc, expected_doc);
    }

    #[test]
    fn test_edge_into_doc() {
        let g = Graph::new();
        g.add_edge(0, "Frodo", "Gandalf", NO_PROPS, Some("talk to"))
            .unwrap();

        let doc = g
            .edge("Frodo", "Gandalf")
            .unwrap()
            .generate_doc(&edge_template)
            .content;
        let expected_doc = "Frodo appeared with Gandalf in lines: 0";
        assert_eq!(doc, expected_doc);
    }

    #[ignore = "this test needs an OpenAI API key to run"]
    #[tokio::test]
    async fn test_vector_store() {
        let g = Graph::new();
        g.add_vertex(
            0,
            "Gandalf",
            [
                ("type".to_string(), Prop::str("wizard")),
                ("age".to_string(), Prop::str("120")),
            ],
        )
        .unwrap();
        g.add_vertex(
            0,
            "Frodo",
            [
                ("type".to_string(), Prop::str("hobbit")),
                ("age".to_string(), Prop::str("30")),
            ],
        )
        .unwrap();
        g.add_edge(0, "Frodo", "Gandalf", NO_PROPS, Some("talk to"))
            .unwrap();
        g.add_vertex(
            2,
            "Aragorn",
            [
                ("type".to_string(), Prop::str("human")),
                ("age".to_string(), Prop::str("40")),
            ],
        )
        .unwrap();

        dotenv().ok();
        let vectors = g
            .vectorize_with_templates(
                Box::new(openai_embedding),
                &PathBuf::from("/tmp/raphtory/vector-cache-lotr-test"),
                node_template,
                edge_template,
            )
            .await;

        let docs = vectors
            .similarity_search("Find a magician", 1, 0, 0, 1, None, None)
            .await;
        // TODO: use the ids instead in all of these cases
        assert!(docs[0].content().contains("Gandalf is a wizard"));

        let docs = vectors
            .similarity_search("Find a young person", 1, 0, 0, 1, None, None)
            .await;
        assert!(docs[0].content().contains("Frodo is a hobbit")); // this fails when using gte-small

        // with window!
        let docs = vectors
            .similarity_search("Find a young person", 1, 0, 0, 1, Some(1), Some(3))
            .await;
        assert!(!docs[0].content().contains("Frodo is a hobbit")); // this fails when using gte-small

        let docs = vectors
            .similarity_search(
                "Has anyone appeared with anyone else?",
                1,
                0,
                0,
                1,
                None,
                None,
            )
            .await;
        assert!(docs[0].content().contains("Frodo appeared with Gandalf"));
    }
}
