use futures_util::future::BoxFuture;
use std::future::Future;

mod document_ref;
pub mod document_template;
mod embedding_cache;
pub mod embeddings;
mod entity_id;
pub mod graph_entity;
pub mod splitting;
pub mod vectorizable;
pub mod vectorized_graph;

pub type Embedding = Vec<f32>;

#[derive(Debug)]
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

// TODO: remove this interface, only used by Document (?)
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

/// struct containing all the necessary information to allow Raphtory creating a document and
/// storing it
#[derive(Clone)]
pub struct DocumentInput {
    pub content: String,
    pub life: Lifespan,
}

#[derive(Clone, Debug)]
pub enum Lifespan {
    Interval { start: i64, end: i64 },
    Event { time: i64 },
    Inherited,
}

impl From<String> for DocumentInput {
    fn from(value: String) -> Self {
        Self {
            content: value,
            life: Lifespan::Inherited,
        }
    }
}

impl From<&str> for DocumentInput {
    fn from(value: &str) -> Self {
        Self {
            content: value.to_owned(),
            life: Lifespan::Inherited,
        }
    }
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
            document_template::DocumentTemplate, embeddings::openai_embedding,
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

    async fn fake_embedding(texts: Vec<String>) -> Vec<Embedding> {
        texts.into_iter().map(|_| vec![1.0, 0.0, 0.0]).collect_vec()
    }

    struct CustomTemplate;

    impl<G: GraphViewOps> DocumentTemplate<G> for CustomTemplate {
        fn node(&self, vertex: &VertexView<G>) -> Box<dyn Iterator<Item = DocumentInput>> {
            let name = vertex.name();
            let node_type = vertex.properties().get("type").unwrap().to_string();
            let property_list =
                vertex.generate_property_list(&format_time, vec!["type", "_id"], vec![]);
            let content =
                format!("{name} is a {node_type} with the following details:\n{property_list}");
            Box::new(std::iter::once(content.into()))
        }

        fn edge(&self, edge: &EdgeView<G>) -> Box<dyn Iterator<Item = DocumentInput>> {
            let src = edge.src().name();
            let dst = edge.dst().name();
            let lines = edge.history().iter().join(",");
            let content = format!("{src} appeared with {dst} in lines: {lines}");
            Box::new(std::iter::once(content.into()))
        }
    }

    // TODO: test default templates

    #[ignore = "this test needs an OpenAI API key to run"]
    #[tokio::test]
    async fn test_empty_graph() {
        dotenv().ok();

        let g = Graph::new();
        let cache = PathBuf::from("/tmp/raphtory/vector-cache-lotr-test");
        let vectors = g.vectorize(Box::new(openai_embedding), Some(cache)).await;
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

        let custom_template = CustomTemplate;
        let doc: DocumentInput = custom_template
            .node(&g.vertex("Frodo").unwrap())
            .next()
            .unwrap()
            .into();
        let content = doc.content;
        let expected_content = r###"Frodo is a hobbit with the following details:
earliest activity: line 0
latest activity: line 0
age: 30"###;
        assert_eq!(content, expected_content);
    }

    #[test]
    fn test_edge_into_doc() {
        let g = Graph::new();
        g.add_edge(0, "Frodo", "Gandalf", NO_PROPS, Some("talk to"))
            .unwrap();

        let custom_template = CustomTemplate;
        let doc: DocumentInput = custom_template
            .edge(&g.edge("Frodo", "Gandalf").unwrap())
            .next()
            .unwrap()
            .into();
        let content = doc.content;
        let expected_content = "Frodo appeared with Gandalf in lines: 0";
        assert_eq!(content, expected_content);
    }

    // const FAKE_DOCUMENTS: Vec<&str> = vec!["doc1", "doc2", "doc3"];
    const FAKE_DOCUMENTS: [&str; 3] = ["doc1", "doc2", "doc3"];
    struct FakeMultiDocumentTemplate;

    impl<G: GraphViewOps> DocumentTemplate<G> for FakeMultiDocumentTemplate {
        fn node(&self, vertex: &VertexView<G>) -> Box<dyn Iterator<Item = DocumentInput>> {
            Box::new(
                Vec::from(FAKE_DOCUMENTS)
                    .into_iter()
                    .map(|text| text.into()),
            )
        }
        fn edge(&self, edge: &EdgeView<G>) -> Box<dyn Iterator<Item = DocumentInput>> {
            Box::new(std::iter::empty())
        }
    }

    #[tokio::test]
    async fn test_vector_store_with_multi_embedding() {
        let g = Graph::new();
        g.add_vertex(0, "test", NO_PROPS).unwrap();

        let vectors = g
            .vectorize_with_template(
                Box::new(fake_embedding),
                Some(PathBuf::from("/tmp/raphtory/vector-cache-multi-test")),
                FakeMultiDocumentTemplate,
            )
            .await;

        let docs = vectors
            .similarity_search("whatever", 1, 0, 0, 10, None, None)
            .await;
        assert_eq!(docs.len(), 3);
        // all documents are present in the result
        for doc_content in FAKE_DOCUMENTS {
            assert!(
                docs.iter().any(|doc| match doc {
                    Document::Node { content, name } => content == doc_content && name == "test",
                    _ => false,
                }),
                "document {doc_content:?} is not present in the result: {docs:?}"
            );
        }
    }

    struct FakeTemplateWithIntervals;

    impl<G: GraphViewOps> DocumentTemplate<G> for FakeTemplateWithIntervals {
        fn node(&self, vertex: &VertexView<G>) -> Box<dyn Iterator<Item = DocumentInput>> {
            let doc_event_20: DocumentInput = DocumentInput {
                content: "event at 20".to_owned(),
                life: Lifespan::Event { time: 20 },
            };

            let doc_interval_30_40: DocumentInput = DocumentInput {
                content: "interval from 30 to 40".to_owned(),
                life: Lifespan::Interval { start: 30, end: 40 },
            };
            Box::new(vec![doc_event_20, doc_interval_30_40].into_iter())
        }
        fn edge(&self, edge: &EdgeView<G>) -> Box<dyn Iterator<Item = DocumentInput>> {
            Box::new(std::iter::empty())
        }
    }

    #[tokio::test]
    async fn test_vector_store_with_window() {
        let g = Graph::new();
        g.add_vertex(0, "test", NO_PROPS).unwrap();
        g.add_edge(40, "test", "test", NO_PROPS, None).unwrap();

        let vectors = g
            .vectorize_with_template(
                Box::new(fake_embedding),
                Some(PathBuf::from("/tmp/raphtory/vector-cache-window-test")),
                FakeTemplateWithIntervals,
            )
            .await;

        let docs = vectors
            .similarity_search("whatever", 1, 0, 0, 10, None, None)
            .await;
        assert_eq!(docs.len(), 2);

        let docs = vectors
            .similarity_search("whatever", 1, 0, 0, 10, None, Some(25))
            .await;
        assert!(
            match &docs[..] {
                [Document::Node { name, content }] => name == "test" && content == "event at 20",
                _ => false,
            },
            "{docs:?} has the wrong content"
        );

        let docs = vectors
            .similarity_search("whatever", 1, 0, 0, 10, Some(35), None)
            .await;
        assert!(
            match &docs[..] {
                [Document::Node { name, content }] =>
                    name == "test" && content == "interval from 30 to 40",
                _ => false,
            },
            "{docs:?} has the wrong content"
        );
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
            .vectorize_with_template(
                Box::new(openai_embedding),
                Some(PathBuf::from("/tmp/raphtory/vector-cache-lotr-test")),
                CustomTemplate,
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
