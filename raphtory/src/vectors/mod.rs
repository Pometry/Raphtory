use crate::core::{DocumentInput, Lifespan};
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use std::future::Future;

mod document_ref;
pub mod document_template;
mod embedding_cache;
pub mod embeddings;
mod entity_id;
pub mod graph_entity;
mod similarity_search_utils;
pub mod splitting;
pub mod vectorisable;
pub mod vectorised_cluster;
pub mod vectorised_graph;
pub mod vectorised_graph_storage;

pub type Embedding = Vec<f32>;

#[derive(Debug)]
pub enum Document {
    Graph {
        name: String,
        content: String,
        life: Lifespan,
    },
    Node {
        name: String,
        content: String,
        life: Lifespan,
    },
    Edge {
        src: String,
        dst: String,
        content: String,
        life: Lifespan,
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
            Document::Graph { content, .. } => content,
            Document::Node { content, .. } => content,
            Document::Edge { content, .. } => content,
        }
    }
    fn into_content(self) -> String {
        match self {
            Document::Graph { content, .. } => content,
            Document::Node { content, .. } => content,
            Document::Edge { content, .. } => content,
        }
    }
}

impl Lifespan {
    pub(crate) fn event(time: i64) -> Self {
        Self::Event { time }
    }
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
        db::{
            api::view::StaticGraphViewOps,
            graph::{edge::EdgeView, node::NodeView},
        },
        prelude::{AdditionOps, EdgeViewOps, Graph, GraphViewOps, NodeViewOps},
        vectors::{
            document_template::{DefaultTemplate, DocumentTemplate},
            embeddings::openai_embedding,
            graph_entity::GraphEntity,
            vectorisable::Vectorisable,
        },
    };
    use dotenv::dotenv;
    use itertools::Itertools;
    use std::{fs::remove_file, path::PathBuf};
    use tokio;

    const NO_PROPS: [(&str, Prop); 0] = [];

    fn format_time(time: i64) -> String {
        format!("line {time}")
    }

    async fn fake_embedding(texts: Vec<String>) -> Vec<Embedding> {
        texts.into_iter().map(|_| vec![1.0, 0.0, 0.0]).collect_vec()
    }

    async fn panicking_embedding(_texts: Vec<String>) -> Vec<Embedding> {
        panic!("embedding function was called")
    }

    struct CustomTemplate;

    impl<G: StaticGraphViewOps> DocumentTemplate<G> for CustomTemplate {
        fn graph(&self, graph: &G) -> Box<dyn Iterator<Item = DocumentInput>> {
            DefaultTemplate.graph(graph)
        }

        fn node(&self, node: &NodeView<G>) -> Box<dyn Iterator<Item = DocumentInput>> {
            let name = node.name();
            let node_type = node.properties().get("type").unwrap().to_string();
            let property_list =
                node.generate_property_list(&format_time, vec!["type", "_id"], vec![]);
            let content =
                format!("{name} is a {node_type} with the following details:\n{property_list}");
            Box::new(std::iter::once(content.into()))
        }

        fn edge(&self, edge: &EdgeView<G, G>) -> Box<dyn Iterator<Item = DocumentInput>> {
            let src = edge.src().name();
            let dst = edge.dst().name();
            let lines = edge.history().iter().join(",");
            let content = format!("{src} appeared with {dst} in lines: {lines}");
            Box::new(std::iter::once(content.into()))
        }
    }

    #[tokio::test]
    async fn test_embedding_cache() {
        let g = Graph::new();
        g.add_node(0, "test", NO_PROPS, None).unwrap();

        // the following succeeds with no cache set up
        g.vectorise(Box::new(fake_embedding), None, true, false)
            .await;

        let path = "/tmp/raphtory/very/deep/path/embedding-cache-test";
        let _ = remove_file(path);

        // the following creates the embeddings, and store them on the cache
        g.vectorise(
            Box::new(fake_embedding),
            Some(PathBuf::from(path)),
            true,
            false,
        )
        .await;

        // the following uses the embeddings from the cache, so it doesn't call the panicking
        // embedding, which would make the test fail
        g.vectorise(
            Box::new(panicking_embedding),
            Some(PathBuf::from(path)),
            true,
            false,
        )
        .await;
    }

    // TODO: test default templates

    #[tokio::test]
    async fn test_empty_graph() {
        let g = Graph::new();
        let cache = PathBuf::from("/tmp/raphtory/vector-cache-lotr-test");
        let vectors = g
            .vectorise(Box::new(fake_embedding), Some(cache), true, false)
            .await;
        let embedding: Embedding = fake_embedding(vec!["whatever".to_owned()]).await.remove(0);
        let docs = vectors
            .append_by_similarity(&embedding, 10, None)
            .expand_by_similarity(&embedding, 10, None)
            .expand(2, None)
            .get_documents();

        assert!(docs.is_empty())
    }

    #[test]
    fn test_node_into_doc() {
        let g = Graph::new();
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

        let custom_template = CustomTemplate;
        let doc: DocumentInput = custom_template
            .node(&g.node("Frodo").unwrap())
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

    const FAKE_DOCUMENTS: [&str; 3] = ["doc1", "doc2", "doc3"];
    struct FakeMultiDocumentTemplate;

    impl<G: StaticGraphViewOps> DocumentTemplate<G> for FakeMultiDocumentTemplate {
        fn graph(&self, graph: &G) -> Box<dyn Iterator<Item = DocumentInput>> {
            DefaultTemplate.graph(graph)
        }

        fn node(&self, _node: &NodeView<G>) -> Box<dyn Iterator<Item = DocumentInput>> {
            Box::new(
                Vec::from(FAKE_DOCUMENTS)
                    .into_iter()
                    .map(|text| text.into()),
            )
        }
        fn edge(&self, _edge: &EdgeView<G, G>) -> Box<dyn Iterator<Item = DocumentInput>> {
            Box::new(std::iter::empty())
        }
    }

    #[tokio::test]
    async fn test_vector_store_with_multi_embedding() {
        let g = Graph::new();
        g.add_node(0, "test", NO_PROPS, None).unwrap();

        let vectors = g
            .vectorise_with_template(
                Box::new(fake_embedding),
                Some(PathBuf::from("/tmp/raphtory/vector-cache-multi-test")),
                true,
                FakeMultiDocumentTemplate,
                false,
            )
            .await;

        let embedding = fake_embedding(vec!["whatever".to_owned()]).await.remove(0);
        let docs = vectors
            .append_by_similarity(&embedding, 1, None)
            .expand_by_similarity(&embedding, 9, None)
            .get_documents();
        assert_eq!(docs.len(), 3);
        // all documents are present in the result
        for doc_content in FAKE_DOCUMENTS {
            assert!(
                docs.iter().any(|doc| match doc {
                    Document::Node { content, name, .. } =>
                        content == doc_content && name == "test",
                    _ => false,
                }),
                "document {doc_content:?} is not present in the result: {docs:?}"
            );
        }
    }

    struct FakeTemplateWithIntervals;

    impl<G: StaticGraphViewOps> DocumentTemplate<G> for FakeTemplateWithIntervals {
        fn graph(&self, graph: &G) -> Box<dyn Iterator<Item = DocumentInput>> {
            DefaultTemplate.graph(graph)
        }

        fn node(&self, _node: &NodeView<G>) -> Box<dyn Iterator<Item = DocumentInput>> {
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
        fn edge(&self, _edge: &EdgeView<G, G>) -> Box<dyn Iterator<Item = DocumentInput>> {
            Box::new(std::iter::empty())
        }
    }

    #[tokio::test]
    async fn test_vector_store_with_window() {
        let g = Graph::new();
        g.add_node(0, "test", NO_PROPS, None).unwrap();
        g.add_edge(40, "test", "test", NO_PROPS, None).unwrap();

        let vectors = g
            .vectorise_with_template(
                Box::new(fake_embedding),
                Some(PathBuf::from("/tmp/raphtory/vector-cache-window-test")),
                true,
                FakeTemplateWithIntervals,
                false,
            )
            .await;

        let embedding = fake_embedding(vec!["whatever".to_owned()]).await.remove(0);
        let docs = vectors
            .append_by_similarity(&embedding, 1, None)
            .expand_by_similarity(&embedding, 9, None)
            .get_documents();
        assert_eq!(docs.len(), 2);

        let docs = vectors
            .append_by_similarity(&embedding, 1, Some((-10, 25)))
            .expand_by_similarity(&embedding, 9, Some((-10, 25)))
            .get_documents();
        assert!(
            match &docs[..] {
                [Document::Node { name, content, .. }] =>
                    name == "test" && content == "event at 20",
                _ => false,
            },
            "{docs:?} has the wrong content"
        );

        let docs = vectors
            .append_by_similarity(&embedding, 1, Some((35, 100)))
            .expand_by_similarity(&embedding, 9, Some((35, 100)))
            .get_documents();
        assert!(
            match &docs[..] {
                [Document::Node { name, content, .. }] =>
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

        dotenv().ok();
        let vectors = g
            .vectorise_with_template(
                Box::new(openai_embedding),
                Some(PathBuf::from("/tmp/raphtory/vector-cache-lotr-test")),
                true,
                CustomTemplate,
                false,
            )
            .await;

        let embedding = openai_embedding(vec!["Find a magician".to_owned()])
            .await
            .remove(0);
        let docs = vectors
            .append_nodes_by_similarity(&embedding, 1, None)
            .get_documents();
        // TODO: use the ids instead in all of these cases
        assert!(docs[0].content().contains("Gandalf is a wizard"));

        let embedding = openai_embedding(vec!["Find a young person".to_owned()])
            .await
            .remove(0);
        let docs = vectors
            .append_nodes_by_similarity(&embedding, 1, None)
            .get_documents();
        assert!(docs[0].content().contains("Frodo is a hobbit")); // this fails when using gte-small

        // with window!
        let embedding = openai_embedding(vec!["Find a young person".to_owned()])
            .await
            .remove(0);
        let docs = vectors
            .append_nodes_by_similarity(&embedding, 1, Some((1, 3)))
            .get_documents();
        assert!(!docs[0].content().contains("Frodo is a hobbit")); // this fails when using gte-small

        let embedding = openai_embedding(vec!["Has anyone appeared with anyone else?".to_owned()])
            .await
            .remove(0);
        let docs = vectors
            .append_edges_by_similarity(&embedding, 1, None)
            .get_documents();
        assert!(docs[0].content().contains("Frodo appeared with Gandalf"));
    }
}
