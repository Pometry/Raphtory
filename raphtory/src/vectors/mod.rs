use crate::{
    core::{DocumentInput, Lifespan},
    db::{
        api::view::StaticGraphViewOps,
        graph::{edge::EdgeView, node::NodeView},
    },
};
use futures_util::future::BoxFuture;
use std::{error, future::Future, ops::Deref, sync::Arc};

pub mod datetimeformat;
mod document_ref;
pub mod embedding_cache;
pub mod embeddings;
mod entity_id;
mod similarity_search_utils;
pub mod splitting;
pub mod template;
pub mod vector_selection;
mod vector_storage;
pub mod vectorisable;
pub mod vectorised_cluster;
pub mod vectorised_graph;

pub type Embedding = Arc<[f32]>;

#[derive(Debug, Clone)]
pub enum DocumentEntity<G: StaticGraphViewOps> {
    Graph { name: Option<String>, graph: G },
    Node(NodeView<G>),
    Edge(EdgeView<G>),
}

#[derive(Debug, Clone)]
pub struct Document<G: StaticGraphViewOps> {
    pub entity: DocumentEntity<G>,
    pub content: String,
    pub embedding: Embedding,
    pub life: Lifespan,
}

impl Lifespan {
    #![allow(dead_code)]
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

pub(crate) type EmbeddingError = Box<dyn error::Error + Send + Sync>;
pub type EmbeddingResult<T> = Result<T, EmbeddingError>;

pub trait EmbeddingFunction: Send + Sync {
    fn call(&self, texts: Vec<String>) -> BoxFuture<'static, EmbeddingResult<Vec<Embedding>>>;
}

impl<T, F> EmbeddingFunction for T
where
    T: Fn(Vec<String>) -> F + Send + Sync,
    F: Future<Output = EmbeddingResult<Vec<Embedding>>> + Send + 'static,
{
    fn call(&self, texts: Vec<String>) -> BoxFuture<'static, EmbeddingResult<Vec<Embedding>>> {
        Box::pin(self(texts))
    }
}

impl EmbeddingFunction for Arc<dyn EmbeddingFunction> {
    fn call(&self, texts: Vec<String>) -> BoxFuture<'static, EmbeddingResult<Vec<Embedding>>> {
        Box::pin(self.deref().call(texts))
    }
}

#[cfg(test)]
mod vector_tests {
    use super::*;
    use crate::{
        core::Prop,
        prelude::{AdditionOps, Graph, GraphViewOps},
        vectors::{embeddings::openai_embedding, vectorisable::Vectorisable},
    };
    use dotenv::dotenv;
    use itertools::Itertools;
    use std::fs::remove_file;
    use template::DocumentTemplate;
    use tokio;

    const NO_PROPS: [(&str, Prop); 0] = [];

    async fn fake_embedding(texts: Vec<String>) -> EmbeddingResult<Vec<Embedding>> {
        Ok(texts
            .into_iter()
            .map(|_| vec![1.0, 0.0, 0.0].into())
            .collect_vec())
    }

    async fn panicking_embedding(_texts: Vec<String>) -> EmbeddingResult<Vec<Embedding>> {
        panic!("embedding function was called")
    }

    fn custom_template() -> DocumentTemplate {
        DocumentTemplate {
            graph_template: None,
            node_template: Some(
                "{{ name}} is a {{ node_type }} aged {{ properties.age }}".to_owned(),
            ),
            edge_template: Some(
                "{{ src.name }} appeared with {{ dst.name}} in lines: {{ history|join(', ') }}"
                    .to_owned(),
            ),
        }
    }

    #[tokio::test]
    async fn test_embedding_cache() {
        let template = custom_template();
        let g = Graph::new();
        g.add_node(0, "test", NO_PROPS, None).unwrap();

        // the following succeeds with no cache set up
        g.vectorise(
            Box::new(fake_embedding),
            None.into(),
            true,
            template.clone(),
            None,
            false,
        )
        .await
        .unwrap();

        let path = "/tmp/raphtory/very/deep/path/embedding-cache-test";
        let _ = remove_file(path);

        // the following creates the embeddings, and store them on the cache
        g.vectorise(
            Box::new(fake_embedding),
            Some(path.to_owned().into()).into(),
            true,
            template.clone(),
            None,
            false,
        )
        .await
        .unwrap();

        // the following uses the embeddings from the cache, so it doesn't call the panicking
        // embedding, which would make the test fail
        g.vectorise(
            Box::new(panicking_embedding),
            Some(path.to_owned().into()).into(),
            true,
            template,
            None,
            false,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_empty_graph() {
        let template = custom_template();
        let g = Graph::new();
        let cache = Some("/tmp/raphtory/vector-cache-lotr-test".to_owned().into()).into();
        let vectors = g
            .vectorise(Box::new(fake_embedding), cache, true, template, None, false)
            .await
            .unwrap();
        let embedding: Embedding = fake_embedding(vec!["whatever".to_owned()])
            .await
            .unwrap()
            .remove(0);

        let mut selection = vectors.documents_by_similarity(&embedding, 10, None);
        selection.expand_documents_by_similarity(&embedding, 10, None);
        selection.expand(2, None);
        let docs = selection.get_documents();

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
        let doc: DocumentInput = template
            .node(g.node("Frodo").unwrap())
            .next()
            .unwrap()
            .into();
        let content = doc.content;
        let expected_content = "Frodo is a hobbit aged 30";
        assert_eq!(content, expected_content);
    }

    #[test]
    fn test_edge_into_doc() {
        let g = Graph::new();
        g.add_edge(0, "Frodo", "Gandalf", NO_PROPS, Some("talk to"))
            .unwrap();

        let template = custom_template();
        let doc: DocumentInput = template
            .edge(g.edge("Frodo", "Gandalf").unwrap().as_ref())
            .next()
            .unwrap()
            .into();
        let content = doc.content;
        let expected_content = "Frodo appeared with Gandalf in lines: 0";
        assert_eq!(content, expected_content);
    }

    // const FAKE_DOCUMENTS: [&str; 3] = ["doc1", "doc2", "doc3"];
    // struct FakeMultiDocumentTemplate;

    // impl<G: StaticGraphViewOps> DocumentTemplate<G> for FakeMultiDocumentTemplate {
    //     fn graph(&self, graph: &G) -> Box<dyn Iterator<Item = DocumentInput>> {
    //         DefaultTemplate.graph(graph)
    //     }

    //     fn node(&self, _node: &NodeView<G>) -> Box<dyn Iterator<Item = DocumentInput>> {
    //         Box::new(
    //             Vec::from(FAKE_DOCUMENTS)
    //                 .into_iter()
    //                 .map(|text| text.into()),
    //         )
    //     }
    //     fn edge(&self, _edge: EdgeView<&G, &G>) -> Box<dyn Iterator<Item = DocumentInput>> {
    //         Box::new(std::iter::empty())
    //     }
    // }

    // #[tokio::test]
    // async fn test_vector_store_with_multi_embedding() {
    //     let g = Graph::new();
    //     g.add_node(0, "test", NO_PROPS, None).unwrap();

    //     let vectors = g
    //         .vectorise_with_template(
    //             Box::new(fake_embedding),
    //             Some(PathBuf::from("/tmp/raphtory/vector-cache-multi-test")),
    //             true,
    //             FakeMultiDocumentTemplate,
    //             false,
    //         )
    //         .await;

    //     let embedding = fake_embedding(vec!["whatever".to_owned()]).await.remove(0);

    //     let mut selection = vectors.search_documents(&embedding, 1, None);
    //     selection.expand_documents_by_similarity(&embedding, 9, None);
    //     let docs = selection.get_documents();
    //     assert_eq!(docs.len(), 3);
    //     // all documents are present in the result
    //     for doc_content in FAKE_DOCUMENTS {
    //         assert!(
    //             docs.iter().any(|doc| match doc {
    //                 Document::Node { content, name, .. } =>
    //                     content == doc_content && name == "test",
    //                 _ => false,
    //             }),
    //             "document {doc_content:?} is not present in the result: {docs:?}"
    //         );
    //     }
    // }

    // struct FakeTemplateWithIntervals;

    // impl<G: StaticGraphViewOps> DocumentTemplate<G> for FakeTemplateWithIntervals {
    //     fn graph(&self, graph: &G) -> Box<dyn Iterator<Item = DocumentInput>> {
    //         DefaultTemplate.graph(graph)
    //     }

    //     fn node(&self, _node: &NodeView<G>) -> Box<dyn Iterator<Item = DocumentInput>> {
    //         let doc_event_20: DocumentInput = DocumentInput {
    //             content: "event at 20".to_owned(),
    //             life: Lifespan::Event { time: 20 },
    //         };

    //         let doc_interval_30_40: DocumentInput = DocumentInput {
    //             content: "interval from 30 to 40".to_owned(),
    //             life: Lifespan::Interval { start: 30, end: 40 },
    //         };
    //         Box::new(vec![doc_event_20, doc_interval_30_40].into_iter())
    //     }
    //     fn edge(&self, _edge: &EdgeView<G, G>) -> Box<dyn Iterator<Item = DocumentInput>> {
    //         Box::new(std::iter::empty())
    //     }
    // }

    // #[tokio::test]
    // async fn test_vector_store_with_window() {
    //     let g = Graph::new();
    //     g.add_node(0, "test", NO_PROPS, None).unwrap();
    //     g.add_edge(40, "test", "test", NO_PROPS, None).unwrap();

    //     let vectors = g
    //         .vectorise_with_template(
    //             Box::new(fake_embedding),
    //             Some(PathBuf::from("/tmp/raphtory/vector-cache-window-test")),
    //             true,
    //             FakeTemplateWithIntervals,
    //             false,
    //         )
    //         .await;

    //     let embedding = fake_embedding(vec!["whatever".to_owned()]).await.remove(0);
    //     let mut selection = vectors.search_documents(&embedding, 1, None);
    //     selection.expand_documents_by_similarity(&embedding, 9, None);
    //     let docs = selection.get_documents();
    //     assert_eq!(docs.len(), 2);

    //     let mut selection = vectors.search_documents(&embedding, 1, Some((-10, 25)));
    //     selection.expand_documents_by_similarity(&embedding, 9, Some((-10, 25)));
    //     let docs = selection.get_documents();
    //     assert!(
    //         match &docs[..] {
    //             [Document::Node { name, content, .. }] =>
    //                 name == "test" && content == "event at 20",
    //             _ => false,
    //         },
    //         "{docs:?} has the wrong content"
    //     );

    //     let mut selection = vectors.search_documents(&embedding, 1, Some((35, 100)));
    //     selection.expand_documents_by_similarity(&embedding, 9, Some((35, 100)));
    //     let docs = selection.get_documents();
    //     assert!(
    //         match &docs[..] {
    //             [Document::Node { name, content, .. }] =>
    //                 name == "test" && content == "interval from 30 to 40",
    //             _ => false,
    //         },
    //         "{docs:?} has the wrong content"
    //     );
    // }

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

        dotenv().ok();
        let vectors = g
            .vectorise(
                Box::new(openai_embedding),
                Some("/tmp/raphtory/vector-cache-lotr-test".to_owned().into()).into(),
                true,
                template,
                None,
                false,
            )
            .await
            .unwrap();

        let embedding = openai_embedding(vec!["Find a magician".to_owned()])
            .await
            .unwrap()
            .remove(0);
        let docs = vectors
            .nodes_by_similarity(&embedding, 1, None)
            .get_documents();
        // TODO: use the ids instead in all of these cases
        assert!(docs[0].content.contains("Gandalf is a wizard"));

        let embedding = openai_embedding(vec!["Find a young person".to_owned()])
            .await
            .unwrap()
            .remove(0);
        let docs = vectors
            .nodes_by_similarity(&embedding, 1, None)
            .get_documents();
        assert!(docs[0].content.contains("Frodo is a hobbit")); // this fails when using gte-small

        // with window!
        let embedding = openai_embedding(vec!["Find a young person".to_owned()])
            .await
            .unwrap()
            .remove(0);
        let docs = vectors
            .nodes_by_similarity(&embedding, 1, Some((1, 3)))
            .get_documents();
        assert!(!docs[0].content.contains("Frodo is a hobbit")); // this fails when using gte-small

        let embedding = openai_embedding(vec!["Has anyone appeared with anyone else?".to_owned()])
            .await
            .unwrap()
            .remove(0);

        let docs = vectors
            .edges_by_similarity(&embedding, 1, None)
            .get_documents();
        assert!(docs[0].content.contains("Frodo appeared with Gandalf"));
    }
}
