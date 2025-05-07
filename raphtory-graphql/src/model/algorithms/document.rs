use crate::model::graph::{edge::Edge, node::Node};
use dynamic_graphql::{SimpleObject, Union};
use raphtory::{
    db::api::view::{IntoDynamic, StaticGraphViewOps},
    vectors::{Document as RustDocument, DocumentEntity},
};

#[derive(SimpleObject)]
struct Graph {
    name: String, // TODO: maybe return the graph as well here
}

impl From<String> for Graph {
    fn from(value: String) -> Self {
        Self { name: value }
    }
}

#[derive(Union)]
enum GqlDocumentEntity {
    Node(Node),
    Edge(Edge),
}

impl<G: StaticGraphViewOps + IntoDynamic> From<DocumentEntity<G>> for GqlDocumentEntity {
    fn from(value: DocumentEntity<G>) -> Self {
        match value {
            DocumentEntity::Node(node) => Self::Node(Node::from(node)),
            DocumentEntity::Edge(edge) => Self::Edge(Edge::from(edge)),
        }
    }
}

#[derive(SimpleObject)]
pub struct Document {
    entity: GqlDocumentEntity,
    content: String,
    embedding: Vec<f32>,
}

impl<G: StaticGraphViewOps + IntoDynamic> From<RustDocument<G>> for Document {
    fn from(value: RustDocument<G>) -> Self {
        let RustDocument {
            entity,
            content,
            embedding,
        } = value;
        Self {
            entity: entity.into(),
            content,
            embedding: embedding.to_vec(),
        }
    }
}
