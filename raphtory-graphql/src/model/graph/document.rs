use crate::model::graph::{edge::Edge, node::Node};
use dynamic_graphql::{SimpleObject, Union};
use raphtory::{
    db::api::view::{IntoDynamic, StaticGraphViewOps},
    vectors::DocumentEntity,
};

#[derive(Union)]
pub(crate) enum GqlDocumentEntity {
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
pub struct GqlDocument {
    pub(crate) entity: GqlDocumentEntity,
    pub(crate) content: String,
    pub(crate) embedding: Vec<f32>,
    pub(crate) score: f32,
}
