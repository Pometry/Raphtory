use dynamic_graphql::{SimpleObject, Union};
use raphtory::{
    db::api::view::{IntoDynamic, StaticGraphViewOps},
    vectors::DocumentEntity,
};

use super::{edge::GqlEdge, node::GqlNode};

#[derive(Union)]
#[graphql(name = "DocumentEntity")]
pub(crate) enum GqlDocumentEntity {
    Node(GqlNode),
    Edge(GqlEdge),
}

impl<G: StaticGraphViewOps + IntoDynamic> From<DocumentEntity<G>> for GqlDocumentEntity {
    fn from(value: DocumentEntity<G>) -> Self {
        match value {
            DocumentEntity::Node(node) => Self::Node(GqlNode::from(node)),
            DocumentEntity::Edge(edge) => Self::Edge(GqlEdge::from(edge)),
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
