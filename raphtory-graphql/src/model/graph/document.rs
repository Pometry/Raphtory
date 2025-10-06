use dynamic_graphql::{SimpleObject, Union};
use raphtory::{
    db::api::view::{IntoDynamic, StaticGraphViewOps},
    vectors::DocumentEntity,
};

use super::{edge::GqlEdge, node::GqlNode};

#[derive(Union)]
#[graphql(name = "DocumentEntity")]
/// Entity associated with document.
pub(crate) enum GqlDocumentEntity {
    /// Node
    Node(GqlNode),
    /// Edge
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

/// Document in a vector graph
#[derive(SimpleObject)]
#[graphql(name = "Document")]
pub struct GqlDocument {
    /// Entity associated with document.
    pub(crate) entity: GqlDocumentEntity,
    /// Content of the document.
    pub(crate) content: String,
    /// Similarity score with a specified query
    pub(crate) embedding: Vec<f32>,
    pub(crate) score: f32,
}
