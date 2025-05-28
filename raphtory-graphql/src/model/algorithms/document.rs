use crate::model::graph::{edge::GqlEdge, node::GqlNode};
use dynamic_graphql::{SimpleObject, Union};
use raphtory::{
    db::api::view::{IntoDynamic, StaticGraphViewOps},
    vectors::{Document as RustDocument, DocumentEntity, Lifespan},
};

#[derive(SimpleObject)]
struct DocumentGraph {
    name: String, // TODO: maybe return the graph as well here
}

impl From<String> for DocumentGraph {
    fn from(value: String) -> Self {
        Self { name: value }
    }
}

#[derive(Union)]
#[graphql(name = "DocumentEntity")]
enum GqlDocumentEntity {
    DocNode(GqlNode),
    DocEdge(GqlEdge),
    DocGraph(DocumentGraph),
}

impl<G: StaticGraphViewOps + IntoDynamic> From<DocumentEntity<G>> for GqlDocumentEntity {
    fn from(value: DocumentEntity<G>) -> Self {
        match value {
            DocumentEntity::Graph { name, .. } => Self::DocGraph(DocumentGraph {
                name: name.unwrap(),
            }),
            DocumentEntity::Node(node) => Self::DocNode(GqlNode::from(node)),
            DocumentEntity::Edge(edge) => Self::DocEdge(GqlEdge::from(edge)),
        }
    }
}

#[derive(SimpleObject)]
pub struct Document {
    entity: GqlDocumentEntity,
    content: String,
    embedding: Vec<f32>,
    life: Vec<i64>, // TODO: give this a proper type
}

impl<G: StaticGraphViewOps + IntoDynamic> From<RustDocument<G>> for Document {
    fn from(value: RustDocument<G>) -> Self {
        let RustDocument {
            entity,
            content,
            embedding,
            life,
        } = value;
        Self {
            entity: entity.into(),
            content,
            embedding: embedding.to_vec(),
            life: lifespan_into_vec(life),
        }
    }
}

fn lifespan_into_vec(life: Lifespan) -> Vec<i64> {
    match life {
        Lifespan::Inherited => vec![],
        Lifespan::Event { time } => vec![time],
        Lifespan::Interval { start, end } => vec![start, end],
    }
}
