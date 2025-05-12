use crate::model::graph::{edge::GqlEdge, node::GqlNode};
use dynamic_graphql::{SimpleObject, Union};
use raphtory::{
    core::Lifespan,
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
#[graphql(name = "DocumentEntity")]
enum GqlDocumentEntity {
    Node(GqlNode),
    Edge(GqlEdge),
    Graph(Graph),
}

impl<G: StaticGraphViewOps + IntoDynamic> From<DocumentEntity<G>> for GqlDocumentEntity {
    fn from(value: DocumentEntity<G>) -> Self {
        match value {
            DocumentEntity::Graph { name, .. } => Self::Graph(Graph {
                name: name.unwrap(),
            }),
            DocumentEntity::Node(node) => Self::Node(GqlNode::from(node)),
            DocumentEntity::Edge(edge) => Self::Edge(GqlEdge::from(edge)),
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
