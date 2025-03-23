use crate::model::graph::{edge::Edge, node::Node};
use dynamic_graphql::{SimpleObject, Union};
use raphtory::{core::Lifespan, db::api::view::DynamicGraph, vectors::Document};

#[derive(SimpleObject)]
struct DocumentGraph {
    name: String,
}

impl From<String> for DocumentGraph {
    fn from(value: String) -> Self {
        Self { name: value }
    }
}

#[derive(Union)]
enum DocumentEntity {
    Node(Node),
    Edge(Edge),
    Graph(DocumentGraph),
}

#[derive(SimpleObject)]
pub struct GqlDocument {
    entity: DocumentEntity,
    content: String,
    embedding: Vec<f32>,
    life: Vec<i64>,
}

impl From<Document<DynamicGraph>> for GqlDocument {
    fn from(value: Document<DynamicGraph>) -> Self {
        match document {
            Document::Graph {
                entity,
                content,
                embedding,
                life,
            } => Self {
                entity: DocumentEntity::Graph(DocumentGraph { name: name? }),
                content,
                embedding: embedding.to_vec(),
                life: lifespan_into_vec(life),
            },
            Document::Node {
                entity,
                content,
                embedding,
                life,
            } => Self {
                entity: DocumentEntity::Node(entity),
                content,
                embedding: embedding.to_vec(),
                life: lifespan_into_vec(life),
            },
            Document::Edge {
                entity,
                content,
                embedding,
                life,
            } => Self {
                entity: DocumentEntity::Edge(entity),
                content,
                embedding: embedding.to_vec(),
                life: lifespan_into_vec(life),
            },
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
