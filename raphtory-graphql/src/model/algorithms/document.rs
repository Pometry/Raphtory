use crate::model::graph::{edge::Edge, node::Node};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields, SimpleObject};
use raphtory::{core::Lifespan, vectors::Document};

#[derive(ResolvedObject)]
pub struct GqlDocuments {
    documents: Vec<GqlDocument>,
    nodes: Vec<Node>,
    edges: Vec<Edge>,
}

impl GqlDocuments {
    pub fn new(documents: Vec<GqlDocument>, nodes: Vec<Node>, edges: Vec<Edge>) -> Self {
        Self {
            documents,
            nodes,
            edges,
        }
    }
    pub fn into_iter(self) -> (Vec<GqlDocument>, Vec<Node>, Vec<Edge>) {
        (self.documents, self.nodes, self.edges)
    }
}

#[ResolvedObjectFields]
impl GqlDocuments {
    async fn documents(&self) -> Vec<GqlDocument> {
        self.documents.clone()
    }

    async fn nodes(&self) -> Vec<Node> {
        self.nodes.clone()
    }

    async fn edges(&self) -> Vec<Edge> {
        self.edges.clone()
    }
}

#[derive(SimpleObject, Clone)]
pub struct GqlDocument {
    /// Return a vector with the name of the node or the names of src and dst of the edge: [src, dst]
    name: Vec<String>, // size 1 for nodes, size 2 for edges: [src, dst]
    /// Return the type of entity: "node" or "edge"
    entity_type: String,
    content: String,
    embedding: Vec<f32>,
    life: Vec<i64>,
}

impl From<Document> for GqlDocument {
    fn from(value: Document) -> Self {
        match value {
            Document::Graph {
                name,
                content,
                embedding,
                life,
            } => Self {
                name: name.map(|name| vec![name]).unwrap_or(vec![]),
                entity_type: "graph".to_owned(),
                content,
                embedding: embedding.to_vec(),
                life: lifespan_into_vec(life),
            },
            Document::Node {
                name,
                content,
                embedding,
                life,
            } => Self {
                name: vec![name],
                entity_type: "node".to_owned(),
                content,
                embedding: embedding.to_vec(),
                life: lifespan_into_vec(life),
            },
            Document::Edge {
                src,
                dst,
                content,
                embedding,
                life,
            } => Self {
                name: vec![src, dst],
                entity_type: "edge".to_owned(),
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
