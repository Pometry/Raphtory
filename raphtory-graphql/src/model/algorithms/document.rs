use dynamic_graphql::SimpleObject;
use raphtory::core::Lifespan;
use raphtory::vectors::Document;

#[derive(SimpleObject)]
pub struct GqlDocument {
    /// Return a vector with the name of the node or the names of src and dst of the edge: [src, dst]
    name: Vec<String>, // size 1 for nodes, size 2 for edges: [src, dst]
    /// Return the type of entity: "node" or "edge"
    entity_type: String,
    content: String,
    life: Vec<i64>,
}

impl From<Document> for GqlDocument {
    fn from(value: Document) -> Self {
        match value {
            Document::Graph {
                name,
                content,
                life,
            } => Self {
                name: vec![name],
                entity_type: "graph".to_owned(),
                content,
                life: lifespan_into_vec(life),
            },
            Document::Node {
                name,
                content,
                life,
            } => Self {
                name: vec![name],
                entity_type: "node".to_owned(),
                content,
                life: lifespan_into_vec(life),
            },
            Document::Edge {
                src,
                dst,
                content,
                life,
            } => Self {
                name: vec![src, dst],
                entity_type: "edge".to_owned(),
                content,
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
