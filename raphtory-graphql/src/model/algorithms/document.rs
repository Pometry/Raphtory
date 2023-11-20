use dynamic_graphql::SimpleObject;
use raphtory::vectors::Document;

#[derive(SimpleObject)]
pub struct GqlDocument {
    /// Return a vector with the name of the node or the names of src and dst of the edge: [src, dst]
    name: Vec<String>, // size 1 for nodes, size 2 for edges: [src, dst]
    /// Return the type of entity: "node" or "edge"
    entity_type: String,
    content: String,
}

impl From<Document> for GqlDocument {
    fn from(value: Document) -> Self {
        match value {
            Document::Node { name, content } => Self {
                name: vec![name],
                entity_type: "node".to_owned(),
                content,
            },
            Document::Edge { src, dst, content } => Self {
                name: vec![src, dst],
                entity_type: "edge".to_owned(),
                content,
            },
        }
    }
}
