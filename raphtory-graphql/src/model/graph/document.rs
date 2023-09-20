use dynamic_graphql::SimpleObject;
use raphtory::vectors::Document;

#[derive(SimpleObject)]
pub(crate) struct GqlDocument {
    id: String,
    entity_type: String,
    content: String,
}

impl From<Document> for GqlDocument {
    fn from(value: Document) -> Self {
        match value {
            Document::Node { id, content } => Self {
                id: id.to_string(),
                entity_type: "node".to_owned(),
                content,
            },
            Document::Edge { src, dst, content } => Self {
                id: format!("{src}-{dst}"),
                entity_type: "edge".to_owned(),
                content,
            },
        }
    }
}
