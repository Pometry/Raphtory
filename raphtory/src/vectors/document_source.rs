use crate::{
    db::graph::{edge::EdgeView, vertex::VertexView},
    prelude::{EdgeViewOps, GraphViewOps, VertexViewOps},
    vectors::{entity_id::EntityId, EntityDocument},
};

pub(crate) trait DocumentSource: Sized {
    fn generate_doc<T>(&self, template: &T) -> EntityDocument
    where
        T: Fn(&Self) -> String;
}

impl<G: GraphViewOps> DocumentSource for VertexView<G> {
    fn generate_doc<T>(&self, template: &T) -> EntityDocument
    where
        T: Fn(&Self) -> String,
    {
        let raw_content = template(self);
        let content = match raw_content.char_indices().nth(1000) {
            Some((index, _)) => (&raw_content[..index]).to_owned(),
            None => raw_content,
        };
        // TODO: allow multi document entities !!!!!
        // shortened to 1000 (around 250 tokens) to avoid exceeding the max number of tokens,
        // when embedding but also when inserting documents into prompts

        EntityDocument {
            id: EntityId::Node { id: self.id() },
            content,
        }
    }
}

impl<G: GraphViewOps> DocumentSource for EdgeView<G> {
    fn generate_doc<T>(&self, template: &T) -> EntityDocument
    where
        T: Fn(&Self) -> String,
    {
        let content = template(self);
        EntityDocument {
            id: EntityId::Edge {
                src: self.src().id(),
                dst: self.dst().id(),
            },
            content,
        }
    }
}
