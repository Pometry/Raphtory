use crate::{
    db::api::view::StaticGraphViewOps,
    vectors::{entity_id::EntityId, template::DocumentTemplate, Document, Embedding, Lifespan},
};
use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};

use super::DocumentEntity;

/// this struct contains the minimum amount of information need to regenerate a document using a
/// template and to quickly apply windows over them
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct DocumentRef {
    pub(crate) entity_id: EntityId,
    index: usize,
    pub(crate) embedding: Embedding,
    pub(crate) life: Lifespan,
}

impl Hash for DocumentRef {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match &self.entity_id {
            EntityId::Graph { .. } => (),
            EntityId::Node { id } => id.hash(state),
            EntityId::Edge { src, dst } => {
                src.hash(state);
                dst.hash(state);
            }
        };
        state.write_usize(self.index);
    }
}

impl PartialEq for DocumentRef {
    fn eq(&self, other: &Self) -> bool {
        self.entity_id == other.entity_id && self.index == other.index
    }
}

impl Eq for DocumentRef {}

impl DocumentRef {
    pub fn new(entity_id: EntityId, index: usize, embedding: Embedding, life: Lifespan) -> Self {
        Self {
            entity_id,
            index,
            embedding,
            life,
        }
    }
    #[allow(dead_code)]
    pub fn id(&self) -> (EntityId, usize) {
        (self.entity_id.clone(), self.index)
    }

    // TODO: review -> does window really need to be an Option
    /// This function expects a graph with a window that matches the one provided in `window`
    pub fn exists_on_window<G>(&self, graph: Option<&G>, window: &Option<(i64, i64)>) -> bool
    where
        G: StaticGraphViewOps,
    {
        match self.life {
            Lifespan::Event { time } => {
                self.entity_exists_in_graph(graph)
                    && window
                        .map(|(start, end)| start <= time && time < end)
                        .unwrap_or(true)
            }
            Lifespan::Interval {
                start: doc_start,
                end: doc_end,
            } => {
                self.entity_exists_in_graph(graph)
                    && window
                        .map(|(start, end)| doc_end > start && doc_start < end)
                        .unwrap_or(true)
            }
            Lifespan::Inherited => self.entity_exists_in_graph(graph),
        }
    }

    fn entity_exists_in_graph<G: StaticGraphViewOps>(&self, graph: Option<&G>) -> bool {
        match &self.entity_id {
            EntityId::Graph { .. } => true, // TODO: maybe consider dead a graph with no entities
            EntityId::Node { id } => graph.map(|g| g.has_node(id)).unwrap_or(true),
            EntityId::Edge { src, dst } => graph.map(|g| g.has_edge(src, dst)).unwrap_or(true),
            // TODO: Edge should probably contain a layer filter that we can pass to has_edge()
        }
    }

    pub fn regenerate<G>(&self, original_graph: &G, template: &DocumentTemplate) -> Document<G>
    where
        G: StaticGraphViewOps,
    {
        // FIXME: there is a problem here. We need to use the original graph so the number of
        // documents is the same and the index is therefore consistent. However, we want to return
        // the document using the windowed values for the properties of the entities
        let (entity, content) = match &self.entity_id {
            EntityId::Graph { name } => (
                DocumentEntity::Graph {
                    name: name.clone(),
                    graph: original_graph.clone(),
                },
                template
                    .graph(original_graph)
                    .nth(self.index)
                    .unwrap()
                    .content,
            ),
            EntityId::Node { id } => (
                DocumentEntity::Node(original_graph.node(id).unwrap()),
                template
                    .node((&&original_graph).node(id).unwrap())
                    .nth(self.index)
                    .unwrap()
                    .content,
            ),
            EntityId::Edge { src, dst } => (
                DocumentEntity::Edge(original_graph.edge(src, dst).unwrap()),
                template
                    .edge(original_graph.edge(src, dst).unwrap().as_ref())
                    .nth(self.index)
                    .unwrap()
                    .content,
            ),
        };
        Document {
            entity,
            content,
            embedding: self.embedding.clone(),
            life: self.life,
        }
    }
}
