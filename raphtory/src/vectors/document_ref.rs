use crate::{
    prelude::{GraphViewOps, Layer, TimeOps, VertexViewOps},
    vectors::{document_template::DocumentTemplate, entity_id::EntityId, Document, Embedding},
};

/// this struct contains the minimum amount of information need to regenerate a document using a
/// template and to quickly apply windows over them
#[derive(Clone)]
pub(crate) struct DocumentRef {
    pub(crate) entity_id: EntityId,
    index: usize,
    pub(crate) embedding: Embedding,
    life: Life,
}

#[derive(Clone)]
pub(crate) enum Life {
    Interval { start: i64, end: i64 },
    Event { time: i64 },
    Inherited,
}

impl PartialEq for DocumentRef {
    fn eq(&self, other: &Self) -> bool {
        self.entity_id == other.entity_id && self.index == other.index
    }
}

impl DocumentRef {
    pub fn new(entity_id: EntityId, index: usize, embedding: Embedding, life: Life) -> Self {
        Self {
            entity_id,
            index,
            embedding,
            life,
        }
    }
    pub fn id(&self) -> (EntityId, usize) {
        (self.entity_id, self.index)
    }
    pub fn exists_on_window<G: GraphViewOps>(&self, graph: &G) -> bool {
        match self.life {
            Life::Event { time } => graph.start().unwrap() <= time && time <= graph.end().unwrap(),
            Life::Interval {
                start: start,
                end: end,
            } => end >= graph.start().unwrap() && start <= graph.end().unwrap(),
            Life::Inherited => match self.entity_id {
                EntityId::Node { id } => graph.has_vertex(id),
                EntityId::Edge { src, dst } => graph.has_edge(src, dst, Layer::All),
                // FIXME: Edge should probably contain a layer filter that we can pass to has_edge()
            },
        }
    }
    pub fn regenerate<G, T>(&self, original_graph: &G) -> Document
    where
        G: GraphViewOps,
        T: DocumentTemplate,
    {
        // FIXME: there is a problem here. We need to use the original graph so the number of
        // documents is the same and the index is therefore consistent. However, we want to return
        // the document using the windowed values for the properties of the entities
        match self.entity_id {
            EntityId::Node { id } => Document::Node {
                name: original_graph.vertex(id).unwrap().name(),
                content: T::node(&original_graph.vertex(id).unwrap())
                    .nth(self.index)
                    .unwrap()
                    .content,
            },
            EntityId::Edge { src, dst } => Document::Edge {
                src: original_graph.vertex(src).unwrap().name(),
                dst: original_graph.vertex(dst).unwrap().name(),
                content: T::edge(&original_graph.edge(src, dst).unwrap())
                    .nth(self.index)
                    .unwrap()
                    .content,
            },
        }
    }
}
