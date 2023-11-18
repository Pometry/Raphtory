use crate::{
    prelude::{GraphViewOps, Layer, VertexViewOps},
    vectors::{
        document_template::DocumentTemplate, entity_id::EntityId, Document, Embedding, Lifespan,
    },
};
use std::hash::{Hash, Hasher};

/// this struct contains the minimum amount of information need to regenerate a document using a
/// template and to quickly apply windows over them
#[derive(Clone, Debug)]
pub(crate) struct DocumentRef {
    pub(crate) entity_id: EntityId,
    index: usize,
    pub(crate) embedding: Embedding,
    life: Lifespan,
}

impl Hash for DocumentRef {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self.entity_id {
            EntityId::Node { id } => state.write_u64(id),
            EntityId::Edge { src, dst } => {
                state.write_u64(src);
                state.write_u64(dst);
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
    pub fn id(&self) -> (EntityId, usize) {
        (self.entity_id, self.index)
    }

    // TODO: review -> does window really need to be an Option
    pub fn exists_on_window<G>(&self, graph: &G, window: Option<(i64, i64)>) -> bool
    where
        G: GraphViewOps,
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

    fn entity_exists_in_graph<G: GraphViewOps>(&self, graph: &G) -> bool {
        match self.entity_id {
            EntityId::Node { id } => graph.has_vertex(id),
            EntityId::Edge { src, dst } => graph.has_edge(src, dst, Layer::All),
            // TODO: Edge should probably contain a layer filter that we can pass to has_edge()
        }
    }

    pub fn regenerate<G, T>(&self, original_graph: &G, template: &T) -> Document
    where
        G: GraphViewOps,
        T: DocumentTemplate<G>,
    {
        // FIXME: there is a problem here. We need to use the original graph so the number of
        // documents is the same and the index is therefore consistent. However, we want to return
        // the document using the windowed values for the properties of the entities
        match self.entity_id {
            EntityId::Node { id } => Document::Node {
                name: original_graph.vertex(id).unwrap().name(),
                content: template
                    .node(&original_graph.vertex(id).unwrap())
                    .nth(self.index)
                    .unwrap()
                    .content,
            },
            EntityId::Edge { src, dst } => Document::Edge {
                src: original_graph.vertex(src).unwrap().name(),
                dst: original_graph.vertex(dst).unwrap().name(),
                content: template
                    .edge(&original_graph.edge(src, dst).unwrap())
                    .nth(self.index)
                    .unwrap()
                    .content,
            },
        }
    }
}
