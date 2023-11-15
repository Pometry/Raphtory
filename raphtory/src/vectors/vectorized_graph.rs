use crate::{
    core::{entities::vertices::vertex_ref::VertexRef, utils::time::IntoTime},
    prelude::{EdgeViewOps, GraphViewOps, VertexViewOps},
    vectors::{
        document_ref::DocumentRef, document_template::DocumentTemplate, entity_id::EntityId,
        vectorized_graph_selection::VectorizedGraphSelection, EmbeddingFunction, ScoredDocument,
    },
};
use itertools::{chain, Itertools};
use std::{
    cmp::{max, min},
    collections::HashMap,
    sync::Arc,
};

pub struct VectorizedGraph<G: GraphViewOps, T: DocumentTemplate<G>> {
    pub(crate) source_graph: G,
    pub(crate) template: Arc<T>,
    pub(crate) embedding: Arc<dyn EmbeddingFunction>,
    // it is not the end of the world but we are storing the entity id twice
    pub(crate) node_documents: Arc<HashMap<EntityId, Vec<DocumentRef>>>, // TODO: replace with FxHashMap
    pub(crate) edge_documents: Arc<HashMap<EntityId, Vec<DocumentRef>>>,
    pub(crate) window_start: Option<i64>,
    pub(crate) window_end: Option<i64>,
    empty_vec: Vec<DocumentRef>,
}

impl<G: GraphViewOps, T: DocumentTemplate<G>> Clone for VectorizedGraph<G, T> {
    fn clone(&self) -> Self {
        Self::new(
            self.source_graph.clone(),
            self.template.clone(),
            self.embedding.clone(),
            self.node_documents.clone(),
            self.edge_documents.clone(),
            self.window_start,
            self.window_end,
        )
    }
}

impl<G: GraphViewOps, T: DocumentTemplate<G>> VectorizedGraph<G, T> {
    pub(crate) fn new(
        graph: G,
        template: Arc<T>,
        embedding: Arc<dyn EmbeddingFunction>,
        node_documents: Arc<HashMap<EntityId, Vec<DocumentRef>>>,
        edge_documents: Arc<HashMap<EntityId, Vec<DocumentRef>>>,
        window_start: Option<i64>,
        window_end: Option<i64>,
    ) -> Self {
        Self {
            source_graph: graph,
            template,
            embedding,
            node_documents,
            edge_documents,
            window_start,
            window_end,
            empty_vec: vec![],
        }
    }

    pub fn window<I: IntoTime>(&self, start: Option<I>, end: Option<I>) -> Self {
        let start = start.map(|start| start.into_time()).unwrap_or(i64::MIN);
        let end = end.map(|end| end.into_time()).unwrap_or(i64::MAX);
        let w_start = self.window_start;
        let w_end = self.window_end;
        let cloned_graph = self.clone();
        Self {
            window_start: w_start.map(|w_start| max(w_start, start)).or(Some(start)),
            window_end: w_end.map(|w_end| min(w_end, end)).or(Some(end)),
            ..cloned_graph
        }
    }

    pub fn empty_selection(&self) -> VectorizedGraphSelection<G, T> {
        VectorizedGraphSelection::new(self.clone(), vec![])
    }

    /// This assumes forced documents to have a score of 0
    pub fn select<V: Into<VertexRef>>(
        &self,
        nodes: Vec<V>,
        edges: Vec<(V, V)>,
    ) -> VectorizedGraphSelection<G, T> {
        let node_docs = nodes.into_iter().flat_map(|id| {
            let vertex = self.source_graph.vertex(id);
            let opt = vertex.map(|vertex| self.node_documents.get(&EntityId::from_node(&vertex)));
            opt.flatten().unwrap_or(&self.empty_vec)
        });
        let edge_docs = edges.into_iter().flat_map(|(src, dst)| {
            let edge = self.source_graph.edge(src, dst);
            let opt = edge.map(|edge| self.edge_documents.get(&EntityId::from_edge(&edge)));
            opt.flatten().unwrap_or(&self.empty_vec)
        });
        let selected = chain!(node_docs, edge_docs)
            .map(|doc| ScoredDocument::new(doc.clone(), 0.0))
            .collect_vec();
        VectorizedGraphSelection::new(self.clone(), selected)
    }

    // this might return the document used as input, uniqueness need to be check outside of this
    pub(crate) fn get_context<'a, W: GraphViewOps>(
        &'a self,
        document: &DocumentRef,
        windowed_graph: &'a W,
        start: Option<i64>,
        end: Option<i64>,
    ) -> Box<dyn Iterator<Item = &DocumentRef> + '_> {
        match document.entity_id {
            EntityId::Node { id } => {
                let self_docs = self.node_documents.get(&document.entity_id).unwrap();
                let edges = windowed_graph.vertex(id).unwrap().edges();
                let edge_docs = edges.flat_map(|edge| {
                    let edge_id = EntityId::from_edge(&edge);
                    self.edge_documents.get(&edge_id).unwrap_or(&self.empty_vec)
                });
                Box::new(
                    chain!(self_docs, edge_docs)
                        .filter(move |doc| doc.exists_on_window(windowed_graph, start, end)),
                )
            }
            EntityId::Edge { src, dst } => {
                let self_docs = self.edge_documents.get(&document.entity_id).unwrap();
                let edge = windowed_graph.edge(src, dst).unwrap();
                let src_id = EntityId::from_node(&edge.src());
                let dst_id = EntityId::from_node(&edge.dst());
                let src_docs = self.node_documents.get(&src_id).unwrap();
                let dst_docs = self.node_documents.get(&dst_id).unwrap();
                Box::new(
                    chain!(self_docs, src_docs, dst_docs)
                        .filter(move |doc| doc.exists_on_window(windowed_graph, start, end)),
                )
            }
        }
    }
}
