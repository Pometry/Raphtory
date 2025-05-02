use itertools::{chain, Itertools};
use std::{
    collections::{HashMap, HashSet},
    ops::Deref,
};

use crate::{
    core::entities::nodes::node_ref::AsNodeRef,
    db::{
        api::{
            storage::graph::edges::edge_storage_ops::EdgeStorageOps,
            view::{BaseNodeViewOps, DynamicGraph, StaticGraphViewOps},
        },
        graph::{edge::EdgeView, node::NodeView},
    },
    prelude::{EdgeViewOps, NodeViewOps, *},
};

use super::{
    document_ref::DocumentRef,
    entity_id::EntityId,
    similarity_search_utils::{
        apply_window, find_top_k, score_document_groups_by_highest, score_documents,
    },
    vectorised_graph::{EntityRef, VectorSearch, VectorisedGraph},
    Document, DocumentEntity, Embedding,
};

#[derive(Clone, Copy)]
enum ExpansionPath {
    Nodes,
    Edges,
    Both,
}

pub type DynamicVectorSelection = VectorSelection<DynamicGraph>;

#[derive(Clone)]
pub struct VectorSelection<G: StaticGraphViewOps> {
    pub(crate) graph: VectorisedGraph<G>,
    selected: Vec<(EntityRef, f32)>,
}

impl<G: StaticGraphViewOps> VectorSelection<G> {
    pub(crate) fn empty(graph: VectorisedGraph<G>) -> Self {
        Self {
            graph,
            selected: vec![],
        }
    }

    pub(crate) fn new(graph: VectorisedGraph<G>, docs: Vec<(EntityRef, f32)>) -> Self {
        Self {
            graph,
            selected: docs,
        }
    }

    /// Return the nodes present in the current selection
    pub fn nodes(&self) -> Vec<NodeView<G>> {
        let g = &self.graph.source_graph;
        self.selected
            .iter()
            .filter_map(|(id, _)| id.as_node())
            .map(|id| g.node(g.node_id(id.into())).unwrap())
            .collect()
    }

    /// Return the edges present in the current selection
    pub fn edges(&self) -> Vec<EdgeView<G>> {
        let g = &self.graph.source_graph;
        self.selected
            .iter()
            .filter_map(|(id, _)| id.as_edge())
            .map(|id| g.core_edge(id.into()))
            .map(|edge| g.edge(edge.src(), edge.dst()).unwrap())
            .collect()
    }

    /// Return the documents present in the current selection
    pub fn get_documents(&self) -> Vec<Document<G>> {
        self.get_documents_with_scores()
            .into_iter()
            .map(|(doc, _)| doc)
            .collect_vec()
    }

    /// Return the documents alongside their scores present in the current selection
    pub fn get_documents_with_scores(&self) -> Vec<(Document<G>, f32)> {
        self.selected
            .iter()
            .map(|(entity, score)| (self.regenerate_doc(*entity), *score))
            .collect()
    }

    /// Add all `nodes` to the current selection
    ///
    /// Documents added by this call are assumed to have a score of 0.
    /// If any node id doesn't exist it will be ignored
    ///
    /// # Arguments
    ///   * nodes - a list of the node ids or nodes to add
    pub fn add_nodes<V: AsNodeRef>(&mut self, nodes: Vec<V>) {
        let new_docs = nodes
            .into_iter()
            .filter_map(|id| self.graph.source_graph.node(id))
            .map(|node| EntityRef::Node(node.node.index()));
        self.selected.extend(new_docs.map(|doc| (doc, 0.0))); // TODO: can add duplicated, use the extend associated function
    }

    /// Add all `edges` to the current selection
    ///
    /// Documents added by this call are assumed to have a score of 0.
    /// If any edge doesn't exist it will be ignored
    ///
    /// # Arguments
    ///   * edges - a list of the edge ids or edges to add
    pub fn add_edges<V: AsNodeRef>(&mut self, edges: Vec<(V, V)>) {
        let new_docs = edges
            .into_iter()
            .filter_map(|(src, dst)| self.graph.source_graph.edge(src, dst))
            .map(|edge| EntityRef::Edge(edge.edge.pid().0));
        // self.extend_selection_with_refs(new_docs);
        self.selected.extend(new_docs.map(|doc| (doc, 0.0))); // TODO: can add duplicated, use the extend associated function
    }

    /// Append all the documents in `selection` to the current selection
    ///
    /// # Arguments
    ///   * selection - a selection to be added
    ///
    /// # Returns
    ///   The selection with the new documents
    pub fn append(&mut self, selection: &Self) -> &Self {
        self.selected.extend(self.selected.clone());
        self
    }

    /// Add all the documents `hops` hops away to the selection
    ///
    /// Two documents A and B are considered to be 1 hop away of each other if they are on the same
    /// entity or if they are on the same node/edge pair. Provided that, two nodes A and C are n
    /// hops away of  each other if there is a document B such that A is n - 1 hops away of B and B
    /// is 1 hop away of C.
    ///
    /// # Arguments
    ///   * hops - the number of hops to carry out the expansion
    ///   * window - the window where documents need to belong to in order to be considered
    pub fn expand(&mut self, hops: usize, window: Option<(i64, i64)>) {
        match window {
            None => self.expand_with_window(hops, window, &self.graph.source_graph.clone()),
            Some((start, end)) => {
                let windowed_graph = self.graph.source_graph.window(start, end);
                self.expand_with_window(hops, window, &windowed_graph)
            }
        }
    }

    /// Add the top `limit` adjacent entities with higher score for `query` to the selection
    ///
    /// The expansion algorithm is a loop with two steps on each iteration:
    ///   1. All the entities 1 hop away of some of the entities included on the selection (and
    /// not already selected) are marked as candidates.
    ///   2. Those candidates are added to the selection in descending order according to the
    /// similarity score obtained against the `query`.
    ///
    /// This loops goes on until the number of new entities reaches a total of `limit`
    /// entities or until no more documents are available
    ///
    /// # Arguments
    ///   * query - the embedding to score against
    ///   * window - the window where documents need to belong to in order to be considered
    pub fn expand_entities_by_similarity(
        &mut self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
    ) {
        let g = &self.graph.source_graph;
        let view = apply_window(g, window);

        let filter = self.get_nodes_in_context(window);
        let nodes = self.graph.node_db.top_k(query, limit, view, Some(filter));

        let filter = self.get_edges_in_context(window);
        let edges = self.graph.edge_db.top_k(query, limit, view, Some(filter));

        let docs = find_top_k(nodes.chain(edges), limit);
        self.extend_selection(docs, limit);
    }

    /// Add the top `limit` adjacent nodes with higher score for `query` to the selection
    ///
    /// This function has the same behavior as expand_entities_by_similarity but it only considers nodes.
    ///
    /// # Arguments
    ///   * query - the embedding to score against
    ///   * limit - the maximum number of new nodes to add
    ///   * window - the window where documents need to belong to in order to be considered
    pub fn expand_nodes_by_similarity(
        &mut self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
    ) {
        let g = &self.graph.source_graph;
        let view = apply_window(g, window);
        let filter = self.get_nodes_in_context(window);
        let docs = self.graph.node_db.top_k(query, limit, view, Some(filter));
        self.extend_selection(docs, limit);
        // TODO: call this recursively until new projected length is reached
    }

    /// Add the top `limit` adjacent edges with higher score for `query` to the selection
    ///
    /// This function has the same behavior as expand_entities_by_similarity but it only considers edges.
    ///
    /// # Arguments
    ///   * query - the embedding to score against
    ///   * limit - the maximum number of new edges to add
    ///   * window - the window where documents need to belong to in order to be considered
    pub fn expand_edges_by_similarity(
        &mut self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
    ) {
        let g = &self.graph.source_graph;
        let view = apply_window(g, window);
        let filter = self.get_edges_in_context(window);
        let docs = self.graph.edge_db.top_k(query, limit, view, Some(filter));
        self.extend_selection(docs, limit);
    }

    fn get_selected_entity_id_set(&self) -> HashSet<EntityId> {
        HashSet::from_iter(self.selected.iter().map(|doc| doc.0.entity_id.clone()))
    }

    fn get_selected_entity_len(&self) -> usize {
        self.get_selected_entity_id_set().len()
    }

    fn get_nodes_in_context(&self, window: Option<(i64, i64)>) -> HashSet<EntityRef> {
        match window {
            Some((start, end)) => {
                self.get_nodes_in_context_for_view(&self.graph.source_graph.window(start, end))
            }
            None => self.get_nodes_in_context_for_view(&self.graph.source_graph),
        }
    }

    fn get_edges_in_context(&self, window: Option<(i64, i64)>) -> HashSet<EntityRef> {
        match window {
            Some((start, end)) => {
                self.get_edges_in_context_for_view(&self.graph.source_graph.window(start, end))
            }
            None => self.get_edges_in_context_for_view(&self.graph.source_graph),
        }
    }

    fn get_nodes_in_context_for_view<W: StaticGraphViewOps>(&self, v: &W) -> HashSet<EntityRef> {
        let iter = self.selected.iter();
        iter.flat_map(|(e, _)| e.get_neighbour_nodes(v)).collect()
    }

    fn get_edges_in_context_for_view<W: StaticGraphViewOps>(&self, v: &W) -> HashSet<EntityRef> {
        let iter = self.selected.iter();
        iter.flat_map(|(e, _)| e.get_neighbour_edges(v)).collect()
    }

    fn regenerate_doc(&self, entity: EntityRef) -> Document<G> {
        let graph = &self.graph.source_graph;
        match entity {
            EntityRef::Node(id) => {
                let node = graph.node(graph.node_id(id.into())).unwrap();
                let embedding = self.graph.node_db.get_id(id);
                Document {
                    entity: DocumentEntity::Node(node),
                    content: self.graph.template.node(node).unwrap(),
                    embedding,
                }
            }
            EntityRef::Edge(id) => {
                let edge_ref = graph.core_edge(id.into());
                let edge = graph.edge(edge_ref.src(), edge_ref.dst()).unwrap();
                let embedding = self.graph.edge_db.get_id(id);
                Document {
                    entity: DocumentEntity::Edge(edge),
                    content: self.graph.template.edge(edge).unwrap(),
                    embedding,
                }
            }
        }
    }

    /// this function assumes that extension might contain duplicates and might contain elements
    /// already present in selection, and returns a sequence with no repetitions and preserving the
    /// elements in selection in the same indexes
    fn extend_selection<I>(&mut self, extension: I, limit: usize)
    where
        I: Iterator<Item = (EntityRef, f32)>,
    {
        let selection_set: HashSet<EntityRef> =
            HashSet::from_iter(self.selected.iter().map(|(doc, _)| doc.clone()));
        let new_docs = extension
            .unique_by(|(entity, _)| entity)
            .filter(|(entity, _)| !selection_set.contains(entity))
            .take(limit);
        self.selected.extend(new_docs);
    }
}

impl EntityRef {
    fn get_neighbour_nodes<G: StaticGraphViewOps>(
        &self,
        view: &G,
    ) -> impl Iterator<Item = EntityRef> {
        let nodes: Box<dyn Iterator<Item = NodeView<_>>> =
            if let Some(node) = self.as_node_view(view) {
                let docs = node.neighbours().into_iter();
                Box::new(docs)
            } else if let Some(edge) = self.as_edge_view(view) {
                Box::new([edge.src(), edge.dst()].into_iter())
            } else {
                Box::new(std::iter::empty())
            };
        nodes.map(|node| EntityRef::Node(node.node.index()))
    }

    fn get_neighbour_edges<G: StaticGraphViewOps>(
        &self,
        view: &G,
    ) -> impl Iterator<Item = EntityRef> {
        let edges: Box<dyn Iterator<Item = EdgeView<_>>> =
            if let Some(node) = self.as_node_view(view) {
                let docs = node.edges().into_iter();
                Box::new(docs)
            } else if let Some(edge) = self.as_edge_view(view) {
                Box::new(std::iter::empty()) // FIXME: probably jump to edges of src and dst, I can call get_neighbour_nodes
            } else {
                Box::new(std::iter::empty())
            };
        edges.map(|node| EntityRef::Edge(edge.node.index()))
    }
}
