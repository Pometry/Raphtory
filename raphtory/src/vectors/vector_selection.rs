use std::{collections::HashSet, usize};

use itertools::{chain, Itertools};

use crate::{
    core::entities::nodes::node_ref::AsNodeRef,
    db::{
        api::view::{DynamicGraph, StaticGraphViewOps},
        graph::{edge::EdgeView, node::NodeView},
    },
    prelude::*,
    prelude::{EdgeViewOps, NodeViewOps},
};

use super::{
    document_ref::DocumentRef,
    document_template::DocumentTemplate,
    entity_id::EntityId,
    similarity_search_utils::{find_top_k, score_document_groups_by_highest, score_documents},
    vectorised_graph::{DynamicTemplate, VectorisedGraph},
    Document, Embedding,
};

#[derive(Clone, Copy)]
enum ExpansionPath {
    Nodes,
    Edges,
    Both,
}

pub type DynamicVectorSelection = VectorSelection<DynamicGraph, DynamicTemplate>;

pub struct VectorSelection<G: StaticGraphViewOps, T: DocumentTemplate<G>> {
    pub(crate) graph: VectorisedGraph<G, T>,
    selected_docs: Vec<(DocumentRef, f32)>,
    // selected_entities: Vec<(EntityId, f32)>,
}

impl<G: StaticGraphViewOps, T: DocumentTemplate<G>> VectorSelection<G, T> {
    pub(crate) fn new(graph: VectorisedGraph<G, T>) -> Self {
        Self {
            graph,
            selected_docs: vec![],
        }
    }

    pub(crate) fn new_with_preselection(
        graph: VectorisedGraph<G, T>,
        docs: Vec<(DocumentRef, f32)>,
    ) -> Self {
        Self {
            graph,
            selected_docs: docs,
        }
    }

    /// Return the nodes present in the current selection
    pub fn nodes(&self) -> Vec<NodeView<G>> {
        self.selected_docs
            .iter()
            .filter_map(|(doc, _)| match &doc.entity_id {
                EntityId::Node { id } => self.graph.source_graph.node(id),
                _ => None,
            })
            .collect_vec()
    }

    /// Return the edges present in the current selection
    pub fn edges(&self) -> Vec<EdgeView<G>> {
        self.selected_docs
            .iter()
            .filter_map(|(doc, _)| match &doc.entity_id {
                EntityId::Edge { src, dst } => self.graph.source_graph.edge(src, dst),
                _ => None,
            })
            .collect_vec()
    }

    /// Return the documents present in the current selection
    pub fn get_documents(&self) -> Vec<Document> {
        self.get_documents_with_scores()
            .into_iter()
            .map(|(doc, _)| doc)
            .collect_vec()
    }

    /// Return the documents alongside their scores present in the current selection
    pub fn get_documents_with_scores(&self) -> Vec<(Document, f32)> {
        self.selected_docs
            .iter()
            .map(|(doc, score)| {
                (
                    doc.regenerate(&self.graph.source_graph, self.graph.template.as_ref()),
                    *score,
                )
            })
            .collect_vec()
    }

    /// Add all the documents associated with the `nodes` to the current selection
    ///
    /// Documents added by this call are assumed to have a score of 0.
    ///
    /// # Arguments
    ///   * nodes - a list of the node ids or nodes to add
    pub fn add_nodes<V: AsNodeRef>(&mut self, nodes: Vec<V>) {
        let node_docs = nodes
            .into_iter()
            .flat_map(|id| {
                let node = self.graph.source_graph.node(id);
                let opt =
                    node.map(|node| self.graph.node_documents.get(&EntityId::from_node(&node)));
                opt.flatten().unwrap_or(&self.graph.empty_vec)
            })
            .map(|doc| (doc.clone(), 0.0));
        self.selected_docs = extend_selection(self.selected_docs.clone(), node_docs, usize::MAX);
    }

    /// Add all the documents associated with the `edges` to the current selection
    ///
    /// Documents added by this call are assumed to have a score of 0.
    ///
    /// # Arguments
    ///   * edges - a list of the edge ids or edges to add
    pub fn add_edges<V: AsNodeRef>(&mut self, edges: Vec<(V, V)>) {
        let edge_docs = edges
            .into_iter()
            .flat_map(|(src, dst)| {
                let edge = self.graph.source_graph.edge(src, dst);
                let opt =
                    edge.map(|edge| self.graph.edge_documents.get(&EntityId::from_edge(&edge)));
                opt.flatten().unwrap_or(&self.graph.empty_vec)
            })
            .map(|doc| (doc.clone(), 0.0));
        self.selected_docs = extend_selection(self.selected_docs.clone(), edge_docs, usize::MAX);
    }

    /// Add all the documents in `selection` to the current selection
    ///
    /// # Arguments
    ///   * selection - a selection to be added
    ///
    /// # Returns
    ///   A new selection containing the join
    pub fn join(&self, selection: &Self) -> Self {
        Self {
            selected_docs: extend_selection(
                self.selected_docs.clone(),
                selection.selected_docs.clone().into_iter(),
                usize::MAX,
            ),
            graph: self.graph.clone(),
        }
    }

    // /// Add all the documents from `nodes` and `edges` to the current selection
    // ///
    // /// Documents added by this call are assumed to have a score of 0.
    // ///
    // /// # Arguments
    // ///   * nodes - a list of the node ids or nodes to add
    // ///   * edges - a list of the edge ids or edges to add
    // ///
    // /// # Returns
    // ///   A new vectorised graph containing the updated selection
    // pub fn append<V: AsNodeRef>(&mut self, nodes: Vec<V>, edges: Vec<(V, V)>) {
    //     let node_docs = nodes.into_iter().flat_map(|id| {
    //         let node = self.source_graph.node(id);
    //         let opt = node.map(|node| self.node_documents.get(&EntityId::from_node(&node)));
    //         opt.flatten().unwrap_or(&self.empty_vec)
    //     });
    //     let edge_docs = edges.into_iter().flat_map(|(src, dst)| {
    //         let edge = self.source_graph.edge(src, dst);
    //         let opt = edge.map(|edge| self.edge_documents.get(&EntityId::from_edge(&edge)));
    //         opt.flatten().unwrap_or(&self.empty_vec)
    //     });
    //     let new_selected = chain!(node_docs, edge_docs).map(|doc| (doc.clone(), 0.0));
    //     Self {
    //         selected_docs: extend_selection(self.selected_docs.clone(), new_selected, usize::MAX),
    //         ..self.clone()
    //     }
    // }

    // /// Add all the documents for this graph to the current selection
    // ///
    // /// Documents added by this call are assumed to have a score of 0.
    // ///
    // /// # Returns
    // ///   A new vectorised graph containing the updated selection
    // pub fn append_graph_documents(&self) -> Self {
    //     let graph_documents = self.graph_documents.iter().map(|doc| (doc.clone(), 0.0));
    //     Self {
    //         selected_docs: extend_selection(
    //             self.selected_docs.clone(),
    //             graph_documents,
    //             usize::MAX,
    //         ),
    //         ..self.clone()
    //     }
    // }

    // /// Add the top `limit` documents to the current selection using `query`
    // ///
    // /// # Arguments
    // ///   * query - the text or the embedding to score against
    // ///   * limit - the maximum number of new documents to add
    // ///   * window - the window where documents need to belong to in order to be considered
    // ///
    // /// # Returns
    // ///   A new vectorised graph containing the updated selection
    // pub fn append_by_similarity(
    //     &self,
    //     query: &Embedding,
    //     limit: usize,
    //     window: Option<(i64, i64)>,
    // ) -> Self {
    //     let joined = chain!(self.node_documents.iter(), self.edge_documents.iter());
    //     self.add_top_documents(joined, query, limit, window)
    // }

    // /// Add the top `limit` node documents to the current selection using `query`
    // ///
    // /// # Arguments
    // ///   * query - the text or the embedding to score against
    // ///   * limit - the maximum number of new documents to add
    // ///   * window - the window where documents need to belong to in order to be considered
    // ///
    // /// # Returns
    // ///   A new vectorised graph containing the updated selection
    // pub fn append_nodes_by_similarity(
    //     &self,
    //     query: &Embedding,
    //     limit: usize,
    //     window: Option<(i64, i64)>,
    // ) -> Self {
    //     self.add_top_documents(self.node_documents.as_ref(), query, limit, window)
    // }

    // /// Add the top `limit` edge documents to the current selection using `query`
    // ///
    // /// # Arguments
    // ///   * query - the text or the embedding to score against
    // ///   * limit - the maximum number of new documents to add
    // ///   * window - the window where documents need to belong to in order to be considered
    // ///
    // /// # Returns
    // ///   A new vectorised graph containing the updated selection
    // pub fn append_edges_by_similarity(
    //     &self,
    //     query: &Embedding,
    //     limit: usize,
    //     window: Option<(i64, i64)>,
    // ) -> Self {
    //     self.add_top_documents(self.edge_documents.as_ref(), query, limit, window)
    // }

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

    fn expand_with_window<W: StaticGraphViewOps>(
        &mut self,
        hops: usize,
        window: Option<(i64, i64)>,
        windowed_graph: &W,
    ) {
        for _ in 0..hops {
            let context = self
                .selected_docs
                .iter()
                .flat_map(|(doc, _)| self.get_context(doc, windowed_graph, window))
                .map(|doc| (doc.clone(), 0.0));
            self.selected_docs = extend_selection(self.selected_docs.clone(), context, usize::MAX);
        }
    }

    /// Add the top `limit` adjacent documents with higher score for `query` to the selection
    ///
    /// The expansion algorithm is a loop with two steps on each iteration:
    ///   1. All the documents 1 hop away of some of the documents included on the selection (and
    /// not already selected) are marked as candidates.
    ///  2. Those candidates are added to the selection in descending order according to the
    /// similarity score obtained against the `query`.
    ///
    /// This loops goes on until the current selection reaches a total of `limit`  documents or
    /// until no more documents are available
    ///
    /// # Arguments
    ///   * query - the text or the embedding to score against
    ///   * window - the window where documents need to belong to in order to be considered
    pub fn expand_documents_by_similarity(
        &mut self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
    ) {
        self.expand_documents_by_similarity_with_path(query, limit, window, ExpansionPath::Both)
    }

    pub fn expand_entities_by_similarity(
        &mut self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
    ) {
        self.expand_entities_by_similarity_with_path(query, limit, window, ExpansionPath::Both)
    }

    /// Add the top `limit` adjacent node documents with higher score for `query` to the selection
    ///
    /// This function has the same behavior as expand_by_similarity but it only considers nodes.
    ///
    /// # Arguments
    ///   * query - the text or the embedding to score against
    ///   * limit - the maximum number of new documents to add
    ///   * window - the window where documents need to belong to in order to be considered
    pub fn expand_nodes_by_similarity(
        &mut self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
    ) {
        self.expand_entities_by_similarity_with_path(query, limit, window, ExpansionPath::Nodes)
    }

    /// Add the top `limit` adjacent edge documents with higher score for `query` to the selection
    ///
    /// This function has the same behavior as expand_by_similarity but it only considers edges.
    ///
    /// # Arguments
    ///   * query - the text or the embedding to score against
    ///   * limit - the maximum number of new documents to add
    ///   * window - the window where documents need to belong to in order to be considered
    pub fn expand_edges_by_similarity(
        &mut self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
    ) {
        self.expand_entities_by_similarity_with_path(query, limit, window, ExpansionPath::Edges)
    }

    fn expand_documents_by_similarity_with_path(
        &mut self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
        path: ExpansionPath,
    ) {
        match window {
            None => self.expand_documents_by_similarity_with_path_and_window(
                query,
                limit,
                window,
                &self.graph.source_graph.clone(),
                path,
            ),
            Some((start, end)) => {
                let windowed_graph = self.graph.source_graph.window(start, end);
                self.expand_documents_by_similarity_with_path_and_window(
                    query,
                    limit,
                    window,
                    &windowed_graph,
                    path,
                )
            }
        }
    }

    /// this function only exists so that we can make the type of graph generic
    fn expand_documents_by_similarity_with_path_and_window<W: StaticGraphViewOps>(
        &mut self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
        windowed_graph: &W,
        path: ExpansionPath,
    ) {
        // let mut selected_docs = self.selected_docs.clone();
        let total_limit = self.selected_docs.len() + limit;

        while self.selected_docs.len() < total_limit {
            let remaining = total_limit - self.selected_docs.len();
            let candidates = self
                .selected_docs
                .iter()
                .flat_map(|(doc, _)| self.get_context(doc, windowed_graph, window))
                .flat_map(|doc| match (path, doc.entity_id.clone()) {
                    // this is to hope from node->node or edge->edge
                    (ExpansionPath::Nodes, EntityId::Edge { .. })
                    | (ExpansionPath::Edges, EntityId::Node { .. }) => {
                        self.get_context(doc, windowed_graph, window)
                    }
                    _ => Box::new(std::iter::once(doc)),
                })
                .filter(|doc| match path {
                    ExpansionPath::Both => true,
                    ExpansionPath::Nodes => doc.entity_id.is_node(),
                    ExpansionPath::Edges => doc.entity_id.is_edge(),
                });

            let scored_candidates = score_documents(query, candidates.cloned());
            let top_sorted_candidates = find_top_k(scored_candidates, usize::MAX);
            self.selected_docs = extend_selection(
                self.selected_docs.clone(),
                top_sorted_candidates,
                total_limit,
            );

            let new_remaining = total_limit - self.selected_docs.len();
            if new_remaining == remaining {
                break; // TODO: try to move this to the top condition
            }
        }
    }

    fn expand_entities_by_similarity_with_path(
        &mut self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
        path: ExpansionPath,
    ) {
        match window {
            None => self.expand_entities_by_similarity_with_path_and_window(
                query,
                limit,
                window,
                &self.graph.source_graph.clone(),
                path,
            ),
            Some((start, end)) => {
                let windowed_graph = self.graph.source_graph.window(start, end);
                self.expand_entities_by_similarity_with_path_and_window(
                    query,
                    limit,
                    window,
                    &windowed_graph,
                    path,
                )
            }
        }
    }

    /// this function only exists so that we can make the type of graph generic
    fn expand_entities_by_similarity_with_path_and_window<W: StaticGraphViewOps>(
        &mut self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
        windowed_graph: &W,
        path: ExpansionPath,
    ) {
        let total_entity_limit = self.get_selected_entity_len() + limit;

        while self.selected_docs.len() < total_entity_limit {
            let remaining = total_entity_limit - self.get_selected_entity_len();

            let candidates: Box<dyn Iterator<Item = (EntityId, Vec<DocumentRef>)>> = match path {
                ExpansionPath::Both => {
                    let node_doc_groups = self.selected_docs.iter().flat_map(|(doc, _)| {
                        self.get_nodes_in_context(doc, windowed_graph, window.clone())
                    });
                    let edge_doc_groups = self.selected_docs.iter().flat_map(|(doc, _)| {
                        self.get_edges_in_context(doc, windowed_graph, window)
                    });

                    Box::new(chain!(node_doc_groups, edge_doc_groups))
                }
                ExpansionPath::Nodes => {
                    let groups = self.selected_docs.iter().flat_map(|(doc, _)| {
                        self.get_nodes_in_context(doc, windowed_graph, window)
                    });
                    Box::new(groups)
                }
                ExpansionPath::Edges => {
                    let groups = self.selected_docs.iter().flat_map(|(doc, _)| {
                        self.get_edges_in_context(doc, windowed_graph, window)
                    });
                    Box::new(groups)
                }
            };

            let scored_candidates = score_document_groups_by_highest(query, candidates);

            let top_sorted_candidates = find_top_k(scored_candidates, usize::MAX);
            self.selected_docs =
                self.extend_selection_with_groups(top_sorted_candidates, total_entity_limit);

            // TODO: this is wrong in this case
            let new_remaining = total_entity_limit - self.get_selected_entity_len();
            if new_remaining == remaining {
                break; // TODO: try to move this to the top condition
            }
        }
    }

    fn get_selected_entities(&self) -> HashSet<EntityId> {
        HashSet::from_iter(self.selected_docs.iter().map(|doc| doc.0.entity_id.clone()))
    }

    fn get_selected_entity_len(&self) -> usize {
        self.get_selected_entities().len()
    }

    // this might return the document used as input, uniqueness need to be check outside of this
    fn get_context<'a, W: StaticGraphViewOps>(
        &'a self,
        document: &DocumentRef,
        windowed_graph: &'a W,
        window: Option<(i64, i64)>,
    ) -> Box<dyn Iterator<Item = &DocumentRef> + '_> {
        match &document.entity_id {
            EntityId::Graph { .. } => Box::new(std::iter::empty()),
            EntityId::Node { id } => {
                let self_docs = self
                    .graph
                    .node_documents
                    .get(&document.entity_id)
                    .unwrap_or(&self.graph.empty_vec);
                match windowed_graph.node(id) {
                    None => Box::new(std::iter::empty()),
                    Some(node) => {
                        let edges = node.edges();
                        let edge_docs = edges.iter().flat_map(|edge| {
                            let edge_id = EntityId::from_edge(&edge);
                            self.graph
                                .edge_documents
                                .get(&edge_id)
                                .unwrap_or(&self.graph.empty_vec)
                        });
                        Box::new(
                            chain!(self_docs, edge_docs).filter(move |doc| {
                                doc.exists_on_window(Some(windowed_graph), &window)
                            }),
                        )
                    }
                }
            }
            EntityId::Edge { src, dst } => {
                let self_docs = self
                    .graph
                    .edge_documents
                    .get(&document.entity_id)
                    .unwrap_or(&self.graph.empty_vec);
                match windowed_graph.edge(src, dst) {
                    None => Box::new(std::iter::empty()),
                    Some(edge) => {
                        let src_id = EntityId::from_node(&edge.src());
                        let dst_id = EntityId::from_node(&edge.dst());
                        let src_docs = self
                            .graph
                            .node_documents
                            .get(&src_id)
                            .unwrap_or(&self.graph.empty_vec);
                        let dst_docs = self
                            .graph
                            .node_documents
                            .get(&dst_id)
                            .unwrap_or(&self.graph.empty_vec);
                        Box::new(
                            chain!(self_docs, src_docs, dst_docs).filter(move |doc| {
                                doc.exists_on_window(Some(windowed_graph), &window)
                            }),
                        )
                    }
                }
            }
        }
    }

    // // this might return the document used as input, uniqueness need to be check outside of this
    // fn get_entity_context<'a, W: StaticGraphViewOps>(
    //     &'a self,
    //     document: &DocumentRef,
    //     windowed_graph: &'a W,
    //     window: Option<(i64, i64)>,
    // ) -> Box<dyn Iterator<Item = Vec<&'a DocumentRef>> + '_> {
    //     // TODO: try to turn the vec into an iter...?
    //     match &document.entity_id {
    //         EntityId::Graph { .. } => Box::new(std::iter::empty()),
    //         EntityId::Node { id } => {
    //             // let self_docs = self // TODO: bring back maybe?
    //             //     .graph
    //             //     .node_documents
    //             //     .get(&document.entity_id)
    //             //     .unwrap_or(&self.graph.empty_vec);
    //             match windowed_graph.node(id) {
    //                 None => Box::new(std::iter::empty()),
    //                 Some(node) => {
    //                     let edges = node.edges();
    //                     let edge_doc_groups = edges.iter().map(move |edge| {
    //                         let edge_id = EntityId::from_edge(&edge);
    //                         let edge_docs = self
    //                             .graph
    //                             .edge_documents
    //                             .get(&edge_id)
    //                             .unwrap_or(&self.graph.empty_vec);

    //                         let filtered_docs = edge_docs
    //                             .iter()
    //                             .filter(|doc| doc.exists_on_window(Some(windowed_graph), window));
    //                         filtered_docs.collect_vec()
    //                     });
    //                     Box::new(edge_doc_groups.filter(|group| group.len() > 0))
    //                 }
    //             }
    //         }
    //         EntityId::Edge { src, dst } => {
    //             // let self_docs = self
    //             //     .graph
    //             //     .edge_documents
    //             //     .get(&document.entity_id)
    //             //     .unwrap_or(&self.graph.empty_vec);
    //             match windowed_graph.edge(src, dst) {
    //                 None => Box::new(std::iter::empty()),
    //                 Some(edge) => {
    //                     // FIXME: some boilerplate across this and the Node pattern match
    //                     let src_id = EntityId::from_node(&edge.src());
    //                     let src_docs = self
    //                         .graph
    //                         .node_documents
    //                         .get(&src_id)
    //                         .unwrap_or(&self.graph.empty_vec);
    //                     let filtered_src_docs = src_docs
    //                         .iter()
    //                         .filter(|doc| doc.exists_on_window(Some(windowed_graph), window))
    //                         .collect_vec();

    //                     let dst_id = EntityId::from_node(&edge.dst());
    //                     let dst_docs = self
    //                         .graph
    //                         .node_documents
    //                         .get(&dst_id)
    //                         .unwrap_or(&self.graph.empty_vec);
    //                     let filtered_dst_docs = dst_docs
    //                         .iter()
    //                         .filter(|doc| doc.exists_on_window(Some(windowed_graph), window))
    //                         .collect_vec();

    //                     Box::new(
    //                         [filtered_src_docs, filtered_dst_docs]
    //                             .into_iter()
    //                             .filter(|group| group.len() > 0),
    //                     )
    //                 }
    //             }
    //         }
    //     }
    // }

    fn nodes_into_document_groups<'a, W: StaticGraphViewOps>(
        &'a self,
        nodes: impl Iterator<Item = NodeView<W>> + 'static,
        windowed_graph: &'a W,
        window: Option<(i64, i64)>,
    ) -> Box<dyn Iterator<Item = (EntityId, Vec<DocumentRef>)> + '_> {
        let groups = nodes
            .map(move |node| {
                let entity_id = EntityId::from_node(&node);
                self.graph.node_documents.get(&entity_id).map(|group| {
                    let docs = group
                        .iter()
                        .filter(|doc| doc.exists_on_window(Some(windowed_graph), &window))
                        .cloned()
                        .collect_vec();
                    (entity_id, docs)
                })
            })
            .flatten()
            .filter(|(_, docs)| docs.len() > 0);
        Box::new(groups)
    }

    fn edges_into_document_groups<'a, W: StaticGraphViewOps>(
        &'a self,
        edges: impl Iterator<Item = EdgeView<W>> + 'a,
        windowed_graph: &'a W,
        window: Option<(i64, i64)>,
    ) -> Box<dyn Iterator<Item = (EntityId, Vec<DocumentRef>)> + '_> {
        let groups = edges
            .map(move |edge| {
                let entity_id = EntityId::from_edge(&edge);
                self.graph.edge_documents.get(&entity_id).map(|group| {
                    let docs = group
                        .iter()
                        .filter(|doc| doc.exists_on_window(Some(windowed_graph), &window))
                        .cloned()
                        .collect_vec();
                    (entity_id, docs)
                })
            })
            .flatten()
            .filter(|(_, docs)| docs.len() > 0);
        Box::new(groups)
    }

    fn get_nodes_in_context<'a, W: StaticGraphViewOps>(
        &'a self,
        document: &'a DocumentRef,
        windowed_graph: &'a W,
        window: Option<(i64, i64)>,
    ) -> Box<dyn Iterator<Item = (EntityId, Vec<DocumentRef>)> + '_> {
        match &document.entity_id {
            EntityId::Graph { .. } => Box::new(std::iter::empty()),
            EntityId::Node { id } => match windowed_graph.node(id) {
                None => Box::new(std::iter::empty()),
                Some(node) => {
                    let nodes = node.neighbours().iter(); // TODO: make nodes_into_document_groups more flexible
                    self.nodes_into_document_groups(nodes, windowed_graph, window)
                }
            },
            EntityId::Edge { src, dst } => match windowed_graph.edge(src, dst) {
                None => Box::new(std::iter::empty()),
                Some(edge) => {
                    let nodes = [edge.src(), edge.dst()].into_iter();
                    self.nodes_into_document_groups(nodes, windowed_graph, window)
                }
            },
        }
    }

    fn get_edges_in_context<'a, W: StaticGraphViewOps>(
        &'a self,
        document: &DocumentRef,
        windowed_graph: &'a W,
        window: Option<(i64, i64)>,
    ) -> Box<dyn Iterator<Item = (EntityId, Vec<DocumentRef>)> + '_> {
        match &document.entity_id {
            EntityId::Graph { .. } => Box::new(std::iter::empty()),
            EntityId::Node { id } => match windowed_graph.node(id) {
                None => Box::new(std::iter::empty()),
                Some(node) => {
                    let edges = node.edges().iter();
                    self.edges_into_document_groups(edges, windowed_graph, window)
                }
            },
            EntityId::Edge { src, dst } => match windowed_graph.edge(src, dst) {
                None => Box::new(std::iter::empty()),
                Some(edge) => {
                    let src_edges = edge.src().edges().iter();
                    let dst_edges = edge.dst().edges().iter();
                    let edges = chain!(src_edges, dst_edges);
                    self.edges_into_document_groups(edges, windowed_graph, window)
                }
            },
        }
    }

    /// this is a wrapper around `extend_selection` for adding in full entities
    fn extend_selection_with_groups<'a, I>(
        &self,
        extension: I,
        total_entity_limit: usize,
    ) -> Vec<(DocumentRef, f32)>
    where
        I: IntoIterator<Item = ((EntityId, Vec<DocumentRef>), f32)>,
    {
        let entity_set = self.get_selected_entities();
        let entity_extension_size = total_entity_limit - self.get_selected_entity_len();
        let new_unique_entities = extension
            .into_iter()
            .unique_by(|((entity_id, _), _score)| entity_id.clone())
            .filter(|((entity_id, _), _score)| !entity_set.contains(entity_id))
            .take(entity_extension_size);
        let documents_to_add = new_unique_entities
            .flat_map(|((_, docs), score)| docs.into_iter().map(move |doc| (doc.clone(), score)));
        extend_selection(self.selected_docs.clone(), documents_to_add, usize::MAX)
    }
}

/// this function assumes that extension might contain duplicates and might contain elements
/// already present in selection, and returns a sequence with no repetitions and preserving the
/// elements in selection in the same indexes
fn extend_selection<I>(
    selection: Vec<(DocumentRef, f32)>,
    extension: I,
    new_total_size: usize,
) -> Vec<(DocumentRef, f32)>
where
    I: IntoIterator<Item = (DocumentRef, f32)>,
{
    let selection_set: HashSet<DocumentRef> =
        HashSet::from_iter(selection.iter().map(|(doc, _)| doc.clone()));
    let new_docs = extension
        .into_iter()
        .unique_by(|(doc, _)| doc.clone())
        .filter(|(doc, _)| !selection_set.contains(doc));
    selection
        .into_iter()
        .chain(new_docs)
        .take(new_total_size)
        .collect_vec()
}
