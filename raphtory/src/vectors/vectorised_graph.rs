use crate::{
    core::entities::nodes::node_ref::NodeRef,
    db::{
        api::view::{DynamicGraph, StaticGraphViewOps},
        graph::{edge::EdgeView, node::NodeView},
    },
    prelude::*,
    vectors::{
        document_ref::DocumentRef, document_template::DocumentTemplate,
        embedding_cache::EmbeddingCache, entity_id::EntityId,
        graph_embeddings::StoredVectorisedGraph, Document, DocumentOps, Embedding,
        EmbeddingFunction,
    },
};
use itertools::{chain, Itertools};
use std::{
    collections::{HashMap, HashSet},
    ops::Deref,
    path::{Path, PathBuf},
    sync::Arc,
};

#[derive(Clone, Copy)]
enum ExpansionPath {
    Nodes,
    Edges,
    Both,
}

pub struct VectorisedGraph<G: StaticGraphViewOps, T: DocumentTemplate<G>> {
    pub(crate) source_graph: G,
    pub(crate) template: Arc<T>,
    pub(crate) embedding: Arc<dyn EmbeddingFunction>,
    // it is not the end of the world but we are storing the entity id twice
    pub(crate) graph_documents: Arc<Vec<DocumentRef>>,
    pub(crate) node_documents: Arc<HashMap<EntityId, Vec<DocumentRef>>>, // TODO: replace with FxHashMap
    pub(crate) edge_documents: Arc<HashMap<EntityId, Vec<DocumentRef>>>,
    selected_docs: Vec<(DocumentRef, f32)>,
    empty_vec: Vec<DocumentRef>,
}

// This has to be here so it is shared between python and graphql
pub type DynamicTemplate = Arc<dyn DocumentTemplate<DynamicGraph> + 'static>;
pub type DynamicVectorisedGraph = VectorisedGraph<DynamicGraph, DynamicTemplate>;

impl<G: StaticGraphViewOps, T: DocumentTemplate<G>> Clone for VectorisedGraph<G, T> {
    fn clone(&self) -> Self {
        Self::new(
            self.source_graph.clone(),
            self.template.clone(),
            self.embedding.clone(),
            self.graph_documents.clone(),
            self.node_documents.clone(),
            self.edge_documents.clone(),
            self.selected_docs.clone(),
        )
    }
}

impl<G: StaticGraphViewOps, T: DocumentTemplate<G>> VectorisedGraph<G, T> {
    pub(crate) fn new(
        graph: G,
        template: Arc<T>,
        embedding: Arc<dyn EmbeddingFunction>,
        graph_documents: Arc<Vec<DocumentRef>>,
        node_documents: Arc<HashMap<EntityId, Vec<DocumentRef>>>,
        edge_documents: Arc<HashMap<EntityId, Vec<DocumentRef>>>,
        selected_docs: Vec<(DocumentRef, f32)>,
    ) -> Self {
        Self {
            source_graph: graph,
            template,
            embedding,
            graph_documents,
            node_documents,
            edge_documents,
            selected_docs,
            empty_vec: vec![],
        }
    }

    fn load_from_embedding_store(file: &Path, graph: G) -> Option<Self> {
        let store = StoredVectorisedGraph::load_from_path(file)?;

        Some(Self {
            source_graph: graph,
            template,
            embedding,
            graph_documents: Arc::new(store.graph_documents),
            node_documents: Arc::new(store.node_documents),
            edge_documents: Arc::new(store.edge_documents),
            selected_docs: vec![],
            empty_vec: vec![],
        })
    }

    fn save_embedding_store(&self, file: &Path) {
        let name = self
            .source_graph
            .properties()
            .get("name")
            .map(|prop| prop.to_string())
            .unwrap_or("".to_owned());
        let store = StoredVectorisedGraph {
            name,
            graph_documents: self.graph_documents.deref().clone(),
            node_documents: self.node_documents.deref().clone(),
            edge_documents: self.edge_documents.deref().clone(),
        };
        store.save_to_path(file);
    }

    // TODO: rename so that is is clear that if refers to the cache
    /// Save the embeddings present in this graph to `file` so they can be further used in a call to `vectorise`
    pub fn save_embeddings(&self, file: PathBuf) {
        let cache = EmbeddingCache::new(file);
        chain!(self.node_documents.iter(), self.edge_documents.iter()).for_each(|(_, group)| {
            group.iter().for_each(|doc| {
                let original = doc.regenerate(&self.source_graph, self.template.as_ref());
                cache.upsert_embedding(original.content(), doc.embedding.clone());
            })
        });
        cache.dump_to_disk();
    }

    /// Return the nodes present in the current selection
    pub fn nodes(&self) -> Vec<NodeView<G>> {
        self.selected_docs
            .iter()
            .filter_map(|(doc, _)| match doc.entity_id {
                EntityId::Node { id } => self.source_graph.node(id),
                _ => None,
            })
            .collect_vec()
    }

    /// Return the edges present in the current selection
    pub fn edges(&self) -> Vec<EdgeView<G>> {
        self.selected_docs
            .iter()
            .filter_map(|(doc, _)| match doc.entity_id {
                EntityId::Edge { src, dst } => self.source_graph.edge(src, dst),
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
                    doc.regenerate(&self.source_graph, self.template.as_ref()),
                    *score,
                )
            })
            .collect_vec()
    }

    /// Add all the documents from `nodes` and `edges` to the current selection
    ///
    /// Documents added by this call are assumed to have a score of 0.
    ///
    /// # Arguments
    ///   * nodes - a list of the node ids or nodes to add
    ///   * edges - a list of the edge ids or edges to add
    ///
    /// # Returns
    ///   A new vectorised graph containing the updated selection
    pub fn append<V: Into<NodeRef>>(&self, nodes: Vec<V>, edges: Vec<(V, V)>) -> Self {
        let node_docs = nodes.into_iter().flat_map(|id| {
            let node = self.source_graph.node(id);
            let opt = node.map(|node| self.node_documents.get(&EntityId::from_node(&node)));
            opt.flatten().unwrap_or(&self.empty_vec)
        });
        let edge_docs = edges.into_iter().flat_map(|(src, dst)| {
            let edge = self.source_graph.edge(src, dst);
            let opt = edge.map(|edge| self.edge_documents.get(&EntityId::from_edge(&edge)));
            opt.flatten().unwrap_or(&self.empty_vec)
        });
        let new_selected = chain!(node_docs, edge_docs).map(|doc| (doc.clone(), 0.0));
        Self {
            selected_docs: extend_selection(self.selected_docs.clone(), new_selected, usize::MAX),
            ..self.clone()
        }
    }

    /// Add all the documents from `nodes` and `edges` to the current selection
    ///
    /// Documents added by this call are assumed to have a score of 0.
    ///
    /// # Arguments
    ///   * nodes - a list of the node ids or nodes to add
    ///   * edges - a list of the edge ids or edges to add
    ///
    /// # Returns
    ///   A new vectorised graph containing the updated selection
    pub fn append_graph_documents(&self) -> Self {
        let graph_documents = self.graph_documents.iter().map(|doc| (doc.clone(), 0.0));
        Self {
            selected_docs: extend_selection(
                self.selected_docs.clone(),
                graph_documents,
                usize::MAX,
            ),
            ..self.clone()
        }
    }

    /// Add the top `limit` documents to the current selection using `query`
    ///
    /// # Arguments
    ///   * query - the text or the embedding to score against
    ///   * limit - the maximum number of new documents to add
    ///   * window - the window where documents need to belong to in order to be considered
    ///
    /// # Returns
    ///   A new vectorised graph containing the updated selection
    pub fn append_by_similarity(
        &self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
    ) -> Self {
        let joined = chain!(self.node_documents.iter(), self.edge_documents.iter());
        self.add_top_documents(joined, query, limit, window)
    }

    /// Add the top `limit` node documents to the current selection using `query`
    ///
    /// # Arguments
    ///   * query - the text or the embedding to score against
    ///   * limit - the maximum number of new documents to add
    ///   * window - the window where documents need to belong to in order to be considered
    ///
    /// # Returns
    ///   A new vectorised graph containing the updated selection
    pub fn append_nodes_by_similarity(
        &self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
    ) -> Self {
        self.add_top_documents(self.node_documents.as_ref(), query, limit, window)
    }

    /// Add the top `limit` edge documents to the current selection using `query`
    ///
    /// # Arguments
    ///   * query - the text or the embedding to score against
    ///   * limit - the maximum number of new documents to add
    ///   * window - the window where documents need to belong to in order to be considered
    ///
    /// # Returns
    ///   A new vectorised graph containing the updated selection
    pub fn append_edges_by_similarity(
        &self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
    ) -> Self {
        self.add_top_documents(self.edge_documents.as_ref(), query, limit, window)
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
    ///
    /// # Returns
    ///   A new vectorised graph containing the updated selection
    pub fn expand(&self, hops: usize, window: Option<(i64, i64)>) -> Self {
        match window {
            None => self.expand_with_window(hops, window, &self.source_graph),
            Some((start, end)) => {
                let windowed_graph = self.source_graph.window(start, end);
                self.expand_with_window(hops, window, &windowed_graph)
            }
        }
    }

    fn expand_with_window<W: StaticGraphViewOps>(
        &self,
        hops: usize,
        window: Option<(i64, i64)>,
        windowed_graph: &W,
    ) -> Self {
        let mut selected_docs = self.selected_docs.clone();
        for _ in 0..hops {
            let context = selected_docs
                .iter()
                .flat_map(|(doc, _)| self.get_context(doc, windowed_graph, window))
                .map(|doc| (doc.clone(), 0.0));
            selected_docs = extend_selection(selected_docs.clone(), context, usize::MAX);
        }

        Self {
            selected_docs,
            ..self.clone()
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
    ///
    /// # Returns
    ///   A new vectorised graph containing the updated selection
    pub fn expand_by_similarity(
        &self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
    ) -> Self {
        self.expand_by_similarity_with_path(query, limit, window, ExpansionPath::Both)
    }

    /// Add the top `limit` adjacent node documents with higher score for `query` to the selection
    ///
    /// This function has the same behavior as expand_by_similarity but it only considers nodes.
    ///
    /// # Arguments
    ///   * query - the text or the embedding to score against
    ///   * limit - the maximum number of new documents to add  
    ///   * window - the window where documents need to belong to in order to be considered
    ///
    /// # Returns
    ///   A new vectorised graph containing the updated selection
    pub fn expand_nodes_by_similarity(
        &self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
    ) -> Self {
        self.expand_by_similarity_with_path(query, limit, window, ExpansionPath::Nodes)
    }

    /// Add the top `limit` adjacent edge documents with higher score for `query` to the selection
    ///
    /// This function has the same behavior as expand_by_similarity but it only considers edges.
    ///
    /// # Arguments
    ///   * query - the text or the embedding to score against
    ///   * limit - the maximum number of new documents to add
    ///   * window - the window where documents need to belong to in order to be considered
    ///
    /// # Returns
    ///   A new vectorised graph containing the updated selection
    pub fn expand_edges_by_similarity(
        &self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
    ) -> Self {
        self.expand_by_similarity_with_path(query, limit, window, ExpansionPath::Edges)
    }

    fn expand_by_similarity_with_path(
        &self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
        path: ExpansionPath,
    ) -> Self {
        match window {
            None => self.expand_by_similarity_with_path_and_window(
                query,
                limit,
                window,
                &self.source_graph,
                path,
            ),
            Some((start, end)) => {
                let windowed_graph = self.source_graph.window(start, end);
                self.expand_by_similarity_with_path_and_window(
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
    fn expand_by_similarity_with_path_and_window<W: StaticGraphViewOps>(
        &self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
        windowed_graph: &W,
        path: ExpansionPath,
    ) -> Self {
        let mut selected_docs = self.selected_docs.clone();
        let total_limit = selected_docs.len() + limit;

        while selected_docs.len() < total_limit {
            let remaining = total_limit - selected_docs.len();
            let candidates = selected_docs
                .iter()
                .flat_map(|(doc, _)| self.get_context(doc, windowed_graph, window))
                .flat_map(|doc| match (path, doc.entity_id.clone()) {
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
            selected_docs =
                extend_selection(selected_docs.clone(), top_sorted_candidates, total_limit);

            let new_remaining = total_limit - selected_docs.len();
            if new_remaining == remaining {
                break; // TODO: try to move this to the top condition
            }
        }

        Self {
            selected_docs,
            ..self.clone()
        }
    }

    fn add_top_documents<'a, I>(
        &self,
        document_groups: I,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
    ) -> Self
    where
        I: IntoIterator<Item = (&'a EntityId, &'a Vec<DocumentRef>)> + 'a,
    {
        let documents = document_groups
            .into_iter()
            .flat_map(|(_, embeddings)| embeddings);

        let window_docs: Box<dyn Iterator<Item = &DocumentRef>> = match window {
            None => Box::new(documents),
            Some((start, end)) => {
                let windowed_graph = self.source_graph.window(start, end);
                let filtered = documents
                    .filter(move |document| document.exists_on_window(&windowed_graph, window));
                Box::new(filtered)
            }
        };

        let new_len = self.selected_docs.len() + limit;
        let scored_nodes = score_documents(query, window_docs.cloned()); // TODO: try to remove this clone
        let candidates = find_top_k(scored_nodes, usize::MAX);
        let new_selected = extend_selection(self.selected_docs.clone(), candidates, new_len);

        Self {
            selected_docs: new_selected,
            ..self.clone()
        }
    }

    // this might return the document used as input, uniqueness need to be check outside of this
    fn get_context<'a, W: StaticGraphViewOps>(
        &'a self,
        document: &DocumentRef,
        windowed_graph: &'a W,
        window: Option<(i64, i64)>,
    ) -> Box<dyn Iterator<Item = &DocumentRef> + '_> {
        match document.entity_id {
            EntityId::Graph { .. } => Box::new(std::iter::empty()),
            EntityId::Node { id } => {
                let self_docs = self.node_documents.get(&document.entity_id).unwrap();
                match windowed_graph.node(id) {
                    None => Box::new(std::iter::empty()),
                    Some(node) => {
                        let edges = node.edges();
                        let edge_docs = edges.iter().flat_map(|edge| {
                            let edge_id = EntityId::from_edge(&edge);
                            self.edge_documents.get(&edge_id).unwrap_or(&self.empty_vec)
                        });
                        Box::new(
                            chain!(self_docs, edge_docs)
                                .filter(move |doc| doc.exists_on_window(windowed_graph, window)),
                        )
                    }
                }
            }
            EntityId::Edge { src, dst } => {
                let self_docs = self.edge_documents.get(&document.entity_id).unwrap();
                match windowed_graph.edge(src, dst) {
                    None => Box::new(std::iter::empty()),
                    Some(edge) => {
                        let src_id = EntityId::from_node(&edge.src());
                        let dst_id = EntityId::from_node(&edge.dst());
                        let src_docs = self.node_documents.get(&src_id).unwrap();
                        let dst_docs = self.node_documents.get(&dst_id).unwrap();
                        Box::new(
                            chain!(self_docs, src_docs, dst_docs)
                                .filter(move |doc| doc.exists_on_window(windowed_graph, window)),
                        )
                    }
                }
            }
        }
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

// TODO: move this and find_top_k to common place
pub(crate) fn score_documents<'a, I>(
    query: &'a Embedding,
    documents: I,
) -> impl Iterator<Item = (DocumentRef, f32)> + 'a
where
    I: IntoIterator<Item = DocumentRef> + 'a,
{
    documents.into_iter().map(|doc| {
        let score = cosine(query, &doc.embedding);
        (doc, score)
    })
}

/// Returns the top k nodes in descending order
pub(crate) fn find_top_k<'a, I>(
    elements: I,
    k: usize,
) -> impl Iterator<Item = (DocumentRef, f32)> + 'a
where
    I: Iterator<Item = (DocumentRef, f32)> + 'a,
{
    // TODO: add optimization for when this is used -> don't maintain more candidates than the max number of documents to return !!!
    elements
        .sorted_by(|(_, score1), (_, score2)| score1.partial_cmp(&score2).unwrap().reverse())
        // We use reverse because default sorting is ascending but we want it descending
        .take(k)
}

fn cosine(vector1: &Embedding, vector2: &Embedding) -> f32 {
    assert_eq!(vector1.len(), vector2.len());

    let dot_product: f32 = vector1.iter().zip(vector2.iter()).map(|(x, y)| x * y).sum();
    let x_length: f32 = vector1.iter().map(|x| x * x).sum();
    let y_length: f32 = vector2.iter().map(|y| y * y).sum();
    // TODO: store the length of the vector as well so we don't need to recompute it
    // Vectors are already normalized for ada but nor for all the models:
    // see: https://platform.openai.com/docs/guides/embeddings/which-distance-function-should-i-use

    let normalized = dot_product / (x_length.sqrt() * y_length.sqrt());
    // println!("cosine for {vector1:?} and {vector2:?} is {normalized}");
    assert!(normalized <= 1.001);
    assert!(normalized >= -1.001);
    normalized
}
