use crate::{
    core::entities::vertices::vertex_ref::VertexRef,
    db::graph::{edge::EdgeView, vertex::VertexView},
    prelude::{EdgeViewOps, GraphViewOps, TimeOps, VertexViewOps},
    vectors::{
        document_ref::DocumentRef, document_template::DocumentTemplate, entity_id::EntityId,
        Document, Embedding, EmbeddingFunction, ScoredDocument,
    },
};
use itertools::{chain, Itertools};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

pub struct VectorizedGraph<G: GraphViewOps, T: DocumentTemplate<G>> {
    pub(crate) source_graph: G,
    template: Arc<T>,
    pub(crate) embedding: Arc<dyn EmbeddingFunction>,
    // it is not the end of the world but we are storing the entity id twice
    node_documents: Arc<HashMap<EntityId, Vec<DocumentRef>>>, // TODO: replace with FxHashMap
    edge_documents: Arc<HashMap<EntityId, Vec<DocumentRef>>>,
    // selected_docs: Vec<ScoredDocument>,
    selected_docs: Vec<(DocumentRef, f32)>,
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
            self.selected_docs.clone(),
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
        selected_docs: Vec<(DocumentRef, f32)>,
    ) -> Self {
        Self {
            source_graph: graph,
            template,
            embedding,
            node_documents,
            edge_documents,
            selected_docs,
            empty_vec: vec![],
        }
    }

    /// This assumes forced documents to have a score of 0
    pub fn select<V: Into<VertexRef>>(&self, nodes: Vec<V>, edges: Vec<(V, V)>) -> Self {
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
        let new_selected = chain!(node_docs, edge_docs).map(|doc| (doc.clone(), 0.0));
        Self {
            selected_docs: extend_selection(self.selected_docs.clone(), new_selected, usize::MAX),
            ..self.clone()
        }
    }

    pub fn nodes(&self) -> Vec<VertexView<G>> {
        self.selected_docs
            .iter()
            .filter_map(|(doc, _)| match doc.entity_id {
                EntityId::Node { id } => self.source_graph.vertex(id),
                EntityId::Edge { .. } => None,
            })
            .collect_vec()
    }

    pub fn edges(&self) -> Vec<EdgeView<G>> {
        self.selected_docs
            .iter()
            .filter_map(|(doc, _)| match doc.entity_id {
                EntityId::Node { .. } => None,
                EntityId::Edge { src, dst } => self.source_graph.edge(src, dst),
            })
            .collect_vec()
    }

    pub fn get_documents(&self) -> Vec<Document> {
        self.get_documents_with_scores()
            .into_iter()
            .map(|(doc, _)| doc)
            .collect_vec()
    }

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

    pub fn search_similar_entities(
        &self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
    ) -> Self {
        let joined = chain!(self.node_documents.iter(), self.edge_documents.iter());
        self.add_top_documents(joined, query, limit, window)
    }

    pub fn search_similar_nodes(
        &self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
    ) -> Self {
        self.add_top_documents(self.node_documents.as_ref(), query, limit, window)
    }
    pub fn search_similar_edges(
        &self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
    ) -> Self {
        self.add_top_documents(self.edge_documents.as_ref(), query, limit, window)
    }

    /// This assumes forced documents to have a score of 0
    pub fn expand(&self, hops: usize, window: Option<(i64, i64)>) -> Self {
        match window {
            None => self.expand_with_window(hops, window, &self.source_graph),
            Some((start, end)) => {
                let windowed_graph = self.source_graph.window(start, end);
                self.expand_with_window(hops, window, &windowed_graph)
            }
        }
    }

    fn expand_with_window<W: GraphViewOps>(
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

    pub fn expand_with_search(
        &self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
    ) -> Self {
        match window {
            None => self.expand_with_search_and_window(query, limit, window, &self.source_graph),
            Some((start, end)) => {
                let windowed_graph = self.source_graph.window(start, end);
                self.expand_with_search_and_window(query, limit, window, &windowed_graph)
            }
        }
    }

    fn expand_with_search_and_window<W: GraphViewOps>(
        &self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
        windowed_graph: &W,
    ) -> Self {
        let mut selected_docs = self.selected_docs.clone();
        let total_limit = selected_docs.len() + limit;

        println!(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

        while selected_docs.len() < total_limit {
            let remaining = total_limit - selected_docs.len();
            let candidates = selected_docs
                .iter()
                .flat_map(|(doc, _)| self.get_context(doc, windowed_graph, window));

            let scored_candidates = score_documents(query, candidates.cloned());
            let top_sorted_candidates = find_top_k(scored_candidates, remaining);
            selected_docs =
                extend_selection(selected_docs.clone(), top_sorted_candidates, total_limit);

            let new_remaining = total_limit - selected_docs.len();
            dbg!(remaining);
            dbg!(new_remaining);
            dbg!(&selected_docs);
            dbg!(total_limit);
            println!("---------------------------------------------------------");
            if new_remaining == remaining {
                break; // TODO: try to move this to the top condition
            }
        }

        println!(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

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
    fn get_context<'a, W: GraphViewOps>(
        &'a self,
        document: &DocumentRef,
        windowed_graph: &'a W,
        window: Option<(i64, i64)>,
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
                        .filter(move |doc| doc.exists_on_window(windowed_graph, window)),
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
                        .filter(move |doc| doc.exists_on_window(windowed_graph, window)),
                )
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

    let extension = extension.into_iter().collect_vec(); // TODO: remove this
    dbg!(&extension);

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

fn score_documents<'a, I>(
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
fn find_top_k<'a, I>(elements: I, k: usize) -> impl Iterator<Item = (DocumentRef, f32)> + 'a
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
