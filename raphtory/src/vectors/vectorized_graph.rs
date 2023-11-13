use crate::{
    core::{entities::vertices::vertex_ref::VertexRef, utils::time::IntoTime},
    db::graph::{edge::EdgeView, vertex::VertexView, views::window_graph::WindowedGraph},
    prelude::{EdgeViewOps, GraphViewOps, TimeOps, VertexViewOps},
    vectors::{
        document_ref::DocumentRef, document_template::DocumentTemplate, entity_id::EntityId,
        Document, Embedding, EmbeddingFunction,
    },
};
use itertools::{chain, Itertools};
use std::{
    cmp::{max, min},
    collections::HashMap,
    sync::Arc,
};

#[derive(Clone)]
struct ScoredDocument {
    doc: DocumentRef,
    score: f32,
}

impl ScoredDocument {
    fn new(doc: DocumentRef, score: f32) -> Self {
        Self { doc, score }
    }
}

pub struct VectorizedGraphSelection<G: GraphViewOps, T: DocumentTemplate<G>> {
    pub(crate) vectors: VectorizedGraph<G, T>,
    selected_docs: Vec<ScoredDocument>,
}

impl<G: GraphViewOps, T: DocumentTemplate<G>> Clone for VectorizedGraphSelection<G, T> {
    fn clone(&self) -> Self {
        Self {
            vectors: self.vectors.clone(),
            selected_docs: self.selected_docs.clone(),
        }
    }
}

impl<G: GraphViewOps, T: DocumentTemplate<G>> VectorizedGraphSelection<G, T> {
    fn new(vectors: VectorizedGraph<G, T>, selected_docs: Vec<ScoredDocument>) -> Self {
        Self {
            vectors,
            selected_docs,
        }
    }

    pub fn nodes(&self) -> Vec<VertexView<G>> {
        self.selected_docs
            .iter()
            .filter_map(|doc| match doc.doc.entity_id {
                EntityId::Node { id } => self.vectors.source_graph.vertex(id),
                EntityId::Edge { .. } => None,
            })
            .collect_vec()
    }

    pub fn edges(&self) -> Vec<EdgeView<G>> {
        self.selected_docs
            .iter()
            .filter_map(|doc| match doc.doc.entity_id {
                EntityId::Node { .. } => None,
                EntityId::Edge { src, dst } => self.vectors.source_graph.edge(src, dst),
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
            .map(|doc| {
                (
                    doc.doc
                        .regenerate(&self.vectors.source_graph, self.vectors.template.as_ref()),
                    doc.score,
                )
            })
            .collect_vec()
    }

    pub fn add_new_entities(&self, query: &Embedding, limit: usize) -> Self {
        let joined = chain!(
            self.vectors.node_documents.iter(),
            self.vectors.edge_documents.iter()
        );
        self.add_top_documents(joined, query, limit)
    }

    pub fn add_new_nodes(&self, query: &Embedding, limit: usize) -> Self {
        self.add_top_documents(self.vectors.node_documents.as_ref(), query, limit)
    }
    pub fn add_new_edges(&self, query: &Embedding, limit: usize) -> Self {
        self.add_top_documents(self.vectors.edge_documents.as_ref(), query, limit)
    }

    /// This assumes forced documents to have a score of 0
    pub fn expand(&self, hops: usize) -> Self {
        match (self.vectors.window_start, self.vectors.window_end) {
            (None, None) => self.expand_with_window(hops, self.vectors.source_graph.clone()),
            _ => {
                let start = self.get_window_start();
                let end = self.get_window_end();
                let window = self.vectors.source_graph.window(start, end);
                self.expand_with_window(hops, window)
            }
        }
    }

    pub fn expand_with_window<W: GraphViewOps>(&self, hops: usize, window: W) -> Self {
        let mut selected_docs = self.selected_docs.clone();
        for _ in 0..hops {
            let context = selected_docs
                .iter()
                .flat_map(|doc| {
                    self.vectors.get_context(
                        &doc.doc,
                        &window,
                        self.vectors.window_start,
                        self.vectors.window_end,
                    )
                })
                .map(|doc| ScoredDocument::new(doc.clone(), 0.0))
                .collect_vec();
            selected_docs.extend(context);
        }

        Self {
            vectors: self.vectors.clone(),
            selected_docs,
        }
    }

    pub fn expand_with_search(&self, query: &Embedding, limit: usize) -> Self {
        match (self.vectors.window_start, self.vectors.window_end) {
            (None, None) => {
                self.expand_with_search_and_window(query, limit, self.vectors.source_graph.clone())
            }
            _ => {
                let start = self.get_window_start();
                let end = self.get_window_end();
                let window = self.vectors.source_graph.window(start, end);
                self.expand_with_search_and_window(query, limit, window)
            }
        }
    }

    fn expand_with_search_and_window<W: GraphViewOps>(
        &self,
        query: &Embedding,
        limit: usize,
        window: W,
    ) -> Self {
        let mut selected_docs = self.selected_docs.clone();

        while selected_docs.len() < limit {
            let candidates = selected_docs.iter().flat_map(|doc| {
                self.vectors.get_context(
                    &doc.doc,
                    &window,
                    self.vectors.window_start,
                    self.vectors.window_end,
                )
            });

            let unique_candidates = candidates.unique_by(|doc| doc.id());
            let valid_candidates =
                unique_candidates.filter(|&doc| !selected_docs.iter().any(|sel| &sel.doc == doc));
            let scored_candidates = score_documents(&query, valid_candidates.cloned());
            let top_sorted_candidates = find_top_k(scored_candidates, usize::MAX).collect_vec();

            if top_sorted_candidates.is_empty() {
                // TODO: use similarity search again with the whole graph with init + 1 !!
                // TODO: avoid this, put all stop conditions at the top
                break;
            }

            selected_docs.extend(top_sorted_candidates);
        }

        Self {
            vectors: self.vectors.clone(),
            selected_docs,
        }
    }

    fn get_window_start(&self) -> i64 {
        self.vectors.window_start.unwrap_or(i64::MIN)
    }

    fn get_window_end(&self) -> i64 {
        self.vectors.window_end.unwrap_or(i64::MAX)
    }

    fn add_top_documents<'a, I>(&self, document_groups: I, query: &Embedding, limit: usize) -> Self
    where
        I: IntoIterator<Item = (&'a EntityId, &'a Vec<DocumentRef>)> + 'a,
    {
        let start = self.vectors.window_start;
        let end = self.vectors.window_end;
        let documents = document_groups
            .into_iter()
            .flat_map(|(_, embeddings)| embeddings);

        let window_docs: Box<dyn Iterator<Item = &DocumentRef>> = match (start, end) {
            (None, None) => Box::new(documents),
            _ => {
                let start = self.get_window_start();
                let end = self.get_window_end();
                let window = self.vectors.source_graph.window(start, end);
                let filtered = documents.filter(move |document| {
                    document.exists_on_window(&window, Some(start), Some(end))
                });
                Box::new(filtered)
            }
        };

        let new_len = self.selected_docs.len() + limit;
        let mut selected_docs = self.selected_docs.clone();
        let scored_nodes = score_documents(&query, window_docs.cloned()); // TODO: try to remove this clone
        let candidates = find_top_k(scored_nodes, new_len);
        let new_selected = candidates
            .filter(|new_doc| !selected_docs.iter().any(|doc| doc.doc == new_doc.doc))
            .take(limit)
            .collect_vec();
        selected_docs.extend(new_selected);

        Self {
            vectors: self.vectors.clone(),
            selected_docs,
        }
    }
}

pub struct VectorizedGraph<G: GraphViewOps, T: DocumentTemplate<G>> {
    pub(crate) source_graph: G,
    pub(crate) template: Arc<T>,
    pub(crate) embedding: Arc<dyn EmbeddingFunction>,
    // it is not the end of the world but we are storing the entity id twice
    pub(crate) node_documents: Arc<HashMap<EntityId, Vec<DocumentRef>>>, // TODO: replace with FxHashMap
    pub(crate) edge_documents: Arc<HashMap<EntityId, Vec<DocumentRef>>>,
    // pub(crate) windowed_graph: W, // TODO: maybe put all window realted stuff on single struct
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
            // self.windowed_graph.clone(),
            self.window_start.clone(),
            self.window_end.clone(),
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
        // windowed_graph: W,
        window_start: Option<i64>,
        window_end: Option<i64>,
    ) -> Self {
        Self {
            source_graph: graph,
            template,
            embedding,
            node_documents,
            edge_documents,
            // windowed_graph,
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
    fn get_context<'a, W: GraphViewOps>(
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

fn score_documents<'a, I>(
    query: &'a Embedding,
    documents: I,
) -> impl Iterator<Item = ScoredDocument> + 'a
where
    I: IntoIterator<Item = DocumentRef> + 'a,
{
    documents.into_iter().map(|doc| {
        let score = cosine(query, &doc.embedding);
        ScoredDocument { doc, score }
    })
}

/// Returns the top k nodes in descending order
fn find_top_k<'a, I>(elements: I, k: usize) -> impl Iterator<Item = ScoredDocument> + 'a
where
    I: Iterator<Item = ScoredDocument> + 'a,
{
    // TODO: add optimization for when this is used -> don't maintain more candidates than the max number of documents to return !!!
    elements
        .sorted_by(|doc1, doc2| doc1.score.partial_cmp(&doc2.score).unwrap().reverse())
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
    assert!(normalized <= 1.0);
    assert!(normalized >= -1.0);
    normalized
}
