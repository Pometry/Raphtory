use crate::{
    db::{
        api::view::internal::{DynamicGraph, IntoDynamic},
        graph::views::window_graph::WindowedGraph,
    },
    prelude::{EdgeViewOps, GraphViewOps, TimeOps, VertexViewOps},
    vectors::{
        document_ref::DocumentRef, document_template::DocumentTemplate, entity_id::EntityId,
        Document, Embedding, EmbeddingFunction,
    },
};
use itertools::{chain, Itertools};
use std::collections::HashMap;

pub struct VectorizedGraph<G: GraphViewOps, T: DocumentTemplate<G>> {
    graph: G,
    template: T,
    embedding: Box<dyn EmbeddingFunction>,
    // it is not the end of the world but we are storing the entity id twice
    node_documents: HashMap<EntityId, Vec<DocumentRef>>, // TODO: replace with FxHashMap
    edge_documents: HashMap<EntityId, Vec<DocumentRef>>,
}

impl<G: GraphViewOps + IntoDynamic, T: DocumentTemplate<G>> VectorizedGraph<G, T> {
    pub(crate) fn new(
        graph: G,
        template: T,
        embedding: Box<dyn EmbeddingFunction>,
        node_documents: HashMap<EntityId, Vec<DocumentRef>>,
        edge_documents: HashMap<EntityId, Vec<DocumentRef>>,
    ) -> Self {
        Self {
            graph,
            template,
            embedding,
            node_documents,
            edge_documents,
        }
    }

    // FIXME: this should return a Result
    pub async fn similarity_search(
        &self,
        query: &str,
        init: usize,
        min_nodes: usize,
        min_edges: usize,
        limit: usize,
        window_start: Option<i64>,
        window_end: Option<i64>,
    ) -> Vec<Document> {
        let query_embedding = self.embedding.call(vec![query.to_owned()]).await.remove(0);

        let (graph, window_nodes, window_edges): (
            DynamicGraph,
            Box<dyn Iterator<Item = &DocumentRef>>,
            Box<dyn Iterator<Item = &DocumentRef>>,
        ) = match (window_start, window_end) {
            (None, None) => (
                self.graph.clone().into_dynamic(),
                Box::new(
                    self.node_documents
                        .iter()
                        .flat_map(|(_, embeddings)| embeddings),
                ),
                Box::new(
                    self.edge_documents
                        .iter()
                        .flat_map(|(_, embeddings)| embeddings),
                ),
            ),
            (start, end) => {
                let start = start.unwrap_or(i64::MIN);
                let end = end.unwrap_or(i64::MAX);
                let window = self.graph.window(start, end);
                let nodes =
                    self.window_embeddings(&self.node_documents, &window, window_start, window_end);
                let edges =
                    self.window_embeddings(&self.edge_documents, &window, window_start, window_end);
                (
                    window.clone().into_dynamic(),
                    Box::new(nodes),
                    Box::new(edges),
                )
            }
        };

        // TODO: split the remaining code into a different function so that it can handle a graph
        // with generic type G, and therefore we don't need to hold a reference to a DynamicGraph
        // for this to work

        // FIRST STEP: ENTRY POINT SELECTION:
        assert!(
            min_nodes + min_edges <= init,
            "min_nodes + min_edges needs to be less or equal to init"
        );
        let generic_init = init - min_nodes - min_edges;

        let mut entry_point: Vec<DocumentRef> = vec![];

        let scored_nodes = score_documents(&query_embedding, window_nodes);
        let mut selected_nodes = find_top_k(scored_nodes, init);

        let scored_edges = score_documents(&query_embedding, window_edges);
        let mut selected_edges = find_top_k(scored_edges, init);

        for _ in 0..min_nodes {
            let (document, _) = selected_nodes.next().unwrap();
            entry_point.push(document.clone());
        }
        for _ in 0..min_edges {
            let (document, _) = selected_edges.next().unwrap();
            entry_point.push(document.clone());
        }

        let remaining_entities = find_top_k(chain!(selected_nodes, selected_edges), generic_init);
        for (document, _) in remaining_entities {
            entry_point.push(document.clone());
        }

        // SECONDS STEP: EXPANSION
        let mut selected_docs = entry_point;

        while selected_docs.len() < limit {
            let candidates = selected_docs
                .iter()
                .flat_map(|doc| self.get_context(doc, &graph, window_start, window_end));

            let unique_candidates = candidates.unique_by(|doc| doc.id());
            let valid_candidates = unique_candidates.filter(|doc| !selected_docs.contains(doc));
            let scored_candidates = score_documents(&query_embedding, valid_candidates);
            let top_sorted_candidates = find_top_k(scored_candidates, usize::MAX)
                .map(|(doc, _)| doc)
                .cloned()
                .collect_vec();

            if top_sorted_candidates.is_empty() {
                // TODO: use similarity search again with the whole graph with init + 1 !!
                // TODO: avoid this, put all stop conditions at the top
                break;
            }

            selected_docs.extend(top_sorted_candidates);
        }

        // FINAL STEP: REPRODUCE DOCUMENTS:
        selected_docs
            .iter()
            .take(limit)
            .map(|doc| doc.regenerate(&self.graph, &self.template))
            .collect_vec()
    }

    fn get_context<'a, V: GraphViewOps>(
        &'a self,
        document: &DocumentRef,
        graph: &'a V,
        start: Option<i64>,
        end: Option<i64>,
    ) -> Box<dyn Iterator<Item = &DocumentRef> + '_> {
        match document.entity_id {
            EntityId::Node { id } => {
                let self_docs = self.node_documents.get(&document.entity_id).unwrap();
                let edges = graph.vertex(id).unwrap().edges();
                let edge_docs = edges.flat_map(|edge| {
                    let edge_id = edge.into();
                    self.edge_documents.get(&edge_id).unwrap()
                });
                Box::new(
                    chain!(self_docs, edge_docs)
                        .filter(move |doc| doc.exists_on_window(graph, start, end)),
                )
            }
            EntityId::Edge { src, dst } => {
                let self_docs = self.edge_documents.get(&document.entity_id).unwrap();
                let edge = graph.edge(src, dst).unwrap();
                let src_id: EntityId = edge.src().into();
                let dst_id: EntityId = edge.dst().into();
                let src_docs = self.node_documents.get(&src_id).unwrap();
                let dst_docs = self.node_documents.get(&dst_id).unwrap();
                Box::new(
                    chain!(self_docs, src_docs, dst_docs)
                        .filter(move |doc| doc.exists_on_window(graph, start, end)),
                )
            }
        }
    }

    fn window_embeddings<'a, I>(
        &self,
        documents: I,
        window: &WindowedGraph<G>,
        start: Option<i64>,
        end: Option<i64>,
    ) -> impl Iterator<Item = &'a DocumentRef> + 'a
    where
        I: IntoIterator<Item = (&'a EntityId, &'a Vec<DocumentRef>)> + 'a,
    {
        let window = window.clone();
        documents
            .into_iter()
            .flat_map(|(_, documents)| documents)
            .filter(move |document| document.exists_on_window(&window, start, end))
    }
}

fn score_documents<'a, I>(
    query: &'a Embedding,
    documents: I,
) -> impl Iterator<Item = (&'a DocumentRef, f32)> + 'a
where
    I: IntoIterator<Item = &'a DocumentRef> + 'a,
{
    documents.into_iter().map(|document| {
        let score = cosine(query, &document.embedding);
        (document, score)
    })
}

/// Returns the top k nodes in descending order
fn find_top_k<'a, I, T: 'a>(elements: I, k: usize) -> impl Iterator<Item = (&'a T, f32)> + 'a
where
    I: Iterator<Item = (&'a T, f32)> + 'a,
{
    // TODO: add optimization for when this is used -> don't maintain more candidates than the max number of documents to return !!!
    elements
        .sorted_by(|(_, d1), (_, d2)| d1.partial_cmp(d2).unwrap().reverse())
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

    dot_product / (x_length.sqrt() * y_length.sqrt())
    // dot_product
    // TODO: assert that the result is between -1 and 1
}
