use crate::{
    db::{
        api::view::internal::{DynamicGraph, IntoDynamic},
        graph::{edge::EdgeView, vertex::VertexView, views::window_graph::WindowedGraph},
    },
    prelude::{EdgeViewOps, GraphViewOps, Layer, TimeOps, VertexViewOps},
    vectors::{entity_id::EntityId, graph_entity::GraphEntity, Embedding, EmbeddingFunction},
};
use itertools::{chain, Itertools};
use rand_distr::weighted_alias::AliasableWeight;
use std::{borrow::Borrow, collections::HashMap};

pub struct VectorizedGraph<G: GraphViewOps> {
    graph: G,
    embedding: Box<dyn EmbeddingFunction>,
    node_embeddings: HashMap<EntityId, Embedding>, // TODO: replace with FxHashMap
    edge_embeddings: HashMap<EntityId, Embedding>,
    node_template: Box<dyn Fn(&VertexView<G>) -> String + Sync + Send>,
    edge_template: Box<dyn Fn(&EdgeView<G>) -> String + Sync + Send>,
}

impl<G: GraphViewOps + IntoDynamic> VectorizedGraph<G> {
    pub(crate) fn new(
        graph: G,
        embedding: Box<dyn EmbeddingFunction>,
        node_embeddings: HashMap<EntityId, Embedding>,
        edge_embeddings: HashMap<EntityId, Embedding>,
        node_template: Box<dyn Fn(&VertexView<G>) -> String + Sync + Send>,
        edge_template: Box<dyn Fn(&EdgeView<G>) -> String + Sync + Send>,
    ) -> Self {
        Self {
            graph,
            embedding,
            node_embeddings,
            edge_embeddings,
            node_template,
            edge_template,
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
    ) -> Vec<String> {
        let query_embedding = self.embedding.call(vec![query.to_owned()]).await.remove(0);

        let (graph, window_nodes, window_edges): (
            DynamicGraph,
            Box<dyn Iterator<Item = (&EntityId, &Embedding)>>,
            Box<dyn Iterator<Item = (&EntityId, &Embedding)>>,
        ) = match (window_start, window_end) {
            (None, None) => (
                self.graph.clone().into_dynamic(),
                Box::new(self.node_embeddings.iter()),
                Box::new(self.edge_embeddings.iter()),
            ),
            (start, end) => {
                let start = start.unwrap_or(i64::MIN);
                let end = end.unwrap_or(i64::MAX);
                let window = self.graph.window(start, end);
                let nodes = self.window_embeddings(&self.node_embeddings, &window);
                let edges = self.window_embeddings(&self.edge_embeddings, &window);
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

        let mut entry_point: Vec<EntityId> = vec![];

        let scored_nodes = score_entities(&query_embedding, window_nodes);
        let mut selected_nodes = find_top_k(scored_nodes, init);

        let scored_edges = score_entities(&query_embedding, window_edges);
        let mut selected_edges = find_top_k(scored_edges, init);

        for _ in 0..min_nodes {
            let (id, _) = selected_nodes.next().unwrap();
            entry_point.push(id.clone());
        }
        for _ in 0..min_edges {
            let (id, _) = selected_edges.next().unwrap();
            entry_point.push(id.clone());
        }

        let remaining_entities = find_top_k(chain!(selected_nodes, selected_edges), generic_init);
        for (id, distance) in remaining_entities {
            entry_point.push(id.clone());
        }

        // SECONDS STEP: EXPANSION
        let mut entity_ids = entry_point;

        while entity_ids.len() < limit {
            let candidates = entity_ids.iter().flat_map(|id| match id {
                EntityId::Node { id } => {
                    let edges = graph.vertex(*id).unwrap().edges();
                    edges
                        .map(|edge| {
                            let edge_id = edge.into();
                            let edge_embedding = self.edge_embeddings.get(&edge_id).unwrap();
                            (edge_id, edge_embedding)
                        })
                        .collect_vec()
                }
                EntityId::Edge { src, dst } => {
                    let edge = graph.edge(*src, *dst).unwrap();
                    let src_id: EntityId = edge.src().into();
                    let dst_id: EntityId = edge.dst().into();
                    let src_embedding = self.node_embeddings.get(&src_id).unwrap();
                    let dst_embedding = self.node_embeddings.get(&dst_id).unwrap();
                    vec![(src_id, src_embedding), (dst_id, dst_embedding)]
                }
            });

            let unique_candidates = candidates.unique_by(|(id, _)| id.clone());
            let valid_candidates = unique_candidates.filter(|(id, _)| !entity_ids.contains(id));
            let scored_candidates = score_entities(&query_embedding, valid_candidates);
            let sorted_candidates = find_top_k(scored_candidates, usize::MAX);
            let sorted_candidates_ids = sorted_candidates.map(|(id, _)| id).collect_vec();

            if sorted_candidates_ids.is_empty() {
                // TODO: use similarity search again with the whole graph with init + 1 !!
                // TODO: avoid this, put all stop conditions at the top
                break;
            }

            entity_ids.extend(sorted_candidates_ids);
        }

        // FINAL STEP: REPRODUCE DOCUMENTS:

        entity_ids
            .iter()
            .take(limit)
            .map(|id| match id {
                EntityId::Node { id } => {
                    self.graph
                        .vertex(*id)
                        .unwrap()
                        .generate_doc(&self.node_template)
                        .content
                }
                EntityId::Edge { src, dst } => {
                    self.graph
                        .edge(*src, *dst)
                        .unwrap()
                        .generate_doc(&self.edge_template)
                        .content
                }
            })
            .collect_vec()
    }

    fn window_embeddings<'a, I>(
        &self,
        embeddings: I,
        window: &WindowedGraph<G>,
    ) -> impl Iterator<Item = (&'a EntityId, &'a Embedding)> + 'a
    where
        I: IntoIterator<Item = (&'a EntityId, &'a Embedding)> + 'a,
    {
        let window = window.clone();
        embeddings.into_iter().filter(move |(id, _)| match id {
            EntityId::Node { id } => window.has_vertex(*id),
            EntityId::Edge { src, dst } => window.has_edge(*src, *dst, Layer::All),
        })
    }

    // pub async fn search_old(
    //     &self,
    //     query: &str,
    //     node_init: usize,
    //     edge_init: usize,
    //     limit: usize,
    // ) -> Vec<String> {
    //     let query_embedding = compute_embeddings(vec![query.to_owned()]).await.remove(0);
    //
    //     let mut entry_point: Vec<EntityId> = vec![];
    //     let selected_nodes = find_top_k(&query_embedding, &self.node_embeddings, node_init);
    //     let selected_edges = find_top_k(&query_embedding, &self.edge_embeddings, edge_init);
    //     for (id, distance) in chain!(selected_nodes, selected_edges) {
    //         println!(" - At {distance}: {id}");
    //         entry_point.push(id.clone());
    //         if let EntityId::Edge { src, dst } = id {
    //             entry_point.push(EntityId::Node { id: *src });
    //             entry_point.push(EntityId::Node { id: *dst });
    //         }
    //     }
    //
    //     let mut entity_ids = entry_point;
    //
    //     // it might happen that a node is include here twice, from two different paths in the graph
    //     // but that is not a problem because the entity_ids list is force to be unique
    //     let candidates: Vec<ExpandCandidate> = entity_ids
    //         .iter()
    //         .filter(|id| matches!(id, EntityId::Node { .. }))
    //         .flat_map(|id| self.get_candidates_from_node(&query_embedding, id))
    //         .unique_by(|candidate| (candidate.node.clone(), candidate.edge.clone()))
    //         .collect_vec();
    //
    //     let mut sorted_candidates = SortedVec::from(candidates);
    //
    //     println!("TODO: print sorted candidates");
    //
    //     while entity_ids.len() < limit && sorted_candidates.len() > 0 {
    //         let ExpandCandidate { node, edge, .. } = sorted_candidates.pop().unwrap();
    //         // we could terminate the loop instead I guess
    //
    //         if !entity_ids.contains(&node) {
    //             entity_ids.push(node.clone());
    //         }
    //         if !entity_ids.contains(&edge) {
    //             entity_ids.push(edge);
    //         }
    //
    //         for new_candidate in self.get_candidates_from_node(&query_embedding, &node) {
    //             let already_candidate = || {
    //                 sorted_candidates
    //                     .iter()
    //                     .any(|candidate| candidate.edge == new_candidate.edge)
    //             };
    //             let already_selected = || entity_ids.iter().any(|id| id == &new_candidate.edge);
    //             if !already_selected() && !already_candidate() {
    //                 sorted_candidates.insert(new_candidate);
    //             }
    //         }
    //     }
    //
    //     entity_ids
    //         .iter()
    //         .take(limit)
    //         .map(|id| match id {
    //             EntityId::Node { id } => {
    //                 self.graph
    //                     .vertex(*id)
    //                     .unwrap()
    //                     .generate_doc(&self.node_template)
    //                     .content
    //             }
    //             EntityId::Edge { src, dst } => {
    //                 self.graph
    //                     .edge(*src, *dst)
    //                     .unwrap()
    //                     .generate_doc(&self.edge_template)
    //                     .content
    //             }
    //         })
    //         .collect_vec()
    // }

    // returns an iterator of triplets: (node id, edge id, score) as candidates to be included in entity_ids
    // fn get_candidates_from_node<'a>(
    //     &'a self,
    //     query: &'a Embedding,
    //     node_id: &EntityId,
    // ) -> impl Iterator<Item = ExpandCandidate> + 'a {
    //     let vertex = self.graph.vertex(node_id.as_node()).unwrap();
    //     let in_edges = vertex.in_edges().map(move |edge| ExpandCandidate {
    //         node: (&edge.src()).into(),
    //         edge: (&edge).into(),
    //         score: self.score_pair(&query, edge.src(), edge),
    //     });
    //     let out_edges = vertex.out_edges().map(move |edge| ExpandCandidate {
    //         node: (&edge.dst()).into(),
    //         edge: (&edge).into(),
    //         score: self.score_pair(&query, edge.dst(), edge),
    //     });
    //     chain!(in_edges, out_edges)
    // }

    // fn score_pair(&self, query: &Embedding, node: VertexView<G>, edge: EdgeView<G>) -> f32 {
    //     let node_vector = self.node_embeddings.get(&(&node).into()).unwrap();
    //     let node_similarity = cosine(query, node_vector);
    //     let edge_vector = self.edge_embeddings.get(&(&edge).into()).unwrap();
    //     let edge_similarity = cosine(query, edge_vector);
    //
    //     if node_similarity > edge_similarity {
    //         node_similarity
    //     } else {
    //         edge_similarity
    //     }
    // }
}

fn score_entities<'a, I, E>(
    query: &'a Embedding,
    entities: I,
) -> impl Iterator<Item = (E, f32)> + 'a
where
    I: IntoIterator<Item = (E, &'a Embedding)> + 'a,
    E: Borrow<EntityId> + 'a,
{
    entities
        .into_iter()
        .map(|(id, embedding)| (id, cosine(query, embedding)))
}

/// Returns the top k nodes in descending order
fn find_top_k<'a, I, E>(entities: I, k: usize) -> impl Iterator<Item = (E, f32)> + 'a
where
    I: Iterator<Item = (E, f32)> + 'a,
    E: Borrow<EntityId> + 'a,
{
    entities
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
