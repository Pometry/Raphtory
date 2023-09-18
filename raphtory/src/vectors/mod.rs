// use async_openai::types::{CreateEmbeddingRequest, EmbeddingInput};
// use async_openai::Client;
use async_trait::async_trait;
use futures_util::future::{join_all, BoxFuture};
// use futures_util::StreamExt;
use itertools::{chain, Itertools};
use std::{
    borrow::Borrow,
    collections::{hash_map::DefaultHasher, HashMap},
    convert::identity,
    fmt::{Display, Formatter},
    fs::{create_dir_all, File},
    future::Future,
    hash::{Hash, Hasher},
    io::{BufReader, BufWriter},
    path::Path,
};

use crate::db::api::{
    properties::internal::{TemporalPropertiesOps, TemporalPropertyViewOps},
    view::internal::{DynamicGraph, IntoDynamic},
};
use serde::{Deserialize, Serialize, Serializer};

// use crate::model::graph::edge::Edge;
// use numpy::PyArray2;
// use pyo3::{types::IntoPyDict, Python};
use crate::{
    db::graph::{edge::EdgeView, vertex::VertexView, views::window_graph::WindowedGraph},
    prelude::{EdgeViewOps, GraphViewOps, Layer, LayerOps, TimeOps, VertexViewOps},
};

// #[derive(Clone)]
// struct EdgeId {
//     src: u64,
//     dst: u64,
// }

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
enum EntityId {
    Node { id: u64 },
    Edge { src: u64, dst: u64 },
}

impl EntityId {
    fn as_node(&self) -> u64 {
        match self {
            EntityId::Node { id } => *id,
            EntityId::Edge { .. } => panic!("edge id unwrapped as a node id"),
        }
    }
}

impl<G: GraphViewOps> From<&VertexView<G>> for EntityId {
    fn from(value: &VertexView<G>) -> Self {
        EntityId::Node { id: value.id() }
    }
}

impl<G: GraphViewOps> From<VertexView<G>> for EntityId {
    fn from(value: VertexView<G>) -> Self {
        EntityId::Node { id: value.id() }
    }
}

impl<G: GraphViewOps> From<&EdgeView<G>> for EntityId {
    fn from(value: &EdgeView<G>) -> Self {
        EntityId::Edge {
            src: value.src().id(),
            dst: value.dst().id(),
        }
    }
}

impl<G: GraphViewOps> From<EdgeView<G>> for EntityId {
    fn from(value: EdgeView<G>) -> Self {
        EntityId::Edge {
            src: value.src().id(),
            dst: value.dst().id(),
        }
    }
}

pub trait EmbeddingFunction: Send + Sync {
    fn call(&self, texts: Vec<String>) -> BoxFuture<'static, Vec<Embedding>>;
}

impl<T, F> EmbeddingFunction for T
where
    T: Fn(Vec<String>) -> F + Send + Sync,
    F: Future<Output = Vec<Embedding>> + Send + 'static,
{
    fn call(&self, texts: Vec<String>) -> BoxFuture<'static, Vec<Embedding>> {
        Box::pin(self(texts))
    }
}

#[async_trait]
pub trait Vectorizable<G: GraphViewOps> {
    async fn vectorize(
        &self,
        embedding: Box<dyn EmbeddingFunction>,
        cache_dir: &Path,
    ) -> VectorizedGraph<G>;

    async fn vectorize_with_templates<N, E>(
        &self,
        embedding: Box<dyn EmbeddingFunction>,
        cache_dir: &Path,
        node_template: N,
        edge_template: E,
        // FIXME: I tried to put templates behind an option but didn't work and hadn't time to fix it
    ) -> VectorizedGraph<G>
    where
        N: Fn(&VertexView<G>) -> String + Sync + Send + 'static,
        E: Fn(&EdgeView<G>) -> String + Sync + Send + 'static;
}

#[async_trait]
impl<G: GraphViewOps> Vectorizable<G> for G {
    async fn vectorize(
        &self,
        embedding: Box<dyn EmbeddingFunction>,
        cache_dir: &Path,
    ) -> VectorizedGraph<G> {
        let node_template = |vertex: &VertexView<G>| default_node_template(vertex);
        let edge_template = |edge: &EdgeView<G>| default_edge_template(edge);

        self.vectorize_with_templates(embedding, cache_dir, node_template, edge_template)
            .await
    }

    async fn vectorize_with_templates<N, E>(
        &self,
        embedding: Box<dyn EmbeddingFunction>,
        cache_dir: &Path,
        node_template: N,
        edge_template: E,
    ) -> VectorizedGraph<G>
    where
        N: Fn(&VertexView<G>) -> String + Sync + Send + 'static,
        E: Fn(&EdgeView<G>) -> String + Sync + Send + 'static,
    {
        create_dir_all(cache_dir).expect("Impossible to use cache dir");

        let node_docs = self
            .vertices()
            .iter()
            .map(|vertex| vertex.generate_doc(&node_template));
        let edge_docs = self.edges().map(|edge| edge.generate_doc(&edge_template));

        let node_embeddings = generate_embeddings(node_docs, &embedding, cache_dir).await;
        let edge_embeddings = generate_embeddings(edge_docs, &embedding, cache_dir).await;

        VectorizedGraph {
            graph: self.clone(),
            embedding,
            node_embeddings,
            edge_embeddings,
            node_template: Box::new(node_template),
            edge_template: Box::new(edge_template),
        }
    }
}

fn default_node_template<G: GraphViewOps>(vertex: &VertexView<G>) -> String {
    let name = vertex.name();
    let property_list = vertex.generate_property_list(&identity, vec![], vec![]);
    format!("The entity {name} has the following details:\n{property_list}")
}

fn default_edge_template<G: GraphViewOps>(edge: &EdgeView<G>) -> String {
    let src = edge.src().name();
    let dst = edge.dst().name();
    // TODO: property list

    edge.layer_names()
        .iter()
        .map(|layer| {
            let times = edge.layer(layer).unwrap().history().iter().join(", ");
            match layer.as_str() {
                "_default" => format!("{src} interacted with {dst} at times: {times}"),
                layer => format!("{src} {layer} {dst} at times: {times}"),
            }
        })
        .intersperse("\n".to_owned())
        .collect()
}

pub struct VectorizedGraph<G: GraphViewOps> {
    graph: G,
    embedding: Box<dyn EmbeddingFunction>,
    node_embeddings: HashMap<EntityId, Embedding>,
    edge_embeddings: HashMap<EntityId, Embedding>,
    node_template: Box<dyn Fn(&VertexView<G>) -> String + Sync + Send>,
    edge_template: Box<dyn Fn(&EdgeView<G>) -> String + Sync + Send>,
}

const CHUNK_SIZE: usize = 1000;

impl<G: GraphViewOps + IntoDynamic> VectorizedGraph<G> {
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

async fn generate_embeddings<I>(
    docs: I,
    embedding: &Box<dyn EmbeddingFunction>,
    cache_dir: &Path,
) -> HashMap<EntityId, Embedding>
where
    I: Iterator<Item = EntityDocument>,
{
    // ----------------- SEQUENTIAL-ASYNC-VERSION -----------------
    // let mut embeddings = vec![];
    // let embedding_stream = stream! {
    //     for doc in docs {
    //         yield (doc.id, doc_to_vec(doc, cache_dir))
    //     }
    // };
    // pin_mut!(embedding_stream);
    // while let Some(embedding) = embedding_stream.next() {
    //     embeddings.push(embedding);
    // }
    // ------------------------------------------------------------

    let mut embeddings = HashMap::new();
    let mut misses = vec![];

    for doc in docs {
        match retrieve_embedding_from_cache(&doc, cache_dir) {
            Some(embedding) => {
                embeddings.insert(doc.id, embedding);
            }
            None => misses.push(doc),
        }
    }

    let embedding_tasks = misses
        .chunks(CHUNK_SIZE)
        .map(|chunk| compute_embeddings_with_cache(chunk.to_vec(), embedding, cache_dir));
    let computed_embeddings = join_all(embedding_tasks).await.into_iter().flatten();
    for (id, embedding) in computed_embeddings {
        embeddings.insert(id, embedding);
    }

    embeddings
}

async fn compute_embeddings_with_cache(
    docs: Vec<EntityDocument>,
    embedding: &Box<dyn EmbeddingFunction>,
    cache_dir: &Path,
) -> Vec<(EntityId, Embedding)> {
    let texts = docs.iter().map(|doc| doc.content.clone()).collect_vec();
    let embeddings = embedding.call(texts).await;
    docs.into_iter()
        .zip(embeddings)
        .map(|(doc, embedding)| {
            let doc_hash = hash_doc(&doc); // FIXME: I'm hashing twice
            let embedding_cache = EmbeddingCache {
                doc_hash,
                embedding,
            };
            let doc_path = cache_dir.join(doc.id.to_string());
            let doc_file =
                File::create(doc_path).expect("Couldn't create file to store embedding cache");
            let mut doc_writer = BufWriter::new(doc_file);
            bincode::serialize_into(&mut doc_writer, &embedding_cache)
                .expect("Couldn't serialize embedding cache");
            (doc.id, embedding_cache.embedding)
        })
        .collect_vec()
}

fn retrieve_embedding_from_cache(doc: &EntityDocument, cache_dir: &Path) -> Option<Embedding> {
    let doc_path = cache_dir.join(doc.id.to_string());
    let doc_file = File::open(doc_path).ok()?;
    let mut doc_reader = BufReader::new(doc_file);
    let embedding_cache: EmbeddingCache = bincode::deserialize_from(&mut doc_reader).ok()?;
    let doc_hash = hash_doc(doc);
    if doc_hash == embedding_cache.doc_hash {
        Some(embedding_cache.embedding)
    } else {
        None
    }
}

// fn find_top_k_old<'a>(
//     query: &'a Embedding,
//     entities: &'a HashMap<EntityId, Embedding>,
//     k: usize,
// ) -> impl Iterator<Item = (&'a EntityId, f32)> {
//     entities
//         .iter()
//         .map(|(id, embedding)| (id, cosine(query, embedding)))
//         .sorted_by(|(_, d1), (_, d2)| d1.partial_cmp(d2).unwrap().reverse())
//         // We use reverse because default sorting is ascending but we want it descending
//         .take(k)
// }

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

#[derive(Clone)]
pub struct EntityDocument {
    id: EntityId,
    content: String,
}

#[derive(Serialize, Deserialize)]
struct EmbeddingCache {
    doc_hash: u64,
    embedding: Embedding,
}

pub type Embedding = Vec<f32>;

// async fn compute_embeddings(texts: Vec<String>) -> Vec<Embedding> {
//     println!("computing embeddings for {} texts", texts.len());
//     Python::with_gil(|py| {
//         let sentence_transformers = py.import("sentence_transformers")?;
//         let locals = [("sentence_transformers", sentence_transformers)].into_py_dict(py);
//         locals.set_item("texts", texts);
//
//         let pyarray: &PyArray2<f32> = py
//             .eval(
//                 &format!(
//                     "sentence_transformers.SentenceTransformer('thenlper/gte-small').encode(texts)"
//                 ),
//                 Some(locals),
//                 None,
//             )?
//             .extract()?;
//
//         let readonly = pyarray.readonly();
//         let chunks = readonly.as_slice().unwrap().chunks(384).into_iter();
//         let embeddings = chunks
//             .map(|chunk| chunk.iter().copied().collect_vec())
//             .collect_vec();
//
//         Ok::<Vec<Vec<f32>>, Box<dyn std::error::Error>>(embeddings)
//     })
//     .unwrap()
// }

fn hash_doc(doc: &EntityDocument) -> u64 {
    let mut hasher = DefaultHasher::new();
    doc.content.hash(&mut hasher);
    hasher.finish()
}

impl Display for EntityId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            EntityId::Node { id } => f.serialize_u64(*id),
            EntityId::Edge { src, dst } => {
                f.serialize_u64(*src)
                    .expect("src ID couldn't be serialized");
                f.write_str("-")
                    .expect("edge ID separator couldn't be serialized");
                f.serialize_u64(*dst)
            }
        }
    }
}

pub trait GraphEntity: Sized {
    // fn entity_id(&self) -> EntityId;
    fn generate_doc<T>(&self, template: &T) -> EntityDocument
    where
        T: Fn(&Self) -> String;

    fn generate_property_list<F, D>(
        &self,
        time_fmt: &F,
        filter_out: Vec<&str>,
        force_static: Vec<&str>,
    ) -> String
    where
        F: Fn(i64) -> D,
        D: Display;
}

impl<G: GraphViewOps> GraphEntity for VertexView<G> {
    fn generate_property_list<F, D>(
        &self,
        time_fmt: &F,
        filter_out: Vec<&str>,
        force_static: Vec<&str>,
    ) -> String
    where
        F: Fn(i64) -> D,
        D: Display,
    {
        let time_fmt = |time: i64| time_fmt(time).to_string();
        let missing = || "missing".to_owned();
        let min_time_fmt = self.earliest_time().map(time_fmt).unwrap_or_else(missing);
        let min_time = format!("earliest activity: {}", min_time_fmt);
        let max_time_fmt = self.latest_time().map(time_fmt).unwrap_or_else(missing);
        let max_time = format!("latest activity: {}", max_time_fmt);

        let temporal_keys = self
            .temporal_property_keys()
            .filter(|key| !filter_out.contains(&key.as_str()))
            .filter(|key| !force_static.contains(&key.as_str()))
            .filter(|key| {
                // the history of the temporal prop has more than one value
                let props = self.temporal_values(key);
                let values = props.iter().map(|prop| prop.to_string());
                values.unique().collect_vec().len() > 1
            })
            .collect_vec();

        let temporal_props = temporal_keys.iter().map(|key| {
            let history = self.temporal_history(&key);
            let props = self.temporal_values(&key);
            let values = props.iter().map(|prop| prop.to_string());
            let time_value_pairs = history.iter().zip(values);
            time_value_pairs
                .unique_by(|(_, value)| value.clone())
                .map(|(time, value)| {
                    let key = key.to_string();
                    let time = time_fmt(*time);
                    format!("{key} changed to {value} at {time}")
                })
                .intersperse("\n".to_owned())
                .collect()
        });

        let prop_storage = self.properties();

        let static_props = prop_storage
            .keys()
            .filter(|key| !filter_out.contains(&key.as_str()))
            .filter(|key| !temporal_keys.contains(key))
            .map(|key| {
                let prop = prop_storage.get(&key).unwrap().to_string();
                let key = key.to_string();
                format!("{key}: {prop}")
            });

        let props = chain!(static_props, temporal_props).sorted_by(|a, b| a.len().cmp(&b.len()));
        // We sort by length so when cutting out the tail of the document we don't remove small properties

        let lines = chain!([min_time, max_time], props);
        lines.intersperse("\n".to_owned()).collect()
    }

    fn generate_doc<T>(&self, template: &T) -> EntityDocument
    where
        T: Fn(&Self) -> String,
    {
        let raw_content = template(self);
        let content = match raw_content.char_indices().nth(1000) {
            Some((index, _)) => (&raw_content[..index]).to_owned(),
            None => raw_content,
        };
        // TODO: allow multi document entities !!!!!
        // shortened to 1000 (around 250 tokens) to avoid exceeding the max number of tokens,
        // when embedding but also when inserting documents into prompts

        EntityDocument {
            id: EntityId::Node { id: self.id() },
            content,
        }
    }
}

impl<G: GraphViewOps> GraphEntity for EdgeView<G> {
    fn generate_property_list<F, D>(
        &self,
        time_fmt: &F,
        filter_out: Vec<&str>,
        force_static: Vec<&str>,
    ) -> String
    where
        F: Fn(i64) -> D,
        D: Display,
    {
        // TODO: not needed yet
        "".to_owned()
    }
    fn generate_doc<T>(&self, template: &T) -> EntityDocument
    where
        T: Fn(&Self) -> String,
    {
        let content = template(self);
        EntityDocument {
            id: EntityId::Edge {
                src: self.src().id(),
                dst: self.dst().id(),
            },
            content,
        }
    }
}

#[cfg(test)]
mod vector_tests {
    use super::*;
    use crate::prelude::AdditionOps;
    use crate::{core::Prop, prelude::Graph};
    use dotenv::dotenv;
    use std::path::PathBuf;

    const NO_PROPS: [(&str, Prop); 0] = [];

    fn format_time(time: i64) -> String {
        format!("line {time}")
    }

    fn node_template(vertex: &VertexView<Graph>) -> String {
        let name = vertex.name();
        let node_type = vertex.properties().get("type").unwrap().to_string();
        let property_list =
            vertex.generate_property_list(&format_time, vec!["type", "_id"], vec![]);
        format!("{name} is a {node_type} with the following details:\n{property_list}")
    }

    fn edge_template(edge: &EdgeView<Graph>) -> String {
        let src = edge.src().name();
        let dst = edge.dst().name();
        let lines = edge.history().iter().join(",");
        format!("{src} appeared with {dst} in lines: {lines}")
    }

    // TODO: test default templates

    // #[test] // TODO: re-enable
    fn test_node_into_doc() {
        let g = Graph::new();
        g.add_vertex(
            0,
            "Frodo",
            [
                ("type".to_string(), Prop::str("hobbit")),
                ("age".to_string(), Prop::str("30")),
            ],
        )
        .unwrap();

        let doc = g
            .vertex("Frodo")
            .unwrap()
            .generate_doc(&node_template)
            .content;
        let expected_doc = r###"Frodo is a hobbit with the following details:
earliest activity: line 0
latest activity: line 0
age: 30"###;
        assert_eq!(doc, expected_doc);
    }

    // #[test] // TODO: re-enable
    fn test_edge_into_doc() {
        let g = Graph::new();
        g.add_edge(0, "Frodo", "Gandalf", NO_PROPS, Some("talk to"))
            .unwrap();

        let doc = g
            .edge("Frodo", "Gandalf")
            .unwrap()
            .generate_doc(&edge_template)
            .content;
        let expected_doc = "Frodo appeared with Gandalf in lines: 0";
        assert_eq!(doc, expected_doc);
    }

    // #[tokio::test] // TODO: re-enable
    async fn test_vector_store() {
        let g = Graph::new();
        g.add_vertex(
            0,
            "Gandalf",
            [
                ("type".to_string(), Prop::str("wizard")),
                ("age".to_string(), Prop::str("120")),
            ],
        )
        .unwrap();
        g.add_vertex(
            0,
            "Frodo",
            [
                ("type".to_string(), Prop::str("hobbit")),
                ("age".to_string(), Prop::str("30")),
            ],
        )
        .unwrap();
        g.add_edge(0, "Frodo", "Gandalf", NO_PROPS, Some("talk to"))
            .unwrap();
        g.add_vertex(
            2,
            "Aragorn",
            [
                ("type".to_string(), Prop::str("human")),
                ("age".to_string(), Prop::str("40")),
            ],
        )
        .unwrap();

        dotenv().ok();
        let vec_store = VectorStore::load_graph(
            g,
            &PathBuf::from("/tmp/raphtory/vector-cache-lotr-test"),
            Some(Box::new(node_template)),
            Some(Box::new(edge_template)),
        )
        .await;

        let docs = vec_store
            .search("Find a magician", 1, 0, 0, 1, None, None)
            .await;
        assert!(docs[0].contains("Gandalf is a wizard"));

        let docs = vec_store
            .search("Find a young person", 1, 0, 0, 1, None, None)
            .await;
        assert!(docs[0].contains("Frodo is a hobbit")); // this fails when using gte-small

        // with window!
        let docs = vec_store
            .search("Find a young person", 1, 0, 0, 1, Some(1), Some(3))
            .await;
        assert!(!docs[0].contains("Frodo is a hobbit")); // this fails when using gte-small

        let docs = vec_store
            .search(
                "Has anyone appeared with anyone else?",
                1,
                0,
                0,
                1,
                None,
                None,
            )
            .await;
        assert!(docs[0].contains("Frodo appeared with Gandalf"));
    }

    fn average_vectors(vec1: &Embedding, vec2: &Embedding) -> Embedding {
        vec1.iter()
            .zip(vec2)
            .map(|(a, b)| (a + b) / 2.0)
            .collect_vec()
    }

    // #[tokio::test] // TODO: re-enable
    async fn test_combinations() {
        dotenv().ok();
        // I want to test if a document tuple node-edge can rank higher than

        let ticket = "DEV-1303 is an issue created by the Pometry team with the following details:\nearliest activity: 1667924841177\nlatest activity: 1676301689177\n_id: DEV-1303\nname: DEV-1303\njira_id: 12212\npriority: Medium\nresolution: Done\nstatus: CANCELLED\njira_url: https://pometry.atlassian.net/rest/agile/1.0/issue/12212\nsummary: Build ReadTheDocs during CI/CD as a Test to ensure it still works\ndescription: {panel:bgColor=#eae6ff}\nRemove me and Insert *what* needs to be done and *why* it needs to be done\n{panel}\n\nThis must replicate the read the docs build process. ";
        let edge =
            "Pedro Rico Pinazo was assigned to work on issue DEV-1303 at time: 2022-06-29 12:34:15";
        let question = "tell me about someone that has been working on documentation";

        let ticket_embedding = compute_embeddings(vec![ticket.to_owned()]).await.remove(0);
        let edge_embedding = compute_embeddings(vec![edge.to_owned()]).await.remove(0);
        let question_embedding = compute_embeddings(vec![question.to_owned()])
            .await
            .remove(0);
        let comb_embedding = average_vectors(&ticket_embedding, &edge_embedding);

        let ticket_score = cosine(&question_embedding, &ticket_embedding);
        let edge_score = cosine(&question_embedding, &edge_embedding);
        let comb_score = cosine(&question_embedding, &comb_embedding);

        dbg!(ticket_score);
        dbg!(edge_score);
        dbg!(comb_score);
    }
}
