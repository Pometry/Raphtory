use crate::{
    // core::entities::nodes::node_ref::AsNodeRef,
    core::{entities::nodes::node_ref::AsNodeRef, utils::errors::GraphResult},
    db::api::view::{internal::CoreGraphOps, DynamicGraph, IntoDynamic, StaticGraphViewOps},
    prelude::*,
    vectors::{
        document_ref::DocumentRef,
        embedding_cache::EmbeddingCache,
        entity_id::EntityId,
        similarity_search_utils::{find_top_k, score_documents},
        template::DocumentTemplate,
        Embedding, EmbeddingFunction,
    },
};
use arroy::{distances::Euclidean, Database as ArroyDatabase, Reader};
use async_trait::async_trait;
use itertools::{chain, Itertools};
use parking_lot::RwLock;
use std::{
    collections::{HashMap, HashSet},
    ops::Deref,
    path::PathBuf,
    sync::Arc,
};
use tempfile::TempDir;

use super::{
    similarity_search_utils::score_document_groups_by_highest,
    vector_selection::VectorSelection,
    vectorisable::{vectorise_edge, vectorise_graph, vectorise_node},
    Document,
};

#[derive(Clone, Copy)]
pub(crate) enum EntityRef {
    Node(usize),
    Edge(usize),
}

impl EntityRef {
    pub(crate) fn as_node(&self) -> Option<usize> {
        if let EntityRef::Node(id) = self {
            Some(id)
        } else {
            None
        }
    }

    pub(crate) fn as_edge(&self) -> Option<usize> {
        if let EntityRef::Edge(id) = self {
            Some(id)
        } else {
            None
        }
    }
}

#[derive(Clone)]
pub(crate) struct VectorDb {
    // FIXME: save index value in here !!!!!!!!!!!!!!!!!!!
    pub(crate) vectors: ArroyDatabase<Euclidean>, // TODO: review is this safe to clone? does it point to the same thing?
    pub(crate) env: heed::Env,
    pub(crate) tempdir: Arc<TempDir>, // do I really need, is the file open not enough
}

// trait VectorSearch {
//     fn top_k<G: StaticGraphViewOps>(
//         &self,
//         query: &Embedding,
//         k: usize,
//         view: Option<G>,
//         filter: HashSet<u32>,
//     ) -> Box<dyn Iterator<Item = (EntityRef, f32)>> {
//         // let count = g.count_nodes();
//         let docs: Vec<_> = self.node_db.top_k(query, k).collect();
//         let count = docs
//             .iter()
//             .filter(|(id, _)| self.entity_valid(id, view, filter))
//             .count();
//         if count > k {
//             boxed_nodes(docs.into_iter().take(k))
//         } else if count == 0 {
//             let filter = w.nodes().iter().map(|node| node.node.index());
//             let docs = self.node_db.top_k_with_filter(query, k, filter);
//             boxed_nodes(docs)
//         } else {
//             let docs = self.top_k(query, k * 10, view, filter).take(k); // FIXME: avoid hardcoding * 10, do it dinamically
//             Box::new(docs)
//         }
//     }

//     fn entity_valid<G: StaticGraphViewOps>(
//         &self,
//         entity: usize,
//         view: Option<G>,
//         filter: HashSet<u32>,
//     ) -> bool;
// }

pub struct Key {
    pub index: u16,
    pub node: u32,
    _padding: u8,
}

impl From<usize> for Key {
    fn from(value: usize) -> Self {
        Self {
            index: 0, // TODO: bear in mind hardcoded index
            node: value as u32,
            _padding: 0,
        }
    }
}

impl VectorDb {
    // FIXME: remove unwraps here
    pub(super) fn top_k(&self, query: &Embedding, k: usize) -> impl Iterator<Item = (usize, f32)> {
        self.inner_top_k(query, k, None::<std::iter::Empty<usize>>)
    }

    pub(super) fn top_k_with_filter(
        &self,
        query: &Embedding,
        k: usize,
        filter: impl Iterator<Item = usize>,
    ) -> impl Iterator<Item = (usize, f32)> {
        self.inner_top_k(query, k, Some(filter))
    }

    fn inner_top_k(
        &self,
        query: &Embedding,
        k: usize,
        filter: Option<impl Iterator<Item = usize>>,
    ) -> impl Iterator<Item = (usize, f32)> {
        let rtxn = self.env.read_txn().unwrap();
        let reader = Reader::open(&rtxn, 0, self.vectors).unwrap(); // TODO: review index value, hardcoded to 0
        let mut query_builder = reader.nns(k);
        let filter =
            filter.map(|filter| roaring::RoaringBitmap::from_iter(filter.map(|id| id as u32)));
        let query_builder = if let Some(filter) = &filter {
            query_builder.candidates(filter)
        } else {
            &query_builder
        };
        query_builder
            .by_vector(&rtxn, query.as_ref())
            .unwrap()
            .into_iter()
            .map(|(id, score)| (id as usize, score))
    }

    pub(super) fn get_id(&self, id: usize) -> Embedding {
        // FIXME: remove unsafe code, ask arroy mantainers to make the type public
        let rtxn = self.env.read_txn().unwrap();
        let key: Key = id.into();
        let arroy_key = unsafe { std::mem::transmute(&key) };
        let node = self.vectors.get(&rtxn, arroy_key).unwrap().unwrap();
        let vector = node.leaf().unwrap().vector.to_vec().into();
        vector
    }
}

#[derive(Clone)]
pub struct VectorisedGraph<G: StaticGraphViewOps> {
    pub(crate) source_graph: G,
    pub(crate) template: DocumentTemplate,
    pub(crate) embedding: Arc<dyn EmbeddingFunction>,
    pub(crate) cache_storage: Arc<Option<EmbeddingCache>>,
    // it is not the end of the world but we are storing the entity id twice
    pub(crate) node_db: VectorDb,
    pub(crate) edge_db: VectorDb,
}

// This has to be here so it is shared between python and graphql
pub type DynamicVectorisedGraph = VectorisedGraph<DynamicGraph>;

impl<G: StaticGraphViewOps + IntoDynamic> VectorisedGraph<G> {
    pub fn into_dynamic(self) -> VectorisedGraph<DynamicGraph> {
        VectorisedGraph {
            source_graph: self.source_graph.clone().into_dynamic(),
            template: self.template,
            embedding: self.embedding,
            cache_storage: self.cache_storage,
            node_db: self.node_db,
            edge_db: self.edge_db,
        }
    }
}

impl<G: StaticGraphViewOps> VectorisedGraph<G> {
    pub async fn update_node<T: AsNodeRef>(&self, node: T) -> GraphResult<()> {
        // if let Some(node) = self.source_graph.node(node) {
        //     let entity_id = EntityId::from_node(node.clone());
        //     let refs = vectorise_node(
        //         node,
        //         &self.template,
        //         &self.embedding,
        //         self.cache_storage.as_ref(),
        //     )
        //     .await?;
        //     self.node_documents.write().insert(entity_id, refs);
        // }
        Ok(())
    }

    pub async fn update_edge<T: AsNodeRef>(&self, src: T, dst: T) -> GraphResult<()> {
        // if let Some(edge) = self.source_graph.edge(src, dst) {
        //     let entity_id = EntityId::from_edge(edge.clone());
        //     let refs = vectorise_edge(
        //         edge,
        //         &self.template,
        //         &self.embedding,
        //         self.cache_storage.as_ref(),
        //     )
        //     .await?;
        //     self.edge_documents.write().insert(entity_id, refs);
        // }
        Ok(())
    }

    /// Save the embeddings present in this graph to `file` so they can be further used in a call to `vectorise`
    pub fn save_embeddings(&self, file: PathBuf) {
        // let cache = EmbeddingCache::new(file);
        // let node_documents = self.node_documents.read();
        // let edge_documents = self.edge_documents.read();
        // chain!(node_documents.iter(), edge_documents.iter()).for_each(|(_, group)| {
        //     group.iter().for_each(|doc| {
        //         let original = doc.regenerate(&self.source_graph, &self.template);
        //         cache.upsert_embedding(&original.content, doc.embedding.clone());
        //     })
        // });
        // cache.dump_to_disk();
    }

    /// Return an empty selection of documents
    pub fn empty_selection(&self) -> VectorSelection<G> {
        VectorSelection::empty(self.clone())
    }

    /// Search the top scoring entities according to `query` with no more than `limit` entities
    ///
    /// # Arguments
    ///   * query - the embedding to score against
    ///   * limit - the maximum number of entities to search
    ///   * window - the window where documents need to belong to in order to be considered
    ///
    /// # Returns
    ///   The vector selection resulting from the search
    pub fn entities_by_similarity(
        &self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
    ) -> VectorSelection<G> {
        let nodes = self.top_k_nodes_with_window(query, limit, window);
        let edges = self.top_k_edges(query, limit);
        let docs = find_top_k(nodes.chain(edges), limit).collect();
        VectorSelection::new(self.clone(), docs)
    }

    /// Search the top scoring nodes according to `query` with no more than `limit` nodes
    ///
    /// # Arguments
    ///   * query - the embedding to score against
    ///   * limit - the maximum number of nodes to search
    ///   * window - the window where documents need to belong to in order to be considered
    ///
    /// # Returns
    ///   The vector selection resulting from the search
    pub fn nodes_by_similarity(
        &self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
    ) -> VectorSelection<G> {
        let docs = self.top_k_nodes_with_window(query, limit, window);
        VectorSelection::new(self.clone(), docs.collect())
    }

    /// Search the top scoring edges according to `query` with no more than `limit` edges
    ///
    /// # Arguments
    ///   * query - the embedding to score against
    ///   * limit - the maximum number of edges to search
    ///   * window - the window where documents need to belong to in order to be considered
    ///
    /// # Returns
    ///   The vector selection resulting from the search
    pub fn edges_by_similarity(
        &self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
    ) -> VectorSelection<G> {
        let docs = self.top_k_edges(query, limit);
        VectorSelection::new(self.clone(), docs.collect())
    }

    // pub(crate) fn top_k_nodes(
    //     &self,
    //     query: &Embedding,
    //     k: usize,
    // ) -> impl Iterator<Item = (EntityRef, f32)> {
    //     let docs = self.node_db.top_k(query, k);
    //     docs.map(|(id, score)| (EntityRef::Node(id), score))
    // }

    // make this generic for edges
    // need to have a function that, given an entity and a graph view, decides if it exists
    // also need a function that given a u32, returns an EntityRef
    // what happens though with expansions ??????????????????????
    // maybe I can have always two things, both optional: a graph view and a set of ids

    pub(crate) fn top_k_nodes_with_window(
        &self,
        query: &Embedding,
        k: usize,
        window: Option<(i64, i64)>,
    ) -> Box<dyn Iterator<Item = (EntityRef, f32)>> {
        match window {
            Some((start, end)) => {
                let w = &self.source_graph.window(start, end);
                // let count = g.count_nodes();
                let docs: Vec<_> = self.node_db.top_k(query, k).collect();
                let count = docs
                    .iter()
                    .filter(|(id, _)| w.has_node(w.node_id((*id).into())))
                    .count();
                if count > k {
                    boxed_nodes(docs.into_iter().take(k))
                } else if count == 0 {
                    let filter = w.nodes().iter().map(|node| node.node.index());
                    let docs = self.node_db.top_k_with_filter(query, k, filter);
                    boxed_nodes(docs)
                } else {
                    let docs = self.top_k_nodes_with_window(query, k * 10, window).take(k); // FIXME: avoid hardcoding * 10, do it dinamically
                    Box::new(docs)
                }
            }
            None => {
                let docs = self.node_db.top_k(query, k);
                boxed_nodes(docs) // TODO: do this in other places !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            }
        }
    }

    pub(crate) fn top_k_edges(
        &self,
        query: &Embedding,
        k: usize,
    ) -> impl Iterator<Item = (EntityRef, f32)> {
        let docs = self.edge_db.top_k(query, k);
        docs.map(|(id, score)| (EntityRef::Edge(id), score))
    }

    fn search_top_documents<'a, I>(
        &self,
        document_groups: I,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
    ) -> Vec<(DocumentRef, f32)>
    where
        I: IntoIterator<Item = (&'a EntityId, &'a Vec<DocumentRef>)> + 'a,
    {
        let all_documents = document_groups
            .into_iter()
            .flat_map(|(_, embeddings)| embeddings);

        let window_docs: Box<dyn Iterator<Item = &DocumentRef>> = match window {
            None => Box::new(all_documents),
            Some((start, end)) => {
                let windowed_graph = self.source_graph.window(start, end);
                let filtered = all_documents.filter(move |document| {
                    document.exists_on_window(Some(&windowed_graph), &window)
                });
                Box::new(filtered)
            }
        };

        let scored_docs = score_documents(query, window_docs.cloned()); // TODO: try to remove this clone
        let top_documents = find_top_k(scored_docs, limit);
        top_documents.collect()
    }

    fn search_top_document_groups<'a, I>(
        &self,
        document_groups: I,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
    ) -> Vec<(DocumentRef, f32)>
    where
        I: IntoIterator<Item = (&'a EntityId, &'a Vec<DocumentRef>)> + 'a,
    {
        let window_docs: Box<dyn Iterator<Item = (EntityId, Vec<DocumentRef>)>> = match window {
            None => Box::new(
                document_groups
                    .into_iter()
                    .map(|(id, docs)| (id.clone(), docs.clone())),
            ),
            Some((start, end)) => {
                let windowed_graph = self.source_graph.window(start, end);
                let filtered = document_groups
                    .into_iter()
                    .map(move |(entity_id, docs)| {
                        let filtered_dcos = docs
                            .iter()
                            .filter(|doc| doc.exists_on_window(Some(&windowed_graph), &window))
                            .cloned()
                            .collect_vec();
                        (entity_id.clone(), filtered_dcos)
                    })
                    .filter(|(_, docs)| docs.len() > 0);
                Box::new(filtered)
            }
        };

        let scored_docs = score_document_groups_by_highest(query, window_docs);

        // let scored_docs = score_documents(query, window_docs.cloned()); // TODO: try to remove this clone
        let top_documents = find_top_k(scored_docs, limit);

        top_documents
            .flat_map(|((_, docs), score)| docs.into_iter().map(move |doc| (doc, score)))
            .collect()
    }
}

trait DocIter: Iterator<Item = (EntityRef, f32)> {}
pub(crate) fn merge_docs<A: DocIter, B: DocIter>(a: A, b: B) -> Vec<(EntityRef, f32)> {
    let mut docs = a.chain(b).collect::<Vec<_>>();
    docs.sort_by_key(|(entity, score)| -score);
    docs
}

fn boxed_nodes(
    nodes: impl Iterator<Item = (usize, f32)>,
) -> Box<dyn Iterator<Item = (EntityRef, f32)>> {
    Box::new(nodes.map(|(id, score)| (EntityRef::Node(id), score)))
}
