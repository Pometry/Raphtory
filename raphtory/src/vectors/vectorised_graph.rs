use crate::{
    // core::entities::nodes::node_ref::AsNodeRef,
    core::{entities::nodes::node_ref::AsNodeRef, utils::errors::GraphResult},
    db::{
        api::{
            storage::graph::{
                edges::edge_storage_ops::EdgeStorageOps, nodes::node_storage_ops::NodeStorageOps,
            },
            view::{
                internal::CoreGraphOps, BaseNodeViewOps, DynamicGraph, IntoDynamic,
                StaticGraphViewOps,
            },
        },
        graph::{edge::EdgeView, node::NodeView, views::window_graph::WindowedGraph},
    },
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
    similarity_search_utils::{apply_window, score_document_groups_by_highest},
    vector_selection::VectorSelection,
    vectorisable::{vectorise_edge, vectorise_graph, vectorise_node},
    Document,
};

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum EntityRef {
    Node(usize), // TODO: store this as a u32
    Edge(usize),
}

impl EntityRef {
    // TODO: there are some places where I should be using this and Im not
    pub(crate) fn as_node_view<G: StaticGraphViewOps>(&self, view: &G) -> Option<NodeView<G>> {
        let node = self.as_node(view)?;
        view.node(node)
    }

    // TODO: there are some places where I should be using this and Im not
    pub(crate) fn as_edge_view<G: StaticGraphViewOps>(&self, view: &G) -> Option<EdgeView<G>> {
        let (src, dst) = self.as_edge(view)?;
        view.edge(src, dst)
    }

    pub(crate) fn as_node<G: StaticGraphViewOps>(&self, view: &G) -> Option<GID> {
        if let EntityRef::Node(id) = self {
            Some(view.node_id(id.into()))
        } else {
            None
        }
    }

    pub(crate) fn as_edge<G: StaticGraphViewOps>(&self, view: &G) -> Option<(GID, GID)> {
        if let EntityRef::Edge(id) = self {
            let edge = view.core_edge((*id).into());
            let src = view.node_id(edge.src());
            let dst = view.node_id(edge.dst());
            Some((src, dst))
        } else {
            None
        }
    }

    fn as_usize(&self) -> usize {
        match self {
            EntityRef::Node(id) => *id,
            EntityRef::Edge(id) => *id,
        }
    }

    fn as_u32(&self) -> usize {
        match self {
            EntityRef::Node(id) => *id as u32,
            EntityRef::Edge(id) => *id as u32,
        }
    }

    // pub(crate) fn as_node(&self) -> Option<usize> {
    //     if let EntityRef::Node(id) = self {
    //         Some(id)
    //     } else {
    //         None
    //     }
    // }

    // pub(crate) fn as_edge(&self) -> Option<usize> {
    //     if let EntityRef::Edge(id) = self {
    //         Some(id)
    //     } else {
    //         None
    //     }
    // }
}

#[derive(Clone)]
pub(crate) struct VectorDb {
    // FIXME: save index value in here !!!!!!!!!!!!!!!!!!!
    pub(crate) vectors: ArroyDatabase<Euclidean>, // TODO: review is this safe to clone? does it point to the same thing?
    pub(crate) env: heed::Env,
    pub(crate) tempdir: Arc<TempDir>, // do I really need, is the file open not enough
}

struct NodeDb {
    db: VectorDb,
}

impl VectorSearch for NodeDb {
    fn get_db(&self) -> &VectorDb {
        &self.db
    }

    fn into_entity_ref(id: u32) -> EntityRef {
        EntityRef::Node(id as usize)
    }

    fn view_has_entity<G: StaticGraphViewOps>(entity: &EntityRef, view: &G) -> bool {
        view.has_node(entity.as_node(view))
    }

    fn all_valid_entities<G: StaticGraphViewOps>(view: G) -> impl Iterator<Item = EntityRef> {
        view.nodes()
            .iter()
            .map(|node| view.core_nodes().node_entry(node.id()).vid())
    }
}

struct EdgeDb {
    db: VectorDb,
}

impl VectorSearch for EdgeDb {
    fn get_db(&self) -> &VectorDb {
        &self.db
    }

    fn into_entity_ref(id: u32) -> EntityRef {
        EntityRef::Edge(id as usize)
    }

    fn view_has_entity<G: StaticGraphViewOps>(entity: &EntityRef, view: &G) -> bool {
        let (src, dst) = entity.as_edge(view).unwrap(); // TODO: remove this?
        view.has_edge(src, dst)
    }

    fn all_valid_entities<G: StaticGraphViewOps>(view: G) -> impl Iterator<Item = EntityRef> {
        view.edges()
            .iter()
            .map(|node| view.core_nodes().node_entry(node.id()).vid())
    }
}

// TODO: rename this to GraphVectorSearch
pub(super) trait VectorSearch {
    fn get_db(&self) -> &VectorDb;
    fn into_entity_ref(id: u32) -> EntityRef;
    fn view_has_entity<G: StaticGraphViewOps>(entity: &EntityRef, view: &G) -> bool;
    fn all_valid_entities<G: StaticGraphViewOps>(view: G) -> impl Iterator<Item = u32>;

    fn top_k<G: StaticGraphViewOps>(
        &self,
        query: &Embedding,
        k: usize,
        view: Option<G>,
        filter: Option<HashSet<EntityRef>>,
    ) -> Box<dyn Iterator<Item = (EntityRef, f32)>> {
        let docs: Vec<_> = self
            .top_k_with_candidates(query, k, None::<std::iter::Empty<u32>>)
            .collect();
        let count = docs
            .iter()
            .filter(|(id, _)| {
                let valid_for_view = view.is_none_or(|view| Self::view_has_entity(id, &view));
                filter.is_none_or(|filter| filter.contains(&id))
            })
            .count();

        // can do some of the things below in parallel ?!?!?
        if count > k {
            Box::new(docs.into_iter().take(k))
        } else if count == 0 {
            let candidates = match filter {
                Some(filter) => filter
                    .iter()
                    .filter(|entity| Self::view_has_entity(entity, &view)),
                None => Self::all_valid_entities(view.unwrap()), // TODO: remove this unwrap
            };
            let docs = self.top_k_with_candidates(query, k, Some(candidates));
            Box::new(docs)
        } else {
            let docs = self.top_k(query, k * 10, view, filter).take(k); // FIXME: avoid hardcoding * 10, do it dinamically
            Box::new(docs)
        }
    }

    fn top_k_with_candidates(
        &self,
        query: &Embedding,
        k: usize,
        candidates: Option<impl Iterator<Item = u32>>,
    ) -> impl Iterator<Item = (EntityRef, f32)> {
        let db = self.get_db();
        let rtxn = db.env.read_txn().unwrap();
        let reader = Reader::open(&rtxn, 0, db.vectors).unwrap(); // TODO: review index value, hardcoded to 0
        let mut query_builder = reader.nns(k);
        let candidates =
            candidates.map(|filter| roaring::RoaringBitmap::from_iter(filter.map(|id| id as u32)));
        let query_builder = if let Some(filter) = &candidates {
            query_builder.candidates(filter)
        } else {
            &query_builder
        };
        query_builder
            .by_vector(&rtxn, query.as_ref())
            .unwrap()
            .into_iter()
            .map(|(id, score)| (Self::into_entity_ref(id), score))
    }

    fn get_id(&self, id: usize) -> Embedding {
        // FIXME: remove unsafe code, ask arroy mantainers to make the type public
        let db = self.get_db();
        let rtxn = db.env.read_txn().unwrap();
        let key: Key = id.into();
        let arroy_key = unsafe { std::mem::transmute(&key) };
        let node = db.vectors.get(&rtxn, arroy_key).unwrap().unwrap();
        let vector = node.leaf().unwrap().vector.to_vec().into();
        vector
    }
}

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
    pub(crate) node_db: NodeDb,
    pub(crate) edge_db: EdgeDb,
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
        let view = apply_window(&self.source_graph, window);
        let nodes = self.node_db.top_k(query, limit, view.clone(), None); // TODO: avoid this clone
        let edges = self.edge_db.top_k(query, limit, view, None);
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
        let view = apply_window(&self.source_graph, window);
        let docs = self.node_db.top_k(query, limit, view, None);
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
        let view = apply_window(&self.source_graph, window);
        let docs = self.edge_db.top_k(query, limit, view, None);
        VectorSelection::new(self.clone(), docs.collect())
    }

    // pub(crate) fn top_k_nodes_with_window(
    //     &self,
    //     query: &Embedding,
    //     k: usize,
    //     window: Option<(i64, i64)>,
    // ) -> Box<dyn Iterator<Item = (EntityRef, f32)>> {
    //     match window {
    //         Some((start, end)) => {
    //             let w = &self.source_graph.window(start, end);
    //             // let count = g.count_nodes();
    //             let docs: Vec<_> = self.node_db.top_k(query, k).collect();
    //             let count = docs
    //                 .iter()
    //                 .filter(|(id, _)| w.has_node(w.node_id((*id).into())))
    //                 .count();
    //             if count > k {
    //                 boxed_nodes(docs.into_iter().take(k))
    //             } else if count == 0 {
    //                 let filter = w.nodes().iter().map(|node| node.node.index());
    //                 let docs = self.node_db.top_k_with_filter(query, k, filter);
    //                 boxed_nodes(docs)
    //             } else {
    //                 let docs = self.top_k_nodes_with_window(query, k * 10, window).take(k); // FIXME: avoid hardcoding * 10, do it dinamically
    //                 Box::new(docs)
    //             }
    //         }
    //         None => {
    //             let docs = self.node_db.top_k(query, k);
    //             boxed_nodes(docs) // TODO: do this in other places !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    //         }
    //     }
    // }
}

trait DocIter: Iterator<Item = (EntityRef, f32)> {}
pub(crate) fn merge_docs<A: DocIter, B: DocIter>(a: A, b: B) -> Vec<(EntityRef, f32)> {
    let mut docs = a.chain(b).collect::<Vec<_>>();
    docs.sort_by_key(|(entity, score)| -score);
    docs
}
