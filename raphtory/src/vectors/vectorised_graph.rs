use crate::{
    // core::entities::nodes::node_ref::AsNodeRef,
    core::{entities::nodes::node_ref::AsNodeRef, utils::errors::GraphResult},
    db::api::view::{DynamicGraph, IntoDynamic, StaticGraphViewOps},
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
use std::{collections::HashMap, ops::Deref, path::PathBuf, sync::Arc};
use tempfile::TempDir;

use super::{
    similarity_search_utils::score_document_groups_by_highest,
    vector_selection::VectorSelection,
    vectorisable::{vectorise_edge, vectorise_graph, vectorise_node},
    Document,
};

struct VectorDb {
    pub(crate) vectors: ArroyDatabase<Euclidean>, // TODO: review is this safe to clone? does it point to the same thing?
    pub(crate) env: heed::Env,
    pub(crate) tempdir: Arc<TempDir>, // do I really need, is the file open not enough
}

#[derive(Clone)]
pub struct VectorisedGraph<G: StaticGraphViewOps> {
    pub(crate) source_graph: G,
    pub(crate) template: DocumentTemplate,
    pub(crate) embedding: Arc<dyn EmbeddingFunction>,
    pub(crate) cache_storage: Arc<Option<EmbeddingCache>>,
    // it is not the end of the world but we are storing the entity id twice
    pub(crate) vectors: ArroyDatabase<Euclidean>, // TODO: review is this safe to clone? does it point to the same thing?
    pub(crate) env: heed::Env,
    pub(crate) tempdir: Arc<TempDir>, // do I really need, is the file open not enough
    pub(crate) node_ids: Arc<HashMap<u32, String>>,
    pub(crate) edge_ids: Arc<HashMap<u32, (String, String)>>,
    pub(crate) graph_ids: Arc<Vec<u32>>,
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
            vectors: self.vectors,
            env: self.env,
            tempdir: self.tempdir,
            node_ids: self.node_ids,
            edge_ids: self.edge_ids,
            graph_ids: self.graph_ids,
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

    pub async fn update_graph(&self, graph_name: Option<String>) -> GraphResult<()> {
        // let refs = vectorise_graph(
        //     &self.source_graph,
        //     graph_name,
        //     &self.template,
        //     &self.embedding,
        //     self.cache_storage.as_ref(),
        // )
        // .await?;
        // *self.graph_documents.write() = refs;
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
        VectorSelection::new(self.clone())
    }

    /// Return all the graph level documents
    // pub fn get_graph_documents(&self) -> Vec<Document<G>> {
    //     self.graph_documents
    //         .read()
    //         .iter()
    //         .map(|doc| doc.regenerate(&self.source_graph, &self.template))
    //         .collect_vec()
    // }

    /// Search the top scoring documents according to `query` with no more than `limit` documents
    ///
    /// # Arguments
    ///   * query - the embedding to score against
    ///   * limit - the maximum number of documents to search
    ///   * window - the window where documents need to belong to in order to be considered
    ///
    /// # Returns
    ///   The vector selection resulting from the search
    pub fn documents_by_similarity(
        &self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
    ) -> VectorSelection<G> {
        // let node_documents = self.node_documents.read();
        // let edge_documents = self.edge_documents.read();
        // let joined = chain!(node_documents.iter(), edge_documents.iter());
        // let docs = self.search_top_documents(joined, query, limit, window);
        // VectorSelection::new_with_preselection(self.clone(), docs)
        self.empty_selection()
    }

    pub fn documents_by_arroy(
        &self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
    ) -> Vec<String> {
        let rtxn = self.env.read_txn().unwrap();
        let reader = Reader::open(&rtxn, 0, self.vectors).unwrap(); // TODO: review index value, hardcoded to 0
        let result = reader.nns(limit).by_vector(&rtxn, query).unwrap();
        result
            .into_iter()
            .filter_map(|(id, score)| self.node_ids.get(&id).cloned())
            .collect()
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
        // let node_documents = self.node_documents.read();
        // let edge_documents = self.edge_documents.read();
        // let joined = chain!(node_documents.iter(), edge_documents.iter());
        // let docs = self.search_top_document_groups(joined, query, limit, window);
        // VectorSelection::new_with_preselection(self.clone(), docs)
        self.empty_selection()
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
        // let node_documents = self.node_documents.read();
        // let docs = self.search_top_document_groups(node_documents.deref(), query, limit, window);
        // VectorSelection::new_with_preselection(self.clone(), docs)
        self.empty_selection()
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
        // let edge_documents = self.edge_documents.read();
        // let docs = self.search_top_document_groups(edge_documents.deref(), query, limit, window);
        // VectorSelection::new_with_preselection(self.clone(), docs)
        self.empty_selection()
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
