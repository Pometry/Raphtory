use crate::{
    // core::entities::nodes::node_ref::AsNodeRef,
    db::api::view::{DynamicGraph, StaticGraphViewOps},
    prelude::*,
    vectors::{
        document_ref::DocumentRef,
        document_template::DocumentTemplate,
        embedding_cache::EmbeddingCache,
        entity_id::EntityId,
        similarity_search_utils::{find_top_k, score_documents},
        DocumentOps, Embedding, EmbeddingFunction,
    },
};
use itertools::{chain, Itertools};
use std::{collections::HashMap, path::PathBuf, sync::Arc};

use super::{
    similarity_search_utils::score_document_groups_by_highest, vector_selection::VectorSelection,
};

pub struct VectorisedGraph<G: StaticGraphViewOps, T: DocumentTemplate<G>> {
    pub(crate) source_graph: G,
    pub(crate) template: Arc<T>,
    pub(crate) embedding: Arc<dyn EmbeddingFunction>,
    // it is not the end of the world but we are storing the entity id twice
    pub(crate) graph_documents: Arc<Vec<DocumentRef>>,
    pub(crate) node_documents: Arc<HashMap<EntityId, Vec<DocumentRef>>>, // TODO: replace with FxHashMap
    pub(crate) edge_documents: Arc<HashMap<EntityId, Vec<DocumentRef>>>,
    pub(crate) empty_vec: Vec<DocumentRef>,
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
    ) -> Self {
        Self {
            source_graph: graph,
            template,
            embedding,
            graph_documents,
            node_documents,
            edge_documents,
            empty_vec: vec![],
        }
    }

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

    /// Return an empty selection of documents
    pub fn empty_selection(&self) -> VectorSelection<G, T> {
        VectorSelection::new(self.clone())
    }

    /// Search the top scoring documents according to `query` with no more than `limit` documents
    ///
    /// # Arguments
    ///   * query - the embedding to score against
    ///   * limit - the maximum number of documents to search
    ///   * window - the window where documents need to belong to in order to be considered
    ///
    /// # Returns
    ///   The vector selection resulting from the search
    pub fn search_documents(
        &self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
    ) -> VectorSelection<G, T> {
        let joined = chain!(self.node_documents.iter(), self.edge_documents.iter());
        let docs = self.search_top_documents(joined, query, limit, window);
        VectorSelection::new_with_preselection(self.clone(), docs)
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
    pub fn search_entities(
        &self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
    ) -> VectorSelection<G, T> {
        let joined = chain!(self.node_documents.iter(), self.edge_documents.iter());
        let docs = self.search_top_document_groups(joined, query, limit, window);
        VectorSelection::new_with_preselection(self.clone(), docs)
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
    pub fn search_nodes(
        &self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
    ) -> VectorSelection<G, T> {
        let docs =
            self.search_top_document_groups(self.node_documents.as_ref(), query, limit, window);
        VectorSelection::new_with_preselection(self.clone(), docs)
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
    pub fn search_edges(
        &self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
    ) -> VectorSelection<G, T> {
        let docs =
            self.search_top_document_groups(self.edge_documents.as_ref(), query, limit, window);
        VectorSelection::new_with_preselection(self.clone(), docs)
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
                // TODO: filter empty vectors here? what happens if the user inputs an empty list as the doc prop
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
