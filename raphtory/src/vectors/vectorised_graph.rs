use crate::{
    core::{entities::nodes::node_ref::AsNodeRef, utils::errors::GraphResult},
    db::api::view::{DynamicGraph, IntoDynamic, StaticGraphViewOps},
    prelude::*,
    vectors::{
        embeddings::compute_embeddings, template::DocumentTemplate, utils::find_top_k, Embedding,
    },
};
use futures_util::StreamExt;
use std::sync::Arc;

use super::{cache::VectorCache, db::EntityDb};
use super::{
    db::{EdgeDb, NodeDb},
    utils::apply_window,
    vector_selection::VectorSelection,
};

#[derive(Clone)]
pub struct VectorisedGraph<G: StaticGraphViewOps> {
    pub(crate) source_graph: G,
    pub(crate) template: DocumentTemplate,
    pub(crate) cache: Arc<VectorCache>,
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
            cache: self.cache,
            node_db: self.node_db,
            edge_db: self.edge_db,
        }
    }
}

impl<G: StaticGraphViewOps> VectorisedGraph<G> {
    pub async fn update_node<T: AsNodeRef>(&self, node: T) -> GraphResult<()> {
        if let Some(node) = self.source_graph.node(node) {
            let id = node.node.index();
            if let Some(doc) = self.template.node(node) {
                let vector = self.compute_embedding(doc).await?;
                self.node_db.0.insert_vector(id, &vector);
            }
        }
        Ok(())
    }

    pub async fn update_edge<T: AsNodeRef>(&self, src: T, dst: T) -> GraphResult<()> {
        if let Some(edge) = self.source_graph.edge(src, dst) {
            let id = edge.edge.pid().0;
            if let Some(doc) = self.template.edge(edge) {
                let vector = self.compute_embedding(doc).await?;
                self.edge_db.0.insert_vector(id, &vector);
            }
        }
        Ok(())
    }

    async fn compute_embedding(&self, doc: String) -> GraphResult<Embedding> {
        let result = compute_embeddings(std::iter::once((0, doc)), &self.cache);
        futures_util::pin_mut!(result);
        Ok(result.next().await.unwrap()?.1)
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
}
