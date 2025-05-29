use crate::{
    core::{entities::nodes::node_ref::AsNodeRef, utils::errors::GraphResult},
    db::api::view::{DynamicGraph, IntoDynamic, StaticGraphViewOps},
    vectors::{template::DocumentTemplate, utils::find_top_k, Embedding},
};

use super::{
    cache::VectorCache,
    db::{EdgeDb, EntityDb, NodeDb},
    utils::apply_window,
    vector_selection::VectorSelection,
};

#[derive(Clone)]
pub struct VectorisedGraph<G: StaticGraphViewOps> {
    pub(crate) source_graph: G,
    pub(crate) template: DocumentTemplate,
    pub(crate) cache: VectorCache,
    pub(super) node_db: NodeDb,
    pub(super) edge_db: EdgeDb,
}

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
                let vector = self.cache.get_single(doc).await?;
                self.node_db.insert_vector(id, &vector)?;
            }
        }
        Ok(())
    }

    pub async fn update_edge<T: AsNodeRef>(&self, src: T, dst: T) -> GraphResult<()> {
        if let Some(edge) = self.source_graph.edge(src, dst) {
            let id = edge.edge.pid().0;
            if let Some(doc) = self.template.edge(edge) {
                let vector = self.cache.get_single(doc).await?;
                self.edge_db.insert_vector(id, &vector)?;
            }
        }
        Ok(())
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
    ) -> GraphResult<VectorSelection<G>> {
        let view = apply_window(&self.source_graph, window);
        let nodes = self.node_db.top_k(query, limit, view.clone(), None)?;
        let edges = self.edge_db.top_k(query, limit, view, None)?;
        let docs = find_top_k(nodes.chain(edges), limit).collect();
        Ok(VectorSelection::new(self.clone(), docs))
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
    ) -> GraphResult<VectorSelection<G>> {
        let view = apply_window(&self.source_graph, window);
        let docs = self.node_db.top_k(query, limit, view, None)?;
        Ok(VectorSelection::new(self.clone(), docs.collect()))
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
    ) -> GraphResult<VectorSelection<G>> {
        let view = apply_window(&self.source_graph, window);
        let docs = self.edge_db.top_k(query, limit, view, None)?;
        Ok(VectorSelection::new(self.clone(), docs.collect()))
    }
}
