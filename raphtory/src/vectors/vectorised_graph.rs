use super::{
    cache::VectorCache,
    db::{EdgeDb, EntityDb, NodeDb},
    utils::apply_window,
    vector_selection::VectorSelection,
};
use crate::{
    core::entities::nodes::node_ref::AsNodeRef,
    db::api::view::{DynamicGraph, IntoDynamic, StaticGraphViewOps},
    errors::GraphResult,
    prelude::GraphViewOps,
    vectors::{template::DocumentTemplate, utils::find_top_k, Embedding},
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
    /// Generates and stores embeddings for a batch of nodes
    pub async fn update_nodes<T: AsNodeRef>(&self, nodes: Vec<T>) -> GraphResult<()> {
        let (ids, docs): (Vec<_>, Vec<_>) = nodes.iter()
            .filter_map(|node| {
                self.source_graph.node(node)
                    .and_then(|node| {
                        let id = node.node.index();

                        self.template.node(node).map(|doc| (id, doc))
                    })
            })
            .unzip();

        let vectors = self.cache.get_embeddings(docs).await?;

        self.node_db
            .insert_vectors(ids.iter().zip(vectors).map(|(id, vector)| (*id, vector)).collect())?;

        Ok(())
    }

    /// Generates and stores embeddings for a batch of edges
    pub async fn update_edges<T: AsNodeRef>(&self, edges: Vec<(T, T)>) -> GraphResult<()> {
        let (ids, docs): (Vec<_>, Vec<_>) = edges.iter()
            .filter_map(|(src, dst)| {
                self.source_graph.edge(src, dst)
                    .and_then(|edge| {
                        let id = edge.edge.pid().0;

                        self.template.edge(edge).map(|doc| (id, doc))
                    })
            })
            .unzip();

        let vectors = self.cache.get_embeddings(docs).await?;

        self.edge_db
            .insert_vectors(ids.iter().zip(vectors).map(|(id, vector)| (*id, vector)).collect())?;

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
