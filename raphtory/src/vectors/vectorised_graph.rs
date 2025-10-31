use super::{
    entity_db::{EdgeDb, EntityDb, NodeDb},
    utils::apply_window,
    vector_selection::VectorSelection,
};
use crate::{
    core::entities::nodes::node_ref::AsNodeRef,
    db::api::view::{DynamicGraph, IntoDynamic, StaticGraphViewOps},
    errors::GraphResult,
    prelude::GraphViewOps,
    vectors::{
        cache::CachedEmbeddingModel,
        template::DocumentTemplate,
        utils::find_top_k,
        vector_collection::{lancedb::LanceDbCollection, VectorCollection},
        Embedding,
    },
};

#[derive(Clone)]
pub struct VectorisedGraph<G: StaticGraphViewOps> {
    pub(crate) source_graph: G,
    pub(crate) template: DocumentTemplate,
    pub(crate) model: CachedEmbeddingModel,
    pub(super) node_db: NodeDb<LanceDbCollection>,
    pub(super) edge_db: EdgeDb<LanceDbCollection>,
}

impl<G: StaticGraphViewOps + IntoDynamic> VectorisedGraph<G> {
    pub fn into_dynamic(self) -> VectorisedGraph<DynamicGraph> {
        VectorisedGraph {
            source_graph: self.source_graph.clone().into_dynamic(),
            template: self.template,
            model: self.model,
            node_db: self.node_db,
            edge_db: self.edge_db,
        }
    }
}

impl<G: StaticGraphViewOps> VectorisedGraph<G> {
    /// Generates and stores embeddings for a batch of nodes
    pub async fn update_nodes<T: AsNodeRef>(&self, nodes: Vec<T>) -> GraphResult<()> {
        let (ids, docs): (Vec<_>, Vec<_>) = nodes
            .iter()
            .filter_map(|node| {
                self.source_graph.node(node).and_then(|node| {
                    let id = node.node.index() as u64;
                    self.template.node(node).map(|doc| (id, doc))
                })
            })
            .unzip();
        let vectors = self.model.get_embeddings(docs).await?;
        self.node_db.insert_vectors(ids, vectors).await?;
        Ok(())
    }

    /// Generates and stores embeddings for a batch of edges
    pub async fn update_edges<T: AsNodeRef>(&self, edges: Vec<(T, T)>) -> GraphResult<()> {
        let (ids, docs): (Vec<_>, Vec<_>) = edges
            .iter()
            .filter_map(|(src, dst)| {
                self.source_graph.edge(src, dst).and_then(|edge| {
                    let id = edge.edge.pid().0 as u64;
                    self.template.edge(edge).map(|doc| (id, doc))
                })
            })
            .unzip();
        let vectors = self.model.get_embeddings(docs).await?;
        self.edge_db.insert_vectors(ids, vectors).await?;
        Ok(())
    }

    /// Return an empty selection of documents
    pub fn empty_selection(&self) -> VectorSelection<G> {
        VectorSelection::empty(self.clone())
    }

    /// Search the closest entities to `query` with no more than `limit` entities
    ///
    /// # Arguments
    ///   * query - the embedding to calculate the distance from
    ///   * limit - the maximum number of entities to search
    ///   * window - the window where documents need to belong to in order to be considered
    ///
    /// # Returns
    ///   The vector selection resulting from the search
    pub async fn entities_by_similarity(
        &self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
    ) -> GraphResult<VectorSelection<G>> {
        let view = apply_window(&self.source_graph, window);
        let nodes = self.node_db.top_k(query, limit, view.clone(), None).await?;
        let edges = self.edge_db.top_k(query, limit, view, None).await?;
        let docs = find_top_k(nodes.chain(edges), limit).collect();
        Ok(VectorSelection::new(self.clone(), docs))
    }

    /// Search the closest nodes to `query` with no more than `limit` nodes
    ///
    /// # Arguments
    ///   * query - the embedding to calculate the distance from
    ///   * limit - the maximum number of nodes to search
    ///   * window - the window where documents need to belong to in order to be considered
    ///
    /// # Returns
    ///   The vector selection resulting from the search
    pub async fn nodes_by_similarity(
        &self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
    ) -> GraphResult<VectorSelection<G>> {
        let view = apply_window(&self.source_graph, window);
        let docs = self.node_db.top_k(query, limit, view, None).await?;
        Ok(VectorSelection::new(self.clone(), docs.collect()))
    }

    /// Search the closest edges to `query` with no more than `limit` edges
    ///
    /// # Arguments
    ///   * query - the embedding to calculate the distance from
    ///   * limit - the maximum number of edges to search
    ///   * window - the window where documents need to belong to in order to be considered
    ///
    /// # Returns
    ///   The vector selection resulting from the search
    pub async fn edges_by_similarity(
        &self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
    ) -> GraphResult<VectorSelection<G>> {
        let view = apply_window(&self.source_graph, window);
        let docs = self.edge_db.top_k(query, limit, view, None).await?;
        Ok(VectorSelection::new(self.clone(), docs.collect()))
    }

    /// Returns the embedding for the given text using the embedding model setup for this graph
    pub async fn embed_text<T: Into<String>>(&self, text: T) -> GraphResult<Embedding> {
        self.model.get_single(text.into()).await
    }
}
