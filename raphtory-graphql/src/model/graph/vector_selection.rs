use super::{
    document::GqlDocument,
    edge::GqlEdge,
    node::GqlNode,
    vectorised_graph::{IntoWindowTuple, VectorisedGraphWindow},
};
use crate::rayon::blocking_compute;
use dynamic_graphql::{InputObject, ResolvedObject, ResolvedObjectFields};
use raphtory::{
    db::api::view::MaterializedGraph,
    errors::GraphResult,
    vectors::{vector_selection::VectorSelection, Embedding},
};

#[derive(InputObject)]
pub(super) struct InputEdge {
    /// Source node.
    src: String,
    /// Destination node.
    dst: String,
}

#[derive(ResolvedObject)]
#[graphql(name = "VectorSelection")]
pub(crate) struct GqlVectorSelection(VectorSelection<MaterializedGraph>);

impl From<VectorSelection<MaterializedGraph>> for GqlVectorSelection {
    fn from(value: VectorSelection<MaterializedGraph>) -> Self {
        Self(value)
    }
}

#[ResolvedObjectFields]
impl GqlVectorSelection {
    /// Returns a list of nodes in the current selection.
    async fn nodes(&self) -> Vec<GqlNode> {
        self.0.nodes().into_iter().map(|e| e.into()).collect()
    }

    /// Returns a list of edges in the current selection.
    async fn edges(&self) -> Vec<GqlEdge> {
        self.0.edges().into_iter().map(|e| e.into()).collect()
    }

    /// Returns a list of documents in the current selection.
    async fn get_documents(&self) -> GraphResult<Vec<GqlDocument>> {
        let cloned = self.0.clone();
        let docs = cloned.get_documents_with_distances().await?.into_iter();
        Ok(docs
            .map(|(doc, score)| GqlDocument {
                content: doc.content,
                entity: doc.entity.into(),
                embedding: doc.embedding.to_vec(),
                score,
            })
            .collect())
    }

    /// Adds all the documents associated with the specified nodes to the current selection.
    ///
    /// Documents added by this call are assumed to have a score of 0.
    async fn add_nodes(&self, nodes: Vec<String>) -> Self {
        let mut selection = self.cloned();
        selection.add_nodes(nodes);
        selection.into()
    }

    /// Adds all the documents associated with the specified edges to the current selection.
    ///
    /// Documents added by this call are assumed to have a score of 0.
    async fn add_edges(&self, edges: Vec<InputEdge>) -> Self {
        let mut selection = self.cloned();
        let edges = edges.into_iter().map(|edge| (edge.src, edge.dst)).collect();
        selection.add_edges(edges);
        selection.into()
    }

    /// Add all the documents a specified number of hops away to the selection.
    ///
    /// Two documents A and B are considered to be 1 hop away of each other if they are on the same entity or if they are on the same node and edge pair.
    async fn expand(&self, hops: usize, window: Option<VectorisedGraphWindow>) -> Self {
        let window = window.into_window_tuple();
        let mut selection = self.cloned();
        blocking_compute(move || {
            selection.expand(hops, window);
            selection.into()
        })
        .await
    }

    /// Adds documents, from the set of one hop neighbours to the current selection, to the selection based on their similarity score with the specified query. This function loops so that the set of one hop neighbours expands on each loop and number of documents added is determined by the specified limit.
    async fn expand_entities_by_similarity(
        &self,
        query: String,
        limit: usize,
        window: Option<VectorisedGraphWindow>,
    ) -> GraphResult<Self> {
        let vector = self.embed_text(query).await?;
        let window = window.into_window_tuple();
        let mut selection = self.cloned();
        selection
            .expand_entities_by_similarity(&vector, limit, window)
            .await?;
        Ok(selection.into())
    }

    /// Add the adjacent nodes with higher score for query to the selection up to a specified limit. This function loops like expand_entities_by_similarity but is restricted to nodes.
    async fn expand_nodes_by_similarity(
        &self,
        query: String,
        limit: usize,
        window: Option<VectorisedGraphWindow>,
    ) -> GraphResult<Self> {
        let vector = self.embed_text(query).await?;
        let window = window.into_window_tuple();
        let mut selection = self.cloned();
        selection
            .expand_nodes_by_similarity(&vector, limit, window)
            .await?;
        Ok(selection.into())
    }

    /// Add the adjacent edges with higher score for query to the selection up to a specified limit. This function loops like expand_entities_by_similarity but is restricted to edges.
    async fn expand_edges_by_similarity(
        &self,
        query: String,
        limit: usize,
        window: Option<VectorisedGraphWindow>,
    ) -> GraphResult<Self> {
        let vector = self.embed_text(query).await?;
        let window = window.into_window_tuple();
        let mut selection = self.cloned();
        selection
            .expand_edges_by_similarity(&vector, limit, window)
            .await?;
        Ok(selection.into())
    }
}

impl GqlVectorSelection {
    fn cloned(&self) -> VectorSelection<MaterializedGraph> {
        self.0.clone()
    }

    async fn embed_text(&self, text: String) -> GraphResult<Embedding> {
        self.0.get_vectorised_graph().embed_text(text).await
    }
}
