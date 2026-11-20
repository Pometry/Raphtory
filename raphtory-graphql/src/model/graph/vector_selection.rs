use super::{
    document::GqlDocument, edge::GqlEdge, node::GqlNode, node_id::GqlNodeId,
    vectorised_graph::VectorisedGraphWindow,
};
use crate::{
    model::graph::vectorised_graph::IntoWindowTuple,
    rayon::{blocking_compute, ComputeError},
    GQLError,
};
use dynamic_graphql::{InputObject, ResolvedObject, ResolvedObjectFields};
use futures_util::{future::BoxFuture, FutureExt};
use parking_lot::Mutex;
use raphtory::{db::api::view::MaterializedGraph, errors::GraphResult};
use raphtory_vectors::{
    vector_selection::{QueryPair, VectorSelection},
    Embedding, VectorsQuery,
};
use std::sync::Arc;

/// [`blocking_compute`] as the executor raphtory-vectors runs graph access on, which wants the
/// plain result back. A failed job (logged where it was caught) finds no matches, which ends
/// the expansion, and is kept to be returned by [`ComputeExecutor::finish`].
#[derive(Clone, Default)]
struct ComputeExecutor(Arc<Mutex<Option<ComputeError>>>);

impl ComputeExecutor {
    fn executor(
        &self,
    ) -> impl Fn(Box<dyn FnOnce() -> QueryPair + Send + 'static>) -> BoxFuture<'static, QueryPair>
           + Send
           + Sync
           + Clone
           + 'static {
        let failure = self.0.clone();
        move |job| {
            let failure = failure.clone();
            async move {
                blocking_compute(job).await.unwrap_or_else(|error| {
                    failure.lock().get_or_insert(error);
                    (
                        VectorsQuery::resolved(Ok(vec![])),
                        VectorsQuery::resolved(Ok(vec![])),
                    )
                })
            }
            .boxed()
        }
    }

    /// The first job that failed, if any did.
    fn finish(self) -> Result<(), ComputeError> {
        match self.0.lock().take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

#[derive(InputObject)]
pub struct InputEdge {
    /// Source node id (string or non-negative integer).
    src: GqlNodeId,
    /// Destination node id (string or non-negative integer).
    dst: GqlNodeId,
}

/// A working set of documents / nodes / edges built up via similarity
/// searches on a `VectorisedGraph`. Selections are mutable: you can grow
/// them with more hops (`expand*`), dereference the contents (`nodes`,
/// `edges`, `getDocuments`), or start fresh with `emptySelection`.
#[derive(ResolvedObject)]
#[graphql(name = "VectorSelection")]
pub struct GqlVectorSelection(VectorSelection<MaterializedGraph>);

impl From<VectorSelection<MaterializedGraph>> for GqlVectorSelection {
    fn from(value: VectorSelection<MaterializedGraph>) -> Self {
        Self(value)
    }
}

#[ResolvedObjectFields]
impl GqlVectorSelection {
    /// Returns a list of nodes in the current selection.
    pub async fn nodes(&self) -> Vec<GqlNode> {
        self.0.nodes().into_iter().map(|e| e.into()).collect()
    }

    /// Returns a list of edges in the current selection.
    pub async fn edges(&self) -> Vec<GqlEdge> {
        self.0.edges().into_iter().map(|e| e.into()).collect()
    }

    /// Returns a list of documents in the current selection.
    pub async fn get_documents(&self) -> Result<Vec<GqlDocument>, GQLError> {
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

    /// Add every document associated with the named nodes to the selection.
    /// Documents added this way receive a score of 0 (no similarity ranking).
    pub async fn add_nodes(
        &self,
        #[graphql(desc = "Node ids whose documents to include.")] nodes: Vec<GqlNodeId>,
    ) -> Self {
        let mut selection = self.cloned();
        selection.add_nodes(nodes);
        selection.into()
    }

    /// Add every document associated with the named edges to the selection.
    /// Documents added this way receive a score of 0 (no similarity ranking).
    pub async fn add_edges(
        &self,
        #[graphql(desc = "List of `{src, dst}` pairs identifying the edges.")] edges: Vec<
            InputEdge,
        >,
    ) -> Self {
        let mut selection = self.cloned();
        let edges = edges.into_iter().map(|edge| (edge.src, edge.dst)).collect();
        selection.add_edges(edges);
        selection.into()
    }

    /// Grow the selection by including documents that are within `hops` of any
    /// document already in the selection. Two documents are 1 hop apart if
    /// they're on the same entity or on a connected node/edge pair.
    pub async fn expand(
        &self,
        #[graphql(desc = "Number of expansion rounds (1 = direct neighbours).")] hops: usize,
        #[graphql(
            desc = "Optional `{start, end}` to restrict expansion to entities active in that interval."
        )]
        window: Option<VectorisedGraphWindow>,
    ) -> Result<Self, GQLError> {
        let window = window.into_window_tuple();
        let mut selection = self.cloned();
        Ok(blocking_compute(move || {
            selection.expand(hops, window);
            selection.into()
        })
        .await?)
    }

    /// Iteratively expand the selection by similarity to a natural-language
    /// query. Each pass takes the one-hop neighbour set of the current
    /// selection and adds the highest-scoring entities (mixed nodes and
    /// edges); the loop continues until `limit` entities have been added.
    pub async fn expand_entities_by_similarity(
        &self,
        #[graphql(desc = "Natural-language search string; embedded by the server.")] query: String,
        #[graphql(desc = "Total number of entities to add across all passes.")] limit: usize,
        #[graphql(
            desc = "Optional `{start, end}` to restrict matches to entities active in that interval."
        )]
        window: Option<VectorisedGraphWindow>,
    ) -> Result<Self, GQLError> {
        let vector = self.embed_text(query).await?;
        let window = window.into_window_tuple();
        let mut selection = self.cloned();
        let compute = ComputeExecutor::default();
        selection
            .expand_entities_by_similarity(&vector, limit, window, compute.executor())
            .await?;
        compute.finish()?;
        Ok(selection.into())
    }

    /// Like `expandEntitiesBySimilarity` but restricted to nodes — iteratively
    /// add the highest-scoring adjacent nodes to the selection.
    pub async fn expand_nodes_by_similarity(
        &self,
        #[graphql(desc = "Natural-language search string; embedded by the server.")] query: String,
        #[graphql(desc = "Total number of nodes to add across all passes.")] limit: usize,
        #[graphql(
            desc = "Optional `{start, end}` to restrict matches to nodes active in that interval."
        )]
        window: Option<VectorisedGraphWindow>,
    ) -> Result<Self, GQLError> {
        let vector = self.embed_text(query).await?;
        let window = window.into_window_tuple();
        let mut selection = self.cloned();
        let compute = ComputeExecutor::default();
        selection
            .expand_nodes_by_similarity(&vector, limit, window, compute.executor())
            .await?;
        compute.finish()?;
        Ok(selection.into())
    }

    /// Like `expandEntitiesBySimilarity` but restricted to edges — iteratively
    /// add the highest-scoring adjacent edges to the selection.
    pub async fn expand_edges_by_similarity(
        &self,
        #[graphql(desc = "Natural-language search string; embedded by the server.")] query: String,
        #[graphql(desc = "Total number of edges to add across all passes.")] limit: usize,
        #[graphql(
            desc = "Optional `{start, end}` to restrict matches to edges active in that interval."
        )]
        window: Option<VectorisedGraphWindow>,
    ) -> Result<Self, GQLError> {
        let vector = self.embed_text(query).await?;
        let window = window.into_window_tuple();
        let mut selection = self.cloned();
        let compute = ComputeExecutor::default();
        selection
            .expand_edges_by_similarity(&vector, limit, window, compute.executor())
            .await?;
        compute.finish()?;
        Ok(selection.into())
    }
}

impl GqlVectorSelection {
    fn cloned(&self) -> VectorSelection<MaterializedGraph> {
        self.0.clone()
    }

    async fn embed_text(&self, text: String) -> GraphResult<Embedding> {
        Ok(self.0.get_vectorised_graph().embed_text(text).await?)
    }
}
