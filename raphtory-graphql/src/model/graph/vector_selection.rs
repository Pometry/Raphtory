use crate::embeddings::EmbedQuery;
use async_graphql::Context;
use dynamic_graphql::{InputObject, ResolvedObject, ResolvedObjectFields};
use raphtory::core::utils::errors::GraphResult;
use raphtory::{db::api::view::MaterializedGraph, vectors::vector_selection::VectorSelection};

use super::document::GqlDocument;
use super::edge::Edge;
use super::node::Node;
use super::vectorised_graph::{IntoWindowTuple, Window};

#[derive(InputObject)]
pub(super) struct InputEdge {
    src: String,
    dst: String,
}

#[derive(ResolvedObject)]
pub(crate) struct GqlVectorSelection(VectorSelection<MaterializedGraph>);

impl From<VectorSelection<MaterializedGraph>> for GqlVectorSelection {
    fn from(value: VectorSelection<MaterializedGraph>) -> Self {
        Self(value)
    }
}

#[ResolvedObjectFields]
impl GqlVectorSelection {
    async fn nodes(&self) -> Vec<Node> {
        self.0.nodes().into_iter().map(|e| e.into()).collect()
    }

    async fn edges(&self) -> Vec<Edge> {
        self.0.edges().into_iter().map(|e| e.into()).collect()
    }

    async fn get_documents(&self) -> Vec<GqlDocument> {
        self.0
            .get_documents_with_scores()
            .into_iter()
            .map(|(doc, score)| GqlDocument {
                content: doc.content,
                entity: doc.entity.into(),
                embedding: doc.embedding.to_vec(),
                score,
            })
            .collect()
    }

    async fn add_nodes(&self, nodes: Vec<String>) -> Self {
        self.apply_and_return(|selection| {
            selection.add_nodes(nodes);
        })
    }

    async fn add_edges(&self, edges: Vec<InputEdge>) -> Self {
        let edges = edges.into_iter().map(|edge| (edge.src, edge.dst)).collect();
        self.apply_and_return(|selection| {
            selection.add_edges(edges);
        })
    }

    async fn expand(&self, hops: usize, window: Option<Window>) -> Self {
        let window = window.into_window_tuple();
        self.apply_and_return(|selection| {
            selection.expand(hops, window);
        })
    }

    async fn expand_entities_by_similarity(
        &self,
        ctx: &Context<'_>,
        query: String,
        limit: usize,
        window: Option<Window>,
    ) -> GraphResult<Self> {
        let vector = ctx.embed_query(query).await?;
        let window = window.into_window_tuple();
        Ok(self.apply_and_return(|selection| {
            selection.expand_entities_by_similarity(&vector, limit, window)
        }))
    }

    async fn expand_nodes_by_similarity(
        &self,
        ctx: &Context<'_>,
        query: String,
        limit: usize,
        window: Option<Window>,
    ) -> GraphResult<Self> {
        let vector = ctx.embed_query(query).await?;
        let window = window.into_window_tuple();
        Ok(self.apply_and_return(|selection| {
            selection.expand_nodes_by_similarity(&vector, limit, window)
        }))
    }

    async fn expand_edges_by_similarity(
        &self,
        ctx: &Context<'_>,
        query: String,
        limit: usize,
        window: Option<Window>,
    ) -> GraphResult<Self> {
        let vector = ctx.embed_query(query).await?;
        let window = window.into_window_tuple();
        Ok(self.apply_and_return(|selection| {
            selection.expand_edges_by_similarity(&vector, limit, window)
        }))
    }
}

impl GqlVectorSelection {
    fn apply_and_return(
        &self,
        mutation: impl FnOnce(&mut VectorSelection<MaterializedGraph>),
    ) -> Self {
        let mut selection: VectorSelection<MaterializedGraph> = self.0.clone();
        mutation(&mut selection);
        selection.into()
    }
}
