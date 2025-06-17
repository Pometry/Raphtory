use super::{
    document::GqlDocument,
    edge::GqlEdge,
    node::GqlNode,
    vectorised_graph::{IntoWindowTuple, Window},
};
use crate::{embeddings::EmbedQuery, model::blocking};
use async_graphql::Context;
use dynamic_graphql::{InputObject, ResolvedObject, ResolvedObjectFields};
use raphtory::{
    db::api::view::MaterializedGraph, errors::GraphResult,
    vectors::vector_selection::VectorSelection,
};

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
    async fn nodes(&self) -> Vec<GqlNode> {
        self.0.nodes().into_iter().map(|e| e.into()).collect()
    }

    async fn edges(&self) -> Vec<GqlEdge> {
        self.0.edges().into_iter().map(|e| e.into()).collect()
    }

    async fn get_documents(&self) -> GraphResult<Vec<GqlDocument>> {
        let cloned = self.0.clone();
        blocking(move || {
            let docs = cloned.get_documents_with_scores()?.into_iter();
            Ok(docs
                .map(|(doc, score)| GqlDocument {
                    content: doc.content,
                    entity: doc.entity.into(),
                    embedding: doc.embedding.to_vec(),
                    score,
                })
                .collect())
        })
        .await
    }

    async fn add_nodes(&self, nodes: Vec<String>) -> Self {
        let mut selection = self.cloned();
        blocking(move || {
            selection.add_nodes(nodes);
            selection.into()
        })
        .await
    }

    async fn add_edges(&self, edges: Vec<InputEdge>) -> Self {
        let mut selection = self.cloned();
        blocking(move || {
            let edges = edges.into_iter().map(|edge| (edge.src, edge.dst)).collect();
            selection.add_edges(edges);
            selection.into()
        })
        .await
    }

    async fn expand(&self, hops: usize, window: Option<Window>) -> Self {
        let window = window.into_window_tuple();
        let mut selection = self.cloned();
        blocking(move || {
            selection.expand(hops, window);
            selection.into()
        })
        .await
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
        let mut selection = self.cloned();
        blocking(move || {
            selection.expand_entities_by_similarity(&vector, limit, window)?;
            Ok(selection.into())
        })
        .await
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
        let mut selection = self.cloned();
        blocking(move || {
            selection.expand_nodes_by_similarity(&vector, limit, window)?;
            Ok(selection.into())
        })
        .await
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
        let mut selection = self.cloned();
        blocking(move || {
            selection.expand_edges_by_similarity(&vector, limit, window)?;
            Ok(selection.into())
        })
        .await
    }
}

impl GqlVectorSelection {
    fn cloned(&self) -> VectorSelection<MaterializedGraph> {
        self.0.clone()
    }
}
