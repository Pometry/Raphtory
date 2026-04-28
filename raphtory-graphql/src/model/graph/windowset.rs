use crate::{
    model::graph::{
        collection::{check_list_allowed, check_page_limit},
        edge::GqlEdge,
        edges::GqlEdges,
        graph::GqlGraph,
        node::GqlNode,
        nodes::GqlNodes,
        path_from_node::GqlPathFromNode,
    },
    paths::ExistingGraphFolder,
    rayon::blocking_compute,
};
use async_graphql::Context;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::db::{
    api::{
        state::ops::DynNodeFilter,
        view::{DynamicGraph, WindowSet},
    },
    graph::{
        edge::EdgeView, edges::Edges, node::NodeView, nodes::Nodes, path::PathFromNode,
        views::property_redacted_graph::PropertyRedaction,
    },
};
use std::sync::Arc;

#[derive(ResolvedObject, Clone)]
#[graphql(name = "GraphWindowSet")]
pub(crate) struct GqlGraphWindowSet {
    pub(crate) ws: WindowSet<'static, DynamicGraph>,
    path: ExistingGraphFolder,
    graph_redaction: Arc<PropertyRedaction>,
}

impl GqlGraphWindowSet {
    pub(crate) fn new(ws: WindowSet<'static, DynamicGraph>, path: ExistingGraphFolder) -> Self {
        Self {
            ws,
            path,
            graph_redaction: Default::default(),
        }
    }

    pub(crate) fn with_graph_redaction(mut self, redaction: Arc<PropertyRedaction>) -> Self {
        self.graph_redaction = redaction;
        self
    }
}
#[ResolvedObjectFields]
impl GqlGraphWindowSet {
    /// Returns the number of items.
    async fn count(&self) -> usize {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.ws.clone().count()).await
    }

    /// Fetch one page with a number of items up to a specified limit, optionally offset by a specified amount.
    /// The page_index sets the number of pages to skip (defaults to 0).
    ///
    /// For example, if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
    /// will be returned.
    async fn page(
        &self,
        ctx: &Context<'_>,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> async_graphql::Result<Vec<GqlGraph>> {
        check_page_limit(ctx, limit)?;
        let self_clone = self.clone();
        Ok(blocking_compute(move || {
            let start = page_index.unwrap_or(0) * limit + offset.unwrap_or(0);
            self_clone
                .ws
                .clone()
                .skip(start)
                .take(limit)
                .map(|g| {
                    GqlGraph::new(self_clone.path.clone(), g)
                        .with_graph_redaction(self_clone.graph_redaction.clone())
                })
                .collect()
        })
        .await)
    }

    async fn list(&self, ctx: &Context<'_>) -> async_graphql::Result<Vec<GqlGraph>> {
        check_list_allowed(ctx)?;
        let self_clone = self.clone();
        Ok(blocking_compute(move || {
            self_clone
                .ws
                .clone()
                .map(|g| {
                    GqlGraph::new(self_clone.path.clone(), g)
                        .with_graph_redaction(self_clone.graph_redaction.clone())
                })
                .collect()
        })
        .await)
    }
}

#[derive(ResolvedObject, Clone)]
#[graphql(name = "NodeWindowSet")]
pub(crate) struct GqlNodeWindowSet {
    pub(crate) ws: WindowSet<'static, NodeView<'static, DynamicGraph>>,
}

impl GqlNodeWindowSet {
    pub(crate) fn new(ws: WindowSet<'static, NodeView<'static, DynamicGraph>>) -> Self {
        Self { ws }
    }
}
#[ResolvedObjectFields]
impl GqlNodeWindowSet {
    async fn count(&self) -> usize {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.ws.clone().count()).await
    }

    /// Fetch one page with a number of items up to a specified limit, optionally offset by a specified amount.
    /// The page_index sets the number of pages to skip (defaults to 0).
    ///
    /// For example, if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
    /// will be returned.
    async fn page(
        &self,
        ctx: &Context<'_>,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> async_graphql::Result<Vec<GqlNode>> {
        check_page_limit(ctx, limit)?;
        let self_clone = self.clone();
        Ok(blocking_compute(move || {
            let start = page_index.unwrap_or(0) * limit + offset.unwrap_or(0);
            self_clone
                .ws
                .clone()
                .skip(start)
                .take(limit)
                .map(|n| n.into())
                .collect()
        })
        .await)
    }

    async fn list(&self, ctx: &Context<'_>) -> async_graphql::Result<Vec<GqlNode>> {
        check_list_allowed(ctx)?;
        let self_clone = self.clone();
        Ok(blocking_compute(move || self_clone.ws.clone().map(|n| n.into()).collect()).await)
    }
}

#[derive(ResolvedObject, Clone)]
#[graphql(name = "NodesWindowSet")]
pub(crate) struct GqlNodesWindowSet {
    pub(crate) ws: WindowSet<'static, Nodes<'static, DynamicGraph, DynamicGraph, DynNodeFilter>>,
}

impl GqlNodesWindowSet {
    pub(crate) fn new(
        ws: WindowSet<'static, Nodes<DynamicGraph, DynamicGraph, DynNodeFilter>>,
    ) -> Self {
        Self { ws }
    }
}
#[ResolvedObjectFields]
impl GqlNodesWindowSet {
    async fn count(&self) -> usize {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.ws.clone().count()).await
    }

    /// Fetch one page with a number of items up to a specified limit, optionally offset by a specified amount.
    /// The page_index sets the number of pages to skip (defaults to 0).
    ///
    /// For example, if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
    /// will be returned.
    async fn page(
        &self,
        ctx: &Context<'_>,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> async_graphql::Result<Vec<GqlNodes>> {
        check_page_limit(ctx, limit)?;
        let self_clone = self.clone();
        Ok(blocking_compute(move || {
            let start = page_index.unwrap_or(0) * limit + offset.unwrap_or(0);
            self_clone
                .ws
                .clone()
                .skip(start)
                .take(limit)
                .map(|n| GqlNodes::new(n))
                .collect()
        })
        .await)
    }

    async fn list(&self, ctx: &Context<'_>) -> async_graphql::Result<Vec<GqlNodes>> {
        check_list_allowed(ctx)?;
        let self_clone = self.clone();
        Ok(
            blocking_compute(move || self_clone.ws.clone().map(|n| GqlNodes::new(n)).collect())
                .await,
        )
    }
}

#[derive(ResolvedObject, Clone)]
#[graphql(name = "PathFromNodeWindowSet")]
pub(crate) struct GqlPathFromNodeWindowSet {
    pub(crate) ws: WindowSet<'static, PathFromNode<'static, DynamicGraph>>,
}

impl GqlPathFromNodeWindowSet {
    pub(crate) fn new(ws: WindowSet<'static, PathFromNode<DynamicGraph>>) -> Self {
        Self { ws }
    }
}
#[ResolvedObjectFields]
impl GqlPathFromNodeWindowSet {
    async fn count(&self) -> usize {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.ws.clone().count()).await
    }

    /// Fetch one page with a number of items up to a specified limit, optionally offset by a specified amount.
    /// The page_index sets the number of pages to skip (defaults to 0).
    ///
    /// For example, if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
    /// will be returned.
    async fn page(
        &self,
        ctx: &Context<'_>,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> async_graphql::Result<Vec<GqlPathFromNode>> {
        check_page_limit(ctx, limit)?;
        let self_clone = self.clone();
        Ok(blocking_compute(move || {
            let start = page_index.unwrap_or(0) * limit + offset.unwrap_or(0);
            self_clone
                .ws
                .clone()
                .skip(start)
                .take(limit)
                .map(|n| GqlPathFromNode::new(n))
                .collect()
        })
        .await)
    }

    async fn list(&self, ctx: &Context<'_>) -> async_graphql::Result<Vec<GqlPathFromNode>> {
        check_list_allowed(ctx)?;
        let self_clone = self.clone();
        Ok(blocking_compute(move || {
            self_clone
                .ws
                .clone()
                .map(|n| GqlPathFromNode::new(n))
                .collect()
        })
        .await)
    }
}

#[derive(ResolvedObject, Clone)]
#[graphql(name = "EdgeWindowSet")]
pub(crate) struct GqlEdgeWindowSet {
    pub(crate) ws: WindowSet<'static, EdgeView<DynamicGraph>>,
}

impl GqlEdgeWindowSet {
    pub(crate) fn new(ws: WindowSet<'static, EdgeView<DynamicGraph>>) -> Self {
        Self { ws }
    }
}
#[ResolvedObjectFields]
impl GqlEdgeWindowSet {
    async fn count(&self) -> usize {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.ws.clone().count()).await
    }

    /// Fetch one page with a number of items up to a specified limit, optionally offset by a specified amount.
    /// The page_index sets the number of pages to skip (defaults to 0).
    ///
    /// For example, if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
    /// will be returned.
    async fn page(
        &self,
        ctx: &Context<'_>,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> async_graphql::Result<Vec<GqlEdge>> {
        check_page_limit(ctx, limit)?;
        let self_clone = self.clone();
        Ok(blocking_compute(move || {
            let start = page_index.unwrap_or(0) * limit + offset.unwrap_or(0);
            self_clone
                .ws
                .clone()
                .skip(start)
                .take(limit)
                .map(|e| e.into())
                .collect()
        })
        .await)
    }

    async fn list(&self, ctx: &Context<'_>) -> async_graphql::Result<Vec<GqlEdge>> {
        check_list_allowed(ctx)?;
        let self_clone = self.clone();
        Ok(blocking_compute(move || self_clone.ws.clone().map(|e| e.into()).collect()).await)
    }
}

#[derive(ResolvedObject, Clone)]
#[graphql(name = "EdgesWindowSet")]
pub(crate) struct GqlEdgesWindowSet {
    pub(crate) ws: WindowSet<'static, Edges<'static, DynamicGraph>>,
}

impl GqlEdgesWindowSet {
    pub(crate) fn new(ws: WindowSet<'static, Edges<DynamicGraph>>) -> Self {
        Self { ws }
    }
}
#[ResolvedObjectFields]
impl GqlEdgesWindowSet {
    async fn count(&self) -> usize {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.ws.clone().count()).await
    }

    /// Fetch one page with a number of items up to a specified limit, optionally offset by a specified amount.
    /// The page_index sets the number of pages to skip (defaults to 0).
    ///
    /// For example, if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
    /// will be returned.
    async fn page(
        &self,
        ctx: &Context<'_>,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> async_graphql::Result<Vec<GqlEdges>> {
        check_page_limit(ctx, limit)?;
        let self_clone = self.clone();
        Ok(blocking_compute(move || {
            let start = page_index.unwrap_or(0) * limit + offset.unwrap_or(0);
            self_clone
                .ws
                .clone()
                .skip(start)
                .take(limit)
                .map(|e| GqlEdges::new(e))
                .collect()
        })
        .await)
    }

    async fn list(&self, ctx: &Context<'_>) -> async_graphql::Result<Vec<GqlEdges>> {
        check_list_allowed(ctx)?;
        let self_clone = self.clone();
        Ok(
            blocking_compute(move || self_clone.ws.clone().map(|e| GqlEdges::new(e)).collect())
                .await,
        )
    }
}
