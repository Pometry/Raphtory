use crate::{
    model::graph::{
        edge::GqlEdge, edges::GqlEdges, graph::GqlGraph, node::GqlNode, nodes::GqlNodes,
        path_from_node::GqlPathFromNode,
    },
    paths::ExistingGraphFolder,
    rayon::blocking_compute,
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::db::{
    api::view::{DynamicGraph, WindowSet},
    graph::{edge::EdgeView, edges::Edges, node::NodeView, nodes::Nodes, path::PathFromNode},
};

#[derive(ResolvedObject, Clone)]
#[graphql(name = "GraphWindowSet")]
pub(crate) struct GqlGraphWindowSet {
    pub(crate) ws: WindowSet<'static, DynamicGraph>,
    path: ExistingGraphFolder,
}

impl GqlGraphWindowSet {
    pub(crate) fn new(ws: WindowSet<'static, DynamicGraph>, path: ExistingGraphFolder) -> Self {
        Self { ws, path }
    }
}
#[ResolvedObjectFields]
impl GqlGraphWindowSet {
    async fn count(&self) -> usize {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.ws.clone().count()).await
    }

    /// Fetch one "page" of items, optionally offset by a specified amount.
    ///
    /// * `limit` - The size of the page (number of items to fetch).
    /// * `offset` - The number of items to skip (defaults to 0).
    /// * `page_index` - The number of pages (of size `limit`) to skip (defaults to 0).
    ///
    /// e.g. if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
    /// will be returned.
    async fn page(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Vec<GqlGraph> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let start = page_index.unwrap_or(0) * limit + offset.unwrap_or(0);
            self_clone
                .ws
                .clone()
                .skip(start)
                .take(limit)
                .map(|g| GqlGraph::new(self_clone.path.clone(), g))
                .collect()
        })
        .await
    }

    async fn list(&self) -> Vec<GqlGraph> {
        let self_clone = self.clone();
        blocking_compute(move || {
            self_clone
                .ws
                .clone()
                .map(|g| GqlGraph::new(self_clone.path.clone(), g))
                .collect()
        })
        .await
    }
}

#[derive(ResolvedObject, Clone)]
#[graphql(name = "NodeWindowSet")]
pub(crate) struct GqlNodeWindowSet {
    pub(crate) ws: WindowSet<'static, NodeView<'static, DynamicGraph, DynamicGraph>>,
}

impl GqlNodeWindowSet {
    pub(crate) fn new(
        ws: WindowSet<'static, NodeView<'static, DynamicGraph, DynamicGraph>>,
    ) -> Self {
        Self { ws }
    }
}
#[ResolvedObjectFields]
impl GqlNodeWindowSet {
    async fn count(&self) -> usize {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.ws.clone().count()).await
    }

    /// Fetch one "page" of items, optionally offset by a specified amount.
    ///
    /// * `limit` - The size of the page (number of items to fetch).
    /// * `offset` - The number of items to skip (defaults to 0).
    /// * `page_index` - The number of pages (of size `limit`) to skip (defaults to 0).
    ///
    /// e.g. if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
    /// will be returned.
    async fn page(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Vec<GqlNode> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let start = page_index.unwrap_or(0) * limit + offset.unwrap_or(0);
            self_clone
                .ws
                .clone()
                .skip(start)
                .take(limit)
                .map(|n| n.into())
                .collect()
        })
        .await
    }

    async fn list(&self) -> Vec<GqlNode> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.ws.clone().map(|n| n.into()).collect()).await
    }
}

#[derive(ResolvedObject, Clone)]
#[graphql(name = "NodesWindowSet")]
pub(crate) struct GqlNodesWindowSet {
    pub(crate) ws: WindowSet<'static, Nodes<'static, DynamicGraph, DynamicGraph>>,
}

impl GqlNodesWindowSet {
    pub(crate) fn new(ws: WindowSet<'static, Nodes<DynamicGraph, DynamicGraph>>) -> Self {
        Self { ws }
    }
}
#[ResolvedObjectFields]
impl GqlNodesWindowSet {
    async fn count(&self) -> usize {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.ws.clone().count()).await
    }

    /// Fetch one "page" of items, optionally offset by a specified amount.
    ///
    /// * `limit` - The size of the page (number of items to fetch).
    /// * `offset` - The number of items to skip (defaults to 0).
    /// * `page_index` - The number of pages (of size `limit`) to skip (defaults to 0).
    ///
    /// e.g. if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
    /// will be returned.
    async fn page(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Vec<GqlNodes> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let start = page_index.unwrap_or(0) * limit + offset.unwrap_or(0);
            self_clone
                .ws
                .clone()
                .skip(start)
                .take(limit)
                .map(|n| GqlNodes::new(n))
                .collect()
        })
        .await
    }

    async fn list(&self) -> Vec<GqlNodes> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.ws.clone().map(|n| GqlNodes::new(n)).collect()).await
    }
}

#[derive(ResolvedObject, Clone)]
#[graphql(name = "PathFromNodeWindowSet")]
pub(crate) struct GqlPathFromNodeWindowSet {
    pub(crate) ws: WindowSet<'static, PathFromNode<'static, DynamicGraph, DynamicGraph>>,
}

impl GqlPathFromNodeWindowSet {
    pub(crate) fn new(ws: WindowSet<'static, PathFromNode<DynamicGraph, DynamicGraph>>) -> Self {
        Self { ws }
    }
}
#[ResolvedObjectFields]
impl GqlPathFromNodeWindowSet {
    async fn count(&self) -> usize {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.ws.clone().count()).await
    }

    /// Fetch one "page" of items, optionally offset by a specified amount.
    ///
    /// * `limit` - The size of the page (number of items to fetch).
    /// * `offset` - The number of items to skip (defaults to 0).
    /// * `page_index` - The number of pages (of size `limit`) to skip (defaults to 0).
    ///
    /// e.g. if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
    /// will be returned.
    async fn page(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Vec<GqlPathFromNode> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let start = page_index.unwrap_or(0) * limit + offset.unwrap_or(0);
            self_clone
                .ws
                .clone()
                .skip(start)
                .take(limit)
                .map(|n| GqlPathFromNode::new(n))
                .collect()
        })
        .await
    }

    async fn list(&self) -> Vec<GqlPathFromNode> {
        let self_clone = self.clone();
        blocking_compute(move || {
            self_clone
                .ws
                .clone()
                .map(|n| GqlPathFromNode::new(n))
                .collect()
        })
        .await
    }
}

#[derive(ResolvedObject, Clone)]
#[graphql(name = "EdgeWindowSet")]
pub(crate) struct GqlEdgeWindowSet {
    pub(crate) ws: WindowSet<'static, EdgeView<DynamicGraph, DynamicGraph>>,
}

impl GqlEdgeWindowSet {
    pub(crate) fn new(ws: WindowSet<'static, EdgeView<DynamicGraph, DynamicGraph>>) -> Self {
        Self { ws }
    }
}
#[ResolvedObjectFields]
impl GqlEdgeWindowSet {
    async fn count(&self) -> usize {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.ws.clone().count()).await
    }

    /// Fetch one "page" of items, optionally offset by a specified amount.
    ///
    /// * `limit` - The size of the page (number of items to fetch).
    /// * `offset` - The number of items to skip (defaults to 0).
    /// * `page_index` - The number of pages (of size `limit`) to skip (defaults to 0).
    ///
    /// e.g. if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
    /// will be returned.
    async fn page(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Vec<GqlEdge> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let start = page_index.unwrap_or(0) * limit + offset.unwrap_or(0);
            self_clone
                .ws
                .clone()
                .skip(start)
                .take(limit)
                .map(|e| e.into())
                .collect()
        })
        .await
    }

    async fn list(&self) -> Vec<GqlEdge> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.ws.clone().map(|e| e.into()).collect()).await
    }
}

#[derive(ResolvedObject, Clone)]
#[graphql(name = "EdgesWindowSet")]
pub(crate) struct GqlEdgesWindowSet {
    pub(crate) ws: WindowSet<'static, Edges<'static, DynamicGraph, DynamicGraph>>,
}

impl GqlEdgesWindowSet {
    pub(crate) fn new(ws: WindowSet<'static, Edges<DynamicGraph, DynamicGraph>>) -> Self {
        Self { ws }
    }
}
#[ResolvedObjectFields]
impl GqlEdgesWindowSet {
    async fn count(&self) -> usize {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.ws.clone().count()).await
    }

    /// Fetch one "page" of items, optionally offset by a specified amount.
    ///
    /// * `limit` - The size of the page (number of items to fetch).
    /// * `offset` - The number of items to skip (defaults to 0).
    /// * `page_index` - The number of pages (of size `limit`) to skip (defaults to 0).
    ///
    /// e.g. if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
    /// will be returned.
    async fn page(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Vec<GqlEdges> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let start = page_index.unwrap_or(0) * limit + offset.unwrap_or(0);
            self_clone
                .ws
                .clone()
                .skip(start)
                .take(limit)
                .map(|e| GqlEdges::new(e))
                .collect()
        })
        .await
    }

    async fn list(&self) -> Vec<GqlEdges> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.ws.clone().map(|e| GqlEdges::new(e)).collect()).await
    }
}
