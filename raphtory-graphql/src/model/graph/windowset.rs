use crate::model::graph::{
    edge::Edge, edges::GqlEdges, node::Node, nodes::GqlNodes, path_from_node::GqlPathFromNode,
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::db::{
    api::view::{DynamicGraph, WindowSet},
    graph::{edge::EdgeView, edges::Edges, node::NodeView, nodes::Nodes, path::PathFromNode},
};

#[derive(ResolvedObject)]
pub(crate) struct GqlNodeWindowSet {
    pub(crate) ws: WindowSet<'static, NodeView<DynamicGraph, DynamicGraph>>,
}

impl GqlNodeWindowSet {
    pub(crate) fn new(ws: WindowSet<'static, NodeView<DynamicGraph, DynamicGraph>>) -> Self {
        Self { ws }
    }
}
#[ResolvedObjectFields]
impl GqlNodeWindowSet {
    async fn count(&self) -> usize {
        self.ws.clone().count()
    }

    async fn page(&self, limit: usize, offset: usize) -> Vec<Node> {
        let start = offset * limit;
        self.ws
            .clone()
            .skip(start)
            .take(limit)
            .map(|n| n.into())
            .collect()
    }

    async fn list(&self) -> Vec<Node> {
        self.ws.clone().map(|n| n.into()).collect()
    }
}

#[derive(ResolvedObject)]
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
        self.ws.clone().count()
    }

    async fn page(&self, limit: usize, offset: usize) -> Vec<GqlNodes> {
        let start = offset * limit;
        self.ws
            .clone()
            .skip(start)
            .take(limit)
            .map(|n| GqlNodes::new(n))
            .collect()
    }

    async fn list(&self) -> Vec<GqlNodes> {
        self.ws.clone().map(|n| GqlNodes::new(n)).collect()
    }
}

#[derive(ResolvedObject)]
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
        self.ws.clone().count()
    }

    async fn page(&self, limit: usize, offset: usize) -> Vec<GqlPathFromNode> {
        let start = offset * limit;
        self.ws
            .clone()
            .skip(start)
            .take(limit)
            .map(|n| GqlPathFromNode::new(n))
            .collect()
    }

    async fn list(&self) -> Vec<GqlPathFromNode> {
        self.ws.clone().map(|n| GqlPathFromNode::new(n)).collect()
    }
}

#[derive(ResolvedObject)]
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
        self.ws.clone().count()
    }

    async fn page(&self, limit: usize, offset: usize) -> Vec<Edge> {
        let start = offset * limit;
        self.ws
            .clone()
            .skip(start)
            .take(limit)
            .map(|e| e.into())
            .collect()
    }

    async fn list(&self) -> Vec<Edge> {
        self.ws.clone().map(|e| e.into()).collect()
    }
}

#[derive(ResolvedObject)]
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
        self.ws.clone().count()
    }

    async fn page(&self, limit: usize, offset: usize) -> Vec<GqlEdges> {
        let start = offset * limit;
        self.ws
            .clone()
            .skip(start)
            .take(limit)
            .map(|e| GqlEdges::new(e))
            .collect()
    }

    async fn list(&self) -> Vec<GqlEdges> {
        self.ws.clone().map(|e| GqlEdges::new(e)).collect()
    }
}
