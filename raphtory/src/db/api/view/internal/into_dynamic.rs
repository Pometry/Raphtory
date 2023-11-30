use crate::db::{
    api::view::{internal::DynamicGraph, StaticGraphViewOps},
    graph::views::{
        layer_graph::LayeredGraph, vertex_subgraph::VertexSubgraph, window_graph::WindowedGraph,
    },
};
use enum_dispatch::enum_dispatch;

#[enum_dispatch]
pub trait IntoDynamic: 'static {
    fn into_dynamic(self) -> DynamicGraph;
}

impl<G: StaticGraphViewOps> IntoDynamic for WindowedGraph<G> {
    fn into_dynamic(self) -> DynamicGraph {
        DynamicGraph::new(self)
    }
}

impl<G: StaticGraphViewOps> IntoDynamic for LayeredGraph<G> {
    fn into_dynamic(self) -> DynamicGraph {
        DynamicGraph::new(self)
    }
}

impl IntoDynamic for DynamicGraph {
    fn into_dynamic(self) -> DynamicGraph {
        self
    }
}

impl<G: StaticGraphViewOps> IntoDynamic for VertexSubgraph<G> {
    fn into_dynamic(self) -> DynamicGraph {
        DynamicGraph::new(self)
    }
}
