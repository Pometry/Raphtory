use crate::db::{
    api::view::{internal::DynamicGraph, GraphViewOps},
    graph::views::{
        layer_graph::LayeredGraph, vertex_subgraph::VertexSubgraph, window_graph::WindowedGraph,
    },
};
use enum_dispatch::enum_dispatch;

#[enum_dispatch]
pub trait IntoDynamic: 'static {
    fn into_dynamic(self) -> DynamicGraph;
}

impl<G: GraphViewOps + 'static> IntoDynamic for WindowedGraph<G> {
    fn into_dynamic(self) -> DynamicGraph {
        DynamicGraph::new(self)
    }
}

impl<G: GraphViewOps + 'static> IntoDynamic for LayeredGraph<G> {
    fn into_dynamic(self) -> DynamicGraph {
        DynamicGraph::new(self)
    }
}

impl IntoDynamic for DynamicGraph {
    fn into_dynamic(self) -> DynamicGraph {
        self
    }
}

impl<G: GraphViewOps + 'static> IntoDynamic for VertexSubgraph<G> {
    fn into_dynamic(self) -> DynamicGraph {
        DynamicGraph::new(self)
    }
}
