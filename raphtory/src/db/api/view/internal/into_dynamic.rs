use crate::db::{
    api::view::{internal::DynamicGraph, GraphViewOps},
    graph::views::{
        layer_graph::LayeredGraph, vertex_subgraph::VertexSubgraph, window_graph::WindowedGraph,
    },
};

pub trait IntoDynamic {
    fn into_dynamic(self) -> DynamicGraph;
}

impl<G: GraphViewOps> IntoDynamic for WindowedGraph<G> {
    fn into_dynamic(self) -> DynamicGraph {
        DynamicGraph::new(self)
    }
}

impl<G: GraphViewOps> IntoDynamic for LayeredGraph<G> {
    fn into_dynamic(self) -> DynamicGraph {
        DynamicGraph::new(self)
    }
}

impl IntoDynamic for DynamicGraph {
    fn into_dynamic(self) -> DynamicGraph {
        self
    }
}

impl<G: GraphViewOps> IntoDynamic for VertexSubgraph<G> {
    fn into_dynamic(self) -> DynamicGraph {
        DynamicGraph::new(self)
    }
}
