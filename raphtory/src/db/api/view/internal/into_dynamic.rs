use crate::db::api::view::internal::DynamicGraph;
use crate::db::api::view::GraphViewOps;
use crate::db::graph::views::graph_window::WindowedGraph;
use crate::db::graph::views::layer_graph::LayeredGraph;
use crate::db::graph::views::subgraph_vertex::VertexSubgraph;

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

/// This macro defines a trait `GraphTag` and implements it for each graph view.
#[macro_export]
macro_rules! graph_tag {
    () => {
        pub trait GraphTag:
            $crate::db::view::GraphViewOps + $crate::db::view::into_dynamic::IntoDynamic
        {
        }

        impl GraphTag for $crate::db::graph::Graph {}
        impl<G: $crate::db::view::GraphViewOps> GraphTag
            for $crate::db::graph_layer::LayeredGraph<G>
        {
        }
        impl<G: $crate::db::view::GraphViewOps> GraphTag
            for $crate::db::graph_window::WindowedGraph<G>
        {
        }
        impl<G: $crate::db::view::GraphViewOps> GraphTag
            for $crate::db::subgraph_vertex::VertexSubgraph<G>
        {
        }
        impl GraphTag for $crate::db::view::internal::DynamicGraph {}
    };
}
