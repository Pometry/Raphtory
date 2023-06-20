use crate::db::graph::Graph;
use crate::db::graph_layer::LayeredGraph;
use crate::db::graph_window::WindowedGraph;
use crate::db::subgraph_vertex::VertexSubgraph;
use crate::db::view_api::internal::DynamicGraph;
use crate::db::view_api::GraphViewOps;
use std::sync::Arc;

pub trait IntoDynamic {
    fn into_dynamic(self) -> DynamicGraph;
}

impl IntoDynamic for Graph {
    fn into_dynamic(self) -> DynamicGraph {
        self.as_arc()
    }
}

impl<G: GraphViewOps> IntoDynamic for WindowedGraph<G> {
    fn into_dynamic(self) -> DynamicGraph {
        Arc::new(self)
    }
}

impl<G: GraphViewOps> IntoDynamic for LayeredGraph<G> {
    fn into_dynamic(self) -> DynamicGraph {
        Arc::new(self)
    }
}

impl IntoDynamic for DynamicGraph {
    fn into_dynamic(self) -> DynamicGraph {
        self
    }
}

impl<G: GraphViewOps> IntoDynamic for VertexSubgraph<G> {
    fn into_dynamic(self) -> DynamicGraph {
        Arc::new(self)
    }
}

/// This macro defines a trait `GraphTag` and implements it for each graph view.
#[macro_export]
macro_rules! graph_tag {
    () => {
        pub trait GraphTag:
            $crate::db::view_api::GraphViewOps + $crate::db::view_api::into_dynamic::IntoDynamic
        {
        }

        impl GraphTag for $crate::db::graph::Graph {}
        impl<G: $crate::db::view_api::GraphViewOps> GraphTag
            for $crate::db::graph_layer::LayeredGraph<G>
        {
        }
        impl<G: $crate::db::view_api::GraphViewOps> GraphTag
            for $crate::db::graph_window::WindowedGraph<G>
        {
        }
        impl<G: $crate::db::view_api::GraphViewOps> GraphTag
            for $crate::db::subgraph_vertex::VertexSubgraph<G>
        {
        }
        impl GraphTag for $crate::db::view_api::internal::DynamicGraph {}
    };
}
