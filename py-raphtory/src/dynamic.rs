use raphtory::db::graph::Graph;
use raphtory::db::graph_layer::LayeredGraph;
use raphtory::db::graph_window::WindowedGraph;
use raphtory::db::view_api::internal::{BoxableGraphView, WrappedGraph};
use raphtory::db::view_api::GraphViewOps;
use std::sync::Arc;

#[derive(Clone)]
pub struct DynamicGraph(Arc<dyn BoxableGraphView + Send + Sync + 'static>);

pub trait IntoDynamic {
    fn into_dynamic(self) -> DynamicGraph;
}

impl IntoDynamic for Graph {
    fn into_dynamic(self) -> DynamicGraph {
        DynamicGraph(self.as_arc())
    }
}

impl<G: GraphViewOps> IntoDynamic for WindowedGraph<G> {
    fn into_dynamic(self) -> DynamicGraph {
        DynamicGraph(Arc::new(self))
    }
}

impl<G: GraphViewOps> IntoDynamic for LayeredGraph<G> {
    fn into_dynamic(self) -> DynamicGraph {
        DynamicGraph(Arc::new(self))
    }
}

impl IntoDynamic for DynamicGraph {
    fn into_dynamic(self) -> DynamicGraph {
        self
    }
}

impl WrappedGraph for DynamicGraph {
    type Internal = dyn BoxableGraphView + Send + Sync + 'static;
    fn graph(&self) -> &(dyn BoxableGraphView + Send + Sync + 'static) {
        &*self.0
    }
}
