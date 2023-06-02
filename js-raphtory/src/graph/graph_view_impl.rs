use raphtory::db::view_api::internal::{GraphViewInternalOps, WrappedGraph};

use super::{Graph, UnderGraph};

impl WrappedGraph for Graph {
    type Internal = dyn GraphViewInternalOps + Send + Sync + 'static;

    fn as_graph(&self) -> &(dyn GraphViewInternalOps + Send + Sync + 'static) {
        match &self.0 {
            UnderGraph::TGraph(g) => g.as_ref(),
            UnderGraph::WindowedGraph(g) => g.as_ref(),
        }
    }
}
