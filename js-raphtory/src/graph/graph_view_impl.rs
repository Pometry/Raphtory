use raphtory::db::view_api::internal::{BoxableGraphView, WrappedGraph};

use super::{Graph, UnderGraph};

impl WrappedGraph for Graph {
    type Internal = dyn BoxableGraphView + Send + Sync + 'static;

    fn graph(&self) -> &(dyn BoxableGraphView + Send + Sync + 'static) {
        match &self.0 {
            UnderGraph::TGraph(g) => g.as_ref(),
            UnderGraph::WindowedGraph(g) => g.as_ref(),
        }
    }
}
