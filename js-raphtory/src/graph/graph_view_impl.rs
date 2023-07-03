use super::{Graph, UnderGraph};
use raphtory::db::api::view::internal::{Base, BoxableGraphView, InheritViewOps};

impl Base for Graph {
    type Base = dyn BoxableGraphView + Send + Sync + 'static;

    fn base(&self) -> &(dyn BoxableGraphView + Send + Sync + 'static) {
        match &self.0 {
            UnderGraph::TGraph(g) => g.as_ref(),
            UnderGraph::WindowedGraph(g) => g.as_ref(),
        }
    }
}

impl InheritViewOps for Graph {}
