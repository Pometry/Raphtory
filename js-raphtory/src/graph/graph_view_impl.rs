use super::{Graph, UnderGraph};
use raphtory::db::api::view::{Base, BoxableGraphView, InheritViewOps};

impl Base for Graph {
    type Base = dyn BoxableGraphView<'static> + Send + Sync + 'static;

    fn base(&self) -> &(dyn BoxableGraphView<'static> + Send + Sync + 'static) {
        match &self.0 {
            UnderGraph::TGraph(g) => g.as_ref(),
            UnderGraph::WindowedGraph(g) => g.as_ref(),
        }
    }
}

impl InheritViewOps for Graph {}
