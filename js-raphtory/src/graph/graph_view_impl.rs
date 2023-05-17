
use raphtory::db::view_api::internal::GraphViewInternalOps;

use super::{Graph, UnderGraph};

impl AsRef<dyn GraphViewInternalOps + Send + Sync + 'static> for Graph {
    fn as_ref(&self) -> &(dyn GraphViewInternalOps + Send + Sync + 'static) {
        match &self.0 {
            UnderGraph::TGraph(g) => g.as_ref(),
            UnderGraph::WindowedGraph(g) => g.as_ref(),
        }
    }
}
