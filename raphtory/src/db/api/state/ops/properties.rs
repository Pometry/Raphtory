use crate::{
    db::{
        api::{properties::Properties, state::NodeOp, storage::graph::storage_ops::GraphStorage},
        graph::node::NodeView,
    },
    prelude::GraphViewOps,
};
use raphtory_api::core::entities::VID;

#[derive(Debug, Clone)]
pub struct GetProperties<G> {
    pub(crate) graph: G,
}

impl<'graph, G: GraphViewOps<'graph>> NodeOp for GetProperties<G> {
    type Output = Properties<NodeView<G, G>>;

    fn apply(&self, _storage: &GraphStorage, node: VID) -> Self::Output {
        Properties::new(NodeView::new_internal(self.graph.clone(), node))
    }
}
