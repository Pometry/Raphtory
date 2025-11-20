pub mod filter;
pub mod history;
pub mod node;
mod properties;

pub use history::*;
pub use node::*;
pub use properties::*;
use raphtory_api::core::Direction;
use raphtory_api::core::entities::VID;
use raphtory_storage::graph::graph::GraphStorage;
use raphtory_storage::graph::nodes::node_storage_ops::NodeStorageOps;
use raphtory_storage::layer_ops::InternalLayerOps;
use std::sync::Arc;
use std::ops::Deref;
use crate::db::api::view::internal::filtered_node::FilteredNodeStorageOps;
use crate::db::api::view::internal::{FilterOps, FilterState, GraphView};

#[derive(Clone)]
pub struct NotANodeFilter;

impl NodeOp for NotANodeFilter {
    type Output = bool;

    fn apply(&self, _storage: &GraphStorage, _node: VID) -> Self::Output {
        panic!("Not a node filter")
    }
}

#[derive(Debug, Clone)]
pub struct Degree<G> {
    pub(crate) dir: Direction,
    pub(crate) view: G,
}

impl<G: GraphView> NodeOp for Degree<G> {
    type Output = usize;

    fn apply(&self, storage: &GraphStorage, node: VID) -> usize {
        let node = storage.core_node(node);
        if matches!(self.view.filter_state(), FilterState::Neither) {
            node.degree(self.view.layer_ids(), self.dir)
        } else {
            node.filtered_neighbours_iter(&self.view, self.view.layer_ids(), self.dir)
                .count()
        }
    }
}

impl<G: GraphView + 'static> IntoDynNodeOp for Degree<G> {}

#[derive(Debug, Copy, Clone)]
pub struct Map<Op: NodeOp, V> {
    op: Op,
    map: fn(Op::Output) -> V,
}

impl<Op: NodeOp, V: Clone + Send + Sync> NodeOp for Map<Op, V> {
    type Output = V;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        (self.map)(self.op.apply(storage, node))
    }
}

impl<Op: NodeOp + 'static, V: Clone + Send + Sync + 'static> IntoDynNodeOp for Map<Op, V> {}

impl<'a, V: Clone + Send + Sync> NodeOp for Arc<dyn NodeOp<Output = V> + 'a> {
    type Output = V;
    fn apply(&self, storage: &GraphStorage, node: VID) -> V {
        self.deref().apply(storage, node)
    }
}

impl<V: Clone + Send + Sync + 'static> IntoDynNodeOp for Arc<dyn NodeOp<Output = V>> {
    fn into_dynamic(self) -> Arc<dyn NodeOp<Output = Self::Output>> {
        self.clone()
    }
}