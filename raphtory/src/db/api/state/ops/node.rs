use crate::db::api::{
    state::{ops::IntoDynNodeOp, NodeOp},
    view::internal::{filtered_node::FilteredNodeStorageOps, FilterOps, FilterState, GraphView},
};
use raphtory_api::core::{
    entities::{GID, VID},
    storage::arc_str::ArcStr,
    Direction,
};
use raphtory_storage::{
    core_ops::CoreGraphOps,
    graph::{graph::GraphStorage, nodes::node_storage_ops::NodeStorageOps},
    layer_ops::InternalLayerOps,
};

#[derive(Debug, Clone, Copy)]
pub struct Name;

impl NodeOp for Name {
    type Output = String;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        storage.node_name(node)
    }
}

impl IntoDynNodeOp for Name {}

#[derive(Debug, Copy, Clone)]
pub struct Id;

impl NodeOp for Id {
    type Output = GID;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        storage.node_id(node)
    }
}

impl IntoDynNodeOp for Id {}

#[derive(Debug, Copy, Clone)]
pub struct Type;

impl NodeOp for Type {
    type Output = Option<ArcStr>;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        storage.node_type(node)
    }
}

impl IntoDynNodeOp for Type {}

#[derive(Debug, Copy, Clone)]
pub struct TypeId;

impl NodeOp for TypeId {
    type Output = usize;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        storage.node_type_id(node)
    }
}

impl IntoDynNodeOp for TypeId {}

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
