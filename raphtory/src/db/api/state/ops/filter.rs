use crate::{
    db::{
        api::{
            state::{
                ops::{Const, IntoDynNodeOp, TypeId},
                NodeOp,
            },
            view::internal::GraphView,
        },
        graph::{
            create_node_type_filter,
            views::filter::{
                model::{filter::Filter, node_filter::NodeFilter},
                CreateFilter,
            },
        },
    },
    prelude::{GraphViewOps, PropertyFilter},
};
use raphtory_api::core::{entities::VID, storage::arc_str::OptionAsStr};
use raphtory_storage::{
    core_ops::CoreGraphOps,
    graph::{graph::GraphStorage, nodes::node_storage_ops::NodeStorageOps},
};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct Mask<Op> {
    op: Op,
    mask: Arc<[bool]>,
}

impl<Op: NodeOp<Output = usize>> NodeOp for Mask<Op> {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        self.mask
            .get(self.op.apply(storage, node))
            .copied()
            .unwrap_or(false)
    }
}

impl<Op: 'static> IntoDynNodeOp for Mask<Op> where Self: NodeOp {}

pub trait MaskOp: Sized {
    fn mask(self, mask: Arc<[bool]>) -> Mask<Self>;
}

impl<Op: NodeOp<Output = usize>> MaskOp for Op {
    fn mask(self, mask: Arc<[bool]>) -> Mask<Self> {
        Mask { op: self, mask }
    }
}

pub const NO_FILTER: Const<bool> = Const(true);

#[derive(Debug, Clone)]
pub struct NodeIdFilterOp {
    filter: Filter,
}

impl NodeIdFilterOp {
    pub(crate) fn new(filter: Filter) -> Self {
        Self { filter }
    }
}

impl NodeOp for NodeIdFilterOp {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        let node = storage.core_node(node);
        self.filter.id_matches(node.id())
    }
}

#[derive(Debug, Clone)]
pub struct NodeNameFilterOp {
    filter: Filter,
}

impl NodeNameFilterOp {
    pub(crate) fn new(filter: Filter) -> Self {
        Self { filter }
    }
}

impl NodeOp for NodeNameFilterOp {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        let node_ref = storage.core_node(node);
        self.filter.matches(node_ref.name().as_str())
    }
}

#[derive(Debug, Clone)]
pub struct NodePropertyFilterOp<G> {
    graph: G,
    prop_id: usize,
    filter: PropertyFilter<NodeFilter>,
}

impl<G> NodePropertyFilterOp<G> {
    pub(crate) fn new(graph: G, prop_id: usize, filter: PropertyFilter<NodeFilter>) -> Self {
        Self {
            graph,
            prop_id,
            filter,
        }
    }
}

impl<G: GraphView> NodeOp for NodePropertyFilterOp<G> {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        let node = storage.core_node(node);
        self.filter
            .matches_node(&self.graph, self.prop_id, node.as_ref())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrOp<L, R> {
    pub(crate) left: L,
    pub(crate) right: R,
}

impl<L, R> NodeOp for OrOp<L, R>
where
    L: NodeOp<Output = bool>,
    R: NodeOp<Output = bool>,
{
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        self.left.apply(storage, node) || self.right.apply(storage, node)
    }
}

impl<L, R> IntoDynNodeOp for OrOp<L, R> where Self: NodeOp + 'static {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AndOp<L, R> {
    pub(crate) left: L,
    pub(crate) right: R,
}

impl<L, R> NodeOp for AndOp<L, R>
where
    L: NodeOp<Output = bool>,
    R: NodeOp<Output = bool>,
{
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        self.left.apply(storage, node) && self.right.apply(storage, node)
    }
}

impl<L, R> IntoDynNodeOp for AndOp<L, R> where Self: NodeOp + 'static {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotOp<T>(pub(crate) T);

impl<T> IntoDynNodeOp for NotOp<T> where Self: NodeOp + 'static {}

impl<T> NodeOp for NotOp<T>
where
    T: NodeOp<Output = bool>,
{
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        !self.0.apply(storage, node)
    }
}

pub type NodeTypeFilterOp = Mask<TypeId>;

impl NodeTypeFilterOp {
    pub fn new_from_values<I: IntoIterator<Item = V>, V: AsRef<str>>(
        node_types: I,
        view: impl GraphView,
    ) -> Self {
        let mask = create_node_type_filter(view.node_meta().node_type_meta(), node_types);
        TypeId.mask(mask)
    }
}

#[cfg(test)]
mod test {
    use crate::db::api::state::ops::{Const, NodeFilterOp};

    #[test]
    fn test_const() {
        let c = Const(true);
        assert!(!c.is_filtered());
    }
}
