use crate::{
    db::{
        api::{
            state::ops::{Const, IntoDynNodeOp, NodeOp, TypeId},
            view::internal::{GraphView, NodeList},
        },
        graph::create_node_type_filter,
    },
    prelude::GraphViewOps,
};
use raphtory_api::core::entities::VID;
use raphtory_storage::graph::graph::GraphStorage;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct Mask<Op> {
    op: Op,
    mask: Arc<[bool]>,
}

impl<Op: NodeOp<Output = usize>> NodeOp for Mask<Op> {
    type Output = bool;

    fn domain(&self, storage: &GraphStorage) -> NodeList {
        self.op.domain(storage)
    }

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
pub struct NodeExistsOp<G> {
    graph: G,
}

impl<G: GraphView> NodeExistsOp<G> {
    pub(crate) fn new(graph: G) -> Self {
        Self { graph }
    }
}

impl<G: GraphView> NodeOp for NodeExistsOp<G> {
    type Output = bool;

    fn apply(&self, _storage: &GraphStorage, node: VID) -> Self::Output {
        self.graph.has_node(node)
    }

    fn domain(&self, _storage: &GraphStorage) -> NodeList {
        self.graph.node_list()
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

    fn domain(&self, storage: &GraphStorage) -> NodeList {
        if matches!(self.const_value_in_domain(), Some(false)) {
            NodeList::empty()
        } else {
            self.left.domain(storage).union(&self.right.domain(storage))
        }
    }

    fn const_value_in_domain(&self) -> Option<Self::Output> {
        match (self.left.const_value(), self.right.const_value()) {
            (Some(true), _) | (_, Some(true)) => Some(true),
            (Some(left), Some(right)) => Some(left || right),
            _ => None,
        }
    }

    fn const_value(&self) -> Option<Self::Output> {
        match (
            self.left.const_value_in_domain(),
            self.right.const_value_in_domain(),
        ) {
            (Some(true), _) | (_, Some(true)) => Some(true),
            (Some(left), Some(right)) => Some(left || right),
            _ => None,
        }
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

    fn domain(&self, storage: &GraphStorage) -> NodeList {
        if matches!(self.const_value_in_domain(), Some(false)) {
            NodeList::empty()
        } else {
            self.left
                .domain(storage)
                .intersection(&self.right.domain(storage))
        }
    }

    fn const_value(&self) -> Option<Self::Output> {
        match (self.left.const_value(), self.right.const_value()) {
            (Some(false), _) | (_, Some(false)) => Some(false),
            (Some(left), Some(right)) => Some(left && right),
            _ => None,
        }
    }

    fn const_value_in_domain(&self) -> Option<Self::Output> {
        match (
            self.left.const_value_in_domain(),
            self.right.const_value_in_domain(),
        ) {
            (Some(false), _) | (_, Some(false)) => Some(false),
            (Some(left), Some(right)) => Some(left && right),
            _ => None,
        }
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

    fn domain(&self, _storage: &GraphStorage) -> NodeList {
        NodeList::All
    }

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
