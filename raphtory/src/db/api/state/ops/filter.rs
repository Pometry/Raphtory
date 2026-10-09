use crate::{
    db::{
        api::{
            state::ops::{Const, IntoDynNodeOp, NodeOp},
            view::internal::{GraphView, NodeList},
        },
        graph::create_node_type_filter,
    },
    prelude::GraphViewOps,
};
use raphtory_api::core::entities::{properties::meta::DEFAULT_NODE_TYPE_ID, VID};
use raphtory_storage::{core_ops::CoreGraphOps, graph::graph::GraphStorage};
use std::sync::Arc;
use storage::api::node_type_index::NodeTypeIndexOps;

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

impl<L, R> OrOp<L, R> {
    pub fn new(left: L, right: R) -> Self {
        Self { left, right }
    }
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
        if matches!(self.const_value_in_domain(storage), Some(false)) {
            NodeList::empty()
        } else {
            self.left
                .domain(storage)
                .union(&self.right.domain(storage), storage)
        }
    }

    fn const_value(&self) -> Option<Self::Output> {
        match (self.left.const_value(), self.right.const_value()) {
            (Some(true), _) | (_, Some(true)) => Some(true),
            (Some(left), Some(right)) => Some(left || right),
            _ => None,
        }
    }

    fn const_value_in_domain(&self, storage: &GraphStorage) -> Option<Self::Output> {
        // The OR is true across its domain (the union of the branches') exactly when every node in
        // that union is guaranteed true by some branch. A branch guarantees true everywhere if it
        // is globally constant-true, and over its own domain if it is constant-true there.
        let left = self.left.const_value_in_domain(storage);
        let right = self.right.const_value_in_domain(storage);
        if left == Some(true) && right == Some(true) {
            return Some(true);
        }
        // If only one branch is constant-true, the union is still covered when the other branch's
        // domain sits inside it (`true || false == true`); a globally-true branch has domain `All`.
        if left == Some(true)
            && self
                .right
                .domain(storage)
                .is_subset(&self.left.domain(storage), storage)
        {
            return Some(true);
        }
        if right == Some(true)
            && self
                .left
                .domain(storage)
                .is_subset(&self.right.domain(storage), storage)
        {
            return Some(true);
        }
        // The whole OR is false everywhere only when both branches are.
        match (left, right) {
            (Some(false), Some(false)) => Some(false),
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

impl<L, R> AndOp<L, R> {
    pub fn new(left: L, right: R) -> Self {
        Self { left, right }
    }
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
        if matches!(self.const_value_in_domain(storage), Some(false)) {
            NodeList::empty()
        } else {
            self.left
                .domain(storage)
                .intersection(&self.right.domain(storage), storage)
        }
    }

    fn const_value(&self) -> Option<Self::Output> {
        match (self.left.const_value(), self.right.const_value()) {
            (Some(false), _) | (_, Some(false)) => Some(false),
            (Some(left), Some(right)) => Some(left && right),
            _ => None,
        }
    }

    fn const_value_in_domain(&self, storage: &GraphStorage) -> Option<Self::Output> {
        match (
            self.left.const_value_in_domain(storage),
            self.right.const_value_in_domain(storage),
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

#[derive(Clone, Debug)]
pub struct NodeTypeFilterOp {
    mask: Arc<[bool]>,

    /// `true` when the node type index is populated and can be used.
    index_backed: bool,
}

impl NodeTypeFilterOp {
    pub fn from_values<I: IntoIterator<Item = V>, V: AsRef<str>>(
        node_types: I,
        view: impl GraphView,
    ) -> Self {
        let node_type_meta = view.node_meta().node_type_meta();
        let mask = create_node_type_filter(node_type_meta, node_types);

        Self::from_mask(mask, view)
    }

    pub fn from_mask(mask: Arc<[bool]>, view: impl GraphView) -> Self {
        // Nodes of the default type are not indexed, so a mask selecting it
        // cannot be served from the index.
        let selects_default = mask.get(DEFAULT_NODE_TYPE_ID).copied().unwrap_or(false);
        let index_backed = !selects_default && !view.core_graph().node_type_index().is_empty();
        Self { mask, index_backed }
    }
}

impl NodeOp for NodeTypeFilterOp {
    type Output = bool;

    fn domain(&self, _storage: &GraphStorage) -> NodeList {
        if !self.index_backed {
            // No index, switch to full scan.
            return NodeList::All;
        }

        let types = self
            .mask
            .iter()
            .enumerate()
            .filter_map(|(type_id, keep)| keep.then_some(type_id))
            .collect();

        NodeList::NodeTypeIdx { types }
    }

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        let node_type_id = storage.node_type_id(node);

        self.mask.get(node_type_id).copied().unwrap_or(false)
    }

    fn const_value_in_domain(&self, _storage: &GraphStorage) -> Option<Self::Output> {
        self.index_backed.then_some(true)
    }
}

impl IntoDynNodeOp for NodeTypeFilterOp {}
