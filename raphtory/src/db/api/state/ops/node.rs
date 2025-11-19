use crate::db::{
    api::{
        state::ops::filter::{Mask, MaskOp},
        view::internal::{
            time_semantics::filtered_node::FilteredNodeStorageOps, FilterOps, FilterState,
            GraphView,
        },
    },
    graph::{
        create_node_type_filter,
        views::filter::model::{and_filter::AndOp, not_filter::NotOp, or_filter::OrOp},
    },
};
use raphtory_api::core::{
    entities::{GID, VID},
    storage::arc_str::ArcStr,
    Direction,
};
use raphtory_storage::{
    core_ops::CoreGraphOps,
    graph::{graph::GraphStorage, nodes::node_storage_ops::NodeStorageOps},
};
use std::{ops::Deref, sync::Arc};

pub trait NodeFilterOp: NodeOp<Output = bool> + Clone {
    fn is_filtered(&self) -> bool;

    fn and<T>(self, other: T) -> AndOp<Self, T>;

    fn or<T>(self, other: T) -> OrOp<Self, T>;

    fn not(self) -> NotOp<Self>;
}

#[derive(Clone)]
pub struct NotANodeFilter;

impl NodeOp for NotANodeFilter {
    type Output = bool;

    fn apply(&self, _storage: &GraphStorage, _node: VID) -> Self::Output {
        panic!("Not a node filter")
    }
}

impl<Op: NodeOp<Output = bool> + Clone> NodeFilterOp for Op {
    fn is_filtered(&self) -> bool {
        // If there is a const true value, it is not filtered
        self.const_value().is_none_or(|v| !v)
    }

    fn and<T>(self, other: T) -> AndOp<Self, T> {
        AndOp {
            left: self,
            right: other,
        }
    }

    fn or<T>(self, other: T) -> OrOp<Self, T> {
        OrOp {
            left: self,
            right: other,
        }
    }

    fn not(self) -> NotOp<Self> {
        NotOp { 0: self }
    }
}

pub type DynNodeFilter = Arc<dyn NodeOp<Output = bool>>;

pub struct Eq<Left, Right> {
    left: Left,
    right: Right,
}

pub trait NodeOp: Send + Sync {
    type Output: Clone + Send + Sync;

    fn const_value(&self) -> Option<Self::Output> {
        None
    }

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output;

    fn map<V: Clone + Send + Sync>(self, map: fn(Self::Output) -> V) -> Map<Self, V>
    where
        Self: Sized,
    {
        Map { op: self, map }
    }
}

pub trait IntoDynNodeOp: NodeOp + Sized + 'static {
    fn into_dynamic(self) -> Arc<dyn NodeOp<Output = Self::Output>> {
        Arc::new(self)
    }
}

pub type DynNodeOp<O> = Arc<dyn NodeOp<Output = O>>;

impl<Left, Right> NodeOp for Eq<Left, Right>
where
    Left: NodeOp,
    Right: NodeOp,
    Left::Output: PartialEq<Right::Output>,
{
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        self.left.apply(storage, node) == self.right.apply(storage, node)
    }
}

impl<Left, Right> IntoDynNodeOp for Eq<Left, Right> where Eq<Left, Right>: NodeOp + 'static {}

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

impl<T> NodeOp for NotOp<T>
where
    T: NodeOp<Output = bool>,
{
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        !self.0.apply(storage, node)
    }
}

impl<T> IntoDynNodeOp for NotOp<T> where Self: NodeOp + 'static {}

#[derive(Clone, Copy, Debug)]
pub struct Const<V>(pub V);

impl<V> NodeOp for Const<V>
where
    V: Send + Sync + Clone,
{
    type Output = V;

    fn const_value(&self) -> Option<Self::Output> {
        Some(self.0.clone())
    }

    fn apply(&self, __storage: &GraphStorage, _node: VID) -> Self::Output {
        self.0.clone()
    }
}

impl<V: Clone + Send + Sync + 'static> IntoDynNodeOp for Const<V> {}

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
