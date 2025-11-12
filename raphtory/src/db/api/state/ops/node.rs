pub mod filter;

use crate::{
    db::{
        api::view::internal::{
            time_semantics::filtered_node::FilteredNodeStorageOps, FilterOps, FilterState,
            GraphView,
        },
        graph::views::filter::model::{not_filter::NotFilter, or_filter::OrFilter, AndFilter},
    },
    prelude::GraphViewOps,
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

    fn and<T>(self, other: T) -> AndFilter<Self, T>;

    fn or<T>(self, other: T) -> OrFilter<Self, T>;

    fn not(self) -> NotFilter<Self>;
}

impl<T: NodeOp<Output = bool> + Clone> NodeFilterOp for T {
    fn is_filtered(&self) -> bool {
        // If there is a const true value, it is not filtered
        self.const_value().is_none_or(|v| !v)
    }

    fn and<T>(self, other: T) -> AndFilter<Self, T> {
        AndFilter {
            left: self,
            right: other,
        }
    }

    fn or<T>(self, other: T) -> OrFilter<Self, T> {
        OrFilter {
            left: self,
            right: other,
        }
    }

    fn not(self) -> NotFilter<Self> {
        NotFilter { 0: self }
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

    fn apply<G: GraphView>(&self, view: &G, storage: &GraphStorage, node: VID) -> Self::Output;

    fn map<V: Clone + Send + Sync>(self, map: fn(Self::Output) -> V) -> Map<Self, V>
    where
        Self: Sized,
    {
        Map { op: self, map }
    }

    fn into_dynamic(self) -> Arc<dyn NodeOp<Output = Self::Output>>
    where
        Self: Sized;
}

impl<Left, Right> NodeOp for Eq<Left, Right>
where
    Left: NodeOp,
    Right: NodeOp,
    Left::Output: PartialEq<Right::Output>,
{
    type Output = bool;

    fn apply<G: GraphView>(&self, view: &G, storage: &GraphStorage, node: VID) -> Self::Output {
        self.left.apply(view, storage, node) == self.right.apply(view, storage, node)
    }

    fn into_dynamic(self) -> Arc<dyn NodeOp<Output = Self::Output>>
    where
        Self: Sized,
    {
        Arc::new(self)
    }
}

impl<L, R> NodeOp for AndFilter<L, R>
where
    L: NodeOp<Output = bool>,
    R: NodeOp<Output = bool>,
{
    type Output = bool;

    fn apply<G: GraphView>(&self, view: &G, storage: &GraphStorage, node: VID) -> Self::Output {
        self.left.apply(view, storage, node) && self.right.apply(view, storage, node)
    }

    fn into_dynamic(self) -> Arc<dyn NodeOp<Output = Self::Output>>
    where
        Self: Sized,
    {
        Arc::new(self)
    }
}

impl<L, R> NodeOp for OrFilter<L, R>
where
    L: NodeOp<Output = bool>,
    R: NodeOp<Output = bool>,
{
    type Output = bool;

    fn apply<G: GraphView>(&self, view: &G, storage: &GraphStorage, node: VID) -> Self::Output {
        self.left.apply(view, storage, node) || self.right.apply(view, storage, node)
    }

    fn into_dynamic(self) -> Arc<dyn NodeOp<Output = Self::Output>>
    where
        Self: Sized,
    {
        Arc::new(self)
    }
}

impl<T> NodeOp for NotFilter<T>
where
    T: NodeOp<Output = bool>,
{
    type Output = bool;

    fn apply<G: GraphView>(&self, view: &G, storage: &GraphStorage, node: VID) -> Self::Output {
        !self.0.apply(view, storage, node)
    }

    fn into_dynamic(self) -> Arc<dyn NodeOp<Output = Self::Output>>
    where
        Self: Sized,
    {
        Arc::new(self)
    }
}

#[derive(Clone, Copy)]
pub struct Const<V>(V);

impl<V> NodeOp for Const<V>
where
    V: Clone,
{
    type Output = V;

    fn const_value(&self) -> Option<Self::Output> {
        self.0.clone()
    }

    fn apply<G: GraphView>(&self, _view: &G, _storage: &GraphStorage, _node: VID) -> Self::Output {
        self.0.clone()
    }

    fn into_dynamic(self) -> Arc<dyn NodeOp<Output = Self::Output>>
    where
        Self: Sized,
    {
        Arc::new(self)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Name;

impl NodeOp for Name {
    type Output = String;

    fn apply<G: GraphView>(&self, _view: &G, storage: &GraphStorage, node: VID) -> Self::Output {
        storage.node_name(node)
    }

    fn into_dynamic(self) -> Arc<dyn NodeOp<Output = Self::Output>>
    where
        Self: Sized,
    {
        Arc::new(self)
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Id;

impl NodeOp for Id {
    type Output = GID;

    fn apply<G: GraphView>(&self, _view: &G, storage: &GraphStorage, node: VID) -> Self::Output {
        storage.node_id(node)
    }

    fn into_dynamic(self) -> Arc<dyn NodeOp<Output = Self::Output>>
    where
        Self: Sized,
    {
        Arc::new(self)
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Type;
impl NodeOp for Type {
    type Output = Option<ArcStr>;

    fn apply<G: GraphView>(&self, _view: &G, storage: &GraphStorage, node: VID) -> Self::Output {
        storage.node_type(node)
    }

    fn into_dynamic(self) -> Arc<dyn NodeOp<Output = Self::Output>>
    where
        Self: Sized,
    {
        Arc::new(self)
    }
}

#[derive(Debug, Copy, Clone)]
pub struct TypeId;
impl NodeOp for TypeId {
    type Output = usize;

    fn apply<G: GraphView>(&self, _view: &G, storage: &GraphStorage, node: VID) -> Self::Output {
        storage.node_type_id(node)
    }

    fn into_dynamic(self) -> Arc<dyn NodeOp<Output = Self::Output>>
    where
        Self: Sized,
    {
        Arc::new(self)
    }
}

#[derive(Debug, Clone)]
pub struct Degree {
    pub(crate) dir: Direction,
}

impl NodeOp for Degree {
    type Output = usize;

    fn apply<G: GraphView>(&self, view: &G, storage: &GraphStorage, node: VID) -> usize {
        let node = storage.core_node(node);
        if matches!(view.filter_state(), FilterState::Neither) {
            node.degree(view.layer_ids(), self.dir)
        } else {
            node.filtered_neighbours_iter(view, view.layer_ids(), self.dir)
                .count()
        }
    }

    fn into_dynamic(self) -> Arc<dyn NodeOp<Output = Self::Output>>
    where
        Self: Sized,
    {
        Arc::new(self)
    }
}

impl<V: Clone + Send + Sync> NodeOp for Arc<dyn NodeOp<Output = V>> {
    type Output = V;
    fn apply<G: GraphView>(&self, view: &G, storage: &GraphStorage, node: VID) -> V {
        self.deref().apply(view, storage, node)
    }

    fn into_dynamic(self) -> Arc<dyn NodeOp<Output = Self::Output>>
    where
        Self: Sized,
    {
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

    fn apply<G: GraphView>(&self, view: &G, storage: &GraphStorage, node: VID) -> Self::Output {
        (self.map)(self.op.apply(view, storage, node))
    }

    fn into_dynamic(self) -> Arc<dyn NodeOp<Output = Self::Output>>
    where
        Self: Sized,
    {
        Arc::new(self)
    }
}
