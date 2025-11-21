use raphtory_api::core::{
    entities::{GID, VID},
    storage::arc_str::ArcStr
    ,
};
use raphtory_storage::{
    core_ops::CoreGraphOps,
    graph::graph::GraphStorage,
};
use std::sync::Arc;
use crate::db::api::state::ops::filter::{AndOp, NotOp, OrOp};
use crate::db::api::state::ops::Map;

pub trait NodeFilterOp: NodeOp<Output = bool> + Clone {
    fn is_filtered(&self) -> bool;

    fn and<T>(self, other: T) -> AndOp<Self, T>;

    fn or<T>(self, other: T) -> OrOp<Self, T>;

    fn not(self) -> NotOp<Self>;
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
