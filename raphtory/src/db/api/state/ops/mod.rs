pub mod filter;
pub mod history;
pub mod node;
pub mod properties;

use crate::db::api::state::ops::filter::{AndOp, NotOp, OrOp};
pub use history::*;
pub use node::*;
pub use properties::*;
use raphtory_api::core::entities::VID;
use raphtory_storage::graph::graph::GraphStorage;
use std::{ops::Deref, sync::Arc};

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

pub type DynNodeFilter = Arc<dyn NodeOp<Output = bool>>;

pub type DynNodeOp<O> = Arc<dyn NodeOp<Output = O>>;

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

pub trait IntoDynNodeOp: NodeOp + Sized + 'static {
    fn into_dynamic(self) -> Arc<dyn NodeOp<Output = Self::Output>> {
        Arc::new(self)
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

impl<'a, V: Clone + Send + Sync> NodeOp for Arc<dyn NodeOp<Output = V> + 'a> {
    type Output = V;
    fn apply(&self, storage: &GraphStorage, node: VID) -> V {
        self.deref().apply(storage, node)
    }
}

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

pub struct Eq<Left, Right> {
    left: Left,
    right: Right,
}

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

#[derive(Clone)]
pub struct NotANodeFilter;

impl NodeOp for NotANodeFilter {
    type Output = bool;

    fn apply(&self, _storage: &GraphStorage, _node: VID) -> Self::Output {
        panic!("Not a node filter")
    }
}
