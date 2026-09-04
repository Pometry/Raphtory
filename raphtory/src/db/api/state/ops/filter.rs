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
        if matches!(self.const_value_in_domain(storage), Some(false)) {
            NodeList::empty()
        } else {
            self.left.domain(storage).union(&self.right.domain(storage))
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
                .is_subset(&self.left.domain(storage))
        {
            return Some(true);
        }
        if right == Some(true)
            && self
                .left
                .domain(storage)
                .is_subset(&self.right.domain(storage))
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
    use super::{AndOp, OrOp};
    use crate::{
        db::api::{
            state::ops::{Const, NodeFilterOp, NodeOp},
            view::internal::NodeList,
        },
        prelude::Graph,
    };
    use raphtory_api::core::entities::VID;
    use raphtory_storage::{core_ops::CoreGraphOps, graph::graph::GraphStorage};

    #[test]
    fn test_const() {
        let c = Const(true);
        assert!(!c.is_filtered());
    }

    /// A stub op with a configurable domain and `const_value` / `const_value_in_domain`, so the
    /// combinators can be exercised over non-trivial domains without building a real filter.
    #[derive(Clone)]
    struct Stub {
        cv: Option<bool>,
        cvid: Option<bool>,
        domain: NodeList,
    }

    impl NodeOp for Stub {
        type Output = bool;
        fn domain(&self, _storage: &GraphStorage) -> NodeList {
            self.domain.clone()
        }
        fn apply(&self, _storage: &GraphStorage, _node: VID) -> bool {
            true
        }
        fn const_value(&self) -> Option<bool> {
            self.cv
        }
        fn const_value_in_domain(&self, _storage: &GraphStorage) -> Option<bool> {
            self.cvid
        }
    }

    fn list(vids: impl IntoIterator<Item = usize>) -> NodeList {
        NodeList::List {
            elems: vids.into_iter().map(VID).collect(),
        }
    }

    /// Constant-true over a bounded domain but not globally — the profile of `name.is_in([...])`.
    fn member(vids: impl IntoIterator<Item = usize>) -> Stub {
        Stub {
            cv: None,
            cvid: Some(true),
            domain: list(vids),
        }
    }

    /// Domain-all and not constant over it — the profile of `node_type.is_in([...])`.
    fn wide() -> Stub {
        Stub {
            cv: None,
            cvid: None,
            domain: NodeList::All,
        }
    }

    /// Not constant, but with a bounded domain — the (currently hypothetical) shape the superset
    /// case exists for.
    fn bounded_wide(vids: impl IntoIterator<Item = usize>) -> Stub {
        Stub {
            cv: None,
            cvid: None,
            domain: list(vids),
        }
    }

    #[test]
    fn or_const_value_in_domain() {
        let g = Graph::new();
        let s = g.core_graph();

        // Both branches constant-true over their domains: the union is covered.
        let both = OrOp {
            left: member([0, 1]),
            right: member([2, 3]),
        };
        assert_eq!(both.const_value_in_domain(s), Some(true));

        // A non-constant branch with domain `All` can match anywhere, so it is never covered.
        let widened = OrOp {
            left: wide(),
            right: member([0, 1]),
        };
        assert_eq!(widened.const_value_in_domain(s), None);

        let nested = OrOp {
            left: wide(),
            right: OrOp {
                left: member([0, 1]),
                right: member([2, 3]),
            },
        };
        assert_eq!(nested.const_value_in_domain(s), None);

        // A constant-true branch whose domain covers the other branch's is trusted even when that
        // other branch is not constant (`true || false == true`)...
        let covered = OrOp {
            left: member([0, 1, 2]),
            right: bounded_wide([0, 1]),
        };
        assert_eq!(covered.const_value_in_domain(s), Some(true));
        // ...but not once the non-constant branch reaches past that domain.
        let uncovered = OrOp {
            left: member([0, 1, 2]),
            right: bounded_wide([0, 3]),
        };
        assert_eq!(uncovered.const_value_in_domain(s), None);

        // A globally-true branch has domain `All`, so it covers anything OR'd with it.
        let global = Stub {
            cv: Some(true),
            cvid: Some(true),
            domain: NodeList::All,
        };
        let global_or = OrOp {
            left: global,
            right: wide(),
        };
        assert_eq!(global_or.const_value(), Some(true));
        assert_eq!(global_or.const_value_in_domain(s), Some(true));
    }

    #[test]
    fn and_const_value_in_domain_is_the_conjunction() {
        let g = Graph::new();
        let s = g.core_graph();
        let both = AndOp {
            left: member([0, 1]),
            right: member([0, 1]),
        };
        assert_eq!(both.const_value_in_domain(s), Some(true));
        let mixed = AndOp {
            left: member([0, 1]),
            right: wide(),
        };
        assert_eq!(mixed.const_value_in_domain(s), None);
    }
}
