use crate::{
    db::{
        api::{
            state::{
                ops::{Const, Degree, IntoDynNodeOp, NodeOp},
                Index,
            },
            view::internal::{GraphView, InnerFilterOps, NodeList},
        },
        graph::{
            create_node_type_filter,
            views::filter::model::{
                degree_filter::DegreeFilter,
                filter::{Filter, FilterValue},
                node_filter::NodeFilter,
                property_filter::{Op, PropertyFilterValue, PropertyRef},
                FilterOperator,
            },
        },
    },
    prelude::{GraphViewOps, PropertyFilter},
};
use raphtory_api::core::entities::{
    properties::{meta::NODE_ID_PROP_ID, prop::Prop},
    VID,
};
use raphtory_core::entities::nodes::node_ref::AsNodeRef;
use raphtory_storage::{
    core_ops::CoreGraphOps,
    graph::{
        graph::{GraphStorage, NodeGlobalPropCandidates, NodePropPredicate, NodePropSemantics},
        nodes::node_storage_ops::NodeStorageOps,
    },
};
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

    fn domain(&self, storage: &GraphStorage) -> NodeList {
        let op = &self.filter.operator;
        match op {
            FilterOperator::Eq => match &self.filter.field_value {
                FilterValue::ID(id) => {
                    let vid = storage.internalise_node(id.as_node_ref());
                    NodeList::List {
                        elems: vid.into_iter().collect(),
                    }
                }
                _ => unreachable!(),
            },
            FilterOperator::IsIn => match &self.filter.field_value {
                FilterValue::IDSet(ids) => NodeList::List {
                    elems: ids
                        .iter()
                        .filter_map(|id| storage.internalise_node(id.as_node_ref()))
                        .collect(),
                },
                _ => unreachable!(),
            },
            FilterOperator::IsNone => NodeList::empty(),
            _ => NodeList::All,
        }
    }

    fn const_value(&self) -> Option<Self::Output> {
        match &self.filter.operator {
            FilterOperator::IsSome => Some(true),
            _ => None,
        }
    }
    fn const_value_in_domain(&self, _storage: &GraphStorage) -> Option<Self::Output> {
        match &self.filter.operator {
            FilterOperator::Eq
            | FilterOperator::IsIn
            | FilterOperator::IsNone
            | FilterOperator::IsSome => Some(true),
            _ => None,
        }
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

impl NodeNameFilterOp {
    /// A node's name is its external id (GID), so an index over it can serve the
    /// pattern operators — the ones `domain` would otherwise answer with
    /// `All`, i.e. a scan of every node.
    fn index_candidates(&self, storage: &GraphStorage) -> Option<NodeGlobalPropCandidates> {
        let FilterValue::Single(pattern) = &self.filter.field_value else {
            return None;
        };
        let predicate = match &self.filter.operator {
            FilterOperator::StartsWith => NodePropPredicate::StartsWith(pattern),
            FilterOperator::EndsWith => NodePropPredicate::EndsWith(pattern),
            FilterOperator::Contains => NodePropPredicate::Contains(pattern),
            _ => return None,
        };
        let mut candidates = storage.node_prop_candidates(
            NODE_ID_PROP_ID,
            true,
            &predicate,
            NodePropSemantics::Latest,
        )?;
        candidates.exact = false;
        Some(candidates)
    }
}

impl NodeOp for NodeNameFilterOp {
    type Output = bool;

    fn domain(&self, storage: &GraphStorage) -> NodeList {
        let op = &self.filter.operator;
        match op {
            FilterOperator::Eq => match &self.filter.field_value {
                FilterValue::Single(name) => {
                    let vid = storage.internalise_node(name.as_node_ref());
                    NodeList::List {
                        elems: vid.into_iter().collect(),
                    }
                }
                _ => unreachable!(),
            },
            FilterOperator::IsIn => match &self.filter.field_value {
                FilterValue::Set(names) => NodeList::List {
                    elems: names
                        .iter()
                        .filter_map(|name| storage.internalise_node(name.as_node_ref()))
                        .collect(),
                },
                _ => unreachable!(),
            },
            FilterOperator::IsNone => NodeList::List {
                elems: Index::default(),
            },
            _ => match self.index_candidates(storage) {
                Some(candidates) => NodeList::List {
                    elems: Index::from_sorted(candidates.vids, candidates.exact),
                }
                .intersection(&NodeList::All),
                None => NodeList::All,
            },
        }
    }

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        let node_ref = storage.core_node(node);
        self.filter.matches(Some(&node_ref.name()))
    }

    fn const_value(&self) -> Option<Self::Output> {
        match &self.filter.operator {
            FilterOperator::IsSome => Some(true),
            _ => None,
        }
    }

    fn const_value_in_domain(&self, _storage: &GraphStorage) -> Option<Self::Output> {
        match &self.filter.operator {
            FilterOperator::Eq
            | FilterOperator::IsIn
            | FilterOperator::IsNone
            | FilterOperator::IsSome => Some(true),
            _ => None,
        }
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

    /// The storage-level predicate for index pushdown, when the filter shape
    /// allows it: no value-transforming ops and a positive operator. The
    /// candidates the storage returns are supersets, so `apply` still runs on
    /// every candidate (`const_value_in_domain` stays `None`).
    fn pushdown_predicate(&self) -> Option<NodePropPredicate<'_>> {
        match (&self.filter.operator, &self.filter.prop_value) {
            (FilterOperator::Eq, PropertyFilterValue::Single(v)) => Some(NodePropPredicate::Eq(v)),
            (FilterOperator::Lt, PropertyFilterValue::Single(v)) => Some(NodePropPredicate::Lt(v)),
            (FilterOperator::Le, PropertyFilterValue::Single(v)) => Some(NodePropPredicate::Le(v)),
            (FilterOperator::Gt, PropertyFilterValue::Single(v)) => Some(NodePropPredicate::Gt(v)),
            (FilterOperator::Ge, PropertyFilterValue::Single(v)) => Some(NodePropPredicate::Ge(v)),
            (FilterOperator::IsIn, PropertyFilterValue::Set(values)) => {
                Some(NodePropPredicate::In(values.as_ref()))
            }
            (FilterOperator::StartsWith, PropertyFilterValue::Single(Prop::Str(p))) => {
                Some(NodePropPredicate::StartsWith(&**p))
            }
            (FilterOperator::EndsWith, PropertyFilterValue::Single(Prop::Str(p))) => {
                Some(NodePropPredicate::EndsWith(&**p))
            }
            (FilterOperator::Contains, PropertyFilterValue::Single(Prop::Str(p))) => {
                Some(NodePropPredicate::Contains(&**p))
            }
            _ => None,
        }
    }
}

impl<G: GraphView> NodePropertyFilterOp<G> {
    /// The value semantics to request from the storage index, plus whether
    /// its exactness claim may be kept. Latest-flag candidates are a SUBSET
    /// of what windowed or layer-restricted views need (a row's visible
    /// latest can differ from its global latest), so restricted views fall
    /// back to Ever candidates — a superset for every view — with exactness
    /// off. `temporal().any()` is served by Ever directly; aggregating
    /// chains are not served.
    fn pushdown_semantics(&self) -> Option<(NodePropSemantics, bool)> {
        let plain_view = !self.graph.window_filtered() && !self.graph.is_layer_filtered();
        match (&self.filter.prop_ref, self.filter.ops.as_slice()) {
            (PropertyRef::Property(_) | PropertyRef::Metadata(_), [])
            | (PropertyRef::TemporalProperty(_), [Op::Last]) => Some(if plain_view {
                (NodePropSemantics::Latest, true)
            } else {
                (NodePropSemantics::Ever, false)
            }),
            (PropertyRef::TemporalProperty(_), [Op::Any]) => {
                Some((NodePropSemantics::Ever, plain_view))
            }
            _ => None,
        }
    }

    fn index_candidates(&self, storage: &GraphStorage) -> Option<NodeGlobalPropCandidates> {
        let (semantics, exact_allowed) = self.pushdown_semantics()?;
        let predicate = self.pushdown_predicate()?;
        let metadata = matches!(self.filter.prop_ref, PropertyRef::Metadata(_));
        let mut candidates =
            storage.node_prop_candidates(self.prop_id, metadata, &predicate, semantics)?;
        candidates.exact &= exact_allowed;
        Some(candidates)
    }
}

impl<G: GraphView> NodeOp for NodePropertyFilterOp<G> {
    type Output = bool;

    fn domain(&self, storage: &GraphStorage) -> NodeList {
        if let Some(candidates) = self.index_candidates(storage) {
            // index candidates are ascending and deduplicated, as `from_sorted` requires
            let list = NodeList::List {
                elems: Index::from_sorted(candidates.vids, candidates.exact),
            };
            return list.intersection(&self.graph.node_list());
        }
        // No index could serve this filter, so it has not been applied to
        // anything: the inner list may be exact for the filters that built it,
        // but it cannot claim to be exact for this one as well.
        self.graph.node_list().into_inexact()
    }

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        let node = storage.core_node(node);
        self.filter
            .matches_node(&self.graph, self.prop_id, node.as_ref())
    }
}

#[derive(Debug, Clone)]
pub struct NodeDegreeFilterOp<G> {
    degree: Degree<G>,
    operator: FilterOperator,
    value: PropertyFilterValue,
}

impl<G> NodeDegreeFilterOp<G> {
    pub(crate) fn new(graph: G, filter: DegreeFilter) -> Self {
        let degree = Degree {
            dir: filter.direction,
            view: graph,
        };
        Self {
            degree,
            operator: filter.operator,
            value: filter.value,
        }
    }
}

impl<G: GraphView> NodeOp for NodeDegreeFilterOp<G> {
    type Output = bool;

    fn domain(&self, storage: &GraphStorage) -> NodeList {
        self.degree.domain(storage)
    }

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        let node_degree = self.degree.apply(storage, node);
        let node_degree_prop = Prop::U64(node_degree as u64);
        self.operator
            .apply_to_property(&self.value, Some(&node_degree_prop))
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
        Self {
            mask,
            index_backed: !view.core_graph().node_type_index().is_empty(),
        }
    }
}

impl NodeOp for NodeTypeFilterOp {
    type Output = bool;

    fn domain(&self, storage: &GraphStorage) -> NodeList {
        if !self.index_backed {
            // No index, switch to full scan.
            return NodeList::All;
        }

        let type_ids: Vec<usize> = self
            .mask
            .iter()
            .enumerate()
            .filter_map(|(type_id, keep)| keep.then_some(type_id))
            .collect();

        let nodes = storage.node_type_index().nodes_of_type(&type_ids);

        NodeList::List {
            elems: nodes.into(),
        }
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
