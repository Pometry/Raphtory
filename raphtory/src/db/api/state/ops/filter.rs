use crate::{
    db::{
        api::{
            state::{
                ops::{Const, Degree, IntoDynNodeOp, NodeOp, TypeId},
                Index,
            },
            view::internal::{GraphView, NodeList},
        },
        graph::{
            create_node_type_filter,
            views::filter::model::{
                degree_filter::DegreeFilter,
                filter::{Filter, FilterValue},
                node_filter::NodeFilter,
                property_filter::{PropertyFilterValue, PropertyRef},
                FilterOperator,
            },
        },
    },
    prelude::{GraphViewOps, PropertyFilter},
};
use raphtory_api::core::entities::{properties::prop::Prop, VID};
use raphtory_core::entities::nodes::node_ref::AsNodeRef;
use raphtory_storage::graph::{
    graph::{GraphStorage, NodeGlobalPropCandidates, NodePropPredicate, NodePropSemantics},
    nodes::node_storage_ops::NodeStorageOps,
};
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
    fn const_value_in_domain(&self) -> Option<Self::Output> {
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
            _ => NodeList::All,
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
    fn const_value_in_domain(&self) -> Option<Self::Output> {
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
        use crate::db::api::view::internal::InnerFilterOps;
        use crate::db::graph::views::filter::model::property_filter::Op;
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
            // candidates arrive ascending and deduplicated from the index;
            // exactness rides with them and is gated on view shape at the
            // consumption sites (FilterOps::trusted_node_list)
            let list = NodeList::List {
                elems: Index::from_sorted(candidates.vids, candidates.exact),
            };
            return list.intersection(&self.graph.node_list());
        }
        self.graph.node_list()
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
