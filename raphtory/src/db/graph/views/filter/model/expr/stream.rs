//! Aggregations and `any()`/`all()` tests that walk a temporal history
//! instead of collecting it into a list first.
//!
//! A history term on its own still produces a list, because a list is the
//! only value the engine can hand to an arbitrary consumer. An aggregate
//! built directly over a history, or `any()`/`all()` over a comparison of a
//! history with a constant (see `CreateOp::history` and
//! `CreateOp::value_test`), builds one of the ops here instead: they ask the
//! history for a stream, from whichever end the question needs, and stop as
//! soon as the answer is known.

use crate::{
    core::utils::iter::GenLockedIter,
    db::{
        api::{
            state::ops::NodeOp,
            view::{
                internal::{DynGraphArc, GraphView, NodeList},
                BoxedLIter, IntoDynBoxed, NodeViewOps,
            },
        },
        graph::{
            edge_reads::{self, EdgeAt},
            views::filter::model::{
                edge_expr::{ops::TemporalEdgePropOp, EdgeOp},
                edge_filter::{EdgeEndpointWrapper, Endpoint},
                filter_operator::{BinaryOp, Comparable, StringComparable, StringOp},
                node_expr::{
                    ops::{agg_out_pt, fold_values, reduce_list, view_node, TemporalNodePropOp},
                    typing::{comparison_shape, list, set_shape, string_shape, Shape},
                },
                property_filter::evaluate::aggregate_list_values,
            },
        },
    },
    errors::GraphError,
};
use raphtory_api::core::{
    entities::{
        properties::prop::{prop_hashable::HashableProp, Prop, PropType},
        LayerId, VID,
    },
    storage::timeindex::EventTime,
};
use raphtory_storage::graph::{edges::edge_storage_ops::EdgeStorageOps, graph::GraphStorage};
use std::{collections::HashSet, iter, sync::Arc};
use storage::EdgeEntryRef;

use super::Agg;

// ── histories ────────────────────────────────────────────────────────────────

/// One property's history on a node, as a stream from either end.
pub trait NodeHistory: Send + Sync {
    /// The type the history has when read as one value: a list of its
    /// element type.
    fn history_type(&self) -> PropType;
    fn domain(&self) -> NodeList;
    /// Oldest first.
    fn values<'a>(&'a self, node: VID) -> BoxedLIter<'a, Prop>;
    /// Newest first.
    fn values_rev<'a>(&'a self, node: VID) -> BoxedLIter<'a, Prop>;
}

/// One property's history on an edge, as a stream from either end, for the
/// edge as a whole, one layer, or one exploded instance.
pub trait EdgeHistory: Send + Sync {
    fn history_type(&self) -> PropType;
    fn values<'a>(&'a self, edge: EdgeEntryRef<'a>, at: EdgeAt) -> BoxedLIter<'a, Prop>;
    fn values_rev<'a>(&'a self, edge: EdgeEntryRef<'a>, at: EdgeAt) -> BoxedLIter<'a, Prop>;
}

impl<G: GraphView> NodeHistory for TemporalNodePropOp<G> {
    fn history_type(&self) -> PropType {
        NodeOp::prop_type(self)
    }

    fn domain(&self) -> NodeList {
        self.graph.node_list()
    }

    fn values<'a>(&'a self, node: VID) -> BoxedLIter<'a, Prop> {
        match view_node(&self.graph, self.narrows, node)
            .and_then(|n| n.properties().temporal().get_by_id(self.prop_id))
        {
            Some(history) => GenLockedIter::from(history, |h| h.values()).into_dyn_boxed(),
            None => iter::empty().into_dyn_boxed(),
        }
    }

    fn values_rev<'a>(&'a self, node: VID) -> BoxedLIter<'a, Prop> {
        match view_node(&self.graph, self.narrows, node)
            .and_then(|n| n.properties().temporal().get_by_id(self.prop_id))
        {
            Some(history) => GenLockedIter::from(history, |h| h.values_rev()).into_dyn_boxed(),
            None => iter::empty().into_dyn_boxed(),
        }
    }
}

impl<G: GraphView> EdgeHistory for TemporalEdgePropOp<G> {
    fn history_type(&self) -> PropType {
        EdgeOp::prop_type(self)
    }

    fn values<'a>(&'a self, edge: EdgeEntryRef<'a>, at: EdgeAt) -> BoxedLIter<'a, Prop> {
        match edge_reads::shown(&self.graph, edge, at) {
            Some(at) => edge_reads::temporal_hist(&self.graph, edge, at, self.prop_id)
                .map(|(_, v)| v)
                .into_dyn_boxed(),
            None => iter::empty().into_dyn_boxed(),
        }
    }

    fn values_rev<'a>(&'a self, edge: EdgeEntryRef<'a>, at: EdgeAt) -> BoxedLIter<'a, Prop> {
        match edge_reads::shown(&self.graph, edge, at) {
            Some(at) => edge_reads::temporal_hist_rev(&self.graph, edge, at, self.prop_id)
                .map(|(_, v)| v)
                .into_dyn_boxed(),
            None => iter::empty().into_dyn_boxed(),
        }
    }
}

/// A node history taken at an edge's source or destination.
struct EndpointHistory<'g> {
    node: Arc<dyn NodeHistory + 'g>,
    endpoint: Endpoint,
}

impl<'g> EndpointHistory<'g> {
    fn node_of(&self, edge: EdgeEntryRef) -> VID {
        match self.endpoint {
            Endpoint::Src => edge.src(),
            Endpoint::Dst => edge.dst(),
        }
    }
}

impl<'g> EdgeHistory for EndpointHistory<'g> {
    fn history_type(&self) -> PropType {
        self.node.history_type()
    }

    fn values<'a>(&'a self, edge: EdgeEntryRef<'a>, _at: EdgeAt) -> BoxedLIter<'a, Prop> {
        self.node.values(self.node_of(edge))
    }

    fn values_rev<'a>(&'a self, edge: EdgeEntryRef<'a>, _at: EdgeAt) -> BoxedLIter<'a, Prop> {
        self.node.values_rev(self.node_of(edge))
    }
}

/// Builds a history against a graph: the erased form of a temporal term.
pub trait DynCreateHistory: Send + Sync + 'static {
    fn create_node_history<'g>(
        &self,
        graph: DynGraphArc<'g>,
    ) -> Result<Arc<dyn NodeHistory + 'g>, GraphError>;

    fn create_edge_history<'g>(
        &self,
        graph: DynGraphArc<'g>,
    ) -> Result<Arc<dyn EdgeHistory + 'g>, GraphError>;
}

impl<T: DynCreateHistory + ?Sized> DynCreateHistory for Arc<T> {
    fn create_node_history<'g>(
        &self,
        graph: DynGraphArc<'g>,
    ) -> Result<Arc<dyn NodeHistory + 'g>, GraphError> {
        self.as_ref().create_node_history(graph)
    }

    fn create_edge_history<'g>(
        &self,
        graph: DynGraphArc<'g>,
    ) -> Result<Arc<dyn EdgeHistory + 'g>, GraphError> {
        self.as_ref().create_edge_history(graph)
    }
}

/// A node history taken through an endpoint is an edge history; it has no
/// node form.
impl<T: DynCreateHistory> DynCreateHistory for EdgeEndpointWrapper<T> {
    fn create_node_history<'g>(
        &self,
        _graph: DynGraphArc<'g>,
    ) -> Result<Arc<dyn NodeHistory + 'g>, GraphError> {
        Err(GraphError::InvalidFilter(
            "an endpoint term is an edge expression".to_string(),
        ))
    }

    fn create_edge_history<'g>(
        &self,
        graph: DynGraphArc<'g>,
    ) -> Result<Arc<dyn EdgeHistory + 'g>, GraphError> {
        Ok(Arc::new(EndpointHistory {
            node: self.inner.create_node_history(graph)?,
            endpoint: self.endpoint(),
        }))
    }
}

// ── reductions ───────────────────────────────────────────────────────────────

/// Whether the updates of a history of this type are lists themselves.
fn updates_are_lists(history_type: &PropType) -> bool {
    matches!(history_type, PropType::List(elem) if matches!(**elem, PropType::List(_)))
}

/// One aggregation over a history, reading from the end the aggregation
/// needs. When the updates are lists, each reduces on its own, one answer
/// per update, as the list path does.
fn reduce<'a>(
    agg: Agg,
    per_update: bool,
    values: impl FnOnce() -> BoxedLIter<'a, Prop>,
    values_rev: impl FnOnce() -> BoxedLIter<'a, Prop>,
) -> Option<Prop> {
    match agg {
        Agg::Earliest => values().next(),
        Agg::Latest => values_rev().next(),
        // A sum that overflows one update but not another answers `Decimal`
        // beside `U64`/`I64`; such a list has no element type, so it has no
        // answer.
        _ if per_update => Prop::list(values().filter_map(|update| {
            aggregate_list_values(Some(update), &|items| reduce_list(agg, items))
        }))
        .ok(),
        Agg::Last => values_rev().next(),
        _ => fold_values(agg, values()),
    }
}

#[derive(Clone)]
pub(crate) struct StreamedAggNodeOp<'g> {
    history: Arc<dyn NodeHistory + 'g>,
    agg: Agg,
    per_update: bool,
}

impl<'g> StreamedAggNodeOp<'g> {
    pub(crate) fn new(history: Arc<dyn NodeHistory + 'g>, agg: Agg) -> Self {
        let per_update = updates_are_lists(&history.history_type());
        Self {
            history,
            agg,
            per_update,
        }
    }
}

impl<'g> NodeOp for StreamedAggNodeOp<'g> {
    type Output = Option<Prop>;

    fn domain(&self, _storage: &GraphStorage) -> NodeList {
        self.history.domain()
    }

    fn prop_type(&self) -> PropType {
        agg_out_pt(self.agg, self.history.history_type())
    }

    fn apply(&self, _storage: &GraphStorage, node: VID) -> Option<Prop> {
        reduce(
            self.agg,
            self.per_update,
            || self.history.values(node),
            || self.history.values_rev(node),
        )
    }
}

#[derive(Clone)]
pub(crate) struct StreamedAggEdgeOp<'g> {
    history: Arc<dyn EdgeHistory + 'g>,
    agg: Agg,
    per_update: bool,
}

impl<'g> StreamedAggEdgeOp<'g> {
    pub(crate) fn new(history: Arc<dyn EdgeHistory + 'g>, agg: Agg) -> Self {
        let per_update = updates_are_lists(&history.history_type());
        Self {
            history,
            agg,
            per_update,
        }
    }

    fn at(&self, edge: EdgeEntryRef, at: EdgeAt) -> Option<Prop> {
        reduce(
            self.agg,
            self.per_update,
            || self.history.values(edge, at),
            || self.history.values_rev(edge, at),
        )
    }
}

impl<'g> EdgeOp for StreamedAggEdgeOp<'g> {
    type Output = Option<Prop>;

    fn prop_type(&self) -> PropType {
        agg_out_pt(self.agg, self.history.history_type())
    }

    fn apply(&self, _storage: &GraphStorage, edge: EdgeEntryRef) -> Option<Prop> {
        self.at(edge, EdgeAt::Whole)
    }

    fn apply_layer(
        &self,
        _storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
    ) -> Option<Prop> {
        self.at(edge, EdgeAt::Layer(layer))
    }

    fn apply_exploded(
        &self,
        _storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
        t: EventTime,
    ) -> Option<Prop> {
        self.at(edge, EdgeAt::Exploded(layer, t))
    }
}

// ── qualified tests ──────────────────────────────────────────────────────────

/// A test of each history value against a constant.
#[derive(Clone, Debug, PartialEq)]
pub enum ValueTest {
    Cmp(BinaryOp, Prop),
    Str(StringOp, Prop),
    In(Arc<HashSet<HashableProp>>, bool),
}

impl ValueTest {
    /// Membership of each value in `values`, or (`negated`) its absence.
    pub fn members(values: impl IntoIterator<Item = Prop>, negated: bool) -> Self {
        ValueTest::In(
            Arc::new(values.into_iter().map(HashableProp).collect()),
            negated,
        )
    }

    /// This test on a history of `history_type`, when the history's
    /// element-wise result is one yes/no answer per value: the same test, with
    /// a member set reduced to the values the type can equal. Anything else
    /// (a nested list, a mismatch) is left to the list path, which reports it
    /// the way it always has.
    pub(crate) fn for_history(&self, history_type: &PropType) -> Option<ValueTest> {
        let one_per_value = |(out, shape): (PropType, Shape)| {
            shape == Shape::Elementwise && out == list(PropType::Bool)
        };
        match self {
            ValueTest::Cmp(op, constant) => {
                let shape = comparison_shape(op, history_type, &constant.dtype()).ok()?;
                one_per_value(shape).then(|| self.clone())
            }
            ValueTest::Str(_, constant) => {
                let shape = string_shape(history_type, &constant.dtype()).ok()?;
                one_per_value(shape).then(|| self.clone())
            }
            ValueTest::In(members, negated) => {
                let values: Vec<Prop> = members.iter().map(|m| m.0.clone()).collect();
                let (out, shape, comparable) = set_shape(history_type, &values);
                one_per_value((out, shape)).then(|| ValueTest::members(comparable, *negated))
            }
        }
    }

    fn holds(&self, value: Prop) -> bool {
        match self {
            ValueTest::Cmp(op, constant) => Prop::binary_cmp(op, &value, constant),
            ValueTest::Str(op, constant) => Prop::string_cmp(op, &value, constant),
            ValueTest::In(members, negated) => members.contains(&HashableProp(value)) != *negated,
        }
    }
}

/// Whether the test holds for any value of the stream, stopping at the first
/// that does.
fn any_holds(test: &ValueTest, mut values: BoxedLIter<'_, Prop>) -> Option<Prop> {
    Some(Prop::Bool(values.any(|v| test.holds(v))))
}

/// Whether the test holds for every value of the stream, stopping at the
/// first that does not. An empty history has no value the test holds for,
/// so the answer is no there too.
fn all_hold(test: &ValueTest, mut values: BoxedLIter<'_, Prop>) -> Option<Prop> {
    let mut seen = false;
    let every = values.all(|v| {
        seen = true;
        test.holds(v)
    });
    Some(Prop::Bool(seen && every))
}

/// `any()` over a test of each history value of a node.
#[derive(Clone)]
pub(crate) struct StreamedAnyNodeOp<'g> {
    pub(crate) history: Arc<dyn NodeHistory + 'g>,
    pub(crate) test: ValueTest,
}

impl<'g> NodeOp for StreamedAnyNodeOp<'g> {
    type Output = Option<Prop>;

    fn domain(&self, _storage: &GraphStorage) -> NodeList {
        self.history.domain()
    }

    fn prop_type(&self) -> PropType {
        PropType::Bool
    }

    fn apply(&self, _storage: &GraphStorage, node: VID) -> Option<Prop> {
        any_holds(&self.test, self.history.values(node))
    }
}

/// `all()` over a test of each history value of a node.
#[derive(Clone)]
pub(crate) struct StreamedAllNodeOp<'g> {
    pub(crate) history: Arc<dyn NodeHistory + 'g>,
    pub(crate) test: ValueTest,
}

impl<'g> NodeOp for StreamedAllNodeOp<'g> {
    type Output = Option<Prop>;

    fn domain(&self, _storage: &GraphStorage) -> NodeList {
        self.history.domain()
    }

    fn prop_type(&self) -> PropType {
        PropType::Bool
    }

    fn apply(&self, _storage: &GraphStorage, node: VID) -> Option<Prop> {
        all_hold(&self.test, self.history.values(node))
    }
}

/// `any()` over a test of each history value of an edge.
#[derive(Clone)]
pub(crate) struct StreamedAnyEdgeOp<'g> {
    pub(crate) history: Arc<dyn EdgeHistory + 'g>,
    pub(crate) test: ValueTest,
}

impl<'g> EdgeOp for StreamedAnyEdgeOp<'g> {
    type Output = Option<Prop>;

    fn prop_type(&self) -> PropType {
        PropType::Bool
    }

    fn apply(&self, _storage: &GraphStorage, edge: EdgeEntryRef) -> Option<Prop> {
        any_holds(&self.test, self.history.values(edge, EdgeAt::Whole))
    }

    fn apply_layer(
        &self,
        _storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
    ) -> Option<Prop> {
        any_holds(&self.test, self.history.values(edge, EdgeAt::Layer(layer)))
    }

    fn apply_exploded(
        &self,
        _storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
        t: EventTime,
    ) -> Option<Prop> {
        any_holds(
            &self.test,
            self.history.values(edge, EdgeAt::Exploded(layer, t)),
        )
    }
}

/// `all()` over a test of each history value of an edge.
#[derive(Clone)]
pub(crate) struct StreamedAllEdgeOp<'g> {
    pub(crate) history: Arc<dyn EdgeHistory + 'g>,
    pub(crate) test: ValueTest,
}

impl<'g> EdgeOp for StreamedAllEdgeOp<'g> {
    type Output = Option<Prop>;

    fn prop_type(&self) -> PropType {
        PropType::Bool
    }

    fn apply(&self, _storage: &GraphStorage, edge: EdgeEntryRef) -> Option<Prop> {
        all_hold(&self.test, self.history.values(edge, EdgeAt::Whole))
    }

    fn apply_layer(
        &self,
        _storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
    ) -> Option<Prop> {
        all_hold(&self.test, self.history.values(edge, EdgeAt::Layer(layer)))
    }

    fn apply_exploded(
        &self,
        _storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
        t: EventTime,
    ) -> Option<Prop> {
        all_hold(
            &self.test,
            self.history.values(edge, EdgeAt::Exploded(layer, t)),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use raphtory_api::core::entities::properties::prop::IntoProp;

    fn stream(values: &[i64]) -> BoxedLIter<'static, Prop> {
        values
            .iter()
            .map(|v| v.into_prop())
            .collect::<Vec<_>>()
            .into_iter()
            .into_dyn_boxed()
    }

    fn reduced(agg: Agg, values: &'static [i64]) -> Option<Prop> {
        reduce(
            agg,
            false,
            || stream(values),
            || {
                stream(values)
                    .collect::<Vec<_>>()
                    .into_iter()
                    .rev()
                    .into_dyn_boxed()
            },
        )
    }

    #[test]
    fn each_aggregation_reads_from_the_end_it_needs() {
        let history = &[3i64, 1, 2];
        assert_eq!(reduced(Agg::Earliest, history), Some(3i64.into_prop()));
        assert_eq!(reduced(Agg::First, history), Some(3i64.into_prop()));
        assert_eq!(reduced(Agg::Latest, history), Some(2i64.into_prop()));
        assert_eq!(reduced(Agg::Last, history), Some(2i64.into_prop()));
        assert_eq!(reduced(Agg::Min, history), Some(1i64.into_prop()));
        assert_eq!(reduced(Agg::Max, history), Some(3i64.into_prop()));
        assert_eq!(reduced(Agg::Sum, history), Some(6i64.into_prop()));
        assert_eq!(reduced(Agg::Avg, history), Some(2f64.into_prop()));
        assert_eq!(reduced(Agg::Len, history), Some(3u64.into_prop()));
    }

    #[test]
    fn an_empty_history_has_no_aggregate_and_a_zero_length() {
        for agg in [
            Agg::Earliest,
            Agg::Latest,
            Agg::First,
            Agg::Last,
            Agg::Min,
            Agg::Max,
            Agg::Sum,
            Agg::Avg,
        ] {
            assert_eq!(reduced(agg, &[]), None, "{agg:?}");
        }
        assert_eq!(reduced(Agg::Len, &[]), Some(0u64.into_prop()));
    }

    #[test]
    fn a_history_of_lists_reduces_each_update() {
        let updates = || {
            vec![
                Prop::list([1i64, 2]).unwrap(),
                Prop::list([5i64]).unwrap(),
                Prop::list(Vec::<i64>::new()).unwrap(),
            ]
            .into_iter()
            .into_dyn_boxed()
        };
        let history_type = PropType::List(Box::new(PropType::List(Box::new(PropType::I64))));
        assert!(updates_are_lists(&history_type));
        assert!(!updates_are_lists(&PropType::List(Box::new(PropType::I64))));
        let sums = reduce(Agg::Sum, true, updates, updates);
        assert_eq!(sums, Some(Prop::list([3i64, 5]).unwrap()));
        let latest = reduce(Agg::Latest, true, updates, || {
            updates()
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .into_dyn_boxed()
        });
        assert_eq!(latest, Some(Prop::list(Vec::<i64>::new()).unwrap()));
        let lasts = reduce(Agg::Last, true, updates, || {
            updates()
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .into_dyn_boxed()
        });
        assert_eq!(lasts, Some(Prop::list([2i64, 5]).unwrap()));
    }

    #[test]
    fn any_and_all_stop_at_the_deciding_value() {
        let gt = ValueTest::Cmp(BinaryOp::Gt, 2i64.into_prop());
        let mut pulled = 0usize;
        let counted = Box::new([1i64, 5, 7].into_iter().map(|v| {
            pulled += 1;
            v.into_prop()
        })) as BoxedLIter<'_, Prop>;
        assert_eq!(any_holds(&gt, counted), Some(Prop::Bool(true)));
        assert_eq!(pulled, 2);

        let mut pulled = 0usize;
        let counted = Box::new([5i64, 1, 7].into_iter().map(|v| {
            pulled += 1;
            v.into_prop()
        })) as BoxedLIter<'_, Prop>;
        assert_eq!(all_hold(&gt, counted), Some(Prop::Bool(false)));
        assert_eq!(pulled, 2);
    }

    #[test]
    fn an_empty_history_satisfies_neither_any_nor_all() {
        let gt = ValueTest::Cmp(BinaryOp::Gt, 2i64.into_prop());
        assert_eq!(any_holds(&gt, stream(&[])), Some(Prop::Bool(false)));
        assert_eq!(all_hold(&gt, stream(&[])), Some(Prop::Bool(false)));
    }

    #[test]
    fn the_set_test_honours_negation() {
        let members: Arc<HashSet<HashableProp>> =
            Arc::new([HashableProp(1i64.into_prop())].into_iter().collect());
        let is_in = ValueTest::In(members.clone(), false);
        let not_in = ValueTest::In(members, true);
        assert_eq!(any_holds(&is_in, stream(&[2, 1])), Some(Prop::Bool(true)));
        assert_eq!(all_hold(&not_in, stream(&[2, 1])), Some(Prop::Bool(false)));
        assert_eq!(all_hold(&not_in, stream(&[2, 3])), Some(Prop::Bool(true)));
    }
}
