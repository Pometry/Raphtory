#![allow(unused_imports)]
#![allow(dead_code)]

use crate::{
    core::{
        storage::timeindex::{AsTime, TimeIndexEntry},
        utils::iter::GenLockedIter,
    },
    db::{
        api::{
            properties::{
                internal::{PropertiesOps, TemporalPropertiesOps},
                TemporalPropertyView,
            },
            state::{
                ops,
                ops::{node::NodeOp, EarliestTime, HistoryOp},
                LazyNodeState,
            },
            view,
            view::{
                internal::{
                    filtered_node::FilteredNodeStorageOps, EdgeTimeSemanticsOps,
                    GraphTimeSemanticsOps, InternalLayerOps, NodeTimeSemanticsOps,
                },
                BaseNodeViewOps, BoxableGraphView, BoxedLIter, IntoDynBoxed,
            },
        },
        graph::{
            edge::{edge_valid_layer, EdgeView},
            node::NodeView,
            nodes::Nodes,
            path::{PathFromGraph, PathFromNode},
        },
    },
    errors::GraphError,
    prelude::*,
    python::types::repr::{iterator_repr, Repr},
};
use chrono::{DateTime, Utc};
use itertools::Itertools;
use raphtory_api::{
    core::{
        entities::LayerIds,
        storage::timeindex::{TimeError, TimeIndexOps},
    },
    iter::BoxedIter,
};
use raphtory_storage::core_ops::CoreGraphOps;
use std::{iter, marker::PhantomData, slice, slice::Iter, sync::Arc};

pub trait InternalHistoryOps: Send + Sync {
    fn iter(&self) -> BoxedLIter<TimeIndexEntry>;
    fn iter_rev(&self) -> BoxedLIter<TimeIndexEntry>;
    fn earliest_time(&self) -> Option<TimeIndexEntry>;
    fn latest_time(&self) -> Option<TimeIndexEntry>;
    // override if we want more efficient implementation
    fn len(&self) -> usize {
        self.iter().count()
    }
}

// TODO: Doesn't support deletions of edges yet
#[derive(Debug, Clone, Copy)]
pub struct History<'a, T>(pub T, PhantomData<&'a T>);

impl<'a, T: InternalHistoryOps> Repr for History<'a, T> {
    fn repr(&self) -> String {
        format!("History({})", iterator_repr(self.iter()))
    }
}

impl<'a, T: InternalHistoryOps + 'a> History<'a, T> {
    pub fn new(item: T) -> Self {
        Self(item, PhantomData)
    }

    // converts operations to return timestamps instead of TimeIndexEntry
    pub fn t(self) -> HistoryTimestamp<T> {
        HistoryTimestamp(self.0)
    }

    // converts operations to return date times instead of TimeIndexEntry
    pub fn dt(self) -> HistoryDateTime<T> {
        HistoryDateTime(self.0)
    }

    // converts operations to return secondary time information inside TimeIndexEntry
    pub fn s(self) -> HistorySecondary<T> {
        HistorySecondary(self.0)
    }

    pub fn intervals(self) -> Intervals<T> {
        Intervals(self.0)
    }

    pub fn merge<R: InternalHistoryOps>(self, right: History<R>) -> History<MergedHistory<T, R>> {
        History::new(MergedHistory::new(self.0, right.0))
    }

    fn into_iter_rev(self) -> BoxedLIter<'a, TimeIndexEntry> {
        GenLockedIter::from(self.0, |item| item.iter_rev()).into_dyn_boxed()
    }

    pub fn iter(&self) -> BoxedLIter<TimeIndexEntry> {
        self.0.iter()
    }

    pub fn iter_rev(&self) -> BoxedLIter<TimeIndexEntry> {
        self.0.iter_rev()
    }

    pub fn collect(&self) -> Vec<TimeIndexEntry> {
        self.0.iter().collect_vec()
    }

    pub fn collect_rev(&self) -> Vec<TimeIndexEntry> {
        self.0.iter_rev().collect_vec()
    }

    pub fn is_empty(&self) -> bool {
        self.iter().next().is_none()
    }

    pub fn earliest_time(&self) -> Option<TimeIndexEntry> {
        self.0.earliest_time()
    }

    pub fn latest_time(&self) -> Option<TimeIndexEntry> {
        self.0.latest_time()
    }

    pub fn print(&self, prelude: &str) {
        println!("{}{:?}", prelude, self.0.iter().collect::<Vec<_>>());
    }
}

impl<T: InternalHistoryOps + 'static> History<'_, T> {
    pub fn into_arc(self) -> History<'static, Arc<dyn InternalHistoryOps>> {
        History::new(Arc::new(self.0))
    }
}

impl<'a, T: InternalHistoryOps + 'a> IntoIterator for History<'a, T> {
    type Item = TimeIndexEntry;
    type IntoIter = BoxedLIter<'a, TimeIndexEntry>;

    fn into_iter(self) -> Self::IntoIter {
        GenLockedIter::from(self.0, |item| item.iter()).into_dyn_boxed()
    }
}

impl<'b, T: InternalHistoryOps + 'b, L, I: Copy> PartialEq<L> for History<'b, T>
where
    for<'a> &'a L: IntoIterator<Item = &'a I>,
    TimeIndexEntry: PartialEq<I>,
{
    fn eq(&self, other: &L) -> bool {
        self.iter().eq(other.into_iter().copied())
    }
}

impl<'a, T: InternalHistoryOps + 'a> PartialEq for History<'a, T> {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}

impl<'a, T: InternalHistoryOps + 'a> Eq for History<'a, T> {}

impl<'a, T: InternalHistoryOps + 'a> std::hash::Hash for History<'a, T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        for item in self.iter() {
            item.hash(state);
        }
    }
}

impl<T: InternalHistoryOps + ?Sized> InternalHistoryOps for Box<T> {
    fn iter(&self) -> BoxedLIter<TimeIndexEntry> {
        T::iter(self)
    }

    fn iter_rev(&self) -> BoxedLIter<TimeIndexEntry> {
        T::iter_rev(self)
    }

    fn earliest_time(&self) -> Option<TimeIndexEntry> {
        T::earliest_time(self)
    }

    fn latest_time(&self) -> Option<TimeIndexEntry> {
        T::latest_time(self)
    }
}

impl<T: InternalHistoryOps + ?Sized> InternalHistoryOps for Arc<T> {
    fn iter(&self) -> BoxedLIter<TimeIndexEntry> {
        T::iter(self)
    }

    fn iter_rev(&self) -> BoxedLIter<TimeIndexEntry> {
        T::iter_rev(self)
    }

    fn earliest_time(&self) -> Option<TimeIndexEntry> {
        T::earliest_time(self)
    }

    fn latest_time(&self) -> Option<TimeIndexEntry> {
        T::latest_time(self)
    }
}

impl<T: InternalHistoryOps> InternalHistoryOps for Vec<History<'_, T>> {
    fn iter(&self) -> BoxedLIter<TimeIndexEntry> {
        self.as_slice() // had to use slice notation to get Vec::iter() instead of InternalHistoryOps::iter()
            .iter()
            .map(|history| history.iter())
            .kmerge()
            .into_dyn_boxed()
    }

    fn iter_rev(&self) -> BoxedLIter<TimeIndexEntry> {
        self.as_slice()
            .iter()
            .map(|history| history.iter_rev())
            .kmerge_by(|a, b| a >= b)
            .into_dyn_boxed()
    }

    fn earliest_time(&self) -> Option<TimeIndexEntry> {
        self.as_slice()
            .iter()
            .filter_map(|history| history.earliest_time())
            .min()
    }

    fn latest_time(&self) -> Option<TimeIndexEntry> {
        self.as_slice()
            .iter()
            .filter_map(|history| history.latest_time())
            .max()
    }
}

/// Separate from CompositeHistory in that it can only hold two items. They can be nested.
/// More efficient because we are calling iter.merge() instead of iter.kmerge(). Efficiency benefits are lost if we nest these objects too much
/// TODO: Write benchmark to evaluate performance benefit tradeoff (ie when there are no more performance benefits)
#[derive(Debug, Clone, Copy)]
pub struct MergedHistory<L, R> {
    left: L,
    right: R,
}

impl<L: InternalHistoryOps, R: InternalHistoryOps> MergedHistory<L, R> {
    pub fn new(left: L, right: R) -> Self {
        Self { left, right }
    }
}

impl<L: InternalHistoryOps, R: InternalHistoryOps> InternalHistoryOps for MergedHistory<L, R> {
    fn iter(&self) -> BoxedLIter<TimeIndexEntry> {
        self.left.iter().merge(self.right.iter()).into_dyn_boxed()
    }

    fn iter_rev(&self) -> BoxedLIter<TimeIndexEntry> {
        self.left
            .iter_rev()
            .merge_by(self.right.iter_rev(), |a, b| a >= b)
            .into_dyn_boxed()
    }

    fn earliest_time(&self) -> Option<TimeIndexEntry> {
        self.left.earliest_time().min(self.right.earliest_time())
    }

    fn latest_time(&self) -> Option<TimeIndexEntry> {
        self.left.latest_time().max(self.right.latest_time())
    }
}

/// Holds a vector of multiple items implementing InternalHistoryOps. If the composite will only hold 2 items, MergedHistory is more efficient.
/// TODO: Write benchmark to see performance hit of Arcs
#[derive(Clone)]
pub struct CompositeHistory<'a> {
    history_objects: Vec<Arc<dyn InternalHistoryOps + 'a>>,
}

impl<'a> CompositeHistory<'a> {
    pub fn new(history_objects: Vec<Arc<dyn InternalHistoryOps + 'a>>) -> Self {
        Self { history_objects }
    }
}

// Note: All the items held by their respective History objects must already be of type Arc<T>
pub fn compose_multiple_histories<'a>(
    objects: impl IntoIterator<Item = History<'a, Arc<dyn InternalHistoryOps + 'a>>>,
) -> History<'a, CompositeHistory<'a>> {
    History::new(CompositeHistory::new(
        objects.into_iter().map(|h| h.0).collect(),
    ))
}

// Note: Items supplied by the iterator must already be of type Arc<T>
pub fn compose_history_from_items<'a>(
    objects: impl IntoIterator<Item = Arc<dyn InternalHistoryOps + 'a>>,
) -> History<'a, CompositeHistory<'a>> {
    History::new(CompositeHistory::new(objects.into_iter().collect()))
}

impl<'a> InternalHistoryOps for CompositeHistory<'a> {
    fn iter(&self) -> BoxedLIter<TimeIndexEntry> {
        self.history_objects
            .iter()
            .map(|object| object.iter())
            .kmerge()
            .into_dyn_boxed()
    }

    fn iter_rev(&self) -> BoxedLIter<TimeIndexEntry> {
        self.history_objects
            .iter()
            .map(|object| object.iter_rev())
            .kmerge_by(|a, b| a >= b)
            .into_dyn_boxed()
    }

    fn earliest_time(&self) -> Option<TimeIndexEntry> {
        self.history_objects
            .iter()
            .filter_map(|history| history.earliest_time())
            .min()
    }

    fn latest_time(&self) -> Option<TimeIndexEntry> {
        self.history_objects
            .iter()
            .filter_map(|history| history.latest_time())
            .max()
    }
}

impl<'graph, G: GraphViewOps<'graph> + Send + Sync, GH: GraphViewOps<'graph> + Send + Sync>
    InternalHistoryOps for NodeView<'graph, G, GH>
{
    fn iter(&self) -> BoxedLIter<TimeIndexEntry> {
        let semantics = self.graph.node_time_semantics();
        let node = self.graph.core_node(self.node);
        GenLockedIter::from(node, move |node| {
            semantics
                .node_history(node.as_ref(), &self.graph)
                .into_dyn_boxed()
        })
        .into_dyn_boxed()
    }

    fn iter_rev(&self) -> BoxedLIter<TimeIndexEntry> {
        let semantics = self.graph.node_time_semantics();
        let node = self.graph.core_node(self.node);
        GenLockedIter::from(node, move |node| {
            semantics
                .node_history_rev(node.as_ref(), &self.graph)
                .into_dyn_boxed()
        })
        .into_dyn_boxed()
    }

    fn earliest_time(&self) -> Option<TimeIndexEntry> {
        ops::EarliestTime {
            graph: self.graph().clone(),
        }
        .apply(self.graph.core_graph(), self.node)
    }

    fn latest_time(&self) -> Option<TimeIndexEntry> {
        ops::LatestTime {
            graph: self.graph().clone(),
        }
        .apply(self.graph.core_graph(), self.node)
    }
}

impl<G: BoxableGraphView + Clone> InternalHistoryOps for EdgeView<G> {
    fn iter(&self) -> BoxedLIter<TimeIndexEntry> {
        let g = &self.graph;
        let e = self.edge;
        if edge_valid_layer(g, e) {
            match e.time() {
                Some(t) => iter::once(t).into_dyn_boxed(),
                None => {
                    let time_semantics = g.edge_time_semantics();
                    let edge = g.core_edge(e.pid());
                    match e.layer() {
                        None => GenLockedIter::from(edge, move |edge| {
                            time_semantics
                                .edge_history(edge.as_ref(), g, g.layer_ids())
                                .map(|(ti, _)| ti)
                                .into_dyn_boxed()
                        })
                        .into_dyn_boxed(),
                        Some(layer) => {
                            let layer_ids = LayerIds::One(layer);
                            GenLockedIter::from((edge, layer_ids), move |(edge, layer_ids)| {
                                time_semantics
                                    .edge_history(edge.as_ref(), g, layer_ids)
                                    .map(|(ti, _)| ti)
                                    .into_dyn_boxed()
                            })
                            .into_dyn_boxed()
                        }
                    }
                }
            }
        } else {
            iter::empty().into_dyn_boxed()
        }
    }

    fn iter_rev(&self) -> BoxedLIter<TimeIndexEntry> {
        let g = &self.graph;
        let e = self.edge;
        if edge_valid_layer(&g, e) {
            match e.time() {
                Some(t) => iter::once(t).into_dyn_boxed(),
                None => {
                    let time_semantics = g.edge_time_semantics();
                    let edge = g.core_edge(e.pid());
                    match e.layer() {
                        None => GenLockedIter::from(edge, move |edge| {
                            time_semantics
                                .edge_history_rev(edge.as_ref(), g, g.layer_ids())
                                .map(|(ti, _)| ti)
                                .into_dyn_boxed()
                        })
                        .into_dyn_boxed(),
                        Some(layer) => {
                            let layer_ids = LayerIds::One(layer);
                            GenLockedIter::from((edge, layer_ids), move |(edge, layer_ids)| {
                                time_semantics
                                    .edge_history_rev(edge.as_ref(), g, layer_ids)
                                    .map(|(ti, _)| ti)
                                    .into_dyn_boxed()
                            })
                            .into_dyn_boxed()
                        }
                    }
                }
            }
        } else {
            iter::empty().into_dyn_boxed()
        }
    }

    fn earliest_time(&self) -> Option<TimeIndexEntry> {
        EdgeViewOps::earliest_time(self)
    }

    fn latest_time(&self) -> Option<TimeIndexEntry> {
        EdgeViewOps::latest_time(self)
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> InternalHistoryOps
    for LazyNodeState<'graph, HistoryOp<'graph, GH>, G, GH>
{
    fn iter(&self) -> BoxedLIter<TimeIndexEntry> {
        // consuming the history objects is fine here because they get recreated on subsequent iter() calls
        NodeStateOps::iter_values(self)
            .map(|history| history.into_iter())
            .kmerge()
            .into_dyn_boxed()
    }

    fn iter_rev(&self) -> BoxedLIter<TimeIndexEntry> {
        // consuming the history objects is fine here because they get recreated on subsequent iter_rev() calls
        NodeStateOps::iter_values(self)
            .map(|history| history.into_iter_rev())
            .kmerge_by(|a, b| a >= b)
            .into_dyn_boxed()
    }

    fn earliest_time(&self) -> Option<TimeIndexEntry> {
        NodeStateOps::iter_values(self)
            .filter_map(|history| history.earliest_time())
            .min()
    }

    fn latest_time(&self) -> Option<TimeIndexEntry> {
        NodeStateOps::iter_values(self)
            .filter_map(|history| history.latest_time())
            .max()
    }
}

impl<P: PropertiesOps> InternalHistoryOps for TemporalPropertyView<P> {
    fn iter(&self) -> BoxedLIter<TimeIndexEntry> {
        self.props
            .temporal_iter(self.id)
            .map(|(t, _)| t)
            .into_dyn_boxed()
    }

    fn iter_rev(&self) -> BoxedLIter<TimeIndexEntry> {
        self.props
            .temporal_iter_rev(self.id)
            .map(|(t, _)| t)
            .into_dyn_boxed()
    }

    fn earliest_time(&self) -> Option<TimeIndexEntry> {
        InternalHistoryOps::iter(self).next()
    }

    fn latest_time(&self) -> Option<TimeIndexEntry> {
        InternalHistoryOps::iter_rev(self).next()
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> InternalHistoryOps
    for PathFromNode<'graph, G, GH>
{
    fn iter(&self) -> BoxedLIter<TimeIndexEntry> {
        self.iter()
            .map(|nodeview| GenLockedIter::from(nodeview, move |node| node.iter()))
            .kmerge()
            .into_dyn_boxed()
    }

    fn iter_rev(&self) -> BoxedLIter<TimeIndexEntry> {
        self.iter()
            .map(|nodeview| GenLockedIter::from(nodeview, move |node| node.iter_rev()))
            .kmerge_by(|a, b| a >= b)
            .into_dyn_boxed()
    }

    fn earliest_time(&self) -> Option<TimeIndexEntry> {
        self.iter()
            .filter_map(|nodeview| InternalHistoryOps::earliest_time(&nodeview))
            .min()
    }

    fn latest_time(&self) -> Option<TimeIndexEntry> {
        self.iter()
            .filter_map(|nodeview| InternalHistoryOps::latest_time(&nodeview))
            .max()
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> InternalHistoryOps
    for PathFromGraph<'graph, G, GH>
{
    fn iter(&self) -> BoxedLIter<TimeIndexEntry> {
        self.iter()
            .map(|path_from_node| {
                GenLockedIter::from(path_from_node, |item| InternalHistoryOps::iter(item))
            })
            .kmerge()
            .into_dyn_boxed()
    }

    fn iter_rev(&self) -> BoxedLIter<TimeIndexEntry> {
        self.iter()
            .map(|path_from_node| {
                GenLockedIter::from(path_from_node, |item| InternalHistoryOps::iter_rev(item))
            })
            .kmerge_by(|a, b| a >= b)
            .into_dyn_boxed()
    }

    fn earliest_time(&self) -> Option<TimeIndexEntry> {
        self.iter()
            .filter_map(|path_from_node| InternalHistoryOps::earliest_time(&path_from_node))
            .min()
    }

    fn latest_time(&self) -> Option<TimeIndexEntry> {
        self.iter()
            .filter_map(|path_from_node| InternalHistoryOps::latest_time(&path_from_node))
            .max()
    }
}

// converts operations to return timestamps instead of TimeIndexEntry
#[derive(Debug, Clone, Copy)]
pub struct HistoryTimestamp<T>(T);

impl<T: InternalHistoryOps> HistoryTimestamp<T> {
    pub fn new(item: T) -> Self {
        Self(item)
    }

    pub fn iter(&self) -> BoxedLIter<i64> {
        self.0.iter().map(|t| t.0).into_dyn_boxed()
    }

    pub fn iter_rev(&self) -> BoxedLIter<i64> {
        self.0.iter_rev().map(|t| t.0).into_dyn_boxed()
    }

    pub fn collect(&self) -> Vec<i64> {
        self.0.iter().map(|t| t.0).collect()
    }

    pub fn collect_rev(&self) -> Vec<i64> {
        self.0.iter_rev().map(|t| t.0).collect()
    }
}

// converts operations to return date times instead of TimeIndexEntry
#[derive(Debug, Clone, Copy)]
pub struct HistoryDateTime<T>(T);

impl<T: InternalHistoryOps> HistoryDateTime<T> {
    pub fn new(item: T) -> Self {
        Self(item)
    }

    pub fn iter(&self) -> BoxedLIter<Result<DateTime<Utc>, TimeError>> {
        self.0.iter().map(|t| t.dt()).into_dyn_boxed()
    }

    pub fn iter_rev(&self) -> BoxedLIter<Result<DateTime<Utc>, TimeError>> {
        self.0.iter_rev().map(|t| t.dt()).into_dyn_boxed()
    }

    pub fn collect(&self) -> Result<Vec<DateTime<Utc>>, TimeError> {
        self.0
            .iter()
            .map(|x| x.dt())
            .collect::<Result<Vec<_>, TimeError>>()
    }

    pub fn collect_rev(&self) -> Result<Vec<DateTime<Utc>>, TimeError> {
        self.0
            .iter_rev()
            .map(|x| x.dt())
            .collect::<Result<Vec<_>, TimeError>>()
    }
}

// converts operations to return secondary time information inside TimeIndexEntry
#[derive(Debug, Clone, Copy)]
pub struct HistorySecondary<T>(T);

impl<T: InternalHistoryOps> HistorySecondary<T> {
    pub fn new(item: T) -> Self {
        Self(item)
    }

    pub fn iter(&self) -> BoxedLIter<usize> {
        self.0.iter().map(|t| t.1).into_dyn_boxed()
    }

    pub fn iter_rev(&self) -> BoxedLIter<usize> {
        self.0.iter_rev().map(|t| t.1).into_dyn_boxed()
    }

    pub fn collect(&self) -> Vec<usize> {
        self.0.iter().map(|x| x.1).collect()
    }

    pub fn collect_rev(&self) -> Vec<usize> {
        self.0.iter_rev().map(|x| x.1).collect()
    }
}

// Holds a reference to the inner item in history object
#[derive(Debug, Clone, Copy)]
pub struct Intervals<T>(T);

impl<T: InternalHistoryOps> Intervals<T> {
    pub fn new(item: T) -> Self {
        Intervals(item)
    }

    pub fn collect(&self) -> Vec<i64> {
        self.iter().collect()
    }

    pub fn iter(&self) -> BoxedLIter<i64> {
        self.0
            .iter()
            .map(|t| t.0)
            .tuple_windows()
            .map(|(w1, w2)| w2 - w1)
            .into_dyn_boxed()
    }

    pub fn iter_rev(&self) -> BoxedLIter<i64> {
        self.0
            .iter_rev()
            .map(|t| t.0)
            .tuple_windows()
            .map(|(w1, w2)| w2 - w1)
            .into_dyn_boxed()
    }

    pub fn mean(&self) -> Option<f64> {
        if self.iter().next().is_none() {
            return None;
        }
        // count and sum in one pass of the iterator
        let (len, sum) = self
            .iter()
            .fold((0i64, 0i64), |(count, sum), item| (count + 1, sum + item));
        Some(sum as f64 / len as f64)
    }

    pub fn median(&self) -> Option<f64> {
        if self.iter().next().is_none() {
            return None;
        }
        let mut intervals: Vec<i64> = self.iter().collect();
        intervals.sort_unstable();

        let mid = intervals.len() / 2;
        if intervals.len() % 2 == 0 {
            Some((intervals[mid - 1] as f64 + intervals[mid] as f64) / 2.0)
        } else {
            Some(intervals[mid] as f64)
        }
    }

    pub fn max(&self) -> Option<i64> {
        self.iter().max()
    }

    pub fn min(&self) -> Option<i64> {
        self.iter().min()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::api::view::internal::CoreGraphOps;

    #[test]
    fn test_neighbours_history() -> Result<(), Box<dyn std::error::Error>> {
        let graph = Graph::new();
        let node = graph.add_node(1, "node", NO_PROPS, None).unwrap();
        let node2 = graph.add_node(2, "node2", NO_PROPS, None).unwrap();
        let node3 = graph.add_node(3, "node3", NO_PROPS, None).unwrap();
        let edge = graph.add_edge(4, &node, &node2, NO_PROPS, None).unwrap();
        let edge2 = graph.add_edge(5, &node, &node3, NO_PROPS, None).unwrap();
        let node4 = graph.add_node(6, "node4", NO_PROPS, None).unwrap();
        let edge3 = graph.add_edge(7, &node2, &node4, NO_PROPS, None).unwrap();

        let history = graph.node("node").unwrap().neighbours().combined_history();
        assert_eq!(history.earliest_time().unwrap().t(), 2);
        assert_eq!(history.t().collect(), [2, 3, 4, 5, 7]);

        let history2 = graph.nodes().neighbours().combined_history();
        assert_eq!(history2.earliest_time().unwrap().t(), 1);
        assert_eq!(history2.latest_time().unwrap().t(), 7);
        let mut history2_collected = history2.t().collect();
        history2_collected.dedup();
        assert_eq!(history2_collected, [1, 2, 3, 4, 5, 6, 7]);

        Ok(())
    }

    // History vs HistoryRef, both have the same lifetime on History object even though History owns a clone of temporal property
    #[test]
    // fn test_history_vs_historyref() -> Result<(), Box<dyn std::error::Error>> {
    //     let graph = Graph::new();
    //     let node = graph.add_node(0, 1, [("cool", Prop::Bool(true))], None).unwrap();
    //     graph.add_node(3, 1, [("cool", Prop::Bool(false))], None).unwrap();
    //
    //     let temp_prop = node.properties().temporal().get("cool").unwrap();
    //     let history = temp_prop.history();
    //     drop(temp_prop);
    //     println!("{:?}", history.iter().collect::<Vec<_>>());
    //
    //     Ok(())
    // }
    #[test]
    fn test_intervals() -> Result<(), Box<dyn std::error::Error>> {
        let graph = Graph::new();
        let node = graph.add_node(1, "node", NO_PROPS, None).unwrap();
        graph.add_node(4, "node", NO_PROPS, None).unwrap();
        graph.add_node(10, "node", NO_PROPS, None).unwrap();
        graph.add_node(30, "node", NO_PROPS, None).unwrap();
        let interval = Intervals(&node);
        assert_eq!(interval.collect(), &[3, 6, 20]);

        // make sure there are no intervals (1 time entry)
        let node2 = graph.add_node(1, "node2", NO_PROPS, None).unwrap();
        let interval2 = Intervals(&node2);
        assert_eq!(interval2.collect(), Vec::<i64>::new());
        Ok(())
    }

    #[test]
    fn test_intervals_same_timestamp() -> Result<(), Box<dyn std::error::Error>> {
        let graph = Graph::new();
        let node = graph.add_node(1, "node", NO_PROPS, None).unwrap();
        graph.add_node(1, "node", NO_PROPS, None).unwrap();
        let interval = Intervals(&node);
        assert_eq!(interval.collect(), &[0]);

        graph.add_node(2, "node", NO_PROPS, None).unwrap();
        assert_eq!(interval.collect(), &[0, 1]);
        Ok(())
    }

    #[test]
    fn test_intervals_mean() -> Result<(), Box<dyn std::error::Error>> {
        let graph = Graph::new();
        let node = graph.add_node(1, "node", NO_PROPS, None).unwrap();
        graph.add_node(4, "node", NO_PROPS, None).unwrap();
        graph.add_node(10, "node", NO_PROPS, None).unwrap();
        graph.add_node(30, "node", NO_PROPS, None).unwrap();
        let interval = Intervals(&node);
        assert_eq!(interval.mean(), Some(29f64 / 3f64));

        // make sure mean is None if there is no interval to be calculated (1 time entry)
        let node2 = graph.add_node(1, "node2", NO_PROPS, None).unwrap();
        let interval2 = Intervals(&node2);
        assert_eq!(interval2.mean(), None);
        Ok(())
    }

    #[test]
    fn test_intervals_median() -> Result<(), Box<dyn std::error::Error>> {
        let graph = Graph::new();
        let node = graph.add_node(1, "node", NO_PROPS, None).unwrap();
        graph.add_node(30, "node", NO_PROPS, None).unwrap();
        graph.add_node(31, "node", NO_PROPS, None).unwrap();
        graph.add_node(40, "node", NO_PROPS, None).unwrap(); // intervals are 29, 1, 9
        let interval = Intervals(&node);
        assert_eq!(interval.median(), Some(9.0));

        // make sure median is None if there is no interval to be calculated (1 time entry)
        let node2 = graph.add_node(1, "node2", NO_PROPS, None).unwrap();
        let interval2 = Intervals(&node2);
        assert_eq!(interval2.median(), None);
        Ok(())
    }

    #[test]
    fn test_intervals_max() -> Result<(), Box<dyn std::error::Error>> {
        let graph = Graph::new();
        let node = graph.add_node(1, "node", NO_PROPS, None).unwrap();
        graph.add_node(30, "node", NO_PROPS, None).unwrap();
        graph.add_node(31, "node", NO_PROPS, None).unwrap();
        graph.add_node(40, "node", NO_PROPS, None).unwrap(); // intervals are 29, 1, 9
        let interval = Intervals(&node);
        assert_eq!(interval.max(), Some(29));

        // make sure max is None if there is no interval to be calculated (1 time entry)
        let node2 = graph.add_node(1, "node2", NO_PROPS, None).unwrap();
        let interval2 = Intervals(&node2);
        assert_eq!(interval2.max(), None);
        Ok(())
    }

    // test nodes and edges
    #[test]
    fn test_basic() -> Result<(), Box<dyn std::error::Error>> {
        let graph = Graph::new();
        let dumbledore_node = graph
            .add_node(1, "Dumbledore", [("type", Prop::str("Character"))], None)
            .unwrap();

        let harry_node = graph
            .add_node(2, "Harry", [("type", Prop::str("Character"))], None)
            .unwrap();

        let character_edge = graph
            .add_edge(
                3,
                "Dumbledore",
                "Harry",
                [("meeting", Prop::str("Character Co-occurrence"))],
                None,
            )
            .unwrap();

        // create dumbledore node history object
        let dumbledore_node_history_object = History::new(dumbledore_node.clone());
        assert_eq!(
            dumbledore_node_history_object.iter().collect_vec(),
            vec![TimeIndexEntry::new(1, 0), TimeIndexEntry::new(3, 2)]
        );

        // create Harry node history object
        let harry_node_history_object = History::new(harry_node.clone());
        assert_eq!(
            harry_node_history_object.iter().collect_vec(),
            vec![TimeIndexEntry::new(2, 1), TimeIndexEntry::new(3, 2)]
        );

        // create edge history object
        let edge_history_object = History::new(character_edge.clone());
        assert_eq!(
            edge_history_object.iter().collect_vec(),
            vec![TimeIndexEntry::new(3, 2)]
        );

        // create Composite History Object
        let tmp_vector: Vec<Arc<dyn InternalHistoryOps>> = vec![
            Arc::new(dumbledore_node),
            Arc::new(harry_node),
            Arc::new(character_edge),
        ];
        let composite_history_object = compose_history_from_items(tmp_vector);
        assert_eq!(
            composite_history_object.iter().collect_vec(),
            vec![
                TimeIndexEntry::new(1, 0),
                TimeIndexEntry::new(2, 1),
                TimeIndexEntry::new(3, 2),
                TimeIndexEntry::new(3, 2),
                TimeIndexEntry::new(3, 2)
            ]
        );

        Ok(())
    }

    // test a layer
    #[test]
    fn test_single_layer() -> Result<(), Box<dyn std::error::Error>> {
        // generate graph
        let graph = Graph::new();
        let dumbledore_node = graph
            .add_node(1, "Dumbledore", [("type", Prop::str("Character"))], None)
            .unwrap();
        let dumbledore_node_id = dumbledore_node.id();

        let harry_node = graph
            .add_node(2, "Harry", [("type", Prop::str("Character"))], None)
            .unwrap();
        let harry_node_id = harry_node.id();

        let character_edge = graph
            .add_edge(
                3,
                "Dumbledore",
                "Harry",
                [("meeting", Prop::str("Character Co-occurrence"))],
                None,
            )
            .unwrap();

        // add broom node
        let broom_node = graph
            .add_node(4, "Broom", [("type", Prop::str("Magical Object"))], None)
            .unwrap();
        let broom_node_id = broom_node.id();

        let broom_harry_magical_edge = graph
            .add_edge(
                4,
                "Broom",
                "Harry",
                [("use", Prop::str("Flying on broom"))],
                Some("Magical Object Uses"),
            )
            .unwrap();
        let broom_harry_magical_edge_id = broom_harry_magical_edge.id();

        let broom_dumbledore_magical_edge = graph
            .add_edge(
                4,
                "Broom",
                "Dumbledore",
                [("use", Prop::str("Flying on broom"))],
                Some("Magical Object Uses"),
            )
            .unwrap();
        let broom_dumbledore_magical_edge_id = broom_dumbledore_magical_edge.id();

        let broom_harry_normal_edge = graph
            .add_edge(
                5,
                "Broom",
                "Harry",
                [("use", Prop::str("Cleaning with broom"))],
                None,
            )
            .unwrap();
        let broom_harry_normal_edge_id = broom_harry_normal_edge.id();

        let broom_dumbledore_normal_edge = graph
            .add_edge(
                5,
                "Broom",
                "Dumbledore",
                [("use", Prop::str("Cleaning with broom"))],
                None,
            )
            .unwrap();
        let broom_dumbledore_normal_edge_id = broom_dumbledore_normal_edge.id();

        let layer_id = graph.get_layer_id("Magical Object Uses").unwrap();

        // node history objects
        let dumbledore_history = History::new(dumbledore_node);
        assert_eq!(
            dumbledore_history.iter().collect_vec(),
            vec![
                TimeIndexEntry::new(1, 0),
                TimeIndexEntry::new(3, 2),
                TimeIndexEntry::new(4, 5),
                TimeIndexEntry::new(5, 7)
            ]
        );

        let harry_history = History::new(harry_node);
        assert_eq!(
            harry_history.iter().collect_vec(),
            vec![
                TimeIndexEntry::new(2, 1),
                TimeIndexEntry::new(3, 2),
                TimeIndexEntry::new(4, 4),
                TimeIndexEntry::new(5, 6)
            ]
        );

        let broom_history = History::new(broom_node);
        assert_eq!(
            broom_history.iter().collect_vec(),
            vec![
                TimeIndexEntry::new(4, 3),
                TimeIndexEntry::new(4, 4),
                TimeIndexEntry::new(4, 5),
                TimeIndexEntry::new(5, 6),
                TimeIndexEntry::new(5, 7)
            ]
        );

        // edge history objects
        let character_edge_history = History::new(character_edge);
        assert_eq!(
            character_edge_history.collect(),
            [TimeIndexEntry::new(3, 2)]
        );

        //normal history differs from "Magical Object Uses" history
        let broom_harry_normal_history = History::new(broom_harry_normal_edge);
        assert_eq!(
            broom_harry_normal_history.collect(),
            [TimeIndexEntry::new(5, 6)]
        );

        let broom_harry_magical_history = History::new(broom_harry_magical_edge);
        assert_eq!(
            broom_harry_magical_history.collect(),
            [TimeIndexEntry::new(4, 4)]
        );

        // make graphview using layer
        let magical_graph_view = graph.layers("Magical Object Uses").unwrap();
        let dumbledore_node_magical_view =
            magical_graph_view.node(dumbledore_node_id.clone()).unwrap();
        let harry_node_magical_view = magical_graph_view.node(harry_node_id.clone()).unwrap();
        let broom_node_magical_view = magical_graph_view.node(broom_node_id.clone()).unwrap();

        // history of nodes are different when only applied to the "Magical Object Uses" layer
        let dumbledore_magical_history = History::new(dumbledore_node_magical_view);
        assert_eq!(
            dumbledore_magical_history.collect(),
            [TimeIndexEntry::new(1, 0), TimeIndexEntry::new(4, 5)]
        );

        let harry_magical_history = History::new(harry_node_magical_view);
        assert_eq!(
            harry_magical_history.collect(),
            [TimeIndexEntry::new(2, 1), TimeIndexEntry::new(4, 4)]
        );

        let broom_magical_history = History::new(broom_node_magical_view);
        assert_eq!(
            broom_magical_history.collect(),
            [
                TimeIndexEntry::new(4, 3),
                TimeIndexEntry::new(4, 4),
                TimeIndexEntry::new(4, 5)
            ]
        );

        // edge retrieved from layered graph view is from the layer
        let broom_dumbledore_magical_edge_retrieved = magical_graph_view
            .edge(broom_node_id, dumbledore_node_id)
            .unwrap();

        let broom_dumbledore_magical_history =
            History::new(broom_dumbledore_magical_edge_retrieved.clone());
        assert_eq!(
            broom_dumbledore_magical_history.collect(),
            [TimeIndexEntry::new(4, 5)]
        );

        // Edge retrieved from LayeredGraphView using one layer, layer_name() returns "Err(LayerNameAPIError)"
        println!("{:?}", broom_dumbledore_magical_edge_retrieved.layer_name());

        Ok(())
    }

    #[test]
    fn test_lazy_node_state() -> Result<(), Box<dyn std::error::Error>> {
        // generate graph
        let graph = Graph::new();
        let dumbledore_node = graph
            .add_node(1, "Dumbledore", [("type", Prop::str("Character"))], None)
            .unwrap();
        let dumbledore_node_id = dumbledore_node.id();

        let harry_node = graph
            .add_node(2, "Harry", [("type", Prop::str("Character"))], None)
            .unwrap();
        let harry_node_id = harry_node.id();

        let character_edge = graph
            .add_edge(
                3,
                "Dumbledore",
                "Harry",
                [("meeting", Prop::str("Character Co-occurrence"))],
                None,
            )
            .unwrap();

        // add broom node
        let broom_node = graph
            .add_node(4, "Broom", [("type", Prop::str("Magical Object"))], None)
            .unwrap();
        let broom_node_id = broom_node.id();

        let broom_harry_magical_edge = graph
            .add_edge(
                4,
                "Broom",
                "Harry",
                [("use", Prop::str("Flying on broom"))],
                Some("Magical Object Uses"),
            )
            .unwrap();
        let broom_harry_magical_edge_id = broom_harry_magical_edge.id();

        let broom_dumbledore_magical_edge = graph
            .add_edge(
                4,
                "Broom",
                "Dumbledore",
                [("use", Prop::str("Flying on broom"))],
                Some("Magical Object Uses"),
            )
            .unwrap();
        let broom_dumbledore_magical_edge_id = broom_dumbledore_magical_edge.id();

        let broom_harry_normal_edge = graph
            .add_edge(
                5,
                "Broom",
                "Harry",
                [("use", Prop::str("Cleaning with broom"))],
                None,
            )
            .unwrap();
        let broom_harry_normal_edge_id = broom_harry_normal_edge.id();

        let broom_dumbledore_normal_edge = graph
            .add_edge(
                5,
                "Broom",
                "Dumbledore",
                [("use", Prop::str("Cleaning with broom"))],
                None,
            )
            .unwrap();
        let broom_dumbledore_normal_edge_id = broom_dumbledore_normal_edge.id();

        // Test basic LazyNodeState history operations
        let all_nodes_history = graph.nodes().history();
        let nodes_history_as_history = History::new(&all_nodes_history);

        // history object orders them automatically bc of kmerge
        let expected_history_all_ordered = [
            TimeIndexEntry::new(1, 0),
            TimeIndexEntry::new(2, 1),
            TimeIndexEntry::new(3, 2),
            TimeIndexEntry::new(3, 2),
            TimeIndexEntry::new(4, 3),
            TimeIndexEntry::new(4, 4),
            TimeIndexEntry::new(4, 4),
            TimeIndexEntry::new(4, 5),
            TimeIndexEntry::new(4, 5),
            TimeIndexEntry::new(5, 6),
            TimeIndexEntry::new(5, 6),
            TimeIndexEntry::new(5, 7),
            TimeIndexEntry::new(5, 7),
        ];

        // lazy_node_state returns an iterator of history objects, not ordered
        let expected_history_all_unordered = [
            TimeIndexEntry::new(1, 0),
            TimeIndexEntry::new(3, 2),
            TimeIndexEntry::new(4, 5),
            TimeIndexEntry::new(5, 7),
            TimeIndexEntry::new(2, 1),
            TimeIndexEntry::new(3, 2),
            TimeIndexEntry::new(4, 4),
            TimeIndexEntry::new(5, 6),
            TimeIndexEntry::new(4, 3),
            TimeIndexEntry::new(4, 4),
            TimeIndexEntry::new(4, 5),
            TimeIndexEntry::new(5, 6),
            TimeIndexEntry::new(5, 7),
        ];

        // Test that the merged history contains all timestamps from all nodes
        // Each operation adds a timestamp, so we should have timestamps from node additions and edge additions
        assert!(!nodes_history_as_history.is_empty());
        assert_eq!(
            nodes_history_as_history.earliest_time().unwrap(),
            TimeIndexEntry::new(1, 0)
        );

        assert_eq!(nodes_history_as_history, expected_history_all_ordered);
        assert_eq!(
            nodes_history_as_history.latest_time().unwrap(),
            TimeIndexEntry::new(5, 7)
        );

        // Test collect_items method on LazyNodeState<HistoryOp>
        let full_collected = all_nodes_history.collect_items();
        assert!(!full_collected.is_empty());
        assert_eq!(full_collected, expected_history_all_unordered);

        // Test individual node history access via flatten()
        let individual_histories: Vec<_> = all_nodes_history.flatten().collect();
        assert_eq!(individual_histories.len(), 3); // We have 3 nodes

        // Test timestamp conversion
        let timestamps: Vec<_> = all_nodes_history.t().flat_map(|ts| ts.collect()).collect();
        assert!(!timestamps.is_empty());
        assert_eq!(timestamps, expected_history_all_unordered.map(|t| t.t()));

        // Test intervals
        let intervals: Vec<_> = all_nodes_history.intervals().collect();
        assert_eq!(intervals.len(), 3); // One per node
        assert_eq!(
            intervals.iter().map(|i| i.collect()).collect::<Vec<_>>(),
            vec!(vec![2, 1, 1], vec![1, 1, 1], vec![0, 0, 1, 0])
        );

        // Test windowed operations
        let windowed_graph = graph.window(2, 4);
        let windowed_nodes_history = windowed_graph.nodes().history();
        let windowed_history_as_history = History::new(&windowed_nodes_history);

        // Window should filter the timestamps
        let windowed_collected = windowed_nodes_history.collect_items();

        // Windowed should have fewer or equal timestamps
        assert!(windowed_collected.len() <= full_collected.len());
        assert_eq!(
            windowed_collected,
            [
                TimeIndexEntry::new(3, 2),
                TimeIndexEntry::new(2, 1),
                TimeIndexEntry::new(3, 2)
            ]
        ); // unordered
        assert_eq!(
            windowed_history_as_history,
            [
                TimeIndexEntry::new(2, 1),
                TimeIndexEntry::new(3, 2),
                TimeIndexEntry::new(3, 2)
            ]
        ); // ordered

        // Test layer-specific operations
        let magical_layer_graph = graph.layers("Magical Object Uses").unwrap();
        let magical_nodes_history = magical_layer_graph.nodes().history();
        let magical_history_as_history = History::new(&magical_nodes_history);

        // Should have different history than the full graph
        let magical_collected = magical_nodes_history.collect_items();
        assert_eq!(
            magical_collected,
            [
                TimeIndexEntry::new(1, 0),
                TimeIndexEntry::new(4, 5),
                TimeIndexEntry::new(2, 1),
                TimeIndexEntry::new(4, 4),
                TimeIndexEntry::new(4, 3),
                TimeIndexEntry::new(4, 4),
                TimeIndexEntry::new(4, 5),
            ]
        ); // unordered
        assert_eq!(
            magical_history_as_history,
            [
                TimeIndexEntry::new(1, 0),
                TimeIndexEntry::new(2, 1),
                TimeIndexEntry::new(4, 3),
                TimeIndexEntry::new(4, 4),
                TimeIndexEntry::new(4, 4),
                TimeIndexEntry::new(4, 5),
                TimeIndexEntry::new(4, 5),
            ]
        ); // ordered

        // Test earliest and latest time operations on LazyNodeState
        let earliest_times = all_nodes_history.earliest_time();
        let latest_times = all_nodes_history.latest_time();

        // These return LazyNodeState with different operations
        assert_eq!(
            earliest_times
                .iter_values()
                .map(|t| t.unwrap())
                .collect_vec(),
            [
                TimeIndexEntry::new(1, 0),
                TimeIndexEntry::new(2, 1),
                TimeIndexEntry::new(4, 3)
            ]
        );

        assert_eq!(
            latest_times.iter_values().map(|t| t.unwrap()).collect_vec(),
            [
                TimeIndexEntry::new(5, 7),
                TimeIndexEntry::new(5, 6),
                TimeIndexEntry::new(5, 7)
            ]
        );

        // Test that History trait methods work on LazyNodeState
        let history_rev = nodes_history_as_history.iter_rev().collect::<Vec<_>>();

        // Reverse should be the reverse of forward iteration
        assert_eq!(
            nodes_history_as_history,
            history_rev.into_iter().rev().collect::<Vec<_>>()
        );

        // Test secondary time access
        let secondary_times_lazy: Vec<_> =
            all_nodes_history.s().flat_map(|s| s.collect()).collect();
        let secondary_times_normal: Vec<_> = nodes_history_as_history.s().collect();
        assert_eq!(
            secondary_times_lazy,
            [0, 2, 5, 7, 1, 2, 4, 6, 3, 4, 5, 6, 7]
        ); // unordered
        assert_eq!(
            secondary_times_normal,
            [0, 1, 2, 2, 3, 4, 4, 5, 5, 6, 6, 7, 7]
        ); // ordered

        // Test combined window and layer filtering
        let windowed_layered_graph = graph.window(3, 6).layers("Magical Object Uses").unwrap();
        let windowed_layered_history = windowed_layered_graph.nodes().history();
        let windowed_layered_history_as_history = History::new(&windowed_layered_history);
        let windowed_layered_collected = windowed_layered_history.collect_items();

        // Should be even more filtered
        assert_eq!(
            windowed_layered_collected,
            [
                TimeIndexEntry::new(4, 5),
                TimeIndexEntry::new(4, 4),
                TimeIndexEntry::new(4, 3),
                TimeIndexEntry::new(4, 4),
                TimeIndexEntry::new(4, 5)
            ]
        ); // unordered
        assert_eq!(
            windowed_layered_history_as_history,
            [
                TimeIndexEntry::new(4, 3),
                TimeIndexEntry::new(4, 4),
                TimeIndexEntry::new(4, 4),
                TimeIndexEntry::new(4, 5),
                TimeIndexEntry::new(4, 5)
            ]
        ); // ordered

        // Test iter and iter_rev on LazyNodeState directly (through InternalHistoryOps)
        let direct_iter: Vec<TimeIndexEntry> =
            InternalHistoryOps::iter(&all_nodes_history).collect();
        let direct_iter_rev: Vec<_> = all_nodes_history.iter_rev().collect();
        assert_eq!(
            direct_iter,
            direct_iter_rev.into_iter().rev().collect::<Vec<_>>()
        );

        Ok(())
    }
}
