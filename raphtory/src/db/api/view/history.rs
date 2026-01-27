use crate::{
    core::{
        storage::timeindex::{AsTime, EventTime},
        utils::iter::GenLockedIter,
    },
    db::{
        api::{
            properties::{internal::InternalPropertiesOps, TemporalPropertyView},
            state::{
                ops,
                ops::{node::NodeOp, HistoryOp, NodeFilterOp},
                LazyNodeState,
            },
            view::{
                internal::{EdgeTimeSemanticsOps, NodeTimeSemanticsOps},
                BaseNodeViewOps, BoxableGraphView, BoxedLIter, IntoDynBoxed,
            },
        },
        graph::{
            edge::{edge_valid_layer, EdgeView},
            node::NodeView,
            path::{PathFromGraph, PathFromNode},
            views::layer_graph::LayeredGraph,
        },
    },
    prelude::*,
};
use chrono::{DateTime, Utc};
use itertools::Itertools;
use raphtory_api::{
    core::{entities::LayerIds, storage::timeindex::TimeError},
    iter::BoxedIter,
};
use rayon::iter::ParallelIterator;
use std::{iter, marker::PhantomData, sync::Arc};

/// Trait declaring the operations needed so that a type's History can be accessed using the `History` object
pub trait InternalHistoryOps: Send + Sync {
    /// Iterate over temporal entries in chronological order.
    fn iter(&self) -> BoxedLIter<'_, EventTime>;
    /// Iterate over temporal entries in reverse chronological order.
    fn iter_rev(&self) -> BoxedLIter<'_, EventTime>;
    /// Get the earliest time entry for this item.
    fn earliest_time(&self) -> Option<EventTime>;
    /// Get the latest time entry for this item.
    fn latest_time(&self) -> Option<EventTime>;
    /// Get the first time entry produced by forward iteration.
    fn first(&self) -> Option<EventTime> {
        self.iter().next()
    }
    /// Get the first time entry produced by reverse iteration.
    fn last(&self) -> Option<EventTime> {
        self.iter_rev().next()
    }
    /// Get the number of time entries held by this item.
    fn len(&self) -> usize {
        self.iter().count()
    }
}

pub trait IntoArcDynHistoryOps: InternalHistoryOps + Sized + 'static {
    /// Convert this `InternalHistoryOps` into an Arc<dyn InternalHistoryOps> to enable dynamic dispatch and sharing (through `Send` + `Sync`). Particularly useful for Python.
    fn into_arc_dyn(self) -> Arc<dyn InternalHistoryOps> {
        Arc::new(self)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct History<'a, T>(pub T, PhantomData<&'a T>);

impl<'a, T: InternalHistoryOps + 'a> History<'a, T> {
    /// Create a new `History` wrapper around an object implementing `InternalHistoryOps`.
    pub fn new(item: T) -> Self {
        Self(item, PhantomData)
    }

    /// Reverse the iteration order of history items returned by iter and iter_rev.
    pub fn reverse(self) -> History<'a, ReversedHistoryOps<T>> {
        History(ReversedHistoryOps(self.0), PhantomData)
    }

    /// Convert this `History` object to return timestamps (milliseconds since the Unix epoch) instead of `EventTime`.
    pub fn t(self) -> HistoryTimestamp<T> {
        HistoryTimestamp(self.0)
    }

    /// Convert this `History` object to return datetimes instead of `EventTime`.
    pub fn dt(self) -> HistoryDateTime<T> {
        HistoryDateTime(self.0)
    }

    /// Convert this `History` object to return the event ids from `EventTime` entries.
    pub fn event_id(self) -> HistoryEventId<T> {
        HistoryEventId(self.0)
    }

    /// Access the intervals (differences in milliseconds) between consecutive timestamps in this `History` object.
    pub fn intervals(self) -> Intervals<T> {
        Intervals(self.0)
    }

    /// Merge this `History` with another `History`.
    pub fn merge<R: InternalHistoryOps>(self, right: History<R>) -> History<MergedHistory<T, R>> {
        History::new(MergedHistory::new(self.0, right.0))
    }

    pub fn into_iter_rev(self) -> BoxedLIter<'a, EventTime> {
        GenLockedIter::from(self.0, |item| item.iter_rev()).into_dyn_boxed()
    }

    /// Iterate over `EventTime` entries in chronological order.
    pub fn iter(&self) -> BoxedLIter<'_, EventTime> {
        self.0.iter()
    }

    /// Iterate over `EventTime` entries in reverse chronological order.
    pub fn iter_rev(&self) -> BoxedLIter<'_, EventTime> {
        self.0.iter_rev()
    }

    /// Collect all `EventTime` entries in chronological order.
    pub fn collect(&self) -> Vec<EventTime> {
        self.0.iter().collect_vec()
    }

    /// Collect all `EventTime` entries in reverse chronological order.
    pub fn collect_rev(&self) -> Vec<EventTime> {
        self.0.iter_rev().collect_vec()
    }

    /// Check whether this history has no entries.
    pub fn is_empty(&self) -> bool {
        self.iter().next().is_none()
    }

    /// Get the number of entries in this history.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Get the earliest `EventTime` entry in this `History`.
    pub fn earliest_time(&self) -> Option<EventTime> {
        self.0.earliest_time()
    }

    /// Get the latest `EventTime` entry in this `History`.
    pub fn latest_time(&self) -> Option<EventTime> {
        self.0.latest_time()
    }

    /// Get the first item produced by forward iteration in this `History`.
    pub fn first(&self) -> Option<EventTime> {
        self.0.first()
    }

    /// Get the first item produced by reverse iteration in this `History`.
    pub fn last(&self) -> Option<EventTime> {
        self.0.last()
    }

    /// Borrow this history as a `History` of a reference to the underlying item.
    pub fn as_ref(&self) -> History<'_, &T> {
        History::new(&self.0)
    }

    /// Print the collected entries with a prelude message.
    pub fn print(&self, prelude: &str) {
        println!("{}{:?}", prelude, self.0.iter().collect::<Vec<_>>());
    }
}

impl<'a, 'b, T: InternalHistoryOps + Clone + 'b> History<'a, &'a T> {
    /// Clone the underlying item and return an owned `History`.
    pub fn cloned(&self) -> History<'b, T> {
        History::new(self.0.clone())
    }
}

impl History<'_, EmptyHistory> {
    /// Create an empty history.
    pub fn create_empty() -> Self {
        History::new(EmptyHistory)
    }
}

impl<T: IntoArcDynHistoryOps> History<'_, T> {
    /// Convert this history into an Arc<dyn InternalHistoryOps> to enable dynamic dispatch and sharing (through `Send` + `Sync`). Particularly useful for Python.
    pub fn into_arc_dyn(self) -> History<'static, Arc<dyn InternalHistoryOps>> {
        History::new(self.0.into_arc_dyn())
    }
}

impl<'a, T: InternalHistoryOps + 'a> IntoIterator for History<'a, T> {
    type Item = EventTime;
    type IntoIter = BoxedLIter<'a, EventTime>;

    fn into_iter(self) -> Self::IntoIter {
        GenLockedIter::from(self.0, |item| item.iter()).into_dyn_boxed()
    }
}

impl<'b, T: InternalHistoryOps + 'b, L, I: Copy> PartialEq<L> for History<'b, T>
where
    for<'a> &'a L: IntoIterator<Item = &'a I>,
    EventTime: PartialEq<I>,
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

impl<T: InternalHistoryOps + ?Sized> InternalHistoryOps for Box<T> {
    fn iter(&self) -> BoxedLIter<'_, EventTime> {
        T::iter(self)
    }

    fn iter_rev(&self) -> BoxedLIter<'_, EventTime> {
        T::iter_rev(self)
    }

    fn earliest_time(&self) -> Option<EventTime> {
        T::earliest_time(self)
    }

    fn latest_time(&self) -> Option<EventTime> {
        T::latest_time(self)
    }

    fn first(&self) -> Option<EventTime> {
        T::first(self)
    }

    fn last(&self) -> Option<EventTime> {
        T::last(self)
    }

    fn len(&self) -> usize {
        T::len(self)
    }
}

impl<T: InternalHistoryOps + 'static> IntoArcDynHistoryOps for Box<T> {
    fn into_arc_dyn(self) -> Arc<dyn InternalHistoryOps> {
        Arc::from(self as Box<dyn InternalHistoryOps>)
    }
}

impl IntoArcDynHistoryOps for Box<dyn InternalHistoryOps> {
    fn into_arc_dyn(self) -> Arc<dyn InternalHistoryOps> {
        Arc::from(self)
    }
}

impl<T: InternalHistoryOps + ?Sized> InternalHistoryOps for Arc<T> {
    fn iter(&self) -> BoxedLIter<'_, EventTime> {
        T::iter(self)
    }

    fn iter_rev(&self) -> BoxedLIter<'_, EventTime> {
        T::iter_rev(self)
    }

    fn earliest_time(&self) -> Option<EventTime> {
        T::earliest_time(self)
    }

    fn latest_time(&self) -> Option<EventTime> {
        T::latest_time(self)
    }

    fn first(&self) -> Option<EventTime> {
        T::first(self)
    }

    fn last(&self) -> Option<EventTime> {
        T::last(self)
    }

    fn len(&self) -> usize {
        T::len(self)
    }
}

impl<T: InternalHistoryOps + 'static> IntoArcDynHistoryOps for Arc<T> {
    fn into_arc_dyn(self) -> Arc<dyn InternalHistoryOps> {
        self
    }
}

impl IntoArcDynHistoryOps for Arc<dyn InternalHistoryOps> {
    fn into_arc_dyn(self) -> Arc<dyn InternalHistoryOps> {
        self
    }
}

impl<T: InternalHistoryOps + ?Sized> InternalHistoryOps for &T {
    fn iter(&self) -> BoxedLIter<'_, EventTime> {
        T::iter(self)
    }

    fn iter_rev(&self) -> BoxedLIter<'_, EventTime> {
        T::iter_rev(self)
    }

    fn earliest_time(&self) -> Option<EventTime> {
        T::earliest_time(self)
    }

    fn latest_time(&self) -> Option<EventTime> {
        T::latest_time(self)
    }

    fn first(&self) -> Option<EventTime> {
        T::first(self)
    }

    fn last(&self) -> Option<EventTime> {
        T::last(self)
    }

    fn len(&self) -> usize {
        T::len(self)
    }
}

// More efficient because we are calling iter.merge() instead of iter.kmerge(). Efficiency benefits are lost if we nest these objects too much
// TODO: Write benchmark to evaluate performance benefit tradeoff (ie when there are no more performance benefits)
/// Merges two history objects together. They can be nested if we merge `MergedHistory` objects. Separate from `CompositeHistory` in that it can only hold two items.
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
    fn iter(&self) -> BoxedLIter<'_, EventTime> {
        self.left.iter().merge(self.right.iter()).into_dyn_boxed()
    }

    fn iter_rev(&self) -> BoxedLIter<'_, EventTime> {
        self.left
            .iter_rev()
            .merge_by(self.right.iter_rev(), |a, b| a >= b)
            .into_dyn_boxed()
    }

    fn earliest_time(&self) -> Option<EventTime> {
        self.left.earliest_time().min(self.right.earliest_time())
    }

    fn latest_time(&self) -> Option<EventTime> {
        self.left.latest_time().max(self.right.latest_time())
    }

    fn len(&self) -> usize {
        self.left.len() + self.right.len()
    }
}

impl<L: InternalHistoryOps + 'static, R: InternalHistoryOps + 'static> IntoArcDynHistoryOps
    for MergedHistory<L, R>
{
}

// TODO: Write benchmark to see performance hit of Arc and Boxes. If the composite will only hold 2 items, MergedHistory is more efficient.
/// Combines multiple history objects together by holding a vector of items implementing `InternalHistoryOps`.
#[derive(Clone)]
pub struct CompositeHistory<'a, T> {
    history_objects: Box<[T]>,
    phantom: PhantomData<&'a T>,
}

impl<'a, T: InternalHistoryOps + 'a> CompositeHistory<'a, T> {
    pub fn new(history_objects: Vec<T>) -> Self {
        Self {
            history_objects: history_objects.into_boxed_slice(),
            phantom: PhantomData,
        }
    }
}

/// Create a CompositeHistory object composed of multiple History objects
pub fn compose_multiple_histories<'a, T: InternalHistoryOps + 'a>(
    objects: impl IntoIterator<Item = History<'a, T>>,
) -> History<'a, CompositeHistory<'a, T>> {
    History::new(CompositeHistory::new(
        objects.into_iter().map(|h| h.0).collect(),
    ))
}

/// Create a CompositeHistory object composed of multiple items implementing `InternalHistoryOps`
pub fn compose_history_from_items<'a, T: InternalHistoryOps + 'a>(
    objects: impl IntoIterator<Item = T>,
) -> History<'a, CompositeHistory<'a, T>> {
    History::new(CompositeHistory::new(objects.into_iter().collect()))
}

impl<'a, T: InternalHistoryOps + 'a> InternalHistoryOps for CompositeHistory<'a, T> {
    fn iter(&self) -> BoxedLIter<'_, EventTime> {
        self.history_objects
            .iter()
            .map(|object| object.iter())
            .kmerge()
            .into_dyn_boxed()
    }

    fn iter_rev(&self) -> BoxedLIter<'_, EventTime> {
        self.history_objects
            .iter()
            .map(|object| object.iter_rev())
            .kmerge_by(|a, b| a >= b)
            .into_dyn_boxed()
    }

    fn earliest_time(&self) -> Option<EventTime> {
        self.history_objects
            .iter()
            .filter_map(|history| history.earliest_time())
            .min()
    }

    fn latest_time(&self) -> Option<EventTime> {
        self.history_objects
            .iter()
            .filter_map(|history| history.latest_time())
            .max()
    }
}

impl<T: InternalHistoryOps + 'static> IntoArcDynHistoryOps for CompositeHistory<'static, T> {}

#[derive(Debug, Clone, Copy)]
pub struct EmptyHistory;

impl InternalHistoryOps for EmptyHistory {
    fn iter(&self) -> BoxedLIter<'_, EventTime> {
        iter::empty().into_dyn_boxed()
    }

    fn iter_rev(&self) -> BoxedLIter<'_, EventTime> {
        iter::empty().into_dyn_boxed()
    }

    fn earliest_time(&self) -> Option<EventTime> {
        None
    }

    fn latest_time(&self) -> Option<EventTime> {
        None
    }

    fn first(&self) -> Option<EventTime> {
        None
    }

    fn last(&self) -> Option<EventTime> {
        None
    }

    fn len(&self) -> usize {
        0
    }
}

impl IntoArcDynHistoryOps for EmptyHistory {}

impl<'graph, G: GraphViewOps<'graph> + Send + Sync + Send + Sync> InternalHistoryOps
    for NodeView<'graph, G>
{
    fn iter(&self) -> BoxedLIter<'_, EventTime> {
        let semantics = self.graph.node_time_semantics();
        let node = self.graph.core_node(self.node);
        GenLockedIter::from(node, move |node| {
            semantics
                .node_history(node.as_ref(), &self.graph)
                .into_dyn_boxed()
        })
        .into_dyn_boxed()
    }

    fn iter_rev(&self) -> BoxedLIter<'_, EventTime> {
        let semantics = self.graph.node_time_semantics();
        let node = self.graph.core_node(self.node);
        GenLockedIter::from(node, move |node| {
            semantics
                .node_history_rev(node.as_ref(), &self.graph)
                .into_dyn_boxed()
        })
        .into_dyn_boxed()
    }

    fn earliest_time(&self) -> Option<EventTime> {
        ops::EarliestTime {
            view: self.graph().clone(),
        }
        .apply(self.graph.core_graph(), self.node)
    }

    fn latest_time(&self) -> Option<EventTime> {
        ops::LatestTime {
            view: self.graph().clone(),
        }
        .apply(self.graph.core_graph(), self.node)
    }
}

impl<G: GraphViewOps<'static> + Send + Sync> IntoArcDynHistoryOps for NodeView<'static, G> {}

impl<G: BoxableGraphView + Clone> InternalHistoryOps for EdgeView<G> {
    fn iter(&self) -> BoxedLIter<'_, EventTime> {
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

    fn iter_rev(&self) -> BoxedLIter<'_, EventTime> {
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

    fn earliest_time(&self) -> Option<EventTime> {
        EdgeViewOps::earliest_time(self)
    }

    fn latest_time(&self) -> Option<EventTime> {
        EdgeViewOps::latest_time(self)
    }

    fn len(&self) -> usize {
        let g = &self.graph;
        let e = self.edge;
        if edge_valid_layer(g, e) {
            match e.time() {
                Some(_) => 1,
                None => match e.layer() {
                    None => g
                        .edge_time_semantics()
                        .edge_exploded_count(g.core_edge(e.pid()).as_ref(), g),
                    Some(layer) => g.edge_time_semantics().edge_exploded_count(
                        g.core_edge(e.pid()).as_ref(),
                        LayeredGraph::new(g, LayerIds::One(layer)),
                    ),
                },
            }
        } else {
            0
        }
    }
}

impl<G: BoxableGraphView + Clone + 'static> IntoArcDynHistoryOps for EdgeView<G> {}

impl<
        'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        F: NodeFilterOp + Clone + 'graph,
    > InternalHistoryOps for LazyNodeState<'graph, HistoryOp<'graph, GH>, G, GH, F>
{
    fn iter(&self) -> BoxedLIter<'_, EventTime> {
        // consuming the history objects is fine here because they get recreated on subsequent iter() calls
        NodeStateOps::iter_values(self)
            .map(|history| history.into_iter())
            .kmerge()
            .into_dyn_boxed()
    }

    fn iter_rev(&self) -> BoxedLIter<'_, EventTime> {
        // consuming the history objects is fine here because they get recreated on subsequent iter_rev() calls
        NodeStateOps::iter_values(self)
            .map(|history| history.into_iter_rev())
            .kmerge_by(|a, b| a >= b)
            .into_dyn_boxed()
    }

    fn earliest_time(&self) -> Option<EventTime> {
        NodeStateOps::par_iter_values(self)
            .filter_map(|history| history.earliest_time())
            .min()
    }

    fn latest_time(&self) -> Option<EventTime> {
        NodeStateOps::par_iter_values(self)
            .filter_map(|history| history.latest_time())
            .max()
    }
}

impl<G: GraphViewOps<'static>, GH: GraphViewOps<'static>, F: NodeFilterOp + Clone + 'static>
    IntoArcDynHistoryOps for LazyNodeState<'static, HistoryOp<'static, GH>, G, GH, F>
{
}

impl<P: InternalPropertiesOps> InternalHistoryOps for TemporalPropertyView<P> {
    fn iter(&self) -> BoxedLIter<'_, EventTime> {
        self.props
            .temporal_iter(self.id)
            .map(|(t, _)| t)
            .into_dyn_boxed()
    }

    fn iter_rev(&self) -> BoxedLIter<'_, EventTime> {
        self.props
            .temporal_iter_rev(self.id)
            .map(|(t, _)| t)
            .into_dyn_boxed()
    }

    fn earliest_time(&self) -> Option<EventTime> {
        InternalHistoryOps::iter(self).next()
    }

    fn latest_time(&self) -> Option<EventTime> {
        InternalHistoryOps::iter_rev(self).next()
    }
}

impl<P: InternalPropertiesOps + 'static> IntoArcDynHistoryOps for TemporalPropertyView<P> {}

impl<'graph, G: GraphViewOps<'graph>> InternalHistoryOps for PathFromNode<'graph, G> {
    fn iter(&self) -> BoxedLIter<'_, EventTime> {
        self.iter()
            .map(|nodeview| GenLockedIter::from(nodeview, move |node| node.iter()))
            .kmerge()
            .into_dyn_boxed()
    }

    fn iter_rev(&self) -> BoxedLIter<'_, EventTime> {
        self.iter()
            .map(|nodeview| GenLockedIter::from(nodeview, move |node| node.iter_rev()))
            .kmerge_by(|a, b| a >= b)
            .into_dyn_boxed()
    }

    fn earliest_time(&self) -> Option<EventTime> {
        self.iter()
            .filter_map(|nodeview| InternalHistoryOps::earliest_time(&nodeview))
            .min()
    }

    fn latest_time(&self) -> Option<EventTime> {
        self.iter()
            .filter_map(|nodeview| InternalHistoryOps::latest_time(&nodeview))
            .max()
    }
}

impl<G: GraphViewOps<'static>> IntoArcDynHistoryOps for PathFromNode<'static, G> {}

impl<'graph, G: GraphViewOps<'graph>> InternalHistoryOps for PathFromGraph<'graph, G> {
    fn iter(&self) -> BoxedLIter<'_, EventTime> {
        self.iter()
            .map(|path_from_node| {
                GenLockedIter::from(path_from_node, |item| InternalHistoryOps::iter(item))
            })
            .kmerge()
            .into_dyn_boxed()
    }

    fn iter_rev(&self) -> BoxedLIter<'_, EventTime> {
        self.iter()
            .map(|path_from_node| {
                GenLockedIter::from(path_from_node, |item| InternalHistoryOps::iter_rev(item))
            })
            .kmerge_by(|a, b| a >= b)
            .into_dyn_boxed()
    }

    fn earliest_time(&self) -> Option<EventTime> {
        self.iter()
            .filter_map(|path_from_node| InternalHistoryOps::earliest_time(&path_from_node))
            .min()
    }

    fn latest_time(&self) -> Option<EventTime> {
        self.iter()
            .filter_map(|path_from_node| InternalHistoryOps::latest_time(&path_from_node))
            .max()
    }
}

impl<G: GraphViewOps<'static>> IntoArcDynHistoryOps for PathFromGraph<'static, G> {}

/// Reverses the order of items returned by iter() and iter_rev() (as such, also reverses first() and last()).
#[derive(Debug, Clone, Copy)]
pub struct ReversedHistoryOps<T>(T);

impl<T: InternalHistoryOps> ReversedHistoryOps<T> {
    pub fn new(item: T) -> Self {
        Self(item)
    }
}

impl<T: InternalHistoryOps> InternalHistoryOps for ReversedHistoryOps<T> {
    fn iter(&self) -> BoxedLIter<'_, EventTime> {
        self.0.iter_rev()
    }

    fn iter_rev(&self) -> BoxedLIter<'_, EventTime> {
        self.0.iter()
    }
    // no need to override first() and last() because the iterators are reversed

    fn earliest_time(&self) -> Option<EventTime> {
        self.0.earliest_time()
    }

    fn latest_time(&self) -> Option<EventTime> {
        self.0.latest_time()
    }

    fn len(&self) -> usize {
        self.0.len()
    }
}

impl<T: InternalHistoryOps + 'static> IntoArcDynHistoryOps for ReversedHistoryOps<T> {}

/// History view that exposes timestamps in milliseconds since the Unix epoch.
#[derive(Debug, Clone, Copy)]
pub struct HistoryTimestamp<T>(pub(crate) T);

impl<T: InternalHistoryOps> HistoryTimestamp<T> {
    pub fn new(item: T) -> Self {
        Self(item)
    }

    pub fn iter(&self) -> BoxedLIter<'_, i64> {
        self.0.iter().map(|t| t.0).into_dyn_boxed()
    }

    pub fn iter_rev(&self) -> BoxedLIter<'_, i64> {
        self.0.iter_rev().map(|t| t.0).into_dyn_boxed()
    }

    pub fn collect(&self) -> Vec<i64> {
        self.0.iter().map(|t| t.0).collect()
    }

    pub fn collect_rev(&self) -> Vec<i64> {
        self.0.iter_rev().map(|t| t.0).collect()
    }
}

// IntoIterator implementations when T has static lifetime is useful in python
impl<T: InternalHistoryOps + 'static> IntoIterator for HistoryTimestamp<T> {
    type Item = i64;
    type IntoIter = BoxedIter<i64>;

    fn into_iter(self) -> Self::IntoIter {
        GenLockedIter::from(self, |item| item.iter()).into_dyn_boxed()
    }
}

impl<'b, T: InternalHistoryOps + 'b, L, I: Copy> PartialEq<L> for HistoryTimestamp<T>
where
    for<'a> &'a L: IntoIterator<Item = &'a I>,
    i64: PartialEq<I>,
{
    fn eq(&self, other: &L) -> bool {
        self.iter().eq(other.into_iter().copied())
    }
}

impl<'a, T: InternalHistoryOps + 'a> PartialEq for HistoryTimestamp<T> {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}

impl<'a, T: InternalHistoryOps + 'a> Eq for HistoryTimestamp<T> {}

/// History view that exposes UTC datetimes.
#[derive(Debug, Clone, Copy)]
pub struct HistoryDateTime<T>(pub(crate) T);

impl<T: InternalHistoryOps> HistoryDateTime<T> {
    pub fn new(item: T) -> Self {
        Self(item)
    }

    pub fn iter(&self) -> BoxedLIter<'_, Result<DateTime<Utc>, TimeError>> {
        self.0.iter().map(|t| t.dt()).into_dyn_boxed()
    }

    pub fn iter_rev(&self) -> BoxedLIter<'_, Result<DateTime<Utc>, TimeError>> {
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

impl<T: InternalHistoryOps + 'static> IntoIterator for HistoryDateTime<T> {
    type Item = Result<DateTime<Utc>, TimeError>;
    type IntoIter = BoxedIter<Result<DateTime<Utc>, TimeError>>;

    fn into_iter(self) -> Self::IntoIter {
        GenLockedIter::from(self, |item| item.iter()).into_dyn_boxed()
    }
}

impl<'b, T: InternalHistoryOps + 'b, L, I: Copy> PartialEq<L> for HistoryDateTime<T>
where
    for<'a> &'a L: IntoIterator<Item = &'a I>,
    Result<DateTime<Utc>, TimeError>: PartialEq<I>,
{
    fn eq(&self, other: &L) -> bool {
        self.iter().eq(other.into_iter().copied())
    }
}

impl<'a, T: InternalHistoryOps + 'a> PartialEq for HistoryDateTime<T> {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}

impl<'a, T: InternalHistoryOps + 'a> Eq for HistoryDateTime<T> {}

/// History view that exposes event ids of time entries. They are used for ordering within the same timestamp.
#[derive(Debug, Clone, Copy)]
pub struct HistoryEventId<T>(pub(crate) T);

impl<T: InternalHistoryOps> HistoryEventId<T> {
    pub fn new(item: T) -> Self {
        Self(item)
    }

    pub fn iter(&self) -> BoxedLIter<'_, usize> {
        self.0.iter().map(|t| t.1).into_dyn_boxed()
    }

    pub fn iter_rev(&self) -> BoxedLIter<'_, usize> {
        self.0.iter_rev().map(|t| t.1).into_dyn_boxed()
    }

    pub fn collect(&self) -> Vec<usize> {
        self.0.iter().map(|t| t.1).collect()
    }

    pub fn collect_rev(&self) -> Vec<usize> {
        self.0.iter_rev().map(|t| t.1).collect()
    }
}

impl<T: InternalHistoryOps + 'static> IntoIterator for HistoryEventId<T> {
    type Item = usize;
    type IntoIter = BoxedIter<usize>;

    fn into_iter(self) -> Self::IntoIter {
        GenLockedIter::from(self, |item| item.iter()).into_dyn_boxed()
    }
}

impl<'b, T: InternalHistoryOps + 'b, L, I: Copy> PartialEq<L> for HistoryEventId<T>
where
    for<'a> &'a L: IntoIterator<Item = &'a I>,
    usize: PartialEq<I>,
{
    fn eq(&self, other: &L) -> bool {
        self.iter().eq(other.into_iter().copied())
    }
}

impl<'a, T: InternalHistoryOps + 'a> PartialEq for HistoryEventId<T> {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}

impl<'a, T: InternalHistoryOps + 'a> Eq for HistoryEventId<T> {}

/// View over the intervals between consecutive timestamps, expressed in milliseconds.
#[derive(Debug, Clone, Copy)]
pub struct Intervals<T>(pub T);

impl<T: InternalHistoryOps> Intervals<T> {
    pub fn new(item: T) -> Self {
        Intervals(item)
    }

    pub fn collect(&self) -> Vec<i64> {
        self.iter().collect()
    }

    pub fn collect_rev(&self) -> Vec<i64> {
        self.iter_rev().collect()
    }

    pub fn iter(&self) -> BoxedLIter<'_, i64> {
        self.0
            .iter()
            .map(|t| t.0)
            .tuple_windows()
            .map(|(w1, w2)| w2 - w1)
            .into_dyn_boxed()
    }

    pub fn iter_rev(&self) -> BoxedLIter<'_, i64> {
        self.0
            .iter_rev()
            .map(|t| t.0)
            .tuple_windows()
            .map(|(w1, w2)| w2 - w1)
            .into_dyn_boxed()
    }

    /// Compute the mean interval between consecutive timestamps.
    pub fn mean(&self) -> Option<f64> {
        if self.iter().next().is_none() {
            return None;
        }
        // count and sum in one pass of the iterator
        let (len, sum) = self.iter().fold((0i64, 0f64), |(count, sum), item| {
            (count + 1, sum + (item as f64))
        });
        Some(sum / len as f64)
    }

    /// Compute the median interval between consecutive timestamps.
    /// If the number of items is even (thus "2 middle elements") and their average isn't even, then the returned median is their average rounded up to the nearest integer.
    pub fn median(&self) -> Option<i64> {
        if self.iter().next().is_none() {
            return None;
        }
        let mut intervals: Vec<i64> = self.iter().collect();
        intervals.sort_unstable();

        let mid = intervals.len() / 2;
        if intervals.len() % 2 == 0 {
            let mid_sum = intervals[mid - 1] + intervals[mid];
            if mid_sum % 2 == 0 {
                Some(mid_sum / 2)
            } else {
                Some((mid_sum / 2) + 1) //round up if there's a decimal because it will always be .5
            }
        } else {
            Some(intervals[mid])
        }
    }

    /// Compute the maximum interval between consecutive timestamps.
    pub fn max(&self) -> Option<i64> {
        self.iter().max()
    }

    /// Compute the minimum interval between consecutive timestamps.
    pub fn min(&self) -> Option<i64> {
        self.iter().min()
    }
}

impl<T: InternalHistoryOps + 'static> IntoIterator for Intervals<T> {
    type Item = i64;
    type IntoIter = BoxedIter<i64>;

    fn into_iter(self) -> Self::IntoIter {
        GenLockedIter::from(self, |item| item.iter()).into_dyn_boxed()
    }
}

impl<'b, T: InternalHistoryOps + 'b, L, I: Copy> PartialEq<L> for Intervals<T>
where
    for<'a> &'a L: IntoIterator<Item = &'a I>,
    i64: PartialEq<I>,
{
    fn eq(&self, other: &L) -> bool {
        self.iter().eq(other.into_iter().copied())
    }
}

impl<'a, T: InternalHistoryOps + 'a> PartialEq for Intervals<T> {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}

impl<'a, T: InternalHistoryOps + 'a> Eq for Intervals<T> {}

/// Trait declaring the operations needed so that a type's deletion history can be accessed using the `History` object
pub trait InternalDeletionOps: Send + Sync {
    /// Iterate over deletion time entries in chronological order.
    fn iter(&self) -> BoxedLIter<'_, EventTime>;
    /// Iterate over deletion time entries in reverse chronological order.
    fn iter_rev(&self) -> BoxedLIter<'_, EventTime>;
    /// Get the earliest deletion's time entry for this item.
    fn earliest_time(&self) -> Option<EventTime>;
    /// Get the latest deletion's time entry for this item.
    fn latest_time(&self) -> Option<EventTime>;
    /// Get the first deletion's time entry produced by forward iteration.
    fn first(&self) -> Option<EventTime> {
        self.iter().next()
    }
    /// Get the first deletion's time entry produced by reverse iteration.
    fn last(&self) -> Option<EventTime> {
        self.iter_rev().next()
    }
    /// Get the number of deletion time entries held by this item.
    fn len(&self) -> usize {
        self.iter().count()
    }
}

/// Gives access to deletion information of an object when used as `History<DeletionHistory<SomeItem>>`
#[derive(Debug, Clone, Copy)]
pub struct DeletionHistory<T>(T);

impl<T: InternalDeletionOps> DeletionHistory<T> {
    pub fn new(item: T) -> Self {
        DeletionHistory(item)
    }
}

// this way, we can use all the History object functionality (converting to timestamps, dt, intervals)
impl<T: InternalDeletionOps> InternalHistoryOps for DeletionHistory<T> {
    fn iter(&self) -> BoxedLIter<'_, EventTime> {
        self.0.iter()
    }

    fn iter_rev(&self) -> BoxedLIter<'_, EventTime> {
        self.0.iter_rev()
    }

    fn earliest_time(&self) -> Option<EventTime> {
        self.0.earliest_time()
    }

    fn latest_time(&self) -> Option<EventTime> {
        self.0.latest_time()
    }
}

impl<T: InternalDeletionOps + 'static> IntoArcDynHistoryOps for DeletionHistory<T> {}

impl<G: BoxableGraphView + Clone> InternalDeletionOps for EdgeView<G> {
    fn iter(&self) -> BoxedLIter<'_, EventTime> {
        let g = &self.graph;
        let e = self.edge;
        if edge_valid_layer(g, e) {
            let time_semantics = g.edge_time_semantics();
            let edge = g.core_edge(e.pid());
            match e.time() {
                Some(t) => {
                    let layer = e.layer().expect("exploded edge should have layer");
                    time_semantics
                        .edge_exploded_deletion(edge.as_ref(), g, t, layer)
                        .into_iter()
                        .into_dyn_boxed()
                }
                None => match e.layer() {
                    None => GenLockedIter::from(edge, move |edge| {
                        time_semantics
                            .edge_deletion_history(edge.as_ref(), g, g.layer_ids())
                            .map(|(t, _)| t)
                            .into_dyn_boxed()
                    })
                    .into_dyn_boxed(),
                    Some(layer) => {
                        if self.graph.layer_ids().contains(&layer) {
                            let layer_ids = LayerIds::One(layer);
                            GenLockedIter::from((edge, layer_ids), move |(edge, layer_ids)| {
                                time_semantics
                                    .edge_deletion_history(edge.as_ref(), g, layer_ids)
                                    .map(|(t, _)| t)
                                    .into_dyn_boxed()
                            })
                            .into_dyn_boxed()
                        } else {
                            iter::empty().into_dyn_boxed()
                        }
                    }
                },
            }
        } else {
            iter::empty().into_dyn_boxed()
        }
    }

    fn iter_rev(&self) -> BoxedLIter<'_, EventTime> {
        let g = &self.graph;
        let e = self.edge;
        if edge_valid_layer(g, e) {
            let time_semantics = g.edge_time_semantics();
            let edge = g.core_edge(e.pid());
            match e.time() {
                Some(t) => {
                    let layer = e.layer().expect("exploded edge should have layer");
                    time_semantics
                        .edge_exploded_deletion(edge.as_ref(), g, t, layer)
                        .into_iter()
                        .into_dyn_boxed()
                }
                None => match e.layer() {
                    None => GenLockedIter::from(edge, move |edge| {
                        time_semantics
                            .edge_deletion_history_rev(edge.as_ref(), g, g.layer_ids())
                            .map(|(t, _)| t)
                            .into_dyn_boxed()
                    })
                    .into_dyn_boxed(),
                    Some(layer) => {
                        if self.graph.layer_ids().contains(&layer) {
                            let layer_ids = LayerIds::One(layer);
                            GenLockedIter::from((edge, layer_ids), move |(edge, layer_ids)| {
                                time_semantics
                                    .edge_deletion_history_rev(edge.as_ref(), g, layer_ids)
                                    .map(|(t, _)| t)
                                    .into_dyn_boxed()
                            })
                            .into_dyn_boxed()
                        } else {
                            iter::empty().into_dyn_boxed()
                        }
                    }
                },
            }
        } else {
            iter::empty().into_dyn_boxed()
        }
    }

    fn earliest_time(&self) -> Option<EventTime> {
        InternalDeletionOps::iter(self).next()
    }

    fn latest_time(&self) -> Option<EventTime> {
        InternalDeletionOps::iter_rev(self).next()
    }
}
