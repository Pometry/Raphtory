use crate::{
    core::{
        storage::timeindex::{AsTime, TimeIndexEntry},
        utils::iter::GenLockedIter,
    },
    db::{
        api::{
            properties::{internal::InternalPropertiesOps, TemporalPropertyView},
            state::{
                ops,
                ops::{node::NodeOp, HistoryOp},
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
use raphtory_api::core::{entities::LayerIds, storage::timeindex::TimeError};
use rayon::iter::ParallelIterator;
use std::{iter, marker::PhantomData, sync::Arc};

pub trait InternalHistoryOps: Send + Sync {
    fn iter(&self) -> BoxedLIter<TimeIndexEntry>;
    fn iter_rev(&self) -> BoxedLIter<TimeIndexEntry>;
    fn earliest_time(&self) -> Option<TimeIndexEntry>;
    fn latest_time(&self) -> Option<TimeIndexEntry>;
    fn first(&self) -> Option<TimeIndexEntry> {
        self.iter().next()
    }
    fn last(&self) -> Option<TimeIndexEntry> {
        self.iter_rev().next()
    }
    // override if we want more efficient implementation
    fn len(&self) -> usize {
        self.iter().count()
    }
}

pub trait IntoArcDynHistoryOps: InternalHistoryOps + Sized + 'static {
    // override to avoid creating a new Arc
    fn into_arc_dyn(self) -> Arc<dyn InternalHistoryOps> {
        Arc::new(self)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct History<'a, T>(pub T, PhantomData<&'a T>);

impl<'a, T: InternalHistoryOps + 'a> History<'a, T> {
    pub fn new(item: T) -> Self {
        Self(item, PhantomData)
    }

    // reverses the order of items returned by iter() and iter_rev()
    pub fn reverse(self) -> History<'a, ReversedHistoryOps<T>> {
        History(ReversedHistoryOps(self.0), PhantomData)
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
    pub fn secondary_index(self) -> HistorySecondaryIndex<T> {
        HistorySecondaryIndex(self.0)
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

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn earliest_time(&self) -> Option<TimeIndexEntry> {
        self.0.earliest_time()
    }

    pub fn latest_time(&self) -> Option<TimeIndexEntry> {
        self.0.latest_time()
    }

    pub fn first(&self) -> Option<TimeIndexEntry> {
        self.0.first()
    }

    pub fn last(&self) -> Option<TimeIndexEntry> {
        self.0.last()
    }

    pub fn print(&self, prelude: &str) {
        println!("{}{:?}", prelude, self.0.iter().collect::<Vec<_>>());
    }
}

impl History<'_, EmptyHistory> {
    pub fn create_empty() -> Self {
        History::new(EmptyHistory)
    }
}

impl<T: IntoArcDynHistoryOps> History<'_, T> {
    pub fn into_arc_dyn(self) -> History<'static, Arc<dyn InternalHistoryOps>> {
        History::new(self.0.into_arc_dyn())
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

    fn first(&self) -> Option<TimeIndexEntry> {
        T::first(self)
    }

    fn last(&self) -> Option<TimeIndexEntry> {
        T::last(self)
    }

    fn len(&self) -> usize {
        T::len(self)
    }
}

impl<T: InternalHistoryOps + 'static> IntoArcDynHistoryOps for Box<T>{
    fn into_arc_dyn(self) -> Arc<dyn InternalHistoryOps> {
        Arc::from(self as Box<dyn InternalHistoryOps>)
    }
}

impl IntoArcDynHistoryOps for Box<dyn InternalHistoryOps>{
    fn into_arc_dyn(self) -> Arc<dyn InternalHistoryOps> {
        Arc::from(self)
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

    fn first(&self) -> Option<TimeIndexEntry> {
        T::first(self)
    }

    fn last(&self) -> Option<TimeIndexEntry> {
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

    fn first(&self) -> Option<TimeIndexEntry> {
        T::first(self)
    }

    fn last(&self) -> Option<TimeIndexEntry> {
        T::last(self)
    }

    fn len(&self) -> usize {
        T::len(self)
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

    fn len(&self) -> usize {
        self.left.len() + self.right.len()
    }
}

impl<L: InternalHistoryOps + 'static, R: InternalHistoryOps + 'static> IntoArcDynHistoryOps for MergedHistory<L, R> {}

/// Holds a vector of multiple items implementing InternalHistoryOps. If the composite will only hold 2 items, MergedHistory is more efficient.
/// TODO: Write benchmark to see performance hit of Arcs
#[derive(Clone)]
pub struct CompositeHistory<'a> {
    history_objects: Vec<Arc<dyn InternalHistoryOps + 'a>>,
    // history_objects: Arc<[Box<dyn InternalHistoryOps + 'a>]>,
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

impl IntoArcDynHistoryOps for CompositeHistory<'static>{}

#[derive(Debug, Clone, Copy)]
pub struct EmptyHistory;

impl InternalHistoryOps for EmptyHistory {
    fn iter(&self) -> BoxedLIter<TimeIndexEntry> {
        iter::empty().into_dyn_boxed()
    }

    fn iter_rev(&self) -> BoxedLIter<TimeIndexEntry> {
        iter::empty().into_dyn_boxed()
    }

    fn earliest_time(&self) -> Option<TimeIndexEntry> {
        None
    }

    fn latest_time(&self) -> Option<TimeIndexEntry> {
        None
    }

    fn first(&self) -> Option<TimeIndexEntry> {
        None
    }

    fn last(&self) -> Option<TimeIndexEntry> {
        None
    }

    fn len(&self) -> usize {
        0
    }
}

impl IntoArcDynHistoryOps for EmptyHistory{}

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

impl<G: GraphViewOps<'static> + Send + Sync, GH: GraphViewOps<'static> + Send + Sync>
IntoArcDynHistoryOps for NodeView<'static, G, GH>{}

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
        NodeStateOps::par_iter_values(self)
            .filter_map(|history| history.earliest_time())
            .min()
    }

    fn latest_time(&self) -> Option<TimeIndexEntry> {
        NodeStateOps::par_iter_values(self)
            .filter_map(|history| history.latest_time())
            .max()
    }
}

impl<G: GraphViewOps<'static>, GH: GraphViewOps<'static>> IntoArcDynHistoryOps
for LazyNodeState<'static, HistoryOp<'static, GH>, G, GH>{}

impl<P: InternalPropertiesOps> InternalHistoryOps for TemporalPropertyView<P> {
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

impl<P: InternalPropertiesOps + 'static> IntoArcDynHistoryOps for TemporalPropertyView<P>{}

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

impl<G: GraphViewOps<'static>, GH: GraphViewOps<'static>> IntoArcDynHistoryOps
for PathFromNode<'static, G, GH>{}

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

impl<G: GraphViewOps<'static>, GH: GraphViewOps<'static>> IntoArcDynHistoryOps
for PathFromGraph<'static, G, GH>{}

// reverses the order of items returned by iter() and iter_rev()
#[derive(Debug, Clone, Copy)]
pub struct ReversedHistoryOps<T>(T);

impl<T: InternalHistoryOps> ReversedHistoryOps<T> {
    pub fn new(item: T) -> Self {
        Self(item)
    }
}

impl<T: InternalHistoryOps> InternalHistoryOps for ReversedHistoryOps<T> {
    fn iter(&self) -> BoxedLIter<TimeIndexEntry> {
        self.0.iter_rev()
    }

    fn iter_rev(&self) -> BoxedLIter<TimeIndexEntry> {
        self.0.iter()
    }
    // no need to override first() and last() because the iterators are reversed

    fn earliest_time(&self) -> Option<TimeIndexEntry> {
        self.0.earliest_time()
    }

    fn latest_time(&self) -> Option<TimeIndexEntry> {
        self.0.latest_time()
    }

    fn len(&self) -> usize {
        self.0.len()
    }
}

impl<T: InternalHistoryOps + 'static> IntoArcDynHistoryOps for ReversedHistoryOps<T>{}

// converts operations to return timestamps instead of TimeIndexEntry
#[derive(Debug, Clone, Copy)]
pub struct HistoryTimestamp<T>(pub(crate) T);

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

// converts operations to return date times instead of TimeIndexEntry
#[derive(Debug, Clone, Copy)]
pub struct HistoryDateTime<T>(pub(crate) T);

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

// converts operations to return secondary time information inside TimeIndexEntry
#[derive(Debug, Clone, Copy)]
pub struct HistorySecondaryIndex<T>(pub(crate) T);

impl<T: InternalHistoryOps> HistorySecondaryIndex<T> {
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
        self.0.iter().map(|t| t.1).collect()
    }

    pub fn collect_rev(&self) -> Vec<usize> {
        self.0.iter_rev().map(|t| t.1).collect()
    }
}

impl<'b, T: InternalHistoryOps + 'b, L, I: Copy> PartialEq<L> for HistorySecondaryIndex<T>
where
    for<'a> &'a L: IntoIterator<Item = &'a I>,
    usize: PartialEq<I>,
{
    fn eq(&self, other: &L) -> bool {
        self.iter().eq(other.into_iter().copied())
    }
}

impl<'a, T: InternalHistoryOps + 'a> PartialEq for HistorySecondaryIndex<T> {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}

impl<'a, T: InternalHistoryOps + 'a> Eq for HistorySecondaryIndex<T> {}

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
        let (len, sum) = self.iter().fold((0i64, 0f64), |(count, sum), item| {
            (count + 1, sum + (item as f64))
        });
        Some(sum / len as f64)
    }

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

    pub fn max(&self) -> Option<i64> {
        self.iter().max()
    }

    pub fn min(&self) -> Option<i64> {
        self.iter().min()
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

// Operations to access deletion history of some type
pub trait InternalDeletionOps: Send + Sync {
    fn iter(&self) -> BoxedLIter<TimeIndexEntry>;
    fn iter_rev(&self) -> BoxedLIter<TimeIndexEntry>;
    fn earliest_time(&self) -> Option<TimeIndexEntry>;
    fn latest_time(&self) -> Option<TimeIndexEntry>;
    fn first(&self) -> Option<TimeIndexEntry> {
        self.iter().next()
    }
    fn last(&self) -> Option<TimeIndexEntry> {
        self.iter_rev().next()
    }
    // override if we want more efficient implementation
    fn len(&self) -> usize {
        self.iter().count()
    }
}

/// Gives access to deletion information of an object when used as:
/// History<DeletionHistory<SomeItem>>
#[derive(Debug, Clone, Copy)]
pub struct DeletionHistory<T>(T);

impl<T: InternalDeletionOps> DeletionHistory<T> {
    pub fn new(item: T) -> Self {
        DeletionHistory(item)
    }
}

// this way, we can use all the History object functionality (converting to timestamps, dt, intervals)
impl<T: InternalDeletionOps> InternalHistoryOps for DeletionHistory<T> {
    fn iter(&self) -> BoxedLIter<TimeIndexEntry> {
        self.0.iter()
    }

    fn iter_rev(&self) -> BoxedLIter<TimeIndexEntry> {
        self.0.iter_rev()
    }

    fn earliest_time(&self) -> Option<TimeIndexEntry> {
        self.0.earliest_time()
    }

    fn latest_time(&self) -> Option<TimeIndexEntry> {
        self.0.latest_time()
    }
}

impl<T: InternalDeletionOps + 'static> IntoArcDynHistoryOps for DeletionHistory<T>{}

impl<G: BoxableGraphView + Clone> InternalDeletionOps for EdgeView<G> {
    fn iter(&self) -> BoxedLIter<TimeIndexEntry> {
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

    fn iter_rev(&self) -> BoxedLIter<TimeIndexEntry> {
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

    fn earliest_time(&self) -> Option<TimeIndexEntry> {
        InternalDeletionOps::iter(self).next()
    }

    fn latest_time(&self) -> Option<TimeIndexEntry> {
        InternalDeletionOps::iter_rev(self).next()
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
        assert_eq!(interval.median(), Some(9));

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
        let timestamps: Vec<_> = all_nodes_history
            .t()
            .iter_values()
            .flat_map(|ts| ts.collect())
            .collect();
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
        let secondary_times_lazy: Vec<_> = all_nodes_history
            .secondary_index()
            .iter_values()
            .flat_map(|s| s.collect())
            .collect();
        let secondary_times_normal: Vec<_> = nodes_history_as_history.secondary_index().collect();
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
