//! A windowed view is a subset of a graph between a specific time window.
//! For example, lets say you wanted to run an algorithm each month over a graph, graph window
//! would allow you to split the graph into 30 day chunks to do so.
//!
//! This module also defines the `GraphWindow` trait, which represents a window of time over
//! which a graph can be queried.
//!
//! GraphWindowSet implements the `Iterator` trait, producing `WindowedGraph` views
//! for each perspective within it.
//!
//! # Types
//!
//! * `GraphWindowSet` - A struct that allows iterating over a Graph broken down into multiple
//!   windowed views. It contains a `Graph` and an iterator of `Perspective`.
//!
//! * `WindowedGraph` - A struct that represents a windowed view of a `Graph`.
//!   It contains a `Graph`, a start time (`start`) and an end time (`end`).
//!
//! # Traits
//!
//! * `GraphViewInternalOps` - A trait that provides operations to a `WindowedGraph`
//!   used internally by the `GraphWindowSet`.
//!
//! # Examples
//!
//! ```rust
//!
//! use raphtory::prelude::*;
//! use raphtory::db::api::view::*;
//!
//! let graph = Graph::new();
//! graph.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
//! graph.add_edge(1, 1, 3, NO_PROPS, None).unwrap();
//! graph.add_edge(2, 2, 3, NO_PROPS, None).unwrap();
//!
//!  let wg = graph.window(0, 1);
//!  assert_eq!(wg.edge(1, 2).unwrap().src().id(), GID::U64(1));
//! ```

use crate::{
    core::entities::LayerIds,
    db::{
        api::{
            properties::internal::{
                InheritEdgePropertySchemaOps, InheritMetadataPropertiesOps,
                InheritNodePropertySchemaOps, InternalTemporalPropertiesOps,
                InternalTemporalPropertyViewOps,
            },
            state::Index,
            view::{
                internal::{
                    EdgeList, FilterOps, GraphTimeSemanticsOps, GraphView, Immutable,
                    InheritLayerOps, InheritMaterialize, InheritStorageOps, InternalEdgeFilterOps,
                    InternalEdgeLayerFilterOps, InternalExplodedEdgeFilterOps, InternalLayerOps,
                    InternalNodeFilterOps, ListOps, NodeList, Static, TimeRanges, TimeSemantics,
                },
                BoxedLIter, IntoDynBoxed,
            },
        },
        graph::{graph::graph_equal, views::layer_graph::LayeredGraph},
    },
    prelude::GraphViewOps,
};
use either::Either;
use raphtory_api::{
    core::{
        entities::{
            properties::prop::{Prop, PropType},
            LayerId, EID, ELID, VID,
        },
        storage::{
            arc_str::ArcStr,
            timeindex::{AsTime, EventTime},
        },
    },
    inherit::Base,
    iter::IntoDynDBoxed,
};
use raphtory_storage::{
    core_ops::{CoreGraphOps, InheritCoreGraphOps},
    graph::{
        edges::{edge_ref::EdgeEntryRef, edge_storage_ops::EdgeStorageOps},
        nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
    },
};
use rayon::prelude::*;
use roaring::RoaringBitmap;
use std::{
    fmt::{Debug, Formatter},
    iter,
    ops::Range,
    sync::{Arc, OnceLock},
};
use storage::api::time_index::SegmentBitmaps;

/// A struct that represents a windowed view of a `Graph`.
#[derive(Clone)]
pub struct WindowedGraph<G> {
    /// The underlying `Graph` object.
    pub graph: G,
    /// The time the view keeps: one window, or ranges already resolved against
    /// `graph` when the view was built, so no call resolves them again.
    bounds: WindowBounds,
    /// Time-index masks, filled by the first listing or count; shared by clones.
    masks: ViewMasks,
}

/// Exact nodes and edges of a window over some layers of an otherwise unfiltered graph.
#[derive(Debug)]
pub struct WindowMasks {
    nodes: SegmentBitmaps,
    edges: SegmentBitmaps,
    // shortcut: 8 bytes per listed node, give `Index` a bitmap variant for billions of nodes
    node_list: OnceLock<Index<VID>>,
}

impl WindowMasks {
    pub(crate) fn node_list(&self) -> NodeList {
        let elems = self
            .node_list
            .get_or_init(|| Index::from_sorted(self.nodes.iter().map(VID).collect(), true));
        NodeList::List {
            elems: elems.clone(),
        }
    }

    pub(crate) fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub(crate) fn edge_count(&self) -> usize {
        self.edges.len()
    }

    #[inline]
    pub(crate) fn has_node(&self, vid: VID) -> bool {
        self.nodes.contains(vid.index())
    }

    #[inline]
    pub(crate) fn has_edge(&self, eid: EID) -> bool {
        self.edges.contains(eid.0)
    }
}

/// Filled by the first listing or count, shared by clones; per-entity checks only read.
#[derive(Clone, Default)]
pub(crate) struct ViewMasks(Arc<OnceLock<Option<WindowMasks>>>);

impl ViewMasks {
    /// Never resolves.
    pub(crate) fn none() -> Self {
        Self(Arc::new(OnceLock::from(None)))
    }

    pub(crate) fn get_or_resolve(
        &self,
        resolve: impl FnOnce() -> Option<WindowMasks>,
    ) -> Option<&WindowMasks> {
        self.0.get_or_init(resolve).as_ref()
    }

    #[inline]
    pub(crate) fn resolved(&self) -> Option<&WindowMasks> {
        self.0.get().and_then(|masks| masks.as_ref())
    }
}

/// The time a view is bounded to: one window, or a set of disjoint ranges. The
/// window arithmetic of a windowed view lives here, so every view that carries
/// a window or a range set shares it.
#[derive(Clone, Debug)]
pub(crate) enum WindowBounds {
    /// An inclusive `start` and an exclusive `end`. A missing bound does not
    /// bound. A `WindowedGraph` builds this on every call, so it never
    /// allocates.
    Window {
        start: Option<EventTime>,
        end: Option<EventTime>,
    },
    /// Sorted, disjoint ranges, already resolved against the graph they bound
    /// when the view was built, so no call intersects them again.
    Ranges(TimeRanges),
}

impl<G> Static for WindowedGraph<G> {}

impl<G: Debug> Debug for WindowedGraph<G> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.bounds {
            WindowBounds::Window { start, end } => write!(
                f,
                "WindowedGraph(start={:?}, end={:?}, graph={:?})",
                start, end, self.graph,
            ),
            WindowBounds::Ranges(ranges) => write!(
                f,
                "WindowedGraph(ranges={:?}, graph={:?})",
                ranges, self.graph,
            ),
        }
    }
}

impl<'graph1, 'graph2, G1: GraphViewOps<'graph1>, G2: GraphViewOps<'graph2>> PartialEq<G2>
    for WindowedGraph<G1>
{
    fn eq(&self, other: &G2) -> bool {
        graph_equal(self, other)
    }
}

impl<G> Base for WindowedGraph<G> {
    type Base = G;
    #[inline(always)]
    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl WindowBounds {
    pub(crate) fn new(start: Option<EventTime>, end: Option<EventTime>) -> Self {
        Self::Window { start, end }
    }

    /// The bounds of a resolved range set: no bound for every time, a window
    /// for one range, and the set itself otherwise.
    pub(crate) fn from_ranges(ranges: TimeRanges) -> Self {
        match (ranges.len(), ranges.first()) {
            (1, Some(range)) => Self::new(
                (range.start != EventTime::MIN).then_some(range.start),
                (range.end != EventTime::MAX).then_some(range.end),
            ),
            _ => Self::Ranges(ranges),
        }
    }

    /// The inclusive start of the bounds, if they have one. Ranges that reach
    /// back to the earliest time have none.
    pub(crate) fn start(&self) -> Option<EventTime> {
        match self {
            Self::Window { start, .. } => *start,
            Self::Ranges(ranges) => ranges.start().filter(|start| *start != EventTime::MIN),
        }
    }

    /// The exclusive end of the bounds, if they have one. Ranges that run on to
    /// the latest time have none.
    pub(crate) fn end(&self) -> Option<EventTime> {
        match self {
            Self::Window { end, .. } => *end,
            Self::Ranges(ranges) => ranges.end().filter(|end| *end != EventTime::MAX),
        }
    }

    /// The times these bounds keep of `graph`: a range set as it was resolved
    /// against `graph` when the view was built, a window as `graph`'s own
    /// ranges cut to it.
    pub(crate) fn view_ranges<G: GraphView>(&self, graph: &G) -> TimeRanges {
        match self {
            Self::Window { .. } => graph.view_ranges().clipped_to(&self.window_bound()),
            Self::Ranges(ranges) => ranges.clone(),
        }
    }

    #[inline(always)]
    fn window_bound(&self) -> Range<EventTime> {
        self.start_bound()..self.end_bound()
    }

    fn start_bound(&self) -> EventTime {
        self.start().unwrap_or(EventTime::MIN)
    }

    #[inline(always)]
    fn end_bound(&self) -> EventTime {
        self.end().unwrap_or(EventTime::MAX)
    }

    #[inline(always)]
    pub(crate) fn window_is_empty(&self) -> bool {
        match self {
            Self::Window { .. } => self.start_bound() >= self.end_bound(),
            Self::Ranges(ranges) => ranges.is_empty(),
        }
    }

    #[inline]
    fn start_is_bounding<G: CoreGraphOps>(&self, graph: &G) -> bool {
        match self.start() {
            None => false,
            Some(start) => match graph.core_graph().earliest_time() {
                None => false,
                Some(graph_earliest) => start >= graph_earliest, // deletions have exclusive window, thus >= here!
            },
        }
    }

    #[inline]
    fn end_is_bounding<G: CoreGraphOps>(&self, graph: &G) -> bool {
        match self.end() {
            None => false,
            Some(end) => match graph.core_graph().latest_time() {
                None => false,
                Some(graph_latest) => end <= graph_latest,
            },
        }
    }

    /// Whether the bounds remove any event of `graph`. A range set holds none
    /// or several ranges (one is a window), so it always does.
    #[inline]
    pub(crate) fn window_is_bounding<G: CoreGraphOps>(&self, graph: &G) -> bool {
        match self {
            Self::Ranges(_) => true,
            Self::Window { .. } => self.start_is_bounding(graph) || self.end_is_bounding(graph),
        }
    }

    /// `graph`'s time semantics seen through the bounds. Bounds that do not
    /// restrict `graph` leave them as they are; a range set was resolved
    /// against `graph` when it was built, so it only replaces the restriction.
    fn bounded(&self, graph: &impl GraphView, semantics: TimeSemantics) -> TimeSemantics {
        match self {
            Self::Window { .. } if self.window_is_bounding(graph) => {
                semantics.window(self.window_bound())
            }
            Self::Window { .. } => semantics,
            Self::Ranges(ranges) => TimeSemantics::from_ranges(semantics.base(), ranges.clone()),
        }
    }

    pub(crate) fn node_time_semantics<G: GraphView>(&self, graph: &G) -> TimeSemantics {
        self.bounded(graph, graph.node_time_semantics())
    }

    pub(crate) fn edge_time_semantics<G: GraphView>(&self, graph: &G) -> TimeSemantics {
        self.bounded(graph, graph.edge_time_semantics())
    }

    pub(crate) fn earliest_time_global<G: GraphView>(&self, graph: &G) -> Option<i64> {
        if self.window_is_empty() {
            return None;
        }
        match self {
            Self::Window { .. } if self.window_is_bounding(graph) => {
                graph.earliest_time_window(self.start_bound(), self.end_bound())
            }
            Self::Window { .. } => graph.earliest_time_global(),
            // The ranges are ordered and disjoint, so the first one holding an
            // event holds the earliest of them all.
            Self::Ranges(ranges) => ranges
                .iter()
                .find_map(|w| graph.earliest_time_window(w.start, w.end)),
        }
    }

    pub(crate) fn latest_time_global<G: GraphView>(&self, graph: &G) -> Option<i64> {
        if self.window_is_empty() {
            return None;
        }
        match self {
            Self::Window { .. } if self.window_is_bounding(graph) => {
                graph.latest_time_window(self.start_bound(), self.end_bound())
            }
            Self::Window { .. } => graph.latest_time_global(),
            Self::Ranges(ranges) => ranges
                .iter()
                .rev()
                .find_map(|w| graph.latest_time_window(w.start, w.end)),
        }
    }

    pub(crate) fn earliest_time_window<G: GraphView>(
        &self,
        graph: &G,
        start: EventTime,
        end: EventTime,
    ) -> Option<i64> {
        match self {
            Self::Window { .. } => graph.earliest_time_window(start, end),
            Self::Ranges(ranges) => ranges
                .clipped_to(&(start..end))
                .iter()
                .find_map(|w| graph.earliest_time_window(w.start, w.end)),
        }
    }

    pub(crate) fn latest_time_window<G: GraphView>(
        &self,
        graph: &G,
        start: EventTime,
        end: EventTime,
    ) -> Option<i64> {
        match self {
            Self::Window { .. } => graph.latest_time_window(start, end),
            Self::Ranges(ranges) => ranges
                .clipped_to(&(start..end))
                .iter()
                .rev()
                .find_map(|w| graph.latest_time_window(w.start, w.end)),
        }
    }

    pub(crate) fn has_temporal_prop<G: GraphView>(&self, graph: &G, prop_id: usize) -> bool {
        if self.window_is_empty() {
            return false;
        }
        match self {
            Self::Window { .. } => graph.has_temporal_prop_window(prop_id, self.window_bound()),
            Self::Ranges(ranges) => ranges
                .iter()
                .any(|w| graph.has_temporal_prop_window(prop_id, w)),
        }
    }

    pub(crate) fn has_temporal_prop_window<G: GraphView>(
        &self,
        graph: &G,
        prop_id: usize,
        w: Range<EventTime>,
    ) -> bool {
        match self {
            Self::Window { .. } => graph.has_temporal_prop_window(prop_id, w),
            Self::Ranges(ranges) => ranges
                .clipped_to(&(w.start..w.end))
                .iter()
                .any(|w| graph.has_temporal_prop_window(prop_id, w)),
        }
    }

    pub(crate) fn temporal_prop_iter<'a, G: GraphView>(
        &self,
        graph: &'a G,
        prop_id: usize,
    ) -> BoxedLIter<'a, (EventTime, Prop)> {
        if self.window_is_empty() {
            return iter::empty().into_dyn_dboxed();
        }
        self.temporal_prop_iter_window(graph, prop_id, self.start_bound(), self.end_bound())
    }

    /// The updates in `start..end`, range by range in order. The ranges are
    /// sorted and disjoint, so the pieces chain into one sorted stream.
    pub(crate) fn temporal_prop_iter_window<'a, G: GraphView>(
        &self,
        graph: &'a G,
        prop_id: usize,
        start: EventTime,
        end: EventTime,
    ) -> BoxedLIter<'a, (EventTime, Prop)> {
        match self {
            Self::Window { .. } => graph.temporal_prop_iter_window(prop_id, start, end),
            Self::Ranges(ranges) => ranges
                .clipped_to(&(start..end))
                .into_iter()
                .flat_map(move |w| graph.temporal_prop_iter_window(prop_id, w.start, w.end))
                .into_dyn_boxed(),
        }
    }

    pub(crate) fn temporal_prop_iter_window_rev<'a, G: GraphView>(
        &self,
        graph: &'a G,
        prop_id: usize,
        start: EventTime,
        end: EventTime,
    ) -> BoxedLIter<'a, (EventTime, Prop)> {
        match self {
            Self::Window { .. } => graph.temporal_prop_iter_window_rev(prop_id, start, end),
            Self::Ranges(ranges) => ranges
                .clipped_to(&(start..end))
                .into_iter()
                .rev()
                .flat_map(move |w| graph.temporal_prop_iter_window_rev(prop_id, w.start, w.end))
                .into_dyn_boxed(),
        }
    }

    pub(crate) fn temporal_prop_last_at<G: GraphView>(
        &self,
        graph: &G,
        prop_id: usize,
        t: EventTime,
    ) -> Option<(EventTime, Prop)> {
        self.temporal_prop_last_at_window(graph, prop_id, t, self.window_bound())
    }

    /// The last update at or before `t` inside `w`. Over a range set, the
    /// ranges that start after `t` cannot hold it, and the latest range that
    /// has one wins.
    pub(crate) fn temporal_prop_last_at_window<G: GraphView>(
        &self,
        graph: &G,
        prop_id: usize,
        t: EventTime,
        w: Range<EventTime>,
    ) -> Option<(EventTime, Prop)> {
        match self {
            Self::Window { .. } => graph.temporal_prop_last_at_window(prop_id, t, w),
            Self::Ranges(ranges) => ranges
                .clipped_to(&(w.start..w.end))
                .iter()
                .rev()
                .skip_while(|w| w.start > t)
                .find_map(|w| graph.temporal_prop_last_at_window(prop_id, t, w)),
        }
    }

    pub(crate) fn temporal_value<G: GraphView>(&self, graph: &G, id: usize) -> Option<Prop> {
        graph.temporal_value_at(id, self.end_bound())
    }

    pub(crate) fn temporal_iter<'a, G: GraphView>(
        &self,
        graph: &'a G,
        id: usize,
    ) -> BoxedLIter<'a, (EventTime, Prop)> {
        if self.window_is_empty() {
            return iter::empty().into_dyn_boxed();
        }
        self.temporal_prop_iter_window(graph, id, self.start_bound(), self.end_bound())
            .into_dyn_boxed()
    }

    pub(crate) fn temporal_iter_rev<'a, G: GraphView>(
        &self,
        graph: &'a G,
        id: usize,
    ) -> BoxedLIter<'a, (EventTime, Prop)> {
        self.temporal_prop_iter_window_rev(graph, id, self.start_bound(), self.end_bound())
            .into_dyn_boxed()
    }

    pub(crate) fn temporal_value_at<G: GraphView>(
        &self,
        graph: &G,
        id: usize,
        t: EventTime,
    ) -> Option<Prop> {
        self.temporal_prop_last_at(graph, id, t).map(|(_, p)| p)
    }

    /// The id of the temporal property `name` of `graph`, when it has a value in
    /// the bounds. Bounds that do not restrict `graph` hide no property.
    pub(crate) fn get_temporal_prop_id<G: GraphView>(
        &self,
        graph: &G,
        name: &str,
    ) -> Option<usize> {
        graph
            .get_temporal_prop_id(name)
            .filter(|id| !self.window_is_bounding(graph) || self.has_temporal_prop(graph, *id))
    }

    /// The ids of the temporal properties of `graph` with a value in the bounds.
    /// Bounds that do not restrict `graph` hide no property.
    pub(crate) fn temporal_prop_ids<'a, G: GraphView>(
        &self,
        graph: &'a G,
    ) -> BoxedLIter<'a, usize> {
        if !self.window_is_bounding(graph) {
            return graph.temporal_prop_ids();
        }
        let bounds = self.clone();
        Box::new(
            graph
                .temporal_prop_ids()
                .filter(move |id| bounds.has_temporal_prop(graph, *id)),
        )
    }
}

impl<G> Immutable for WindowedGraph<G> {}

impl<G> InheritCoreGraphOps for WindowedGraph<G> {}

impl<G> InheritStorageOps for WindowedGraph<G> {}

impl<G> InheritMaterialize for WindowedGraph<G> {}

impl<G> InheritMetadataPropertiesOps for WindowedGraph<G> {}

impl<G> InheritNodePropertySchemaOps for WindowedGraph<G> {}

impl<G> InheritEdgePropertySchemaOps for WindowedGraph<G> {}

impl<G> InheritLayerOps for WindowedGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> ListOps for WindowedGraph<G> {
    fn node_list(&self) -> NodeList {
        if self.bounds.window_is_empty() {
            NodeList::List {
                elems: Index::default(),
            }
        } else if let Some(masks) = self.masks() {
            masks.node_list()
        } else {
            self.graph.node_list()
        }
    }

    fn node_count(&self) -> Option<usize> {
        self.masks().map(WindowMasks::node_count)
    }

    fn edge_count(&self) -> Option<usize> {
        self.masks().map(WindowMasks::edge_count)
    }

    fn window_masks(&self, layer_ids: &LayerIds) -> Option<WindowMasks> {
        let WindowBounds::Window { start, end } = &self.bounds else {
            return None;
        };
        if self.bounds.window_is_empty()
            || !self.bounds.window_is_bounding(&self.graph)
            || self.graph.filtered_excluding_layers()
        {
            return None;
        }
        let persistent = self.graph.edge_time_semantics().is_persistent();
        let candidates = self.graph.core_graph().window_candidates(
            persistent,
            start.map_or(i64::MIN, |start| start.t()),
            end.map_or(i64::MAX, |end| end.t()),
            layer_ids,
        )?;
        // an unmasked copy of this view checks `maybe_*`
        let scan = LayeredGraph::unmasked(
            WindowedGraph {
                graph: &self.graph,
                bounds: self.bounds.clone(),
                masks: ViewMasks::none(),
            },
            layer_ids.clone(),
        );
        let nodes = verified(candidates.sure_nodes, candidates.maybe_nodes, |vid| {
            scan.filter_node(scan.core_node(VID(vid)).as_ref())
        });
        let edges = verified(candidates.sure_edges, candidates.maybe_edges, |eid| {
            scan.filter_edge(scan.core_edge(Either::Left(EID(eid))).as_ref())
        });
        Some(WindowMasks {
            nodes,
            edges,
            node_list: OnceLock::new(),
        })
    }

    fn edge_list(&self) -> EdgeList {
        if self.bounds.window_is_empty() {
            EdgeList::List {
                elems: Index::default(),
            }
        } else {
            self.graph.edge_list()
        }
    }
}

impl<G: GraphView> InternalNodeFilterOps for WindowedGraph<G> {
    #[inline]
    fn internal_nodes_filtered(&self) -> bool {
        self.bounds.window_is_empty() || self.graph.internal_nodes_filtered()
    }

    #[inline]
    fn internal_node_list_trusted(&self) -> bool {
        self.bounds.window_is_empty()
            || self.masks.resolved().is_some()
            || (self.graph.internal_node_list_trusted()
                && !self.bounds.window_is_bounding(&self.graph))
    }

    #[inline]
    fn node_filter_includes_window_filter(&self) -> bool {
        self.masks.resolved().is_some()
    }

    #[inline]
    fn edge_filter_includes_node_filter(&self) -> bool {
        self.bounds.window_is_empty() || self.graph.edge_filter_includes_node_filter()
    }

    #[inline]
    fn edge_layer_filter_includes_node_filter(&self) -> bool {
        self.bounds.window_is_empty() || self.graph.edge_layer_filter_includes_node_filter()
    }

    #[inline]
    fn exploded_edge_filter_includes_node_filter(&self) -> bool {
        self.bounds.window_is_empty() || self.graph.exploded_edge_filter_includes_node_filter()
    }

    #[inline]
    fn internal_filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        !self.bounds.window_is_empty()
            && self
                .masks
                .resolved()
                .is_none_or(|masks| masks.has_node(node.vid()))
            && self.graph.internal_filter_node(node, layer_ids)
    }
}

impl<G: GraphView> InternalTemporalPropertyViewOps for WindowedGraph<G> {
    fn dtype(&self, id: usize) -> PropType {
        self.graph
            .graph_props_meta()
            .temporal_prop_mapper()
            .get_dtype(id)
            .unwrap()
    }

    fn temporal_value(&self, id: usize) -> Option<Prop> {
        self.bounds.temporal_value(&self.graph, id)
    }

    fn temporal_iter(&self, id: usize) -> BoxedLIter<'_, (EventTime, Prop)> {
        self.bounds.temporal_iter(&self.graph, id)
    }

    fn temporal_iter_rev(&self, id: usize) -> BoxedLIter<'_, (EventTime, Prop)> {
        self.bounds.temporal_iter_rev(&self.graph, id)
    }

    fn temporal_value_at(&self, id: usize, t: EventTime) -> Option<Prop> {
        self.bounds.temporal_value_at(&self.graph, id, t)
    }
}

impl<G: GraphView> InternalTemporalPropertiesOps for WindowedGraph<G> {
    fn get_temporal_prop_id(&self, name: &str) -> Option<usize> {
        self.bounds.get_temporal_prop_id(&self.graph, name)
    }

    fn get_temporal_prop_name(&self, id: usize) -> ArcStr {
        self.graph.get_temporal_prop_name(id)
    }

    fn temporal_prop_ids(&self) -> BoxedLIter<'_, usize> {
        self.bounds.temporal_prop_ids(&self.graph)
    }
}

impl<G: GraphView> GraphTimeSemanticsOps for WindowedGraph<G> {
    fn node_time_semantics(&self) -> TimeSemantics {
        self.bounds.node_time_semantics(&self.graph)
    }

    fn edge_time_semantics(&self) -> TimeSemantics {
        self.bounds.edge_time_semantics(&self.graph)
    }

    #[inline]
    fn window_filtered(&self) -> bool {
        self.bounds.window_is_bounding(&self.graph)
    }

    fn view_start(&self) -> Option<EventTime> {
        self.bounds.start()
    }

    fn view_end(&self) -> Option<EventTime> {
        self.bounds.end()
    }

    fn view_ranges(&self) -> TimeRanges {
        self.bounds.view_ranges(&self.graph)
    }

    #[inline]
    fn earliest_time_global(&self) -> Option<i64> {
        self.bounds.earliest_time_global(&self.graph)
    }

    #[inline]
    fn latest_time_global(&self) -> Option<i64> {
        self.bounds.latest_time_global(&self.graph)
    }

    #[inline]
    fn earliest_time_window(&self, start: EventTime, end: EventTime) -> Option<i64> {
        self.bounds.earliest_time_window(&self.graph, start, end)
    }

    #[inline]
    fn latest_time_window(&self, start: EventTime, end: EventTime) -> Option<i64> {
        self.bounds.latest_time_window(&self.graph, start, end)
    }

    fn has_temporal_prop(&self, prop_id: usize) -> bool {
        self.bounds.has_temporal_prop(&self.graph, prop_id)
    }

    fn temporal_prop_iter(&self, prop_id: usize) -> BoxedLIter<'_, (EventTime, Prop)> {
        self.bounds.temporal_prop_iter(&self.graph, prop_id)
    }

    fn has_temporal_prop_window(&self, prop_id: usize, w: Range<EventTime>) -> bool {
        self.bounds
            .has_temporal_prop_window(&self.graph, prop_id, w)
    }

    fn temporal_prop_iter_window(
        &self,
        prop_id: usize,
        start: EventTime,
        end: EventTime,
    ) -> BoxedLIter<'_, (EventTime, Prop)> {
        self.bounds
            .temporal_prop_iter_window(&self.graph, prop_id, start, end)
    }

    fn temporal_prop_iter_window_rev(
        &self,
        prop_id: usize,
        start: EventTime,
        end: EventTime,
    ) -> BoxedLIter<'_, (EventTime, Prop)> {
        self.bounds
            .temporal_prop_iter_window_rev(&self.graph, prop_id, start, end)
    }

    fn temporal_prop_last_at(&self, prop_id: usize, t: EventTime) -> Option<(EventTime, Prop)> {
        self.bounds.temporal_prop_last_at(&self.graph, prop_id, t)
    }

    fn temporal_prop_last_at_window(
        &self,
        prop_id: usize,
        t: EventTime,
        w: Range<EventTime>,
    ) -> Option<(EventTime, Prop)> {
        self.bounds
            .temporal_prop_last_at_window(&self.graph, prop_id, t, w)
    }
}

// actual filtering is handled upstream for efficiency and to avoid double-checking nested windows
// here we just define the optimisation flags
impl<G: GraphView> InternalEdgeFilterOps for WindowedGraph<G> {
    #[inline]
    fn internal_edge_filtered(&self) -> bool {
        self.bounds.window_is_empty() || self.graph.internal_edge_filtered()
    }

    #[inline]
    fn internal_edge_list_trusted(&self) -> bool {
        self.bounds.window_is_empty()
            || (!self.bounds.window_is_bounding(&self.graph)
                && self.graph.internal_edge_list_trusted())
    }

    #[inline]
    fn internal_filter_edge(&self, edge: EdgeEntryRef, layer_ids: &LayerIds) -> bool {
        !self.bounds.window_is_empty()
            && self
                .masks
                .resolved()
                .is_none_or(|masks| masks.has_edge(edge.eid()))
            && self.graph.internal_filter_edge(edge, layer_ids)
    }

    #[inline]
    fn edge_filter_includes_window_filter(&self) -> bool {
        self.masks.resolved().is_some()
    }

    #[inline]
    fn internal_filter_edge_id(&self, eid: EID, layer_ids: &LayerIds) -> Option<bool> {
        // masks cover this view's layers only
        let masks = self
            .masks
            .resolved()
            .filter(|_| layer_ids == self.layer_ids())?;
        Some(masks.has_edge(eid))
    }

    #[inline]
    fn node_filter_includes_edge_filter(&self) -> bool {
        self.bounds.window_is_empty() || self.graph.node_filter_includes_edge_filter()
    }
}
impl<G: GraphView> InternalEdgeLayerFilterOps for WindowedGraph<G> {
    #[inline]
    fn internal_edge_layer_filtered(&self) -> bool {
        self.graph.internal_edge_layer_filtered()
    }

    #[inline]
    fn internal_layer_filter_edge_list_trusted(&self) -> bool {
        self.bounds.window_is_empty()
            || (!self.bounds.window_is_bounding(&self.graph)
                && self.graph.internal_layer_filter_edge_list_trusted())
    }

    #[inline]
    fn internal_filter_edge_layer(&self, edge: EdgeEntryRef, layer: LayerId) -> bool {
        self.graph.internal_filter_edge_layer(edge, layer)
    }

    #[inline]
    fn node_filter_includes_edge_layer_filter(&self) -> bool {
        self.bounds.window_is_empty() || self.graph.node_filter_includes_edge_layer_filter()
    }
}

impl<G: GraphView> InternalExplodedEdgeFilterOps for WindowedGraph<G> {
    #[inline]
    fn internal_exploded_edge_filtered(&self) -> bool {
        self.graph.internal_exploded_edge_filtered()
    }

    #[inline]
    fn internal_exploded_filter_edge_list_trusted(&self) -> bool {
        self.bounds.window_is_empty()
            || (!self.bounds.window_is_bounding(&self.graph)
                && self.graph.internal_exploded_filter_edge_list_trusted())
    }

    #[inline]
    fn internal_filter_exploded_edge(&self, eid: ELID, t: EventTime, layer_ids: &LayerIds) -> bool {
        self.graph.internal_filter_exploded_edge(eid, t, layer_ids)
    }

    #[inline]
    fn node_filter_includes_exploded_edge_filter(&self) -> bool {
        self.bounds.window_is_empty() || self.graph.node_filter_includes_exploded_edge_filter()
    }
}

/// A windowed graph is a graph that only allows access to nodes and edges within a time window.
///
/// This struct is used to represent a graph with a time window. It is constructed
/// by providing a `Graph` object and a time range that defines the window.
///
/// # Examples
///
/// ```rust
/// use raphtory::db::api::view::*;
/// use raphtory::prelude::*;
///
/// let graph = Graph::new();
/// graph.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
/// graph.add_edge(1, 2, 3, NO_PROPS, None).unwrap();
/// let windowed_graph = graph.window(0, 1);
/// ```
impl<G: GraphView> WindowedGraph<G> {
    /// Create a new windowed graph
    ///
    /// # Arguments
    ///
    /// - `graph` - The graph to create the windowed graph from
    /// - `start` - The inclusive start time of the window.
    /// - `end` - The exclusive end time of the window.
    ///
    /// Returns:
    ///
    /// A new windowed graph
    pub fn new(graph: G, start: Option<EventTime>, end: Option<EventTime>) -> Self {
        WindowedGraph {
            graph,
            bounds: WindowBounds::new(start, end),
            masks: Default::default(),
        }
    }

    /// A view of `graph` restricted to `ranges`. Any windowing the graph
    /// already carries is folded in here, once.
    pub fn from_ranges(graph: G, ranges: TimeRanges) -> Self {
        let ranges = ranges.intersect(&graph.view_ranges());
        WindowedGraph {
            graph,
            bounds: WindowBounds::from_ranges(ranges),
            masks: Default::default(),
        }
    }

    fn masks(&self) -> Option<&WindowMasks> {
        self.masks
            .get_or_resolve(|| self.window_masks(self.layer_ids()))
    }
}

/// `sure` plus the ids of `maybe` that pass `check`.
fn verified(
    mut sure: SegmentBitmaps,
    mut maybe: SegmentBitmaps,
    check: impl Fn(usize) -> bool + Sync,
) -> SegmentBitmaps {
    let stride = sure.stride;
    let segments = sure.segments.len().max(maybe.segments.len());
    sure.segments.resize(segments, RoaringBitmap::new());
    maybe.segments.resize(segments, RoaringBitmap::new());
    sure.segments
        .par_iter_mut()
        .zip(maybe.segments.par_iter())
        .enumerate()
        .for_each(|(segment, (sure, maybe))| {
            sure.extend(
                maybe
                    .iter()
                    .filter(|pos| check(segment * stride + *pos as usize)),
            );
        });
    sure
}
