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
                    EdgeList, GraphTimeSemanticsOps, GraphView, Immutable, InheritLayerOps,
                    InheritMaterialize, InheritStorageOps, InternalEdgeFilterOps,
                    InternalEdgeLayerFilterOps, InternalExplodedEdgeFilterOps,
                    InternalNodeFilterOps, ListOps, NodeList, Static, TimeSemantics,
                },
                BoxedLIter, IntoDynBoxed,
            },
        },
        graph::graph::graph_equal,
    },
    prelude::GraphViewOps,
};
use raphtory_api::{
    core::{
        entities::{
            properties::prop::{Prop, PropType},
            LayerId, ELID,
        },
        storage::{arc_str::ArcStr, timeindex::EventTime},
    },
    inherit::Base,
    iter::IntoDynDBoxed,
};
use raphtory_storage::{
    core_ops::{CoreGraphOps, InheritCoreGraphOps},
    graph::{edges::edge_ref::EdgeEntryRef, nodes::node_ref::NodeStorageRef},
};
use std::{
    fmt::{Debug, Formatter},
    iter,
    ops::Range,
};

/// A struct that represents a windowed view of a `Graph`.
#[derive(Copy, Clone)]
pub struct WindowedGraph<G> {
    /// The underlying `Graph` object.
    pub graph: G,
    /// The inclusive start time of the window.
    pub start: Option<EventTime>,
    /// The exclusive end time of the window.
    pub end: Option<EventTime>,
}

/// The optional bounds of a window: an inclusive `start` and an exclusive
/// `end`. A missing bound does not bound. The window arithmetic of a windowed
/// view lives here, so every view that carries a window shares it.
#[derive(Copy, Clone, Debug)]
pub(crate) struct WindowBounds {
    pub(crate) start: Option<EventTime>,
    pub(crate) end: Option<EventTime>,
}

impl<G> Static for WindowedGraph<G> {}

impl<G: Debug> Debug for WindowedGraph<G> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WindowedGraph(start={:?}, end={:?}, graph={:?})",
            self.start, self.end, self.graph,
        )
    }
}

impl<'graph1, 'graph2, G1: GraphViewOps<'graph1>, G2: GraphViewOps<'graph2>> PartialEq<G2>
    for WindowedGraph<G1>
{
    fn eq(&self, other: &G2) -> bool {
        graph_equal(self, other)
    }
}

impl<'graph, G> Base for WindowedGraph<G> {
    type Base = G;
    #[inline(always)]
    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G> WindowedGraph<G> {
    /// The window the graph is seen through.
    #[inline(always)]
    fn bounds(&self) -> WindowBounds {
        WindowBounds::new(self.start, self.end)
    }
}

impl WindowBounds {
    pub(crate) fn new(start: Option<EventTime>, end: Option<EventTime>) -> Self {
        Self { start, end }
    }

    #[inline(always)]
    pub(crate) fn window_bound(&self) -> Range<EventTime> {
        self.start_bound()..self.end_bound()
    }

    pub(crate) fn start_bound(&self) -> EventTime {
        self.start.unwrap_or(EventTime::MIN)
    }

    #[inline(always)]
    pub(crate) fn end_bound(&self) -> EventTime {
        self.end.unwrap_or(EventTime::MAX)
    }

    #[inline(always)]
    pub(crate) fn window_is_empty(&self) -> bool {
        self.start_bound() >= self.end_bound()
    }

    #[inline]
    pub(crate) fn start_is_bounding<G: CoreGraphOps>(&self, graph: &G) -> bool {
        match self.start {
            None => false,
            Some(start) => match graph.core_graph().earliest_time() {
                None => false,
                Some(graph_earliest) => start >= graph_earliest, // deletions have exclusive window, thus >= here!
            },
        }
    }

    #[inline]
    pub(crate) fn end_is_bounding<G: CoreGraphOps>(&self, graph: &G) -> bool {
        match self.end {
            None => false,
            Some(end) => match graph.core_graph().latest_time() {
                None => false,
                Some(graph_latest) => end <= graph_latest,
            },
        }
    }

    #[inline]
    pub(crate) fn window_is_bounding<G: CoreGraphOps>(&self, graph: &G) -> bool {
        self.start_is_bounding(graph) || self.end_is_bounding(graph)
    }

    /// `graph`'s time semantics seen through the window. A window that does not
    /// bound `graph` leaves them as they are.
    fn windowed(&self, graph: &impl GraphView, semantics: TimeSemantics) -> TimeSemantics {
        if self.window_is_bounding(graph) {
            semantics.window(self.window_bound())
        } else {
            semantics
        }
    }

    pub(crate) fn node_time_semantics<G: GraphView>(&self, graph: &G) -> TimeSemantics {
        self.windowed(graph, graph.node_time_semantics())
    }

    pub(crate) fn edge_time_semantics<G: GraphView>(&self, graph: &G) -> TimeSemantics {
        self.windowed(graph, graph.edge_time_semantics())
    }

    pub(crate) fn earliest_time_global<G: GraphView>(&self, graph: &G) -> Option<i64> {
        if self.window_is_empty() {
            return None;
        }
        if self.window_is_bounding(graph) {
            graph.earliest_time_window(self.start_bound(), self.end_bound())
        } else {
            graph.earliest_time_global()
        }
    }

    pub(crate) fn latest_time_global<G: GraphView>(&self, graph: &G) -> Option<i64> {
        if self.window_is_empty() {
            return None;
        }
        if self.window_is_bounding(graph) {
            graph.latest_time_window(self.start_bound(), self.end_bound())
        } else {
            graph.latest_time_global()
        }
    }

    pub(crate) fn has_temporal_prop<G: GraphView>(&self, graph: &G, prop_id: usize) -> bool {
        if self.window_is_empty() {
            return false;
        }
        graph.has_temporal_prop_window(prop_id, self.window_bound())
    }

    pub(crate) fn temporal_prop_iter<'a, G: GraphView>(
        &self,
        graph: &'a G,
        prop_id: usize,
    ) -> BoxedLIter<'a, (EventTime, Prop)> {
        if self.window_is_empty() {
            return iter::empty().into_dyn_dboxed();
        }
        graph.temporal_prop_iter_window(prop_id, self.start_bound(), self.end_bound())
    }

    pub(crate) fn temporal_prop_last_at<G: GraphView>(
        &self,
        graph: &G,
        prop_id: usize,
        t: EventTime,
    ) -> Option<(EventTime, Prop)> {
        graph.temporal_prop_last_at_window(prop_id, t, self.window_bound())
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
        graph
            .temporal_prop_iter_window(id, self.start_bound(), self.end_bound())
            .into_dyn_boxed()
    }

    pub(crate) fn temporal_iter_rev<'a, G: GraphView>(
        &self,
        graph: &'a G,
        id: usize,
    ) -> BoxedLIter<'a, (EventTime, Prop)> {
        graph
            .temporal_prop_iter_window_rev(id, self.start_bound(), self.end_bound())
            .into_dyn_boxed()
    }

    pub(crate) fn temporal_value_at<G: GraphView>(
        &self,
        graph: &G,
        id: usize,
        t: EventTime,
    ) -> Option<Prop> {
        graph
            .temporal_prop_last_at_window(id, t, self.window_bound())
            .map(|(_, p)| p)
    }

    /// The id of the temporal property `name` of `graph`, when it has a value in
    /// the window. A window that does not bound `graph` hides no property.
    pub(crate) fn get_temporal_prop_id<G: GraphView>(
        &self,
        graph: &G,
        name: &str,
    ) -> Option<usize> {
        graph
            .get_temporal_prop_id(name)
            .filter(|id| !self.window_is_bounding(graph) || self.has_temporal_prop(graph, *id))
    }

    /// The ids of the temporal properties of `graph` with a value in the window.
    /// A window that does not bound `graph` hides no property.
    pub(crate) fn temporal_prop_ids<'a, G: GraphView>(
        &self,
        graph: &'a G,
    ) -> BoxedLIter<'a, usize> {
        if !self.window_is_bounding(graph) {
            return graph.temporal_prop_ids();
        }
        let bounds = *self;
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
        if self.bounds().window_is_empty() {
            NodeList::List {
                elems: Index::default(),
            }
        } else {
            self.graph.node_list()
        }
    }

    fn edge_list(&self) -> EdgeList {
        if self.bounds().window_is_empty() {
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
        self.bounds().window_is_empty() || self.graph.internal_nodes_filtered()
    }

    #[inline]
    fn internal_node_list_trusted(&self) -> bool {
        self.bounds().window_is_empty()
            || (self.graph.internal_node_list_trusted()
                && !self.bounds().window_is_bounding(&self.graph))
    }

    #[inline]
    fn edge_filter_includes_node_filter(&self) -> bool {
        self.bounds().window_is_empty() || self.graph.edge_filter_includes_node_filter()
    }

    #[inline]
    fn edge_layer_filter_includes_node_filter(&self) -> bool {
        self.bounds().window_is_empty() || self.graph.edge_layer_filter_includes_node_filter()
    }

    #[inline]
    fn exploded_edge_filter_includes_node_filter(&self) -> bool {
        self.bounds().window_is_empty() || self.graph.exploded_edge_filter_includes_node_filter()
    }

    #[inline]
    fn internal_filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        !self.bounds().window_is_empty() && self.graph.internal_filter_node(node, layer_ids)
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
        self.bounds().temporal_value(&self.graph, id)
    }

    fn temporal_iter(&self, id: usize) -> BoxedLIter<'_, (EventTime, Prop)> {
        self.bounds().temporal_iter(&self.graph, id)
    }

    fn temporal_iter_rev(&self, id: usize) -> BoxedLIter<'_, (EventTime, Prop)> {
        self.bounds().temporal_iter_rev(&self.graph, id)
    }

    fn temporal_value_at(&self, id: usize, t: EventTime) -> Option<Prop> {
        self.bounds().temporal_value_at(&self.graph, id, t)
    }
}

impl<G: GraphView> InternalTemporalPropertiesOps for WindowedGraph<G> {
    fn get_temporal_prop_id(&self, name: &str) -> Option<usize> {
        self.bounds().get_temporal_prop_id(&self.graph, name)
    }

    fn get_temporal_prop_name(&self, id: usize) -> ArcStr {
        self.graph.get_temporal_prop_name(id)
    }

    fn temporal_prop_ids(&self) -> BoxedLIter<'_, usize> {
        self.bounds().temporal_prop_ids(&self.graph)
    }
}

impl<G: GraphView> GraphTimeSemanticsOps for WindowedGraph<G> {
    fn node_time_semantics(&self) -> TimeSemantics {
        self.bounds().node_time_semantics(&self.graph)
    }

    fn edge_time_semantics(&self) -> TimeSemantics {
        self.bounds().edge_time_semantics(&self.graph)
    }

    #[inline]
    fn window_filtered(&self) -> bool {
        self.bounds().window_is_bounding(&self.graph)
    }

    fn view_start(&self) -> Option<EventTime> {
        self.bounds().start
    }

    fn view_end(&self) -> Option<EventTime> {
        self.bounds().end
    }

    #[inline]
    fn earliest_time_global(&self) -> Option<i64> {
        self.bounds().earliest_time_global(&self.graph)
    }

    #[inline]
    fn latest_time_global(&self) -> Option<i64> {
        self.bounds().latest_time_global(&self.graph)
    }

    #[inline]
    fn earliest_time_window(&self, start: EventTime, end: EventTime) -> Option<i64> {
        self.graph.earliest_time_window(start, end)
    }

    #[inline]
    fn latest_time_window(&self, start: EventTime, end: EventTime) -> Option<i64> {
        self.graph.latest_time_window(start, end)
    }

    fn has_temporal_prop(&self, prop_id: usize) -> bool {
        self.bounds().has_temporal_prop(&self.graph, prop_id)
    }

    fn temporal_prop_iter(&self, prop_id: usize) -> BoxedLIter<'_, (EventTime, Prop)> {
        self.bounds().temporal_prop_iter(&self.graph, prop_id)
    }

    fn has_temporal_prop_window(&self, prop_id: usize, w: Range<EventTime>) -> bool {
        self.graph.has_temporal_prop_window(prop_id, w.start..w.end)
    }

    fn temporal_prop_iter_window(
        &self,
        prop_id: usize,
        start: EventTime,
        end: EventTime,
    ) -> BoxedLIter<'_, (EventTime, Prop)> {
        self.graph.temporal_prop_iter_window(prop_id, start, end)
    }

    fn temporal_prop_iter_window_rev(
        &self,
        prop_id: usize,
        start: EventTime,
        end: EventTime,
    ) -> BoxedLIter<'_, (EventTime, Prop)> {
        self.graph
            .temporal_prop_iter_window_rev(prop_id, start, end)
    }

    fn temporal_prop_last_at(&self, prop_id: usize, t: EventTime) -> Option<(EventTime, Prop)> {
        self.bounds().temporal_prop_last_at(&self.graph, prop_id, t)
    }

    fn temporal_prop_last_at_window(
        &self,
        prop_id: usize,
        t: EventTime,
        w: Range<EventTime>,
    ) -> Option<(EventTime, Prop)> {
        self.graph.temporal_prop_last_at_window(prop_id, t, w)
    }
}

// actual filtering is handled upstream for efficiency and to avoid double-checking nested windows
// here we just define the optimisation flags
impl<G: GraphView> InternalEdgeFilterOps for WindowedGraph<G> {
    #[inline]
    fn internal_edge_filtered(&self) -> bool {
        self.bounds().window_is_empty() || self.graph.internal_edge_filtered()
    }

    #[inline]
    fn internal_edge_list_trusted(&self) -> bool {
        self.bounds().window_is_empty()
            || (!self.bounds().window_is_bounding(&self.graph)
                && self.graph.internal_edge_list_trusted())
    }

    #[inline]
    fn internal_filter_edge(&self, edge: EdgeEntryRef, layer_ids: &LayerIds) -> bool {
        !self.bounds().window_is_empty() && self.graph.internal_filter_edge(edge, layer_ids)
    }

    #[inline]
    fn node_filter_includes_edge_filter(&self) -> bool {
        self.bounds().window_is_empty() || self.graph.node_filter_includes_edge_filter()
    }
}
impl<G: GraphView> InternalEdgeLayerFilterOps for WindowedGraph<G> {
    #[inline]
    fn internal_edge_layer_filtered(&self) -> bool {
        self.graph.internal_edge_layer_filtered()
    }

    #[inline]
    fn internal_layer_filter_edge_list_trusted(&self) -> bool {
        self.bounds().window_is_empty()
            || (!self.bounds().window_is_bounding(&self.graph)
                && self.graph.internal_layer_filter_edge_list_trusted())
    }

    #[inline]
    fn internal_filter_edge_layer(&self, edge: EdgeEntryRef, layer: LayerId) -> bool {
        self.graph.internal_filter_edge_layer(edge, layer)
    }

    #[inline]
    fn node_filter_includes_edge_layer_filter(&self) -> bool {
        self.bounds().window_is_empty() || self.graph.node_filter_includes_edge_layer_filter()
    }
}

impl<G: GraphView> InternalExplodedEdgeFilterOps for WindowedGraph<G> {
    #[inline]
    fn internal_exploded_edge_filtered(&self) -> bool {
        self.graph.internal_exploded_edge_filtered()
    }

    #[inline]
    fn internal_exploded_filter_edge_list_trusted(&self) -> bool {
        self.bounds().window_is_empty()
            || (!self.bounds().window_is_bounding(&self.graph)
                && self.graph.internal_exploded_filter_edge_list_trusted())
    }

    #[inline]
    fn internal_filter_exploded_edge(&self, eid: ELID, t: EventTime, layer_ids: &LayerIds) -> bool {
        self.graph.internal_filter_exploded_edge(eid, t, layer_ids)
    }

    #[inline]
    fn node_filter_includes_exploded_edge_filter(&self) -> bool {
        self.bounds().window_is_empty() || self.graph.node_filter_includes_exploded_edge_filter()
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
        WindowedGraph { graph, start, end }
    }
}
