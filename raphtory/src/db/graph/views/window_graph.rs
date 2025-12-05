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
//! windowed views. It contains a `Graph` and an iterator of `Perspective`.
//!
//! * `WindowedGraph` - A struct that represents a windowed view of a `Graph`.
//! It contains a `Graph`, a start time (`start`) and an end time (`end`).
//!
//! # Traits
//!
//! * `GraphViewInternalOps` - A trait that provides operations to a `WindowedGraph`
//! used internally by the `GraphWindowSet`.
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
                InheritMetadataPropertiesOps, InternalTemporalPropertiesOps,
                InternalTemporalPropertyViewOps,
            },
            state::Index,
            view::{
                internal::{
                    EdgeHistoryFilter, EdgeList, GraphTimeSemanticsOps, GraphView, Immutable,
                    InheritLayerOps, InheritMaterialize, InheritStorageOps, InternalEdgeFilterOps,
                    InternalEdgeLayerFilterOps, InternalExplodedEdgeFilterOps,
                    InternalNodeFilterOps, ListOps, NodeHistoryFilter, NodeList, Static,
                    TimeSemantics,
                },
                BoxableGraphView, BoxedLIter, IntoDynBoxed,
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
            EID, ELID, VID,
        },
        storage::{arc_str::ArcStr, timeindex::TimeIndexEntry},
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
    pub start: Option<i64>,
    /// The exclusive end time of the window.
    pub end: Option<i64>,
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

impl<'graph, G: GraphViewOps<'graph>> Base for WindowedGraph<G> {
    type Base = G;
    #[inline(always)]
    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G: BoxableGraphView + Clone> WindowedGraph<G> {
    #[inline(always)]
    fn window_bound(&self) -> Range<i64> {
        self.start_bound()..self.end_bound()
    }

    fn start_bound(&self) -> i64 {
        self.start.unwrap_or(i64::MIN)
    }

    #[inline(always)]
    fn end_bound(&self) -> i64 {
        self.end.unwrap_or(i64::MAX)
    }

    #[inline(always)]
    fn window_is_empty(&self) -> bool {
        self.start_bound() >= self.end_bound()
    }

    #[inline]
    fn start_is_bounding(&self) -> bool {
        match self.start {
            None => false,
            Some(start) => match self.graph.core_graph().earliest_time() {
                None => false,
                Some(graph_earliest) => start >= graph_earliest, // deletions have exclusive window, thus >= here!
            },
        }
    }

    #[inline]
    fn end_is_bounding(&self) -> bool {
        match self.end {
            None => false,
            Some(end) => match self.core_graph().latest_time() {
                None => false,
                Some(graph_latest) => end <= graph_latest,
            },
        }
    }
    #[inline]
    fn window_is_bounding(&self) -> bool {
        self.start_is_bounding() || self.end_is_bounding()
    }
}

impl<'graph, G: GraphViewOps<'graph>> Immutable for WindowedGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritCoreGraphOps for WindowedGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps for WindowedGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> NodeHistoryFilter for WindowedGraph<G> {
    fn is_node_prop_update_available(
        &self,
        prop_id: usize,
        node_id: VID,
        time: TimeIndexEntry,
    ) -> bool {
        self.graph
            .is_node_prop_update_available_window(prop_id, node_id, time, self.window_bound())
    }

    fn is_node_prop_update_available_window(
        &self,
        prop_id: usize,
        node_id: VID,
        time: TimeIndexEntry,
        w: Range<i64>,
    ) -> bool {
        self.graph
            .is_node_prop_update_available_window(prop_id, node_id, time, w)
    }

    fn is_node_prop_update_latest(
        &self,
        prop_id: usize,
        node_id: VID,
        time: TimeIndexEntry,
    ) -> bool {
        self.graph
            .is_node_prop_update_latest_window(prop_id, node_id, time, self.window_bound())
    }

    fn is_node_prop_update_latest_window(
        &self,
        prop_id: usize,
        node_id: VID,
        time: TimeIndexEntry,
        w: Range<i64>,
    ) -> bool {
        self.graph
            .is_node_prop_update_latest_window(prop_id, node_id, time, w)
    }
}

impl<'graph, G: GraphViewOps<'graph>> EdgeHistoryFilter for WindowedGraph<G> {
    fn is_edge_prop_update_available(
        &self,
        layer_id: usize,
        prop_id: usize,
        edge_id: EID,
        time: TimeIndexEntry,
    ) -> bool {
        self.graph.is_edge_prop_update_available_window(
            layer_id,
            prop_id,
            edge_id,
            time,
            self.window_bound(),
        )
    }

    fn is_edge_prop_update_available_window(
        &self,
        layer_id: usize,
        prop_id: usize,
        edge_id: EID,
        time: TimeIndexEntry,
        w: Range<i64>,
    ) -> bool {
        self.graph
            .is_edge_prop_update_available_window(layer_id, prop_id, edge_id, time, w)
    }

    fn is_edge_prop_update_latest(
        &self,
        layer_ids: &LayerIds,
        layer_id: usize,
        prop_id: usize,
        edge_id: EID,
        time: TimeIndexEntry,
    ) -> bool {
        self.graph.is_edge_prop_update_latest_window(
            layer_ids,
            layer_id,
            prop_id,
            edge_id,
            time,
            self.window_bound(),
        )
    }

    fn is_edge_prop_update_latest_window(
        &self,
        layer_ids: &LayerIds,
        layer_id: usize,
        prop_id: usize,
        edge_id: EID,
        time: TimeIndexEntry,
        w: Range<i64>,
    ) -> bool {
        self.graph
            .is_edge_prop_update_latest_window(layer_ids, layer_id, prop_id, edge_id, time, w)
    }
}

impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for WindowedGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritMetadataPropertiesOps for WindowedGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for WindowedGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> ListOps for WindowedGraph<G> {
    fn node_list(&self) -> NodeList {
        if self.window_is_empty() {
            NodeList::List {
                elems: Index::default(),
            }
        } else {
            self.graph.node_list()
        }
    }

    fn edge_list(&self) -> EdgeList {
        if self.window_is_empty() {
            EdgeList::List {
                elems: Index::default(),
            }
        } else {
            self.graph.edge_list()
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>> InternalNodeFilterOps for WindowedGraph<G> {
    #[inline]
    fn internal_nodes_filtered(&self) -> bool {
        self.window_is_empty() || self.graph.internal_nodes_filtered() || self.window_is_bounding()
    }

    #[inline]
    fn internal_node_list_trusted(&self) -> bool {
        self.window_is_empty()
            || (self.graph.internal_node_list_trusted() && !self.window_is_bounding())
    }

    #[inline]
    fn edge_filter_includes_node_filter(&self) -> bool {
        self.window_is_empty() || self.graph.edge_filter_includes_node_filter()
    }

    #[inline]
    fn edge_layer_filter_includes_node_filter(&self) -> bool {
        self.window_is_empty() || self.graph.edge_layer_filter_includes_node_filter()
    }

    #[inline]
    fn exploded_edge_filter_includes_node_filter(&self) -> bool {
        self.window_is_empty() || self.graph.exploded_edge_filter_includes_node_filter()
    }

    #[inline]
    fn internal_filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        !self.window_is_empty() && self.graph.internal_filter_node(node, layer_ids)
    }
}

impl<'graph, G: GraphViewOps<'graph>> InternalTemporalPropertyViewOps for WindowedGraph<G> {
    fn dtype(&self, id: usize) -> PropType {
        self.graph
            .graph_props_meta()
            .temporal_prop_mapper()
            .get_dtype(id)
            .unwrap()
    }

    fn temporal_value(&self, id: usize) -> Option<Prop> {
        self.graph.temporal_value_at(id, self.end_bound())
    }

    fn temporal_iter(&self, id: usize) -> BoxedLIter<'_, (TimeIndexEntry, Prop)> {
        if self.window_is_empty() {
            return iter::empty().into_dyn_boxed();
        }
        self.graph
            .temporal_prop_iter_window(id, self.start_bound(), self.end_bound())
            .into_dyn_boxed()
    }

    fn temporal_iter_rev(&self, id: usize) -> BoxedLIter<'_, (TimeIndexEntry, Prop)> {
        self.graph
            .temporal_prop_iter_window_rev(id, self.start_bound(), self.end_bound())
            .into_dyn_boxed()
    }

    fn temporal_value_at(&self, id: usize, t: i64) -> Option<Prop> {
        self.graph
            .temporal_prop_last_at_window(
                id,
                TimeIndexEntry::end(t),
                self.start_bound()..self.end_bound(),
            )
            .map(|(_, p)| p)
    }
}

impl<'graph, G: GraphViewOps<'graph>> InternalTemporalPropertiesOps for WindowedGraph<G> {
    fn get_temporal_prop_id(&self, name: &str) -> Option<usize> {
        self.graph
            .get_temporal_prop_id(name)
            .filter(|id| self.has_temporal_prop(*id))
    }

    fn get_temporal_prop_name(&self, id: usize) -> ArcStr {
        self.graph.get_temporal_prop_name(id)
    }

    fn temporal_prop_ids(&self) -> BoxedLIter<'_, usize> {
        Box::new(
            self.graph
                .temporal_prop_ids()
                .filter(|id| self.has_temporal_prop(*id)),
        )
    }
}

impl<'graph, G: GraphViewOps<'graph>> GraphTimeSemanticsOps for WindowedGraph<G> {
    fn node_time_semantics(&self) -> TimeSemantics {
        self.graph
            .node_time_semantics()
            .window(self.start_bound()..self.end_bound())
    }

    fn edge_time_semantics(&self) -> TimeSemantics {
        self.graph
            .edge_time_semantics()
            .window(self.start_bound()..self.end_bound())
    }
    fn view_start(&self) -> Option<i64> {
        self.start
    }

    fn view_end(&self) -> Option<i64> {
        self.end
    }

    #[inline]
    fn earliest_time_global(&self) -> Option<i64> {
        if self.window_is_empty() {
            return None;
        }
        self.graph
            .earliest_time_window(self.start_bound(), self.end_bound())
    }

    #[inline]
    fn latest_time_global(&self) -> Option<i64> {
        if self.window_is_empty() {
            return None;
        }
        self.graph
            .latest_time_window(self.start_bound(), self.end_bound())
    }

    #[inline]
    fn earliest_time_window(&self, start: i64, end: i64) -> Option<i64> {
        self.graph.earliest_time_window(start, end)
    }

    #[inline]
    fn latest_time_window(&self, start: i64, end: i64) -> Option<i64> {
        self.graph.latest_time_window(start, end)
    }

    fn has_temporal_prop(&self, prop_id: usize) -> bool {
        if self.window_is_empty() {
            return false;
        }
        self.graph
            .has_temporal_prop_window(prop_id, self.start_bound()..self.end_bound())
    }

    fn temporal_prop_iter(&self, prop_id: usize) -> BoxedLIter<'_, (TimeIndexEntry, Prop)> {
        if self.window_is_empty() {
            return iter::empty().into_dyn_dboxed();
        }
        self.graph
            .temporal_prop_iter_window(prop_id, self.start_bound(), self.end_bound())
    }

    fn has_temporal_prop_window(&self, prop_id: usize, w: Range<i64>) -> bool {
        self.graph.has_temporal_prop_window(prop_id, w.start..w.end)
    }

    fn temporal_prop_iter_window(
        &self,
        prop_id: usize,
        start: i64,
        end: i64,
    ) -> BoxedLIter<'_, (TimeIndexEntry, Prop)> {
        self.graph.temporal_prop_iter_window(prop_id, start, end)
    }

    fn temporal_prop_iter_window_rev(
        &self,
        prop_id: usize,
        start: i64,
        end: i64,
    ) -> BoxedLIter<'_, (TimeIndexEntry, Prop)> {
        self.graph
            .temporal_prop_iter_window_rev(prop_id, start, end)
    }

    fn temporal_prop_last_at(
        &self,
        prop_id: usize,
        t: TimeIndexEntry,
    ) -> Option<(TimeIndexEntry, Prop)> {
        self.graph
            .temporal_prop_last_at_window(prop_id, t, self.start_bound()..self.end_bound())
    }

    fn temporal_prop_last_at_window(
        &self,
        prop_id: usize,
        t: TimeIndexEntry,
        w: Range<i64>,
    ) -> Option<(TimeIndexEntry, Prop)> {
        self.graph.temporal_prop_last_at_window(prop_id, t, w)
    }
}

// actual filtering is handled upstream for efficiency and to avoid double-checking nested windows
// here we just define the optimisation flags
impl<G: GraphView> InternalEdgeFilterOps for WindowedGraph<G> {
    fn internal_edge_filtered(&self) -> bool {
        self.window_is_bounding() || self.graph.internal_edge_filtered()
    }

    fn internal_edge_list_trusted(&self) -> bool {
        self.window_is_empty()
            || (!self.window_is_bounding() && self.graph.internal_edge_list_trusted())
    }

    fn internal_filter_edge(&self, edge: EdgeEntryRef, layer_ids: &LayerIds) -> bool {
        self.graph.internal_filter_edge(edge, layer_ids)
    }

    fn node_filter_includes_edge_filter(&self) -> bool {
        self.window_is_empty() || self.graph.node_filter_includes_edge_filter()
    }
}
impl<G: GraphView> InternalEdgeLayerFilterOps for WindowedGraph<G> {
    fn internal_edge_layer_filtered(&self) -> bool {
        self.window_is_bounding() || self.graph.internal_edge_layer_filtered()
    }

    fn internal_layer_filter_edge_list_trusted(&self) -> bool {
        self.window_is_empty()
            || (!self.window_is_bounding() && self.graph.internal_layer_filter_edge_list_trusted())
    }

    fn internal_filter_edge_layer(&self, edge: EdgeEntryRef, layer: usize) -> bool {
        self.graph.internal_filter_edge_layer(edge, layer)
    }

    fn node_filter_includes_edge_layer_filter(&self) -> bool {
        self.window_is_empty() || self.graph.node_filter_includes_edge_layer_filter()
    }
}

impl<G: GraphView> InternalExplodedEdgeFilterOps for WindowedGraph<G> {
    fn internal_exploded_edge_filtered(&self) -> bool {
        self.graph.internal_exploded_edge_filtered()
    }

    fn internal_exploded_filter_edge_list_trusted(&self) -> bool {
        self.window_is_empty()
            || (!self.window_is_bounding()
                && self.graph.internal_exploded_filter_edge_list_trusted())
    }

    fn internal_filter_exploded_edge(
        &self,
        eid: ELID,
        t: TimeIndexEntry,
        layer_ids: &LayerIds,
    ) -> bool {
        self.graph.internal_filter_exploded_edge(eid, t, layer_ids)
    }

    fn node_filter_includes_exploded_edge_filter(&self) -> bool {
        self.window_is_empty() || self.graph.node_filter_includes_exploded_edge_filter()
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
impl<'graph, G: GraphViewOps<'graph>> WindowedGraph<G> {
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
    pub(crate) fn new(graph: G, start: Option<i64>, end: Option<i64>) -> Self {
        WindowedGraph { graph, start, end }
    }
}
