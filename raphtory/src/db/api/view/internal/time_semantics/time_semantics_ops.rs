use crate::{
    core::Prop,
    db::api::storage::graph::{edges::edge_ref::EdgeStorageRef, nodes::node_ref::NodeStorageRef},
    prelude::GraphViewOps,
};
use raphtory_api::{
    core::{
        entities::{edges::edge_ref::EdgeRef, LayerIds, EID},
        storage::timeindex::TimeIndexEntry,
    },
    iter::{BoxedLDIter, BoxedLIter},
};
use std::{borrow::Cow, ops::Range};

pub trait NodeTimeSemanticsOps {
    fn node_earliest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> Option<i64>;

    fn node_latest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> Option<i64>;

    fn node_earliest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> Option<i64>;

    fn node_latest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> Option<i64>;

    fn node_history<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> BoxedLIter<'graph, i64>;

    fn node_history_window<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, i64>;

    fn node_updates<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, Vec<(usize, Prop)>)>;

    fn node_updates_window<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, Vec<(usize, Prop)>)>;

    /// Check if the node is part of the graph based on the history
    fn node_valid<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> bool;

    fn node_valid_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> bool;

    fn node_tprop_iter<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        prop_id: usize,
    ) -> BoxedLDIter<'graph, (TimeIndexEntry, Prop)>;

    fn node_tprop_iter_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        w: Range<i64>,
    ) -> BoxedLDIter<'graph, (TimeIndexEntry, Prop)>;

    fn node_tprop_last_at<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        t: TimeIndexEntry,
    ) -> Option<(TimeIndexEntry, Prop)>;

    fn node_tprop_last_at_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        t: TimeIndexEntry,
        w: Range<i64>,
    ) -> Option<(TimeIndexEntry, Prop)>;
}

pub trait EdgeTimeSemanticsOps {
    /// check if edge `e` should be included in window `w`
    fn include_edge_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        edge: EdgeStorageRef,
        view: G,
        w: Range<i64>,
    ) -> bool;

    /// returns the update history of an edge
    ///
    /// # Returns
    ///
    /// An iterator over timestamp and layer pairs
    fn edge_history<'graph, G: GraphViewOps<'graph>>(
        self,
        edge: EdgeStorageRef<'graph>,
        view: G,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize)>;

    /// returns the update history of an edge in a window
    ///
    /// # Returns
    ///
    /// An iterator over timestamp and layer pairs
    fn edge_history_window<'graph, G: GraphViewOps<'graph>>(
        self,
        edge: EdgeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize)>;

    /// The number of exploded edge events for the `edge`
    fn edge_exploded_count<'graph, G: GraphViewOps<'graph>>(
        &self,
        edge: EdgeStorageRef,
        view: G,
    ) -> usize;

    /// The number of exploded edge events for the edge in the window `w`
    fn edge_exploded_count_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        edge: EdgeStorageRef,
        view: G,
        w: Range<i64>,
    ) -> usize;

    /// Exploded edge iterator for edge `e`
    fn edge_exploded<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
    ) -> BoxedLIter<'graph, EdgeRef>;

    /// Explode edge iterator for edge `e` for every layer
    fn edge_layers<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
    ) -> BoxedLIter<'graph, EdgeRef>;

    /// Exploded edge iterator for edge`e` over window `w`
    fn edge_window_exploded<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, EdgeRef>;

    /// Exploded edge iterator for edge `e` over window `w` for every layer
    fn edge_window_layers<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, EdgeRef>;

    /// Get the time of the earliest activity of an edge
    fn edge_earliest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
    ) -> Option<i64>;

    /// Get the time of the earliest activity of an edge `e` in window `w`
    fn edge_earliest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
        w: Range<i64>,
    ) -> Option<i64>;

    /// Get the time of the latest activity of an edge
    fn edge_latest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
    ) -> Option<i64>;

    /// Get the time of the latest activity of an edge `e` in window `w`
    fn edge_latest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
        w: Range<i64>,
    ) -> Option<i64>;

    /// Get the edge deletions for use with materialize
    fn edge_deletion_history<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize)>;

    /// Get the edge deletions for use with materialize restricted to window `w`
    fn edge_deletion_history_window<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize)>;

    /// Check if  edge `e` is currently valid in any layer included in `layer_ids`
    fn edge_is_valid<'graph, G: GraphViewOps<'graph>>(&self, e: EdgeStorageRef, view: G) -> bool;

    /// Check if edge `e` is valid at the end of a window with exclusive end time `t` in all layers included in `layer_ids`
    fn edge_is_valid_at_end<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
        t: i64,
    ) -> bool;

    /// Return the value of an edge temporal property at a given point in time and layer if it exists
    fn temporal_edge_prop_at<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
        id: usize,
        t: TimeIndexEntry,
        layer_id: usize,
    ) -> Option<Prop>;

    /// Return the last value of a temporal edge property at or before a given point in time
    fn temporal_edge_prop_last_at<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
        id: usize,
        t: TimeIndexEntry,
    ) -> Option<Prop>;

    fn temporal_edge_prop_last_at_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
        prop_id: usize,
        t: TimeIndexEntry,
        w: Range<i64>,
    ) -> Option<Prop>;

    /// Return property history of an edge in temporal order
    ///
    /// Items are (timestamp, layer_id, property value)
    fn temporal_edge_prop_hist<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        prop_id: usize,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize, Prop)>;

    /// Return property history for an edge in reverse-temporal order
    ///
    /// Items are (timestamp, layer_id, property value)
    fn temporal_edge_prop_hist_rev<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        prop_id: usize,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize, Prop)>;

    /// Returns an Iterator of tuples containing the values of the temporal property with the given name
    /// for the given edge reference within the specified time window.
    ///
    /// # Arguments
    ///
    /// * `e` - An `EdgeRef` reference to the edge of interest.
    /// * `name` - A `String` containing the name of the temporal property.
    /// * `start` - An `i64` containing the start time of the time window (inclusive).
    /// * `end` - An `i64` containing the end time of the time window (exclusive).
    ///
    /// Items are (timestamp, layer_id, property value)
    fn temporal_edge_prop_hist_window<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        id: usize,
        start: i64,
        end: i64,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize, Prop)>;

    /// Return temporal property history for a window of an edge in reverse-temporal order
    ///
    /// Items are (timestamp, layer_id, property value)
    fn temporal_edge_prop_hist_window_rev<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        id: usize,
        start: i64,
        end: i64,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize, Prop)>;

    /// Get constant edge property
    fn constant_edge_prop<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
        id: usize,
    ) -> Option<Prop>;

    /// Get constant edge property for a window
    ///
    /// Should only return the property for a layer if the edge exists in the window in that layer
    fn constant_edge_prop_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
        id: usize,
        w: Range<i64>,
    ) -> Option<Prop>;
}
