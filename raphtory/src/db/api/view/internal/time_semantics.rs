use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, LayerIds, VID},
        Prop,
    },
    db::api::{
        storage::graph::{edges::edge_ref::EdgeStorageRef, nodes::node_ref::NodeStorageRef},
        view::{internal::Base, BoxedLIter, MaterializedGraph},
    },
};
use enum_dispatch::enum_dispatch;
use raphtory_api::core::{entities::EID, storage::timeindex::TimeIndexEntry};
use std::ops::Range;

/// Methods for defining time windowing semantics for a graph
#[enum_dispatch]
pub trait TimeSemantics {
    /// Return the earliest time for a node
    fn node_earliest_time(&self, v: VID) -> Option<i64>;

    /// Return the latest time for a node
    fn node_latest_time(&self, v: VID) -> Option<i64>;

    /// Returns the start of the current view or `None` if unbounded
    fn view_start(&self) -> Option<i64>;

    /// Returns the end of the current view or `None` if unbounded
    fn view_end(&self) -> Option<i64>;

    /// Returns the timestamp for the earliest activity
    fn earliest_time_global(&self) -> Option<i64>;
    /// Returns the timestamp for the latest activity
    fn latest_time_global(&self) -> Option<i64>;
    /// Returns the timestamp for the earliest activity in the window
    fn earliest_time_window(&self, start: i64, end: i64) -> Option<i64>;

    /// Returns the timestamp for the latest activity in the window
    fn latest_time_window(&self, start: i64, end: i64) -> Option<i64>;

    /// Return the earliest time for a node in a window
    fn node_earliest_time_window(&self, v: VID, start: i64, end: i64) -> Option<i64>;

    /// Return the latest time for a node in a window
    fn node_latest_time_window(&self, v: VID, start: i64, end: i64) -> Option<i64>;
    /// check if node `v` should be included in window `w`
    fn include_node_window(&self, v: NodeStorageRef, w: Range<i64>, layer_ids: &LayerIds) -> bool;

    /// check if edge `e` should be included in window `w`
    fn include_edge_window(
        &self,
        edge: EdgeStorageRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> bool;

    /// Get the timestamps at which a node `v` is active (i.e has an edge addition, or a property change)
    fn node_history(&self, v: VID) -> BoxedLIter<TimeIndexEntry>;

    /// Get the timestamps at which a node `v` is active in window `w` (i.e has an edge addition)
    fn node_history_window(&self, v: VID, w: Range<i64>) -> BoxedLIter<TimeIndexEntry>;

    /// Get the timestamps associated with properties or node events only (Excluding edge events)
    fn node_property_history(&self, v: VID, w: Option<Range<i64>>) -> BoxedLIter<TimeIndexEntry>;

    /// Get the timestamps associated with edge events only (Excluding node or property events)
    fn node_edge_history(&self, v: VID, w: Option<Range<i64>>)
        -> BoxedLIter<(TimeIndexEntry, EID)>;

    fn node_history_rows(
        &self,
        v: VID,
        w: Option<Range<i64>>,
    ) -> BoxedLIter<(TimeIndexEntry, Vec<Option<Prop>>)>;

    fn edge_history<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: &'a LayerIds,
    ) -> BoxedLIter<'a, TimeIndexEntry>;

    fn edge_history_window<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: &'a LayerIds,
        w: Range<i64>,
    ) -> BoxedLIter<'a, TimeIndexEntry>;

    /// The number of exploded edge events for the `edge`
    fn edge_exploded_count(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> usize;

    /// The number of exploded edge events for the edge in the window `w`
    fn edge_exploded_count_window(
        &self,
        edge: EdgeStorageRef,
        layer_ids: &LayerIds,
        w: Range<i64>,
    ) -> usize;

    /// Exploded edge iterator for edge `e`
    fn edge_exploded<'a>(&'a self, e: EdgeRef, layer_ids: &'a LayerIds) -> BoxedLIter<'a, EdgeRef>;

    /// Explode edge iterator for edge `e` for every layer
    fn edge_layers<'a>(&'a self, e: EdgeRef, layer_ids: &'a LayerIds) -> BoxedLIter<'a, EdgeRef>;

    /// Exploded edge iterator for edge`e` over window `w`
    fn edge_window_exploded<'a>(
        &'a self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &'a LayerIds,
    ) -> BoxedLIter<'a, EdgeRef>;

    /// Exploded edge iterator for edge `e` over window `w` for every layer
    fn edge_window_layers<'a>(
        &'a self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &'a LayerIds,
    ) -> BoxedLIter<'a, EdgeRef>;

    /// Get the time of the earliest activity of an edge
    fn edge_earliest_time(&self, e: EdgeRef, layer_ids: &LayerIds) -> Option<i64>;

    /// Get the time of the earliest activity of an edge `e` in window `w`
    fn edge_earliest_time_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> Option<i64>;

    /// Get the time of the latest activity of an edge
    fn edge_latest_time(&self, e: EdgeRef, layer_ids: &LayerIds) -> Option<i64>;

    /// Get the time of the latest activity of an edge `e` in window `w`
    fn edge_latest_time_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> Option<i64>;

    /// Get the edge deletions for use with materialize
    fn edge_deletion_history<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: &'a LayerIds,
    ) -> BoxedLIter<'a, TimeIndexEntry>;

    /// Get the edge deletions for use with materialize restricted to window `w`
    fn edge_deletion_history_window<'a>(
        &'a self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &'a LayerIds,
    ) -> BoxedLIter<'a, TimeIndexEntry>;

    /// Check if  edge `e` is currently valid in any layer included in `layer_ids`
    fn edge_is_valid(&self, e: EdgeRef, layer_ids: &LayerIds) -> bool;

    /// Check if edge `e` is valid at the end of a window with exclusive end time `t` in all layers included in `layer_ids`
    fn edge_is_valid_at_end(&self, e: EdgeRef, layer_ids: &LayerIds, t: i64) -> bool;

    /// Check if graph has temporal property with the given id
    ///
    /// # Arguments
    ///
    /// * `prop_id` - The id of the property to retrieve.
    fn has_temporal_prop(&self, prop_id: usize) -> bool;

    /// Returns a vector of all temporal values of the graph property with the given id
    ///
    /// # Arguments
    ///
    /// * `prop_id` - The id of the property to retrieve.
    ///
    /// Returns:
    ///
    /// A vector of tuples representing the temporal values of the property
    /// that fall within the specified time window, where the first element of each tuple is the timestamp
    /// and the second element is the property value.
    fn temporal_prop_vec(&self, prop_id: usize) -> Vec<(i64, Prop)>;

    fn temporal_prop_iter(&self, prop_id: usize) -> BoxedLIter<(i64, Prop)> {
        Box::new(self.temporal_prop_vec(prop_id).into_iter())
    }

    /// Check if graph has temporal property with the given id in the window
    ///
    /// # Arguments
    ///
    /// * `prop_id` - The id of the property to retrieve.
    /// * `w` - time window
    fn has_temporal_prop_window(&self, prop_id: usize, w: Range<i64>) -> bool;

    /// Returns a vector of all temporal values of the graph property with the given name
    /// that fall within the specified time window.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property to retrieve.
    /// * `start` - The start time of the window to consider.
    /// * `end` - The end time of the window to consider.
    ///
    /// Returns:
    ///
    /// A vector of tuples representing the temporal values of the property
    /// that fall within the specified time window, where the first element of each tuple is the timestamp
    /// and the second element is the property value.
    fn temporal_prop_vec_window(&self, prop_id: usize, start: i64, end: i64) -> Vec<(i64, Prop)>;

    fn temporal_prop_iter_window(
        &self,
        prop_id: usize,
        start: i64,
        end: i64,
    ) -> BoxedLIter<(i64, Prop)> {
        Box::new(
            self.temporal_prop_vec_window(prop_id, start, end)
                .into_iter(),
        )
    }

    /// Check if node has temporal property with the given id
    ///
    /// # Arguments
    ///
    /// * `v` - The id of the node
    /// * `prop_id` - The id of the property to retrieve.
    fn has_temporal_node_prop(&self, v: VID, prop_id: usize) -> bool;

    /// Returns a vector of all temporal values of the node property with the given name for the
    /// given node
    ///
    /// # Arguments
    ///
    /// * `v` - A reference to the node for which to retrieve the temporal property vector.
    /// * `name` - The name of the property to retrieve.
    ///
    /// Returns:
    ///
    /// A vector of tuples representing the temporal values of the property for the given node
    /// that fall within the specified time window, where the first element of each tuple is the timestamp
    /// and the second element is the property value.
    fn temporal_node_prop_hist(&self, v: VID, id: usize) -> BoxedLIter<(TimeIndexEntry, Prop)>;

    /// Check if node has temporal property with the given id in the window
    ///
    /// # Arguments
    ///
    /// * `v` - the id of the node
    /// * `prop_id` - The id of the property to retrieve.
    /// * `w` - time window
    fn has_temporal_node_prop_window(&self, v: VID, prop_id: usize, w: Range<i64>) -> bool;

    /// Returns a vector of all temporal values of the node property with the given name for the given node
    /// that fall within the specified time window.
    ///
    /// # Arguments
    ///
    /// * `v` - A reference to the node for which to retrieve the temporal property vector.
    /// * `name` - The name of the property to retrieve.
    /// * `start` - The start time of the window to consider.
    /// * `end` - The end time of the window to consider.
    ///
    /// Returns:
    ///
    /// A vector of tuples representing the temporal values of the property for the given node
    /// that fall within the specified time window, where the first element of each tuple is the timestamp
    /// and the second element is the property value.
    fn temporal_node_prop_hist_window(
        &self,
        v: VID,
        id: usize,
        start: i64,
        end: i64,
    ) -> BoxedLIter<(TimeIndexEntry, Prop)>;

    /// Check if edge has temporal property with the given id in the window
    ///
    /// # Arguments
    ///
    /// * `e` - the id of the edge
    /// * `prop_id` - The id of the property to retrieve.
    /// * `w` - time window
    fn has_temporal_edge_prop_window(
        &self,
        e: EdgeRef,
        prop_id: usize,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> bool;

    /// Returns a vector of tuples containing the values of the temporal property with the given name
    /// for the given edge reference within the specified time window.
    ///
    /// # Arguments
    ///
    /// * `e` - An `EdgeRef` reference to the edge of interest.
    /// * `name` - A `String` containing the name of the temporal property.
    /// * `start` - An `i64` containing the start time of the time window (inclusive).
    /// * `end` - An `i64` containing the end time of the time window (exclusive).
    ///
    /// Returns:
    ///
    /// * A `Vec` of tuples containing the values of the temporal property with the given name for the given edge
    /// within the specified time window.
    ///
    fn temporal_edge_prop_hist_window<'a>(
        &'a self,
        e: EdgeRef,
        id: usize,
        start: i64,
        end: i64,
        layer_ids: &LayerIds,
    ) -> BoxedLIter<'a, (TimeIndexEntry, Prop)>;

    /// Return the value of an edge temporal property at a given point in time if it exists
    fn temporal_edge_prop_at(
        &self,
        e: EdgeRef,
        id: usize,
        t: TimeIndexEntry,
        layer_ids: &LayerIds,
    ) -> Option<Prop>;

    /// Check if edge has temporal property with the given id
    ///
    /// # Arguments
    ///
    /// * `e` - The id of the edge
    /// * `prop_id` - The id of the property to retrieve.
    fn has_temporal_edge_prop(&self, e: EdgeRef, prop_id: usize, layer_ids: &LayerIds) -> bool;

    /// Returns a vector of tuples containing the values of the temporal property with the given name
    /// for the given edge reference.
    ///
    /// # Arguments
    ///
    /// * `e` - An `EdgeRef` reference to the edge of interest.
    /// * `name` - A `String` containing the name of the temporal property.
    ///
    /// Returns:
    ///
    /// * A `Vec` of tuples containing the values of the temporal property with the given name for the given edge.
    fn temporal_edge_prop_hist<'a>(
        &'a self,
        e: EdgeRef,
        id: usize,
        layer_ids: &LayerIds,
    ) -> BoxedLIter<'a, (TimeIndexEntry, Prop)>;
}

pub trait InheritTimeSemantics: Base {}

impl<G: InheritTimeSemantics> DelegateTimeSemantics for G
where
    <G as Base>::Base: TimeSemantics,
{
    type Internal = <G as Base>::Base;

    fn graph(&self) -> &Self::Internal {
        self.base()
    }
}

pub trait DelegateTimeSemantics {
    type Internal: TimeSemantics + ?Sized;

    fn graph(&self) -> &Self::Internal;
}

impl<G: DelegateTimeSemantics + ?Sized> TimeSemantics for G {
    #[inline]
    fn node_earliest_time(&self, v: VID) -> Option<i64> {
        self.graph().node_earliest_time(v)
    }

    #[inline]
    fn node_latest_time(&self, v: VID) -> Option<i64> {
        self.graph().node_latest_time(v)
    }

    #[inline]
    fn view_start(&self) -> Option<i64> {
        self.graph().view_start()
    }
    #[inline]
    fn view_end(&self) -> Option<i64> {
        self.graph().view_end()
    }
    #[inline]
    fn earliest_time_global(&self) -> Option<i64> {
        self.graph().earliest_time_global()
    }
    #[inline]
    fn latest_time_global(&self) -> Option<i64> {
        self.graph().latest_time_global()
    }
    #[inline]
    fn earliest_time_window(&self, start: i64, end: i64) -> Option<i64> {
        self.graph().earliest_time_window(start, end)
    }
    #[inline]
    fn latest_time_window(&self, start: i64, end: i64) -> Option<i64> {
        self.graph().latest_time_window(start, end)
    }
    #[inline]
    fn node_earliest_time_window(&self, v: VID, start: i64, end: i64) -> Option<i64> {
        self.graph().node_earliest_time_window(v, start, end)
    }
    #[inline]
    fn node_latest_time_window(&self, v: VID, start: i64, end: i64) -> Option<i64> {
        self.graph().node_latest_time_window(v, start, end)
    }
    #[inline]
    fn include_node_window(
        &self,
        node: NodeStorageRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> bool {
        self.graph().include_node_window(node, w, layer_ids)
    }

    #[inline]
    fn include_edge_window(
        &self,
        edge: EdgeStorageRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> bool {
        self.graph().include_edge_window(edge, w, layer_ids)
    }

    #[inline]
    fn node_history(&self, v: VID) -> BoxedLIter<'_, TimeIndexEntry> {
        self.graph().node_history(v)
    }

    #[inline]
    fn node_history_window(&self, v: VID, w: Range<i64>) -> BoxedLIter<'_, TimeIndexEntry> {
        self.graph().node_history_window(v, w)
    }

    #[inline]
    fn edge_history<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: &'a LayerIds,
    ) -> BoxedLIter<'a, TimeIndexEntry> {
        self.graph().edge_history(e, layer_ids)
    }

    #[inline]
    fn edge_history_window<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: &'a LayerIds,
        w: Range<i64>,
    ) -> BoxedLIter<'a, TimeIndexEntry> {
        self.graph().edge_history_window(e, layer_ids, w)
    }

    #[inline]
    fn edge_exploded_count(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> usize {
        self.graph().edge_exploded_count(edge, layer_ids)
    }

    #[inline]
    fn edge_exploded_count_window(
        &self,
        edge: EdgeStorageRef,
        layer_ids: &LayerIds,
        w: Range<i64>,
    ) -> usize {
        self.graph().edge_exploded_count_window(edge, layer_ids, w)
    }

    #[inline]
    fn edge_exploded<'a>(&'a self, e: EdgeRef, layer_ids: &'a LayerIds) -> BoxedLIter<'a, EdgeRef> {
        self.graph().edge_exploded(e, layer_ids)
    }

    #[inline]
    fn edge_layers<'a>(&'a self, e: EdgeRef, layer_ids: &'a LayerIds) -> BoxedLIter<'a, EdgeRef> {
        self.graph().edge_layers(e, &layer_ids)
    }

    #[inline]
    fn edge_window_exploded<'a>(
        &'a self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &'a LayerIds,
    ) -> BoxedLIter<'a, EdgeRef> {
        self.graph().edge_window_exploded(e, w, layer_ids)
    }

    #[inline]
    fn edge_window_layers<'a>(
        &'a self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &'a LayerIds,
    ) -> BoxedLIter<'a, EdgeRef> {
        self.graph().edge_window_layers(e, w, layer_ids)
    }

    #[inline]
    fn edge_earliest_time(&self, e: EdgeRef, layer_ids: &LayerIds) -> Option<i64> {
        self.graph().edge_earliest_time(e, layer_ids)
    }

    #[inline]
    fn edge_earliest_time_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> Option<i64> {
        self.graph().edge_earliest_time_window(e, w, layer_ids)
    }

    #[inline]
    fn edge_latest_time(&self, e: EdgeRef, layer_ids: &LayerIds) -> Option<i64> {
        self.graph().edge_latest_time(e, layer_ids)
    }

    #[inline]
    fn edge_latest_time_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> Option<i64> {
        self.graph().edge_latest_time_window(e, w, layer_ids)
    }

    #[inline]
    fn edge_deletion_history<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: &'a LayerIds,
    ) -> BoxedLIter<'a, TimeIndexEntry> {
        self.graph().edge_deletion_history(e, layer_ids)
    }

    #[inline]
    fn edge_deletion_history_window<'a>(
        &'a self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &'a LayerIds,
    ) -> BoxedLIter<'a, TimeIndexEntry> {
        self.graph().edge_deletion_history_window(e, w, layer_ids)
    }

    #[inline]
    fn edge_is_valid(&self, e: EdgeRef, layer_ids: &LayerIds) -> bool {
        self.graph().edge_is_valid(e, layer_ids)
    }

    #[inline]
    fn edge_is_valid_at_end(&self, e: EdgeRef, layer_ids: &LayerIds, t: i64) -> bool {
        self.graph().edge_is_valid_at_end(e, layer_ids, t)
    }

    #[inline]
    fn has_temporal_prop(&self, prop_id: usize) -> bool {
        self.graph().has_temporal_prop(prop_id)
    }

    #[inline]
    fn temporal_prop_vec(&self, prop_id: usize) -> Vec<(i64, Prop)> {
        self.graph().temporal_prop_vec(prop_id)
    }

    #[inline]
    fn has_temporal_prop_window(&self, prop_id: usize, w: Range<i64>) -> bool {
        self.graph().has_temporal_prop_window(prop_id, w)
    }

    #[inline]
    fn temporal_prop_vec_window(&self, prop_id: usize, start: i64, end: i64) -> Vec<(i64, Prop)> {
        self.graph().temporal_prop_vec_window(prop_id, start, end)
    }

    #[inline]
    fn has_temporal_node_prop(&self, v: VID, prop_id: usize) -> bool {
        self.graph().has_temporal_node_prop(v, prop_id)
    }

    #[inline]
    fn temporal_node_prop_hist(
        &self,
        v: VID,
        prop_id: usize,
    ) -> BoxedLIter<(TimeIndexEntry, Prop)> {
        self.graph().temporal_node_prop_hist(v, prop_id)
    }

    #[inline]
    fn has_temporal_node_prop_window(&self, v: VID, prop_id: usize, w: Range<i64>) -> bool {
        self.graph().has_temporal_node_prop_window(v, prop_id, w)
    }

    #[inline]
    fn temporal_node_prop_hist_window(
        &self,
        v: VID,
        prop_id: usize,
        start: i64,
        end: i64,
    ) -> BoxedLIter<(TimeIndexEntry, Prop)> {
        self.graph()
            .temporal_node_prop_hist_window(v, prop_id, start, end)
    }

    fn has_temporal_edge_prop_window(
        &self,
        e: EdgeRef,
        prop_id: usize,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> bool {
        self.graph()
            .has_temporal_edge_prop_window(e, prop_id, w, layer_ids)
    }

    #[inline]
    fn temporal_edge_prop_hist_window<'a>(
        &'a self,
        e: EdgeRef,
        prop_id: usize,
        start: i64,
        end: i64,
        layer_ids: &LayerIds,
    ) -> BoxedLIter<'a, (TimeIndexEntry, Prop)> {
        self.graph()
            .temporal_edge_prop_hist_window(e, prop_id, start, end, layer_ids)
    }

    #[inline]
    fn temporal_edge_prop_at(
        &self,
        e: EdgeRef,
        id: usize,
        t: TimeIndexEntry,
        layer_ids: &LayerIds,
    ) -> Option<Prop> {
        self.graph().temporal_edge_prop_at(e, id, t, layer_ids)
    }

    #[inline]
    fn has_temporal_edge_prop(&self, e: EdgeRef, prop_id: usize, layer_ids: &LayerIds) -> bool {
        self.graph().has_temporal_edge_prop(e, prop_id, layer_ids)
    }

    #[inline]
    fn temporal_edge_prop_hist<'a>(
        &'a self,
        e: EdgeRef,
        prop_id: usize,
        layer_ids: &LayerIds,
    ) -> BoxedLIter<'a, (TimeIndexEntry, Prop)> {
        self.graph().temporal_edge_prop_hist(e, prop_id, layer_ids)
    }

    fn node_property_history(&self, v: VID, w: Option<Range<i64>>) -> BoxedLIter<TimeIndexEntry> {
        self.graph().node_property_history(v, w)
    }

    fn node_edge_history(
        &self,
        v: VID,
        w: Option<Range<i64>>,
    ) -> BoxedLIter<(TimeIndexEntry, EID)> {
        self.graph().node_edge_history(v, w)
    }

    fn node_history_rows(
        &self,
        v: VID,
        w: Option<Range<i64>>,
    ) -> BoxedLIter<(TimeIndexEntry, Vec<Option<Prop>>)> {
        self.graph().node_history_rows(v, w)
    }
}
