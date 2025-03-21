mod base_time_semantics;
mod event_semantics;
mod persistent_semantics;
mod time_semantics;
mod time_semantics_ops;
mod window_time_semantics;

pub use time_semantics::TimeSemantics;
pub use time_semantics_ops::*;

use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, LayerIds},
        Prop,
    },
    db::api::{
        storage::graph::edges::edge_ref::EdgeStorageRef,
        view::{internal::Base, BoxedLDIter, BoxedLIter, MaterializedGraph},
    },
};
use enum_dispatch::enum_dispatch;
use raphtory_api::core::{entities::EID, storage::timeindex::TimeIndexEntry};
use std::{borrow::Cow, ops::Range};

/// Methods for defining time windowing semantics for a graph
#[enum_dispatch]
pub trait GraphTimeSemanticsOps {
    fn node_time_semantics(&self) -> TimeSemantics;

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

    /// check if edge `e` should be included in window `w`
    fn include_edge_window(
        &self,
        edge: EdgeStorageRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> bool;

    /// returns the update history of an edge
    ///
    /// # Returns
    ///
    /// An iterator over timestamp and layer pairs
    fn edge_history<'a>(
        &'a self,
        e: EID,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, (TimeIndexEntry, usize)>;

    /// returns the update history of an edge in a window
    ///
    /// # Returns
    ///
    /// An iterator over timestamp and layer pairs
    fn edge_history_window<'a>(
        &'a self,
        e: EID,
        layer_ids: Cow<'a, LayerIds>,
        w: Range<i64>,
    ) -> BoxedLIter<'a, (TimeIndexEntry, usize)>;

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
    fn edge_exploded<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, EdgeRef>;

    /// Explode edge iterator for edge `e` for every layer
    fn edge_layers<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, EdgeRef>;

    /// Exploded edge iterator for edge`e` over window `w`
    fn edge_window_exploded<'a>(
        &'a self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, EdgeRef>;

    /// Exploded edge iterator for edge `e` over window `w` for every layer
    fn edge_window_layers<'a>(
        &'a self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: Cow<'a, LayerIds>,
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
        e: EID,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, (TimeIndexEntry, usize)>;

    /// Get the edge deletions for use with materialize restricted to window `w`
    fn edge_deletion_history_window<'a>(
        &'a self,
        e: EID,
        w: Range<i64>,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, (TimeIndexEntry, usize)>;

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

    /// Returns an Iterator of all temporal values of the graph property with the given id
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
    fn temporal_prop_iter(&self, prop_id: usize) -> BoxedLDIter<(TimeIndexEntry, Prop)>;
    /// Check if graph has temporal property with the given id in the window
    ///
    /// # Arguments
    ///
    /// * `prop_id` - The id of the property to retrieve.
    /// * `w` - time window
    fn has_temporal_prop_window(&self, prop_id: usize, w: Range<i64>) -> bool;

    /// Returns all temporal values of the graph property with the given name
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
    /// Iterator of tuples representing the temporal values of the property
    /// that fall within the specified time window, where the first element of each tuple is the timestamp
    /// and the second element is the property value.
    fn temporal_prop_iter_window(
        &self,
        prop_id: usize,
        start: i64,
        end: i64,
    ) -> BoxedLDIter<(TimeIndexEntry, Prop)>;

    /// Returns the value and update time for the temporal graph property at or before a given timestamp
    fn temporal_prop_last_at(
        &self,
        prop_id: usize,
        t: TimeIndexEntry,
    ) -> Option<(TimeIndexEntry, Prop)>;

    fn temporal_prop_last_at_window(
        &self,
        prop_id: usize,
        t: TimeIndexEntry,
        w: Range<i64>,
    ) -> Option<(TimeIndexEntry, Prop)>;

    /// Return the value of an edge temporal property at a given point in time and layer if it exists
    fn temporal_edge_prop_at(
        &self,
        e: EID,
        id: usize,
        t: TimeIndexEntry,
        layer_id: usize,
    ) -> Option<Prop>;

    /// Return the last value of a temporal edge property at or before a given point in time
    fn temporal_edge_prop_last_at(
        &self,
        e: EID,
        id: usize,
        t: TimeIndexEntry,
        layer_ids: Cow<LayerIds>,
    ) -> Option<Prop>;

    fn temporal_edge_prop_last_at_window(
        &self,
        e: EID,
        prop_id: usize,
        t: TimeIndexEntry,
        layer_ids: Cow<LayerIds>,
        w: Range<i64>,
    ) -> Option<Prop>;

    /// Return property history of an edge in temporal order
    ///
    /// Items are (timestamp, layer_id, property value)
    fn temporal_edge_prop_hist<'a>(
        &'a self,
        e: EID,
        prop_id: usize,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, (TimeIndexEntry, usize, Prop)>;

    /// Return property history for an edge in reverse-temporal order
    ///
    /// Items are (timestamp, layer_id, property value)
    fn temporal_edge_prop_hist_rev<'a>(
        &'a self,
        e: EID,
        prop_id: usize,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, (TimeIndexEntry, usize, Prop)>;

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
    fn temporal_edge_prop_hist_window<'a>(
        &'a self,
        e: EID,
        id: usize,
        start: i64,
        end: i64,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, (TimeIndexEntry, usize, Prop)>;

    /// Return temporal property history for a window of an edge in reverse-temporal order
    ///
    /// Items are (timestamp, layer_id, property value)
    fn temporal_edge_prop_hist_window_rev<'a>(
        &'a self,
        e: EID,
        id: usize,
        start: i64,
        end: i64,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, (TimeIndexEntry, usize, Prop)>;

    /// Get constant edge property
    fn constant_edge_prop(&self, e: EID, id: usize, layer_ids: Cow<LayerIds>) -> Option<Prop>;

    /// Get constant edge property for a window
    ///
    /// Should only return the property for a layer if the edge exists in the window in that layer
    fn constant_edge_prop_window(
        &self,
        e: EID,
        id: usize,
        layer_ids: Cow<LayerIds>,
        w: Range<i64>,
    ) -> Option<Prop>;
}

pub trait InheritTimeSemantics: Base {}

impl<G: InheritTimeSemantics> DelegateTimeSemantics for G
where
    <G as Base>::Base: GraphTimeSemanticsOps,
{
    type Internal = <G as Base>::Base;

    fn graph(&self) -> &Self::Internal {
        self.base()
    }
}

pub trait DelegateTimeSemantics {
    type Internal: GraphTimeSemanticsOps + ?Sized;

    fn graph(&self) -> &Self::Internal;
}

impl<G: DelegateTimeSemantics + ?Sized> GraphTimeSemanticsOps for G {
    #[inline]
    fn node_time_semantics(&self) -> TimeSemantics {
        self.graph().node_time_semantics()
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
    fn include_edge_window(
        &self,
        edge: EdgeStorageRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> bool {
        self.graph().include_edge_window(edge, w, layer_ids)
    }

    #[inline]
    fn edge_history<'a>(
        &'a self,
        e: EID,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, (TimeIndexEntry, usize)> {
        self.graph().edge_history(e, layer_ids)
    }

    #[inline]
    fn edge_history_window<'a>(
        &'a self,
        e: EID,
        layer_ids: Cow<'a, LayerIds>,
        w: Range<i64>,
    ) -> BoxedLIter<'a, (TimeIndexEntry, usize)> {
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
    fn edge_exploded<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, EdgeRef> {
        self.graph().edge_exploded(e, layer_ids)
    }

    #[inline]
    fn edge_layers<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, EdgeRef> {
        self.graph().edge_layers(e, layer_ids)
    }

    #[inline]
    fn edge_window_exploded<'a>(
        &'a self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, EdgeRef> {
        self.graph().edge_window_exploded(e, w, layer_ids)
    }

    #[inline]
    fn edge_window_layers<'a>(
        &'a self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: Cow<'a, LayerIds>,
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
        e: EID,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, (TimeIndexEntry, usize)> {
        self.graph().edge_deletion_history(e, layer_ids)
    }

    #[inline]
    fn edge_deletion_history_window<'a>(
        &'a self,
        e: EID,
        w: Range<i64>,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, (TimeIndexEntry, usize)> {
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
    fn temporal_prop_iter(&self, prop_id: usize) -> BoxedLDIter<(TimeIndexEntry, Prop)> {
        self.graph().temporal_prop_iter(prop_id)
    }

    #[inline]
    fn has_temporal_prop_window(&self, prop_id: usize, w: Range<i64>) -> bool {
        self.graph().has_temporal_prop_window(prop_id, w)
    }

    #[inline]
    fn temporal_prop_iter_window(
        &self,
        prop_id: usize,
        start: i64,
        end: i64,
    ) -> BoxedLDIter<(TimeIndexEntry, Prop)> {
        self.graph().temporal_prop_iter_window(prop_id, start, end)
    }

    #[inline]
    fn temporal_prop_last_at(
        &self,
        prop_id: usize,
        t: TimeIndexEntry,
    ) -> Option<(TimeIndexEntry, Prop)> {
        self.graph().temporal_prop_last_at(prop_id, t)
    }

    #[inline]
    fn temporal_prop_last_at_window(
        &self,
        prop_id: usize,
        t: TimeIndexEntry,
        w: Range<i64>,
    ) -> Option<(TimeIndexEntry, Prop)> {
        self.graph().temporal_prop_last_at_window(prop_id, t, w)
    }

    #[inline]
    fn temporal_edge_prop_at(
        &self,
        e: EID,
        id: usize,
        t: TimeIndexEntry,
        layer_id: usize,
    ) -> Option<Prop> {
        self.graph().temporal_edge_prop_at(e, id, t, layer_id)
    }

    #[inline]
    fn temporal_edge_prop_last_at(
        &self,
        e: EID,
        id: usize,
        t: TimeIndexEntry,
        layer_ids: Cow<LayerIds>,
    ) -> Option<Prop> {
        self.graph().temporal_edge_prop_last_at(e, id, t, layer_ids)
    }

    #[inline]
    fn temporal_edge_prop_last_at_window(
        &self,
        e: EID,
        prop_id: usize,
        t: TimeIndexEntry,
        layer_ids: Cow<LayerIds>,
        w: Range<i64>,
    ) -> Option<Prop> {
        self.graph()
            .temporal_edge_prop_last_at_window(e, prop_id, t, layer_ids, w)
    }

    #[inline]
    fn temporal_edge_prop_hist<'a>(
        &'a self,
        e: EID,
        prop_id: usize,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, (TimeIndexEntry, usize, Prop)> {
        self.graph().temporal_edge_prop_hist(e, prop_id, layer_ids)
    }

    #[inline]
    fn temporal_edge_prop_hist_rev<'a>(
        &'a self,
        e: EID,
        id: usize,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, (TimeIndexEntry, usize, Prop)> {
        self.graph().temporal_edge_prop_hist_rev(e, id, layer_ids)
    }

    #[inline]
    fn temporal_edge_prop_hist_window<'a>(
        &'a self,
        e: EID,
        prop_id: usize,
        start: i64,
        end: i64,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, (TimeIndexEntry, usize, Prop)> {
        self.graph()
            .temporal_edge_prop_hist_window(e, prop_id, start, end, layer_ids)
    }

    #[inline]
    fn temporal_edge_prop_hist_window_rev<'a>(
        &'a self,
        e: EID,
        id: usize,
        start: i64,
        end: i64,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, (TimeIndexEntry, usize, Prop)> {
        self.graph()
            .temporal_edge_prop_hist_window_rev(e, id, start, end, layer_ids)
    }

    #[inline]
    fn constant_edge_prop(&self, e: EID, id: usize, layer_ids: Cow<LayerIds>) -> Option<Prop> {
        self.graph().constant_edge_prop(e, id, layer_ids)
    }

    #[inline]
    fn constant_edge_prop_window(
        &self,
        e: EID,
        id: usize,
        layer_ids: Cow<LayerIds>,
        w: Range<i64>,
    ) -> Option<Prop> {
        self.graph().constant_edge_prop_window(e, id, layer_ids, w)
    }
}
