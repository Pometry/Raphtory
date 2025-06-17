mod base_time_semantics;
mod event_semantics;
pub mod filtered_edge;
pub mod filtered_node;
mod persistent_semantics;
mod time_semantics;
mod time_semantics_ops;
mod window_time_semantics;

mod history_filter;

pub use history_filter::*;
pub use time_semantics::TimeSemantics;
pub use time_semantics_ops::*;

use crate::db::api::view::{BoxedLDIter, MaterializedGraph};
use enum_dispatch::enum_dispatch;
use raphtory_api::{
    core::{entities::properties::prop::Prop, storage::timeindex::TimeIndexEntry},
    inherit::Base,
};
use std::ops::Range;

/// Methods for defining time windowing semantics for a graph
#[enum_dispatch]
pub trait GraphTimeSemanticsOps {
    fn node_time_semantics(&self) -> TimeSemantics;

    fn edge_time_semantics(&self) -> TimeSemantics;

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
    fn edge_time_semantics(&self) -> TimeSemantics {
        self.graph().edge_time_semantics()
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
}
