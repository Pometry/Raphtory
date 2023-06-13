use crate::core::edge_ref::EdgeRef;
use crate::core::timeindex::{TimeIndex, TimeIndexOps};
use crate::core::vertex_ref::LocalVertexRef;
use crate::core::Prop;
use crate::db::view_api::internal::core_ops::CoreGraphOps;
use crate::db::view_api::internal::GraphViewInternalOps;
use crate::db::view_api::{BoxedIter, GraphViewOps};
use std::ops::Range;

pub trait TimeSemantics {
    /// Return the earliest time for a vertex
    fn vertex_earliest_time(&self, v: LocalVertexRef) -> Option<i64>;

    /// Return the latest time for a vertex
    fn vertex_latest_time(&self, v: LocalVertexRef) -> Option<i64>;

    /// Returns the default start time for perspectives over the view
    fn view_start(&self) -> Option<i64>;

    /// Returns the default end time for perspectives over the view
    fn view_end(&self) -> Option<i64>;

    /// Returns the timestamp for the earliest activity
    fn earliest_time_global(&self) -> Option<i64>;

    /// Returns the timestamp for the latest activity
    fn latest_time_global(&self) -> Option<i64>;
    /// Returns the timestamp for the earliest activity in the window
    fn earliest_time_window(&self, t_start: i64, t_end: i64) -> Option<i64>;

    /// Returns the timestamp for the latest activity in the window
    fn latest_time_window(&self, t_start: i64, t_end: i64) -> Option<i64>;

    /// Return the earliest time for a vertex in a window
    fn vertex_earliest_time_window(
        &self,
        v: LocalVertexRef,
        t_start: i64,
        t_end: i64,
    ) -> Option<i64>;

    /// Return the latest time for a vertex in a window
    fn vertex_latest_time_window(&self, v: LocalVertexRef, t_start: i64, t_end: i64)
        -> Option<i64>;
    /// check if vertex `v` should be included in window `w`
    fn include_vertex_window(&self, v: LocalVertexRef, w: Range<i64>) -> bool;

    /// check if vertex `e` should be included in window `w`
    fn include_edge_window(&self, e: EdgeRef, w: Range<i64>) -> bool;

    /// Get the history for a vertex `v`.
    fn vertex_history(&self, v: LocalVertexRef) -> BoxedIter<i64>;

    /// Get the history for a vertex `v` in window `w`.
    fn vertex_history_window(&self, v: LocalVertexRef, w: Range<i64>) -> BoxedIter<i64>;

    /// Get the history for an edge `e`. Each history event is a tuple (start, duration).
    fn edge_history(&self, e: EdgeRef) -> BoxedIter<(i64, i64)>;

    /// Get the history for an edge `e` in window `w`. Each history event is a tuple (start, duration).
    fn edge_history_window(&self, e: EdgeRef, w: Range<i64>) -> BoxedIter<(i64, i64)>;

    /// Returns a vector of all temporal values of the vertex property with the given name for the
    /// given vertex
    ///
    /// # Arguments
    ///
    /// * `v` - A reference to the vertex for which to retrieve the temporal property vector.
    /// * `name` - The name of the property to retrieve.
    ///
    /// # Returns
    ///
    /// A vector of tuples representing the temporal values of the property for the given vertex
    /// that fall within the specified time window, where the first element of each tuple is the timestamp
    /// and the second element is the property value.
    fn temporal_vertex_prop_vec(&self, v: LocalVertexRef, name: String) -> Vec<(i64, Prop)>;

    /// Returns a vector of all temporal values of the vertex property with the given name for the given vertex
    /// that fall within the specified time window.
    ///
    /// # Arguments
    ///
    /// * `v` - A reference to the vertex for which to retrieve the temporal property vector.
    /// * `name` - The name of the property to retrieve.
    /// * `t_start` - The start time of the window to consider.
    /// * `t_end` - The end time of the window to consider.
    ///
    /// # Returns
    ///
    /// A vector of tuples representing the temporal values of the property for the given vertex
    /// that fall within the specified time window, where the first element of each tuple is the timestamp
    /// and the second element is the property value.
    fn temporal_vertex_prop_vec_window(
        &self,
        v: LocalVertexRef,
        name: String,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)>;

    /// Returns a vector of tuples containing the values of the temporal property with the given name
    /// for the given edge reference within the specified time window.
    ///
    /// # Arguments
    ///
    /// * `e` - An `EdgeRef` reference to the edge of interest.
    /// * `name` - A `String` containing the name of the temporal property.
    /// * `t_start` - An `i64` containing the start time of the time window (inclusive).
    /// * `t_end` - An `i64` containing the end time of the time window (exclusive).
    ///
    /// # Returns
    ///
    /// * A `Vec` of tuples containing the values of the temporal property with the given name for the given edge
    /// within the specified time window.
    ///
    fn temporal_edge_prop_vec_window(
        &self,
        e: EdgeRef,
        name: String,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)>;

    /// Returns a vector of tuples containing the values of the temporal property with the given name
    /// for the given edge reference.
    ///
    /// # Arguments
    ///
    /// * `e` - An `EdgeRef` reference to the edge of interest.
    /// * `name` - A `String` containing the name of the temporal property.
    ///
    /// # Returns
    ///
    /// * A `Vec` of tuples containing the values of the temporal property with the given name for the given edge.
    fn temporal_edge_prop_vec(&self, e: EdgeRef, name: String) -> Vec<(i64, Prop)>;
}

pub trait InheritTimeSemantics {
    type Internal: TimeSemantics + ?Sized;

    fn graph(&self) -> &Self::Internal;
}

impl<G: InheritTimeSemantics + ?Sized> TimeSemantics for G {
    fn vertex_earliest_time(&self, v: LocalVertexRef) -> Option<i64> {
        self.graph().vertex_earliest_time(v)
    }

    fn vertex_latest_time(&self, v: LocalVertexRef) -> Option<i64> {
        self.graph().vertex_latest_time(v)
    }

    fn view_start(&self) -> Option<i64> {
        self.graph().view_start()
    }

    fn view_end(&self) -> Option<i64> {
        self.graph().view_end()
    }

    fn earliest_time_global(&self) -> Option<i64> {
        self.graph().earliest_time_global()
    }

    fn latest_time_global(&self) -> Option<i64> {
        self.graph().latest_time_global()
    }

    fn earliest_time_window(&self, t_start: i64, t_end: i64) -> Option<i64> {
        self.graph().earliest_time_window(t_start, t_end)
    }

    fn latest_time_window(&self, t_start: i64, t_end: i64) -> Option<i64> {
        self.graph().latest_time_window(t_start, t_end)
    }

    fn vertex_earliest_time_window(
        &self,
        v: LocalVertexRef,
        t_start: i64,
        t_end: i64,
    ) -> Option<i64> {
        self.graph().vertex_earliest_time_window(v, t_start, t_end)
    }

    fn vertex_latest_time_window(
        &self,
        v: LocalVertexRef,
        t_start: i64,
        t_end: i64,
    ) -> Option<i64> {
        self.graph().vertex_latest_time_window(v, t_start, t_end)
    }

    fn include_vertex_window(&self, v: LocalVertexRef, w: Range<i64>) -> bool {
        self.graph().include_vertex_window(v, w)
    }

    fn include_edge_window(&self, e: EdgeRef, w: Range<i64>) -> bool {
        self.graph().include_edge_window(e, w)
    }

    fn vertex_history(&self, v: LocalVertexRef) -> BoxedIter<i64> {
        self.graph().vertex_history(v)
    }

    fn vertex_history_window(&self, v: LocalVertexRef, w: Range<i64>) -> BoxedIter<i64> {
        self.graph().vertex_history_window(v, w)
    }

    fn edge_history(&self, e: EdgeRef) -> BoxedIter<(i64, i64)> {
        self.graph().edge_history(e)
    }

    fn edge_history_window(&self, e: EdgeRef, w: Range<i64>) -> BoxedIter<(i64, i64)> {
        self.graph().edge_history_window(e, w)
    }

    fn temporal_vertex_prop_vec(&self, v: LocalVertexRef, name: String) -> Vec<(i64, Prop)> {
        self.graph().temporal_vertex_prop_vec(v, name)
    }

    fn temporal_vertex_prop_vec_window(
        &self,
        v: LocalVertexRef,
        name: String,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)> {
        self.graph()
            .temporal_vertex_prop_vec_window(v, name, t_start, t_end)
    }

    fn temporal_edge_prop_vec_window(
        &self,
        e: EdgeRef,
        name: String,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)> {
        self.graph()
            .temporal_edge_prop_vec_window(e, name, t_start, t_end)
    }

    fn temporal_edge_prop_vec(&self, e: EdgeRef, name: String) -> Vec<(i64, Prop)> {
        self.graph().temporal_edge_prop_vec(e, name)
    }
}
