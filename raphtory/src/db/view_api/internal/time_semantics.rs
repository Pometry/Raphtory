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

    /// Get the timestamps at which a vertex `v` is active (i.e has an edge addition)
    fn vertex_history(&self, v: LocalVertexRef) -> Vec<i64>;

    /// Get the timestamps at which a vertex `v` is active in window `w` (i.e has an edge addition)
    fn vertex_history_window(&self, v: LocalVertexRef, w: Range<i64>) -> Vec<i64>;

    /// Exploded edge iterator for edge `e`
    fn edge_t(&self, e: EdgeRef) -> BoxedIter<EdgeRef>;

    /// Exploded edge iterator for edge`e` over window `w`
    fn edge_window_t(&self, e: EdgeRef, w: Range<i64>) -> BoxedIter<EdgeRef>;

    /// Get the time of the earliest activity of an edge
    fn edge_earliest_time(&self, e: EdgeRef) -> Option<i64>;

    /// Get the time of the earliest activity of an edge `e` in window `w`
    fn edge_earliest_time_window(&self, e: EdgeRef, w: Range<i64>) -> Option<i64>;

    /// Get the time of the latest activity of an edge
    fn edge_latest_time(&self, e: EdgeRef) -> Option<i64>;

    /// Get the time of the latest activity of an edge `e` in window `w`
    fn edge_latest_time_window(&self, e: EdgeRef, w: Range<i64>) -> Option<i64>;

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
    fn temporal_vertex_prop_vec(&self, v: LocalVertexRef, name: &str) -> Vec<(i64, Prop)>;

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
        name: &str,
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
        name: &str,
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
    fn temporal_edge_prop_vec(&self, e: EdgeRef, name: &str) -> Vec<(i64, Prop)>;
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

    fn vertex_history(&self, v: LocalVertexRef) -> Vec<i64> {
        self.graph().vertex_history(v)
    }

    fn vertex_history_window(&self, v: LocalVertexRef, w: Range<i64>) -> Vec<i64> {
        self.graph().vertex_history_window(v, w)
    }

    fn edge_t(&self, e: EdgeRef) -> BoxedIter<EdgeRef> {
        self.graph().edge_t(e)
    }

    fn edge_window_t(&self, e: EdgeRef, w: Range<i64>) -> BoxedIter<EdgeRef> {
        self.graph().edge_window_t(e, w)
    }

    fn edge_earliest_time(&self, e: EdgeRef) -> Option<i64> {
        self.graph().edge_earliest_time(e)
    }

    fn edge_earliest_time_window(&self, e: EdgeRef, w: Range<i64>) -> Option<i64> {
        self.graph().edge_earliest_time_window(e, w)
    }

    fn edge_latest_time(&self, e: EdgeRef) -> Option<i64> {
        self.graph().edge_latest_time(e)
    }

    fn edge_latest_time_window(&self, e: EdgeRef, w: Range<i64>) -> Option<i64> {
        self.graph().edge_latest_time_window(e, w)
    }

    fn temporal_vertex_prop_vec(&self, v: LocalVertexRef, name: &str) -> Vec<(i64, Prop)> {
        self.graph().temporal_vertex_prop_vec(v, name)
    }

    fn temporal_vertex_prop_vec_window(
        &self,
        v: LocalVertexRef,
        name: &str,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)> {
        self.graph()
            .temporal_vertex_prop_vec_window(v, name, t_start, t_end)
    }

    fn temporal_edge_prop_vec_window(
        &self,
        e: EdgeRef,
        name: &str,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)> {
        self.graph()
            .temporal_edge_prop_vec_window(e, name, t_start, t_end)
    }

    fn temporal_edge_prop_vec(&self, e: EdgeRef, name: &str) -> Vec<(i64, Prop)> {
        self.graph().temporal_edge_prop_vec(e, name)
    }
}
