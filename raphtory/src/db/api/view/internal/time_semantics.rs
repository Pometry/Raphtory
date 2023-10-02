use crate::{
    core::{
        entities::{
            edges::{edge_ref::EdgeRef, edge_store::EdgeStore},
            LayerIds, VID,
        },
        storage::timeindex::TimeIndexOps,
        Prop,
    },
    db::api::view::{
        internal::{materialize::MaterializedGraph, Base, CoreGraphOps, EdgeFilter, GraphOps},
        BoxedIter,
    },
};
use enum_dispatch::enum_dispatch;
use std::ops::Range;

/// Methods for defining time windowing semantics for a graph
#[enum_dispatch]
pub trait TimeSemantics: GraphOps + CoreGraphOps {
    /// Return the earliest time for a vertex
    fn vertex_earliest_time(&self, v: VID) -> Option<i64> {
        self.vertex_additions(v).first_t()
    }

    /// Return the latest time for a vertex
    fn vertex_latest_time(&self, v: VID) -> Option<i64> {
        self.vertex_additions(v).last_t()
    }

    /// Returns the default start time for perspectives over the view
    #[inline]
    fn view_start(&self) -> Option<i64> {
        self.earliest_time_global()
    }

    /// Returns the default end time for perspectives over the view
    #[inline]
    fn view_end(&self) -> Option<i64> {
        self.latest_time_global().map(|v| v.saturating_add(1))
    }

    /// Returns the timestamp for the earliest activity
    fn earliest_time_global(&self) -> Option<i64>;
    /// Returns the timestamp for the latest activity
    fn latest_time_global(&self) -> Option<i64>;
    /// Returns the timestamp for the earliest activity in the window
    fn earliest_time_window(&self, start: i64, end: i64) -> Option<i64>;

    /// Returns the timestamp for the latest activity in the window
    fn latest_time_window(&self, start: i64, end: i64) -> Option<i64>;

    /// Return the earliest time for a vertex in a window
    fn vertex_earliest_time_window(&self, v: VID, start: i64, end: i64) -> Option<i64> {
        self.vertex_additions(v).range(start..end).first_t()
    }

    /// Return the latest time for a vertex in a window
    fn vertex_latest_time_window(&self, v: VID, start: i64, end: i64) -> Option<i64> {
        self.vertex_additions(v).range(start..end).last_t()
    }
    /// check if vertex `v` should be included in window `w`
    fn include_vertex_window(
        &self,
        v: VID,
        w: Range<i64>,
        layer_ids: &LayerIds,
        edge_filter: Option<&EdgeFilter>,
    ) -> bool;

    /// check if edge `e` should be included in window `w`
    fn include_edge_window(&self, e: &EdgeStore, w: Range<i64>, layer_ids: &LayerIds) -> bool;

    /// Get the timestamps at which a vertex `v` is active (i.e has an edge addition)
    fn vertex_history(&self, v: VID) -> Vec<i64> {
        self.vertex_additions(v).iter_t().copied().collect()
    }

    /// Get the timestamps at which a vertex `v` is active in window `w` (i.e has an edge addition)
    fn vertex_history_window(&self, v: VID, w: Range<i64>) -> Vec<i64> {
        self.vertex_additions(v)
            .range(w)
            .iter_t()
            .copied()
            .collect()
    }

    /// Exploded edge iterator for edge `e`
    fn edge_exploded(&self, e: EdgeRef, layer_ids: LayerIds) -> BoxedIter<EdgeRef>;

    /// Explode edge iterator for edge `e` for every layer
    fn edge_layers(&self, e: EdgeRef, layer_ids: LayerIds) -> BoxedIter<EdgeRef>;

    /// Exploded edge iterator for edge`e` over window `w`
    fn edge_window_exploded(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> BoxedIter<EdgeRef>;

    /// Exploded edge iterator for edge `e` over window `w` for every layer
    fn edge_window_layers(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> BoxedIter<EdgeRef>;

    /// Get the time of the earliest activity of an edge
    fn edge_earliest_time(&self, e: EdgeRef, layer_ids: LayerIds) -> Option<i64>;

    /// Get the time of the earliest activity of an edge `e` in window `w`
    fn edge_earliest_time_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> Option<i64>;

    /// Get the time of the latest activity of an edge
    fn edge_latest_time(&self, e: EdgeRef, layer_ids: LayerIds) -> Option<i64>;

    /// Get the time of the latest activity of an edge `e` in window `w`
    fn edge_latest_time_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> Option<i64>;

    /// Get the edge deletions for use with materialize
    fn edge_deletion_history(&self, e: EdgeRef, layer_ids: LayerIds) -> Vec<i64>;

    /// Get the edge deletions for use with materialize restricted to window `w`
    fn edge_deletion_history_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> Vec<i64>;

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
    fn temporal_prop_vec_window(
        &self,
        prop_id: usize,
        start: i64,
        end: i64,
    ) -> Vec<(i64, Prop)>;

    /// Check if vertex has temporal property with the given id
    ///
    /// # Arguments
    ///
    /// * `v` - The id of the vertex
    /// * `prop_id` - The id of the property to retrieve.
    fn has_temporal_vertex_prop(&self, v: VID, prop_id: usize) -> bool;

    /// Returns a vector of all temporal values of the vertex property with the given name for the
    /// given vertex
    ///
    /// # Arguments
    ///
    /// * `v` - A reference to the vertex for which to retrieve the temporal property vector.
    /// * `name` - The name of the property to retrieve.
    ///
    /// Returns:
    ///
    /// A vector of tuples representing the temporal values of the property for the given vertex
    /// that fall within the specified time window, where the first element of each tuple is the timestamp
    /// and the second element is the property value.
    fn temporal_vertex_prop_vec(&self, v: VID, id: usize) -> Vec<(i64, Prop)>;

    /// Check if vertex has temporal property with the given id in the window
    ///
    /// # Arguments
    ///
    /// * `v` - the id of the vertex
    /// * `prop_id` - The id of the property to retrieve.
    /// * `w` - time window
    fn has_temporal_vertex_prop_window(&self, v: VID, prop_id: usize, w: Range<i64>) -> bool;

    /// Returns a vector of all temporal values of the vertex property with the given name for the given vertex
    /// that fall within the specified time window.
    ///
    /// # Arguments
    ///
    /// * `v` - A reference to the vertex for which to retrieve the temporal property vector.
    /// * `name` - The name of the property to retrieve.
    /// * `start` - The start time of the window to consider.
    /// * `end` - The end time of the window to consider.
    ///
    /// Returns:
    ///
    /// A vector of tuples representing the temporal values of the property for the given vertex
    /// that fall within the specified time window, where the first element of each tuple is the timestamp
    /// and the second element is the property value.
    fn temporal_vertex_prop_vec_window(
        &self,
        v: VID,
        id: usize,
        start: i64,
        end: i64,
    ) -> Vec<(i64, Prop)>;

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
        layer_ids: LayerIds,
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
    fn temporal_edge_prop_vec_window(
        &self,
        e: EdgeRef,
        id: usize,
        start: i64,
        end: i64,
        layer_ids: LayerIds,
    ) -> Vec<(i64, Prop)>;

    /// Check if edge has temporal property with the given id
    ///
    /// # Arguments
    ///
    /// * `e` - The id of the edge
    /// * `prop_id` - The id of the property to retrieve.
    fn has_temporal_edge_prop(&self, e: EdgeRef, prop_id: usize, layer_ids: LayerIds) -> bool;

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
    fn temporal_edge_prop_vec(
        &self,
        e: EdgeRef,
        id: usize,
        layer_ids: LayerIds,
    ) -> Vec<(i64, Prop)>;
}

pub trait InheritTimeSemantics: Base + GraphOps + CoreGraphOps {}

impl<G: InheritTimeSemantics> DelegateTimeSemantics for G
where
    <G as Base>::Base: TimeSemantics,
{
    type Internal = <G as Base>::Base;

    fn graph(&self) -> &Self::Internal {
        self.base()
    }
}

pub trait DelegateTimeSemantics: GraphOps + CoreGraphOps {
    type Internal: TimeSemantics + ?Sized;

    fn graph(&self) -> &Self::Internal;
}

impl<G: DelegateTimeSemantics + ?Sized> TimeSemantics for G {
    #[inline]
    fn vertex_earliest_time(&self, v: VID) -> Option<i64> {
        self.graph().vertex_earliest_time(v)
    }

    #[inline]
    fn vertex_latest_time(&self, v: VID) -> Option<i64> {
        self.graph().vertex_latest_time(v)
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
    fn vertex_earliest_time_window(&self, v: VID, start: i64, end: i64) -> Option<i64> {
        self.graph().vertex_earliest_time_window(v, start, end)
    }
    #[inline]
    fn vertex_latest_time_window(&self, v: VID, start: i64, end: i64) -> Option<i64> {
        self.graph().vertex_latest_time_window(v, start, end)
    }
    #[inline]
    fn include_vertex_window(
        &self,
        v: VID,
        w: Range<i64>,
        layer_ids: &LayerIds,
        edge_filter: Option<&EdgeFilter>,
    ) -> bool {
        self.graph()
            .include_vertex_window(v, w, layer_ids, edge_filter)
    }

    #[inline]
    fn include_edge_window(&self, e: &EdgeStore, w: Range<i64>, layer_ids: &LayerIds) -> bool {
        self.graph().include_edge_window(e, w, layer_ids)
    }

    #[inline]
    fn vertex_history(&self, v: VID) -> Vec<i64> {
        self.graph().vertex_history(v)
    }

    #[inline]
    fn vertex_history_window(&self, v: VID, w: Range<i64>) -> Vec<i64> {
        self.graph().vertex_history_window(v, w)
    }

    #[inline]
    fn edge_exploded(&self, e: EdgeRef, layer_ids: LayerIds) -> BoxedIter<EdgeRef> {
        self.graph().edge_exploded(e, layer_ids)
    }

    #[inline]
    fn edge_layers(&self, e: EdgeRef, layer_ids: LayerIds) -> BoxedIter<EdgeRef> {
        self.graph().edge_layers(e, layer_ids)
    }

    #[inline]
    fn edge_window_exploded(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> BoxedIter<EdgeRef> {
        self.graph().edge_window_exploded(e, w, layer_ids)
    }

    #[inline]
    fn edge_window_layers(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> BoxedIter<EdgeRef> {
        self.graph().edge_window_layers(e, w, layer_ids)
    }

    #[inline]
    fn edge_earliest_time(&self, e: EdgeRef, layer_ids: LayerIds) -> Option<i64> {
        self.graph().edge_earliest_time(e, layer_ids)
    }

    #[inline]
    fn edge_earliest_time_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> Option<i64> {
        self.graph().edge_earliest_time_window(e, w, layer_ids)
    }

    #[inline]
    fn edge_latest_time(&self, e: EdgeRef, layer_ids: LayerIds) -> Option<i64> {
        self.graph().edge_latest_time(e, layer_ids)
    }

    #[inline]
    fn edge_latest_time_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> Option<i64> {
        self.graph().edge_latest_time_window(e, w, layer_ids)
    }

    #[inline]
    fn edge_deletion_history(&self, e: EdgeRef, layer_ids: LayerIds) -> Vec<i64> {
        self.graph().edge_deletion_history(e, layer_ids)
    }

    #[inline]
    fn edge_deletion_history_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> Vec<i64> {
        self.graph().edge_deletion_history_window(e, w, layer_ids)
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
    fn temporal_prop_vec_window(
        &self,
        prop_id: usize,
        start: i64,
        end: i64,
    ) -> Vec<(i64, Prop)> {
        self.graph()
            .temporal_prop_vec_window(prop_id, start, end)
    }

    #[inline]
    fn has_temporal_vertex_prop(&self, v: VID, prop_id: usize) -> bool {
        self.graph().has_temporal_vertex_prop(v, prop_id)
    }

    #[inline]
    fn temporal_vertex_prop_vec(&self, v: VID, prop_id: usize) -> Vec<(i64, Prop)> {
        self.graph().temporal_vertex_prop_vec(v, prop_id)
    }

    #[inline]
    fn has_temporal_vertex_prop_window(&self, v: VID, prop_id: usize, w: Range<i64>) -> bool {
        self.graph().has_temporal_vertex_prop_window(v, prop_id, w)
    }

    #[inline]
    fn temporal_vertex_prop_vec_window(
        &self,
        v: VID,
        prop_id: usize,
        start: i64,
        end: i64,
    ) -> Vec<(i64, Prop)> {
        self.graph()
            .temporal_vertex_prop_vec_window(v, prop_id, start, end)
    }

    fn has_temporal_edge_prop_window(
        &self,
        e: EdgeRef,
        prop_id: usize,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> bool {
        self.graph()
            .has_temporal_edge_prop_window(e, prop_id, w, layer_ids)
    }

    #[inline]
    fn temporal_edge_prop_vec_window(
        &self,
        e: EdgeRef,
        prop_id: usize,
        start: i64,
        end: i64,
        layer_ids: LayerIds,
    ) -> Vec<(i64, Prop)> {
        self.graph()
            .temporal_edge_prop_vec_window(e, prop_id, start, end, layer_ids)
    }

    fn has_temporal_edge_prop(&self, e: EdgeRef, prop_id: usize, layer_ids: LayerIds) -> bool {
        self.graph().has_temporal_edge_prop(e, prop_id, layer_ids)
    }

    #[inline]
    fn temporal_edge_prop_vec(
        &self,
        e: EdgeRef,
        prop_id: usize,
        layer_ids: LayerIds,
    ) -> Vec<(i64, Prop)> {
        self.graph().temporal_edge_prop_vec(e, prop_id, layer_ids)
    }
}
