use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, LayerIds, VID},
        Prop,
    },
    db::api::view::{
        internal::{Base, EdgeFilter, EdgeWindowFilter},
        BoxedIter, MaterializedGraph,
    },
};
use enum_dispatch::enum_dispatch;
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
    fn include_node_window(
        &self,
        v: VID,
        w: Range<i64>,
        layer_ids: &LayerIds,
        edge_filter: Option<&EdgeFilter>,
    ) -> bool;

    /// check if edge `e` should be included in window `w`
    fn include_edge_window(&self) -> &EdgeWindowFilter;

    /// Get the timestamps at which a node `v` is active (i.e has an edge addition)
    fn node_history(&self, v: VID) -> Vec<i64>;

    /// Get the timestamps at which a node `v` is active in window `w` (i.e has an edge addition)
    fn node_history_window(&self, v: VID, w: Range<i64>) -> Vec<i64>;

    fn edge_history(&self, e: EdgeRef, layer_ids: LayerIds) -> Vec<i64>;

    fn edge_history_window(&self, e: EdgeRef, layer_ids: LayerIds, w: Range<i64>) -> Vec<i64>;

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

    /// Check if  edge `e` is currently valid in any layer included in `layer_ids`
    fn edge_is_valid(&self, e: EdgeRef, layer_ids: LayerIds) -> bool;

    /// Check if edge `e` is valid at the end of a window with exclusive end time `t` in all layers included in `layer_ids`
    fn edge_is_valid_at_end(&self, e: EdgeRef, layer_ids: LayerIds, t: i64) -> bool;

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
    fn temporal_prop_vec_window(&self, prop_id: usize, start: i64, end: i64) -> Vec<(i64, Prop)>;

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
    fn temporal_node_prop_vec(&self, v: VID, id: usize) -> Vec<(i64, Prop)>;

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
    fn temporal_node_prop_vec_window(
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
        v: VID,
        w: Range<i64>,
        layer_ids: &LayerIds,
        edge_filter: Option<&EdgeFilter>,
    ) -> bool {
        self.graph()
            .include_node_window(v, w, layer_ids, edge_filter)
    }

    #[inline]
    fn include_edge_window(&self) -> &EdgeWindowFilter {
        self.graph().include_edge_window()
    }

    #[inline]
    fn node_history(&self, v: VID) -> Vec<i64> {
        self.graph().node_history(v)
    }

    #[inline]
    fn node_history_window(&self, v: VID, w: Range<i64>) -> Vec<i64> {
        self.graph().node_history_window(v, w)
    }

    #[inline]
    fn edge_history(&self, e: EdgeRef, layer_ids: LayerIds) -> Vec<i64> {
        self.graph().edge_history(e, layer_ids)
    }

    #[inline]
    fn edge_history_window(&self, e: EdgeRef, layer_ids: LayerIds, w: Range<i64>) -> Vec<i64> {
        self.graph().edge_history_window(e, layer_ids, w)
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
    fn edge_is_valid(&self, e: EdgeRef, layer_ids: LayerIds) -> bool {
        self.graph().edge_is_valid(e, layer_ids)
    }

    #[inline]
    fn edge_is_valid_at_end(&self, e: EdgeRef, layer_ids: LayerIds, t: i64) -> bool {
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
    fn temporal_node_prop_vec(&self, v: VID, prop_id: usize) -> Vec<(i64, Prop)> {
        self.graph().temporal_node_prop_vec(v, prop_id)
    }

    #[inline]
    fn has_temporal_node_prop_window(&self, v: VID, prop_id: usize, w: Range<i64>) -> bool {
        self.graph().has_temporal_node_prop_window(v, prop_id, w)
    }

    #[inline]
    fn temporal_node_prop_vec_window(
        &self,
        v: VID,
        prop_id: usize,
        start: i64,
        end: i64,
    ) -> Vec<(i64, Prop)> {
        self.graph()
            .temporal_node_prop_vec_window(v, prop_id, start, end)
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
