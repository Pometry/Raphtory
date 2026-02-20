use crate::db::api::view::internal::GraphView;
use raphtory_api::core::{
    entities::{properties::prop::Prop, LayerId, LayerIds, ELID},
    storage::timeindex::EventTime,
};
use raphtory_storage::graph::nodes::node_ref::NodeStorageRef;
use std::ops::Range;
use storage::EdgeEntryRef;

pub trait NodeTimeSemanticsOps {
    fn node_earliest_time<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> Option<EventTime>;

    fn node_latest_time<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> Option<EventTime>;

    fn node_earliest_time_window<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<EventTime>,
    ) -> Option<EventTime>;

    fn node_latest_time_window<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<EventTime>,
    ) -> Option<EventTime>;

    fn node_history<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> impl Iterator<Item = EventTime> + Send + Sync + 'graph;

    fn node_history_rev<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> impl Iterator<Item = EventTime> + Send + Sync + 'graph;

    fn node_history_window<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = EventTime> + Send + Sync + 'graph;

    fn node_history_window_rev<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = EventTime> + Send + Sync + 'graph;

    fn node_edge_history_count<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> usize;

    fn node_edge_history_count_window<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<EventTime>,
    ) -> usize;

    fn node_edge_history<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> impl Iterator<Item = (EventTime, ELID)> + Send + Sync + 'graph;

    fn node_edge_history_window<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = (EventTime, ELID)> + Send + Sync + 'graph;

    fn node_edge_history_rev<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> impl Iterator<Item = (EventTime, ELID)> + Send + Sync + 'graph;

    fn node_edge_history_rev_window<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = (EventTime, ELID)> + Send + Sync + 'graph;

    fn node_updates<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> impl Iterator<Item = (EventTime, LayerId, Vec<(usize, Prop)>)> + Send + Sync + 'graph;

    fn node_updates_window<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = (EventTime, LayerId, Vec<(usize, Prop)>)> + Send + Sync + 'graph;

    /// Check if the node is part of the graph based on the history
    fn node_valid<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> bool;

    fn node_valid_window<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<EventTime>,
    ) -> bool;

    fn node_tprop_iter<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        prop_id: usize,
    ) -> impl Iterator<Item = (EventTime, Prop)> + Send + Sync + 'graph;

    fn node_tprop_iter_rev<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        prop_id: usize,
    ) -> impl Iterator<Item = (EventTime, Prop)> + Send + Sync + 'graph;

    fn node_tprop_iter_window<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = (EventTime, Prop)> + Send + Sync + 'graph;

    fn node_tprop_iter_window_rev<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = (EventTime, Prop)> + Send + Sync + 'graph;

    fn node_tprop_last_at<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        t: EventTime,
    ) -> Option<(EventTime, Prop)>;

    fn node_tprop_last_at_window<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        t: EventTime,
        w: Range<EventTime>,
    ) -> Option<(EventTime, Prop)>;
}

pub trait EdgeTimeSemanticsOps {
    fn handle_edge_update_filter<G: GraphView>(
        &self,
        t: EventTime,
        eid: ELID,
        view: G,
    ) -> Option<(EventTime, ELID)>;

    fn include_edge<G: GraphView>(&self, edge: EdgeEntryRef, view: G, layer_id: LayerId) -> bool;

    /// check if edge `e` should be included in window `w`
    fn include_edge_window<G: GraphView>(
        &self,
        edge: EdgeEntryRef,
        view: G,
        layer_id: LayerId,
        w: Range<EventTime>,
    ) -> bool;

    /// Check if exploded edge update should be included
    fn include_exploded_edge<G: GraphView>(&self, elid: ELID, t: EventTime, view: G) -> bool;

    /// Check if exploded edge update should be included in window
    fn include_exploded_edge_window<G: GraphView>(
        &self,
        elid: ELID,
        t: EventTime,
        view: G,
        w: Range<EventTime>,
    ) -> bool;

    /// returns the update history of an edge
    ///
    /// # Returns
    ///
    /// An iterator over timestamp and layer pairs
    fn edge_history<'graph, G: GraphView + 'graph>(
        self,
        edge: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
    ) -> impl Iterator<Item = (EventTime, LayerId)> + Send + Sync + 'graph;

    /// returns the update history of an edge in reverse
    ///
    /// # Returns
    ///
    /// An iterator over timestamp and layer pairs
    fn edge_history_rev<'graph, G: GraphView + 'graph>(
        self,
        edge: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
    ) -> impl Iterator<Item = (EventTime, LayerId)> + Send + Sync + 'graph;

    /// returns the update history of an edge in a window
    ///
    /// # Returns
    ///
    /// An iterator over timestamp and layer pairs
    fn edge_history_window<'graph, G: GraphView + 'graph>(
        self,
        edge: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = (EventTime, LayerId)> + Send + Sync + 'graph;

    /// returns the update history of an edge in a window in reverse
    ///
    /// # Returns
    ///
    /// An iterator over timestamp and layer pairs
    fn edge_history_window_rev<'graph, G: GraphView + 'graph>(
        self,
        edge: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = (EventTime, LayerId)> + Send + Sync + 'graph;

    /// The number of exploded edge events for the `edge`
    fn edge_exploded_count<'graph, G: GraphView + 'graph>(
        &self,
        edge: EdgeEntryRef,
        view: G,
    ) -> usize;

    /// The number of exploded edge events for the edge in the window `w`
    fn edge_exploded_count_window<'graph, G: GraphView + 'graph>(
        &self,
        edge: EdgeEntryRef,
        view: G,
        w: Range<EventTime>,
    ) -> usize;

    /// Exploded edge iterator for edge `e`
    fn edge_exploded<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
    ) -> impl Iterator<Item = (EventTime, LayerId)> + Send + Sync + 'graph;

    /// Explode edge iterator for edge `e` for every layer
    fn edge_layers<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
    ) -> impl Iterator<Item = LayerId> + Send + Sync + 'graph;

    /// Exploded edge iterator for edge`e` over window `w`
    fn edge_window_exploded<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = (EventTime, LayerId)> + Send + Sync + 'graph;

    /// Exploded edge iterator for edge `e` over window `w` for every layer
    fn edge_window_layers<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = LayerId> + Send + Sync + 'graph;

    /// Get the time of the earliest activity of an edge
    fn edge_earliest_time<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef,
        view: G,
    ) -> Option<EventTime>;

    /// Get the time of the earliest activity of an edge `e` in window `w`
    fn edge_earliest_time_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef,
        view: G,
        w: Range<EventTime>,
    ) -> Option<EventTime>;

    fn edge_exploded_earliest_time<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef,
        view: G,
        t: EventTime,
        layer: LayerId,
    ) -> Option<EventTime>;

    fn edge_exploded_earliest_time_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef,
        view: G,
        t: EventTime,
        layer: LayerId,
        w: Range<EventTime>,
    ) -> Option<EventTime>;

    /// Get the time of the latest activity of an edge
    fn edge_latest_time<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef,
        view: G,
    ) -> Option<EventTime>;

    /// Get the time of the latest activity of an edge `e` in window `w`
    fn edge_latest_time_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef,
        view: G,
        w: Range<EventTime>,
    ) -> Option<EventTime>;

    fn edge_exploded_latest_time<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef,
        view: G,
        t: EventTime,
        layer: LayerId,
    ) -> Option<EventTime>;

    fn edge_exploded_latest_time_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef,
        view: G,
        t: EventTime,
        layer: LayerId,
        w: Range<EventTime>,
    ) -> Option<EventTime>;

    /// Get the edge deletions for use with materialize
    fn edge_deletion_history<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
    ) -> impl Iterator<Item = (EventTime, LayerId)> + Send + Sync + 'graph;

    /// Get the edge deletions in reverse order for use with materialize
    fn edge_deletion_history_rev<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
    ) -> impl Iterator<Item = (EventTime, LayerId)> + Send + Sync + 'graph;

    /// Get the edge deletions for use with materialize restricted to window `w`
    fn edge_deletion_history_window<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = (EventTime, LayerId)> + Send + Sync + 'graph;

    /// Get the edge deletions in reverse order for use with materialize restricted to window `w`
    fn edge_deletion_history_window_rev<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = (EventTime, LayerId)> + Send + Sync + 'graph;

    /// Check if  edge `e` is currently valid in any layer included in the view
    fn edge_is_valid<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
    ) -> bool;

    /// Check if edge `e` is valid at the end of a window with exclusive end time `t`
    /// in any layer included in the view
    fn edge_is_valid_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        r: Range<EventTime>,
    ) -> bool;

    fn edge_is_deleted<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
    ) -> bool;

    fn edge_is_deleted_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        w: Range<EventTime>,
    ) -> bool;

    fn edge_is_active<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
    ) -> bool;

    fn edge_is_active_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        w: Range<EventTime>,
    ) -> bool;

    fn edge_is_active_exploded<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        t: EventTime,
        layer: LayerId,
    ) -> bool;

    fn edge_is_active_exploded_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        t: EventTime,
        layer: LayerId,
        w: Range<EventTime>,
    ) -> bool;

    fn edge_is_valid_exploded<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        t: EventTime,
        layer: LayerId,
    ) -> bool;

    fn edge_is_valid_exploded_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        t: EventTime,
        layer: LayerId,
        w: Range<EventTime>,
    ) -> bool;

    fn edge_exploded_deletion<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        t: EventTime,
        layer: LayerId,
    ) -> Option<EventTime>;

    fn edge_exploded_deletion_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        t: EventTime,
        layer: LayerId,
        w: Range<EventTime>,
    ) -> Option<EventTime>;

    fn edge_is_deleted_exploded<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        t: EventTime,
        layer: LayerId,
    ) -> bool {
        self.edge_exploded_deletion(e, view, t, layer).is_some()
    }

    fn edge_is_deleted_exploded_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        t: EventTime,
        layer: LayerId,
        w: Range<EventTime>,
    ) -> bool {
        self.edge_exploded_deletion_window(e, view, t, layer, w)
            .is_some()
    }

    /// Return the value of an edge temporal property at a given point in time and layer if it exists
    fn temporal_edge_prop_exploded<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        prop_id: usize,
        t: EventTime,
        layer_id: LayerId,
    ) -> Option<Prop>;

    fn temporal_edge_prop_exploded_last_at<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        edge_time: EventTime,
        layer_id: LayerId,
        prop_id: usize,
        at: EventTime,
    ) -> Option<Prop>;

    fn temporal_edge_prop_exploded_last_at_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        edge_time: EventTime,
        layer_id: LayerId,
        prop_id: usize,
        at: EventTime,
        w: Range<EventTime>,
    ) -> Option<Prop>;

    /// Return the last value of a temporal edge property at or before a given point in time
    fn temporal_edge_prop_last_at<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        prop_id: usize,
        t: EventTime,
    ) -> Option<Prop>;

    fn temporal_edge_prop_last_at_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        prop_id: usize,
        t: EventTime,
        w: Range<EventTime>,
    ) -> Option<Prop>;

    /// Return property history of an edge in temporal order
    ///
    /// Items are (timestamp, layer_id, property value)
    fn temporal_edge_prop_hist<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        prop_id: usize,
    ) -> impl Iterator<Item = (EventTime, LayerId, Prop)> + Send + Sync + 'graph;

    /// Return property history for an edge in reverse-temporal order
    ///
    /// Items are (timestamp, layer_id, property value)
    fn temporal_edge_prop_hist_rev<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        prop_id: usize,
    ) -> impl Iterator<Item = (EventTime, LayerId, Prop)> + Send + Sync + 'graph;

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
    fn temporal_edge_prop_hist_window<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        prop_id: usize,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = (EventTime, LayerId, Prop)> + Send + Sync + 'graph;

    /// Return temporal property history for a window of an edge in reverse-temporal order
    ///
    /// Items are (timestamp, layer_id, property value)
    fn temporal_edge_prop_hist_window_rev<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        prop_id: usize,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = (EventTime, LayerId, Prop)> + Send + Sync + 'graph;

    /// Get edge metadata
    ///
    /// Returns:
    ///     PropValue:
    fn edge_metadata<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        prop_id: usize,
    ) -> Option<Prop>;

    /// Get metadata edge property for a window
    ///
    /// Should only return the property for a layer if the edge exists in the window in that layer
    fn edge_metadata_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        prop_id: usize,
        w: Range<EventTime>,
    ) -> Option<Prop>;
}
