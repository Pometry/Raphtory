use crate::db::api::view::internal::{
    time_semantics::{
        event_semantics::EventSemantics, persistent_semantics::PersistentSemantics,
        time_semantics_ops::NodeTimeSemanticsOps,
    },
    EdgeTimeSemanticsOps, GraphView,
};
use iter_enum::{DoubleEndedIterator, ExactSizeIterator, FusedIterator, Iterator};
use raphtory_api::core::{
    entities::{properties::prop::Prop, LayerId, LayerIds, ELID},
    storage::timeindex::EventTime,
};
use raphtory_storage::graph::nodes::node_ref::NodeStorageRef;
use std::ops::Range;
use storage::EdgeEntryRef;

#[derive(Copy, Clone, Debug)]
pub enum BaseTimeSemantics {
    Persistent(PersistentSemantics),
    Event(EventSemantics),
}

macro_rules! for_all {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            BaseTimeSemantics::Persistent($pattern) => $result,
            BaseTimeSemantics::Event($pattern) => $result,
        }
    };
}

macro_rules! for_all_iter {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            BaseTimeSemantics::Persistent($pattern) => {
                BaseTimeSemanticsVariants::Persistent($result)
            }
            BaseTimeSemantics::Event($pattern) => BaseTimeSemanticsVariants::Event($result),
        }
    };
}

#[derive(Iterator, DoubleEndedIterator, ExactSizeIterator, FusedIterator)]
pub enum BaseTimeSemanticsVariants<Persistent, Event> {
    Persistent(Persistent),
    Event(Event),
}

impl NodeTimeSemanticsOps for BaseTimeSemantics {
    #[inline]
    fn node_earliest_time<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> Option<EventTime> {
        for_all!(self, semantics => semantics.node_earliest_time(node, view))
    }

    #[inline]
    fn node_latest_time<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> Option<EventTime> {
        for_all!(self, semantics => semantics.node_latest_time(node, view))
    }

    #[inline]
    fn node_earliest_time_window<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<EventTime>,
    ) -> Option<EventTime> {
        for_all!(self, semantics => semantics.node_earliest_time_window(node, view, w))
    }

    #[inline]
    fn node_latest_time_window<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<EventTime>,
    ) -> Option<EventTime> {
        for_all!(self, semantics => semantics.node_latest_time_window(node, view, w))
    }

    #[inline]
    fn node_history<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> impl Iterator<Item = EventTime> + Send + Sync + 'graph {
        for_all_iter!(self, semantics => semantics.node_history(node, view))
    }

    #[inline]
    fn node_history_rev<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> impl Iterator<Item = EventTime> + Send + Sync + 'graph {
        for_all_iter!(self, semantics => semantics.node_history_rev(node, view))
    }

    #[inline]
    fn node_history_window<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = EventTime> + Send + Sync + 'graph {
        for_all_iter!(self, semantics => semantics.node_history_window(node, view, w))
    }

    #[inline]
    fn node_history_window_rev<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = EventTime> + Send + Sync + 'graph {
        for_all_iter!(self, semantics => semantics.node_history_window_rev(node, view, w))
    }

    #[inline]
    fn node_edge_history_count<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> usize {
        for_all!(self, semantics => semantics.node_edge_history_count(node, view))
    }

    #[inline]
    fn node_edge_history<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> impl Iterator<Item = (EventTime, ELID)> + Send + Sync + 'graph {
        for_all_iter!(self, semantics => semantics.node_edge_history(node, view))
    }

    #[inline]
    fn node_edge_history_window<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = (EventTime, ELID)> + Send + Sync + 'graph {
        for_all_iter!(self, semantics => semantics.node_edge_history_window(node, view, w))
    }

    #[inline]
    fn node_edge_history_rev<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> impl Iterator<Item = (EventTime, ELID)> + Send + Sync + 'graph {
        for_all_iter!(self, semantics => semantics.node_edge_history_rev(node, view))
    }

    #[inline]
    fn node_edge_history_rev_window<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = (EventTime, ELID)> + Send + Sync + 'graph {
        for_all_iter!(self, semantics => semantics.node_edge_history_rev_window(node, view, w))
    }

    #[inline]
    fn node_edge_history_count_window<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<EventTime>,
    ) -> usize {
        for_all!(self, semantics => semantics.node_edge_history_count_window(node, view, w))
    }

    #[inline]
    fn node_updates<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> impl Iterator<Item = (EventTime, LayerId, Vec<(usize, Prop)>)> + Send + Sync + 'graph {
        for_all_iter!(self, semantics => semantics.node_updates(node, view))
    }

    #[inline]
    fn node_updates_window<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = (EventTime, LayerId, Vec<(usize, Prop)>)> + Send + Sync + 'graph {
        for_all_iter!(self, semantics => semantics.node_updates_window(node, view, w))
    }

    #[inline]
    fn node_valid<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> bool {
        for_all!(self, semantics => semantics.node_valid(node, view))
    }

    #[inline]
    fn node_valid_window<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<EventTime>,
    ) -> bool {
        for_all!(self, semantics => semantics.node_valid_window(node, view, w))
    }

    #[inline]
    fn node_tprop_iter<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        prop_id: usize,
    ) -> impl Iterator<Item = (EventTime, Prop)> + Send + Sync + 'graph {
        for_all_iter!(self, semantics => semantics.node_tprop_iter(node, view, prop_id))
    }

    #[inline]
    fn node_tprop_iter_rev<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        prop_id: usize,
    ) -> impl Iterator<Item = (EventTime, Prop)> + Send + Sync + 'graph {
        for_all_iter!(self, semantics => semantics.node_tprop_iter_rev(node, view, prop_id))
    }

    #[inline]
    fn node_tprop_iter_window<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = (EventTime, Prop)> + Send + Sync + 'graph {
        for_all_iter!(self, semantics => semantics.node_tprop_iter_window(node, view, prop_id, w))
    }

    #[inline]
    fn node_tprop_iter_window_rev<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = (EventTime, Prop)> + Send + Sync + 'graph {
        for_all_iter!(self, semantics => semantics.node_tprop_iter_window_rev(node, view, prop_id, w))
    }

    #[inline]
    fn node_tprop_last_at<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        t: EventTime,
    ) -> Option<(EventTime, Prop)> {
        for_all!(self, semantics => semantics.node_tprop_last_at(node, view, prop_id, t))
    }

    #[inline]
    fn node_tprop_last_at_window<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        t: EventTime,
        w: Range<EventTime>,
    ) -> Option<(EventTime, Prop)> {
        for_all!(self, semantics => semantics.node_tprop_last_at_window(node, view, prop_id, t, w))
    }
}

impl EdgeTimeSemanticsOps for BaseTimeSemantics {
    fn handle_edge_update_filter<G: GraphView>(
        &self,
        t: EventTime,
        eid: ELID,
        view: G,
    ) -> Option<(EventTime, ELID)> {
        for_all!(self, semantics => semantics.handle_edge_update_filter(t, eid, view))
    }

    fn include_edge<G: GraphView>(&self, edge: EdgeEntryRef, view: G, layer_id: LayerId) -> bool {
        for_all!(self, semantics => semantics.include_edge(edge, view, layer_id))
    }

    #[inline]
    fn include_edge_window<G: GraphView>(
        &self,
        edge: EdgeEntryRef,
        view: G,
        layer_id: LayerId,
        w: Range<EventTime>,
    ) -> bool {
        for_all!(self, semantics => semantics.include_edge_window(edge, view, layer_id, w))
    }

    fn include_exploded_edge<G: GraphView>(&self, elid: ELID, t: EventTime, view: G) -> bool {
        for_all!(self, semantics => semantics.include_exploded_edge(elid, t, view))
    }

    fn include_exploded_edge_window<G: GraphView>(
        &self,
        elid: ELID,
        t: EventTime,
        view: G,
        w: Range<EventTime>,
    ) -> bool {
        for_all!(self, semantics => semantics.include_exploded_edge_window(elid, t, view, w))
    }

    #[inline]
    fn edge_history<'graph, G: GraphView + 'graph>(
        self,
        edge: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
    ) -> impl Iterator<Item = (EventTime, LayerId)> + Send + Sync + 'graph {
        for_all_iter!(self, semantics => semantics.edge_history(edge, view, layer_ids))
    }

    #[inline]
    fn edge_history_rev<'graph, G: GraphView + 'graph>(
        self,
        edge: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
    ) -> impl Iterator<Item = (EventTime, LayerId)> + Send + Sync + 'graph {
        for_all_iter!(self, semantics => semantics.edge_history_rev(edge, view, layer_ids))
    }

    #[inline]
    fn edge_history_window<'graph, G: GraphView + 'graph>(
        self,
        edge: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = (EventTime, LayerId)> + Send + Sync + 'graph {
        for_all_iter!(self, semantics => semantics.edge_history_window(edge, view, layer_ids, w))
    }

    #[inline]
    fn edge_history_window_rev<'graph, G: GraphView + 'graph>(
        self,
        edge: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = (EventTime, LayerId)> + Send + Sync + 'graph {
        for_all_iter!(self, semantics => semantics.edge_history_window_rev(edge, view, layer_ids, w))
    }

    #[inline]
    fn edge_exploded_count<'graph, G: GraphView + 'graph>(
        &self,
        edge: EdgeEntryRef,
        view: G,
    ) -> usize {
        for_all!(self, semantics => semantics.edge_exploded_count(edge, view))
    }

    #[inline]
    fn edge_exploded_count_window<'graph, G: GraphView + 'graph>(
        &self,
        edge: EdgeEntryRef,
        view: G,
        w: Range<EventTime>,
    ) -> usize {
        for_all!(self, semantics => semantics.edge_exploded_count_window(edge, view, w))
    }

    #[inline]
    fn edge_exploded<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
    ) -> impl Iterator<Item = (EventTime, LayerId)> + Send + Sync + 'graph {
        for_all_iter!(self, semantics => semantics.edge_exploded(e, view, layer_ids))
    }

    #[inline]
    fn edge_layers<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
    ) -> impl Iterator<Item = LayerId> + Send + Sync + 'graph {
        for_all_iter!(self, semantics => semantics.edge_layers(e, view, layer_ids))
    }

    #[inline]
    fn edge_window_exploded<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = (EventTime, LayerId)> + Send + Sync + 'graph {
        for_all_iter!(self, semantics => semantics.edge_window_exploded(e, view, layer_ids, w))
    }

    #[inline]
    fn edge_window_layers<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = LayerId> + Send + Sync + 'graph {
        for_all_iter!(self, semantics => semantics.edge_window_layers(e, view, layer_ids, w))
    }

    #[inline]
    fn edge_earliest_time<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef,
        view: G,
    ) -> Option<EventTime> {
        for_all!(self, semantics => semantics.edge_earliest_time(e, view))
    }

    #[inline]
    fn edge_earliest_time_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef,
        view: G,
        w: Range<EventTime>,
    ) -> Option<EventTime> {
        for_all!(self, semantics => semantics.edge_earliest_time_window(e, view, w))
    }

    #[inline]
    fn edge_exploded_earliest_time<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef,
        view: G,
        t: EventTime,
        layer: LayerId,
    ) -> Option<EventTime> {
        for_all!(self, semantics => semantics.edge_exploded_earliest_time(e, view, t, layer))
    }

    #[inline]
    fn edge_exploded_earliest_time_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef,
        view: G,
        t: EventTime,
        layer: LayerId,
        w: Range<EventTime>,
    ) -> Option<EventTime> {
        for_all!(self, semantics => semantics.edge_exploded_earliest_time_window(e, view, t, layer, w))
    }

    #[inline]
    fn edge_latest_time<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef,
        view: G,
    ) -> Option<EventTime> {
        for_all!(self, semantics => semantics.edge_latest_time(e, view))
    }

    #[inline]
    fn edge_latest_time_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef,
        view: G,
        w: Range<EventTime>,
    ) -> Option<EventTime> {
        for_all!(self, semantics => semantics.edge_latest_time_window(e, view, w))
    }

    #[inline]
    fn edge_exploded_latest_time<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef,
        view: G,
        t: EventTime,
        layer: LayerId,
    ) -> Option<EventTime> {
        for_all!(self, semantics => semantics.edge_exploded_latest_time(e, view, t, layer))
    }

    #[inline]
    fn edge_exploded_latest_time_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef,
        view: G,
        t: EventTime,
        layer: LayerId,
        w: Range<EventTime>,
    ) -> Option<EventTime> {
        for_all!(self, semantics => semantics.edge_exploded_latest_time_window(e, view, t, layer, w))
    }

    #[inline]
    fn edge_deletion_history<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
    ) -> impl Iterator<Item = (EventTime, LayerId)> + Send + Sync + 'graph {
        for_all_iter!(self, semantics => semantics.edge_deletion_history(e, view, layer_ids))
    }

    #[inline]
    fn edge_deletion_history_rev<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
    ) -> impl Iterator<Item = (EventTime, LayerId)> + Send + Sync + 'graph {
        for_all_iter!(self, semantics => semantics.edge_deletion_history_rev(e, view, layer_ids))
    }

    #[inline]
    fn edge_deletion_history_window<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = (EventTime, LayerId)> + Send + Sync + 'graph {
        for_all_iter!(self, semantics => semantics.edge_deletion_history_window(e, view, layer_ids, w))
    }

    #[inline]
    fn edge_deletion_history_window_rev<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = (EventTime, LayerId)> + Send + Sync + 'graph {
        for_all_iter!(self, semantics => semantics.edge_deletion_history_window_rev(e, view, layer_ids, w))
    }

    #[inline]
    fn edge_is_valid<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
    ) -> bool {
        for_all!(self, semantics => semantics.edge_is_valid(e, view))
    }

    #[inline]
    fn edge_is_valid_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        r: Range<EventTime>,
    ) -> bool {
        for_all!(self, semantics => semantics.edge_is_valid_window(e, view, r))
    }

    #[inline]
    fn edge_is_deleted<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
    ) -> bool {
        for_all!(self, semantics => semantics.edge_is_deleted(e, view))
    }

    #[inline]
    fn edge_is_deleted_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        w: Range<EventTime>,
    ) -> bool {
        for_all!(self, semantics => semantics.edge_is_deleted_window(e, view, w))
    }

    #[inline]
    fn edge_is_active<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
    ) -> bool {
        for_all!(self, semantics => semantics.edge_is_active(e, view))
    }

    #[inline]
    fn edge_is_active_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        w: Range<EventTime>,
    ) -> bool {
        for_all!(self, semantics => semantics.edge_is_active_window(e, view, w))
    }

    #[inline]
    fn edge_is_active_exploded<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        t: EventTime,
        layer: LayerId,
    ) -> bool {
        for_all!(self, semantics => semantics.edge_is_active_exploded(e, view, t, layer))
    }

    #[inline]
    fn edge_is_active_exploded_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        t: EventTime,
        layer: LayerId,
        w: Range<EventTime>,
    ) -> bool {
        for_all!(self, semantics => semantics.edge_is_active_exploded_window(e, view, t, layer, w))
    }

    #[inline]
    fn edge_is_valid_exploded<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        t: EventTime,
        layer: LayerId,
    ) -> bool {
        for_all!(self, semantics => semantics.edge_is_valid_exploded(e, view, t, layer))
    }

    #[inline]
    fn edge_is_valid_exploded_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        t: EventTime,
        layer: LayerId,
        w: Range<EventTime>,
    ) -> bool {
        for_all!(self, semantics => semantics.edge_is_valid_exploded_window(e, view, t, layer, w))
    }

    #[inline]
    fn edge_exploded_deletion<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        t: EventTime,
        layer: LayerId,
    ) -> Option<EventTime> {
        for_all!(self, semantics => semantics.edge_exploded_deletion(e, view, t, layer))
    }

    #[inline]
    fn edge_exploded_deletion_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        t: EventTime,
        layer: LayerId,
        w: Range<EventTime>,
    ) -> Option<EventTime> {
        for_all!(self, semantics => semantics.edge_exploded_deletion_window(e, view, t, layer, w))
    }

    #[inline]
    fn temporal_edge_prop_exploded<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        prop_id: usize,
        t: EventTime,
        layer_id: LayerId,
    ) -> Option<Prop> {
        for_all!(self, semantics => semantics.temporal_edge_prop_exploded(e, view, prop_id, t, layer_id))
    }

    #[inline]
    fn temporal_edge_prop_exploded_last_at<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        edge_time: EventTime,
        layer_id: LayerId,
        prop_id: usize,
        at: EventTime,
    ) -> Option<Prop> {
        for_all!(self, semantics => semantics.temporal_edge_prop_exploded_last_at(e, view, edge_time, layer_id, prop_id, at))
    }

    #[inline]
    fn temporal_edge_prop_exploded_last_at_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        edge_time: EventTime,
        layer_id: LayerId,
        prop_id: usize,
        at: EventTime,
        w: Range<EventTime>,
    ) -> Option<Prop> {
        for_all!(self, semantics => semantics.temporal_edge_prop_exploded_last_at_window(e, view, edge_time, layer_id, prop_id, at, w))
    }

    #[inline]
    fn temporal_edge_prop_last_at<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        prop_id: usize,
        t: EventTime,
    ) -> Option<Prop> {
        for_all!(self, semantics => semantics.temporal_edge_prop_last_at(e, view, prop_id, t))
    }

    #[inline]
    fn temporal_edge_prop_last_at_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        prop_id: usize,
        t: EventTime,
        w: Range<EventTime>,
    ) -> Option<Prop> {
        for_all!(self, semantics => semantics.temporal_edge_prop_last_at_window(e, view, prop_id, t, w))
    }

    #[inline]
    fn temporal_edge_prop_hist<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        prop_id: usize,
    ) -> impl Iterator<Item = (EventTime, LayerId, Prop)> + Send + Sync + 'graph {
        for_all_iter!(self, semantics => semantics.temporal_edge_prop_hist(e, view, layer_ids, prop_id))
    }

    #[inline]
    fn temporal_edge_prop_hist_rev<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        prop_id: usize,
    ) -> impl Iterator<Item = (EventTime, LayerId, Prop)> + Send + Sync + 'graph {
        for_all_iter!(self, semantics => semantics.temporal_edge_prop_hist_rev(e, view,layer_ids, prop_id))
    }

    #[inline]
    fn temporal_edge_prop_hist_window<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        prop_id: usize,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = (EventTime, LayerId, Prop)> + Send + Sync + 'graph {
        for_all_iter!(self, semantics => semantics.temporal_edge_prop_hist_window(e, view, layer_ids, prop_id, w))
    }

    #[inline]
    fn temporal_edge_prop_hist_window_rev<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        prop_id: usize,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = (EventTime, LayerId, Prop)> + Send + Sync + 'graph {
        for_all_iter!(self, semantics => semantics.temporal_edge_prop_hist_window_rev(e, view, layer_ids, prop_id, w))
    }

    #[inline]
    fn edge_metadata<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        prop_id: usize,
    ) -> Option<Prop> {
        for_all!(self, semantics => semantics.edge_metadata(e, view, prop_id))
    }

    #[inline]
    fn edge_metadata_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        prop_id: usize,
        w: Range<EventTime>,
    ) -> Option<Prop> {
        for_all!(self, semantics => semantics.edge_metadata_window(e, view, prop_id, w))
    }
}
