use crate::{
    core::Prop,
    db::api::{
        storage::graph::{
            edges::{edge_entry::EdgeStorageEntry, edge_ref::EdgeStorageRef},
            nodes::node_ref::NodeStorageRef,
        },
        view::internal::{
            time_semantics::{
                event_semantics::EventSemantics, persistent_semantics::PersistentSemantics,
                time_semantics_ops::NodeTimeSemanticsOps,
            },
            EdgeTimeSemanticsOps,
        },
    },
    prelude::GraphViewOps,
};
use raphtory_api::{
    core::{entities::edges::edge_ref::EdgeRef, storage::timeindex::TimeIndexEntry},
    iter::{BoxedLDIter, BoxedLIter},
};
use std::ops::Range;

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

impl NodeTimeSemanticsOps for BaseTimeSemantics {
    fn node_earliest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> Option<i64> {
        for_all!(self, semantics => semantics.node_earliest_time(node, view))
    }

    fn node_latest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> Option<i64> {
        for_all!(self, semantics => semantics.node_latest_time(node, view))
    }

    fn node_earliest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> Option<i64> {
        for_all!(self, semantics => semantics.node_earliest_time_window(node, view, w))
    }

    fn node_latest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> Option<i64> {
        for_all!(self, semantics => semantics.node_latest_time_window(node, view, w))
    }

    fn node_history<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> BoxedLIter<'graph, i64> {
        for_all!(self, semantics => semantics.node_history(node, view))
    }

    fn node_history_window<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, i64> {
        for_all!(self, semantics => semantics.node_history_window(node, view, w))
    }

    fn node_updates<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, Vec<(usize, Prop)>)> {
        for_all!(self, semantics => semantics.node_updates(node, view))
    }

    fn node_updates_window<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, Vec<(usize, Prop)>)> {
        for_all!(self, semantics => semantics.node_updates_window(node, view, w))
    }

    fn node_valid<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> bool {
        for_all!(self, semantics => semantics.node_valid(node, view))
    }

    fn node_valid_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> bool {
        for_all!(self, semantics => semantics.node_valid_window(node, view, w))
    }

    fn node_tprop_iter<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        prop_id: usize,
    ) -> BoxedLDIter<'graph, (TimeIndexEntry, Prop)> {
        for_all!(self, semantics => semantics.node_tprop_iter(node, view, prop_id))
    }

    fn node_tprop_iter_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        w: Range<i64>,
    ) -> BoxedLDIter<'graph, (TimeIndexEntry, Prop)> {
        for_all!(self, semantics => semantics.node_tprop_iter_window(node, view, prop_id, w))
    }

    fn node_tprop_last_at<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        t: TimeIndexEntry,
    ) -> Option<(TimeIndexEntry, Prop)> {
        for_all!(self, semantics => semantics.node_tprop_last_at(node, view, prop_id, t))
    }

    fn node_tprop_last_at_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        t: TimeIndexEntry,
        w: Range<i64>,
    ) -> Option<(TimeIndexEntry, Prop)> {
        for_all!(self, semantics => semantics.node_tprop_last_at_window(node, view, prop_id, t, w))
    }
}

impl EdgeTimeSemanticsOps for BaseTimeSemantics {
    fn include_edge_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        edge: EdgeStorageRef,
        view: G,
        w: Range<i64>,
    ) -> bool {
        for_all!(self, semantics => semantics.include_edge_window(edge, view, w))
    }

    fn edge_history<'graph, G: GraphViewOps<'graph>>(
        self,
        edge: EdgeStorageRef<'graph>,
        view: G,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize)> {
        for_all!(self, semantics => semantics.edge_history(edge, view))
    }

    fn edge_history_window<'graph, G: GraphViewOps<'graph>>(
        self,
        edge: EdgeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize)> {
        for_all!(self, semantics => semantics.edge_history_window(edge, view, w))
    }

    fn edge_exploded_count<'graph, G: GraphViewOps<'graph>>(
        &self,
        edge: EdgeStorageRef,
        view: G,
    ) -> usize {
        for_all!(self, semantics => semantics.edge_exploded_count(edge, view))
    }

    fn edge_exploded_count_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        edge: EdgeStorageRef,
        view: G,
        w: Range<i64>,
    ) -> usize {
        for_all!(self, semantics => semantics.edge_exploded_count_window(edge, view, w))
    }

    fn edge_exploded<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
    ) -> BoxedLIter<'graph, EdgeRef> {
        for_all!(self, semantics => semantics.edge_exploded(e, view))
    }

    fn edge_layers<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
    ) -> BoxedLIter<'graph, EdgeRef> {
        for_all!(self, semantics => semantics.edge_layers(e, view))
    }

    fn edge_window_exploded<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, EdgeRef> {
        for_all!(self, semantics => semantics.edge_window_exploded(e, view, w))
    }

    fn edge_window_layers<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, EdgeRef> {
        for_all!(self, semantics => semantics.edge_window_layers(e, view, w))
    }

    fn edge_earliest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
    ) -> Option<i64> {
        for_all!(self, semantics => semantics.edge_earliest_time(e, view))
    }

    fn edge_earliest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
        w: Range<i64>,
    ) -> Option<i64> {
        for_all!(self, semantics => semantics.edge_earliest_time_window(e, view, w))
    }

    fn edge_latest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
    ) -> Option<i64> {
        for_all!(self, semantics => semantics.edge_latest_time(e, view))
    }

    fn edge_latest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
        w: Range<i64>,
    ) -> Option<i64> {
        for_all!(self, semantics => semantics.edge_latest_time_window(e, view, w))
    }

    fn edge_deletion_history<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize)> {
        for_all!(self, semantics => semantics.edge_deletion_history(e, view))
    }

    fn edge_deletion_history_window<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize)> {
        for_all!(self, semantics => semantics.edge_deletion_history_window(e, view, w))
    }

    fn edge_is_valid<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
    ) -> bool {
        for_all!(self, semantics => semantics.edge_is_valid(e, view))
    }

    fn edge_is_valid_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        r: Range<i64>,
    ) -> bool {
        for_all!(self, semantics => semantics.edge_is_valid_window(e, view, r))
    }

    fn edge_is_deleted<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
    ) -> bool {
        for_all!(self, semantics => semantics.edge_is_deleted(e, view))
    }

    fn edge_is_deleted_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> bool {
        for_all!(self, semantics => semantics.edge_is_deleted_window(e, view, w))
    }

    fn edge_is_active<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
    ) -> bool {
        for_all!(self, semantics => semantics.edge_is_active(e, view))
    }

    fn edge_is_active_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> bool {
        for_all!(self, semantics => semantics.edge_is_active_window(e, view, w))
    }

    fn edge_is_active_exploded<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        t: TimeIndexEntry,
        layer: usize,
    ) -> bool {
        for_all!(self, semantics => semantics.edge_is_active_exploded(e, view, t, layer))
    }

    fn edge_is_active_exploded_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        t: TimeIndexEntry,
        layer: usize,
        w: Range<i64>,
    ) -> bool {
        for_all!(self, semantics => semantics.edge_is_active_exploded_window(e, view, t, layer, w))
    }

    fn edge_is_valid_exploded<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        t: TimeIndexEntry,
        layer: usize,
    ) -> bool {
        for_all!(self, semantics => semantics.edge_is_valid_exploded(e, view, t, layer))
    }

    fn edge_is_valid_exploded_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        t: TimeIndexEntry,
        layer: usize,
        w: Range<i64>,
    ) -> bool {
        for_all!(self, semantics => semantics.edge_is_valid_exploded_window(e, view, t, layer, w))
    }

    fn edge_exploded_deletion<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        t: TimeIndexEntry,
        layer: usize,
    ) -> Option<TimeIndexEntry> {
        for_all!(self, semantics => semantics.edge_exploded_deletion(e, view, t, layer))
    }

    fn edge_exploded_deletion_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        t: TimeIndexEntry,
        layer: usize,
        w: Range<i64>,
    ) -> Option<TimeIndexEntry> {
        for_all!(self, semantics => semantics.edge_exploded_deletion_window(e, view, t, layer, w))
    }

    fn temporal_edge_prop_exploded<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        t: TimeIndexEntry,
        layer_id: usize,
    ) -> Option<Prop> {
        for_all!(self, semantics => semantics.temporal_edge_prop_exploded(e, view, prop_id, t, layer_id))
    }

    fn temporal_edge_prop_last_at<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        t: TimeIndexEntry,
    ) -> Option<Prop> {
        for_all!(self, semantics => semantics.temporal_edge_prop_last_at(e, view, prop_id, t))
    }

    fn temporal_edge_prop_last_at_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        t: TimeIndexEntry,
        w: Range<i64>,
    ) -> Option<Prop> {
        for_all!(self, semantics => semantics.temporal_edge_prop_last_at_window(e, view, prop_id, t, w))
    }

    fn temporal_edge_prop_hist<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        prop_id: usize,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize, Prop)> {
        for_all!(self, semantics => semantics.temporal_edge_prop_hist(e, view, prop_id))
    }

    fn temporal_edge_prop_hist_rev<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        prop_id: usize,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize, Prop)> {
        for_all!(self, semantics => semantics.temporal_edge_prop_hist_rev(e, view, prop_id))
    }

    fn temporal_edge_prop_hist_window<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize, Prop)> {
        for_all!(self, semantics => semantics.temporal_edge_prop_hist_window(e, view, prop_id, w))
    }

    fn temporal_edge_prop_hist_window_rev<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize, Prop)> {
        for_all!(self, semantics => semantics.temporal_edge_prop_hist_window_rev(e, view, prop_id, w))
    }

    fn constant_edge_prop<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        prop_id: usize,
    ) -> Option<Prop> {
        for_all!(self, semantics => semantics.constant_edge_prop(e, view, prop_id))
    }

    fn constant_edge_prop_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        w: Range<i64>,
    ) -> Option<Prop> {
        for_all!(self, semantics => semantics.constant_edge_prop_window(e, view, prop_id, w))
    }
}
