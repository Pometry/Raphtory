use crate::{
    core::Prop,
    db::api::{
        storage::graph::{edges::edge_ref::EdgeStorageRef, nodes::node_ref::NodeStorageRef},
        view::internal::{
            time_semantics::{
                base_time_semantics::BaseTimeSemantics, time_semantics_ops::NodeTimeSemanticsOps,
            },
            EdgeTimeSemanticsOps,
        },
    },
    prelude::GraphViewOps,
};
use raphtory_api::{
    core::storage::timeindex::TimeIndexEntry,
    iter::{BoxedLDIter, BoxedLIter},
};
use std::ops::Range;

pub struct WindowTimeSemantics {
    pub(super) semantics: BaseTimeSemantics,
    pub(super) window: Range<i64>,
}

impl WindowTimeSemantics {
    pub fn window(self, w: Range<i64>) -> Self {
        let start = self.window.start.max(w.start);
        let end = self.window.end.min(w.end).max(start);
        WindowTimeSemantics {
            window: start..end,
            ..self
        }
    }
}

impl NodeTimeSemanticsOps for WindowTimeSemantics {
    fn node_earliest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> Option<i64> {
        self.semantics
            .node_earliest_time_window(node, view, self.window.clone())
    }

    fn node_latest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> Option<i64> {
        self.semantics
            .node_latest_time_window(node, view, self.window.clone())
    }

    fn node_earliest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> Option<i64> {
        self.semantics.node_earliest_time_window(node, view, w)
    }

    fn node_latest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> Option<i64> {
        self.semantics.node_latest_time_window(node, view, w)
    }

    fn node_history<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> BoxedLIter<'graph, i64> {
        self.semantics
            .node_history_window(node, view, self.window.clone())
    }

    fn node_history_window<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, i64> {
        self.semantics.node_history_window(node, view, w)
    }

    fn node_updates<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, Vec<(usize, Prop)>)> {
        self.semantics
            .node_updates_window(node, view, self.window.clone())
    }

    fn node_updates_window<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, Vec<(usize, Prop)>)> {
        self.semantics.node_updates_window(node, view, w)
    }

    fn node_valid<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> bool {
        self.semantics
            .node_valid_window(node, view, self.window.clone())
    }

    fn node_valid_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> bool {
        self.semantics.node_valid_window(node, view, w)
    }

    fn node_tprop_iter<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        prop_id: usize,
    ) -> BoxedLDIter<'graph, (TimeIndexEntry, Prop)> {
        self.semantics
            .node_tprop_iter_window(node, view, prop_id, self.window.clone())
    }

    fn node_tprop_iter_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        w: Range<i64>,
    ) -> BoxedLDIter<'graph, (TimeIndexEntry, Prop)> {
        self.semantics
            .node_tprop_iter_window(node, view, prop_id, w)
    }

    fn node_tprop_last_at<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        t: TimeIndexEntry,
    ) -> Option<(TimeIndexEntry, Prop)> {
        self.semantics
            .node_tprop_last_at_window(node, view, prop_id, t, self.window.clone())
    }

    fn node_tprop_last_at_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        t: TimeIndexEntry,
        w: Range<i64>,
    ) -> Option<(TimeIndexEntry, Prop)> {
        self.semantics
            .node_tprop_last_at_window(node, view, prop_id, t, w)
    }
}

impl EdgeTimeSemanticsOps for WindowTimeSemantics {
    fn include_edge_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        edge: EdgeStorageRef,
        view: G,
        w: Range<i64>,
    ) -> bool {
        self.semantics.include_edge_window(edge, view, w)
    }

    fn edge_history<'graph, G: GraphViewOps<'graph>>(
        self,
        edge: EdgeStorageRef<'graph>,
        view: G,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize)> {
        self.semantics.edge_history_window(edge, view, self.window)
    }

    fn edge_history_window<'graph, G: GraphViewOps<'graph>>(
        self,
        edge: EdgeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize)> {
        self.semantics.edge_history_window(edge, view, w)
    }

    fn edge_exploded_count<'graph, G: GraphViewOps<'graph>>(
        &self,
        edge: EdgeStorageRef,
        view: G,
    ) -> usize {
        self.semantics
            .edge_exploded_count_window(edge, view, self.window.clone())
    }

    fn edge_exploded_count_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        edge: EdgeStorageRef,
        view: G,
        w: Range<i64>,
    ) -> usize {
        self.semantics.edge_exploded_count_window(edge, view, w)
    }

    fn edge_exploded<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize)> {
        self.semantics.edge_window_exploded(e, view, self.window)
    }

    fn edge_layers<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
    ) -> BoxedLIter<'graph, usize> {
        self.semantics.edge_window_layers(e, view, self.window)
    }

    fn edge_window_exploded<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize)> {
        self.semantics.edge_window_exploded(e, view, w)
    }

    fn edge_window_layers<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, usize> {
        self.semantics.edge_window_layers(e, view, w)
    }

    fn edge_earliest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
    ) -> Option<i64> {
        self.semantics
            .edge_earliest_time_window(e, view, self.window.clone())
    }

    fn edge_earliest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
        w: Range<i64>,
    ) -> Option<i64> {
        self.semantics.edge_earliest_time_window(e, view, w)
    }

    fn edge_exploded_earliest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
        t: TimeIndexEntry,
        layer: usize,
    ) -> Option<i64> {
        self.semantics
            .edge_exploded_earliest_time_window(e, view, t, layer, self.window.clone())
    }

    fn edge_exploded_earliest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
        t: TimeIndexEntry,
        layer: usize,
        w: Range<i64>,
    ) -> Option<i64> {
        self.semantics
            .edge_exploded_earliest_time_window(e, view, t, layer, w)
    }

    fn edge_latest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
    ) -> Option<i64> {
        self.semantics
            .edge_latest_time_window(e, view, self.window.clone())
    }

    fn edge_latest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
        w: Range<i64>,
    ) -> Option<i64> {
        self.semantics.edge_latest_time_window(e, view, w)
    }

    fn edge_exploded_latest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
        t: TimeIndexEntry,
        layer: usize,
    ) -> Option<i64> {
        self.semantics
            .edge_exploded_latest_time_window(e, view, t, layer, self.window.clone())
    }

    fn edge_exploded_latest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
        t: TimeIndexEntry,
        layer: usize,
        w: Range<i64>,
    ) -> Option<i64> {
        self.semantics
            .edge_exploded_latest_time_window(e, view, t, layer, w)
    }

    fn edge_deletion_history<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize)> {
        self.semantics
            .edge_deletion_history_window(e, view, self.window)
    }

    fn edge_deletion_history_window<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize)> {
        self.semantics.edge_deletion_history_window(e, view, w)
    }

    fn edge_is_valid<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
    ) -> bool {
        self.semantics
            .edge_is_valid_window(e, view, self.window.clone())
    }

    fn edge_is_valid_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        r: Range<i64>,
    ) -> bool {
        self.semantics.edge_is_valid_window(e, view, r)
    }

    fn edge_is_deleted<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
    ) -> bool {
        self.semantics
            .edge_is_deleted_window(e, view, self.window.clone())
    }

    fn edge_is_deleted_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> bool {
        self.semantics.edge_is_deleted_window(e, view, w)
    }

    fn edge_is_active<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
    ) -> bool {
        self.semantics
            .edge_is_active_window(e, view, self.window.clone())
    }

    fn edge_is_active_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> bool {
        self.semantics.edge_is_active_window(e, view, w)
    }

    fn edge_is_active_exploded<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        t: TimeIndexEntry,
        layer: usize,
    ) -> bool {
        self.semantics
            .edge_is_active_exploded_window(e, view, t, layer, self.window.clone())
    }

    fn edge_is_active_exploded_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        t: TimeIndexEntry,
        layer: usize,
        w: Range<i64>,
    ) -> bool {
        self.semantics
            .edge_is_active_exploded_window(e, view, t, layer, w)
    }

    fn edge_is_valid_exploded<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        t: TimeIndexEntry,
        layer: usize,
    ) -> bool {
        self.semantics
            .edge_is_valid_exploded_window(e, view, t, layer, self.window.clone())
    }

    fn edge_is_valid_exploded_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        t: TimeIndexEntry,
        layer: usize,
        w: Range<i64>,
    ) -> bool {
        self.semantics
            .edge_is_valid_exploded_window(e, view, t, layer, w)
    }

    fn edge_exploded_deletion<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        t: TimeIndexEntry,
        layer: usize,
    ) -> Option<TimeIndexEntry> {
        self.semantics
            .edge_exploded_deletion_window(e, view, t, layer, self.window.clone())
    }

    fn edge_exploded_deletion_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        t: TimeIndexEntry,
        layer: usize,
        w: Range<i64>,
    ) -> Option<TimeIndexEntry> {
        self.semantics
            .edge_exploded_deletion_window(e, view, t, layer, w)
    }

    fn temporal_edge_prop_exploded<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        t: TimeIndexEntry,
        layer_id: usize,
    ) -> Option<Prop> {
        self.semantics
            .temporal_edge_prop_exploded(e, view, prop_id, t, layer_id)
    }

    fn temporal_edge_prop_last_at<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        t: TimeIndexEntry,
    ) -> Option<Prop> {
        self.semantics
            .temporal_edge_prop_last_at_window(e, view, prop_id, t, self.window.clone())
    }

    fn temporal_edge_prop_last_at_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        t: TimeIndexEntry,
        w: Range<i64>,
    ) -> Option<Prop> {
        self.semantics
            .temporal_edge_prop_last_at_window(e, view, prop_id, t, w)
    }

    fn temporal_edge_prop_hist<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        prop_id: usize,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize, Prop)> {
        self.semantics
            .temporal_edge_prop_hist_window(e, view, prop_id, self.window)
    }

    fn temporal_edge_prop_hist_rev<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        prop_id: usize,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize, Prop)> {
        self.semantics
            .temporal_edge_prop_hist_window_rev(e, view, prop_id, self.window)
    }

    fn temporal_edge_prop_hist_window<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize, Prop)> {
        self.semantics
            .temporal_edge_prop_hist_window(e, view, prop_id, w)
    }

    fn temporal_edge_prop_hist_window_rev<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize, Prop)> {
        self.semantics
            .temporal_edge_prop_hist_window_rev(e, view, prop_id, w)
    }

    fn constant_edge_prop<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        prop_id: usize,
    ) -> Option<Prop> {
        self.semantics
            .constant_edge_prop_window(e, view, prop_id, self.window.clone())
    }

    fn constant_edge_prop_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        w: Range<i64>,
    ) -> Option<Prop> {
        self.semantics
            .constant_edge_prop_window(e, view, prop_id, w)
    }
}
