use crate::db::api::view::internal::{
    time_semantics::{
        base_time_semantics::BaseTimeSemantics, time_semantics_ops::NodeTimeSemanticsOps,
    },
    EdgeTimeSemanticsOps, GraphView,
};
use raphtory_api::core::{
    entities::{properties::prop::Prop, LayerIds},
    storage::timeindex::TimeIndexEntry,
};
use raphtory_storage::graph::{edges::edge_ref::EdgeStorageRef, nodes::node_ref::NodeStorageRef};
use std::ops::Range;

#[derive(Clone, Debug)]
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
    #[inline]
    fn node_earliest_time<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> Option<i64> {
        self.semantics
            .node_earliest_time_window(node, view, self.window.clone())
    }

    #[inline]
    fn node_latest_time<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> Option<i64> {
        self.semantics
            .node_latest_time_window(node, view, self.window.clone())
    }

    #[inline]
    fn node_earliest_time_window<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> Option<i64> {
        self.semantics.node_earliest_time_window(node, view, w)
    }

    #[inline]
    fn node_latest_time_window<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> Option<i64> {
        self.semantics.node_latest_time_window(node, view, w)
    }

    #[inline]
    fn node_history<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> impl Iterator<Item = i64> + Send + Sync + 'graph {
        self.semantics
            .node_history_window(node, view, self.window.clone())
    }

    #[inline]
    fn node_history_window<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> impl Iterator<Item = i64> + Send + Sync + 'graph {
        self.semantics.node_history_window(node, view, w)
    }

    #[inline]
    fn node_edge_history_count<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> usize {
        self.semantics
            .node_edge_history_count_window(node, view, self.window.clone())
    }

    #[inline]
    fn node_edge_history_count_window<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> usize {
        self.semantics.node_edge_history_count_window(node, view, w)
    }

    #[inline]
    fn node_updates<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> impl Iterator<Item = (TimeIndexEntry, Vec<(usize, Prop)>)> + Send + Sync + 'graph {
        self.semantics
            .node_updates_window(node, view, self.window.clone())
    }

    #[inline]
    fn node_updates_window<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Vec<(usize, Prop)>)> + Send + Sync + 'graph {
        self.semantics.node_updates_window(node, view, w)
    }

    #[inline]
    fn node_valid<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> bool {
        self.semantics
            .node_valid_window(node, view, self.window.clone())
    }

    #[inline]
    fn node_valid_window<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> bool {
        self.semantics.node_valid_window(node, view, w)
    }

    #[inline]
    fn node_tprop_iter<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        prop_id: usize,
    ) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'graph {
        self.semantics
            .node_tprop_iter_window(node, view, prop_id, self.window.clone())
    }

    #[inline]
    fn node_tprop_iter_window<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        w: Range<i64>,
    ) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'graph {
        self.semantics
            .node_tprop_iter_window(node, view, prop_id, w)
    }

    #[inline]
    fn node_tprop_last_at<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        t: TimeIndexEntry,
    ) -> Option<(TimeIndexEntry, Prop)> {
        self.semantics
            .node_tprop_last_at_window(node, view, prop_id, t, self.window.clone())
    }

    #[inline]
    fn node_tprop_last_at_window<'graph, G: GraphView + 'graph>(
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
    #[inline]
    fn include_edge_window<'graph, G: GraphView + 'graph>(
        &self,
        edge: EdgeStorageRef,
        view: G,
        layer_ids: &LayerIds,
        w: Range<i64>,
    ) -> bool {
        self.semantics.include_edge_window(edge, view, layer_ids, w)
    }

    #[inline]
    fn edge_history<'graph, G: GraphView + 'graph>(
        self,
        edge: EdgeStorageRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
    ) -> impl Iterator<Item = (TimeIndexEntry, usize)> + Send + Sync + 'graph {
        self.semantics
            .edge_history_window(edge, view, layer_ids, self.window)
    }

    #[inline]
    fn edge_history_window<'graph, G: GraphView + 'graph>(
        self,
        edge: EdgeStorageRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        w: Range<i64>,
    ) -> impl Iterator<Item = (TimeIndexEntry, usize)> + Send + Sync + 'graph {
        self.semantics.edge_history_window(edge, view, layer_ids, w)
    }

    #[inline]
    fn edge_exploded_count<'graph, G: GraphView + 'graph>(
        &self,
        edge: EdgeStorageRef,
        view: G,
    ) -> usize {
        self.semantics
            .edge_exploded_count_window(edge, view, self.window.clone())
    }

    #[inline]
    fn edge_exploded_count_window<'graph, G: GraphView + 'graph>(
        &self,
        edge: EdgeStorageRef,
        view: G,
        w: Range<i64>,
    ) -> usize {
        self.semantics.edge_exploded_count_window(edge, view, w)
    }

    #[inline]
    fn edge_exploded<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
    ) -> impl Iterator<Item = (TimeIndexEntry, usize)> + Send + Sync + 'graph {
        self.semantics
            .edge_window_exploded(e, view, layer_ids, self.window)
    }

    #[inline]
    fn edge_layers<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
    ) -> impl Iterator<Item = usize> + Send + Sync + 'graph {
        self.semantics
            .edge_window_layers(e, view, layer_ids, self.window)
    }

    #[inline]
    fn edge_window_exploded<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        w: Range<i64>,
    ) -> impl Iterator<Item = (TimeIndexEntry, usize)> + Send + Sync + 'graph {
        self.semantics.edge_window_exploded(e, view, layer_ids, w)
    }

    #[inline]
    fn edge_window_layers<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        w: Range<i64>,
    ) -> impl Iterator<Item = usize> + Send + Sync + 'graph {
        self.semantics.edge_window_layers(e, view, layer_ids, w)
    }

    #[inline]
    fn edge_earliest_time<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeStorageRef,
        view: G,
    ) -> Option<i64> {
        self.semantics
            .edge_earliest_time_window(e, view, self.window.clone())
    }

    #[inline]
    fn edge_earliest_time_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeStorageRef,
        view: G,
        w: Range<i64>,
    ) -> Option<i64> {
        self.semantics.edge_earliest_time_window(e, view, w)
    }

    #[inline]
    fn edge_exploded_earliest_time<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeStorageRef,
        view: G,
        t: TimeIndexEntry,
        layer: usize,
    ) -> Option<i64> {
        self.semantics
            .edge_exploded_earliest_time_window(e, view, t, layer, self.window.clone())
    }

    #[inline]
    fn edge_exploded_earliest_time_window<'graph, G: GraphView + 'graph>(
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

    #[inline]
    fn edge_latest_time<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeStorageRef,
        view: G,
    ) -> Option<i64> {
        self.semantics
            .edge_latest_time_window(e, view, self.window.clone())
    }

    #[inline]
    fn edge_latest_time_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeStorageRef,
        view: G,
        w: Range<i64>,
    ) -> Option<i64> {
        self.semantics.edge_latest_time_window(e, view, w)
    }

    #[inline]
    fn edge_exploded_latest_time<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeStorageRef,
        view: G,
        t: TimeIndexEntry,
        layer: usize,
    ) -> Option<i64> {
        self.semantics
            .edge_exploded_latest_time_window(e, view, t, layer, self.window.clone())
    }

    #[inline]
    fn edge_exploded_latest_time_window<'graph, G: GraphView + 'graph>(
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

    #[inline]
    fn edge_deletion_history<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
    ) -> impl Iterator<Item = (TimeIndexEntry, usize)> + Send + Sync + 'graph {
        self.semantics
            .edge_deletion_history_window(e, view, layer_ids, self.window)
    }

    #[inline]
    fn edge_deletion_history_window<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        w: Range<i64>,
    ) -> impl Iterator<Item = (TimeIndexEntry, usize)> + Send + Sync + 'graph {
        self.semantics
            .edge_deletion_history_window(e, view, layer_ids, w)
    }

    #[inline]
    fn edge_is_valid<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
    ) -> bool {
        self.semantics
            .edge_is_valid_window(e, view, self.window.clone())
    }

    #[inline]
    fn edge_is_valid_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        r: Range<i64>,
    ) -> bool {
        self.semantics.edge_is_valid_window(e, view, r)
    }

    #[inline]
    fn edge_is_deleted<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
    ) -> bool {
        self.semantics
            .edge_is_deleted_window(e, view, self.window.clone())
    }

    #[inline]
    fn edge_is_deleted_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> bool {
        self.semantics.edge_is_deleted_window(e, view, w)
    }

    #[inline]
    fn edge_is_active<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
    ) -> bool {
        self.semantics
            .edge_is_active_window(e, view, self.window.clone())
    }

    #[inline]
    fn edge_is_active_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> bool {
        self.semantics.edge_is_active_window(e, view, w)
    }

    #[inline]
    fn edge_is_active_exploded<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        t: TimeIndexEntry,
        layer: usize,
    ) -> bool {
        self.semantics
            .edge_is_active_exploded_window(e, view, t, layer, self.window.clone())
    }

    #[inline]
    fn edge_is_active_exploded_window<'graph, G: GraphView + 'graph>(
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

    #[inline]
    fn edge_is_valid_exploded<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        t: TimeIndexEntry,
        layer: usize,
    ) -> bool {
        self.semantics
            .edge_is_valid_exploded_window(e, view, t, layer, self.window.clone())
    }

    #[inline]
    fn edge_is_valid_exploded_window<'graph, G: GraphView + 'graph>(
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

    #[inline]
    fn edge_exploded_deletion<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        t: TimeIndexEntry,
        layer: usize,
    ) -> Option<TimeIndexEntry> {
        self.semantics
            .edge_exploded_deletion_window(e, view, t, layer, self.window.clone())
    }

    #[inline]
    fn edge_exploded_deletion_window<'graph, G: GraphView + 'graph>(
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

    #[inline]
    fn temporal_edge_prop_exploded<'graph, G: GraphView + 'graph>(
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

    #[inline]
    fn temporal_edge_prop_exploded_last_at<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        edge_time: TimeIndexEntry,
        layer_id: usize,
        prop_id: usize,
        at: TimeIndexEntry,
    ) -> Option<Prop> {
        self.semantics.temporal_edge_prop_exploded_last_at_window(
            e,
            view,
            edge_time,
            layer_id,
            prop_id,
            at,
            self.window.clone(),
        )
    }

    #[inline]
    fn temporal_edge_prop_exploded_last_at_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        edge_time: TimeIndexEntry,
        layer_id: usize,
        prop_id: usize,
        at: TimeIndexEntry,
        w: Range<i64>,
    ) -> Option<Prop> {
        self.semantics.temporal_edge_prop_exploded_last_at_window(
            e, view, edge_time, layer_id, prop_id, at, w,
        )
    }

    #[inline]
    fn temporal_edge_prop_last_at<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        t: TimeIndexEntry,
    ) -> Option<Prop> {
        self.semantics
            .temporal_edge_prop_last_at_window(e, view, prop_id, t, self.window.clone())
    }

    #[inline]
    fn temporal_edge_prop_last_at_window<'graph, G: GraphView + 'graph>(
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

    #[inline]
    fn temporal_edge_prop_hist<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        prop_id: usize,
    ) -> impl Iterator<Item = (TimeIndexEntry, usize, Prop)> + Send + Sync + 'graph {
        self.semantics
            .temporal_edge_prop_hist_window(e, view, layer_ids, prop_id, self.window)
    }

    #[inline]
    fn temporal_edge_prop_hist_rev<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        prop_id: usize,
    ) -> impl Iterator<Item = (TimeIndexEntry, usize, Prop)> + Send + Sync + 'graph {
        self.semantics
            .temporal_edge_prop_hist_window_rev(e, view, layer_ids, prop_id, self.window)
    }

    #[inline]
    fn temporal_edge_prop_hist_window<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        prop_id: usize,
        w: Range<i64>,
    ) -> impl Iterator<Item = (TimeIndexEntry, usize, Prop)> + Send + Sync + 'graph {
        self.semantics
            .temporal_edge_prop_hist_window(e, view, layer_ids, prop_id, w)
    }

    #[inline]
    fn temporal_edge_prop_hist_window_rev<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        prop_id: usize,
        w: Range<i64>,
    ) -> impl Iterator<Item = (TimeIndexEntry, usize, Prop)> + Send + Sync + 'graph {
        self.semantics
            .temporal_edge_prop_hist_window_rev(e, view, layer_ids, prop_id, w)
    }

    #[inline]
    fn constant_edge_prop<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        prop_id: usize,
    ) -> Option<Prop> {
        self.semantics
            .constant_edge_prop_window(e, view, prop_id, self.window.clone())
    }

    #[inline]
    fn constant_edge_prop_window<'graph, G: GraphView + 'graph>(
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
