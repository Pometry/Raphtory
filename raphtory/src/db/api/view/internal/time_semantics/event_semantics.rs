use crate::db::api::view::internal::{
    time_semantics::{
        filtered_edge::FilteredEdgeStorageOps, filtered_node::FilteredNodeStorageOps,
        time_semantics_ops::NodeTimeSemanticsOps,
    },
    EdgeTimeSemanticsOps, FilterOps, GraphView, InnerFilterOps,
};
use either::Either;
use itertools::Itertools;
use raphtory_api::core::{
    entities::{
        properties::{prop::Prop, tprop::TPropOps},
        LayerIds, ELID,
    },
    storage::timeindex::{AsTime, TimeIndexEntry, TimeIndexOps},
};
use raphtory_storage::graph::{
    edges::{edge_ref::EdgeStorageRef, edge_storage_ops::EdgeStorageOps},
    nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
};
use std::ops::Range;

#[derive(Debug, Copy, Clone)]
pub struct EventSemantics;

impl NodeTimeSemanticsOps for EventSemantics {
    fn node_earliest_time<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> Option<i64> {
        node.history(view).first_t()
    }

    fn node_latest_time<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> Option<i64> {
        node.history(view).last_t()
    }

    fn node_earliest_time_window<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> Option<i64> {
        node.history(view).range_t(w).first_t()
    }

    fn node_latest_time_window<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> Option<i64> {
        node.history(view).range_t(w).last_t()
    }

    fn node_history<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> impl Iterator<Item = i64> + Send + Sync + 'graph {
        node.history(view).iter_t()
    }

    fn node_history_window<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> impl Iterator<Item = i64> + Send + Sync + 'graph {
        node.history(view).range_t(w).iter_t()
    }

    fn node_edge_history_count<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> usize {
        node.history(view).edge_history().len()
    }

    fn node_edge_history_count_window<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> usize {
        node.history(view).range_t(w).edge_history().len()
    }

    fn node_updates<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        _view: G,
    ) -> impl Iterator<Item = (TimeIndexEntry, Vec<(usize, Prop)>)> + Send + Sync + 'graph {
        node.temp_prop_rows().map(|(t, row)| {
            (
                t,
                row.into_iter()
                    .filter_map(|(id, prop)| Some((id, prop?)))
                    .collect(),
            )
        })
    }

    fn node_updates_window<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        _view: G,
        w: Range<i64>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Vec<(usize, Prop)>)> + Send + Sync + 'graph {
        node.temp_prop_rows_window(TimeIndexEntry::range(w))
            .map(|(t, row)| {
                (
                    t,
                    row.into_iter()
                        .filter_map(|(id, prop)| Some((id, prop?)))
                        .collect(),
                )
            })
    }

    fn node_valid<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> bool {
        !node.history(view).is_empty()
    }

    fn node_valid_window<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> bool {
        node.history(view).active_t(w)
    }

    fn node_tprop_iter<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        _view: G,
        prop_id: usize,
    ) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'graph {
        node.tprop(prop_id).iter()
    }

    fn node_tprop_iter_window<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        _view: G,
        prop_id: usize,
        w: Range<i64>,
    ) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'graph {
        node.tprop(prop_id).iter_window(TimeIndexEntry::range(w))
    }

    fn node_tprop_last_at<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        _view: G,
        prop_id: usize,
        t: TimeIndexEntry,
    ) -> Option<(TimeIndexEntry, Prop)> {
        let prop = node.tprop(prop_id);
        prop.last_before(t.next())
    }

    fn node_tprop_last_at_window<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        _view: G,
        prop_id: usize,
        t: TimeIndexEntry,
        w: Range<i64>,
    ) -> Option<(TimeIndexEntry, Prop)> {
        let w = TimeIndexEntry::range(w);
        if w.contains(&t) {
            let prop = node.tprop(prop_id);
            prop.last_before(t.next()).filter(|(t, _)| w.contains(t))
        } else {
            None
        }
    }
}

impl EdgeTimeSemanticsOps for EventSemantics {
    fn handle_edge_update_filter<G: GraphView>(
        &self,
        t: TimeIndexEntry,
        eid: ELID,
        view: G,
    ) -> Option<(TimeIndexEntry, ELID)> {
        view.filter_exploded_edge_inner(eid, t).then_some((t, eid))
    }

    fn include_edge<G: GraphView>(&self, edge: EdgeStorageRef, view: G, layer_id: usize) -> bool {
        !edge.filtered_additions(layer_id, &view).is_empty()
            || !edge.filtered_deletions(layer_id, &view).is_empty()
    }

    fn include_edge_window<G: GraphView>(
        &self,
        edge: EdgeStorageRef,
        view: G,
        layer_id: usize,
        w: Range<i64>,
    ) -> bool {
        edge.filtered_additions(layer_id, &view).active_t(w.clone())
            || edge.filtered_deletions(layer_id, &view).active_t(w)
    }

    fn include_exploded_edge<G: GraphView>(&self, eid: ELID, t: TimeIndexEntry, view: G) -> bool {
        view.layer_ids().contains(&eid.layer())
            && view.internal_filter_exploded_edge(eid, t, view.layer_ids())
            && (view.exploded_filter_independent() || {
                let edge = view.core_edge(eid.edge);
                (view.exploded_edge_filter_includes_edge_layer_filter()
                    || view.internal_filter_edge_layer(edge.as_ref(), eid.layer()))
                    && (view.exploded_edge_filter_includes_edge_filter()
                        || view.edge_layer_filter_includes_edge_filter()
                        || view.internal_filter_edge(edge.as_ref(), view.layer_ids()))
                    && view.filter_edge_from_nodes(edge.as_ref())
            })
    }

    fn include_exploded_edge_window<G: GraphView>(
        &self,
        elid: ELID,
        t: TimeIndexEntry,
        view: G,
        w: Range<i64>,
    ) -> bool {
        w.contains(&t.t()) && self.include_exploded_edge(elid, t, view)
    }

    fn edge_history<'graph, G: GraphView + 'graph>(
        self,
        edge: EdgeStorageRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
    ) -> impl Iterator<Item = (TimeIndexEntry, usize)> + Send + Sync + 'graph {
        edge.filtered_additions_iter(view, layer_ids)
            .map(|(layer_id, additions)| additions.iter().map(move |t| (t, layer_id)))
            .kmerge()
    }

    fn edge_history_window<'graph, G: GraphView + 'graph>(
        self,
        edge: EdgeStorageRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        w: Range<i64>,
    ) -> impl Iterator<Item = (TimeIndexEntry, usize)> + Send + Sync + 'graph {
        edge.filtered_additions_iter(view, layer_ids)
            .map(move |(layer_id, additions)| {
                additions
                    .range_t(w.clone())
                    .iter()
                    .map(move |t| (t, layer_id))
            })
            .kmerge()
    }

    fn edge_exploded_count<'graph, G: GraphView + 'graph>(
        &self,
        edge: EdgeStorageRef,
        view: G,
    ) -> usize {
        edge.filtered_additions_iter(&view, view.layer_ids())
            .map(|(_, additions)| additions.len())
            .sum()
    }

    fn edge_exploded_count_window<'graph, G: GraphView + 'graph>(
        &self,
        edge: EdgeStorageRef,
        view: G,
        w: Range<i64>,
    ) -> usize {
        edge.filtered_additions_iter(&view, view.layer_ids())
            .map(|(_, additions)| additions.range_t(w.clone()).len())
            .sum()
    }

    fn edge_exploded<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
    ) -> impl Iterator<Item = (TimeIndexEntry, usize)> + Send + Sync + 'graph {
        self.edge_history(e, view, layer_ids)
    }

    fn edge_layers<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
    ) -> impl Iterator<Item = usize> + Send + Sync + 'graph {
        if view.filtered() {
            Either::Left(e.filtered_updates_iter(view, layer_ids).filter_map(
                move |(layer_id, additions, deletions)| {
                    if additions.is_empty() && deletions.is_empty() {
                        None
                    } else {
                        Some(layer_id)
                    }
                },
            ))
        } else {
            Either::Right(e.layer_ids_iter(layer_ids))
        }
    }

    fn edge_window_exploded<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        w: Range<i64>,
    ) -> impl Iterator<Item = (TimeIndexEntry, usize)> + Send + Sync + 'graph {
        self.edge_history_window(e, view, layer_ids, w)
    }

    fn edge_window_layers<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        w: Range<i64>,
    ) -> impl Iterator<Item = usize> + Send + Sync + 'graph {
        e.filtered_updates_iter(view, layer_ids).filter_map(
            move |(layer_id, additions, deletions)| {
                (additions.active_t(w.clone()) || deletions.active_t(w.clone())).then_some(layer_id)
            },
        )
    }

    fn edge_earliest_time<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeStorageRef,
        view: G,
    ) -> Option<i64> {
        e.filtered_additions_iter(&view, view.layer_ids())
            .filter_map(|(_, additions)| additions.first_t())
            .chain(
                e.filtered_deletions_iter(&view, view.layer_ids())
                    .filter_map(|(_, deletions)| deletions.first_t()),
            )
            .min()
    }

    fn edge_earliest_time_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeStorageRef,
        view: G,
        w: Range<i64>,
    ) -> Option<i64> {
        e.filtered_additions_iter(&view, view.layer_ids())
            .filter_map(|(_, additions)| additions.range_t(w.clone()).first_t())
            .chain(
                e.filtered_deletions_iter(&view, view.layer_ids())
                    .filter_map(|(_, deletions)| deletions.range_t(w.clone()).first_t()),
            )
            .min()
    }

    fn edge_exploded_earliest_time<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeStorageRef,
        view: G,
        t: TimeIndexEntry,
        layer: usize,
    ) -> Option<i64> {
        view.internal_filter_exploded_edge(e.eid().with_layer(layer), t, view.layer_ids())
            .then_some(t.t())
    }

    fn edge_exploded_earliest_time_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeStorageRef,
        view: G,
        t: TimeIndexEntry,
        layer: usize,
        w: Range<i64>,
    ) -> Option<i64> {
        if !w.contains(&t.t()) {
            return None;
        }
        self.edge_exploded_earliest_time(e, view, t, layer)
    }

    fn edge_latest_time<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeStorageRef,
        view: G,
    ) -> Option<i64> {
        e.filtered_additions_iter(&view, view.layer_ids())
            .filter_map(|(_, additions)| additions.last_t())
            .chain(
                e.filtered_deletions_iter(&view, view.layer_ids())
                    .filter_map(|(_, deletions)| deletions.last_t()),
            )
            .max()
    }

    fn edge_latest_time_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeStorageRef,
        view: G,
        w: Range<i64>,
    ) -> Option<i64> {
        e.filtered_additions_iter(&view, view.layer_ids())
            .filter_map(|(_, additions)| additions.range_t(w.clone()).last_t())
            .chain(
                e.filtered_deletions_iter(&view, view.layer_ids())
                    .filter_map(|(_, deletions)| deletions.range_t(w.clone()).last_t()),
            )
            .max()
    }

    fn edge_exploded_latest_time<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeStorageRef,
        view: G,
        t: TimeIndexEntry,
        layer: usize,
    ) -> Option<i64> {
        self.edge_exploded_earliest_time(e, view, t, layer)
    }

    fn edge_exploded_latest_time_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeStorageRef,
        view: G,
        t: TimeIndexEntry,
        layer: usize,
        w: Range<i64>,
    ) -> Option<i64> {
        self.edge_exploded_earliest_time_window(e, view, t, layer, w)
    }

    fn edge_deletion_history<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
    ) -> impl Iterator<Item = (TimeIndexEntry, usize)> + Send + Sync + 'graph {
        e.filtered_deletions_iter(view, layer_ids)
            .map(|(layer_id, t)| t.iter().map(move |t| (t, layer_id)))
            .kmerge()
    }

    fn edge_deletion_history_window<'graph, G: GraphView + 'graph>(
        self,
        edge: EdgeStorageRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        w: Range<i64>,
    ) -> impl Iterator<Item = (TimeIndexEntry, usize)> + Send + Sync + 'graph {
        edge.filtered_deletions_iter(view, layer_ids)
            .map(move |(layer_id, additions)| {
                additions
                    .range_t(w.clone())
                    .iter()
                    .map(move |t| (t, layer_id))
            })
            .kmerge()
    }

    /// An edge is valid with event semantics if it has at least one addition event in the current view
    fn edge_is_valid<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
    ) -> bool {
        e.filtered_additions_iter(&view, view.layer_ids())
            .any(|(_, additions)| !additions.is_empty())
    }

    /// An edge is valid in a window with event semantics if it has at least one addition event in the current view in the window
    fn edge_is_valid_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> bool {
        e.filtered_additions_iter(&view, view.layer_ids())
            .any(|(_, additions)| !additions.range_t(w.clone()).is_empty())
    }

    /// An edge is deleted with event semantics if it has at least one deletion event in the current view
    fn edge_is_deleted<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
    ) -> bool {
        e.filtered_deletions_iter(&view, view.layer_ids())
            .any(|(_, deletions)| !deletions.is_empty())
    }

    /// An edge is deleted in a window with event semantics if it has at least one deletion event in the current view in the window
    fn edge_is_deleted_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> bool {
        e.filtered_deletions_iter(&view, view.layer_ids())
            .any(|(_, deletions)| !deletions.range_t(w.clone()).is_empty())
    }

    /// An edge is valid with event semantics if it has at least one event in the current view
    fn edge_is_active<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
    ) -> bool {
        self.edge_is_valid(e, &view) || self.edge_is_deleted(e, &view)
    }

    /// An edge is active in a window with event semantics if it has at least one event in the current view in the window
    fn edge_is_active_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> bool {
        self.edge_is_valid_window(e, &view, w.clone()) || self.edge_is_deleted_window(e, &view, w)
    }

    fn edge_is_active_exploded<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        t: TimeIndexEntry,
        layer: usize,
    ) -> bool {
        view.internal_filter_exploded_edge(e.eid().with_layer(layer), t, view.layer_ids())
    }

    fn edge_is_active_exploded_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        t: TimeIndexEntry,
        layer: usize,
        w: Range<i64>,
    ) -> bool {
        w.contains(&t.t())
            && view.internal_filter_exploded_edge(e.eid().with_layer(layer), t, view.layer_ids())
    }

    /// An exploded edge is valid with event semantics if it is active
    /// (i.e., it's corresponding event is part of the view)
    fn edge_is_valid_exploded<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        t: TimeIndexEntry,
        layer: usize,
    ) -> bool {
        self.edge_is_active_exploded(e, view, t, layer)
    }

    /// An exploded edge is valid with event semantics if it is active
    /// (i.e., it's corresponding event is part of the view)
    fn edge_is_valid_exploded_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        t: TimeIndexEntry,
        layer: usize,
        w: Range<i64>,
    ) -> bool {
        self.edge_is_active_exploded_window(e, view, t, layer, w)
    }

    fn edge_exploded_deletion<'graph, G: GraphView + 'graph>(
        &self,
        _e: EdgeStorageRef<'graph>,
        _view: G,
        _t: TimeIndexEntry,
        _layer: usize,
    ) -> Option<TimeIndexEntry> {
        None
    }

    fn edge_exploded_deletion_window<'graph, G: GraphView + 'graph>(
        &self,
        _e: EdgeStorageRef<'graph>,
        _view: G,
        _t: TimeIndexEntry,
        _layer: usize,
        _w: Range<i64>,
    ) -> Option<TimeIndexEntry> {
        None
    }

    fn temporal_edge_prop_exploded<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeStorageRef,
        view: G,
        prop_id: usize,
        t: TimeIndexEntry,
        layer_id: usize,
    ) -> Option<Prop> {
        let eid = e.eid();
        if view.internal_filter_exploded_edge(eid.with_layer(layer_id), t, view.layer_ids()) {
            e.temporal_prop_layer(layer_id, prop_id).at(&t)
        } else {
            None
        }
    }

    fn temporal_edge_prop_exploded_last_at<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        edge_time: TimeIndexEntry,
        layer_id: usize,
        prop_id: usize,
        at: TimeIndexEntry,
    ) -> Option<Prop> {
        if at == edge_time {
            self.temporal_edge_prop_exploded(e, view, prop_id, edge_time, layer_id)
        } else {
            None
        }
    }

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
        if edge_time == at && w.contains(&edge_time.t()) {
            self.temporal_edge_prop_exploded(e, view, prop_id, edge_time, layer_id)
        } else {
            None
        }
    }

    fn temporal_edge_prop_last_at<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        t: TimeIndexEntry,
    ) -> Option<Prop> {
        e.filtered_temporal_prop_iter(prop_id, &view, view.layer_ids())
            .filter_map(|(_, prop)| prop.last_before(t.next()))
            .max_by(|(t1, _), (t2, _)| t1.cmp(t2))
            .map(|(_, v)| v)
    }

    fn temporal_edge_prop_last_at_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        t: TimeIndexEntry,
        w: Range<i64>,
    ) -> Option<Prop> {
        if w.contains(&t.t()) {
            e.filtered_temporal_prop_iter(prop_id, &view, view.layer_ids())
                .filter_map(|(_, prop)| {
                    prop.last_before(t.next())
                        .filter(|(t, _)| w.contains(&t.t()))
                })
                .max_by(|(t1, _), (t2, _)| t1.cmp(t2))
                .map(|(_, v)| v)
        } else {
            None
        }
    }

    fn temporal_edge_prop_hist<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        prop_id: usize,
    ) -> impl Iterator<Item = (TimeIndexEntry, usize, Prop)> + Send + Sync + 'graph {
        e.filtered_temporal_prop_iter(prop_id, view, layer_ids)
            .map(|(layer_id, prop)| prop.iter().map(move |(t, v)| (t, layer_id, v)))
            .kmerge_by(|(t1, _, _), (t2, _, _)| t1 <= t2)
    }

    fn temporal_edge_prop_hist_rev<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        prop_id: usize,
    ) -> impl Iterator<Item = (TimeIndexEntry, usize, Prop)> + Send + Sync + 'graph {
        e.filtered_temporal_prop_iter(prop_id, view, layer_ids)
            .map(|(layer_id, prop)| prop.iter().rev().map(move |(t, v)| (t, layer_id, v)))
            .kmerge_by(|(t1, _, _), (t2, _, _)| t1 >= t2)
    }

    fn temporal_edge_prop_hist_window<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        prop_id: usize,
        w: Range<i64>,
    ) -> impl Iterator<Item = (TimeIndexEntry, usize, Prop)> + Send + Sync + 'graph {
        e.filtered_temporal_prop_iter(prop_id, view, layer_ids)
            .map(move |(layer_id, prop)| {
                prop.iter_window(TimeIndexEntry::range(w.clone()))
                    .map(move |(t, v)| (t, layer_id, v))
            })
            .kmerge_by(|(t1, _, _), (t2, _, _)| t1 <= t2)
    }

    fn temporal_edge_prop_hist_window_rev<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        prop_id: usize,
        w: Range<i64>,
    ) -> impl Iterator<Item = (TimeIndexEntry, usize, Prop)> + Send + Sync + 'graph {
        e.filtered_temporal_prop_iter(prop_id, view, layer_ids)
            .map(move |(layer_id, prop)| {
                prop.iter_window(TimeIndexEntry::range(w.clone()))
                    .rev()
                    .map(move |(t, v)| (t, layer_id, v))
            })
            .kmerge_by(|(t1, _, _), (t2, _, _)| t1 >= t2)
    }

    fn edge_metadata<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeStorageRef,
        view: G,
        prop_id: usize,
    ) -> Option<Prop> {
        let layer_filter = |layer| {
            view.internal_filter_edge_layer(e, layer)
                && !e.filtered_additions(layer, &view).is_empty()
        };
        e.filtered_edge_metadata(&view, prop_id, layer_filter)
    }

    fn edge_metadata_window<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        w: Range<i64>,
    ) -> Option<Prop> {
        let layer_filter = |layer| {
            !e.filtered_additions(layer, &view)
                .range_t(w.clone())
                .is_empty()
        };
        e.filtered_edge_metadata(&view, prop_id, layer_filter)
    }
}
