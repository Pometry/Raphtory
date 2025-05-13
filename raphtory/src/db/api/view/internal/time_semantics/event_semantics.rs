use crate::{
    core::Prop,
    db::api::{
        storage::graph::{
            edges::{edge_ref::EdgeStorageRef, edge_storage_ops::EdgeStorageOps},
            nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
            tprop_storage_ops::TPropOps,
        },
        view::internal::{
            time_semantics::time_semantics_ops::NodeTimeSemanticsOps, EdgeTimeSemanticsOps,
        },
    },
    prelude::GraphViewOps,
};
use itertools::Itertools;
use raphtory_api::{
    core::{
        entities::LayerIds,
        storage::timeindex::{AsTime, TimeIndexEntry, TimeIndexOps},
    },
    iter::{BoxedLDIter, BoxedLIter, IntoDynBoxed, IntoDynDBoxed},
};
use std::ops::Range;

#[derive(Debug, Copy, Clone)]
pub struct EventSemantics;

impl NodeTimeSemanticsOps for EventSemantics {
    fn node_earliest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> Option<i64> {
        node.history(view).first_t()
    }

    fn node_latest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> Option<i64> {
        node.history(view).last_t()
    }

    fn node_earliest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> Option<i64> {
        node.history(view).range_t(w).first_t()
    }

    fn node_latest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> Option<i64> {
        node.history(view).range_t(w).last_t()
    }

    fn node_history<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> BoxedLIter<'graph, i64> {
        node.history(view).iter_t().into_dyn_boxed()
    }

    fn node_history_window<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, i64> {
        node.history(view).range_t(w).iter_t().into_dyn_boxed()
    }

    fn node_updates<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        _view: G,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, Vec<(usize, Prop)>)> {
        node.temp_prop_rows()
            .map(|(t, row)| {
                (
                    t,
                    row.into_iter()
                        .filter_map(|(id, prop)| Some((id, prop?)))
                        .collect(),
                )
            })
            .into_dyn_boxed()
    }

    fn node_updates_window<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        _view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, Vec<(usize, Prop)>)> {
        node.temp_prop_rows_window(TimeIndexEntry::range(w))
            .map(|(t, row)| {
                (
                    t,
                    row.into_iter()
                        .filter_map(|(id, prop)| Some((id, prop?)))
                        .collect(),
                )
            })
            .into_dyn_boxed()
    }

    fn node_valid<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> bool {
        !node.history(view).is_empty()
    }

    fn node_valid_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> bool {
        node.history(view).active_t(w)
    }

    fn node_tprop_iter<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        _view: G,
        prop_id: usize,
    ) -> BoxedLDIter<'graph, (TimeIndexEntry, Prop)> {
        let prop = node.tprop(prop_id);
        prop.iter().into_dyn_dboxed()
    }

    fn node_tprop_iter_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        _view: G,
        prop_id: usize,
        w: Range<i64>,
    ) -> BoxedLDIter<'graph, (TimeIndexEntry, Prop)> {
        let prop = node.tprop(prop_id);
        prop.iter_window(TimeIndexEntry::range(w)).into_dyn_dboxed()
    }

    fn node_tprop_last_at<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        _view: G,
        prop_id: usize,
        t: TimeIndexEntry,
    ) -> Option<(TimeIndexEntry, Prop)> {
        let prop = node.tprop(prop_id);
        prop.last_before(t.next())
    }

    fn node_tprop_last_at_window<'graph, G: GraphViewOps<'graph>>(
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
    fn include_edge_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        edge: EdgeStorageRef,
        view: G,
        layer_ids: &LayerIds,
        w: Range<i64>,
    ) -> bool {
        edge.filtered_additions_iter(&view, layer_ids)
            .any(|(_, additions)| additions.active_t(w.clone()))
            || edge
                .filtered_deletions_iter(&view, layer_ids)
                .any(|(_, deletions)| deletions.active_t(w.clone()))
    }

    fn edge_history<'graph, G: GraphViewOps<'graph>>(
        self,
        edge: EdgeStorageRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize)> {
        edge.filtered_additions_iter(view, layer_ids)
            .map(|(layer_id, additions)| additions.iter().map(move |t| (t, layer_id)))
            .kmerge()
            .into_dyn_boxed()
    }

    fn edge_history_window<'graph, G: GraphViewOps<'graph>>(
        self,
        edge: EdgeStorageRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize)> {
        edge.filtered_additions_iter(view, layer_ids)
            .map(move |(layer_id, additions)| {
                additions
                    .range_t(w.clone())
                    .iter()
                    .map(move |t| (t, layer_id))
            })
            .kmerge()
            .into_dyn_boxed()
    }

    fn edge_exploded_count<'graph, G: GraphViewOps<'graph>>(
        &self,
        edge: EdgeStorageRef,
        view: G,
    ) -> usize {
        edge.filtered_additions_iter(&view, view.layer_ids())
            .map(|(_, additions)| additions.len())
            .sum()
    }

    fn edge_exploded_count_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        edge: EdgeStorageRef,
        view: G,
        w: Range<i64>,
    ) -> usize {
        edge.filtered_additions_iter(&view, view.layer_ids())
            .map(|(_, additions)| additions.range_t(w.clone()).len())
            .sum()
    }

    fn edge_exploded<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize)> {
        self.edge_history(e, view, layer_ids)
    }

    fn edge_layers<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
    ) -> BoxedLIter<'graph, usize> {
        if view.edge_history_filtered() {
            e.filtered_updates_iter(view, layer_ids)
                .filter_map(move |(layer_id, additions, deletions)| {
                    if additions.is_empty() && deletions.is_empty() {
                        None
                    } else {
                        Some(layer_id)
                    }
                })
                .into_dyn_boxed()
        } else {
            e.layer_ids_iter(layer_ids).into_dyn_boxed()
        }
    }

    fn edge_window_exploded<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize)> {
        self.edge_history_window(e, view, layer_ids, w)
    }

    fn edge_window_layers<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, usize> {
        e.filtered_updates_iter(view, layer_ids)
            .filter_map(move |(layer_id, additions, deletions)| {
                (additions.active_t(w.clone()) || deletions.active_t(w.clone())).then_some(layer_id)
            })
            .into_dyn_boxed()
    }

    fn edge_earliest_time<'graph, G: GraphViewOps<'graph>>(
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

    fn edge_earliest_time_window<'graph, G: GraphViewOps<'graph>>(
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

    fn edge_exploded_earliest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
        t: TimeIndexEntry,
        layer: usize,
    ) -> Option<i64> {
        view.filter_edge_history(e.eid().with_layer(layer), t, view.layer_ids())
            .then_some(t.t())
    }

    fn edge_exploded_earliest_time_window<'graph, G: GraphViewOps<'graph>>(
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

    fn edge_latest_time<'graph, G: GraphViewOps<'graph>>(
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

    fn edge_latest_time_window<'graph, G: GraphViewOps<'graph>>(
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

    fn edge_exploded_latest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
        t: TimeIndexEntry,
        layer: usize,
    ) -> Option<i64> {
        self.edge_exploded_earliest_time(e, view, t, layer)
    }

    fn edge_exploded_latest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
        t: TimeIndexEntry,
        layer: usize,
        w: Range<i64>,
    ) -> Option<i64> {
        self.edge_exploded_earliest_time_window(e, view, t, layer, w)
    }

    fn edge_deletion_history<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize)> {
        e.filtered_deletions_iter(view, layer_ids)
            .map(|(layer_id, t)| t.iter().map(move |t| (t, layer_id)))
            .kmerge()
            .into_dyn_boxed()
    }

    fn edge_deletion_history_window<'graph, G: GraphViewOps<'graph>>(
        self,
        edge: EdgeStorageRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize)> {
        edge.filtered_deletions_iter(view, layer_ids)
            .map(move |(layer_id, additions)| {
                additions
                    .range_t(w.clone())
                    .iter()
                    .map(move |t| (t, layer_id))
            })
            .kmerge()
            .into_dyn_boxed()
    }

    /// An edge is valid with event semantics if it has at least one addition event in the current view
    fn edge_is_valid<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
    ) -> bool {
        e.filtered_additions_iter(&view, view.layer_ids())
            .any(|(_, additions)| !additions.is_empty())
    }

    /// An edge is valid in a window with event semantics if it has at least one addition event in the current view in the window
    fn edge_is_valid_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> bool {
        e.filtered_additions_iter(&view, view.layer_ids())
            .any(|(_, additions)| !additions.range_t(w.clone()).is_empty())
    }

    /// An edge is deleted with event semantics if it has at least one deletion event in the current view
    fn edge_is_deleted<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
    ) -> bool {
        e.filtered_deletions_iter(&view, view.layer_ids())
            .any(|(_, deletions)| !deletions.is_empty())
    }

    /// An edge is deleted in a window with event semantics if it has at least one deletion event in the current view in the window
    fn edge_is_deleted_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> bool {
        e.filtered_deletions_iter(&view, view.layer_ids())
            .any(|(_, deletions)| !deletions.range_t(w.clone()).is_empty())
    }

    /// An edge is valid with event semantics if it has at least one event in the current view
    fn edge_is_active<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
    ) -> bool {
        self.edge_is_valid(e, &view) || self.edge_is_deleted(e, &view)
    }

    /// An edge is active in a window with event semantics if it has at least one event in the current view in the window
    fn edge_is_active_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> bool {
        self.edge_is_valid_window(e, &view, w.clone()) || self.edge_is_deleted_window(e, &view, w)
    }

    fn edge_is_active_exploded<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        t: TimeIndexEntry,
        layer: usize,
    ) -> bool {
        view.filter_edge_history(e.eid().with_layer(layer), t, view.layer_ids())
    }

    fn edge_is_active_exploded_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        t: TimeIndexEntry,
        layer: usize,
        w: Range<i64>,
    ) -> bool {
        w.contains(&t.t())
            && view.filter_edge_history(e.eid().with_layer(layer), t, view.layer_ids())
    }

    /// An exploded edge is valid with event semantics if it is active
    /// (i.e., it's corresponding event is part of the view)
    fn edge_is_valid_exploded<'graph, G: GraphViewOps<'graph>>(
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
    fn edge_is_valid_exploded_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef<'graph>,
        view: G,
        t: TimeIndexEntry,
        layer: usize,
        w: Range<i64>,
    ) -> bool {
        self.edge_is_active_exploded_window(e, view, t, layer, w)
    }

    fn edge_exploded_deletion<'graph, G: GraphViewOps<'graph>>(
        &self,
        _e: EdgeStorageRef<'graph>,
        _view: G,
        _t: TimeIndexEntry,
        _layer: usize,
    ) -> Option<TimeIndexEntry> {
        None
    }

    fn edge_exploded_deletion_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        _e: EdgeStorageRef<'graph>,
        _view: G,
        _t: TimeIndexEntry,
        _layer: usize,
        _w: Range<i64>,
    ) -> Option<TimeIndexEntry> {
        None
    }

    fn temporal_edge_prop_exploded<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
        prop_id: usize,
        t: TimeIndexEntry,
        layer_id: usize,
    ) -> Option<Prop> {
        let eid = e.eid();
        if view.filter_edge_history(eid.with_layer(layer_id), t, view.layer_ids()) {
            e.temporal_prop_layer(layer_id, prop_id).at(&t)
        } else {
            None
        }
    }

    fn temporal_edge_prop_exploded_last_at<'graph, G: GraphViewOps<'graph>>(
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

    fn temporal_edge_prop_exploded_last_at_window<'graph, G: GraphViewOps<'graph>>(
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

    fn temporal_edge_prop_last_at<'graph, G: GraphViewOps<'graph>>(
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

    fn temporal_edge_prop_last_at_window<'graph, G: GraphViewOps<'graph>>(
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

    fn temporal_edge_prop_hist<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        prop_id: usize,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize, Prop)> {
        e.filtered_temporal_prop_iter(prop_id, view, layer_ids)
            .map(|(layer_id, prop)| prop.iter().map(move |(t, v)| (t, layer_id, v)))
            .kmerge_by(|(t1, _, _), (t2, _, _)| t1 <= t2)
            .into_dyn_boxed()
    }

    fn temporal_edge_prop_hist_rev<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        prop_id: usize,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize, Prop)> {
        e.filtered_temporal_prop_iter(prop_id, view, layer_ids)
            .map(|(layer_id, prop)| prop.iter().rev().map(move |(t, v)| (t, layer_id, v)))
            .kmerge_by(|(t1, _, _), (t2, _, _)| t1 >= t2)
            .into_dyn_boxed()
    }

    fn temporal_edge_prop_hist_window<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        prop_id: usize,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize, Prop)> {
        e.filtered_temporal_prop_iter(prop_id, view, layer_ids)
            .map(move |(layer_id, prop)| {
                prop.iter_window(TimeIndexEntry::range(w.clone()))
                    .map(move |(t, v)| (t, layer_id, v))
            })
            .kmerge_by(|(t1, _, _), (t2, _, _)| t1 <= t2)
            .into_dyn_boxed()
    }

    fn temporal_edge_prop_hist_window_rev<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        prop_id: usize,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize, Prop)> {
        e.filtered_temporal_prop_iter(prop_id, view, layer_ids)
            .map(move |(layer_id, prop)| {
                prop.iter_window(TimeIndexEntry::range(w.clone()))
                    .rev()
                    .map(move |(t, v)| (t, layer_id, v))
            })
            .kmerge_by(|(t1, _, _), (t2, _, _)| t1 >= t2)
            .into_dyn_boxed()
    }

    fn constant_edge_prop<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
        prop_id: usize,
    ) -> Option<Prop> {
        let layer_filter =
            |layer| !view.edge_history_filtered() || !e.filtered_additions(layer, &view).is_empty();

        let layer_ids = view.layer_ids();
        match layer_ids {
            LayerIds::None => return None,
            LayerIds::All => match view.unfiltered_num_layers() {
                0 => return None,
                1 => {
                    return if layer_filter(0) {
                        e.constant_prop_layer(0, prop_id)
                    } else {
                        None
                    }
                }
                _ => {}
            },
            LayerIds::One(layer_id) => {
                return if layer_filter(*layer_id) {
                    e.constant_prop_layer(*layer_id, prop_id)
                } else {
                    None
                }
            }
            _ => {}
        };
        let mut values = e
            .constant_prop_iter(layer_ids, prop_id)
            .filter(|(layer, _)| layer_filter(*layer))
            .map(|(layer, v)| (view.get_layer_name(layer), v))
            .peekable();
        if values.peek().is_some() {
            Some(Prop::map(values))
        } else {
            None
        }
    }

    fn constant_edge_prop_window<'graph, G: GraphViewOps<'graph>>(
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

        let layer_ids = view.layer_ids();
        match layer_ids {
            LayerIds::None => return None,
            LayerIds::All => match view.unfiltered_num_layers() {
                0 => return None,
                1 => {
                    return if layer_filter(0) {
                        e.constant_prop_layer(0, prop_id)
                    } else {
                        None
                    }
                }
                _ => {}
            },
            LayerIds::One(layer_id) => {
                return if layer_filter(*layer_id) {
                    e.constant_prop_layer(*layer_id, prop_id)
                } else {
                    None
                }
            }
            _ => {}
        };
        let mut values = e
            .constant_prop_iter(layer_ids, prop_id)
            .filter_map(|(layer, v)| layer_filter(layer).then(|| (view.get_layer_name(layer), v)))
            .peekable();
        if values.peek().is_some() {
            Some(Prop::map(values))
        } else {
            None
        }
    }
}
