use crate::{
    core::Prop,
    db::api::{
        storage::graph::{
            edges::{edge_ref::EdgeStorageRef, edge_storage_ops::EdgeStorageOps},
            nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
            tprop_storage_ops::TPropOps,
        },
        view::internal::{
            time_semantics::{
                event_semantics::EventSemantics, time_semantics_ops::NodeTimeSemanticsOps,
            },
            CoreGraphOps, EdgeTimeSemanticsOps,
        },
    },
    prelude::GraphViewOps,
};
use itertools::Itertools;
use num_traits::Saturating;
use raphtory_api::{
    core::{
        entities::{edges::edge_ref::EdgeRef, LayerIds},
        storage::timeindex::{AsTime, TimeIndexEntry, TimeIndexOps},
    },
    iter::{BoxedLDIter, BoxedLIter, IntoDynBoxed, IntoDynDBoxed},
};
use std::ops::Range;

fn alive_before<
    'a,
    A: TimeIndexOps<'a, IndexType = TimeIndexEntry>,
    D: TimeIndexOps<'a, IndexType = TimeIndexEntry>,
>(
    additions: A,
    deletions: D,
    t: i64,
) -> bool {
    let last_addition_before_start = additions.range_t(i64::MIN..t).last();
    let last_deletion_before_start = deletions.range_t(i64::MIN..t).last();
    last_addition_before_start > last_deletion_before_start
}

fn has_persisted_event<
    'a,
    A: TimeIndexOps<'a, IndexType = TimeIndexEntry>,
    D: TimeIndexOps<'a, IndexType = TimeIndexEntry>,
>(
    additions: A,
    deletions: D,
    t: i64,
) -> bool {
    let active_at_start =
        deletions.active_t(t..t.saturating_add(1)) || additions.active_t(t..t.saturating_add(1));
    !active_at_start && alive_before(additions, deletions, t)
}

fn edge_alive_at_end(e: EdgeStorageRef, t: i64, layer_ids: &LayerIds) -> bool {
    e.updates_iter(layer_ids.clone())
        .any(|(_, additions, deletions)| alive_before(additions, deletions, t))
}

fn edge_alive_at_start(e: EdgeStorageRef, t: i64, layer_ids: &LayerIds) -> bool {
    // The semantics are tricky here, an edge is not alive at the start of the window if the last event at time t is a deletion
    e.updates_iter(layer_ids.clone())
        .any(|(_, additions, deletions)| alive_before(additions, deletions, t.saturating_add(1)))
}

/// Get the last update of a property before `t` (exclusive), taking deletions into account.
/// The update is only returned if the edge was not deleted since.
fn last_prop_value_before<'a>(
    t: TimeIndexEntry,
    props: impl TPropOps<'a>,
    deletions: impl TimeIndexOps<'a, IndexType = TimeIndexEntry>,
) -> Option<(TimeIndexEntry, Prop)> {
    props
        .last_before(t) // inclusive
        .filter(|(last_t, _)| !deletions.active(*last_t..t))
}

/// Gets the potentially persisted property value at a point in time
///
/// Persisted value can only exist if there is no update at time `t` and the edge is not deleted at time `t`
/// and if it exists it is the last value of the property before `t` as computed by `last_prop_value_before`.
fn persisted_prop_value_at<'a>(
    t: i64,
    props: impl TPropOps<'a>,
    deletions: impl TimeIndexOps<'a, IndexType = TimeIndexEntry>,
) -> Option<Prop> {
    if props.clone().active_t(t..t.saturating_add(1)) || deletions.active_t(t..t.saturating_add(1))
    {
        None
    } else {
        last_prop_value_before(TimeIndexEntry::start(t), props, deletions).map(|(_, v)| v)
    }
}

/// Exclude anything from the window that happens before the last deletion at the start of the window
fn interior_window<'a>(
    w: Range<i64>,
    deletions: &impl TimeIndexOps<'a, IndexType = TimeIndexEntry>,
) -> Range<TimeIndexEntry> {
    let start = deletions
        .range_t(w.start..w.start.saturating_add(1))
        .last()
        .map(|t| t.next())
        .unwrap_or(TimeIndexEntry::start(w.start));
    start..TimeIndexEntry::start(w.end)
}

pub struct PersistentSemantics();

impl NodeTimeSemanticsOps for PersistentSemantics {
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
        let history = node.history(view);
        if history.active_t(i64::MIN..w.start) {
            Some(w.start)
        } else {
            history.range_t(w).first_t()
        }
    }

    fn node_latest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> Option<i64> {
        node.history(view)
            .range_t(i64::MIN..w.end)
            .last_t()
            .map(|t| t.max(w.start))
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
                    row.into_iter().filter_map(|(i, v)| Some((i, v?))).collect(),
                )
            })
            .into_dyn_boxed()
    }

    fn node_updates_window<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, Vec<(usize, Prop)>)> {
        let start = w.start;
        let first_row = if node.history(view).active_t(i64::MIN..start) {
            Some(
                node.tprops()
                    .filter_map(|(i, tprop)| {
                        if tprop.active_t(start..start.saturating_add(1)) {
                            None
                        } else {
                            tprop
                                .last_before(TimeIndexEntry::start(start))
                                .map(|(_, v)| (i, v))
                        }
                    })
                    .collect(),
            )
        } else {
            None
        };
        first_row
            .into_iter()
            .map(move |row| (TimeIndexEntry::start(start), row))
            .chain(
                node.temp_prop_rows_window(TimeIndexEntry::range(w))
                    .map(|(t, row)| {
                        (
                            t,
                            row.into_iter().filter_map(|(i, v)| Some((i, v?))).collect(),
                        )
                    }),
            )
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
        node.history(view).active_t(i64::MIN..w.end)
    }

    fn node_tprop_iter<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        _view: G,
        prop_id: usize,
    ) -> BoxedLDIter<'graph, (TimeIndexEntry, Prop)> {
        node.tprop(prop_id).iter().into_dyn_dboxed()
    }

    fn node_tprop_iter_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        _view: G,
        prop_id: usize,
        w: Range<i64>,
    ) -> BoxedLDIter<'graph, (TimeIndexEntry, Prop)> {
        let prop = node.tprop(prop_id);
        let first = if prop.active_t(w.start..w.start.saturating_add(1)) {
            None
        } else {
            prop.last_before(TimeIndexEntry::start(w.start))
                .map(|(t, v)| (t.max(TimeIndexEntry::start(w.start)), v))
        };
        first
            .into_iter()
            .chain(prop.iter_window(TimeIndexEntry::range(w)))
            .into_dyn_dboxed()
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
        if TimeIndexEntry::range(w.clone()).contains(&t) {
            let prop = node.tprop(prop_id);
            prop.last_before(t.next())
                .map(|(t, v)| (t.max(TimeIndexEntry::start(w.start)), v))
        } else {
            None
        }
    }
}

impl EdgeTimeSemanticsOps for PersistentSemantics {
    fn include_edge_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        edge: EdgeStorageRef,
        view: G,
        w: Range<i64>,
    ) -> bool {
        // If an edge has any event in the interior (both end exclusive) of the window it is always included.
        // Additionally, the edge is included if the last event at or before the start of the window was an addition.
        let exclusive_start = w.start.saturating_add(1);
        edge.filtered_updates_iter(&view)
            .any(|(_, additions, deletions)| {
                additions.active_t(exclusive_start..w.end)
                    || deletions.active_t(exclusive_start..w.end)
                    || alive_before(additions, deletions, exclusive_start)
            })
    }

    fn edge_history<'graph, G: GraphViewOps<'graph>>(
        self,
        edge: EdgeStorageRef<'graph>,
        view: G,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize)> {
        EventSemantics.edge_history(edge, view)
    }

    fn edge_history_window<'graph, G: GraphViewOps<'graph>>(
        self,
        edge: EdgeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize)> {
        edge.filtered_updates_iter(view)
            .map(|(layer, additions, deletions)| {
                let window = interior_window(w.clone(), &deletions);
                additions.range(window).iter().map(move |t| (t, layer))
            })
            .kmerge()
            .into_dyn_boxed()
    }

    fn edge_exploded_count<'graph, G: GraphViewOps<'graph>>(
        &self,
        edge: EdgeStorageRef,
        view: G,
    ) -> usize {
        EventSemantics.edge_exploded_count(edge, view)
    }

    fn edge_exploded_count_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        edge: EdgeStorageRef,
        view: G,
        w: Range<i64>,
    ) -> usize {
        if view.edge_history_filtered() {
            edge.updates_iter(view.layer_ids().clone())
                .map(|(layer_id, additions, deletions)| {
                    let actual_window = interior_window(w.clone(), &deletions);
                    let mut len = additions.range(actual_window).len();
                    if has_persisted_event(additions, deletions, w.start) {
                        len += 1
                    }
                    len
                })
                .sum()
        } else {
            edge.updates_iter(view.layer_ids().clone())
                .map(|(layer_id, additions, deletions)| {
                    let actual_window = interior_window(w.clone(), &deletions);
                    let mut len = additions.range(actual_window).len();
                    if has_persisted_event(additions, deletions, w.start) {
                        len += 1
                    }
                    len
                })
                .sum()
        }
    }

    fn edge_exploded<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
    ) -> BoxedLIter<'graph, EdgeRef> {
        todo!()
    }

    fn edge_layers<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
    ) -> BoxedLIter<'graph, EdgeRef> {
        todo!()
    }

    fn edge_window_exploded<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, EdgeRef> {
        todo!()
    }

    fn edge_window_layers<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, EdgeRef> {
        todo!()
    }

    fn edge_earliest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
    ) -> Option<i64> {
        todo!()
    }

    fn edge_earliest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
        w: Range<i64>,
    ) -> Option<i64> {
        todo!()
    }

    fn edge_latest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
    ) -> Option<i64> {
        todo!()
    }

    fn edge_latest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
        w: Range<i64>,
    ) -> Option<i64> {
        todo!()
    }

    fn edge_deletion_history<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize)> {
        todo!()
    }

    fn edge_deletion_history_window<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize)> {
        todo!()
    }

    fn edge_is_valid<'graph, G: GraphViewOps<'graph>>(&self, e: EdgeStorageRef, view: G) -> bool {
        todo!()
    }

    fn edge_is_valid_at_end<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
        t: i64,
    ) -> bool {
        todo!()
    }

    fn temporal_edge_prop_at<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
        prop_id: usize,
        t: TimeIndexEntry,
        layer_id: usize,
    ) -> Option<Prop> {
        todo!()
    }

    fn temporal_edge_prop_last_at<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
        prop_id: usize,
        t: TimeIndexEntry,
    ) -> Option<Prop> {
        todo!()
    }

    fn temporal_edge_prop_last_at_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
        prop_id: usize,
        t: TimeIndexEntry,
        w: Range<i64>,
    ) -> Option<Prop> {
        todo!()
    }

    fn temporal_edge_prop_hist<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        prop_id: usize,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize, Prop)> {
        todo!()
    }

    fn temporal_edge_prop_hist_rev<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        prop_id: usize,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize, Prop)> {
        todo!()
    }

    fn temporal_edge_prop_hist_window<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize, Prop)> {
        todo!()
    }

    fn temporal_edge_prop_hist_window_rev<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize, Prop)> {
        todo!()
    }

    fn constant_edge_prop<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
        prop_id: usize,
    ) -> Option<Prop> {
        todo!()
    }

    fn constant_edge_prop_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
        prop_id: usize,
        w: Range<i64>,
    ) -> Option<Prop> {
        todo!()
    }
}
