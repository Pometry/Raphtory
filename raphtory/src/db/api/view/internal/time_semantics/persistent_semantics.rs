use crate::{
    db::api::view::internal::{
        filtered_edge::{FilteredEdgeTimeIndex, InvertedFilteredEdgeTimeIndex},
        filtered_node::NodeEdgeHistory,
        time_semantics::{
            event_semantics::EventSemantics, filtered_edge::FilteredEdgeStorageOps,
            filtered_node::FilteredNodeStorageOps, time_semantics_ops::NodeTimeSemanticsOps,
        },
        EdgeTimeSemanticsOps, FilterOps, GraphView, InnerFilterOps,
    },
    prelude::GraphViewOps,
};
use ahash::AHashSet;
use either::Either;
use itertools::Itertools;
use raphtory_api::core::{
    entities::{
        properties::{meta::STATIC_GRAPH_LAYER_ID, prop::Prop, tprop::TPropOps},
        LayerId, LayerIds, ELID,
    },
    storage::timeindex::{AsTime, EventTime, MergedTimeIndex, TimeIndexOps},
};
use raphtory_storage::graph::{
    edges::edge_storage_ops::EdgeStorageOps,
    nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
};
use std::{iter, ops::Range};
use storage::{EdgeAdditions, EdgeDeletions, EdgeEntryRef};

fn alive_before<
    'a,
    G: GraphViewOps<'a>,
    TSA: TimeIndexOps<'a, IndexType = EventTime, RangeType = TSA>,
    TSD: TimeIndexOps<'a, IndexType = EventTime, RangeType = TSD>,
>(
    additions: FilteredEdgeTimeIndex<'a, G, TSA>,
    deletions: FilteredEdgeTimeIndex<'a, G, TSD>,
    t: EventTime,
) -> bool {
    last_before(additions, deletions, t).is_some()
}

fn last_before<
    'a,
    G: GraphViewOps<'a>,
    TSA: TimeIndexOps<'a, IndexType = EventTime, RangeType = TSA>,
    TSD: TimeIndexOps<'a, IndexType = EventTime, RangeType = TSD>,
>(
    additions: FilteredEdgeTimeIndex<'a, G, TSA>,
    deletions: FilteredEdgeTimeIndex<'a, G, TSD>,
    t: EventTime,
) -> Option<EventTime> {
    let last_addition_before_start = additions.range(EventTime::MIN..t).last();
    let last_deletion_before_start = deletions
        .merge(additions.invert())
        .range(EventTime::MIN..t)
        .last();
    if last_addition_before_start > last_deletion_before_start {
        last_addition_before_start
    } else {
        None
    }
}

fn persisted_event<
    'a,
    G: GraphViewOps<'a>,
    TSA: TimeIndexOps<'a, IndexType = EventTime, RangeType = TSA>,
    TSD: TimeIndexOps<'a, IndexType = EventTime, RangeType = TSD>,
>(
    additions: FilteredEdgeTimeIndex<'a, G, TSA>,
    deletions: FilteredEdgeTimeIndex<'a, G, TSD>,
    t: EventTime,
) -> Option<EventTime> {
    let active_at_start = deletions.active(t..EventTime::start(t.t().saturating_add(1)))
        || additions
            .unfiltered()
            .active(t..EventTime::start(t.t().saturating_add(1)));
    if active_at_start {
        return None;
    }

    last_before(additions, deletions, t)
}

fn edge_alive_at_end<'graph, G: GraphViewOps<'graph>>(
    e: EdgeEntryRef<'graph>,
    t: EventTime,
    view: G,
) -> bool {
    e.filtered_updates_iter(&view, view.layer_ids())
        .any(|(_, additions, deletions)| alive_before(additions, deletions, t))
}

fn edge_alive_at_start<'graph, G: GraphViewOps<'graph>>(
    e: EdgeEntryRef<'graph>,
    t: EventTime,
    view: G,
) -> bool {
    // The semantics are tricky here, an edge is not alive at the start of the window if the last event at time t is a deletion
    e.filtered_updates_iter(&view, view.layer_ids())
        .any(|(_, additions, deletions)| {
            alive_before(
                additions,
                deletions,
                EventTime::start(t.t().saturating_add(1)),
            )
        })
}

fn node_has_valid_edges<'graph, G: GraphView>(
    history: NodeEdgeHistory<'graph, G>,
    t: EventTime,
) -> bool {
    let mut deleted = AHashSet::new();
    history
        .range(EventTime::MIN..t.next())
        .history_rev()
        .any(|(_, e)| {
            // scan backwards in time over filtered history and keep track of deletions
            let eid = e.edge;
            let layer = e.layer();
            if e.is_deletion() {
                deleted.insert((eid, layer));
                false
            } else {
                !deleted.contains(&(eid, layer))
            }
        })
}

fn merged_deletions<'a, G: GraphView + 'a>(
    e: EdgeEntryRef<'a>,
    view: G,
    layer: usize,
) -> MergedTimeIndex<
    FilteredEdgeTimeIndex<'a, G, EdgeDeletions<'a>>,
    InvertedFilteredEdgeTimeIndex<'a, G, EdgeAdditions<'a>>,
> {
    e.filtered_deletions(layer, view.clone())
        .merge(e.filtered_additions(layer, view).invert())
}
/// Get the last update of a property before `t` (exclusive), taking deletions into account.
/// The update is only returned if the edge was not deleted since.
fn last_prop_value_before<'a, 'b>(
    t: EventTime,
    props: impl TPropOps<'a>,
    deletions: impl TimeIndexOps<'b, IndexType = EventTime>,
) -> Option<(EventTime, Prop)> {
    props
        .last_before(t) // inclusive
        .filter(|(last_t, _)| !deletions.active(*last_t..t))
}

/// Gets the potentially persisted property value at a point in time
///
/// Persisted value can only exist if there is no update at time `t` and the edge is not deleted at time `t`
/// and if it exists it is the last value of the property before `t` as computed by `last_prop_value_before`.
fn persisted_prop_value_at<'a, 'b>(
    t: EventTime,
    props: impl TPropOps<'a>,
    additions: impl TimeIndexOps<'b, IndexType = EventTime>,
    deletions: impl TimeIndexOps<'b, IndexType = EventTime>,
) -> Option<(EventTime, Prop)> {
    if props.active(t..EventTime::start(t.t().saturating_add(1)))
        || deletions.active(t..EventTime::start(t.t().saturating_add(1)))
    {
        None
    } else {
        persisted_secondary_index(t, additions).and_then(|index| {
            last_prop_value_before(t, props, deletions).map(|(_, v)| (t.set_event_id(index), v))
        })
    }
}

fn persisted_secondary_index<'a>(
    t: EventTime,
    additions: impl TimeIndexOps<'a, IndexType = EventTime>,
) -> Option<usize> {
    additions
        .range(t..EventTime::start(t.t().saturating_add(1)))
        .first()
        .or_else(|| additions.range(EventTime::MIN..t).last())
        .map(|t| t.i())
}

/// Exclude anything from the window that happens before the last deletion at the start of the window
fn interior_window<'a>(
    w: Range<EventTime>,
    deletions: &impl TimeIndexOps<'a, IndexType = EventTime>,
) -> Range<EventTime> {
    let start = deletions
        .range(w.start..EventTime::start(w.start.t().saturating_add(1)))
        .last()
        .map(|t| t.next())
        .unwrap_or(w.start);
    start..w.end
}

#[derive(Debug, Copy, Clone)]
pub struct PersistentSemantics;

impl NodeTimeSemanticsOps for PersistentSemantics {
    fn node_earliest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> Option<EventTime> {
        node.history(view).first()
    }

    fn node_latest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> Option<EventTime> {
        node.history(view).last()
    }

    fn node_earliest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<EventTime>,
    ) -> Option<EventTime> {
        let history = node.history(&view);
        let prop_earliest = history.prop_history().range(EventTime::MIN..w.end).first();

        if let Some(prop_earliest) = prop_earliest {
            if prop_earliest <= w.start {
                return Some(w.start);
            }
        }

        if node_has_valid_edges(history.edge_history(), EventTime::end(w.start.t())) {
            return Some(w.start);
        }

        let edge_earliest = history
            .edge_history()
            .range(EventTime::start(w.start.t().saturating_add(1))..w.end)
            .first();
        prop_earliest.into_iter().chain(edge_earliest).min()
    }

    fn node_latest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<EventTime>,
    ) -> Option<EventTime> {
        let history = node.history(&view);
        history
            .range(EventTime::start(w.start.t().saturating_add(1))..w.end)
            .last()
            .or_else(|| {
                (history
                    .prop_history()
                    .active(EventTime::MIN..EventTime::start(w.start.t().saturating_add(1)))
                    || node_has_valid_edges(history.edge_history(), EventTime::end(w.start.t())))
                .then_some(w.start)
            })
    }

    fn node_history<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> impl Iterator<Item = EventTime> + Send + Sync + 'graph {
        node.history(view).iter()
    }

    fn node_history_rev<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> impl Iterator<Item = EventTime> + Send + Sync + 'graph {
        node.history(view).iter_rev()
    }

    fn node_history_window<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = EventTime> + Send + Sync + 'graph {
        node.history(view).range(w).iter()
    }

    fn node_history_window_rev<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = EventTime> + Send + Sync + 'graph {
        node.history(view).range(w).iter_rev()
    }

    fn node_edge_history_count<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> usize {
        EventSemantics.node_edge_history_count(node, view)
    }

    fn node_edge_history_count_window<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<EventTime>,
    ) -> usize {
        EventSemantics.node_edge_history_count_window(node, view, w)
    }

    fn node_edge_history<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> impl Iterator<Item = (EventTime, ELID)> + Send + Sync + 'graph {
        EventSemantics.node_edge_history(node, view)
    }

    fn node_edge_history_window<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = (EventTime, ELID)> + Send + Sync + 'graph {
        EventSemantics.node_edge_history_window(node, view, w)
    }

    fn node_edge_history_rev<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> impl Iterator<Item = (EventTime, ELID)> + Send + Sync + 'graph {
        EventSemantics.node_edge_history_rev(node, view)
    }

    fn node_edge_history_rev_window<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = (EventTime, ELID)> + Send + Sync + 'graph {
        EventSemantics.node_edge_history_rev_window(node, view, w)
    }

    fn node_updates<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        _view: G,
    ) -> impl Iterator<Item = (EventTime, LayerId, Vec<(usize, Prop)>)> + Send + Sync + 'graph {
        node.temp_prop_rows()
            .map(|(t, l, row)| (t, LayerId(l), row))
    }

    fn node_updates_window<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = (EventTime, LayerId, Vec<(usize, Prop)>)> + Send + Sync + 'graph {
        let start = w.start;
        let first_row = if node
            .additions()
            .range(EventTime::MIN..start)
            .iter()
            .next()
            .is_some()
        {
            Some(
                view.node_meta()
                    .temporal_prop_mapper()
                    .ids()
                    .map(|prop_id| (prop_id, node.tprop(prop_id)))
                    .filter_map(|(i, tprop)| {
                        if tprop.active(start..EventTime::start(start.t().saturating_add(1))) {
                            None
                        } else {
                            tprop.last_before(start).map(|(_, v)| (i, v))
                        }
                    })
                    .collect(),
            )
        } else {
            None
        };
        first_row
            .into_iter()
            .map(move |row| (start, LayerId(STATIC_GRAPH_LAYER_ID), row))
            .chain(
                node.temp_prop_rows_range(Some(w))
                    .map(|(t, l, row)| (t, LayerId(l), row)),
            )
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
        w: Range<EventTime>,
    ) -> bool {
        if w.end <= w.start {
            // empty window
            return false;
        }
        let history = node.history(&view);
        history.prop_history().active(EventTime::MIN..w.end)
            || history
                .edge_history()
                .active(EventTime::start(w.start.t().saturating_add(1))..w.end)
            || node_has_valid_edges(history.edge_history(), EventTime::end(w.start.t()))
    }

    fn node_tprop_iter<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        _view: G,
        prop_id: usize,
    ) -> impl Iterator<Item = (EventTime, Prop)> + Send + Sync + 'graph {
        node.tprop(prop_id).iter()
    }

    fn node_tprop_iter_rev<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        _view: G,
        prop_id: usize,
    ) -> impl Iterator<Item = (EventTime, Prop)> + Send + Sync + 'graph {
        node.tprop(prop_id).iter_rev()
    }

    fn node_tprop_iter_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        _view: G,
        prop_id: usize,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = (EventTime, Prop)> + Send + Sync + 'graph {
        let prop = node.tprop(prop_id);
        let first = if prop.active(w.start..EventTime::start(w.start.t().saturating_add(1))) {
            None
        } else {
            prop.last_before(w.start).map(|(t, v)| (t.max(w.start), v))
        };
        first.into_iter().chain(prop.iter_window(w))
    }

    fn node_tprop_iter_window_rev<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        _view: G,
        prop_id: usize,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = (EventTime, Prop)> + Send + Sync + 'graph {
        let prop = node.tprop(prop_id);
        let first = if prop.active(w.start..EventTime::start(w.start.t().saturating_add(1))) {
            None
        } else {
            prop.last_before(w.start).map(|(t, v)| (t.max(w.start), v))
        };
        prop.iter_window_rev(w).chain(first)
    }
    fn node_tprop_last_at<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        _view: G,
        prop_id: usize,
        t: EventTime,
    ) -> Option<(EventTime, Prop)> {
        let prop = node.tprop(prop_id);
        prop.last_before(t.next())
    }

    fn node_tprop_last_at_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        _view: G,
        prop_id: usize,
        t: EventTime,
        w: Range<EventTime>,
    ) -> Option<(EventTime, Prop)> {
        if w.contains(&t) {
            let prop = node.tprop(prop_id);
            prop.last_before(t.next()).map(|(t, v)| (t.max(w.start), v))
        } else {
            None
        }
    }
}

impl EdgeTimeSemanticsOps for PersistentSemantics {
    fn handle_edge_update_filter<G: GraphView>(
        &self,
        t: EventTime,
        eid: ELID,
        view: G,
    ) -> Option<(EventTime, ELID)> {
        let layer = eid.layer();
        // any update for an edge that is globally filtered (i.e., filtered via edge filter, edge layer filter, or node filter) should still be removed
        // updates filtered via exploded edge filter need to be changed to deletions
        if view.layer_ids().contains(&layer)
            && ((!view.internal_nodes_filtered()
                && !view.internal_edge_filtered()
                && !view.internal_edge_layer_filtered())
                || {
                    let edge = view.core_edge(eid.edge);
                    view.internal_filter_edge_layer(edge.as_ref(), layer)
                        && view.internal_filter_edge(edge.as_ref(), view.layer_ids())
                        && view.filter_edge_from_nodes(edge.as_ref())
                })
        {
            if view.internal_filter_exploded_edge(eid, t, view.layer_ids())
                && (!view.internal_nodes_filtered() || {
                    let edge = view.core_edge(eid.edge);
                    view.internal_filter_node(view.core_node(edge.src()).as_ref(), view.layer_ids())
                        && view.internal_filter_node(
                            view.core_node(edge.dst()).as_ref(),
                            view.layer_ids(),
                        )
                })
            {
                Some((t, eid))
            } else {
                Some((t, eid.into_deletion()))
            }
        } else {
            None
        }
    }

    fn include_edge<G: GraphView>(&self, _edge: EdgeEntryRef, _view: G, _layer_id: usize) -> bool {
        // history filtering only maps additions to deletions and thus doesn't filter edges
        true
    }

    fn include_edge_window<G: GraphView>(
        &self,
        edge: EdgeEntryRef,
        view: G,
        layer_id: usize,
        w: Range<EventTime>,
    ) -> bool {
        // If an edge has any event in the interior (both end exclusive) of the window it is always included.
        // Additionally, the edge is included if the last event at or before the start of the window was an addition.
        if w.is_empty() {
            return false;
        }
        let exclusive_start = EventTime::start(w.start.t().saturating_add(1));
        let additions = edge.filtered_additions(layer_id, &view);
        let deletions = edge.filtered_deletions(layer_id, &view);
        additions.unfiltered().active(exclusive_start..w.end)
            || deletions.active(exclusive_start..w.end)
            || alive_before(additions, deletions, exclusive_start)
    }

    fn include_exploded_edge<G: GraphView>(&self, elid: ELID, t: EventTime, view: G) -> bool {
        view.filter_exploded_edge_inner(elid, t)
    }

    fn include_exploded_edge_window<G: GraphView>(
        &self,
        elid: ELID,
        t: EventTime,
        view: G,
        w: Range<EventTime>,
    ) -> bool {
        if t >= w.end {
            return false;
        }
        if t <= w.start && elid.is_deletion() {
            return false;
        }

        if view.filter_exploded_edge_inner(elid, t) {
            if (EventTime::start(w.start.t().saturating_add(1))..w.end).contains(&t) {
                return true;
            }

            let edge = view.core_edge(elid.edge);
            let e = edge.as_ref();
            let layer = elid.layer();
            !e.filtered_deletions(layer, &view)
                .active(t.next()..EventTime::start(w.start.t().saturating_add(1)))
                && !e
                    .additions(layer) // unfiltered as filtered additions act as deletions
                    .active(t.next()..EventTime::start(w.start.t().saturating_add(1)))
        } else {
            false
        }
    }

    fn edge_history<'graph, G: GraphViewOps<'graph>>(
        self,
        edge: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
    ) -> impl Iterator<Item = (EventTime, usize)> + Send + Sync + 'graph {
        EventSemantics.edge_history(edge, view, layer_ids)
    }

    fn edge_history_rev<'graph, G: GraphViewOps<'graph>>(
        self,
        edge: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
    ) -> impl Iterator<Item = (EventTime, usize)> + Send + Sync + 'graph {
        EventSemantics.edge_history_rev(edge, view, layer_ids)
    }

    fn edge_history_window<'graph, G: GraphViewOps<'graph>>(
        self,
        edge: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = (EventTime, usize)> + Send + Sync + 'graph {
        edge.filtered_updates_iter(view, layer_ids)
            .map(|(layer, additions, deletions)| {
                let window = interior_window(w.clone(), &deletions);
                additions.range(window).iter().map(move |t| (t, layer))
            })
            .kmerge()
    }

    fn edge_history_window_rev<'graph, G: GraphViewOps<'graph>>(
        self,
        edge: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = (EventTime, usize)> + Send + Sync + 'graph {
        edge.filtered_updates_iter(view, layer_ids)
            .map(|(layer, additions, deletions)| {
                let window = interior_window(w.clone(), &deletions);
                additions.range(window).iter_rev().map(move |t| (t, layer))
            })
            .kmerge_by(|a, b| a >= b)
    }

    fn edge_exploded_count<'graph, G: GraphViewOps<'graph>>(
        &self,
        edge: EdgeEntryRef,
        view: G,
    ) -> usize {
        EventSemantics.edge_exploded_count(edge, view)
    }

    fn edge_exploded_count_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        edge: EdgeEntryRef,
        view: G,
        w: Range<EventTime>,
    ) -> usize {
        edge.filtered_updates_iter(&view, view.layer_ids())
            .map(|(_, additions, deletions)| {
                let actual_window = interior_window(w.clone(), &deletions);
                let mut len = additions.range(actual_window).len();
                if persisted_event(additions, deletions, w.start).is_some() {
                    len += 1
                }
                len
            })
            .sum()
    }

    fn edge_exploded<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
    ) -> impl Iterator<Item = (EventTime, usize)> + Send + Sync + 'graph {
        EventSemantics.edge_exploded(e, view, layer_ids)
    }

    fn edge_layers<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
    ) -> impl Iterator<Item = usize> + Send + Sync + 'graph {
        e.filtered_layer_ids_iter(view, layer_ids)
    }

    fn edge_window_exploded<'graph, G: GraphViewOps<'graph>>(
        self,
        edge: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = (EventTime, usize)> + Send + Sync + 'graph {
        if w.end <= w.start {
            Either::Left(iter::empty())
        } else {
            Either::Right(
                edge.filtered_updates_iter(view, layer_ids)
                    .map(|(layer, additions, deletions)| {
                        let window = interior_window(w.clone(), &deletions);
                        let first = persisted_event(additions.clone(), deletions, w.start)
                            .map(|EventTime(_, s)| (EventTime(w.start.t(), s), layer));
                        first
                            .into_iter()
                            .chain(additions.range(window).iter().map(move |t| (t, layer)))
                    })
                    .kmerge(),
            )
        }
    }

    fn edge_window_layers<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = usize> + Send + Sync + 'graph {
        let exclusive_start = EventTime::start(w.start.t().saturating_add(1));
        e.filtered_updates_iter(view, layer_ids)
            .filter_map(move |(layer, additions, deletions)| {
                if additions.unfiltered().active(exclusive_start..w.end)
                    || deletions.active(exclusive_start..w.end)
                    || alive_before(additions, deletions, exclusive_start)
                {
                    Some(layer)
                } else {
                    None
                }
            })
    }

    fn edge_earliest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeEntryRef,
        view: G,
    ) -> Option<EventTime> {
        e.filtered_additions_iter(&view, view.layer_ids())
            .flat_map(|(_, additions)| additions.unfiltered().first())
            .chain(
                e.filtered_deletions_iter(&view, view.layer_ids())
                    .flat_map(|(_, deletions)| deletions.first()),
            )
            .min()
    }

    fn edge_earliest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeEntryRef,
        view: G,
        w: Range<EventTime>,
    ) -> Option<EventTime> {
        if edge_alive_at_start(e, w.start, &view) {
            Some(w.start)
        } else {
            e.filtered_updates_iter(&view, view.layer_ids())
                .flat_map(|(_, additions, deletions)| {
                    let deletions = additions.clone().invert().merge(deletions);
                    let window = interior_window(w.clone(), &deletions);
                    additions
                        .unfiltered()
                        .range(window.clone())
                        .first()
                        .into_iter()
                        .chain(deletions.range(window).first())
                })
                .min()
        }
    }

    fn edge_exploded_earliest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeEntryRef,
        view: G,
        t: EventTime,
        layer: usize,
    ) -> Option<EventTime> {
        EventSemantics.edge_exploded_earliest_time(e, view, t, layer)
    }

    fn edge_exploded_earliest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeEntryRef,
        view: G,
        t: EventTime,
        layer: usize,
        w: Range<EventTime>,
    ) -> Option<EventTime> {
        // past the end of the window
        if t >= w.end {
            return None;
        }

        let deletions = e.filtered_deletions(layer, &view);
        let interior = interior_window(w.clone(), &deletions);
        // in the window
        if t >= interior.start {
            return Some(t);
        }

        let additions = e.additions(layer); // unfiltered additions as filtered additions act like deletions
                                            // check if it is the last exploded edge before the window starts and still alive
        if additions.active(t.next()..interior.start.next())
            || deletions.active(t.next()..interior.start.next())
        {
            None
        } else {
            Some(w.start)
        }
    }

    fn edge_latest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeEntryRef,
        view: G,
    ) -> Option<EventTime> {
        e.filtered_additions_iter(&view, view.layer_ids())
            .flat_map(|(_, additions)| additions.unfiltered().last())
            .chain(
                e.filtered_deletions_iter(&view, view.layer_ids())
                    .flat_map(|(_, deletions)| deletions.last()),
            )
            .max()
    }

    fn edge_latest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeEntryRef,
        view: G,
        w: Range<EventTime>,
    ) -> Option<EventTime> {
        let interior_window = EventTime::start(w.start.t().saturating_add(1))..w.end;
        let last_update_in_window = e
            .filtered_additions_iter(&view, view.layer_ids())
            .flat_map(|(_, additions)| additions.unfiltered().range(interior_window.clone()).last())
            .chain(
                e.filtered_deletions_iter(&view, view.layer_ids())
                    .flat_map(|(_, deletions)| deletions.range(interior_window.clone()).last()),
            )
            .max();

        if last_update_in_window.is_some() {
            last_update_in_window
        } else {
            edge_alive_at_start(e, w.start, &view).then_some(w.start)
        }
    }

    fn edge_exploded_latest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeEntryRef,
        view: G,
        t: EventTime,
        layer: usize,
    ) -> Option<EventTime> {
        let deletions = e.filtered_deletions(layer, &view);
        let additions = e.additions(layer); // unfiltered as filtered additions act like deletions
        deletions
            .range(t.next()..EventTime::MAX)
            .first()
            .into_iter()
            .chain(additions.range(t.next()..EventTime::MAX).first())
            .min()
            .or_else(|| view.latest_time_global().map(EventTime::end))
    }

    fn edge_exploded_latest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeEntryRef,
        view: G,
        t: EventTime,
        layer: usize,
        w: Range<EventTime>,
    ) -> Option<EventTime> {
        // past the end of the window
        if t >= w.end {
            return None;
        }

        let additions = e.additions(layer); // unfiltered as filtered additions act like deletions
        let deletions = e.filtered_deletions(layer, &view);

        let w = interior_window(w.clone(), &deletions);
        let end = additions
            .range(t.next()..w.end)
            .first()
            .into_iter()
            .chain(deletions.range(t.next()..w.end).first())
            .min()
            .unwrap_or(w.end);
        // in the window
        if t >= w.start {
            Some(end)
        } else {
            None
        }
    }

    fn edge_deletion_history<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
    ) -> impl Iterator<Item = (EventTime, usize)> + Send + Sync + 'graph {
        e.filtered_updates_iter(view, layer_ids)
            .map(|(layer, additions, deletions)| {
                deletions
                    .merge(additions.invert())
                    .iter()
                    .map(move |t| (t, layer))
            })
            .kmerge()
    }

    fn edge_deletion_history_rev<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
    ) -> impl Iterator<Item = (EventTime, usize)> + Send + Sync + 'graph {
        e.filtered_updates_iter(view, layer_ids)
            .map(|(layer, additions, deletions)| {
                deletions
                    .merge(additions.invert())
                    .iter_rev()
                    .map(move |t| (t, layer))
            })
            .kmerge_by(|(t1, _), (t2, _)| t1 >= t2)
    }

    fn edge_deletion_history_window<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = (EventTime, usize)> + Send + Sync + 'graph {
        // window for deletions has exclusive start as deletions at the start are not considered part of the window
        let w = EventTime::start(w.start.t().saturating_add(1))..w.end;
        e.filtered_updates_iter(view, layer_ids)
            .map(|(layer, additions, deletions)| {
                deletions
                    .merge(additions.invert())
                    .range(w.clone())
                    .iter()
                    .map(move |t| (t, layer))
            })
            .kmerge()
    }

    fn edge_deletion_history_window_rev<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = (EventTime, usize)> + Send + Sync + 'graph {
        // window for deletions has exclusive start as deletions at the start are not considered part of the window
        let w = EventTime::start(w.start.t().saturating_add(1))..w.end;
        e.filtered_updates_iter(view, layer_ids)
            .map(|(layer, additions, deletions)| {
                deletions
                    .merge(additions.invert())
                    .range(w.clone())
                    .iter_rev()
                    .map(move |t| (t, layer))
            })
            .kmerge_by(|(t1, _), (t2, _)| t1 >= t2)
    }

    fn edge_is_valid<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
    ) -> bool {
        edge_alive_at_end(e, EventTime::MAX, view)
    }

    fn edge_is_valid_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        r: Range<EventTime>,
    ) -> bool {
        edge_alive_at_end(e, r.end, view)
    }

    fn edge_is_deleted<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
    ) -> bool {
        !edge_alive_at_end(e, EventTime::MAX, view)
    }

    fn edge_is_deleted_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        w: Range<EventTime>,
    ) -> bool {
        !edge_alive_at_end(e, w.end, view)
    }

    fn edge_is_active<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
    ) -> bool {
        e.additions_iter(view.layer_ids())
            .any(|(_, additions)| !additions.is_empty())
            || e.filtered_deletions_iter(&view, view.layer_ids())
                .any(|(_, deletions)| !deletions.is_empty())
    }

    fn edge_is_active_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        w: Range<EventTime>,
    ) -> bool {
        e.filtered_updates_iter(&view, view.layer_ids())
            .any(|(_, additions, deletions)| {
                let w = interior_window(
                    w.clone(),
                    &deletions.clone().merge(additions.clone().invert()),
                );
                additions.unfiltered().active(w.clone()) || deletions.active(w)
            })
    }

    fn edge_is_active_exploded<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        t: EventTime,
        layer: usize,
    ) -> bool {
        EventSemantics.edge_is_active_exploded(e, view, t, layer)
    }

    fn edge_is_active_exploded_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        t: EventTime,
        layer: usize,
        w: Range<EventTime>,
    ) -> bool {
        EventSemantics.edge_is_active_exploded_window(e, view, t, layer, w)
    }

    /// An exploded edge is valid if it is the last exploded view and the edge is not deleted (i.e., there are no additions or deletions for the edge after t in the layer)
    fn edge_is_valid_exploded<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        t: EventTime,
        layer: usize,
    ) -> bool {
        !e.filtered_deletions(layer, &view)
            .active(t.next()..EventTime::MAX)
            && !e
                .additions(layer) // unfiltered as filtered additions act as deletions
                .active(t.next()..EventTime::MAX)
    }

    /// An exploded edge is valid in a window if it is the last exploded
    /// view in the window and is not deleted before the end of the window
    /// (i.e., there are no additions or deletions for the edge after t in the layer in the window)
    fn edge_is_valid_exploded_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        t: EventTime,
        layer: usize,
        w: Range<EventTime>,
    ) -> bool {
        !e.filtered_deletions(layer, &view).active(t.next()..w.end)
            && !e
                .additions(layer) // unfiltered as filtered additions act as deletions
                .active(t.next()..w.end)
    }

    fn edge_exploded_deletion<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        t: EventTime,
        layer: usize,
    ) -> Option<EventTime> {
        let deletions = merged_deletions(e, &view, layer);
        let next_deletion = deletions.range(t.next()..EventTime::MAX).first()?;
        if let Some(next_addition) = e
            .filtered_additions(layer, &view)
            .range(t.next()..EventTime::MAX)
            .first()
        {
            if next_deletion <= next_addition {
                Some(next_deletion)
            } else {
                None
            }
        } else {
            Some(next_deletion)
        }
    }

    fn edge_exploded_deletion_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        t: EventTime,
        layer: usize,
        w: Range<EventTime>,
    ) -> Option<EventTime> {
        let deletions = merged_deletions(e, &view, layer);
        let next_deletion = deletions.range(t.next()..w.end).first()?;
        if let Some(next_addition) = e
            .filtered_additions(layer, &view)
            .range(t.next()..w.end)
            .first()
        {
            if next_deletion <= next_addition {
                Some(next_deletion)
            } else {
                None
            }
        } else {
            Some(next_deletion)
        }
    }

    fn temporal_edge_prop_exploded<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        prop_id: usize,
        t: EventTime,
        layer_id: usize,
    ) -> Option<Prop> {
        let search_start = merged_deletions(e, &view, layer_id)
            .range(EventTime::MIN..t)
            .last()
            .unwrap_or(EventTime::MIN);
        e.filtered_temporal_prop_layer(layer_id, prop_id, &view)
            .iter_inner_rev(Some(search_start..t.next()))
            .next()
            .map(|(_, v)| v)
    }

    fn temporal_edge_prop_exploded_last_at<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        edge_time: EventTime,
        layer_id: usize,
        prop_id: usize,
        at: EventTime,
    ) -> Option<Prop> {
        if at < edge_time {
            return None;
        }
        let deletion = merged_deletions(e, &view, layer_id)
            .range(edge_time.next()..EventTime::MAX)
            .first()
            .unwrap_or(EventTime::MAX);
        if at < deletion {
            self.temporal_edge_prop_exploded(e, view, prop_id, at, layer_id)
        } else {
            None
        }
    }

    fn temporal_edge_prop_exploded_last_at_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        edge_time: EventTime,
        layer_id: usize,
        prop_id: usize,
        at: EventTime,
        w: Range<EventTime>,
    ) -> Option<Prop> {
        if w.contains(&edge_time) {
            self.temporal_edge_prop_exploded_last_at(e, view, edge_time, layer_id, prop_id, at)
        } else {
            None
        }
    }

    fn temporal_edge_prop_last_at<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        prop_id: usize,
        t: EventTime,
    ) -> Option<Prop> {
        EventSemantics.temporal_edge_prop_last_at(e, view, prop_id, t) // TODO: double check this
    }

    fn temporal_edge_prop_last_at_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeEntryRef,
        view: G,
        prop_id: usize,
        t: EventTime,
        w: Range<EventTime>,
    ) -> Option<Prop> {
        if w.contains(&t) {
            e.filtered_updates_iter(&view, view.layer_ids())
                .filter_map(|(layer, additions, deletions)| {
                    let start = deletions
                        .merge(additions.invert())
                        .range(EventTime::MIN..t.next())
                        .last()
                        .map(|t| t.next())
                        .unwrap_or(EventTime::MIN);
                    e.filtered_temporal_prop_layer(layer, prop_id, &view)
                        .iter_inner_rev(Some(start..t.next()))
                        .next()
                })
                .max_by(|(t1, _), (t2, _)| t1.cmp(t2))
                .map(|(_, v)| v)
        } else {
            None
        }
    }

    fn temporal_edge_prop_hist<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        prop_id: usize,
    ) -> impl Iterator<Item = (EventTime, usize, Prop)> + Send + Sync + 'graph {
        EventSemantics.temporal_edge_prop_hist(e, view, layer_ids, prop_id)
    }

    fn temporal_edge_prop_hist_rev<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        prop_id: usize,
    ) -> impl Iterator<Item = (EventTime, usize, Prop)> + Send + Sync + 'graph {
        EventSemantics.temporal_edge_prop_hist_rev(e, view, layer_ids, prop_id)
    }

    fn temporal_edge_prop_hist_window<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        prop_id: usize,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = (EventTime, usize, Prop)> + Send + Sync + 'graph {
        e.filtered_temporal_prop_iter(prop_id, view.clone(), layer_ids)
            .map(|(layer, props)| {
                let additions = e.filtered_additions(layer, &view);
                let deletions = e.filtered_deletions(layer, &view);
                let merged_deletions = deletions.clone().merge(additions.clone().invert());
                let first_prop =
                    persisted_prop_value_at(w.start, props.clone(), additions, &merged_deletions)
                        .map(|(t, v)| (t, layer, v));
                first_prop.into_iter().chain(
                    props
                        .iter_window(interior_window(w.clone(), &merged_deletions))
                        .map(move |(t, v)| (t, layer, v)),
                )
            })
            .kmerge_by(|(t1, _, _), (t2, _, _)| t1 <= t2)
    }

    fn temporal_edge_prop_hist_window_rev<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        prop_id: usize,
        w: Range<EventTime>,
    ) -> impl Iterator<Item = (EventTime, usize, Prop)> + Send + Sync + 'graph {
        e.filtered_temporal_prop_iter(prop_id, view.clone(), layer_ids)
            .map(|(layer, props)| {
                let additions = e.filtered_additions(layer, &view);
                let deletions = e.filtered_deletions(layer, &view);
                let merged_deletions = deletions.clone().merge(additions.clone().invert());
                let first_prop =
                    persisted_prop_value_at(w.start, props.clone(), additions, &merged_deletions)
                        .map(|(t, v)| (t, layer, v));
                props
                    .iter_inner_rev(Some(interior_window(w.clone(), &merged_deletions)))
                    .map(move |(t, v)| (t, layer, v))
                    .chain(first_prop)
            })
            .kmerge_by(|(t1, _, _), (t2, _, _)| t1 >= t2)
    }

    fn edge_metadata<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeEntryRef,
        view: G,
        prop_id: usize,
    ) -> Option<Prop> {
        let layer_filter = |layer| {
            view.internal_filter_edge_layer(e, layer)
                && (!e.additions(layer).is_empty()
                    || !e.filtered_deletions(layer, &view).is_empty())
        };
        e.filtered_edge_metadata(&view, prop_id, layer_filter)
    }

    fn edge_metadata_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeEntryRef,
        view: G,
        prop_id: usize,
        w: Range<EventTime>,
    ) -> Option<Prop> {
        let exclusive_start = EventTime::start(w.start.t().saturating_add(1));
        let layer_filter = |layer| {
            let additions = e.filtered_additions(layer, &view);
            let deletions = e.filtered_deletions(layer, &view);
            view.internal_filter_edge_layer(e, layer)
                && (additions.active(exclusive_start..w.end)
                    || deletions.active(exclusive_start..w.end)
                    || alive_before(additions, deletions, exclusive_start))
        };
        e.filtered_edge_metadata(&view, prop_id, layer_filter)
    }
}
