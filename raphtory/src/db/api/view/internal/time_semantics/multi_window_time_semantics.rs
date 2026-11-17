//! The union of the windowed views over a set of disjoint time ranges.
//!
//! Every method answers for the whole union by asking [`BaseTimeSemantics`] the
//! corresponding `_window` question once per range. Iterators concatenate the
//! per-range streams and stay sorted, because [`TimeRanges`] keeps its ranges
//! sorted and pairwise disjoint, so no event is reported twice and none arrives
//! out of order. Activity and existence are an `any` over the ranges, counts
//! sum, and earliest and latest are the first and last range with a value.
//! Validity and deletion follow the base semantics: on an event graph they ask
//! whether a range holds an addition or a deletion, so any range answers; on a
//! persistent graph they are the state at the end of the view, which is where
//! the last range ends. On a persistent graph an edge that stays alive
//! across a gap contributes a boundary event to every range it is alive at the
//! start of, which is exactly what the individual windows report on their own.

use crate::db::api::view::internal::{
    time_semantics::{
        base_time_semantics::BaseTimeSemantics,
        filtered_edge::FilteredEdgeStorageOps,
        time_ranges::{RangeIter, TimeRanges},
        time_semantics_ops::{NodeTimeSemanticsOps, NodeTimeSemanticsWindowOps},
    },
    EdgeTimeSemanticsOps, EdgeTimeSemanticsWindowOps, GraphView,
};
use raphtory_api::core::{
    entities::{properties::prop::Prop, LayerId, LayerIds, ELID},
    storage::timeindex::EventTime,
};
use raphtory_storage::graph::nodes::node_ref::NodeStorageRef;
use std::{iter::Rev, ops::Range, sync::Arc};
use storage::EdgeEntryRef;

#[derive(Clone, Debug)]
pub struct MultiWindowTimeSemantics {
    pub(super) semantics: BaseTimeSemantics,
    /// Invariant: at least two ranges. The constructor in `time_semantics.rs`
    /// collapses fewer than two to `Window`.
    pub(super) windows: TimeRanges,
}

impl MultiWindowTimeSemantics {
    pub fn windows(&self) -> &TimeRanges {
        &self.windows
    }

    /// The ranges of the view, oldest first.
    fn ranges(&self) -> RangeIter {
        self.windows.clone().into_iter()
    }

    /// The ranges of the view, newest first, for the reverse iterators: the
    /// per-range streams only concatenate into a sorted stream if the ranges
    /// themselves come in the same order as the events.
    fn ranges_rev(&self) -> Rev<RangeIter> {
        self.ranges().rev()
    }

    /// Whether a validity or deletion `test` of one window holds for the view.
    /// On a persistent graph it is the state at the end of the view, so the
    /// last range answers; on an event graph it asks whether a window holds
    /// the event, so any range does.
    fn holds(&self, test: impl Fn(Range<EventTime>) -> bool) -> bool {
        match self.semantics {
            BaseTimeSemantics::Persistent(_) => self.windows.last().is_some_and(test),
            BaseTimeSemantics::Event(_) => self.windows.iter().any(test),
        }
    }
}

impl NodeTimeSemanticsOps for MultiWindowTimeSemantics {
    fn node_earliest_time<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> Option<EventTime> {
        self.windows.iter().find_map(|w| {
            self.semantics
                .node_earliest_time_window(node, view.clone(), w.clone())
        })
    }

    fn node_latest_time<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> Option<EventTime> {
        self.windows.iter().rev().find_map(|w| {
            self.semantics
                .node_latest_time_window(node, view.clone(), w.clone())
        })
    }

    fn node_history<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
    ) -> impl Iterator<Item = EventTime> + Send + Sync + 'graph {
        let semantics = self.semantics;
        self.ranges()
            .flat_map(move |w| semantics.node_history_window(node, view.clone(), layer_ids, w))
    }

    fn node_history_rev<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
    ) -> impl Iterator<Item = EventTime> + Send + Sync + 'graph {
        let semantics = self.semantics;
        self.ranges_rev()
            .flat_map(move |w| semantics.node_history_window_rev(node, view.clone(), layer_ids, w))
    }

    fn node_edge_history_count<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> usize {
        self.windows
            .iter()
            .map(|w| {
                self.semantics
                    .node_edge_history_count_window(node, view.clone(), w.clone())
            })
            .sum()
    }

    fn node_edge_history<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
    ) -> impl Iterator<Item = (EventTime, ELID)> + Send + Sync + 'graph {
        let semantics = self.semantics;
        self.ranges()
            .flat_map(move |w| semantics.node_edge_history_window(node, view.clone(), layer_ids, w))
    }

    fn node_edge_history_rev<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
    ) -> impl Iterator<Item = (EventTime, ELID)> + Send + Sync + 'graph {
        let semantics = self.semantics;
        self.ranges_rev().flat_map(move |w| {
            semantics.node_edge_history_rev_window(node, view.clone(), layer_ids, w)
        })
    }

    fn node_updates<'graph, G: GraphView + 'graph>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        prop_ids: Arc<[usize]>,
    ) -> impl Iterator<Item = (EventTime, LayerId, Vec<(usize, Prop)>)> + Send + Sync + 'graph {
        let semantics = self.semantics;
        self.ranges().flat_map(move |w| {
            semantics.node_updates_window(node, view.clone(), w, prop_ids.clone())
        })
    }

    fn node_valid<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> bool {
        // Not `holds`: a node is in the view when any range has it. A persistent
        // window counts edge activity only inside it, so a node in an earlier
        // range need not be in the last.
        self.windows.iter().any(|w| {
            self.semantics
                .node_valid_window(node, view.clone(), w.clone())
        })
    }

    fn node_tprop_iter<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        prop_id: usize,
    ) -> impl Iterator<Item = (EventTime, Prop)> + Send + Sync + 'graph {
        let semantics = self.semantics;
        self.ranges()
            .flat_map(move |w| semantics.node_tprop_iter_window(node, view.clone(), prop_id, w))
    }

    fn node_tprop_iter_rev<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        prop_id: usize,
    ) -> impl Iterator<Item = (EventTime, Prop)> + Send + Sync + 'graph {
        let semantics = self.semantics;
        self.ranges_rev()
            .flat_map(move |w| semantics.node_tprop_iter_window_rev(node, view.clone(), prop_id, w))
    }

    fn node_tprop_last<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        prop_id: usize,
    ) -> Option<(EventTime, Prop)> {
        self.windows.iter().rev().find_map(|w| {
            self.semantics
                .node_tprop_last_window(node, view.clone(), prop_id, w.clone())
        })
    }

    fn node_tprop_last_at<'graph, G: GraphView + 'graph>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        t: EventTime,
    ) -> Option<(EventTime, Prop)> {
        self.windows.iter().rev().find_map(|w| {
            self.semantics
                .node_tprop_last_at_window(node, view.clone(), prop_id, t, w.clone())
        })
    }
}

impl EdgeTimeSemanticsOps for MultiWindowTimeSemantics {
    fn handle_edge_update_filter<G: GraphView>(
        &self,
        t: EventTime,
        eid: ELID,
        view: G,
    ) -> Option<(EventTime, ELID)> {
        self.semantics.handle_edge_update_filter(t, eid, view)
    }

    fn include_edge<G: GraphView>(&self, edge: EdgeEntryRef, view: G, layer_id: LayerId) -> bool {
        self.windows.iter().any(|w| {
            self.semantics
                .include_edge_window(edge, view.clone(), layer_id, w.clone())
        })
    }

    fn include_exploded_edge<G: GraphView>(&self, elid: ELID, t: EventTime, view: G) -> bool {
        self.windows.iter().any(|w| {
            self.semantics
                .include_exploded_edge_window(elid, t, view.clone(), w.clone())
        })
    }

    fn edge_history<'graph, G: GraphView + 'graph>(
        self,
        edge: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
    ) -> impl Iterator<Item = (EventTime, LayerId)> + Send + Sync + 'graph {
        let semantics = self.semantics;
        self.ranges()
            .flat_map(move |w| semantics.edge_history_window(edge, view.clone(), layer_ids, w))
    }

    fn edge_history_rev<'graph, G: GraphView + 'graph>(
        self,
        edge: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
    ) -> impl Iterator<Item = (EventTime, LayerId)> + Send + Sync + 'graph {
        let semantics = self.semantics;
        self.ranges_rev()
            .flat_map(move |w| semantics.edge_history_window_rev(edge, view.clone(), layer_ids, w))
    }

    fn edge_exploded_count<'graph, G: GraphView + 'graph>(
        &self,
        edge: EdgeEntryRef,
        view: G,
    ) -> usize {
        self.windows
            .iter()
            .map(|w| {
                self.semantics
                    .edge_exploded_count_window(edge, view.clone(), w.clone())
            })
            .sum()
    }

    fn edge_exploded<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
    ) -> impl Iterator<Item = (EventTime, LayerId)> + Send + Sync + 'graph {
        let semantics = self.semantics;
        self.ranges()
            .flat_map(move |w| semantics.edge_window_exploded(e, view.clone(), layer_ids, w))
    }

    fn edge_layers<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
    ) -> impl Iterator<Item = LayerId> + Send + Sync + 'graph {
        // Each layer once: kept when the edge is in it in any range.
        let semantics = self.semantics;
        let windows = self.windows;
        e.filtered_layer_ids_iter(view.clone(), layer_ids)
            .filter(move |layer| {
                windows
                    .iter()
                    .any(|w| semantics.include_edge_window(e, view.clone(), *layer, w))
            })
    }

    fn edge_earliest_time<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef,
        view: G,
    ) -> Option<EventTime> {
        self.windows.iter().find_map(|w| {
            self.semantics
                .edge_earliest_time_window(e, view.clone(), w.clone())
        })
    }

    fn edge_exploded_earliest_time<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef,
        view: G,
        t: EventTime,
        layer: LayerId,
    ) -> Option<EventTime> {
        self.windows.iter().find_map(|w| {
            self.semantics
                .edge_exploded_earliest_time_window(e, view.clone(), t, layer, w.clone())
        })
    }

    fn edge_latest_time<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef,
        view: G,
    ) -> Option<EventTime> {
        self.windows.iter().rev().find_map(|w| {
            self.semantics
                .edge_latest_time_window(e, view.clone(), w.clone())
        })
    }

    fn edge_exploded_latest_time<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef,
        view: G,
        t: EventTime,
        layer: LayerId,
    ) -> Option<EventTime> {
        self.windows.iter().rev().find_map(|w| {
            self.semantics
                .edge_exploded_latest_time_window(e, view.clone(), t, layer, w.clone())
        })
    }

    fn edge_deletion_history<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
    ) -> impl Iterator<Item = (EventTime, LayerId)> + Send + Sync + 'graph {
        let semantics = self.semantics;
        self.ranges().flat_map(move |w| {
            semantics.edge_deletion_history_window(e, view.clone(), layer_ids, w)
        })
    }

    fn edge_deletion_history_rev<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
    ) -> impl Iterator<Item = (EventTime, LayerId)> + Send + Sync + 'graph {
        let semantics = self.semantics;
        self.ranges_rev().flat_map(move |w| {
            semantics.edge_deletion_history_window_rev(e, view.clone(), layer_ids, w)
        })
    }

    fn edge_is_valid<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
    ) -> bool {
        self.holds(|w| self.semantics.edge_is_valid_window(e, view.clone(), w))
    }

    fn edge_is_deleted<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
    ) -> bool {
        self.holds(|w| self.semantics.edge_is_deleted_window(e, view.clone(), w))
    }

    fn edge_is_active<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
    ) -> bool {
        self.windows.iter().any(|w| {
            self.semantics
                .edge_is_active_window(e, view.clone(), w.clone())
        })
    }

    fn edge_is_active_exploded<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        t: EventTime,
        layer: LayerId,
    ) -> bool {
        self.windows.iter().any(|w| {
            self.semantics
                .edge_is_active_exploded_window(e, view.clone(), t, layer, w.clone())
        })
    }

    fn edge_is_valid_exploded<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        t: EventTime,
        layer: LayerId,
    ) -> bool {
        self.holds(|w| {
            self.semantics
                .edge_is_valid_exploded_window(e, view.clone(), t, layer, w)
        })
    }

    /// The first deletion of this event that the view can see: the first range
    /// that sees one, as each window reports it on its own.
    fn edge_exploded_deletion<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        t: EventTime,
        layer: LayerId,
    ) -> Option<EventTime> {
        self.windows.iter().find_map(|w| {
            self.semantics
                .edge_exploded_deletion_window(e, view.clone(), t, layer, w.clone())
        })
    }

    fn temporal_edge_prop_exploded<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        prop_id: usize,
        t: EventTime,
        layer_id: LayerId,
    ) -> Option<Prop> {
        self.windows.iter().find_map(|w| {
            self.semantics.temporal_edge_prop_exploded_window(
                e,
                view.clone(),
                prop_id,
                t,
                layer_id,
                w.clone(),
            )
        })
    }

    fn temporal_edge_prop_exploded_last_at<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        edge_time: EventTime,
        layer_id: LayerId,
        prop_id: usize,
        at: EventTime,
    ) -> Option<Prop> {
        self.windows.iter().rev().find_map(|w| {
            self.semantics.temporal_edge_prop_exploded_last_at_window(
                e,
                view.clone(),
                edge_time,
                layer_id,
                prop_id,
                at,
                w.clone(),
            )
        })
    }

    fn temporal_edge_prop_last_at<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        prop_id: usize,
        t: EventTime,
    ) -> Option<Prop> {
        self.windows.iter().rev().find_map(|w| {
            self.semantics
                .temporal_edge_prop_last_at_window(e, view.clone(), prop_id, t, w.clone())
        })
    }

    fn temporal_edge_prop_last<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        prop_id: usize,
    ) -> Option<Prop> {
        self.windows.iter().rev().find_map(|w| {
            self.semantics
                .temporal_edge_prop_last_window(e, view.clone(), prop_id, w.clone())
        })
    }

    fn temporal_edge_prop_hist<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        prop_id: usize,
    ) -> impl Iterator<Item = (EventTime, LayerId, Prop)> + Send + Sync + 'graph {
        let semantics = self.semantics;
        self.ranges().flat_map(move |w| {
            semantics.temporal_edge_prop_hist_window(e, view.clone(), layer_ids, prop_id, w)
        })
    }

    fn temporal_edge_prop_hist_rev<'graph, G: GraphView + 'graph>(
        self,
        e: EdgeEntryRef<'graph>,
        view: G,
        layer_ids: &'graph LayerIds,
        prop_id: usize,
    ) -> impl Iterator<Item = (EventTime, LayerId, Prop)> + Send + Sync + 'graph {
        let semantics = self.semantics;
        self.ranges_rev().flat_map(move |w| {
            semantics.temporal_edge_prop_hist_window_rev(e, view.clone(), layer_ids, prop_id, w)
        })
    }

    fn edge_metadata<'graph, G: GraphView + 'graph>(
        &self,
        e: EdgeEntryRef<'graph>,
        view: G,
        prop_id: usize,
    ) -> Option<Prop> {
        self.windows.iter().rev().find_map(|w| {
            self.semantics
                .edge_metadata_window(e, view.clone(), prop_id, w.clone())
        })
    }
}
