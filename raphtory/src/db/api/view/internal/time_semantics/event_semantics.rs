use crate::{
    core::Prop,
    db::api::{
        storage::graph::{
            edges::edge_ref::EdgeStorageRef,
            nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
            tprop_storage_ops::TPropOps,
        },
        view::internal::{
            time_semantics::time_semantics_ops::NodeTimeSemanticsOps, EdgeTimeSemanticsOps,
        },
    },
    prelude::GraphViewOps,
};
use raphtory_api::{
    core::{
        entities::edges::edge_ref::EdgeRef,
        storage::timeindex::{AsTime, TimeIndexEntry, TimeIndexOps},
    },
    iter::{BoxedLDIter, BoxedLIter, IntoDynBoxed, IntoDynDBoxed},
};
use std::ops::Range;

pub struct EventSemantics();

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
        w: Range<i64>,
    ) -> bool {
        todo!()
    }

    fn edge_history<'graph, G: GraphViewOps<'graph>>(
        &'graph self,
        edge: EdgeStorageRef<'graph>,
        view: G,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize)> {
        todo!()
    }

    fn edge_history_window<'graph, G: GraphViewOps<'graph>>(
        &'graph self,
        edge: EdgeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize)> {
        todo!()
    }

    fn edge_exploded_count<'graph, G: GraphViewOps<'graph>>(
        &self,
        edge: EdgeStorageRef,
        view: G,
    ) -> usize {
        todo!()
    }

    fn edge_exploded_count_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        edge: EdgeStorageRef,
        view: G,
        w: Range<i64>,
    ) -> usize {
        todo!()
    }

    fn edge_exploded<'graph, G: GraphViewOps<'graph>>(
        &'graph self,
        e: EdgeStorageRef<'graph>,
        view: G,
    ) -> BoxedLIter<'graph, EdgeRef> {
        todo!()
    }

    fn edge_layers<'graph, G: GraphViewOps<'graph>>(
        &'graph self,
        e: EdgeStorageRef<'graph>,
        view: G,
    ) -> BoxedLIter<'graph, EdgeRef> {
        todo!()
    }

    fn edge_window_exploded<'graph, G: GraphViewOps<'graph>>(
        &'graph self,
        e: EdgeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, EdgeRef> {
        todo!()
    }

    fn edge_window_layers<'graph, G: GraphViewOps<'graph>>(
        &'graph self,
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
        &'graph self,
        e: EdgeStorageRef<'graph>,
        view: G,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize)> {
        todo!()
    }

    fn edge_deletion_history_window<'graph, G: GraphViewOps<'graph>>(
        &'graph self,
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
        id: usize,
        t: TimeIndexEntry,
        layer_id: usize,
    ) -> Option<Prop> {
        todo!()
    }

    fn temporal_edge_prop_last_at<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
        id: usize,
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
        &'graph self,
        e: EdgeStorageRef<'graph>,
        view: G,
        prop_id: usize,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize, Prop)> {
        todo!()
    }

    fn temporal_edge_prop_hist_rev<'graph, G: GraphViewOps<'graph>>(
        &'graph self,
        e: EdgeStorageRef<'graph>,
        view: G,
        prop_id: usize,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize, Prop)> {
        todo!()
    }

    fn temporal_edge_prop_hist_window<'graph, G: GraphViewOps<'graph>>(
        &'graph self,
        e: EdgeStorageRef<'graph>,
        view: G,
        id: usize,
        start: i64,
        end: i64,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize, Prop)> {
        todo!()
    }

    fn temporal_edge_prop_hist_window_rev<'graph, G: GraphViewOps<'graph>>(
        &'graph self,
        e: EdgeStorageRef<'graph>,
        view: G,
        id: usize,
        start: i64,
        end: i64,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize, Prop)> {
        todo!()
    }

    fn constant_edge_prop<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
        id: usize,
    ) -> Option<Prop> {
        todo!()
    }

    fn constant_edge_prop_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
        id: usize,
        w: Range<i64>,
    ) -> Option<Prop> {
        todo!()
    }
}
