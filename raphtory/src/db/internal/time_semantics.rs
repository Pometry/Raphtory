use std::ops::Range;

use genawaiter::sync::GenBoxed;

use crate::core::storage::timeindex::TimeIndexOps;
use crate::core::tgraph::edges::edge_ref::EdgeRef;
use crate::core::tgraph::graph::tgraph::InnerTemporalGraph;
use crate::{
    core::tgraph::VID,
    db::view_api::{
        internal::{CoreDeletionOps, CoreGraphOps, TimeSemantics},
        BoxedIter,
    },
    prelude::Prop,
};

impl<const N: usize> TimeSemantics for InnerTemporalGraph<N> {
    fn vertex_earliest_time(&self, v: VID) -> Option<i64> {
        self.node_entry(v)
            .value()
            .and_then(|node| node.timestamps().first())
    }

    fn vertex_latest_time(&self, v: VID) -> Option<i64> {
        self.node_entry(v)
            .value()
            .and_then(|node| node.timestamps().last())
    }

    fn view_start(&self) -> Option<i64> {
        self.earliest_time_global()
    }

    fn view_end(&self) -> Option<i64> {
        self.latest_time_global().map(|t| t + 1) // so it is exclusive
    }

    fn earliest_time_global(&self) -> Option<i64> {
        self.graph_earliest_time()
    }

    fn latest_time_global(&self) -> Option<i64> {
        self.graph_latest_time()
    }

    fn include_vertex_window(&self, v: VID, w: Range<i64>) -> bool {
        self.node_entry(v).timestamps().active(w)
    }

    fn include_edge_window(&self, e: EdgeRef, w: Range<i64>) -> bool {
        self.edge(e.pid()).active(e.layer(), w)
    }

    fn edge_t(&self, e: EdgeRef) -> BoxedIter<EdgeRef> {
        let arc = self.edge_arc(e.pid());
        let iter: GenBoxed<EdgeRef> = GenBoxed::new_boxed(|co| async move {
            for t in arc.timestamps(e.layer()) {
                co.yield_(e.at(*t)).await;
            }
        });
        Box::new(iter.into_iter())
    }

    fn edge_window_t(&self, e: EdgeRef, w: Range<i64>) -> BoxedIter<EdgeRef> {
        let arc = self.edge_arc(e.pid());
        let iter: GenBoxed<EdgeRef> = GenBoxed::new_boxed(|co| async move {
            for t in arc.timestamps_window(e.layer(), w) {
                co.yield_(e.at(*t)).await;
            }
        });
        Box::new(iter.into_iter())
    }

    fn edge_earliest_time(&self, e: EdgeRef) -> Option<i64> {
        e.time().or_else(|| self.edge_additions(e).first())
    }

    fn edge_earliest_time_window(&self, e: EdgeRef, w: Range<i64>) -> Option<i64> {
        e.time().or_else(|| self.edge_additions(e).range(w).first())
    }

    fn edge_latest_time(&self, e: EdgeRef) -> Option<i64> {
        e.time().or_else(|| self.edge_additions(e).last())
    }

    fn edge_latest_time_window(&self, e: EdgeRef, w: Range<i64>) -> Option<i64> {
        e.time().or_else(|| self.edge_additions(e).range(w).last())
    }

    fn vertex_earliest_time_window(&self, v: VID, t_start: i64, t_end: i64) -> Option<i64> {
        self.node_entry(v)
            .value()
            .and_then(|node| node.timestamps().range(t_start..t_end).first())
    }

    fn vertex_latest_time_window(&self, v: VID, t_start: i64, t_end: i64) -> Option<i64> {
        self.node_entry(v)
            .value()
            .and_then(|node| node.timestamps().range(t_start..t_end).last())
    }

    fn vertex_history(&self, v: VID) -> Vec<i64> {
        self.vertex_additions(v).iter().copied().collect()
    }

    fn vertex_history_window(&self, v: VID, w: Range<i64>) -> Vec<i64> {
        self.vertex_additions(v).range(w).iter().copied().collect()
    }

    fn edge_deletion_history(&self, e: EdgeRef) -> Vec<i64> {
        self.edge_deletions(e).iter().copied().collect()
    }

    fn edge_deletion_history_window(&self, e: EdgeRef, w: Range<i64>) -> Vec<i64> {
        self.edge_deletions(e).range(w).iter().copied().collect()
    }

    fn temporal_prop_vec(&self, name: &str) -> Vec<(i64, Prop)> {
        self.get_temporal_prop(name)
            .map(|prop| prop.iter().collect())
            .unwrap_or_default()
    }

    fn temporal_prop_vec_window(&self, name: &str, t_start: i64, t_end: i64) -> Vec<(i64, Prop)> {
        self.get_temporal_prop(name)
            .map(|prop| prop.iter_window(t_start..t_end).collect())
            .unwrap_or_default()
    }

    fn temporal_vertex_prop_vec(&self, v: VID, name: &str) -> Vec<(i64, Prop)> {
        self.vertex(v).temporal_properties(name, None).collect()
    }

    fn temporal_vertex_prop_vec_window(
        &self,
        v: VID,
        name: &str,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)> {
        self.vertex(v)
            .temporal_properties(name, Some(t_start..t_end))
            .collect()
    }

    fn temporal_edge_prop_vec_window(
        &self,
        e: EdgeRef,
        name: &str,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)> {
        self.prop_vec_window(e.pid(), name, t_start, t_end, e.layer())
    }

    fn temporal_edge_prop_vec(&self, e: EdgeRef, name: &str) -> Vec<(i64, Prop)> {
        self.edge(e.pid())
            .temporal_properties(name, e.layer(), None)
    }
}
