use crate::{
    core::{
        entities::{
            edges::{edge_ref::EdgeRef, edge_store::EdgeStore},
            graph::tgraph::InnerTemporalGraph,
            LayerIds, VID,
        },
        storage::timeindex::{AsTime, TimeIndexOps},
    },
    db::api::view::{
        internal::{CoreDeletionOps, CoreGraphOps, EdgeFilter, TimeSemantics},
        BoxedIter,
    },
    prelude::Prop,
};
use genawaiter::sync::GenBoxed;
use rayon::prelude::*;
use std::ops::Range;

impl<const N: usize> TimeSemantics for InnerTemporalGraph<N> {
    fn vertex_earliest_time(&self, v: VID) -> Option<i64> {
        self.inner().node_entry(v).value().timestamps().first_t()
    }

    fn vertex_latest_time(&self, v: VID) -> Option<i64> {
        self.inner().node_entry(v).value().timestamps().last_t()
    }

    fn view_start(&self) -> Option<i64> {
        self.earliest_time_global()
    }

    fn view_end(&self) -> Option<i64> {
        self.latest_time_global().map(|t| t + 1) // so it is exclusive
    }

    fn earliest_time_global(&self) -> Option<i64> {
        self.inner().graph_earliest_time()
    }

    fn latest_time_global(&self) -> Option<i64> {
        self.inner().graph_latest_time()
    }

    fn earliest_time_window(&self, t_start: i64, t_end: i64) -> Option<i64> {
        self.inner()
            .storage
            .nodes
            .read_lock()
            .into_par_iter()
            .flat_map(|v| v.timestamps().range(t_start..t_end).first_t())
            .min()
    }

    fn latest_time_window(&self, t_start: i64, t_end: i64) -> Option<i64> {
        self.inner()
            .storage
            .nodes
            .read_lock()
            .into_par_iter()
            .flat_map(|v| v.timestamps().range(t_start..t_end).last_t())
            .max()
    }

    fn vertex_earliest_time_window(&self, v: VID, t_start: i64, t_end: i64) -> Option<i64> {
        self.inner()
            .node_entry(v)
            .value()
            .timestamps()
            .range(t_start..t_end)
            .first_t()
    }

    fn vertex_latest_time_window(&self, v: VID, t_start: i64, t_end: i64) -> Option<i64> {
        self.inner()
            .node_entry(v)
            .value()
            .timestamps()
            .range(t_start..t_end)
            .last_t()
    }

    fn include_vertex_window(
        &self,
        v: VID,
        w: Range<i64>,
        layer_ids: &LayerIds,
        edge_filter: Option<EdgeFilter>,
    ) -> bool {
        self.inner().node_entry(v).timestamps().active(w)
    }

    fn include_edge_window(&self, e: &EdgeStore, w: Range<i64>, layer_ids: &LayerIds) -> bool {
        e.active(layer_ids, w)
    }

    fn vertex_history(&self, v: VID) -> Vec<i64> {
        self.vertex_additions(v).iter_t().copied().collect()
    }

    fn vertex_history_window(&self, v: VID, w: Range<i64>) -> Vec<i64> {
        self.vertex_additions(v)
            .range(w)
            .iter_t()
            .copied()
            .collect()
    }

    fn edge_exploded(&self, e: EdgeRef, layer_ids: LayerIds) -> BoxedIter<EdgeRef> {
        let arc = self.inner().edge_arc(e.pid());
        let layer_id = layer_ids.constrain_from_edge(e);
        let iter: GenBoxed<EdgeRef> = GenBoxed::new_boxed(|co| async move {
            // this is for when we explode edges we want to select the layer we get the timestamps from
            for (l, t) in arc.timestamps_and_layers(layer_id) {
                co.yield_(e.at(*t).at_layer(l)).await;
            }
        });
        Box::new(iter.into_iter())
    }

    fn edge_layers(&self, e: EdgeRef, layer_ids: LayerIds) -> BoxedIter<EdgeRef> {
        let arc = self.inner().edge_arc(e.pid());
        let layer_ids = layer_ids.constrain_from_edge(e);
        let iter: GenBoxed<EdgeRef> = GenBoxed::new_boxed(|co| async move {
            for l in arc.layers() {
                if layer_ids.contains(&l) {
                    co.yield_(e.at_layer(l)).await;
                }
            }
        });
        Box::new(iter.into_iter())
    }

    fn edge_window_exploded(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> BoxedIter<EdgeRef> {
        let arc = self.inner().edge_arc(e.pid());
        let layer_ids = layer_ids.constrain_from_edge(e);
        let iter: GenBoxed<EdgeRef> = GenBoxed::new_boxed(|co| async move {
            // this is for when we explode edges we want to select the layer we get the timestamps from
            for (l, t) in arc.timestamps_and_layers_window(layer_ids, w) {
                co.yield_(e.at(*t).at_layer(l)).await;
            }
        });
        Box::new(iter.into_iter())
    }

    fn edge_window_layers(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> BoxedIter<EdgeRef> {
        let arc = self.inner().edge_arc(e.pid());
        let iter: GenBoxed<EdgeRef> = GenBoxed::new_boxed(|co| async move {
            for l in arc.layers_window(w) {
                if layer_ids.contains(&l) {
                    co.yield_(e.at_layer(l)).await;
                }
            }
        });
        Box::new(iter.into_iter())
    }

    fn edge_earliest_time(&self, e: EdgeRef, layer_ids: LayerIds) -> Option<i64> {
        e.time_t()
            .or_else(|| self.edge_additions(e, layer_ids).first_t())
    }

    fn edge_earliest_time_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> Option<i64> {
        e.time_t()
            .or_else(|| self.edge_additions(e, layer_ids).range(w).first_t())
    }

    fn edge_latest_time(&self, e: EdgeRef, layer_ids: LayerIds) -> Option<i64> {
        e.time_t()
            .or_else(|| self.edge_additions(e, layer_ids).last_t())
    }

    fn edge_latest_time_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> Option<i64> {
        e.time_t()
            .or_else(|| self.edge_additions(e, layer_ids).range(w).last_t())
    }

    fn edge_deletion_history(&self, e: EdgeRef, layer_ids: LayerIds) -> Vec<i64> {
        self.edge_deletions(e, layer_ids)
            .iter_t()
            .copied()
            .collect()
    }

    fn edge_deletion_history_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> Vec<i64> {
        self.edge_deletions(e, layer_ids)
            .range(w)
            .iter_t()
            .copied()
            .collect()
    }

    fn temporal_prop_vec(&self, name: &str) -> Vec<(i64, Prop)> {
        self.inner()
            .get_temporal_prop(name)
            .map(|prop| prop.iter().collect())
            .unwrap_or_default()
    }

    fn temporal_prop_vec_window(&self, name: &str, t_start: i64, t_end: i64) -> Vec<(i64, Prop)> {
        self.inner()
            .get_temporal_prop(name)
            .map(|prop| prop.iter_window(t_start..t_end).collect())
            .unwrap_or_default()
    }

    fn temporal_vertex_prop_vec(&self, v: VID, name: &str) -> Vec<(i64, Prop)> {
        self.inner()
            .vertex(v)
            .temporal_properties(name, None)
            .collect()
    }

    fn temporal_vertex_prop_vec_window(
        &self,
        v: VID,
        name: &str,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)> {
        self.inner()
            .vertex(v)
            .temporal_properties(name, Some(t_start..t_end))
            .collect()
    }

    fn temporal_edge_prop_vec_window(
        &self,
        e: EdgeRef,
        name: &str,
        t_start: i64,
        t_end: i64,
        layer_ids: LayerIds,
    ) -> Vec<(i64, Prop)> {
        self.temporal_edge_prop(e, name, layer_ids)
            .map(|p| match e.time() {
                Some(t) => {
                    if *t.t() >= t_start && *t.t() < t_end {
                        p.at(&t).map(|v| vec![(*t.t(), v)]).unwrap_or_default()
                    } else {
                        vec![]
                    }
                }
                None => p.iter_window(t_start..t_end).collect(),
            })
            .unwrap_or_default()
    }

    fn temporal_edge_prop_vec(
        &self,
        e: EdgeRef,
        name: &str,
        layer_ids: LayerIds,
    ) -> Vec<(i64, Prop)> {
        self.temporal_edge_prop(e, name, layer_ids)
            .map(|p| match e.time() {
                Some(t) => p.at(&t).map(|v| vec![(*t.t(), v)]).unwrap_or_default(),
                None => p.iter().collect(),
            })
            .unwrap_or_default()
    }
}
