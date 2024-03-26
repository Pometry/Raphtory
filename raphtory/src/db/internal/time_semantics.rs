use crate::{
    core::{
        entities::{
            edges::{edge_ref::EdgeRef, edge_store::EdgeStore},
            graph::tgraph::InnerTemporalGraph,
            nodes::node_store::NodeStore,
            LayerIds, VID,
        },
        storage::timeindex::{AsTime, TimeIndexIntoOps, TimeIndexOps},
    },
    db::api::view::{
        internal::{CoreDeletionOps, CoreGraphOps, TimeSemantics},
        BoxedIter,
    },
    prelude::Prop,
};
use genawaiter::sync::GenBoxed;
use itertools::kmerge;
use rayon::prelude::*;
use std::ops::Range;

impl<const N: usize> TimeSemantics for InnerTemporalGraph<N> {
    fn node_earliest_time(&self, v: VID) -> Option<i64> {
        self.inner().node_entry(v).value().timestamps().first_t()
    }

    fn node_latest_time(&self, v: VID) -> Option<i64> {
        self.inner().node_entry(v).value().timestamps().last_t()
    }

    fn view_start(&self) -> Option<i64> {
        None
    }

    fn view_end(&self) -> Option<i64> {
        None
    }

    fn earliest_time_global(&self) -> Option<i64> {
        self.inner().graph_earliest_time()
    }

    fn latest_time_global(&self) -> Option<i64> {
        self.inner().graph_latest_time()
    }

    fn earliest_time_window(&self, start: i64, end: i64) -> Option<i64> {
        self.inner()
            .storage
            .nodes
            .read_lock()
            .into_par_iter()
            .flat_map(|v| v.timestamps().range(start..end).first_t())
            .min()
    }

    fn latest_time_window(&self, start: i64, end: i64) -> Option<i64> {
        self.inner()
            .storage
            .nodes
            .read_lock()
            .into_par_iter()
            .flat_map(|v| v.timestamps().range(start..end).last_t())
            .max()
    }

    fn node_earliest_time_window(&self, v: VID, start: i64, end: i64) -> Option<i64> {
        self.inner()
            .node_entry(v)
            .value()
            .timestamps()
            .range(start..end)
            .first_t()
    }

    fn node_latest_time_window(&self, v: VID, start: i64, end: i64) -> Option<i64> {
        self.inner()
            .node_entry(v)
            .value()
            .timestamps()
            .range(start..end)
            .last_t()
    }

    #[inline]
    fn include_node_window(&self, node: &NodeStore, w: Range<i64>, _layer_ids: &LayerIds) -> bool {
        node.timestamps().active(w)
    }

    #[inline]
    fn include_edge_window(&self, edge: &EdgeStore, w: Range<i64>, layer_ids: &LayerIds) -> bool {
        edge.active(layer_ids, w)
    }

    fn node_history(&self, v: VID) -> Vec<i64> {
        self.node_additions(v).iter_t().collect()
    }

    fn node_history_window(&self, v: VID, w: Range<i64>) -> Vec<i64> {
        self.node_additions(v).range(w).iter_t().collect()
    }

    fn edge_history(&self, e: EdgeRef, layer_ids: LayerIds) -> Vec<i64> {
        let core_edge = self.core_edge_arc(e.pid());
        kmerge(
            core_edge
                .additions_iter(&layer_ids)
                .map(|index| index.iter()),
        )
        .map(|te| te.t())
        .collect()
    }

    fn edge_history_window(&self, e: EdgeRef, layer_ids: LayerIds, w: Range<i64>) -> Vec<i64> {
        let core_edge = self.core_edge_arc(e.pid());
        kmerge(
            core_edge
                .additions_iter(&layer_ids)
                .map(move |index| index.range(w.clone()).into_iter_t()),
        )
        .collect()
    }

    fn edge_exploded_count(&self, edge: &EdgeStore, layer_ids: &LayerIds) -> usize {
        match layer_ids {
            LayerIds::None => 0,
            LayerIds::All => edge.additions.par_iter().map(|a| a.len()).sum(),
            LayerIds::One(l) => edge.additions.get(*l).map(|a| a.len()).unwrap_or(0),
            LayerIds::Multiple(ids) => ids
                .par_iter()
                .map(|l| edge.additions.get(*l).map(|a| a.len()).unwrap_or(0))
                .sum(),
        }
    }

    fn edge_exploded_count_window(
        &self,
        edge: &EdgeStore,
        layer_ids: &LayerIds,
        w: Range<i64>,
    ) -> usize {
        match layer_ids {
            LayerIds::None => 0,
            LayerIds::All => edge
                .additions
                .par_iter()
                .map(|a| a.range(w.clone()).len())
                .sum(),
            LayerIds::One(l) => edge
                .additions
                .get(*l)
                .map(|a| a.range(w).len())
                .unwrap_or(0),
            LayerIds::Multiple(ids) => ids
                .par_iter()
                .map(|l| {
                    edge.additions
                        .get(*l)
                        .map(|a| a.range(w.clone()).len())
                        .unwrap_or(0)
                })
                .sum(),
        }
    }

    fn edge_exploded(&self, e: EdgeRef, layer_ids: &LayerIds) -> BoxedIter<EdgeRef> {
        let arc = self.inner().edge_arc(e.pid());
        let layer_id = layer_ids.clone().constrain_from_edge(e);
        let iter: GenBoxed<EdgeRef> = GenBoxed::new_boxed(|co| async move {
            // this is for when we explode edges we want to select the layer we get the timestamps from
            for (l, t) in arc.timestamps_and_layers(layer_id) {
                co.yield_(e.at(t).at_layer(l)).await;
            }
        });
        Box::new(iter.into_iter())
    }

    fn edge_layers(&self, e: EdgeRef, layer_ids: &LayerIds) -> BoxedIter<EdgeRef> {
        let arc = self.inner().edge_arc(e.pid());
        let layer_ids = layer_ids.clone().constrain_from_edge(e);
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
        layer_ids: &LayerIds,
    ) -> BoxedIter<EdgeRef> {
        let arc = self.inner().edge_arc(e.pid());
        let layer_ids = layer_ids.clone().constrain_from_edge(e);
        let iter: GenBoxed<EdgeRef> = GenBoxed::new_boxed(|co| async move {
            // this is for when we explode edges we want to select the layer we get the timestamps from
            for (l, t) in arc.timestamps_and_layers_window(layer_ids, w) {
                co.yield_(e.at(t).at_layer(l)).await;
            }
        });
        Box::new(iter.into_iter())
    }

    fn edge_window_layers(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> BoxedIter<EdgeRef> {
        let arc = self.inner().edge_arc(e.pid());
        let layer_ids = layer_ids.clone();
        let iter: GenBoxed<EdgeRef> = GenBoxed::new_boxed(|co| async move {
            for l in arc.layers_window(w) {
                if layer_ids.contains(&l) {
                    co.yield_(e.at_layer(l)).await;
                }
            }
        });
        Box::new(iter.into_iter())
    }

    fn edge_earliest_time(&self, e: EdgeRef, layer_ids: &LayerIds) -> Option<i64> {
        e.time_t()
            .or_else(|| self.edge_additions(e, layer_ids.clone()).first_t())
    }

    fn edge_earliest_time_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> Option<i64> {
        e.time_t()
            .or_else(|| self.edge_additions(e, layer_ids.clone()).range(w).first_t())
    }

    fn edge_latest_time(&self, e: EdgeRef, layer_ids: &LayerIds) -> Option<i64> {
        e.time_t()
            .or_else(|| self.edge_additions(e, layer_ids.clone()).last_t())
    }

    fn edge_latest_time_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> Option<i64> {
        e.time_t()
            .or_else(|| self.edge_additions(e, layer_ids.clone()).range(w).last_t())
    }

    fn edge_deletion_history(&self, e: EdgeRef, layer_ids: &LayerIds) -> Vec<i64> {
        self.edge_deletions(e, layer_ids.clone()).iter_t().collect()
    }

    fn edge_deletion_history_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> Vec<i64> {
        self.edge_deletions(e, layer_ids.clone())
            .range(w)
            .iter_t()
            .collect()
    }

    fn edge_is_valid(&self, _e: EdgeRef, _layer_ids: &LayerIds) -> bool {
        true
    }

    fn edge_is_valid_at_end(&self, _e: EdgeRef, _layer_ids: &LayerIds, _t: i64) -> bool {
        true
    }

    fn has_temporal_prop(&self, prop_id: usize) -> bool {
        prop_id < self.inner().graph_meta.temporal_prop_meta().len()
    }

    fn temporal_prop_vec(&self, prop_id: usize) -> Vec<(i64, Prop)> {
        self.inner()
            .get_temporal_prop(prop_id)
            .map(|prop| prop.iter().collect())
            .unwrap_or_default()
    }

    fn has_temporal_prop_window(&self, prop_id: usize, w: Range<i64>) -> bool {
        self.inner()
            .graph_meta
            .get_temporal_prop(prop_id)
            .filter(|p| p.iter_window_t(w).next().is_some())
            .is_some()
    }

    fn temporal_prop_vec_window(&self, prop_id: usize, start: i64, end: i64) -> Vec<(i64, Prop)> {
        self.inner()
            .get_temporal_prop(prop_id)
            .map(|prop| prop.iter_window_t(start..end).collect())
            .unwrap_or_default()
    }

    fn has_temporal_node_prop(&self, v: VID, prop_id: usize) -> bool {
        let entry = self.inner().storage.get_node(v);
        entry.temporal_property(prop_id).is_some()
    }

    fn temporal_node_prop_vec(&self, v: VID, prop_id: usize) -> Vec<(i64, Prop)> {
        self.inner()
            .node(v)
            .temporal_properties(prop_id, None)
            .collect()
    }

    fn has_temporal_node_prop_window(&self, v: VID, prop_id: usize, w: Range<i64>) -> bool {
        let entry = self.inner().storage.get_node(v);
        entry
            .temporal_property(prop_id)
            .filter(|p| p.iter_window_t(w).next().is_some())
            .is_some()
    }

    fn temporal_node_prop_vec_window(
        &self,
        v: VID,
        prop_id: usize,
        start: i64,
        end: i64,
    ) -> Vec<(i64, Prop)> {
        self.inner()
            .node(v)
            .temporal_properties(prop_id, Some(start..end))
            .collect()
    }

    fn has_temporal_edge_prop_window(
        &self,
        e: EdgeRef,
        prop_id: usize,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> bool {
        let entry = self.inner().storage.get_edge(e.pid());
        entry.has_temporal_prop_window(layer_ids, prop_id, w)
    }

    fn temporal_edge_prop_vec_window(
        &self,
        e: EdgeRef,
        prop_id: usize,
        start: i64,
        end: i64,
        layer_ids: LayerIds,
    ) -> Vec<(i64, Prop)> {
        self.temporal_edge_prop(e, prop_id, layer_ids)
            .map(|p| match e.time() {
                Some(t) => {
                    if t.t() >= start && t.t() < end {
                        p.at(&t).map(|v| vec![(t.t(), v)]).unwrap_or_default()
                    } else {
                        vec![]
                    }
                }
                None => p.iter_window(start..end).collect(),
            })
            .unwrap_or_default()
    }

    fn has_temporal_edge_prop(&self, e: EdgeRef, prop_id: usize, layer_ids: LayerIds) -> bool {
        let entry = self.inner().storage.get_edge(e.pid());
        entry.has_temporal_prop(&layer_ids, prop_id)
    }

    fn temporal_edge_prop_vec(
        &self,
        e: EdgeRef,
        prop_id: usize,
        layer_ids: LayerIds,
    ) -> Vec<(i64, Prop)> {
        self.temporal_edge_prop(e, prop_id, layer_ids)
            .map(|p| match e.time() {
                Some(t) => p.at(&t).map(|v| vec![(t.t(), v)]).unwrap_or_default(),
                None => p.iter().collect(),
            })
            .unwrap_or_default()
    }
}
