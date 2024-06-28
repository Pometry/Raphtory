use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, graph::tgraph::InternalGraph, LayerIds, VID},
        storage::timeindex::{AsTime, TimeIndexIntoOps, TimeIndexOps},
    },
    db::api::{
        storage::{
            edges::{
                edge_ref::EdgeStorageRef,
                edge_storage_ops::{EdgeStorageIntoOps, EdgeStorageOps},
            },
            nodes::{node_ref::NodeStorageRef, node_storage_ops::*},
            tprop_storage_ops::TPropOps,
        },
        view::{
            internal::{CoreGraphOps, TimeSemantics},
            BoxedIter, IntoDynBoxed,
        },
    },
    prelude::Prop,
};
use itertools::{kmerge, Itertools};
use raphtory_api::core::storage::timeindex::TimeIndexEntry;
use rayon::prelude::*;
use std::ops::Range;

impl TimeSemantics for InternalGraph {
    fn node_earliest_time(&self, v: VID) -> Option<i64> {
        self.inner().storage.get_node(v).timestamps().first_t()
    }

    fn node_latest_time(&self, v: VID) -> Option<i64> {
        self.inner().storage.get_node(v).timestamps().last_t()
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
            .flat_map(|v| v.timestamps().range_t(start..end).first_t())
            .min()
    }

    fn latest_time_window(&self, start: i64, end: i64) -> Option<i64> {
        self.inner()
            .storage
            .nodes
            .read_lock()
            .into_par_iter()
            .flat_map(|v| v.timestamps().range_t(start..end).last_t())
            .max()
    }

    fn node_earliest_time_window(&self, v: VID, start: i64, end: i64) -> Option<i64> {
        self.inner()
            .storage
            .get_node(v)
            .timestamps()
            .range_t(start..end)
            .first_t()
    }

    fn node_latest_time_window(&self, v: VID, start: i64, end: i64) -> Option<i64> {
        self.inner()
            .storage
            .get_node(v)
            .timestamps()
            .range_t(start..end)
            .last_t()
    }

    #[inline]
    fn include_node_window(
        &self,
        node: NodeStorageRef,
        w: Range<i64>,
        _layer_ids: &LayerIds,
    ) -> bool {
        node.additions().active_t(w)
    }

    #[inline]
    fn include_edge_window(
        &self,
        edge: EdgeStorageRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> bool {
        edge.active(layer_ids, w)
    }

    fn node_history(&self, v: VID) -> Vec<i64> {
        let node = self.core_node_entry(v);
        let collect = node.additions().iter_t().collect();
        collect
    }

    fn node_history_window(&self, v: VID, w: Range<i64>) -> Vec<i64> {
        let node = self.core_node_entry(v);
        let collect = node.additions().range_t(w).iter_t().collect();
        collect
    }

    fn edge_history(&self, e: EdgeRef, layer_ids: LayerIds) -> Vec<i64> {
        let core_edge = self.core_edge(e.into());
        kmerge(
            core_edge
                .additions_iter(&layer_ids)
                .map(|(_, index)| index.into_iter()),
        )
        .map(|te| te.t())
        .collect()
    }

    fn edge_history_window(&self, e: EdgeRef, layer_ids: LayerIds, w: Range<i64>) -> Vec<i64> {
        let core_edge = self.core_edge(e.into());
        kmerge(
            core_edge
                .additions_iter(&layer_ids)
                .map(move |(_, index)| index.into_range_t(w.clone()).into_iter_t()),
        )
        .collect()
    }

    fn edge_exploded_count(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> usize {
        edge.additions_par_iter(layer_ids)
            .map(|(_, a)| a.len())
            .sum()
    }

    fn edge_exploded_count_window(
        &self,
        edge: EdgeStorageRef,
        layer_ids: &LayerIds,
        w: Range<i64>,
    ) -> usize {
        edge.additions_par_iter(layer_ids)
            .map(|(_, a)| a.range_t(w.clone()).len())
            .sum()
    }

    fn edge_exploded(&self, e: EdgeRef, layer_ids: &LayerIds) -> BoxedIter<EdgeRef> {
        let entry = self.inner().storage.get_edge_arc(e.pid());
        entry.into_exploded(layer_ids.clone(), e).into_dyn_boxed()
    }

    fn edge_layers(&self, e: EdgeRef, layer_ids: &LayerIds) -> BoxedIter<EdgeRef> {
        let entry = self.core_edge_arc(e.into());
        entry.into_layers(layer_ids.clone(), e).into_dyn_boxed()
    }

    fn edge_window_exploded(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> BoxedIter<EdgeRef> {
        let arc = self.core_edge_arc(e.into());
        let layer_ids = layer_ids.clone();
        arc.into_exploded_window(layer_ids, TimeIndexEntry::range(w), e)
            .into_dyn_boxed()
    }

    fn edge_window_layers(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> BoxedIter<EdgeRef> {
        let entry = self.core_edge_arc(e.into());
        entry
            .clone()
            .into_layers(layer_ids.clone(), e)
            .filter(move |e| entry.additions(*e.layer().unwrap()).active_t(w.clone()))
            .into_dyn_boxed()
    }

    fn edge_earliest_time(&self, e: EdgeRef, layer_ids: &LayerIds) -> Option<i64> {
        e.time_t().or_else(|| {
            let entry = self.core_edge(e.into());
            entry
                .additions_par_iter(layer_ids)
                .flat_map(|(_, a)| a.first_t())
                .min()
        })
    }

    fn edge_earliest_time_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> Option<i64> {
        match e.time_t() {
            Some(t) => w.contains(&t).then_some(t),
            None => {
                let entry = self.core_edge(e.into());
                entry
                    .additions_par_iter(layer_ids)
                    .flat_map(|(_, a)| a.range_t(w.clone()).first_t())
                    .min()
            }
        }
    }

    fn edge_latest_time(&self, e: EdgeRef, layer_ids: &LayerIds) -> Option<i64> {
        e.time_t().or_else(|| {
            let entry = self.core_edge(e.into());
            entry
                .additions_par_iter(layer_ids)
                .flat_map(|(_, a)| a.last_t())
                .max()
        })
    }

    fn edge_latest_time_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> Option<i64> {
        match e.time_t() {
            Some(t) => w.contains(&t).then_some(t),
            None => {
                let entry = self.core_edge(e.into());
                entry
                    .additions_par_iter(layer_ids)
                    .flat_map(|(_, a)| a.range_t(w.clone()).last_t())
                    .max()
            }
        }
    }

    fn edge_deletion_history(&self, e: EdgeRef, layer_ids: &LayerIds) -> Vec<i64> {
        let entry = self.core_edge(e.into());
        entry
            .deletions_iter(layer_ids)
            .map(|(_, d)| d.into_iter_t())
            .kmerge()
            .collect()
    }

    fn edge_deletion_history_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> Vec<i64> {
        let entry = self.core_edge(e.into());
        entry
            .deletions_iter(layer_ids)
            .map(|(_, d)| d.into_range_t(w.clone()).into_iter_t())
            .kmerge()
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
            .map(|prop| prop.iter_t().collect())
            .unwrap_or_default()
    }

    fn has_temporal_prop_window(&self, prop_id: usize, w: Range<i64>) -> bool {
        self.inner()
            .graph_meta
            .get_temporal_prop(prop_id)
            .map(|p| p.iter_window_t(w).next().is_some())
            .filter(|p| *p)
            .is_some()
    }

    fn temporal_prop_vec_window(&self, prop_id: usize, start: i64, end: i64) -> Vec<(i64, Prop)> {
        self.inner()
            .get_temporal_prop(prop_id)
            .map(|prop| prop.iter_window_t(start..end).collect())
            .unwrap_or_default()
    }

    fn has_temporal_node_prop(&self, v: VID, prop_id: usize) -> bool {
        let entry = self.inner().storage.nodes.entry(v);
        entry.temporal_property(prop_id).is_some()
    }

    fn temporal_node_prop_vec(&self, v: VID, prop_id: usize) -> Vec<(i64, Prop)> {
        let node = self.inner().storage.nodes.entry(v);
        node.temporal_properties(prop_id, None).collect()
    }

    fn has_temporal_node_prop_window(&self, v: VID, prop_id: usize, w: Range<i64>) -> bool {
        let entry = self.inner().storage.nodes.entry(v);
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
            .storage
            .nodes
            .entry(v)
            .temporal_properties(prop_id, Some(start..end))
            .collect()
    }

    fn has_temporal_edge_prop_window(
        &self,
        e: EdgeRef,
        prop_id: usize,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> bool {
        let entry = self.core_edge(e.into());
        entry
            .temporal_prop_par_iter(layer_ids, prop_id)
            .any(|(_, p)| p.active(w.clone()))
    }

    fn temporal_edge_prop_vec_window(
        &self,
        e: EdgeRef,
        prop_id: usize,
        start: i64,
        end: i64,
        layer_ids: &LayerIds,
    ) -> Vec<(i64, Prop)> {
        let entry = self.core_edge(e.into());
        match e.time() {
            Some(t) => {
                if (start..end).contains(&t.t()) {
                    entry
                        .temporal_prop_iter(layer_ids, prop_id)
                        .flat_map(|(_, p)| p.at(&t).map(|v| (t.t(), v)))
                        .collect()
                } else {
                    vec![]
                }
            }
            None => entry
                .temporal_prop_iter(layer_ids, prop_id)
                .map(|(_, p)| p.iter_window_t(start..end))
                .kmerge()
                .collect(),
        }
    }

    fn has_temporal_edge_prop(&self, e: EdgeRef, prop_id: usize, layer_ids: &LayerIds) -> bool {
        let entry = self.core_edge(e.into());
        (&entry).has_temporal_prop(layer_ids, prop_id)
    }

    fn temporal_edge_prop_vec(
        &self,
        e: EdgeRef,
        prop_id: usize,
        layer_ids: &LayerIds,
    ) -> Vec<(i64, Prop)> {
        let entry = self.core_edge(e.into());
        match e.time() {
            Some(t) => entry
                .temporal_prop_iter(layer_ids, prop_id)
                .flat_map(|(_, p)| p.at(&t).map(|v| (t.t(), v)))
                .collect(),
            None => entry
                .temporal_prop_iter(layer_ids, prop_id)
                .map(|(_, p)| p.iter_t())
                .kmerge()
                .collect(),
        }
    }
}
