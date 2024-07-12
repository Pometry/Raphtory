use std::ops::Range;

use itertools::{kmerge, Itertools};
use raphtory_api::core::{
    entities::{edges::edge_ref::EdgeRef, VID},
    storage::timeindex::{AsTime, TimeIndexEntry},
};
use rayon::iter::ParallelIterator;

use crate::{
    core::{
        entities::LayerIds,
        storage::timeindex::{TimeIndexIntoOps, TimeIndexOps},
    },
    db::api::{
        storage::{
            edges::{
                edge_ref::EdgeStorageRef,
                edge_storage_ops::{EdgeStorageIntoOps, EdgeStorageOps},
            },
            nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
            tprop_storage_ops::TPropOps,
        },
        view::{
            internal::{CoreGraphOps, TimeSemantics},
            BoxedIter,
        },
    },
    prelude::Prop,
};

use super::GraphStorage;

impl TimeSemantics for GraphStorage {
    fn node_earliest_time(&self, v: VID) -> Option<i64> {
        self.node_entry(v).additions().first_t()
    }

    fn node_latest_time(&self, v: VID) -> Option<i64> {
        self.node_entry(v).additions().last_t()
    }

    fn view_start(&self) -> Option<i64> {
        None
    }

    fn view_end(&self) -> Option<i64> {
        None
    }

    fn earliest_time_global(&self) -> Option<i64> {
        match self {
            GraphStorage::Mem(storage) => storage.graph.graph_earliest_time(),
            GraphStorage::Unlocked(storage) => storage.graph_earliest_time(),
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => storage.inner.earliest(),
        }
    }

    fn latest_time_global(&self) -> Option<i64> {
        match self {
            GraphStorage::Mem(storage) => storage.graph.graph_latest_time(),
            GraphStorage::Unlocked(storage) => storage.graph_latest_time(),
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => storage.inner.latest(),
        }
    }

    fn earliest_time_window(&self, start: i64, end: i64) -> Option<i64> {
        self.nodes()
            .par_iter()
            .flat_map(|node| node.additions().range_t(start..end).first_t())
            .min()
    }

    fn latest_time_window(&self, start: i64, end: i64) -> Option<i64> {
        self.nodes()
            .par_iter()
            .flat_map(|node| node.additions().range_t(start..end).last_t())
            .max()
    }

    fn node_earliest_time_window(&self, v: VID, start: i64, end: i64) -> Option<i64> {
        self.node_entry(v).additions().range_t(start..end).first_t()
    }

    fn node_latest_time_window(&self, v: VID, start: i64, end: i64) -> Option<i64> {
        self.node_entry(v).additions().range_t(start..end).last_t()
    }

    fn include_node_window(&self, v: NodeStorageRef, w: Range<i64>, _layer_ids: &LayerIds) -> bool {
        v.additions().active_t(w)
    }

    fn include_edge_window(
        &self,
        edge: EdgeStorageRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> bool {
        edge.active(layer_ids, w)
    }

    fn node_history(&self, v: VID) -> Vec<i64> {
        self.node_entry(v).additions().iter_t().collect()
    }

    fn node_history_window(&self, v: VID, w: Range<i64>) -> Vec<i64> {
        self.node_entry(v).additions().range_t(w).iter().collect()
    }

    fn edge_history(&self, e: EdgeRef, layer_ids: LayerIds) -> Vec<i64> {
        let core_edge = self.edge_entry(e.into());
        kmerge(
            core_edge
                .additions_iter(&layer_ids)
                .map(|(_, index)| index.into_iter()),
        )
        .map(|te| te.t())
        .collect()
    }

    fn edge_history_window(&self, e: EdgeRef, layer_ids: LayerIds, w: Range<i64>) -> Vec<i64> {
        let core_edge = self.edge_entry(e.into());
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

    fn edge_exploded(&self, eref: EdgeRef, layer_ids: &LayerIds) -> BoxedIter<EdgeRef> {
        let edge = self.core_edge_arc(eref.into());
        let layer_ids = layer_ids.constrain_from_edge(eref);
        Box::new(edge.into_exploded(layer_ids, eref))
    }

    fn edge_layers(&self, e: EdgeRef, layer_ids: &LayerIds) -> BoxedIter<EdgeRef> {
        let entry = self.core_edge_arc(e.into());
        Box::new(entry.into_layers(layer_ids.clone(), e))
    }

    fn edge_window_exploded(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> BoxedIter<EdgeRef> {
        let arc = self.core_edge_arc(e.into());
        let layer_ids = layer_ids.clone();
        Box::new(arc.into_exploded_window(layer_ids, TimeIndexEntry::range(w), e))
    }

    fn edge_window_layers(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> BoxedIter<EdgeRef> {
        let entry = self.core_edge_arc(e.into());
        Box::new(
            entry
                .clone()
                .into_layers(layer_ids.clone(), e)
                .filter(move |e| entry.additions(*e.layer().unwrap()).active_t(w.clone())),
        )
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

    fn edge_is_valid_at_end(&self, _e: EdgeRef, _layer_idss: &LayerIds, _t: i64) -> bool {
        true
    }

    fn has_temporal_prop(&self, prop_id: usize) -> bool {
        prop_id < self.graph_meta().temporal_prop_meta().len()
    }

    fn temporal_prop_vec(&self, prop_id: usize) -> Vec<(i64, Prop)> {
        self.graph_meta()
            .get_temporal_prop(prop_id)
            .map(|prop| prop.iter_t().collect())
            .unwrap_or_default()
    }

    fn has_temporal_prop_window(&self, prop_id: usize, w: Range<i64>) -> bool {
        self.graph_meta()
            .get_temporal_prop(prop_id)
            .map(|p| p.iter_window_t(w).next().is_some())
            .filter(|p| *p)
            .is_some()
    }

    fn temporal_prop_vec_window(&self, prop_id: usize, start: i64, end: i64) -> Vec<(i64, Prop)> {
        self.graph_meta()
            .get_temporal_prop(prop_id)
            .map(|prop| prop.iter_window_t(start..end).collect())
            .unwrap_or_default()
    }

    fn has_temporal_node_prop(&self, v: VID, prop_id: usize) -> bool {
        let node = self.node_entry(v);
        node.tprop(prop_id).len() > 0
    }

    fn temporal_node_prop_vec(&self, v: VID, id: usize) -> Vec<(i64, Prop)> {
        let node = self.node_entry(v);
        node.tprop(id).iter_t().collect()
    }

    fn has_temporal_node_prop_window(&self, v: VID, prop_id: usize, w: Range<i64>) -> bool {
        let node = self.node_entry(v);
        let tprop = node.tprop(prop_id);
        let iter_window_t = &mut tprop.iter_window_t(w);
        iter_window_t.next().is_some()
    }

    fn temporal_node_prop_vec_window(
        &self,
        v: VID,
        id: usize,
        start: i64,
        end: i64,
    ) -> Vec<(i64, Prop)> {
        let node = self.node_entry(v);
        node.tprop(id).iter_window_t(start..end).collect()
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
