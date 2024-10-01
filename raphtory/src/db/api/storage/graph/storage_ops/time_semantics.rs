use std::{iter, ops::Range};

use itertools::{kmerge, Itertools};
use raphtory_api::core::{
    entities::{edges::edge_ref::EdgeRef, VID},
    storage::timeindex::{AsTime, TimeIndexEntry},
};
use rayon::iter::ParallelIterator;

use super::GraphStorage;
use crate::{
    core::{
        entities::LayerIds,
        storage::timeindex::{TimeIndexIntoOps, TimeIndexOps},
        utils::iter::GenLockedIter,
    },
    db::api::{
        storage::graph::{
            edges::{edge_ref::EdgeStorageRef, edge_storage_ops::EdgeStorageOps},
            nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
            tprop_storage_ops::TPropOps,
        },
        view::{
            internal::{CoreGraphOps, TimeSemantics},
            BoxedLIter, IntoDynBoxed,
        },
    },
    prelude::Prop,
};

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

    fn edge_history<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: &'a LayerIds,
    ) -> BoxedLIter<'a, TimeIndexEntry> {
        let core_edge = self.edge_entry(e.into());
        let layer_ids = layer_ids.constrain_from_edge(e);
        GenLockedIter::from((core_edge, layer_ids), |(core_edge, layer_ids)| {
            kmerge(
                core_edge
                    .additions_iter(&layer_ids)
                    .map(|(_, index)| index.into_iter()),
            )
            .into_dyn_boxed()
        })
        .into_dyn_boxed()
    }

    fn edge_history_window<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: &'a LayerIds,
        w: Range<i64>,
    ) -> BoxedLIter<'a, TimeIndexEntry> {
        let core_edge = self.edge_entry(e.into());
        let layer_ids = layer_ids.constrain_from_edge(e);
        GenLockedIter::from((core_edge, layer_ids), |(core_edge, layer_ids)| {
            kmerge(
                core_edge
                    .additions_iter(&layer_ids)
                    .map(move |(_, index)| index.into_range_t(w.clone()).into_iter()),
            )
            .into_dyn_boxed()
        })
        .into_dyn_boxed()
    }

    fn edge_exploded_count(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> usize {
        edge.additions_par_iter(&layer_ids)
            .map(|(_, a)| a.len())
            .sum()
    }

    fn edge_exploded_count_window(
        &self,
        edge: EdgeStorageRef,
        layer_ids: &LayerIds,
        w: Range<i64>,
    ) -> usize {
        edge.additions_par_iter(&layer_ids)
            .map(|(_, a)| a.range_t(w.clone()).len())
            .sum()
    }

    fn edge_exploded<'a>(
        &'a self,
        eref: EdgeRef,
        layer_ids: &'a LayerIds,
    ) -> BoxedLIter<'a, EdgeRef> {
        let edge = self.core_edge(eref.into());
        let layer_ids = layer_ids.constrain_from_edge(eref);
        GenLockedIter::from((edge, layer_ids), move |(edge, layers)| {
            edge.additions_iter(layers)
                .map(move |(l, a)| a.into_iter().map(move |t| eref.at(t).at_layer(l)))
                .kmerge_by(|e1, e2| e1.time() <= e2.time())
                .into_dyn_boxed()
        })
        .into_dyn_boxed()
    }

    fn edge_layers<'a>(&'a self, e: EdgeRef, layer_ids: &'a LayerIds) -> BoxedLIter<'a, EdgeRef> {
        let entry = self.core_edge(e.into());
        let layer_ids = layer_ids.constrain_from_edge(e);
        GenLockedIter::from((entry, layer_ids), move |(edge, layers)| {
            Box::new(edge.layer_ids_iter(layers).map(move |l| e.at_layer(l)))
        })
        .into_dyn_boxed()
    }

    fn edge_window_exploded<'a>(
        &'a self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &'a LayerIds,
    ) -> BoxedLIter<'a, EdgeRef> {
        let entry = self.core_edge(e.into());
        let layer_ids = layer_ids.constrain_from_edge(e);
        GenLockedIter::from((entry, layer_ids), move |(edge, layers)| {
            edge.additions_iter(layers)
                .map(move |(l, a)| {
                    a.into_range(TimeIndexEntry::range(w.clone()))
                        .into_iter()
                        .map(move |t| e.at(t).at_layer(l))
                })
                .kmerge_by(|e1, e2| e1.time() <= e2.time())
                .into_dyn_boxed()
        })
        .into_dyn_boxed()
    }

    fn edge_window_layers<'a>(
        &'a self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &'a LayerIds,
    ) -> BoxedLIter<'a, EdgeRef> {
        let entry = self.core_edge(e.into());
        self.edge_layers(e, layer_ids)
            .filter(move |e| entry.additions(e.layer().unwrap()).active_t(w.clone()))
            .into_dyn_boxed()
    }

    fn edge_earliest_time(&self, e: EdgeRef, layer_ids: &LayerIds) -> Option<i64> {
        e.time_t().or_else(|| {
            let entry = self.core_edge(e.into());
            entry
                .additions_par_iter(&layer_ids)
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

    fn edge_deletion_history<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: &'a LayerIds,
    ) -> BoxedLIter<'a, TimeIndexEntry> {
        let entry = self.core_edge(e.into());
        GenLockedIter::from(entry, |entry| {
            entry
                .deletions_iter(layer_ids)
                .map(|(_, d)| d.into_iter())
                .kmerge()
                .into_dyn_boxed()
        })
        .into_dyn_boxed()
    }

    fn edge_deletion_history_window<'a>(
        &'a self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &'a LayerIds,
    ) -> BoxedLIter<'a, TimeIndexEntry> {
        let entry = self.core_edge(e.into());
        GenLockedIter::from(entry, |entry| {
            entry
                .deletions_iter(layer_ids)
                .map(|(_, d)| d.into_range_t(w.clone()).into_iter())
                .kmerge()
                .into_dyn_boxed()
        })
        .into_dyn_boxed()
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

    fn temporal_node_prop_hist(&self, v: VID, id: usize) -> BoxedLIter<(TimeIndexEntry, Prop)> {
        let node = self.node_entry(v);
        GenLockedIter::from(node, |node| node.tprop(id).iter().into_dyn_boxed()).into_dyn_boxed()
    }

    fn has_temporal_node_prop_window(&self, v: VID, prop_id: usize, w: Range<i64>) -> bool {
        let node = self.node_entry(v);
        let tprop = node.tprop(prop_id);
        let iter_window_t = &mut tprop.iter_window_t(w);
        iter_window_t.next().is_some()
    }

    fn temporal_node_prop_hist_window(
        &self,
        v: VID,
        id: usize,
        start: i64,
        end: i64,
    ) -> BoxedLIter<(TimeIndexEntry, Prop)> {
        let node = self.node_entry(v);
        GenLockedIter::from(node, |node| {
            node.tprop(id)
                .iter_window(TimeIndexEntry::range(start..end))
                .into_dyn_boxed()
        })
        .into_dyn_boxed()
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

    fn temporal_edge_prop_hist_window<'a>(
        &'a self,
        e: EdgeRef,
        prop_id: usize,
        start: i64,
        end: i64,
        layer_ids: &'a LayerIds,
    ) -> BoxedLIter<'a, (TimeIndexEntry, Prop)> {
        let entry = self.core_edge(e.into());
        match e.time() {
            Some(t) => {
                if (start..end).contains(&t.t()) {
                    GenLockedIter::from(entry, move |entry| {
                        entry
                            .temporal_prop_iter(layer_ids, prop_id)
                            .flat_map(move |(_, p)| p.at(&t).map(move |v| (t, v)))
                            .into_dyn_boxed()
                    })
                    .into_dyn_boxed()
                } else {
                    iter::empty().into_dyn_boxed()
                }
            }
            None => GenLockedIter::from(entry, |entry| {
                entry
                    .temporal_prop_iter(layer_ids, prop_id)
                    .map(|(_, p)| p.iter_window(TimeIndexEntry::range(start..end)))
                    .kmerge()
                    .into_dyn_boxed()
            })
            .into_dyn_boxed(),
        }
    }

    fn temporal_edge_prop_at(
        &self,
        e: EdgeRef,
        id: usize,
        t: TimeIndexEntry,
        layer_ids: &LayerIds,
    ) -> Option<Prop> {
        let entry = self.core_edge(e.into());
        let res = entry
            .temporal_prop_iter(&layer_ids.constrain_from_edge(e), id)
            .filter_map(|(_, p)| p.at(&t))
            .next();
        res
    }

    fn has_temporal_edge_prop(&self, e: EdgeRef, prop_id: usize, layer_ids: &LayerIds) -> bool {
        let entry = self.core_edge(e.into());
        (&entry).has_temporal_prop(&layer_ids.constrain_from_edge(e), prop_id)
    }

    fn temporal_edge_prop_hist<'a>(
        &'a self,
        e: EdgeRef,
        prop_id: usize,
        layer_ids: &'a LayerIds,
    ) -> BoxedLIter<(TimeIndexEntry, Prop)> {
        let entry = self.core_edge(e.into());
        let layer_ids = layer_ids.constrain_from_edge(e);
        match e.time() {
            Some(t) => GenLockedIter::from((entry, layer_ids), move |(entry, layer_ids)| {
                entry
                    .temporal_prop_iter(layer_ids, prop_id)
                    .flat_map(move |(_, p)| p.at(&t).map(move |v| (t, v)))
                    .into_dyn_boxed()
            })
            .into_dyn_boxed(),
            None => GenLockedIter::from((entry, layer_ids), |(entry, layer_ids)| {
                entry
                    .temporal_prop_iter(layer_ids, prop_id)
                    .map(|(_, p)| p.iter())
                    .kmerge()
                    .into_dyn_boxed()
            })
            .into_dyn_boxed(),
        }
    }
}
