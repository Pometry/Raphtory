use itertools::{kmerge, Itertools};
use raphtory_api::{
    core::{
        entities::{edges::edge_ref::EdgeRef, VID},
        storage::timeindex::{AsTime, TimeIndexEntry},
    },
    iter::{BoxedLDIter, IntoDynDBoxed},
};
use rayon::iter::ParallelIterator;
use std::{borrow::Cow, iter, ops::Range};

use super::GraphStorage;
use crate::{
    core::{
        entities::LayerIds,
        storage::timeindex::{TimeIndexIntoOps, TimeIndexOps},
        utils::iter::{GenLockedDIter, GenLockedIter},
    },
    db::api::{
        storage::graph::{
            edges::{edge_ref::EdgeStorageRef, edge_storage_ops::EdgeStorageOps},
            nodes::node_storage_ops::NodeStorageOps,
            tprop_storage_ops::TPropOps,
        },
        view::{
            internal::{CoreGraphOps, GraphTimeSemanticsOps, TimeSemantics},
            BoxedLIter, IntoDynBoxed,
        },
    },
    prelude::Prop,
};

impl GraphTimeSemanticsOps for GraphStorage {
    fn view_start(&self) -> Option<i64> {
        None
    }

    fn view_end(&self) -> Option<i64> {
        None
    }

    #[inline]
    fn earliest_time_global(&self) -> Option<i64> {
        match self {
            GraphStorage::Mem(storage) => storage.graph.graph_earliest_time(),
            GraphStorage::Unlocked(storage) => storage.graph_earliest_time(),
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => storage.inner.earliest(),
        }
    }

    #[inline]
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

    fn include_edge_window(
        &self,
        edge: EdgeStorageRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> bool {
        edge.added(layer_ids, w)
    }

    fn edge_history<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, TimeIndexEntry> {
        let core_edge = self.edge_entry(e.pid());
        let layer_ids = layer_ids.constrain_from_edge(e);
        GenLockedIter::from(core_edge, |core_edge| match layer_ids.as_ref() {
            LayerIds::None => std::iter::empty().into_dyn_boxed(),
            LayerIds::One(_) => core_edge
                .additions_iter(&layer_ids)
                .map(|(_, index)| index.iter())
                .flatten()
                .into_dyn_boxed(),
            _ => kmerge(
                core_edge
                    .additions_iter(&layer_ids)
                    .map(|(_, index)| index.iter()),
            )
            .into_dyn_boxed(),
        })
        .into_dyn_boxed()
    }

    fn edge_history_window<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: Cow<'a, LayerIds>,
        w: Range<i64>,
    ) -> BoxedLIter<'a, TimeIndexEntry> {
        let core_edge = self.edge_entry(e.pid());
        let layer_ids = layer_ids.constrain_from_edge(e);
        GenLockedIter::from(core_edge, |core_edge| {
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
        edge.additions_iter(layer_ids).map(|(_, a)| a.len()).sum()
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

    fn edge_exploded<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, EdgeRef> {
        let edge = self.core_edge(e.pid());
        let layer_ids = layer_ids.constrain_from_edge(e);
        GenLockedIter::from(edge, move |edge| {
            edge.additions_iter(&layer_ids)
                .map(move |(l, a)| a.into_iter().map(move |t| e.at(t).at_layer(l)))
                .kmerge_by(|e1, e2| e1.time() <= e2.time())
                .into_dyn_boxed()
        })
        .into_dyn_boxed()
    }

    fn edge_layers<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, EdgeRef> {
        let entry = self.core_edge(e.pid());
        let layer_ids = layer_ids.constrain_from_edge(e);
        GenLockedIter::from(entry, move |edge| {
            Box::new(edge.layer_ids_iter(&layer_ids).map(move |l| e.at_layer(l)))
        })
        .into_dyn_boxed()
    }

    fn edge_window_exploded<'a>(
        &'a self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, EdgeRef> {
        let entry = self.core_edge(e.pid());
        let layer_ids = layer_ids.constrain_from_edge(e);
        GenLockedIter::from(entry, move |edge| {
            edge.additions_iter(&layer_ids)
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
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, EdgeRef> {
        let entry = self.core_edge(e.pid());
        self.edge_layers(e, layer_ids)
            .filter(move |e| entry.additions(e.layer().unwrap()).active_t(w.clone()))
            .into_dyn_boxed()
    }

    fn edge_earliest_time(&self, e: EdgeRef, layer_ids: &LayerIds) -> Option<i64> {
        e.time_t().or_else(|| {
            let entry = self.core_edge(e.pid());
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
                let entry = self.core_edge(e.pid());
                entry
                    .additions_par_iter(layer_ids)
                    .flat_map(|(_, a)| a.range_t(w.clone()).first_t())
                    .min()
            }
        }
    }

    fn edge_latest_time(&self, e: EdgeRef, layer_ids: &LayerIds) -> Option<i64> {
        e.time_t().or_else(|| {
            let entry = self.core_edge(e.pid());
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
                let entry = self.core_edge(e.pid());
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
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, TimeIndexEntry> {
        let entry = self.core_edge(e.pid());
        GenLockedIter::from(entry, |entry| {
            entry
                .deletions_iter(&layer_ids)
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
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, TimeIndexEntry> {
        let entry = self.core_edge(e.pid());
        GenLockedIter::from(entry, |entry| {
            entry
                .deletions_iter(&layer_ids)
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

    fn temporal_prop_iter(&self, prop_id: usize) -> BoxedLIter<(i64, Prop)> {
        self.graph_meta()
            .get_temporal_prop(prop_id)
            .into_iter()
            .flat_map(move |prop| GenLockedIter::from(prop, |prop| prop.iter_t().into_dyn_boxed()))
            .into_dyn_boxed()
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

    fn temporal_prop_iter_window(
        &self,
        prop_id: usize,
        start: i64,
        end: i64,
    ) -> BoxedLIter<(i64, Prop)> {
        self.graph_meta()
            .get_temporal_prop(prop_id)
            .into_iter()
            .flat_map(move |prop| {
                GenLockedIter::from(prop, |prop| prop.iter_window_t(start..end).into_dyn_boxed())
            })
            .into_dyn_boxed()
    }

    fn temporal_node_prop_hist(&self, v: VID, id: usize) -> BoxedLDIter<(TimeIndexEntry, Prop)> {
        let node = self.node_entry(v);
        GenLockedDIter::from(node, |node| node.tprop(id).iter().into_dyn_dboxed()).into_dyn_dboxed()
    }

    fn temporal_node_prop_hist_window(
        &self,
        v: VID,
        id: usize,
        start: i64,
        end: i64,
    ) -> BoxedLDIter<(TimeIndexEntry, Prop)> {
        let node = self.node_entry(v);
        GenLockedDIter::from(node, |node| {
            node.tprop(id)
                .iter_window(TimeIndexEntry::range(start..end))
                .into_dyn_dboxed()
        })
        .into_dyn_dboxed()
    }

    fn temporal_edge_prop_hist_window<'a>(
        &'a self,
        e: EdgeRef,
        prop_id: usize,
        start: i64,
        end: i64,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, (TimeIndexEntry, Prop)> {
        let entry = self.core_edge(e.pid());
        match e.time() {
            Some(t) => {
                if (start..end).contains(&t.t()) {
                    GenLockedIter::from(entry, move |entry| {
                        entry
                            .temporal_prop_iter(&layer_ids, prop_id)
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
                    .temporal_prop_iter(&layer_ids, prop_id)
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
        let entry = self.core_edge(e.pid());
        let res = entry
            .temporal_prop_iter(&layer_ids.constrain_from_edge(e), id)
            .filter_map(|(_, p)| p.at(&t))
            .next();
        res
    }

    fn temporal_edge_prop_hist<'a>(
        &'a self,
        e: EdgeRef,
        prop_id: usize,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, (TimeIndexEntry, Prop)> {
        let entry = self.core_edge(e.pid());
        let layer_ids = layer_ids.constrain_from_edge(e);
        match e.time() {
            Some(t) => GenLockedIter::from(entry, move |entry| {
                entry
                    .temporal_prop_iter(&layer_ids, prop_id)
                    .flat_map(move |(_, p)| p.at(&t).map(move |v| (t, v)))
                    .into_dyn_boxed()
            })
            .into_dyn_boxed(),
            None => GenLockedIter::from(entry, |entry| {
                entry
                    .temporal_prop_iter(&layer_ids, prop_id)
                    .map(|(_, p)| p.iter())
                    .kmerge_by(|(t1, _), (t2, _)| t1 <= t2)
                    .into_dyn_boxed()
            })
            .into_dyn_boxed(),
        }
    }

    fn constant_edge_prop(&self, e: EdgeRef, id: usize, layer_ids: &LayerIds) -> Option<Prop> {
        let layer_ids = layer_ids.constrain_from_edge(e);
        let layer_ids: &LayerIds = &layer_ids;
        let entry = self.core_edge(e.pid());
        match layer_ids {
            LayerIds::None => None,
            LayerIds::All => match self.unfiltered_num_layers() {
                0 => None,
                1 => entry.constant_prop_layer(0, id),
                _ => {
                    let mut values = entry
                        .layer_ids_iter(layer_ids)
                        .filter_map(|layer_id| {
                            entry
                                .constant_prop_layer(layer_id, id)
                                .map(|v| (self.get_layer_name(layer_id), v))
                        })
                        .peekable();
                    if values.peek().is_some() {
                        Some(Prop::map(values))
                    } else {
                        None
                    }
                }
            },
            LayerIds::One(layer_id) => entry.constant_prop_layer(*layer_id, id),
            LayerIds::Multiple(_) => {
                let mut values = entry
                    .layer_ids_iter(layer_ids)
                    .filter_map(|layer_id| {
                        entry
                            .constant_prop_layer(layer_id, id)
                            .map(|v| (self.get_layer_name(layer_id), v))
                    })
                    .peekable();
                if values.peek().is_some() {
                    Some(Prop::map(values))
                } else {
                    None
                }
            }
        }
    }

    fn constant_edge_prop_window(
        &self,
        e: EdgeRef,
        id: usize,
        layer_ids: &LayerIds,
        w: Range<i64>,
    ) -> Option<Prop> {
        let layer_ids = layer_ids.constrain_from_edge(e);
        let layer_ids: &LayerIds = &layer_ids;
        let entry = self.core_edge(e.pid());
        match layer_ids {
            LayerIds::None => None,
            LayerIds::All => match self.unfiltered_num_layers() {
                0 => None,
                1 => {
                    if entry.additions(0).active_t(w) {
                        entry.constant_prop_layer(0, id)
                    } else {
                        None
                    }
                }
                _ => {
                    let mut values = entry
                        .layer_ids_iter(layer_ids)
                        .filter_map(|layer_id| {
                            if entry.additions(layer_id).active_t(w.clone()) {
                                entry
                                    .constant_prop_layer(layer_id, id)
                                    .map(|v| (self.get_layer_name(layer_id), v))
                            } else {
                                None
                            }
                        })
                        .peekable();
                    if values.peek().is_some() {
                        Some(Prop::map(values))
                    } else {
                        None
                    }
                }
            },
            LayerIds::One(layer_id) => {
                if entry.additions(*layer_id).active_t(w) {
                    entry.constant_prop_layer(*layer_id, id)
                } else {
                    None
                }
            }
            LayerIds::Multiple(_) => {
                let mut values = entry
                    .layer_ids_iter(layer_ids)
                    .filter_map(|layer_id| {
                        if entry.additions(layer_id).active_t(w.clone()) {
                            entry
                                .constant_prop_layer(layer_id, id)
                                .map(|v| (self.get_layer_name(layer_id), v))
                        } else {
                            None
                        }
                    })
                    .peekable();
                if values.peek().is_some() {
                    Some(Prop::map(values))
                } else {
                    None
                }
            }
        }
    }

    fn node_time_semantics(&self) -> TimeSemantics {
        TimeSemantics::event()
    }
}
