use std::{iter, ops::Range};

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
            internal::{
                CoreGraphOps, DelegateCoreOps, DelegateTimeSemantics, EdgeHistoryFilter,
                InternalLayerOps, NodeHistoryFilter, TimeSemantics,
            },
            Base, BoxedLIter, IntoDynBoxed,
        },
    },
    prelude::{GraphViewOps, NodeViewOps, Prop},
};
use itertools::{kmerge, Itertools};
use raphtory_api::core::{
    entities::{edges::edge_ref::EdgeRef, EID, VID},
    storage::timeindex::{AsTime, TimeIndexEntry},
};
use rayon::iter::ParallelIterator;

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

    fn node_earliest_time_window(&self, v: VID, start: i64, end: i64) -> Option<i64> {
        self.node_entry(v).additions().range_t(start..end).first_t()
    }

    fn node_latest_time_window(&self, v: VID, start: i64, end: i64) -> Option<i64> {
        self.node_entry(v).additions().range_t(start..end).last_t()
    }

    #[inline]
    fn include_node_window(&self, v: NodeStorageRef, w: Range<i64>, _layer_ids: &LayerIds) -> bool {
        v.additions().active_t(w)
    }

    fn include_edge_window(
        &self,
        edge: EdgeStorageRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> bool {
        edge.added(layer_ids, w)
    }

    fn node_history<'a>(&'a self, v: VID) -> BoxedLIter<'a, TimeIndexEntry> {
        GenLockedIter::from(self.node_entry(v), |node| {
            node.additions().into_iter().into_dyn_boxed()
        })
        .into_dyn_boxed()
    }

    fn node_history_window<'a>(&'a self, v: VID, w: Range<i64>) -> BoxedLIter<'a, TimeIndexEntry> {
        GenLockedIter::from(self.node_entry(v), |node| {
            node.additions()
                .into_range_t(w)
                .into_iter()
                .into_dyn_boxed()
        })
        .into_dyn_boxed()
    }

    fn node_property_history<'a>(
        &'a self,
        v: VID,
        w: Option<Range<i64>>,
    ) -> BoxedLIter<'a, TimeIndexEntry> {
        GenLockedIter::from(self.node_entry(v), |node| match w {
            Some(w) => node
                .additions()
                .into_range(TimeIndexEntry::range(w))
                .into_prop_events(),
            None => node.additions().into_prop_events(),
        })
        .into_dyn_boxed()
    }

    fn node_edge_history<'a>(
        &'a self,
        v: VID,
        w: Option<Range<i64>>,
    ) -> BoxedLIter<'a, TimeIndexEntry> {
        GenLockedIter::from(self.node_entry(v), |node| match w {
            Some(w) => node
                .additions()
                .into_range(TimeIndexEntry::range(w))
                .into_edge_events(),
            None => node.additions().into_edge_events(),
        })
        .into_dyn_boxed()
    }

    fn node_history_rows<'a>(
        &'a self,
        v: VID,
        w: Option<Range<i64>>,
    ) -> BoxedLIter<'a, (TimeIndexEntry, Vec<(usize, Prop)>)> {
        let node = self.node_entry(v);
        GenLockedIter::from(node, |node| match w {
            Some(range) => {
                let range = TimeIndexEntry::range(range);
                node.as_ref()
                    .temp_prop_rows_window(range)
                    .map(|(t, row)| {
                        (
                            t,
                            row.into_iter()
                                .filter_map(|(prop_id, p)| p.map(|p| (prop_id, p)))
                                .collect(),
                        )
                    })
                    .into_dyn_boxed()
            }
            None => node
                .as_ref()
                .temp_prop_rows()
                .map(|(t, row)| {
                    (
                        t,
                        row.into_iter()
                            .filter_map(|(prop_id, p)| p.map(|p| (prop_id, p)))
                            .collect(),
                    )
                })
                .into_dyn_boxed(),
        })
        .into_dyn_boxed()
    }

    fn edge_history<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: &'a LayerIds,
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
        layer_ids: &'a LayerIds,
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

    fn edge_exploded<'a>(&'a self, e: EdgeRef, layer_ids: &LayerIds) -> BoxedLIter<'a, EdgeRef> {
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

    fn edge_layers<'a>(&'a self, e: EdgeRef, layer_ids: &LayerIds) -> BoxedLIter<'a, EdgeRef> {
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
        layer_ids: &LayerIds,
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
        layer_ids: &'a LayerIds,
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
        layer_ids: &LayerIds,
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
        layer_ids: &LayerIds,
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

    fn temporal_node_prop_hist(&self, v: VID, id: usize) -> BoxedLIter<(TimeIndexEntry, Prop)> {
        let node = self.node_entry(v);
        GenLockedIter::from(node, |node| node.tprop(id).iter().into_dyn_boxed()).into_dyn_boxed()
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

    fn temporal_edge_prop_hist_window<'a>(
        &'a self,
        e: EdgeRef,
        prop_id: usize,
        start: i64,
        end: i64,
        layer_ids: &LayerIds,
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
        layer_ids: &LayerIds,
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
}

impl NodeHistoryFilter for GraphStorage {
    fn is_node_prop_update_available(
        &self,
        _prop_id: usize,
        _node_id: VID,
        _time: TimeIndexEntry,
    ) -> bool {
        // let nse = self.core_node_entry(node_id);
        // nse.tprop(prop_id).at(&time).is_some()
        true
    }

    fn is_node_prop_update_available_window(
        &self,
        _prop_id: usize,
        _node_id: VID,
        time: TimeIndexEntry,
        w: Range<i64>,
    ) -> bool {
        w.contains(&time.t())
    }

    fn is_node_prop_update_latest(
        &self,
        prop_id: usize,
        node_id: VID,
        time: TimeIndexEntry,
    ) -> bool {
        let nse = self.core_node_entry(node_id);
        let x = nse.tprop(prop_id).active(time.next()..TimeIndexEntry::MAX);
        !x
    }

    fn is_node_prop_update_latest_window(
        &self,
        prop_id: usize,
        node_id: VID,
        time: TimeIndexEntry,
        w: Range<i64>,
    ) -> bool {
        w.contains(&time.t()) && {
            let nse = self.core_node_entry(node_id);
            let x = nse
                .tprop(prop_id)
                .active(time.next()..TimeIndexEntry::start(w.end));
            !x
        }
    }
}

impl EdgeHistoryFilter for GraphStorage {
    fn is_edge_prop_update_available(
        &self,
        _layer_id: usize,
        _prop_id: usize,
        _edge_id: EID,
        _time: TimeIndexEntry,
    ) -> bool {
        // let nse = self.core_node_entry(node_id);
        // nse.tprop(prop_id).at(&time).is_some()
        true
    }

    fn is_edge_prop_update_available_window(
        &self,
        _layer_id: usize,
        _prop_id: usize,
        _edge_id: EID,
        time: TimeIndexEntry,
        w: Range<i64>,
    ) -> bool {
        w.contains(&time.t())
    }

    /// Latest Edge Property Update Semantics:
    /// Determines whether an edge property update at a given `time` is the latest across all layers.
    /// - If any update exists beyond the given `time` in any layer,
    ///   then the update at `time` is not considered the latest.
    /// - The "latest" status is determined globally across layers, not per individual layer.
    fn is_edge_prop_update_latest(
        &self,
        prop_id: usize,
        edge_id: EID,
        time: TimeIndexEntry,
    ) -> bool {
        let time = time.next();
        let ese = self.core_edge(edge_id);
        let mut layer_ids: Box<dyn Iterator<Item = usize>> =
            self.base().layer_ids_iter(self.base());

        // Check if any layer has an active update beyond `time`
        let has_future_update = layer_ids.any(|layer_id| {
            ese.temporal_prop_layer(layer_id, prop_id)
                .active(time..TimeIndexEntry::MAX)
        });

        // If no layer has a future update, return true
        !has_future_update
    }

    fn is_edge_prop_update_latest_window(
        &self,
        prop_id: usize,
        edge_id: EID,
        time: TimeIndexEntry,
        w: Range<i64>,
    ) -> bool {
        w.contains(&time.t()) && {
            let time = time.next();
            let ese = self.core_edge(edge_id);
            let mut layer_ids = self.base().layer_ids_iter(self.base());

            // Check if any layer has an active update beyond `time`
            let has_future_update = layer_ids.any(|layer_id| {
                ese.temporal_prop_layer(layer_id, prop_id)
                    .active(time..TimeIndexEntry::start(w.end))
            });

            // If no layer has a future update, return true
            !has_future_update
        }
    }
}

#[cfg(test)]
mod test_graph_storage {
    use crate::{core::Prop, db::api::view::StaticGraphViewOps, prelude::AdditionOps};

    fn init_graph_for_nodes_tests<G: StaticGraphViewOps + AdditionOps>(graph: G) -> G {
        graph
            .add_node(6, "N1", [("p1", Prop::U64(2u64))], None)
            .unwrap();
        graph
            .add_node(7, "N1", [("p1", Prop::U64(1u64))], None)
            .unwrap();

        graph
            .add_node(6, "N2", [("p1", Prop::U64(1u64))], None)
            .unwrap();
        graph
            .add_node(7, "N2", [("p1", Prop::U64(2u64))], None)
            .unwrap();

        graph
            .add_node(8, "N3", [("p1", Prop::U64(1u64))], None)
            .unwrap();

        graph
            .add_node(9, "N4", [("p1", Prop::U64(1u64))], None)
            .unwrap();

        graph
            .add_node(5, "N5", [("p1", Prop::U64(1u64))], None)
            .unwrap();
        graph
            .add_node(6, "N5", [("p1", Prop::U64(2u64))], None)
            .unwrap();

        graph
            .add_node(5, "N6", [("p1", Prop::U64(1u64))], None)
            .unwrap();
        graph
            .add_node(6, "N6", [("p1", Prop::U64(1u64))], None)
            .unwrap();

        graph
            .add_node(3, "N7", [("p1", Prop::U64(1u64))], None)
            .unwrap();
        graph
            .add_node(5, "N7", [("p1", Prop::U64(1u64))], None)
            .unwrap();

        graph
            .add_node(3, "N8", [("p1", Prop::U64(1u64))], None)
            .unwrap();
        graph
            .add_node(4, "N8", [("p1", Prop::U64(2u64))], None)
            .unwrap();

        graph
    }

    fn init_graph_for_edges_tests<G: StaticGraphViewOps + AdditionOps>(graph: G) -> G {
        graph
            .add_edge(6, "N1", "N2", [("p1", Prop::U64(2u64))], Some("layer1"))
            .unwrap();
        graph
            .add_edge(7, "N1", "N2", [("p1", Prop::U64(1u64))], Some("layer2"))
            .unwrap();

        graph
            .add_edge(6, "N2", "N3", [("p1", Prop::U64(1u64))], Some("layer1"))
            .unwrap();
        graph
            .add_edge(7, "N2", "N3", [("p1", Prop::U64(2u64))], Some("layer2"))
            .unwrap();

        graph
            .add_edge(8, "N3", "N4", [("p1", Prop::U64(1u64))], Some("layer1"))
            .unwrap();

        graph
            .add_edge(9, "N4", "N5", [("p1", Prop::U64(1u64))], Some("layer1"))
            .unwrap();

        graph
            .add_edge(5, "N5", "N6", [("p1", Prop::U64(1u64))], Some("layer1"))
            .unwrap();
        graph
            .add_edge(6, "N5", "N6", [("p1", Prop::U64(2u64))], Some("layer2"))
            .unwrap();

        graph
            .add_edge(5, "N6", "N7", [("p1", Prop::U64(1u64))], Some("layer1"))
            .unwrap();
        graph
            .add_edge(6, "N6", "N7", [("p1", Prop::U64(1u64))], Some("layer2"))
            .unwrap();

        graph
            .add_edge(3, "N7", "N8", [("p1", Prop::U64(1u64))], Some("layer1"))
            .unwrap();
        graph
            .add_edge(5, "N7", "N8", [("p1", Prop::U64(1u64))], Some("layer2"))
            .unwrap();

        graph
            .add_edge(3, "N8", "N1", [("p1", Prop::U64(1u64))], Some("layer1"))
            .unwrap();
        graph
            .add_edge(4, "N8", "N1", [("p1", Prop::U64(2u64))], Some("layer2"))
            .unwrap();

        // TODO: Revisit this test after supporting secondary indexes
        // graph
        //     .add_edge(3, "N9", "N2", [("p1", Prop::U64(1u64))], Some("layer1"))
        //     .unwrap();
        // graph
        //     .add_edge(3, "N9", "N2", [("p1", Prop::U64(2u64))], Some("layer2"))
        //     .unwrap();

        graph
    }

    #[cfg(test)]
    mod test_node_history_filter_event_graph {
        use crate::{
            core::Prop,
            db::api::{
                storage::graph::storage_ops::time_semantics::test_graph_storage::init_graph_for_nodes_tests,
                view::{
                    internal::{CoreGraphOps, NodeHistoryFilter},
                    StaticGraphViewOps,
                },
            },
            prelude::{AdditionOps, Graph, GraphViewOps},
        };
        use raphtory_api::core::storage::timeindex::TimeIndexEntry;

        #[test]
        fn test_is_node_prop_update_latest() {
            let g = Graph::new();
            let g = init_graph_for_nodes_tests(g);

            let prop_id = g.node_meta().temporal_prop_meta().get_id("p1").unwrap();

            let node_id = g.node("N1").unwrap().node;
            let bool = g.is_node_prop_update_latest(prop_id, node_id, TimeIndexEntry::end(7));
            assert!(bool);

            let node_id = g.node("N2").unwrap().node;
            let bool = g.is_node_prop_update_latest(prop_id, node_id, TimeIndexEntry::end(6));
            assert!(!bool);

            let node_id = g.node("N3").unwrap().node;
            let bool = g.is_node_prop_update_latest(prop_id, node_id, TimeIndexEntry::end(8));
            assert!(bool);

            let node_id = g.node("N4").unwrap().node;
            let bool = g.is_node_prop_update_latest(prop_id, node_id, TimeIndexEntry::end(9));
            assert!(bool);

            let node_id = g.node("N5").unwrap().node;
            let bool = g.is_node_prop_update_latest(prop_id, node_id, TimeIndexEntry::end(5));
            assert!(!bool);

            let node_id = g.node("N6").unwrap().node;
            let bool = g.is_node_prop_update_latest(prop_id, node_id, TimeIndexEntry::end(5));
            assert!(!bool);
            let node_id = g.node("N6").unwrap().node;
            let bool = g.is_node_prop_update_latest(prop_id, node_id, TimeIndexEntry::end(6));
            assert!(bool);

            let node_id = g.node("N7").unwrap().node;
            let bool = g.is_node_prop_update_latest(prop_id, node_id, TimeIndexEntry::end(3));
            assert!(!bool);
            let node_id = g.node("N7").unwrap().node;
            let bool = g.is_node_prop_update_latest(prop_id, node_id, TimeIndexEntry::end(5));
            assert!(bool);

            let node_id = g.node("N8").unwrap().node;
            let bool = g.is_node_prop_update_latest(prop_id, node_id, TimeIndexEntry::end(3));
            assert!(!bool);
        }

        #[test]
        fn test_is_node_prop_update_latest_w() {
            let g = Graph::new();
            let g = init_graph_for_nodes_tests(g);

            let prop_id = g.node_meta().temporal_prop_meta().get_id("p1").unwrap();
            let w = 6..9;

            let node_id = g.node("N1").unwrap().node;
            let bool = g.is_node_prop_update_latest_window(
                prop_id,
                node_id,
                TimeIndexEntry::end(7),
                w.clone(),
            );
            assert!(bool);

            let node_id = g.node("N2").unwrap().node;
            let bool = g.is_node_prop_update_latest_window(
                prop_id,
                node_id,
                TimeIndexEntry::end(6),
                w.clone(),
            );
            assert!(!bool);

            let node_id = g.node("N3").unwrap().node;
            let bool = g.is_node_prop_update_latest_window(
                prop_id,
                node_id,
                TimeIndexEntry::end(8),
                w.clone(),
            );
            assert!(bool);

            let node_id = g.node("N4").unwrap().node;
            let bool = g.is_node_prop_update_latest_window(
                prop_id,
                node_id,
                TimeIndexEntry::end(9),
                w.clone(),
            );
            assert!(!bool);

            let node_id = g.node("N5").unwrap().node;
            let bool = g.is_node_prop_update_latest_window(
                prop_id,
                node_id,
                TimeIndexEntry::end(5),
                w.clone(),
            );
            assert!(!bool);

            let node_id = g.node("N6").unwrap().node;
            let bool = g.is_node_prop_update_latest_window(
                prop_id,
                node_id,
                TimeIndexEntry::end(5),
                w.clone(),
            );
            assert!(!bool);
            let node_id = g.node("N6").unwrap().node;
            let bool = g.is_node_prop_update_latest_window(
                prop_id,
                node_id,
                TimeIndexEntry::end(6),
                w.clone(),
            );
            assert!(bool);

            let node_id = g.node("N7").unwrap().node;
            let bool = g.is_node_prop_update_latest_window(
                prop_id,
                node_id,
                TimeIndexEntry::end(3),
                w.clone(),
            );
            assert!(!bool);
            let node_id = g.node("N7").unwrap().node;
            let bool = g.is_node_prop_update_latest_window(
                prop_id,
                node_id,
                TimeIndexEntry::end(5),
                w.clone(),
            );
            assert!(!bool);

            let node_id = g.node("N8").unwrap().node;
            let bool = g.is_node_prop_update_latest_window(
                prop_id,
                node_id,
                TimeIndexEntry::end(3),
                w.clone(),
            );
            assert!(!bool);
        }
    }

    #[cfg(test)]
    mod test_edge_history_filter_event_graph {
        use crate::{
            core::Prop,
            db::api::{
                storage::graph::storage_ops::time_semantics::test_graph_storage::init_graph_for_edges_tests,
                view::{
                    internal::{CoreGraphOps, EdgeHistoryFilter},
                    StaticGraphViewOps,
                },
            },
            prelude::{AdditionOps, Graph, GraphViewOps},
        };
        use raphtory_api::core::storage::timeindex::TimeIndexEntry;

        #[test]
        fn test_is_edge_prop_update_latest() {
            let g = Graph::new();
            let g = init_graph_for_edges_tests(g);

            let prop_id = g.edge_meta().temporal_prop_meta().get_id("p1").unwrap();

            let edge_id = g.edge("N1", "N2").unwrap().edge.pid();
            let bool = g.is_edge_prop_update_latest(prop_id, edge_id, TimeIndexEntry::end(7));
            assert!(bool);

            let edge_id = g.edge("N2", "N3").unwrap().edge.pid();
            let bool = g.is_edge_prop_update_latest(prop_id, edge_id, TimeIndexEntry::end(6));
            assert!(!bool);

            let edge_id = g.edge("N3", "N4").unwrap().edge.pid();
            let bool = g.is_edge_prop_update_latest(prop_id, edge_id, TimeIndexEntry::end(8));
            assert!(bool);

            let edge_id = g.edge("N4", "N5").unwrap().edge.pid();
            let bool = g.is_edge_prop_update_latest(prop_id, edge_id, TimeIndexEntry::end(9));
            assert!(bool);

            let edge_id = g.edge("N5", "N6").unwrap().edge.pid();
            let bool = g.is_edge_prop_update_latest(prop_id, edge_id, TimeIndexEntry::end(5));
            assert!(!bool);

            let edge_id = g.edge("N6", "N7").unwrap().edge.pid();
            let bool = g.is_edge_prop_update_latest(prop_id, edge_id, TimeIndexEntry::end(5));
            assert!(!bool);
            let edge_id = g.edge("N6", "N7").unwrap().edge.pid();
            let bool = g.is_edge_prop_update_latest(prop_id, edge_id, TimeIndexEntry::end(6));
            assert!(bool);

            let edge_id = g.edge("N7", "N8").unwrap().edge.pid();
            let bool = g.is_edge_prop_update_latest(prop_id, edge_id, TimeIndexEntry::end(3));
            assert!(!bool);
            let edge_id = g.edge("N7", "N8").unwrap().edge.pid();
            let bool = g.is_edge_prop_update_latest(prop_id, edge_id, TimeIndexEntry::end(5));
            assert!(bool);

            let edge_id = g.edge("N8", "N1").unwrap().edge.pid();
            let bool = g.is_edge_prop_update_latest(prop_id, edge_id, TimeIndexEntry::end(3));
            assert!(!bool);

            // TODO: Revisit this test after supporting secondary indexes
            // let edge_id = g.edge("N9", "N2").unwrap().edge.pid();
            // let bool = g.is_edge_prop_update_latest(prop_id, edge_id, TimeIndexEntry::end(3));
            // assert!(!bool);
        }

        #[test]
        fn test_is_edge_prop_update_latest_w() {
            let g = Graph::new();
            let g = init_graph_for_edges_tests(g);

            let prop_id = g.edge_meta().temporal_prop_meta().get_id("p1").unwrap();
            let w = 6..9;

            let edge_id = g.edge("N1", "N2").unwrap().edge.pid();
            let bool = g.is_edge_prop_update_latest_window(
                prop_id,
                edge_id,
                TimeIndexEntry::end(7),
                w.clone(),
            );
            assert!(bool);

            let edge_id = g.edge("N2", "N3").unwrap().edge.pid();
            let bool = g.is_edge_prop_update_latest_window(
                prop_id,
                edge_id,
                TimeIndexEntry::end(6),
                w.clone(),
            );
            assert!(!bool);

            let edge_id = g.edge("N3", "N4").unwrap().edge.pid();
            let bool = g.is_edge_prop_update_latest_window(
                prop_id,
                edge_id,
                TimeIndexEntry::end(8),
                w.clone(),
            );
            assert!(bool);

            let edge_id = g.edge("N4", "N5").unwrap().edge.pid();
            let bool = g.is_edge_prop_update_latest_window(
                prop_id,
                edge_id,
                TimeIndexEntry::end(9),
                w.clone(),
            );
            assert!(!bool);

            let edge_id = g.edge("N5", "N6").unwrap().edge.pid();
            let bool = g.is_edge_prop_update_latest_window(
                prop_id,
                edge_id,
                TimeIndexEntry::end(5),
                w.clone(),
            );
            assert!(!bool);

            let edge_id = g.edge("N6", "N7").unwrap().edge.pid();
            let bool = g.is_edge_prop_update_latest_window(
                prop_id,
                edge_id,
                TimeIndexEntry::end(5),
                w.clone(),
            );
            assert!(!bool);
            let edge_id = g.edge("N6", "N7").unwrap().edge.pid();
            let bool = g.is_edge_prop_update_latest_window(
                prop_id,
                edge_id,
                TimeIndexEntry::end(6),
                w.clone(),
            );
            assert!(bool);

            let edge_id = g.edge("N7", "N8").unwrap().edge.pid();
            let bool = g.is_edge_prop_update_latest_window(
                prop_id,
                edge_id,
                TimeIndexEntry::end(3),
                w.clone(),
            );
            assert!(!bool);
            let edge_id = g.edge("N7", "N8").unwrap().edge.pid();
            let bool = g.is_edge_prop_update_latest_window(
                prop_id,
                edge_id,
                TimeIndexEntry::end(5),
                w.clone(),
            );
            assert!(!bool);

            let edge_id = g.edge("N8", "N1").unwrap().edge.pid();
            let bool = g.is_edge_prop_update_latest_window(
                prop_id,
                edge_id,
                TimeIndexEntry::end(3),
                w.clone(),
            );
            assert!(!bool);

            // TODO: Revisit this test after supporting secondary indexes
            // let edge_id = g.edge("N9", "N2").unwrap().edge.pid();
            // let bool = g.is_edge_prop_update_latest_window(prop_id, edge_id, TimeIndexEntry::end(3), w.clone());
            // assert!(!bool);
        }
    }

    #[cfg(all(test, feature = "search"))]
    mod search_nodes {
        use super::*;
        use crate::{
            db::{
                api::view::SearchableGraphOps, graph::views::property_filter::CompositeNodeFilter,
            },
            prelude::{Graph, NodeViewOps, PropertyFilter},
        };

        #[test]
        fn test_search_nodes_latest() {
            let g = Graph::new();
            let g = init_graph_for_nodes_tests(g);
            let mut results = g
                .search_nodes_latest(
                    &CompositeNodeFilter::Property(PropertyFilter::eq("p1", 1u64)),
                    10,
                    0,
                )
                .expect("Failed to search for nodes")
                .into_iter()
                .map(|v| v.name())
                .collect::<Vec<_>>();
            results.sort();

            assert_eq!(results, vec!["N1", "N3", "N4", "N6", "N7"]);
        }
    }

    #[cfg(all(test, feature = "search"))]
    mod search_edges {
        use super::*;
        use crate::{
            db::{
                api::view::SearchableGraphOps, graph::views::property_filter::CompositeEdgeFilter,
            },
            prelude::{EdgeViewOps, Graph, NodeViewOps, PropertyFilter},
        };

        #[test]
        fn test_search_edges_latest() {
            let g = Graph::new();
            let g = init_graph_for_edges_tests(g);
            let mut results = g
                .search_edges_latest(
                    &CompositeEdgeFilter::Property(PropertyFilter::eq("p1", 1u64)),
                    10,
                    0,
                )
                .expect("Failed to search for nodes")
                .into_iter()
                .map(|e| format!("{}->{}", e.src().name(), e.dst().name()))
                .collect::<Vec<_>>();
            results.sort();

            assert_eq!(
                results,
                vec!["N1->N2", "N3->N4", "N4->N5", "N6->N7", "N7->N8"]
            );
        }
    }
}
