use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, LayerIds, VID},
        storage::timeindex::{AsTime, TimeIndexEntry, TimeIndexIntoOps, TimeIndexOps},
        utils::iter::GenLockedIter,
        Prop,
    },
    db::{
        api::{
            mutation::internal::InheritMutationOps,
            properties::internal::InheritPropertiesOps,
            storage::{
                graph::{
                    edges::{edge_ref::EdgeStorageRef, edge_storage_ops::EdgeStorageOps},
                    nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
                    storage_ops::GraphStorage,
                    tprop_storage_ops::TPropOps,
                },
                storage::Storage,
            },
            view::{internal::*, BoxedLIter, IntoDynBoxed},
        },
        graph::graph::graph_equal,
    },
    prelude::*,
};
use itertools::Itertools;
use raphtory_api::GraphType;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::{
    cmp::min,
    fmt::{Display, Formatter},
    iter,
    ops::Range,
    sync::Arc,
};

/// A graph view where an edge remains active from the time it is added until it is explicitly marked as deleted.
///
/// Note that the graph will give you access to all edges that were added at any point in time, even those that are marked as deleted.
/// The deletion only has an effect on the exploded edge view that are returned. An edge is included in a windowed view of the graph if
/// it is considered active at any point in the window. Note that this means that if the last event at the start of the window (by secondary index) is a deletion,
/// the edge is not considered active at the start of the window, even if there are simultaneous addition events.
///
///
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct PersistentGraph(pub(crate) Arc<Storage>);

impl Static for PersistentGraph {}

impl From<GraphStorage> for PersistentGraph {
    fn from(value: GraphStorage) -> Self {
        Self(Arc::new(Storage::from_inner(value)))
    }
}

impl Display for PersistentGraph {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

fn alive_before<
    A: TimeIndexOps<IndexType = TimeIndexEntry> + ?Sized,
    D: TimeIndexOps<IndexType = TimeIndexEntry> + ?Sized,
>(
    additions: &A,
    deletions: &D,
    t: i64,
) -> bool {
    let last_addition_before_start = additions.range_t(i64::MIN..t).last();
    let last_deletion_before_start = deletions.range_t(i64::MIN..t).last();
    last_addition_before_start > last_deletion_before_start
}

fn has_persisted_event<
    A: TimeIndexOps<IndexType = TimeIndexEntry> + ?Sized,
    D: TimeIndexOps<IndexType = TimeIndexEntry> + ?Sized,
>(
    additions: &A,
    deletions: &D,
    t: i64,
) -> bool {
    let active_at_start =
        deletions.active_t(t..t.saturating_add(1)) || additions.active_t(t..t.saturating_add(1));
    !active_at_start && alive_before(additions, deletions, t)
}

fn edge_alive_at_end(e: EdgeStorageRef, t: i64, layer_ids: &LayerIds) -> bool {
    e.updates_iter(layer_ids)
        .any(|(_, additions, deletions)| alive_before(&additions, &deletions, t))
}

fn edge_alive_at_start(e: EdgeStorageRef, t: i64, layer_ids: &LayerIds) -> bool {
    // The semantics are tricky here, an edge is not alive at the start of the window if the last event at time t is a deletion
    e.updates_iter(layer_ids)
        .any(|(_, additions, deletions)| alive_before(&additions, &deletions, t.saturating_add(1)))
}

/// Get the last update of a property before `t` (exclusive), taking deletions into account.
/// The update is only returned if the edge was not deleted since.
fn last_prop_value_before<'a>(
    t: TimeIndexEntry,
    props: impl TPropOps<'a>,
    deletions: impl TimeIndexOps<IndexType = TimeIndexEntry>,
) -> Option<(TimeIndexEntry, Prop)> {
    props
        .last_before(t) // inclusive
        .filter(|(last_t, _)| !deletions.active(*last_t..t))
}

/// Gets the potentially persisted property value at a point in time
///
/// Persisted value can only exist if there is no update at time `t` and the edge is not deleted at time `t`
/// and if it exists it is the last value of the property before `t` as computed by `last_prop_value_before`.
fn persisted_prop_value_at<'a>(
    t: i64,
    props: impl TPropOps<'a>,
    deletions: impl TimeIndexOps<IndexType = TimeIndexEntry>,
) -> Option<Prop> {
    if props.clone().active(t..t.saturating_add(1)) || deletions.active_t(t..t.saturating_add(1)) {
        None
    } else {
        last_prop_value_before(TimeIndexEntry::start(t), props, deletions).map(|(_, v)| v)
    }
}

/// Exclude anything from the window that happens before the last deletion at the start of the window
fn interior_window(
    w: Range<i64>,
    deletions: &impl TimeIndexOps<IndexType = TimeIndexEntry>,
) -> Range<TimeIndexEntry> {
    let start = deletions
        .range_t(w.start..w.start.saturating_add(1))
        .last()
        .map(|t| t.next())
        .unwrap_or(TimeIndexEntry::start(w.start));
    start..TimeIndexEntry::start(w.end)
}

impl PersistentGraph {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_storage(storage: Arc<Storage>) -> Self {
        Self(storage)
    }

    pub fn from_internal_graph(internal_graph: GraphStorage) -> Self {
        Self(Arc::new(Storage::from_inner(internal_graph)))
    }

    /// Get event graph
    pub fn event_graph(&self) -> Graph {
        Graph::from_storage(self.0.clone())
    }
    pub fn persistent_graph(&self) -> PersistentGraph {
        self.clone()
    }
}

impl<'graph, G: GraphViewOps<'graph>> PartialEq<G> for PersistentGraph {
    fn eq(&self, other: &G) -> bool {
        graph_equal(self, other)
    }
}

impl Base for PersistentGraph {
    type Base = Storage;
    #[inline(always)]
    fn base(&self) -> &Self::Base {
        &self.0
    }
}

impl InternalMaterialize for PersistentGraph {
    fn graph_type(&self) -> GraphType {
        GraphType::PersistentGraph
    }

    fn include_deletions(&self) -> bool {
        true
    }
}

impl DeletionOps for PersistentGraph {}

impl InheritMutationOps for PersistentGraph {}

impl InheritListOps for PersistentGraph {}

impl InheritCoreOps for PersistentGraph {}

impl HasDeletionOps for PersistentGraph {}

impl InheritPropertiesOps for PersistentGraph {}

impl InheritLayerOps for PersistentGraph {}

impl InheritEdgeFilterOps for PersistentGraph {}

impl InheritNodeFilterOps for PersistentGraph {}

impl TimeSemantics for PersistentGraph {
    fn node_earliest_time(&self, v: VID) -> Option<i64> {
        self.0.node_earliest_time(v)
    }

    fn node_latest_time(&self, _v: VID) -> Option<i64> {
        Some(i64::MAX)
    }

    fn view_start(&self) -> Option<i64> {
        self.0.view_start()
    }

    fn view_end(&self) -> Option<i64> {
        self.0.view_end()
    }

    fn earliest_time_global(&self) -> Option<i64> {
        self.0.earliest_time_global()
    }

    fn latest_time_global(&self) -> Option<i64> {
        self.0.latest_time_global()
    }

    fn earliest_time_window(&self, start: i64, end: i64) -> Option<i64> {
        self.earliest_time_global()
            .map(|t| t.max(start))
            .filter(|&t| t < end)
    }

    fn latest_time_window(&self, start: i64, end: i64) -> Option<i64> {
        if self.0.earliest_time_global()? >= end {
            return None;
        }
        self.latest_time_global()
            .map(|t| t.min(end.saturating_sub(1)).max(start))
    }

    fn node_earliest_time_window(&self, v: VID, start: i64, end: i64) -> Option<i64> {
        let v = self.core_node_entry(v);
        let additions = v.additions();
        if additions.first_t()? <= start {
            Some(start)
        } else {
            additions.range_t(start..end).first_t()
        }
    }

    fn node_latest_time_window(&self, v: VID, _start: i64, end: i64) -> Option<i64> {
        let v = self.core_node_entry(v);
        if v.additions().first_t()? < end {
            Some(end - 1)
        } else {
            None
        }
    }

    fn include_node_window(
        &self,
        node: NodeStorageRef,
        w: Range<i64>,
        _layer_ids: &LayerIds,
    ) -> bool {
        node.additions().first_t().filter(|&t| t < w.end).is_some()
    }

    fn include_edge_window(
        &self,
        edge: EdgeStorageRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> bool {
        // If an edge has any event in the interior (both end exclusive) of the window it is always included.
        // Additionally, the edge is included if the last event at or before the start of the window was an addition.
        edge.added(layer_ids, w.start.saturating_add(1)..w.end)
            || edge.deleted(layer_ids, w.start.saturating_add(1)..w.end)
            || edge_alive_at_start(edge, w.start, layer_ids)
    }

    fn node_history(&self, v: VID) -> BoxedLIter<'_, TimeIndexEntry> {
        self.0.node_history(v)
    }

    fn node_history_window(&self, v: VID, w: Range<i64>) -> BoxedLIter<'_, TimeIndexEntry> {
        self.0.node_history_window(v, w)
    }

    fn node_edge_history(&self, v: VID, w: Option<Range<i64>>) -> BoxedLIter<TimeIndexEntry> {
        self.0.node_edge_history(v, w)
    }

    fn node_history_rows(
        &self,
        v: VID,
        w: Option<Range<i64>>,
    ) -> BoxedLIter<(TimeIndexEntry, Vec<(usize, Prop)>)> {
        // if window exists, we need to add the first row before the window
        // FIXME: this doesn't work correctly for multiple updates at the start of the window
        if let Some(w) = w {
            let node = self.core_node_entry(v);
            let first_row = if node.additions().active_t(i64::MIN..w.start) {
                Some(
                    (0..self.node_meta().temporal_prop_meta().len())
                        .filter_map(|prop_id| {
                            let prop = node.tprop(prop_id);
                            if prop.active(w.start..w.start.saturating_add(1)) {
                                None
                            } else {
                                prop.last_before(TimeIndexEntry::start(w.start))
                                    .map(|(_, v)| (prop_id, v))
                            }
                        })
                        .collect_vec(),
                )
            } else {
                None
            };
            Box::new(
                first_row
                    .into_iter()
                    .map(move |row| (TimeIndexEntry::start(w.start), row))
                    .chain(self.0.node_history_rows(v, Some(w))),
            )
        } else {
            self.0.node_history_rows(v, w)
        }
    }

    fn node_property_history(&self, v: VID, w: Option<Range<i64>>) -> BoxedLIter<TimeIndexEntry> {
        self.0.node_property_history(v, w)
    }

    fn edge_history<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: &'a LayerIds,
    ) -> BoxedLIter<'a, TimeIndexEntry> {
        self.0.edge_history(e, layer_ids)
    }

    fn edge_history_window<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: &'a LayerIds,
        w: Range<i64>,
    ) -> BoxedLIter<'a, TimeIndexEntry> {
        let layer_ids = layer_ids.constrain_from_edge(e);
        let entry = self.core_edge(e.pid());
        GenLockedIter::from(entry, |entry| {
            entry
                .updates_iter(&layer_ids)
                .map(|(_, additions, deletions)| {
                    let window = interior_window(w.clone(), &deletions);
                    additions.into_range(window).into_iter()
                })
                .kmerge()
                .into_dyn_boxed()
        })
        .into_dyn_boxed()
    }

    fn edge_exploded_count(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> usize {
        self.0.edge_exploded_count(edge, layer_ids)
    }

    fn edge_exploded_count_window(
        &self,
        edge: EdgeStorageRef,
        layer_ids: &LayerIds,
        w: Range<i64>,
    ) -> usize {
        match layer_ids {
            LayerIds::None => 0,
            LayerIds::All => (0..self.unfiltered_num_layers())
                .into_par_iter()
                .map(|id| self.edge_exploded_count_window(edge, &LayerIds::One(id), w.clone()))
                .sum(),
            LayerIds::One(id) => {
                let additions = edge.additions(*id);
                let deletions = edge.deletions(*id);
                let actual_window = interior_window(w.clone(), &deletions);
                let mut len = additions.range(actual_window).len();
                if has_persisted_event(&additions, &deletions, w.start) {
                    len += 1
                }
                len
            }
            LayerIds::Multiple(layers) => layers
                .clone()
                .par_iter()
                .map(|id| self.edge_exploded_count(edge, &LayerIds::One(id)))
                .sum(),
        }
    }

    fn edge_exploded<'a>(&'a self, e: EdgeRef, layer_ids: &'a LayerIds) -> BoxedLIter<'a, EdgeRef> {
        self.0.edge_exploded(e, layer_ids)
    }

    fn edge_layers<'a>(&'a self, e: EdgeRef, layer_ids: &'a LayerIds) -> BoxedLIter<'a, EdgeRef> {
        self.0.edge_layers(e, layer_ids)
    }

    fn edge_window_exploded<'a>(
        &'a self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &'a LayerIds,
    ) -> BoxedLIter<'a, EdgeRef> {
        if w.end <= w.start {
            return Box::new(iter::empty());
        }
        let edge = self.0.core_edge(e.pid());
        let layer_ids = layer_ids.constrain_from_edge(e);

        let alive_layers: Vec<_> = edge
            .updates_iter(&layer_ids)
            .filter_map(|(l, additions, deletions)| {
                has_persisted_event(&additions, &deletions, w.start).then_some(l)
            })
            .collect();
        alive_layers
            .into_iter()
            .map(move |l| e.at(w.start.into()).at_layer(l))
            .chain(GenLockedIter::from(edge, move |edge| {
                edge.updates_iter(&layer_ids)
                    .map(move |(l, additions, deletions)| {
                        let window = interior_window(w.clone(), &deletions);
                        additions
                            .into_range(window)
                            .into_iter()
                            .map(move |t| e.at(t).at_layer(l))
                    })
                    .kmerge_by(|e1, e2| e1.time() <= e2.time())
                    .into_dyn_boxed()
            }))
            .into_dyn_boxed()
    }

    fn edge_window_layers<'a>(
        &'a self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &'a LayerIds,
    ) -> BoxedLIter<'a, EdgeRef> {
        let edge = self.core_edge(e.pid());
        Box::new(self.edge_layers(e, layer_ids).filter(move |&e| {
            self.include_edge_window(edge.as_ref(), w.clone(), &LayerIds::One(e.layer().unwrap()))
        }))
    }

    fn edge_earliest_time(&self, e: EdgeRef, layer_ids: &LayerIds) -> Option<i64> {
        e.time().map(|ti| ti.t()).or_else(|| {
            let entry = self.core_edge(e.pid());
            entry
                .additions_iter(layer_ids)
                .filter_map(|(_, a)| a.first_t())
                .chain(
                    entry
                        .deletions_iter(layer_ids)
                        .filter_map(|(_, d)| d.first_t()),
                )
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
                if edge_alive_at_start(entry.as_ref(), w.start, &layer_ids) {
                    Some(w.start)
                } else {
                    entry
                        .updates_iter(&layer_ids)
                        .flat_map(|(_, additions, deletions)| {
                            let window = interior_window(w.clone(), &deletions);
                            additions
                                .range(window.clone())
                                .first_t()
                                .into_iter()
                                .chain(deletions.range(window).first_t())
                        })
                        .min()
                }
            }
        }
    }

    fn edge_latest_time(&self, e: EdgeRef, layer_ids: &LayerIds) -> Option<i64> {
        let edge = self.core_edge(e.pid());
        match e.time() {
            Some(t) => {
                let t_start = t.next();
                edge.updates_iter(&layer_ids)
                    .map(|(_, a, d)| {
                        // last time for exploded edge is next addition or deletion
                        min(
                            a.range(t_start..TimeIndexEntry::MAX)
                                .first_t()
                                .unwrap_or(i64::MAX),
                            d.range(t_start..TimeIndexEntry::MAX)
                                .first_t()
                                .unwrap_or(i64::MAX),
                        )
                    })
                    .min()
            }
            None => {
                if edge_alive_at_end(edge.as_ref(), i64::MAX, &layer_ids) {
                    Some(i64::MAX)
                } else {
                    edge.deletions_iter(&layer_ids)
                        .map(|(_, d)| d.last_t())
                        .flatten()
                        .max()
                }
            }
        }
    }

    fn edge_latest_time_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> Option<i64> {
        let edge = self.core_edge(e.pid());
        let layer_ids = layer_ids.constrain_from_edge(e);
        match e.time().map(|ti| ti.t()) {
            Some(t) => {
                let t_start = t.saturating_add(1);
                edge.updates_iter(&layer_ids)
                    .map(|(_, a, d)| {
                        // last time for exploded edge is next addition or deletion
                        min(
                            a.range_t(t_start..w.end).first_t().unwrap_or(w.end - 1),
                            d.range_t(t_start..w.end).first_t().unwrap_or(w.end - 1),
                        )
                    })
                    .min()
            }
            None => {
                let entry = self.core_edge(e.pid());
                if edge_alive_at_end(entry.as_ref(), w.end, &layer_ids) {
                    return Some(w.end - 1);
                }
                // window for deletions has start exclusive
                entry
                    .deletions_iter(&layer_ids)
                    .filter_map(|(_, d)| {
                        d.range_t(w.start.saturating_add(1)..w.end)
                            .last_t()
                            .filter(|t| w.contains(t))
                    })
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

    /// Note that the window for deletions considers the start as exclusive for consistency with
    /// the other semantics (i.e., the actual window starts after the last deletion at the start)
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
                .map(|(_, d)| d.into_range_t(w.start.saturating_add(1)..w.end).into_iter())
                .kmerge()
                .into_dyn_boxed()
        })
        .into_dyn_boxed()
    }

    fn edge_is_valid(&self, e: EdgeRef, layer_ids: &LayerIds) -> bool {
        let edge = self.0.core_edge(e.pid());
        let res = edge
            .updates_iter(&layer_ids)
            .any(|(_, additions, deletions)| additions.last() > deletions.last());
        res
    }

    fn edge_is_valid_at_end(&self, e: EdgeRef, layer_ids: &LayerIds, end: i64) -> bool {
        let edge = self.0.core_edge(e.pid());
        edge_alive_at_end(edge.as_ref(), end, &layer_ids)
    }

    #[inline]
    fn has_temporal_prop(&self, prop_id: usize) -> bool {
        self.0.has_temporal_prop(prop_id)
    }

    fn temporal_prop_vec(&self, prop_id: usize) -> Vec<(i64, Prop)> {
        self.0.temporal_prop_vec(prop_id)
    }

    fn temporal_prop_iter(&self, prop_id: usize) -> BoxedLIter<(i64, Prop)> {
        self.0.temporal_prop_iter(prop_id)
    }

    fn temporal_prop_iter_window(
        &self,
        prop_id: usize,
        start: i64,
        end: i64,
    ) -> BoxedLIter<(i64, Prop)> {
        // FIXME: needs persistent semantics
        self.0.temporal_prop_iter_window(prop_id, start, end)
    }

    #[inline]
    fn has_temporal_prop_window(&self, prop_id: usize, w: Range<i64>) -> bool {
        self.0.has_temporal_prop_window(prop_id, w)
    }

    fn temporal_prop_vec_window(&self, prop_id: usize, start: i64, end: i64) -> Vec<(i64, Prop)> {
        // FIXME: needs persistent semantics
        self.0.temporal_prop_vec_window(prop_id, start, end)
    }
    fn temporal_node_prop_hist(
        &self,
        v: VID,
        prop_id: usize,
    ) -> BoxedLIter<(TimeIndexEntry, Prop)> {
        self.0.temporal_node_prop_hist(v, prop_id)
    }
    fn temporal_node_prop_hist_window(
        &self,
        v: VID,
        prop_id: usize,
        start: i64,
        end: i64,
    ) -> BoxedLIter<(TimeIndexEntry, Prop)> {
        let node = self.core_node_entry(v);
        GenLockedIter::from(node, move |node| {
            let prop = node.tprop(prop_id);
            prop.last_before(TimeIndexEntry::start(start.saturating_add(1)))
                .into_iter()
                .map(move |(_, v)| (TimeIndexEntry::start(start), v))
                .chain(prop.iter_window(TimeIndexEntry::range(start.saturating_add(1)..end)))
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
        let layer_ids = layer_ids.constrain_from_edge(e);
        match e.time() {
            Some(t) => GenLockedIter::from(entry, move |entry| {
                entry
                    .temporal_prop_iter(&layer_ids, prop_id)
                    .filter_map(move |(layer_id, props)| {
                        let actual_window = interior_window(start..end, &entry.deletions(layer_id));
                        if actual_window.contains(&t) {
                            last_prop_value_before(t.next(), props, entry.deletions(layer_id))
                                .map(|(_, v)| (t, v))
                        } else {
                            None
                        }
                    })
                    .into_dyn_boxed()
            })
            .into_dyn_boxed(),
            None => GenLockedIter::from(entry, |entry| {
                entry
                    .temporal_prop_iter(&layer_ids, prop_id)
                    .map(|(l, prop)| {
                        let first_prop =
                            persisted_prop_value_at(start, prop.clone(), entry.deletions(l))
                                .map(|v| (TimeIndexEntry::start(start), v));
                        first_prop.into_iter().chain(
                            prop.iter_window(interior_window(start..end, &entry.deletions(l))),
                        )
                    })
                    .kmerge_by(|(t1, _), (t2, _)| t1 <= t2)
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
        let layer_ids = layer_ids.constrain_from_edge(e);
        let res = entry
            .temporal_prop_iter(&layer_ids, id)
            .filter_map(|(layer_id, prop)| {
                last_prop_value_before(t.next(), prop, entry.deletions(layer_id))
                // check with inclusive window
            })
            .max_by_key(|(t, _)| *t)
            .map(|(_, v)| v);
        res
    }

    fn temporal_edge_prop_hist<'a>(
        &'a self,
        e: EdgeRef,
        prop_id: usize,
        layer_ids: &LayerIds,
    ) -> BoxedLIter<'a, (TimeIndexEntry, Prop)> {
        self.0.temporal_edge_prop_hist(e, prop_id, layer_ids)
    }

    #[inline]
    fn constant_edge_prop(&self, e: EdgeRef, id: usize, layer_ids: &LayerIds) -> Option<Prop> {
        self.0.constant_edge_prop(e, id, layer_ids)
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
        let edge = self.core_edge(e.pid());
        let edge = edge.as_ref();
        match layer_ids {
            LayerIds::None => None,
            LayerIds::All => match self.unfiltered_num_layers() {
                0 => None,
                1 => {
                    if self.include_edge_window(edge, w, &LayerIds::One(0)) {
                        edge.constant_prop_layer(0, id)
                    } else {
                        None
                    }
                }
                _ => {
                    let mut values = edge
                        .layer_ids_iter(layer_ids)
                        .filter_map(|layer_id| {
                            if self.include_edge_window(edge, w.clone(), &LayerIds::One(layer_id)) {
                                edge.constant_prop_layer(layer_id, id)
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
                if self.include_edge_window(edge, w, &LayerIds::One(*layer_id)) {
                    edge.constant_prop_layer(*layer_id, id)
                } else {
                    None
                }
            }
            LayerIds::Multiple(_) => {
                let mut values = edge
                    .layer_ids_iter(layer_ids)
                    .filter_map(|layer_id| {
                        if self.include_edge_window(edge, w.clone(), &LayerIds::One(layer_id)) {
                            edge.constant_prop_layer(layer_id, id)
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

#[cfg(test)]
mod test_deletions {
    use crate::{
        db::{
            api::view::time::internal::InternalTimeOps,
            graph::{
                edge::EdgeView,
                graph::assert_graph_equal,
                views::deletion_graph::{PersistentGraph, TimeSemantics},
            },
        },
        prelude::*,
        test_storage,
        test_utils::{build_graph, build_graph_strat},
    };
    use itertools::Itertools;
    use proptest::{arbitrary::any, proptest};
    use raphtory_api::core::entities::GID;
    use std::ops::Range;

    #[test]
    fn test_nodes() {
        let g = PersistentGraph::new();

        g.add_edge(0, 1, 2, [("added", Prop::I64(0))], Some("assigned"))
            .unwrap();
        g.add_edge(1, 1, 3, [("added", Prop::I64(0))], Some("assigned"))
            .unwrap();
        g.add_edge(2, 4, 2, [("added", Prop::I64(0))], Some("has"))
            .unwrap();
        g.add_edge(3, 4, 2, [("added", Prop::I64(0))], Some("has"))
            .unwrap();
        g.add_edge(4, 5, 2, [("added", Prop::I64(0))], Some("blocks"))
            .unwrap();
        g.add_edge(5, 4, 5, [("added", Prop::I64(0))], Some("has"))
            .unwrap();
        g.add_edge(6, 6, 5, [("added", Prop::I64(0))], Some("assigned"))
            .unwrap();

        let nodes = g
            .window(0, 1701786285758)
            .layers(vec!["assigned", "has", "blocks"])
            .unwrap()
            .nodes()
            .into_iter()
            .map(|vv| vv.name())
            .collect_vec();

        assert_eq!(nodes, vec!["1", "2", "3", "4", "5", "6"]);

        let nodes = g
            .at(1701786285758)
            .layers(vec!["assigned", "has", "blocks"])
            .unwrap()
            .nodes()
            .into_iter()
            .map(|vv| vv.name())
            .collect_vec();

        assert_eq!(nodes, vec!["1", "2", "3", "4", "5", "6"]);
    }

    #[test]
    fn test_edge_deletions() {
        let g = PersistentGraph::new();

        g.add_edge(0, 0, 1, [("added", Prop::I64(0))], None)
            .unwrap();
        g.delete_edge(10, 0, 1, None).unwrap();

        assert_eq!(
            g.edges().id().collect::<Vec<_>>(),
            vec![(GID::U64(0), GID::U64(1))]
        );

        assert_eq!(
            g.window(1, 2).edges().id().collect::<Vec<_>>(),
            vec![(GID::U64(0), GID::U64(1))]
        );

        assert_eq!(g.window(1, 2).count_edges(), 1);

        assert_eq!(g.window(11, 12).count_edges(), 0);

        assert_eq!(
            g.window(1, 2)
                .edge(0, 1)
                .unwrap()
                .properties()
                .get("added")
                .unwrap_i64(),
            0
        );

        assert!(g.window(11, 12).edge(0, 1).is_none());

        assert_eq!(
            g.window(1, 2)
                .edge(0, 1)
                .unwrap()
                .properties()
                .temporal()
                .get("added")
                .unwrap()
                .iter()
                .collect_vec(),
            vec![(1, Prop::I64(0))]
        );

        assert_eq!(g.window(1, 2).node(0).unwrap().out_degree(), 1)
    }

    #[test]
    fn test_window_semantics() {
        let g = PersistentGraph::new();
        g.add_edge(1, 1, 2, [("test", "test")], None).unwrap();
        g.delete_edge(10, 1, 2, None).unwrap();

        assert_eq!(g.count_edges(), 1);

        assert_eq!(g.at(12).count_edges(), 0);
        assert_eq!(g.at(11).count_edges(), 0);
        assert_eq!(g.at(10).count_edges(), 0);
        assert_eq!(g.at(9).count_edges(), 1);
        assert_eq!(g.window(5, 9).count_edges(), 1);
        assert_eq!(g.window(5, 10).count_edges(), 1);
        assert_eq!(g.window(5, 11).count_edges(), 1);
        assert_eq!(g.window(10, 12).count_edges(), 0);
        assert_eq!(g.before(10).count_edges(), 1);
        assert_eq!(g.after(10).count_edges(), 0);
    }

    #[test]
    fn test_timestamps() {
        let g = PersistentGraph::new();
        let e = g.add_edge(1, 1, 2, [("test", "test")], None).unwrap();
        assert_eq!(e.earliest_time().unwrap(), 1); // time of first addition
        assert_eq!(e.latest_time(), Some(i64::MAX)); // not deleted so alive forever
        g.delete_edge(10, 1, 2, None).unwrap();
        assert_eq!(e.latest_time().unwrap(), 10); // deleted, so time of last deletion

        g.delete_edge(10, 3, 4, None).unwrap();
        let e = g.edge(3, 4).unwrap();
        assert_eq!(e.earliest_time(), Some(10)); // only deleted, earliest and latest time are the same
        assert_eq!(e.latest_time().unwrap(), 10);
        g.add_edge(1, 3, 4, [("test", "test")], None).unwrap();
        assert_eq!(e.latest_time().unwrap(), 10);
        assert_eq!(e.earliest_time().unwrap(), 1); // added so timestamp is now the first addition
    }

    #[test]
    fn test_materialize_only_deletion() {
        let g = PersistentGraph::new();
        g.delete_edge(1, 1, 2, None).unwrap();
        g.add_edge(2, 1, 2, NO_PROPS, None).unwrap();
        g.delete_edge(5, 1, 2, None).unwrap();
        g.delete_edge(10, 1, 2, None).unwrap();
        assert_eq!(
            g.window(0, 11).count_temporal_edges(),
            g.count_temporal_edges()
        );
        assert_graph_equal(&g.materialize().unwrap(), &g);
    }

    #[test]
    fn materialize_prop_test() {
        proptest!(|(graph_f in build_graph_strat(10, 10))| {
            let g = build_graph(graph_f).persistent_graph();
            let gm = g.materialize().unwrap();
            assert_graph_equal(&g, &gm);
        })
    }

    #[test]
    fn materialize_window_prop_test() {
        proptest!(|(graph_f in build_graph_strat(2, 1), w in any::<Range<i64>>())| {
            let g = build_graph(graph_f).persistent_graph();
            let gw = g.window(w.start, w.end);
            let gmw = gw.materialize().unwrap();
            assert_graph_equal(&gw, &gmw);
        })
    }

    #[test]
    fn materialize_broken_time() {
        let g = PersistentGraph::new();
        g.add_edge(
            -7868307470600541330,
            0,
            0,
            [("test", Prop::map([("x", "y")]))],
            Some("a"),
        )
        .unwrap();
        g.add_edge(
            -8675512464616562592,
            0,
            0,
            [("test", Prop::map([("z", "hi")]))],
            None,
        )
        .unwrap();
        g.edge(0, 0)
            .unwrap()
            .add_constant_properties([("other", "b")], None)
            .unwrap();
        let gw = g.window(-7549523977641994620, -995047120251067629);
        assert_graph_equal(&gw, &gw.materialize().unwrap())
    }

    #[test]
    fn test_materialize_window_start_before_node_add() {
        let g = PersistentGraph::new();
        g.add_node(-1, 0, [("test", "test")], None).unwrap();
        // g.add_node(5, 0, [("test", "blob")], None).unwrap();
        // g.add_edge(0, 0, 0, NO_PROPS, None).unwrap();
        let gw = g.window(-5, 8);
        let gmw = gw.materialize().unwrap();
        assert_graph_equal(&gw, &gmw);
    }

    #[test]
    fn test_materialize_constant_edge_props() {
        let g = PersistentGraph::new();
        g.add_edge(0, 1, 2, NO_PROPS, Some("a")).unwrap();
        let e = g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        e.add_constant_properties([("test", "test")], None).unwrap();
        g.delete_edge(1, 1, 2, None).unwrap();

        let gw = g.after(1);
        let gmw = gw.materialize().unwrap();
        assert_graph_equal(&gw, &gmw);
    }

    ///
    #[test]
    fn test_constant_properties_multiple_layers() {
        let g = PersistentGraph::new();
        g.add_edge(0, 1, 2, NO_PROPS, Some("a")).unwrap();
        let e = g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        e.add_constant_properties([("test", "test")], None).unwrap();
        g.delete_edge(1, 1, 2, None).unwrap();
        assert_eq!(
            g.edge(1, 2)
                .unwrap()
                .properties()
                .constant()
                .iter()
                .collect_vec(),
            [("test".into(), Prop::map([("_default", "test")]))]
        );
        let gw = g.after(1);
        assert!(gw
            .edge(1, 2)
            .unwrap()
            .properties()
            .constant()
            .iter()
            .next()
            .is_none());
        let g_before = g.before(1);
        assert_eq!(
            g_before
                .edge(1, 2)
                .unwrap()
                .properties()
                .constant()
                .iter()
                .collect_vec(),
            [("test".into(), Prop::map([("_default", "test")]))]
        );
    }

    #[test]
    fn test_materialize_window() {
        let g = PersistentGraph::new();
        g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        g.delete_edge(10, 1, 2, None).unwrap();

        let gm = g
            .window(3, 5)
            .materialize()
            .unwrap()
            .into_persistent()
            .unwrap();
        assert_graph_equal(&gm, &g.window(3, 5))
    }

    #[test]
    fn test_materialize_window_earliest_time() {
        let g = PersistentGraph::new();
        g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        g.delete_edge(10, 1, 2, None).unwrap();

        let ltg = g.latest_time_global();
        assert_eq!(ltg, Some(10));

        let wg = g.window(3, 5);

        let e = wg.edge(1, 2).unwrap();
        assert_eq!(e.earliest_time(), Some(3));
        assert_eq!(e.latest_time(), Some(4));
        let n1 = wg.node(1).unwrap();
        assert_eq!(n1.earliest_time(), Some(3));
        assert_eq!(n1.latest_time(), Some(4));
        let n2 = wg.node(2).unwrap();
        assert_eq!(n2.earliest_time(), Some(3));
        assert_eq!(n2.latest_time(), Some(4));

        let actual_lt = wg.latest_time();
        assert_eq!(actual_lt, Some(4));

        let actual_et = wg.earliest_time();
        assert_eq!(actual_et, Some(3));

        let gm = g
            .window(3, 5)
            .materialize()
            .unwrap()
            .into_persistent()
            .unwrap();

        let expected_et = gm.earliest_time();
        assert_eq!(actual_et, expected_et);
    }

    #[test]
    fn test_materialize_window_node_props() {
        let g = Graph::new();
        g.add_node(0, 1, [("test", "test")], None).unwrap();

        test_storage!(&g, |g| {
            let g = g.persistent_graph();

            let wg = g.window(3, 5);
            let mg = wg.materialize().unwrap();
            assert_graph_equal(&wg, &mg);
        });
    }

    #[test]
    fn test_exploded_latest_time() {
        let g = PersistentGraph::new();
        let e = g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        g.delete_edge(10, 1, 2, None).unwrap();
        assert_eq!(e.latest_time(), Some(10));
        assert_eq!(e.explode().latest_time().collect_vec(), vec![Some(10)]);
    }

    #[test]
    fn test_exploded_window() {
        let g = PersistentGraph::new();
        let e = g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        for t in [5, 10, 15] {
            e.add_updates(t, NO_PROPS, None).unwrap();
        }
        assert_eq!(
            e.after(2).explode().time().flatten().collect_vec(),
            [3, 5, 10, 15]
        );
    }

    #[test]
    fn test_edge_properties() {
        let g = PersistentGraph::new();
        let e = g.add_edge(0, 1, 2, [("test", "test")], None).unwrap();
        assert_eq!(e.properties().get("test").unwrap_str(), "test");
        e.delete(10, None).unwrap();
        assert_eq!(e.properties().get("test").unwrap_str(), "test");
        assert_eq!(e.at(10).properties().get("test"), None);
        e.add_updates(11, [("test", "test11")], None).unwrap();
        assert_eq!(
            e.window(10, 12).properties().get("test").unwrap_str(),
            "test11"
        );
        assert_eq!(
            e.window(5, 12)
                .properties()
                .temporal()
                .get("test")
                .unwrap()
                .iter()
                .collect_vec(),
            vec![(5, Prop::str("test")), (11i64, Prop::str("test11"))],
        );
    }

    #[test]
    fn test_edge_history() {
        let g = PersistentGraph::new();
        let e = g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        e.delete(5, None).unwrap();
        e.add_updates(10, NO_PROPS, None).unwrap();
        assert_eq!(e.history(), [0, 10]);
        assert_eq!(e.after(1).history(), [10]);
        assert!(e.window(1, 4).history().is_empty());

        // exploded edge still exists
        assert_eq!(
            e.window(1, 4)
                .explode()
                .earliest_time()
                .flatten()
                .collect_vec(),
            [1]
        );
    }

    #[test]
    fn test_ordering_of_addition_and_deletion() {
        let g = PersistentGraph::new();

        //deletion before addition (deletion has no effect and edge exists (1, inf)
        g.delete_edge(1, 1, 2, None).unwrap();
        let e_1_2 = g.add_edge(1, 1, 2, [("test", "test")], None).unwrap();

        //deletion after addition (edge exists only at 2)
        let e_3_4 = g.add_edge(2, 3, 4, [("test", "test")], None).unwrap();
        g.delete_edge(2, 3, 4, None).unwrap();

        assert_eq!(e_1_2.at(0).properties().get("test"), None);
        assert_eq!(e_1_2.at(1).properties().get("test").unwrap_str(), "test");
        assert_eq!(e_1_2.at(2).properties().get("test").unwrap_str(), "test");

        assert_eq!(e_3_4.at(0).properties().get("test"), None);
        assert_eq!(e_3_4.at(2).properties().get("test"), None);
        assert_eq!(e_3_4.at(3).properties().get("test"), None);

        assert!(!g.window(0, 1).has_edge(1, 2));
        assert!(!g.window(0, 2).has_edge(3, 4));
        assert!(g.window(1, 2).has_edge(1, 2));
        assert!(!g.window(2, 3).has_edge(3, 4)); // deleted at start of window
        assert!(!g.window(3, 4).has_edge(3, 4));
    }

    #[test]
    fn test_deletions() {
        let edges = [
            (1, 1, 2),
            (2, 1, 3),
            (-1, 2, 1),
            (0, 1, 1),
            (7, 3, 2),
            (1, 1, 1),
        ];
        let g = PersistentGraph::new();
        for (t, s, d) in edges.iter() {
            g.add_edge(*t, *s, *d, NO_PROPS, None).unwrap();
        }
        g.delete_edge(10, edges[0].1, edges[0].2, None).unwrap();

        for (t, s, d) in &edges {
            assert!(g.at(*t).has_edge(*s, *d));
        }
        assert!(!g.after(10).has_edge(edges[0].1, edges[0].2));
        for (_, s, d) in &edges[1..] {
            assert!(g.after(10).has_edge(*s, *d));
        }
        assert_eq!(
            g.edge(edges[0].1, edges[0].2)
                .unwrap()
                .explode()
                .latest_time()
                .collect_vec(),
            [Some(10)]
        );
    }

    fn check_valid<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>>(e: &EdgeView<G, GH>) {
        assert!(e.is_valid());
        assert!(!e.is_deleted());
        assert!(e.graph.has_edge(e.src(), e.dst()));
        assert!(e.graph.edge(e.src(), e.dst()).is_some());
    }

    fn check_deleted<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>>(
        e: &EdgeView<G, GH>,
    ) {
        assert!(!e.is_valid());
        assert!(e.is_deleted());
        let t = e.latest_time().unwrap_or(i64::MAX);
        let g = e.graph.at(t); // latest view of the graph
        assert!(!g.has_edge(e.src(), e.dst()));
        assert!(g.edge(e.src(), e.dst()).is_none());
    }

    #[test]
    fn test_deletion_multiple_layers() {
        let g = PersistentGraph::new();

        g.add_edge(1, 1, 2, NO_PROPS, Some("1")).unwrap();
        g.delete_edge(2, 1, 2, Some("2")).unwrap();
        g.delete_edge(10, 1, 2, Some("1")).unwrap();
        g.add_edge(10, 1, 2, NO_PROPS, Some("2")).unwrap();

        let e = g.edge(1, 2).unwrap();
        let e_layer_1 = e.layers("1").unwrap();
        let e_layer_2 = e.layers("2").unwrap();

        assert!(!g.at(0).has_edge(1, 2));
        check_deleted(&e.at(0));
        for t in 1..13 {
            assert!(g.at(t).has_edge(1, 2));
            check_valid(&e.at(t));
        }

        check_valid(&e);

        check_deleted(&e_layer_1);
        check_deleted(&e_layer_1.at(10));
        check_valid(&e_layer_1.at(9));
        check_valid(&e_layer_1.at(1));
        check_deleted(&e_layer_1.at(0));

        check_valid(&e_layer_2);
        check_deleted(&e_layer_2.at(9));
        check_valid(&e_layer_2.at(10));
    }

    #[test]
    fn test_edge_is_valid() {
        let g = PersistentGraph::new();

        g.add_edge(1, 1, 2, NO_PROPS, None).unwrap();
        let e = g.edge(1, 2).unwrap();
        check_deleted(&e.before(1));
        check_valid(&e.after(1));
        check_valid(&e);

        g.add_edge(2, 1, 2, NO_PROPS, Some("1")).unwrap();
        check_valid(&e);

        g.delete_edge(3, 1, 2, Some("1")).unwrap();
        check_valid(&e);
        check_deleted(&e.layers("1").unwrap());
        check_deleted(&e.layers("1").unwrap().at(3));
        check_deleted(&e.layers("1").unwrap().after(3));
        check_valid(&e.layers("1").unwrap().before(3));
        check_valid(&e.default_layer());

        g.delete_edge(4, 1, 2, None).unwrap();
        check_deleted(&e);
        check_deleted(&e.layers("1").unwrap());
        check_deleted(&e.default_layer());

        g.add_edge(5, 1, 2, NO_PROPS, None).unwrap();
        check_valid(&e);
        check_valid(&e.default_layer());
        check_deleted(&e.layers("1").unwrap());
    }

    /// Each layer is handled individually, deletions in one layer do not have an effect on other layers.
    /// Layers that have only deletions are ignored
    #[test]
    fn test_explode_multiple_layers() {
        let g = PersistentGraph::new();
        g.add_edge(0, 1, 2, NO_PROPS, Some("1")).unwrap();
        g.add_edge(1, 1, 2, NO_PROPS, Some("2")).unwrap();
        g.delete_edge(1, 1, 2, Some("1")).unwrap();
        g.delete_edge(2, 1, 2, Some("2")).unwrap();
        g.delete_edge(3, 1, 2, Some("3")).unwrap();

        let e = g.edge(1, 2).unwrap();
        assert_eq!(e.explode().iter().count(), 2);
        assert_eq!(e.before(4).explode().iter().count(), 2);
        assert_eq!(e.window(1, 3).explode().iter().count(), 1);
        assert_eq!(e.window(2, 3).explode().iter().count(), 0);
    }

    #[test]
    fn test_edge_latest_time() {
        let g = PersistentGraph::new();
        let e = g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        e.delete(2, None).unwrap();
        assert_eq!(e.at(2).earliest_time(), None);
        assert_eq!(e.at(2).latest_time(), None);
        assert!(e.at(2).is_deleted());
        assert_eq!(e.latest_time(), Some(2));
        e.add_updates(4, NO_PROPS, None).unwrap();
        assert_eq!(e.latest_time(), Some(i64::MAX));

        assert_eq!(e.window(0, 3).latest_time(), Some(2));
    }

    #[test]
    fn test_view_start_end() {
        let g = PersistentGraph::new();
        let e = g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        assert_eq!(g.start(), None);
        assert_eq!(g.timeline_start(), Some(0));
        assert_eq!(g.end(), None);
        assert_eq!(g.timeline_end(), Some(1));
        e.delete(2, None).unwrap();
        assert_eq!(g.timeline_start(), Some(0));
        assert_eq!(g.timeline_end(), Some(3));
        let w = g.window(g.timeline_start().unwrap(), g.timeline_end().unwrap());
        assert!(g.has_edge(1, 2));
        assert!(w.has_edge(1, 2));
        assert_eq!(w.start(), Some(0));
        assert_eq!(w.timeline_start(), Some(0));
        assert_eq!(w.end(), Some(3));
        assert_eq!(w.timeline_end(), Some(3));

        e.add_updates(4, NO_PROPS, None).unwrap();
        assert_eq!(g.timeline_start(), Some(0));
        assert_eq!(g.timeline_end(), Some(5));
    }

    #[test]
    fn test_node_property_semantics() {
        let g = PersistentGraph::new();
        let _v = g
            .add_node(1, 1, [("test_prop", "test value")], None)
            .unwrap();
        let v = g
            .add_node(11, 1, [("test_prop", "test value 2")], None)
            .unwrap();
        let v_from_graph = g.at(10).node(1).unwrap();
        assert_eq!(v.properties().get("test_prop").unwrap_str(), "test value 2");
        assert_eq!(
            v.at(10).properties().get("test_prop").unwrap_str(),
            "test value"
        );
        assert_eq!(
            v.at(11).properties().get("test_prop").unwrap_str(),
            "test value 2"
        );
        assert_eq!(
            v_from_graph.properties().get("test_prop").unwrap_str(),
            "test value"
        );

        assert_eq!(
            v.before(11).properties().get("test_prop").unwrap_str(),
            "test value"
        );

        assert_eq!(
            v.properties()
                .temporal()
                .get("test_prop")
                .unwrap()
                .history()
                .collect_vec(),
            [1, 11]
        );
        assert_eq!(
            v_from_graph
                .properties()
                .temporal()
                .get("test_prop")
                .unwrap()
                .history()
                .collect_vec(),
            [10]
        );

        assert_eq!(v_from_graph.earliest_time(), Some(10));
        assert_eq!(v.earliest_time(), Some(1));
        assert_eq!(v.at(10).earliest_time(), Some(10));
        assert_eq!(v.at(10).latest_time(), Some(10));
        assert_eq!(v.latest_time(), Some(i64::MAX));
    }

    #[test]
    fn test_event_graph() {
        let pg = PersistentGraph::new();
        pg.add_edge(0, 0, 1, [("added", Prop::I64(0))], None)
            .unwrap();
        pg.delete_edge(10, 0, 1, None).unwrap();
        assert_eq!(
            pg.edges().id().collect::<Vec<_>>(),
            vec![(GID::U64(0), GID::U64(1))]
        );

        let g = pg.event_graph();
        assert_eq!(
            g.edges().id().collect::<Vec<_>>(),
            vec![(GID::U64(0), GID::U64(1))]
        );
    }

    #[test]
    fn test_exploded_latest_time_deleted() {
        let g = PersistentGraph::new();
        g.add_edge(1, 1, 2, NO_PROPS, None).unwrap();
        g.delete_edge(1, 1, 2, None).unwrap();

        assert_eq!(
            g.edge(1, 2).unwrap().explode().latest_time().collect_vec(),
            [Some(1)]
        );
        assert_eq!(
            g.edge(1, 2)
                .unwrap()
                .explode()
                .earliest_time()
                .collect_vec(),
            [Some(1)]
        )
    }

    #[test]
    fn test_empty_window_has_no_nodes() {
        let g = PersistentGraph::new();
        g.add_node(1, 1, NO_PROPS, None).unwrap();
        assert_eq!(g.window(2, 2).count_nodes(), 0);
        assert_eq!(g.window(1, 1).count_nodes(), 0);
        assert_eq!(g.window(0, 0).count_nodes(), 0);
    }

    // #[test]
    // fn test_earliest_latest_only_deletion() {
    //     let g = PersistentGraph::new();
    //     g.delete_edge(1, 1, 2, None).unwrap();
    //     let gw = g.window(0, 1);
    //     assert_eq!(gw.earliest_time(), Some(0));
    //     assert_eq!(gw.latest_time(), Some(0));
    // }

    #[test]
    fn test_earliest_latest_time_window() {
        let g = PersistentGraph::new();
        g.add_edge(0, 0, 0, NO_PROPS, None).unwrap();

        assert_eq!(g.window(-1, 0).earliest_time(), None);
        assert_eq!(g.window(-1, 0).latest_time(), None);
    }

    #[test]
    fn test_node_earliest_time_window() {
        let g = PersistentGraph::new();
        g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        g.add_edge(4, 1, 3, NO_PROPS, None).unwrap();

        assert_eq!(g.window(2, 7).node(3).unwrap().earliest_time(), Some(4));
        assert_eq!(g.window(2, 7).node(1).unwrap().earliest_time(), Some(2));
    }

    #[test]
    fn test_exploded_edge_window() {
        let g = PersistentGraph::new();
        g.add_edge(1, 0, 1, [("test", 1i64)], None).unwrap();
        g.add_edge(3, 0, 1, [("test", 3i64)], None).unwrap();
        g.add_edge(4, 0, 1, [("test", 4i64)], None).unwrap();

        let prop_values = g
            .window(2, 5)
            .edges()
            .explode()
            .properties()
            .map(|props| props.get("test").unwrap_i64())
            .collect::<Vec<_>>();
        assert_eq!(prop_values, [1, 3, 4]);
    }

    /// This is a weird edge case
    ///
    /// An edge deletion creates the corresponding nodes so they should be alive from that point on.
    /// We might consider changing the exact semantics here...
    #[test]
    fn test_node_earliest_latest_time_edge_deletion_only() {
        let g = PersistentGraph::new();
        g.delete_edge(10, 0, 1, None).unwrap();
        assert_eq!(g.node(0).unwrap().earliest_time(), Some(10));
        assert_eq!(g.node(1).unwrap().earliest_time(), Some(10));
        assert_eq!(g.node(0).unwrap().latest_time(), Some(i64::MAX));
        assert_eq!(g.node(1).unwrap().latest_time(), Some(i64::MAX));
    }

    /// For an edge the earliest time is the time of the first update (either addition or deletion)
    ///
    /// The latest time is the time stamp of the last deletion if the last update is a deletion or
    /// i64::MAX otherwise.
    #[test]
    fn test_edge_earliest_latest_time_edge_deletion_only() {
        let g = PersistentGraph::new();
        g.delete_edge(10, 0, 1, None).unwrap();
        assert_eq!(g.edge(0, 1).unwrap().earliest_time(), Some(10));
        assert_eq!(g.edge(0, 1).unwrap().latest_time(), Some(10));
    }

    /// Repeated deletions are ignored, only the first one is relevant. Subsequent deletions do not
    /// create exploded edges
    #[test]
    fn test_repeated_deletions() {
        let g = PersistentGraph::new();
        g.add_edge(0, 0, 1, NO_PROPS, None).unwrap();
        g.delete_edge(2, 0, 1, None).unwrap();
        g.delete_edge(4, 0, 1, None).unwrap();

        let e = g.edge(0, 1).unwrap();
        let ex_earliest_t = e.explode().earliest_time().collect_vec();
        assert_eq!(ex_earliest_t, [Some(0)]);
    }

    /// Only additions create exploded edges
    #[test]
    fn test_only_deletions() {
        let g = PersistentGraph::new();
        g.delete_edge(2, 0, 1, None).unwrap();
        g.delete_edge(4, 0, 1, None).unwrap();

        let e = g.edge(0, 1).unwrap();
        let ex_earliest_t = e.explode().earliest_time().collect_vec();
        assert_eq!(ex_earliest_t, []);
    }

    /// Deletions only bring an edge into the window if they fall inside the window, not if they fall
    /// onto the start of the window. The edge is already considered deleted at the time of the
    /// deletion event, not at the next step.
    #[test]
    fn test_only_deletions_window() {
        let g = PersistentGraph::new();
        g.delete_edge(1, 0, 1, None).unwrap();

        // the edge exists
        assert!(g.has_edge(0, 1));

        // the deletion falls inside the window so the edge exists
        assert!(g.window(0, 2).has_edge(0, 1));

        // the deletion falls on the start of the window so the edge is already deleted and does not exist
        assert!(!g.window(1, 2).has_edge(0, 1));

        // windows that don't contain the deletion event don't have the edge
        assert!(!g.window(0, 1).has_edge(0, 1));
        assert!(!g.window(2, 3).has_edge(0, 1));
    }

    #[test]
    fn test_multiple_updates_at_start() {
        let g = PersistentGraph::new();
        g.add_edge(0, 0, 1, [("test", 1i64)], None).unwrap();
        g.add_edge(0, 0, 1, [("test", 2i64)], None).unwrap();

        let e = g.edge(0, 1).unwrap();
        assert_eq!(e.properties().get("test").unwrap_i64(), 2);
        assert_eq!(
            e.properties()
                .temporal()
                .get("test")
                .unwrap()
                .iter()
                .collect_vec(),
            [(0, Prop::I64(1)), (0, Prop::I64(2))]
        );

        assert_eq!(e.at(0).properties().get("test").unwrap_i64(), 2);
        assert_eq!(
            e.at(0)
                .properties()
                .temporal()
                .get("test")
                .unwrap()
                .iter()
                .collect_vec(),
            [(0, Prop::I64(1)), (0, Prop::I64(2))]
        );

        assert_eq!(
            e.at(0)
                .explode()
                .properties()
                .map(|p| p.get("test").unwrap_i64())
                .collect_vec(),
            [1, 2]
        );
        assert_eq!(e.at(0).history(), [0, 0]);
        assert_eq!(e.history(), [0, 0]);
    }

    #[test]
    fn no_persistence_if_updated_at_start() {
        let g = PersistentGraph::new();
        g.add_edge(0, 0, 1, [("test", 1i64)], None).unwrap();
        g.add_edge(2, 0, 1, [("test", 2i64)], None).unwrap();
        g.add_edge(4, 0, 1, [("test", 4i64)], None).unwrap();

        let e = g.edge(0, 1).unwrap().window(2, 5);

        assert_eq!(e.properties().get("test").unwrap_i64(), 4);
        assert_eq!(
            e.properties()
                .temporal()
                .get("test")
                .unwrap()
                .iter()
                .collect_vec(),
            [(2, Prop::I64(2)), (4, Prop::I64(4))]
        );
        assert_eq!(
            e.explode()
                .properties()
                .map(|p| p.get("test").unwrap_i64())
                .collect_vec(),
            [2, 4]
        );
        assert_eq!(e.history(), [2, 4]);
        assert!(e.deletions().is_empty());
    }

    #[test]
    fn persistence_if_not_updated_at_start() {
        let g = PersistentGraph::new();
        g.add_edge(0, 0, 1, [("test", 1i64)], None).unwrap();
        g.add_edge(2, 0, 1, [("test", 2i64)], None).unwrap();
        g.add_edge(4, 0, 1, [("test", 4i64)], None).unwrap();

        let e = g.edge(0, 1).unwrap().window(1, 5);

        assert_eq!(e.properties().get("test").unwrap_i64(), 4);
        assert_eq!(
            e.properties()
                .temporal()
                .get("test")
                .unwrap()
                .iter()
                .collect_vec(),
            [(1, Prop::I64(1)), (2, Prop::I64(2)), (4, Prop::I64(4))]
        );
        assert_eq!(
            e.explode()
                .properties()
                .map(|p| p.get("test").unwrap_i64())
                .collect_vec(),
            [1, 2, 4]
        );
        assert_eq!(e.history(), [2, 4]); // is this actually what we want?
        assert!(e.deletions().is_empty());
        assert_eq!(g.window(1, 5).count_temporal_edges(), 3);
        assert_eq!(g.window(2, 5).count_temporal_edges(), 2);
        assert_eq!(g.window(3, 5).count_temporal_edges(), 2);
    }

    #[test]
    fn no_persistence_if_deleted() {
        let g = PersistentGraph::new();
        g.add_edge(-1, 0, 1, [("test", 1i64)], None).unwrap();
        g.delete_edge(0, 0, 1, None).unwrap();
        g.add_edge(2, 0, 1, [("test", 2i64)], None).unwrap();
        g.add_edge(4, 0, 1, [("test", 4i64)], None).unwrap();

        let e = g.edge(0, 1).unwrap().window(1, 5);

        assert_eq!(e.properties().get("test").unwrap_i64(), 4);
        assert_eq!(
            e.properties()
                .temporal()
                .get("test")
                .unwrap()
                .iter()
                .collect_vec(),
            [(2, Prop::I64(2)), (4, Prop::I64(4))]
        );
        assert_eq!(
            e.explode()
                .properties()
                .map(|p| p.get("test").unwrap_i64())
                .collect_vec(),
            [2, 4]
        );
        assert_eq!(e.history(), [2, 4]);
        assert!(e.deletions().is_empty());
        assert_eq!(g.window(0, 5).count_temporal_edges(), 2);
        assert_eq!(g.window(1, 5).count_temporal_edges(), 2);
        assert_eq!(g.window(3, 5).count_temporal_edges(), 2);
    }

    #[test]
    fn test_deletion_at_start() {
        let g = PersistentGraph::new();
        g.add_edge(0, 0, 1, [("test", 1i64)], None).unwrap();
        g.add_edge(2, 0, 1, [("test", 2i64)], None).unwrap();
        g.delete_edge(2, 0, 1, None).unwrap();
        g.add_edge(2, 0, 1, [("test", 3i64)], None).unwrap();
        g.add_edge(4, 0, 1, [("test", 4i64)], None).unwrap();

        let e = g.edge(0, 1).unwrap().window(2, 5);

        assert_eq!(e.properties().get("test").unwrap_i64(), 4);
        assert_eq!(
            e.properties()
                .temporal()
                .get("test")
                .unwrap()
                .iter()
                .collect_vec(),
            [(2, Prop::I64(3)), (4, Prop::I64(4))]
        );

        assert_eq!(
            e.explode()
                .properties()
                .map(|p| p.get("test").unwrap_i64())
                .collect_vec(),
            [3, 4]
        );

        assert!(e.deletions().is_empty());
        assert_eq!(e.history(), [2, 4]);
        assert_eq!(g.window(1, 5).count_temporal_edges(), 4);
        assert_eq!(g.window(2, 5).count_temporal_edges(), 2);
        assert_eq!(g.window(3, 5).count_temporal_edges(), 2);
    }
}
