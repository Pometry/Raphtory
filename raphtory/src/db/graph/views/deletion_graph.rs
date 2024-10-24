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
/// it is considered active at any point in the window.
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
    let first_addition = additions.first();
    let first_deletion = deletions.first();
    let last_addition_before_start = additions.range_t(i64::MIN..t).last();
    let last_deletion_before_start = deletions.range_t(i64::MIN..t).last();

    let only_deleted = match (first_addition, first_deletion) {
        (Some(a), Some(d)) => d < a && d >= t.into(),
        (None, Some(d)) => d >= t.into(),
        (Some(_), None) => false,
        (None, None) => false,
    };
    // None is less than any value (see test below)
    only_deleted || last_addition_before_start > last_deletion_before_start
}

fn alive_at<
    A: TimeIndexOps<IndexType = TimeIndexEntry> + ?Sized,
    D: TimeIndexOps<IndexType = TimeIndexEntry> + ?Sized,
>(
    additions: &A,
    deletions: &D,
    t: i64,
) -> bool {
    let deleted_at_start = match (
        deletions.range_t(t..t.saturating_add(1)).first(),
        additions.range_t(t..t.saturating_add(1)).first(),
    ) {
        (Some(d), Some(a)) => d < a,
        (Some(_), None) => true,
        _ => false,
    };
    !deleted_at_start && alive_before(additions, deletions, t)
}

fn edge_alive_at_end(e: EdgeStorageRef, t: i64, layer_ids: LayerIds) -> bool {
    e.updates_iter(layer_ids)
        .any(|(_, additions, deletions)| alive_before(&additions, &deletions, t))
}

fn edge_alive_at_start(e: EdgeStorageRef, t: i64, layer_ids: LayerIds) -> bool {
    // The semantics are tricky here, an edge is not alive at the start of the window if the first event at time t is a deletion
    e.updates_iter(layer_ids)
        .any(|(_, additions, deletions)| alive_at(&additions, &deletions, t))
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
            .map(|t| t.min(end.saturating_sub(1)))
            .filter(|&t| t >= start)
    }

    fn node_earliest_time_window(&self, v: VID, start: i64, end: i64) -> Option<i64> {
        let v = self.core_node_entry(v);
        let additions = v.additions();
        if additions.first_t()? <= start {
            Some(additions.range_t(start..end).first_t().unwrap_or(start))
        } else {
            None
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
        _layer_ids: LayerIds,
    ) -> bool {
        node.additions().first_t().filter(|&t| t < w.end).is_some()
    }

    fn include_edge_window(
        &self,
        edge: EdgeStorageRef,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> bool {
        // includes edge if it is alive at the start of the window or added during the window
        edge.active(layer_ids.clone(), w.clone()) || edge_alive_at_start(edge, w.start, layer_ids)
    }

    fn node_history(&self, v: VID) -> Vec<i64> {
        self.0.node_history(v)
    }

    fn node_history_window(&self, v: VID, w: Range<i64>) -> Vec<i64> {
        self.0.node_history_window(v, w)
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
        self.0.edge_history_window(e, layer_ids, w)
    }

    fn edge_exploded_count(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> usize {
        match layer_ids {
            LayerIds::None => 0,
            LayerIds::All => (0..self.unfiltered_num_layers())
                .into_par_iter()
                .map(|id| self.edge_exploded_count(edge, &LayerIds::One(id)))
                .sum(),
            LayerIds::One(id) => {
                let additions = edge.additions(*id);
                let deletions = edge.deletions(*id);
                let a_first = additions.first().unwrap_or(TimeIndexEntry::MAX);
                let d_first = deletions.first().unwrap_or(TimeIndexEntry::MAX);
                if d_first < a_first {
                    additions.len() + 1
                } else {
                    additions.len()
                }
            }
            LayerIds::Multiple(layers) => layers
                .clone()
                .par_iter()
                .map(|id| self.edge_exploded_count(edge, &LayerIds::One(id)))
                .sum(),
        }
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
                let mut len = additions.range_t(w.clone()).len();
                if alive_at(&additions, &deletions, w.start) {
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

    fn edge_exploded<'a>(&'a self, e: EdgeRef, layer_ids: LayerIds) -> BoxedLIter<'a, EdgeRef> {
        let edge = self.0.core_edge(e.pid());

        let alive_layers: Vec<_> = edge
            .updates_iter(layer_ids.constrain_from_edge(e))
            .filter_map(
                |(l, additions, deletions)| match (additions.first(), deletions.first()) {
                    (Some(a), Some(d)) => (d < a).then_some(l),
                    (None, Some(_)) => Some(l),
                    _ => None,
                },
            )
            .collect();
        alive_layers
            .into_iter()
            .map(move |l| e.at(i64::MIN.into()).at_layer(l))
            .chain(self.0.edge_exploded(e, layer_ids))
            .into_dyn_boxed()
    }

    fn edge_layers<'a>(&'a self, e: EdgeRef, layer_ids: LayerIds) -> BoxedLIter<'a, EdgeRef> {
        self.0.edge_layers(e, layer_ids)
    }

    fn edge_window_exploded<'a>(
        &'a self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> BoxedLIter<'a, EdgeRef> {
        if w.end <= w.start {
            return Box::new(iter::empty());
        }
        let edge = self.0.core_edge(e.pid());

        let alive_layers: Vec<_> = edge
            .updates_iter(layer_ids.constrain_from_edge(e))
            .filter_map(|(l, additions, deletions)| {
                alive_at(&additions, &deletions, w.start).then_some(l)
            })
            .collect();
        alive_layers
            .into_iter()
            .map(move |l| e.at(w.start.into()).at_layer(l))
            .chain(self.0.edge_window_exploded(e, w, layer_ids))
            .into_dyn_boxed()
    }

    fn edge_window_layers<'a>(
        &'a self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> BoxedLIter<'a, EdgeRef> {
        let edge = self.core_edge(e.pid());
        Box::new(self.edge_layers(e, layer_ids).filter(move |&e| {
            self.include_edge_window(edge.as_ref(), w.clone(), LayerIds::One(e.layer().unwrap()))
        }))
    }

    fn edge_earliest_time(&self, e: EdgeRef, layer_ids: LayerIds) -> Option<i64> {
        e.time().map(|ti| ti.t()).or_else(|| {
            let entry = self.core_edge(e.pid());
            if edge_alive_at_start(entry.as_ref(), i64::MIN, layer_ids.clone()) {
                Some(i64::MIN)
            } else {
                entry
                    .additions_iter(layer_ids)
                    .map(|(_, a)| a.first_t())
                    .flatten()
                    .min()
            }
        })
    }

    fn edge_earliest_time_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> Option<i64> {
        let entry = self.core_edge(e.pid());
        if edge_alive_at_start(entry.as_ref(), w.start, layer_ids.clone()) {
            Some(w.start)
        } else {
            entry
                .additions_iter(layer_ids)
                .map(|(_, a)| a.range_t(w.clone()).first_t())
                .flatten()
                .min()
        }
    }

    fn edge_latest_time(&self, e: EdgeRef, layer_ids: LayerIds) -> Option<i64> {
        let edge = self.core_edge(e.pid());
        match e.time() {
            Some(t) => {
                let t_start = t.next();
                edge.updates_iter(layer_ids)
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
                if edge_alive_at_end(edge.as_ref(), i64::MAX, layer_ids.clone()) {
                    Some(i64::MAX)
                } else {
                    edge.deletions_iter(layer_ids)
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
        layer_ids: LayerIds,
    ) -> Option<i64> {
        let edge = self.core_edge(e.pid());
        match e.time().map(|ti| ti.t()) {
            Some(t) => {
                let t_start = t.saturating_add(1);
                edge.updates_par_iter(layer_ids)
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
                if edge_alive_at_end(entry.as_ref(), w.end, layer_ids.clone()) {
                    return Some(w.end - 1);
                }
                entry
                    .updates_par_iter(layer_ids)
                    .flat_map(|(_, additions, deletions)| {
                        let last_deletion = deletions.range_t(w.clone()).last()?;
                        if last_deletion.t() > w.start || additions.active_t(w.clone()) {
                            Some(last_deletion.t())
                        } else {
                            None
                        }
                    })
                    .max()
            }
        }
    }

    fn edge_deletion_history<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: LayerIds,
    ) -> BoxedLIter<'a, TimeIndexEntry> {
        let entry = self.core_edge(e.pid());
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
        layer_ids: LayerIds,
    ) -> BoxedLIter<'a, TimeIndexEntry> {
        let entry = self.core_edge(e.pid());
        GenLockedIter::from(entry, |entry| {
            entry
                .deletions_iter(layer_ids)
                .map(|(_, d)| d.into_range_t(w.clone()).into_iter())
                .kmerge()
                .into_dyn_boxed()
        })
        .into_dyn_boxed()
    }

    fn edge_is_valid(&self, e: EdgeRef, layer_ids: LayerIds) -> bool {
        let edge = self.0.core_edge(e.pid());
        let res = edge
            .updates_iter(layer_ids)
            .any(|(_, additions, deletions)| additions.last() > deletions.last());
        res
    }

    fn edge_is_valid_at_end(&self, e: EdgeRef, layer_ids: LayerIds, end: i64) -> bool {
        let edge = self.0.core_edge(e.pid());
        edge_alive_at_end(edge.as_ref(), end, layer_ids)
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
        self.0.temporal_prop_iter_window(prop_id, start, end)
    }

    #[inline]
    fn has_temporal_prop_window(&self, prop_id: usize, w: Range<i64>) -> bool {
        self.0.has_temporal_prop_window(prop_id, w)
    }

    fn temporal_prop_vec_window(&self, prop_id: usize, start: i64, end: i64) -> Vec<(i64, Prop)> {
        self.0.temporal_prop_vec_window(prop_id, start, end)
    }

    #[inline]
    fn has_temporal_node_prop(&self, v: VID, prop_id: usize) -> bool {
        self.0.has_temporal_node_prop(v, prop_id)
    }

    fn temporal_node_prop_hist(
        &self,
        v: VID,
        prop_id: usize,
    ) -> BoxedLIter<(TimeIndexEntry, Prop)> {
        self.0.temporal_node_prop_hist(v, prop_id)
    }

    fn has_temporal_node_prop_window(&self, v: VID, prop_id: usize, w: Range<i64>) -> bool {
        self.0
            .has_temporal_node_prop_window(v, prop_id, i64::MIN..w.end)
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

    fn has_temporal_edge_prop_window(
        &self,
        e: EdgeRef,
        prop_id: usize,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> bool {
        let entry = self.core_edge(e.pid());

        if (&entry).has_temporal_prop(layer_ids.clone(), prop_id) {
            // if property was added at any point since the last deletion, it is still there,
            // if deleted at the start of the window, we still need to check for any additions
            // that happened at the same time
            entry
                .updates_par_iter(layer_ids)
                .any(|(layer_id, _, deletions)| {
                    let search_start = deletions
                        .range_t(i64::MIN..w.start.saturating_add(1))
                        .last()
                        .unwrap_or(TimeIndexEntry::MIN)
                        .min(TimeIndexEntry::start(w.start));
                    let search_end = TimeIndexEntry::start(w.end);
                    entry
                        .temporal_prop_layer(layer_id, prop_id)
                        .iter_window_te(search_start..search_end)
                        .next()
                        .is_some()
                })
        } else {
            false
        }
    }

    fn temporal_edge_prop_hist_window<'a>(
        &'a self,
        e: EdgeRef,
        prop_id: usize,
        start: i64,
        end: i64,
        layer_ids: LayerIds,
    ) -> BoxedLIter<'a, (TimeIndexEntry, Prop)> {
        let entry = self.core_edge(e.pid());
        GenLockedIter::from(entry, |entry| {
            entry
                .temporal_prop_iter(layer_ids, prop_id)
                .map(|(l, prop)| {
                    let first_prop = prop
                        .last_before(TimeIndexEntry::start(start.saturating_add(1)))
                        .filter(|(t, _)| {
                            !entry
                                .deletions(l)
                                .active(*t..TimeIndexEntry::start(start.saturating_add(1)))
                        })
                        .map(|(_, v)| (TimeIndexEntry::start(start), v));
                    first_prop.into_iter().chain(
                        prop.iter_window(TimeIndexEntry::range(start.saturating_add(1)..end)),
                    )
                })
                .kmerge_by(|(t1, _), (t2, _)| t1 <= t2)
                .into_dyn_boxed()
        })
        .into_dyn_boxed()
    }

    fn temporal_edge_prop_at(
        &self,
        e: EdgeRef,
        id: usize,
        t: TimeIndexEntry,
        layer_ids: LayerIds,
    ) -> Option<Prop> {
        let entry = self.core_edge(e.pid());
        let res = entry
            .temporal_prop_iter(layer_ids, id)
            .filter_map(|(layer_id, prop)| {
                prop.last_before(t.next()) // inclusive
                    .filter(|(last_t, _)| !entry.deletions(layer_id).active(*last_t..t.next())) // check with inclusive window
                    .map(|(_, v)| v)
            })
            .next();
        res
    }

    fn has_temporal_edge_prop(&self, e: EdgeRef, prop_id: usize, layer_ids: LayerIds) -> bool {
        self.0.has_temporal_edge_prop(e, prop_id, layer_ids)
    }

    fn temporal_edge_prop_hist<'a>(
        &'a self,
        e: EdgeRef,
        prop_id: usize,
        layer_ids: LayerIds,
    ) -> BoxedLIter<'a, (TimeIndexEntry, Prop)> {
        self.0.temporal_edge_prop_hist(e, prop_id, layer_ids)
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
    };
    use itertools::Itertools;
    use raphtory_api::core::{entities::GID, utils::logging::global_info_logger};
    use tracing::info;

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
        assert_eq!(e.earliest_time().unwrap(), 1);
        assert_eq!(e.latest_time(), Some(i64::MAX));
        g.delete_edge(10, 1, 2, None).unwrap();
        assert_eq!(e.latest_time().unwrap(), 10);

        g.delete_edge(10, 3, 4, None).unwrap();
        let e = g.edge(3, 4).unwrap();
        assert_eq!(e.earliest_time(), Some(i64::MIN));
        assert_eq!(e.latest_time().unwrap(), 10);
        g.add_edge(1, 3, 4, [("test", "test")], None).unwrap();
        assert_eq!(e.latest_time().unwrap(), 10);
        assert_eq!(e.earliest_time().unwrap(), 1);
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

        //deletion before addition (edge exists from (-inf,1) and (1, inf)
        g.delete_edge(1, 1, 2, None).unwrap();
        let e_1_2 = g.add_edge(1, 1, 2, [("test", "test")], None).unwrap();

        //deletion after addition (edge exists only at 2)
        let e_3_4 = g.add_edge(2, 3, 4, [("test", "test")], None).unwrap();
        g.delete_edge(2, 3, 4, None).unwrap();

        assert_eq!(e_1_2.at(0).properties().get("test"), None);
        assert_eq!(e_1_2.at(1).properties().get("test").unwrap_str(), "test");
        assert_eq!(e_1_2.at(2).properties().get("test").unwrap_str(), "test");

        assert_eq!(e_3_4.at(0).properties().get("test"), None);
        assert_eq!(e_3_4.at(2).properties().get("test"), None); // TODO: should this show the property or not?
        assert_eq!(e_3_4.at(3).properties().get("test"), None);

        assert!(g.window(0, 1).has_edge(1, 2));
        assert!(!g.window(0, 2).has_edge(3, 4));
        assert!(g.window(1, 2).has_edge(1, 2));
        assert!(g.window(2, 3).has_edge(3, 4));
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

        for t in 0..11 {
            assert!(g.at(t).has_edge(1, 2));
        }

        assert!(e.is_valid());
        assert!(!e_layer_1.is_valid());
        assert!(e_layer_2.is_valid());
        assert!(!e_layer_1.at(10).is_valid());
        for t in 0..11 {
            assert!(e.at(t).is_valid());
        }
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

    #[test]
    fn test_explode_multiple_layers() {
        let g = PersistentGraph::new();
        g.delete_edge(1, 1, 2, Some("1")).unwrap();
        g.delete_edge(2, 1, 2, Some("2")).unwrap();
        g.delete_edge(3, 1, 2, Some("3")).unwrap();

        let e = g.edge(1, 2).unwrap();
        assert_eq!(e.explode().iter().count(), 3);
        assert_eq!(e.before(4).explode().iter().count(), 3);
        assert_eq!(e.window(2, 3).explode().iter().count(), 1);
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
    fn test_jira() {
        global_info_logger();
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
            .edges()
            .into_iter()
            .map(|vv| vv.id())
            .collect_vec();

        info!("windowed edges = {:?}", nodes);

        let nodes = g
            .window(0, 1701786285758)
            .layers(vec!["assigned", "has", "blocks"])
            .unwrap()
            .nodes()
            .into_iter()
            .map(|vv| vv.name())
            .collect_vec();

        info!("windowed nodes = {:?}", nodes);

        let nodes = g
            .at(1701786285758)
            .layers(vec!["assigned", "has", "blocks"])
            .unwrap()
            .edges()
            .into_iter()
            .map(|vv| vv.id())
            .collect_vec();
        info!("at edges = {:?}", nodes);

        let nodes = g
            .at(1701786285758)
            .layers(vec!["assigned", "has", "blocks"])
            .unwrap()
            .nodes()
            .into_iter()
            .map(|vv| vv.id())
            .collect_vec();

        info!("at nodes = {:?}", nodes);
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
}
