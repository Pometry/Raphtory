use crate::{
    core::{
        entities::LayerIds,
        storage::timeindex::{AsTime, TimeIndex, TimeIndexEntry, TimeIndexOps},
    },
    db::{
        api::{
            properties::internal::InheritPropertiesOps, storage::storage::Storage,
            view::internal::*,
        },
        graph::graph::graph_equal,
    },
    prelude::*,
};
use raphtory_api::{
    core::entities::{properties::tprop::TPropOps, EID, VID},
    inherit::Base,
    iter::{BoxedLIter, IntoDynBoxed},
    GraphType,
};
use raphtory_core::utils::iter::GenLockedIter;
use raphtory_storage::{
    graph::{
        edges::edge_storage_ops::EdgeStorageOps, graph::GraphStorage,
        nodes::node_storage_ops::NodeStorageOps,
    },
    mutation::InheritMutationOps,
};
use std::{
    fmt::{Display, Formatter},
    iter,
    ops::{Deref, Range},
    path::Path,
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
#[derive(Clone, Debug, Default)]
pub struct PersistentGraph(pub(crate) Arc<Storage>);

impl Static for PersistentGraph {}

impl From<GraphStorage> for PersistentGraph {
    fn from(value: GraphStorage) -> Self {
        Self(Arc::new(Storage::from_inner(value)))
    }
}

impl From<Arc<Storage>> for PersistentGraph {
    fn from(value: Arc<Storage>) -> Self {
        Self(value)
    }
}

impl Display for PersistentGraph {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

/// Get the last update of a property before `t` (exclusive), taking deletions into account.
/// The update is only returned if the edge was not deleted since.
fn last_prop_value_before<'a>(
    t: TimeIndexEntry,
    props: impl TPropOps<'a>,
    deletions: impl TimeIndexOps<'a, IndexType = TimeIndexEntry>,
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
    deletions: impl TimeIndexOps<'a, IndexType = TimeIndexEntry>,
) -> Option<Prop> {
    if props.active_t(t..t.saturating_add(1)) || deletions.active_t(t..t.saturating_add(1)) {
        None
    } else {
        last_prop_value_before(TimeIndexEntry::start(t), props, deletions).map(|(_, v)| v)
    }
}

impl PersistentGraph {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_at_path(path: impl AsRef<Path>) -> Self {
        Self(Arc::new(Storage::new_at_path(path)))
    }

    pub fn load_from_path(path: impl AsRef<Path>) -> Self {
        Self(Arc::new(Storage::load_from(path)))
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

impl InheritStorageOps for PersistentGraph {}

impl InternalMaterialize for PersistentGraph {
    fn graph_type(&self) -> GraphType {
        GraphType::PersistentGraph
    }
}

impl InheritMutationOps for PersistentGraph {}

impl InheritListOps for PersistentGraph {}

impl InheritCoreGraphOps for PersistentGraph {}

impl InheritPropertiesOps for PersistentGraph {}

impl InheritLayerOps for PersistentGraph {}

impl InheritAllEdgeFilterOps for PersistentGraph {}

impl InheritNodeFilterOps for PersistentGraph {}

impl GraphTimeSemanticsOps for PersistentGraph {
    fn node_time_semantics(&self) -> TimeSemantics {
        TimeSemantics::persistent()
    }

    fn edge_time_semantics(&self) -> TimeSemantics {
        TimeSemantics::persistent()
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

    #[inline]
    fn has_temporal_prop(&self, prop_id: usize) -> bool {
        self.0.has_temporal_prop(prop_id)
    }

    fn temporal_prop_iter(&self, prop_id: usize) -> BoxedLIter<'_, (TimeIndexEntry, Prop)> {
        self.0.temporal_prop_iter(prop_id)
    }

    #[inline]
    fn has_temporal_prop_window(&self, prop_id: usize, w: Range<i64>) -> bool {
        self.temporal_prop_iter_window(prop_id, w.start, w.end)
            .next()
            .is_some()
    }

    fn temporal_prop_iter_window(
        &self,
        prop_id: usize,
        start: i64,
        end: i64,
    ) -> BoxedLIter<'_, (TimeIndexEntry, Prop)> {
        if let Some(prop) = self.graph_meta().get_temporal_prop(prop_id) {
            let first = persisted_prop_value_at(start, &*prop, &TimeIndex::Empty)
                .map(|v| (TimeIndexEntry::start(start), v));
            first
                .into_iter()
                .chain(GenLockedIter::from(prop, |prop| {
                    prop.deref()
                        .iter_window(TimeIndexEntry::range(start..end))
                        .into_dyn_boxed()
                }))
                .into_dyn_boxed()
        } else {
            iter::empty().into_dyn_boxed()
        }
    }

    fn temporal_prop_iter_window_rev(
        &self,
        prop_id: usize,
        start: i64,
        end: i64,
    ) -> BoxedLIter<'_, (TimeIndexEntry, Prop)> {
        if let Some(prop) = self.graph_meta().get_temporal_prop(prop_id) {
            let first = persisted_prop_value_at(start, &*prop, &TimeIndex::Empty)
                .map(|v| (TimeIndexEntry::start(start), v));
            let iter = GenLockedIter::from(prop, |prop| {
                prop.deref()
                    .iter_window_rev(TimeIndexEntry::range(start..end))
                    .into_dyn_boxed()
            });

            iter.into_iter().chain(first).into_dyn_boxed()
        } else {
            iter::empty().into_dyn_boxed()
        }
    }

    fn temporal_prop_last_at(
        &self,
        prop_id: usize,
        t: TimeIndexEntry,
    ) -> Option<(TimeIndexEntry, Prop)> {
        self.0.temporal_prop_last_at(prop_id, t)
    }

    fn temporal_prop_last_at_window(
        &self,
        prop_id: usize,
        t: TimeIndexEntry,
        w: Range<i64>,
    ) -> Option<(TimeIndexEntry, Prop)> {
        let w = TimeIndexEntry::range(w);
        if w.contains(&t) {
            self.0
                .temporal_prop_last_at(prop_id, t)
                .map(|(t, v)| (t.max(w.start), v))
        } else {
            None
        }
    }
}

impl NodeHistoryFilter for PersistentGraph {
    fn is_node_prop_update_available(
        &self,
        _prop_id: usize,
        _node_id: VID,
        _time: TimeIndexEntry,
    ) -> bool {
        true
    }

    fn is_node_prop_update_available_window(
        &self,
        prop_id: usize,
        node_id: VID,
        time: TimeIndexEntry,
        w: Range<i64>,
    ) -> bool {
        if time.t() >= w.end {
            false
        } else if w.contains(&time.t()) {
            true
        } else {
            let nse = self.0.core_node(node_id);
            let x = nse
                .tprop(prop_id)
                .last_before(TimeIndexEntry::start(w.start))
                .map(|(t, _)| t.t().eq(&time.t()))
                .unwrap_or(false);
            x
        }
    }

    fn is_node_prop_update_latest(
        &self,
        prop_id: usize,
        node_id: VID,
        time: TimeIndexEntry,
    ) -> bool {
        self.0.is_node_prop_update_latest(prop_id, node_id, time)
    }

    fn is_node_prop_update_latest_window(
        &self,
        prop_id: usize,
        node_id: VID,
        time: TimeIndexEntry,
        w: Range<i64>,
    ) -> bool {
        time.t() < w.end && {
            let nse = self.0.core_node(node_id);
            let x = nse
                .tprop(prop_id)
                .active(time.next()..TimeIndexEntry::start(w.end));
            !x
        }
    }
}

impl EdgeHistoryFilter for PersistentGraph {
    fn is_edge_prop_update_available(
        &self,
        _layer_id: usize,
        _prop_id: usize,
        _edge_id: EID,
        _time: TimeIndexEntry,
    ) -> bool {
        // let nse = self.0.core_node(node_id);
        // nse.tprop(prop_id).at(&time).is_some()
        true
    }

    fn is_edge_prop_update_available_window(
        &self,
        layer_id: usize,
        prop_id: usize,
        edge_id: EID,
        time: TimeIndexEntry,
        w: Range<i64>,
    ) -> bool {
        if time.t() >= w.end {
            false
        } else if w.contains(&time.t()) {
            true
        } else {
            let ese = self.core_edge(edge_id);
            let bool = ese
                .temporal_prop_layer(layer_id, prop_id)
                .last_before(TimeIndexEntry::start(w.start))
                .map(|(t, _)| time.t().eq(&t.t()))
                .unwrap_or(false);
            bool
        }
    }

    fn is_edge_prop_update_latest(
        &self,
        layer_ids: &LayerIds,
        layer_id: usize,
        prop_id: usize,
        edge_id: EID,
        time: TimeIndexEntry,
    ) -> bool {
        self.0
            .is_edge_prop_update_latest(layer_ids, layer_id, prop_id, edge_id, time)
    }

    fn is_edge_prop_update_latest_window(
        &self,
        layer_ids: &LayerIds,
        layer_id: usize,
        prop_id: usize,
        edge_id: EID,
        time: TimeIndexEntry,
        w: Range<i64>,
    ) -> bool {
        time.t() < w.end && {
            let time = time.next();
            let ese = self.core_edge(edge_id);

            if layer_ids.contains(&layer_id) {
                // Check if any layer has an active update beyond `time`
                let has_future_update = ese.layer_ids_iter(layer_ids).any(|layer_id| {
                    ese.temporal_prop_layer(layer_id, prop_id)
                        .active(time..TimeIndexEntry::start(w.end))
                });

                // If no layer has a future update, return true
                return !has_future_update;
            };
            false
        }
    }
}

#[cfg(test)]
mod test {
    use crate::db::api::view::time::internal::InternalTimeOps;

    use super::*;

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
    fn test_materialize_window_earliest_time() {
        let g = PersistentGraph::new();
        g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        g.delete_edge(10, 1, 2, None).unwrap();

        let ltg = g.latest_time_global();
        assert_eq!(ltg, Some(10));

        let wg = g.window(3, 5);

        let e = wg.edge(1, 2).unwrap();
        assert_eq!(e.earliest_time(), Some(3));
        assert_eq!(e.latest_time(), Some(3));
        let n1 = wg.node(1).unwrap();
        assert_eq!(n1.earliest_time(), Some(3));
        assert_eq!(n1.latest_time(), Some(3));
        let n2 = wg.node(2).unwrap();
        assert_eq!(n2.earliest_time(), Some(3));
        assert_eq!(n2.latest_time(), Some(3));

        let actual_lt = wg.latest_time();
        assert_eq!(actual_lt, Some(3));

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
}
