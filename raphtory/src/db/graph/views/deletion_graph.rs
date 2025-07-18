use crate::{
    core::{
        entities::LayerIds,
        storage::timeindex::{AsTime, TimeIndex, TimeIndexEntry, TimeIndexOps},
        utils::iter::GenLockedDIter,
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
    iter::{BoxedLDIter, IntoDynDBoxed},
    GraphType,
};
use raphtory_storage::{
    graph::{
        edges::edge_storage_ops::EdgeStorageOps, graph::GraphStorage,
        nodes::node_storage_ops::NodeStorageOps,
    },
    mutation::InheritMutationOps,
};
use serde::{Deserialize, Serialize};
use std::{
    fmt::{Display, Formatter},
    iter,
    ops::{Deref, Range},
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

    fn temporal_prop_iter(&self, prop_id: usize) -> BoxedLDIter<(TimeIndexEntry, Prop)> {
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
    ) -> BoxedLDIter<(TimeIndexEntry, Prop)> {
        if let Some(prop) = self.graph_meta().get_temporal_prop(prop_id) {
            let first = persisted_prop_value_at(start, &*prop, &TimeIndex::Empty)
                .map(|v| (TimeIndexEntry::start(start), v));
            first
                .into_iter()
                .chain(GenLockedDIter::from(prop, |prop| {
                    prop.deref()
                        .iter_window(TimeIndexEntry::range(start..end))
                        .into_dyn_dboxed()
                }))
                .into_dyn_dboxed()
        } else {
            iter::empty().into_dyn_dboxed()
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
mod test_deletions {
    use crate::{
        db::{
            api::view::time::internal::InternalTimeOps,
            graph::{
                edge::EdgeView,
                graph::{assert_graph_equal, assert_persistent_materialize_graph_equal},
                views::deletion_graph::{GraphTimeSemanticsOps, PersistentGraph},
            },
        },
        prelude::*,
        test_storage,
        test_utils::{build_graph, build_graph_strat},
    };
    use itertools::Itertools;
    use proptest::{arbitrary::any, proptest, sample::subsequence};
    use raphtory_api::core::entities::GID;
    use raphtory_storage::mutation::addition_ops::InternalAdditionOps;
    use std::ops::Range;

    #[test]
    fn test_nodes() {
        let g = PersistentGraph::new();

        let edges = [
            (0, 1, 2, "assigned"),
            (1, 1, 3, "assigned"),
            (2, 4, 2, "has"),
            (3, 4, 2, "has"),
            (4, 5, 2, "blocks"),
            (5, 4, 5, "has"),
            (6, 6, 5, "assigned"),
        ];

        for (time, src, dst, layer) in edges {
            g.add_edge(time, src, dst, [("added", Prop::I64(0))], Some(layer))
                .unwrap();
        }

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
        assert_eq!(e.latest_time(), Some(1)); // not deleted so alive forever
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
        proptest!(|(graph_f in build_graph_strat(10, 10, true))| {
            let g = PersistentGraph(build_graph(&graph_f));
            let gm = g.materialize().unwrap();
            assert_graph_equal(&g, &gm);
        })
    }

    #[test]
    fn materialize_window_prop_test() {
        proptest!(|(graph_f in build_graph_strat(10, 10, true), w in any::<Range<i64>>())| {
            let g = PersistentGraph(build_graph(&graph_f));
            let gw = g.window(w.start, w.end);
            let gmw = gw.materialize().unwrap();
            assert_persistent_materialize_graph_equal(&gw, &gmw);
        })
    }

    #[test]
    fn test_multilayer_window() {
        let g = PersistentGraph::new();
        g.add_edge(0, 0, 0, NO_PROPS, None).unwrap();
        g.add_edge(10, 0, 0, NO_PROPS, Some("a")).unwrap();
        println!("all: {g:?}");
        let gw = g.window(1, 10);
        println!("windowed nodes: {:?}", gw.nodes());
        let gm = gw.materialize().unwrap();

        println!("materialized: {:?}", gm);

        assert_eq!(gw.valid_layers("a").count_nodes(), 0);
        assert_eq!(gm.valid_layers("a").count_nodes(), 0);

        let expected = PersistentGraph::new();
        expected.add_edge(1, 0, 0, NO_PROPS, None).unwrap();
        expected.resolve_layer(Some("a")).unwrap(); // empty layer exists

        println!("expected: {:?}", expected);
        assert_persistent_materialize_graph_equal(&gw, &expected);

        assert_persistent_materialize_graph_equal(&gw, &gm);
    }

    #[test]
    fn test_multilayer_window2() {
        let g = PersistentGraph::new();
        g.add_edge(0, 0, 0, NO_PROPS, None).unwrap();
        g.add_edge(0, 0, 0, NO_PROPS, Some("a")).unwrap();
        let gw = g.window(1, i64::MAX);
        let gm = gw.materialize().unwrap();
        println!("original: {g:?}");
        assert_eq!(g.default_layer().count_nodes(), 1);
        println!("materialized: {gm:?}");
        assert_eq!(gm.default_layer().count_nodes(), 1);

        assert_persistent_materialize_graph_equal(&gw, &gm.clone().into_persistent().unwrap());
        assert_persistent_materialize_graph_equal(&gw, &gm);
    }

    #[test]
    fn test_deletion_at_window_start() {
        let g = PersistentGraph::new();
        g.add_edge(2, 0, 1, NO_PROPS, None).unwrap();
        g.delete_edge(0, 0, 1, None).unwrap();

        // deletion at start of window is not part of the view
        let gw = g.window(0, 1);
        assert!(gw.is_empty());

        let gw = g.window(0, 3);
        assert_eq!(gw.node(0).unwrap().earliest_time(), Some(2));
        assert_eq!(gw.node(1).unwrap().earliest_time(), Some(2));
    }
    #[test]
    fn materialize_window_layers_prop_test() {
        proptest!(|(graph_f in build_graph_strat(10, 10, true), w in any::<Range<i64>>(), l in subsequence(&["a", "b"], 0..=2))| {
            let g = PersistentGraph(build_graph(&graph_f));
            let glw = g.valid_layers(l).window(w.start, w.end);
            let gmlw = glw.materialize().unwrap();
            assert_persistent_materialize_graph_equal(&glw, &gmlw);
        })
    }

    #[test]
    fn test_materialize_deleted_edge() {
        let g = PersistentGraph::new();
        g.add_edge(0, 0, 1, NO_PROPS, None).unwrap();
        g.delete_edge(1, 0, 1, None).unwrap();
        g.delete_edge(5, 0, 1, None).unwrap();

        let gw = g.window(2, 10);

        let gm = gw.materialize().unwrap();

        assert_graph_equal(&gw, &gm);
    }

    #[test]
    fn test_addition_deletion_multilayer_window() {
        let g = PersistentGraph::new();
        g.add_edge(0, 0, 0, NO_PROPS, Some("a")).unwrap();
        g.delete_edge(0, 0, 0, None).unwrap();
        let gw = g.window(0, 0).valid_layers("a");
        let expected_gw = PersistentGraph::new();
        expected_gw.resolve_layer(Some("a")).unwrap();
        assert_graph_equal(&gw, &expected_gw);
        let gwm = gw.materialize().unwrap();
        assert_graph_equal(&gw, &gwm);
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
        assert_persistent_materialize_graph_equal(&gw, &gw.materialize().unwrap())
    }

    #[test]
    fn test_materialize_window_start_before_node_add() {
        let g = PersistentGraph::new();
        g.add_node(-1, 0, [("test", "test")], None).unwrap();
        g.add_node(5, 0, [("test", "blob")], None).unwrap();
        g.add_edge(0, 0, 0, NO_PROPS, None).unwrap();
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
        assert_persistent_materialize_graph_equal(&gw, &gmw);
    }

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
            [("test".into(), "test".into())]
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
            [("test".into(), "test".into())]
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
        assert_persistent_materialize_graph_equal(&g.window(3, 5), &gm); // ignore start of window as it has different updates by design
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

    #[test]
    fn test_materialize_window_node_props() {
        let g = Graph::new();
        g.add_node(0, 1, [("test", "test")], None).unwrap();

        test_storage!(&g, |g| {
            let g = g.persistent_graph();

            let wg = g.window(3, 5);
            let mg = wg.materialize().unwrap();
            assert_persistent_materialize_graph_equal(&wg, &mg);
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
        assert_eq!(e.latest_time(), Some(4));

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
        assert_eq!(v.latest_time(), Some(11));
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
    fn test_graph_property_semantics() {
        let g = PersistentGraph::new();
        g.add_properties(1, [("weight", 10i64)]).unwrap();
        g.add_properties(3, [("weight", 20i64)]).unwrap();
        let prop = g.properties().temporal().get("weight").unwrap();

        assert_eq!(prop, [(1, 10i64), (3, 20i64)]);

        let prop = g
            .window(5, 7)
            .properties()
            .temporal()
            .get("weight")
            .unwrap();
        assert_eq!(prop, [(5, 20i64)])
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
        assert_eq!(g.node(0).unwrap().latest_time(), Some(10));
        assert_eq!(g.node(1).unwrap().latest_time(), Some(10));
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

    #[test]
    fn multiple_node_updates_at_same_time() {
        let g = PersistentGraph::new();

        g.add_node(1, 1, [("prop1", 1)], None).unwrap();
        g.add_node(2, 1, [("prop1", 2)], None).unwrap();
        g.add_node(2, 1, [("prop1", 3)], None).unwrap();
        g.add_node(8, 1, [("prop1", 4)], None).unwrap();
        g.add_node(9, 1, [("prop1", 5)], None).unwrap();

        assert_eq!(
            g.window(2, 10)
                .node(1)
                .unwrap()
                .properties()
                .temporal()
                .get("prop1")
                .unwrap()
                .values()
                .collect_vec(),
            [Prop::I32(2), Prop::I32(3), Prop::I32(4), Prop::I32(5)]
        )
    }

    #[test]
    fn filtering_all_layers_keeps_explicitly_added_nodes() {
        let g = PersistentGraph::new();
        g.add_node(0, 0, [("prop1", false)], None).unwrap();
        let view = g.valid_layers(Layer::None).window(0, 1);
        assert_eq!(view.count_nodes(), 1);
        assert_eq!(view.count_edges(), 0);
        assert_graph_equal(&view, &view.materialize().unwrap())
    }

    #[test]
    fn filtering_all_layers_removes_other_nodes() {
        let g = PersistentGraph::new();
        g.add_edge(0, 0, 1, NO_PROPS, None).unwrap();

        let view = g.valid_layers(Layer::None).window(0, 1);
        assert_eq!(view.count_nodes(), 0);
        assert_eq!(view.count_edges(), 0);
        assert_graph_equal(&view, &view.materialize().unwrap())
    }

    #[test]
    fn deletions_window_has_exclusive_start() {
        let g = PersistentGraph::new();
        g.add_edge(0, 0, 1, NO_PROPS, None).unwrap();
        g.delete_edge(2, 0, 1, None).unwrap();
        let e = g.edge(0, 1).unwrap();
        assert!(e.is_active()); // has updates
        assert!(!e.is_valid()); // last update is a deletion
        assert!(e.is_deleted());

        assert!(e.window(0, 1).is_active()); // addition in window
        assert!(e.window(0, 1).is_valid()); // not deleted
        assert!(!e.window(0, 1).is_deleted());

        assert!(e.window(1, 3).is_active()); // deletion in window
        assert!(!e.window(1, 3).is_valid());
        assert!(e.window(1, 3).is_deleted());

        assert!(!e.window(1, 2).is_active()); // no updates in window
        assert!(e.window(1, 2).is_valid()); // deletion not in window (exclusive end)
        assert!(!e.window(1, 2).is_deleted());

        assert!(!e.window(2, 3).is_active()); // deletion at start of window are not included
        assert!(!e.window(2, 3).is_valid());
        assert!(e.window(2, 3).is_deleted());
        assert!(!e.latest().is_active()); // this is the same as above
    }
}

#[cfg(test)]
mod test_node_history_filter_persistent_graph {
    use crate::{
        db::{
            api::view::{internal::NodeHistoryFilter, StaticGraphViewOps},
            graph::views::deletion_graph::PersistentGraph,
        },
        prelude::{AdditionOps, GraphViewOps},
    };
    use raphtory_api::core::{
        entities::properties::prop::Prop, storage::timeindex::TimeIndexEntry,
    };
    use raphtory_storage::core_ops::CoreGraphOps;

    fn init_graph<G: StaticGraphViewOps + AdditionOps>(graph: G) -> G {
        let nodes = [
            (6, "N1", Prop::U64(2)),
            (7, "N1", Prop::U64(1)),
            (6, "N2", Prop::U64(1)),
            (7, "N2", Prop::U64(2)),
            (8, "N3", Prop::U64(1)),
            (9, "N4", Prop::U64(1)),
            (5, "N5", Prop::U64(1)),
            (6, "N5", Prop::U64(2)),
            (5, "N6", Prop::U64(1)),
            (6, "N6", Prop::U64(1)),
            (3, "N7", Prop::U64(1)),
            (5, "N7", Prop::U64(1)),
            (3, "N8", Prop::U64(1)),
            (4, "N8", Prop::U64(2)),
        ];

        for (time, id, prop) in nodes {
            graph.add_node(time, id, [("p1", prop)], None).unwrap();
        }

        graph
    }

    #[test]
    fn test_is_prop_update_latest() {
        let g = PersistentGraph::new();
        let g = init_graph(g);

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
    fn test_is_prop_update_latest_w() {
        let g = PersistentGraph::new();
        let g = init_graph(g);

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
        assert!(bool);

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
mod test_edge_history_filter_persistent_graph {
    use crate::{
        db::{
            api::view::{
                internal::{EdgeHistoryFilter, InternalLayerOps},
                StaticGraphViewOps,
            },
            graph::views::deletion_graph::PersistentGraph,
        },
        prelude::{AdditionOps, GraphViewOps},
    };
    use raphtory_api::core::{
        entities::properties::prop::Prop, storage::timeindex::TimeIndexEntry,
    };
    use raphtory_storage::core_ops::CoreGraphOps;

    fn init_graph<G: StaticGraphViewOps + AdditionOps>(graph: G) -> G {
        let edges = [
            (6, "N1", "N2", Prop::U64(2), Some("layer1")),
            (7, "N1", "N2", Prop::U64(1), Some("layer2")),
            (6, "N2", "N3", Prop::U64(1), Some("layer1")),
            (7, "N2", "N3", Prop::U64(2), Some("layer2")),
            (8, "N3", "N4", Prop::U64(1), Some("layer1")),
            (9, "N4", "N5", Prop::U64(1), Some("layer1")),
            (5, "N5", "N6", Prop::U64(1), Some("layer1")),
            (6, "N5", "N6", Prop::U64(2), Some("layer2")),
            (5, "N6", "N7", Prop::U64(1), Some("layer1")),
            (6, "N6", "N7", Prop::U64(1), Some("layer2")),
            (3, "N7", "N8", Prop::U64(1), Some("layer1")),
            (5, "N7", "N8", Prop::U64(1), Some("layer2")),
            (3, "N8", "N1", Prop::U64(1), Some("layer1")),
            (4, "N8", "N1", Prop::U64(2), Some("layer2")),
            (3, "N9", "N2", Prop::U64(1), Some("layer1")),
            (3, "N9", "N2", Prop::U64(2), Some("layer2")),
        ];

        for (time, src, dst, prop, layer) in edges {
            graph
                .add_edge(time, src, dst, [("p1", prop)], layer)
                .unwrap();
        }

        graph
    }

    #[test]
    fn test_is_edge_prop_update_latest() {
        let g = PersistentGraph::new();
        let g = init_graph(g);

        let prop_id = g.edge_meta().temporal_prop_meta().get_id("p1").unwrap();

        let edge_id = g.edge("N1", "N2").unwrap().edge.pid();
        let bool = g.is_edge_prop_update_latest(
            g.layer_ids(),
            g.get_layer_id("layer2").unwrap(),
            prop_id,
            edge_id,
            TimeIndexEntry::end(7),
        );
        assert!(bool);

        let edge_id = g.edge("N2", "N3").unwrap().edge.pid();
        let bool = g.is_edge_prop_update_latest(
            g.layer_ids(),
            g.get_layer_id("layer1").unwrap(),
            prop_id,
            edge_id,
            TimeIndexEntry::end(6),
        );
        assert!(!bool);

        let edge_id = g.edge("N3", "N4").unwrap().edge.pid();
        let bool = g.is_edge_prop_update_latest(
            g.layer_ids(),
            g.get_layer_id("layer1").unwrap(),
            prop_id,
            edge_id,
            TimeIndexEntry::end(8),
        );
        assert!(bool);

        let edge_id = g.edge("N4", "N5").unwrap().edge.pid();
        let bool = g.is_edge_prop_update_latest(
            g.layer_ids(),
            g.get_layer_id("layer1").unwrap(),
            prop_id,
            edge_id,
            TimeIndexEntry::end(9),
        );
        assert!(bool);

        let edge_id = g.edge("N5", "N6").unwrap().edge.pid();
        let bool = g.is_edge_prop_update_latest(
            g.layer_ids(),
            g.get_layer_id("layer1").unwrap(),
            prop_id,
            edge_id,
            TimeIndexEntry::end(5),
        );
        assert!(!bool);

        let edge_id = g.edge("N6", "N7").unwrap().edge.pid();
        let bool = g.is_edge_prop_update_latest(
            g.layer_ids(),
            g.get_layer_id("layer1").unwrap(),
            prop_id,
            edge_id,
            TimeIndexEntry::end(5),
        );
        assert!(!bool);
        let edge_id = g.edge("N6", "N7").unwrap().edge.pid();
        let bool = g.is_edge_prop_update_latest(
            g.layer_ids(),
            g.get_layer_id("layer2").unwrap(),
            prop_id,
            edge_id,
            TimeIndexEntry::end(6),
        );
        assert!(bool);

        let edge_id = g.edge("N7", "N8").unwrap().edge.pid();
        let bool = g.is_edge_prop_update_latest(
            g.layer_ids(),
            g.get_layer_id("layer1").unwrap(),
            prop_id,
            edge_id,
            TimeIndexEntry::end(3),
        );
        assert!(!bool);
        let edge_id = g.edge("N7", "N8").unwrap().edge.pid();
        let bool = g.is_edge_prop_update_latest(
            g.layer_ids(),
            g.get_layer_id("layer2").unwrap(),
            prop_id,
            edge_id,
            TimeIndexEntry::end(5),
        );
        assert!(bool);

        let edge_id = g.edge("N8", "N1").unwrap().edge.pid();
        let bool = g.is_edge_prop_update_latest(
            g.layer_ids(),
            g.get_layer_id("layer1").unwrap(),
            prop_id,
            edge_id,
            TimeIndexEntry::end(3),
        );
        assert!(!bool);

        // TODO: Revisit this test
        // let edge_id = g.edge("N9", "N2").unwrap().edge.pid();
        // let bool = g.is_edge_prop_update_latest(prop_id, edge_id, TimeIndexEntry::end(3));
        // assert!(!bool);
    }

    #[test]
    fn test_is_edge_prop_update_latest_w() {
        let g = PersistentGraph::new();
        let g = init_graph(g);

        let prop_id = g.edge_meta().temporal_prop_meta().get_id("p1").unwrap();
        let w = 6..9;

        let edge_id = g.edge("N1", "N2").unwrap().edge.pid();
        let bool = g.is_edge_prop_update_latest_window(
            g.layer_ids(),
            g.get_layer_id("layer1").unwrap(),
            prop_id,
            edge_id,
            TimeIndexEntry::end(7),
            w.clone(),
        );
        assert!(bool);

        let edge_id = g.edge("N2", "N3").unwrap().edge.pid();
        let bool = g.is_edge_prop_update_latest_window(
            g.layer_ids(),
            g.get_layer_id("layer1").unwrap(),
            prop_id,
            edge_id,
            TimeIndexEntry::end(6),
            w.clone(),
        );
        assert!(!bool);

        let edge_id = g.edge("N3", "N4").unwrap().edge.pid();
        let bool = g.is_edge_prop_update_latest_window(
            g.layer_ids(),
            g.get_layer_id("layer1").unwrap(),
            prop_id,
            edge_id,
            TimeIndexEntry::end(8),
            w.clone(),
        );
        assert!(bool);

        let edge_id = g.edge("N4", "N5").unwrap().edge.pid();
        let bool = g.is_edge_prop_update_latest_window(
            g.layer_ids(),
            g.get_layer_id("layer1").unwrap(),
            prop_id,
            edge_id,
            TimeIndexEntry::end(9),
            w.clone(),
        );
        assert!(!bool);

        let edge_id = g.edge("N5", "N6").unwrap().edge.pid();
        let bool = g.is_edge_prop_update_latest_window(
            g.layer_ids(),
            g.get_layer_id("layer1").unwrap(),
            prop_id,
            edge_id,
            TimeIndexEntry::end(5),
            w.clone(),
        );
        assert!(!bool);

        let edge_id = g.edge("N6", "N7").unwrap().edge.pid();
        let bool = g.is_edge_prop_update_latest_window(
            g.layer_ids(),
            g.get_layer_id("layer1").unwrap(),
            prop_id,
            edge_id,
            TimeIndexEntry::end(5),
            w.clone(),
        );
        assert!(!bool);
        let edge_id = g.edge("N6", "N7").unwrap().edge.pid();
        let bool = g.is_edge_prop_update_latest_window(
            g.layer_ids(),
            g.get_layer_id("layer2").unwrap(),
            prop_id,
            edge_id,
            TimeIndexEntry::end(6),
            w.clone(),
        );
        assert!(bool);

        let edge_id = g.edge("N7", "N8").unwrap().edge.pid();
        let bool = g.is_edge_prop_update_latest_window(
            g.layer_ids(),
            g.get_layer_id("layer1").unwrap(),
            prop_id,
            edge_id,
            TimeIndexEntry::end(3),
            w.clone(),
        );
        assert!(!bool);
        let edge_id = g.edge("N7", "N8").unwrap().edge.pid();
        let bool = g.is_edge_prop_update_latest_window(
            g.layer_ids(),
            g.get_layer_id("layer2").unwrap(),
            prop_id,
            edge_id,
            TimeIndexEntry::end(5),
            w.clone(),
        );
        assert!(bool);

        let edge_id = g.edge("N8", "N1").unwrap().edge.pid();
        let bool = g.is_edge_prop_update_latest_window(
            g.layer_ids(),
            g.get_layer_id("layer1").unwrap(),
            prop_id,
            edge_id,
            TimeIndexEntry::end(3),
            w.clone(),
        );
        assert!(!bool);

        // TODO: Revisit this test
        // let edge_id = g.edge("N9", "N2").unwrap().edge.pid();
        // let bool = g.is_edge_prop_update_latest_window(prop_id, edge_id, TimeIndexEntry::end(3), w.clone());
        // assert!(!bool);
    }
}
