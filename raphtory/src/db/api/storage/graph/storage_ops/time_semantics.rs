use super::GraphStorage;
use crate::{
    core::{entities::LayerIds, storage::timeindex::TimeIndexOps, utils::iter::GenLockedDIter},
    db::api::view::internal::{
        EdgeHistoryFilter, GraphTimeSemanticsOps, NodeHistoryFilter, TimeSemantics,
    },
    prelude::Prop,
};
use raphtory_api::{
    core::{
        entities::{properties::tprop::TPropOps, EID, VID},
        storage::timeindex::{AsTime, TimeIndexEntry},
    },
    iter::{BoxedLDIter, IntoDynDBoxed},
};
use raphtory_storage::{
    core_ops::CoreGraphOps,
    graph::{edges::edge_storage_ops::EdgeStorageOps, nodes::node_storage_ops::NodeStorageOps},
};
use rayon::iter::ParallelIterator;
use std::ops::{Deref, Range};

impl GraphTimeSemanticsOps for GraphStorage {
    fn node_time_semantics(&self) -> TimeSemantics {
        TimeSemantics::event()
    }

    fn edge_time_semantics(&self) -> TimeSemantics {
        TimeSemantics::event()
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

    fn has_temporal_prop(&self, prop_id: usize) -> bool {
        prop_id < self.graph_meta().temporal_prop_meta().len()
    }

    fn temporal_prop_iter(&self, prop_id: usize) -> BoxedLDIter<(TimeIndexEntry, Prop)> {
        self.graph_meta()
            .get_temporal_prop(prop_id)
            .into_iter()
            .flat_map(move |prop| {
                GenLockedDIter::from(prop, |prop| prop.deref().iter().into_dyn_dboxed())
            })
            .into_dyn_dboxed()
    }

    fn has_temporal_prop_window(&self, prop_id: usize, w: Range<i64>) -> bool {
        self.graph_meta()
            .get_temporal_prop(prop_id)
            .filter(|p| p.deref().iter_window_t(w).next().is_some())
            .is_some()
    }

    fn temporal_prop_iter_window(
        &self,
        prop_id: usize,
        start: i64,
        end: i64,
    ) -> BoxedLDIter<(TimeIndexEntry, Prop)> {
        self.graph_meta()
            .get_temporal_prop(prop_id)
            .into_iter()
            .flat_map(move |prop| {
                GenLockedDIter::from(prop, |prop| {
                    prop.deref()
                        .iter_window(TimeIndexEntry::range(start..end))
                        .into_dyn_dboxed()
                })
            })
            .into_dyn_dboxed()
    }

    fn temporal_prop_last_at(
        &self,
        prop_id: usize,
        t: TimeIndexEntry,
    ) -> Option<(TimeIndexEntry, Prop)> {
        self.graph_meta()
            .get_temporal_prop(prop_id)
            .and_then(|p| p.deref().last_before(t.next()))
    }

    fn temporal_prop_last_at_window(
        &self,
        prop_id: usize,
        t: TimeIndexEntry,
        w: Range<i64>,
    ) -> Option<(TimeIndexEntry, Prop)> {
        let w = TimeIndexEntry::range(w);
        if w.contains(&t) {
            self.graph_meta().get_temporal_prop(prop_id).and_then(|p| {
                p.deref()
                    .last_before(t.next())
                    .filter(|(t, _)| w.contains(t))
            })
        } else {
            None
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
        let nse = self.core_node(node_id);
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
            let nse = self.core_node(node_id);
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
        layer_ids: &LayerIds,
        layer_id: usize,
        prop_id: usize,
        edge_id: EID,
        time: TimeIndexEntry,
    ) -> bool {
        let time = time.next();
        let ese = self.core_edge(edge_id);

        if layer_ids.contains(&layer_id) {
            // Check if any layer has an active update beyond `time`
            let has_future_update = ese.layer_ids_iter(layer_ids).any(|layer_id| {
                ese.temporal_prop_layer(layer_id, prop_id)
                    .active(time..TimeIndexEntry::MAX)
            });

            // If no layer has a future update, return true
            return !has_future_update;
        };

        false
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
        w.contains(&time.t()) && {
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
mod test_graph_storage {
    use crate::{db::api::view::StaticGraphViewOps, prelude::AdditionOps};
    use raphtory_api::core::entities::properties::prop::Prop;

    fn init_graph_for_nodes_tests<G: StaticGraphViewOps + AdditionOps>(graph: G) -> G {
        let nodes = vec![
            (6, "N1", vec![("p1", Prop::U64(2u64))]),
            (7, "N1", vec![("p1", Prop::U64(1u64))]),
            (6, "N2", vec![("p1", Prop::U64(1u64))]),
            (7, "N2", vec![("p1", Prop::U64(2u64))]),
            (8, "N3", vec![("p1", Prop::U64(1u64))]),
            (9, "N4", vec![("p1", Prop::U64(1u64))]),
            (5, "N5", vec![("p1", Prop::U64(1u64))]),
            (6, "N5", vec![("p1", Prop::U64(2u64))]),
            (5, "N6", vec![("p1", Prop::U64(1u64))]),
            (6, "N6", vec![("p1", Prop::U64(1u64))]),
            (3, "N7", vec![("p1", Prop::U64(1u64))]),
            (5, "N7", vec![("p1", Prop::U64(1u64))]),
            (3, "N8", vec![("p1", Prop::U64(1u64))]),
            (4, "N8", vec![("p1", Prop::U64(2u64))]),
        ];

        for (id, name, props) in nodes {
            graph.add_node(id, name, props, None).unwrap();
        }

        graph
    }

    fn init_graph_for_edges_tests<G: StaticGraphViewOps + AdditionOps>(graph: G) -> G {
        let edges = vec![
            (6, "N1", "N2", vec![("p1", Prop::U64(2u64))], Some("layer1")),
            (7, "N1", "N2", vec![("p1", Prop::U64(1u64))], Some("layer2")),
            (6, "N2", "N3", vec![("p1", Prop::U64(1u64))], Some("layer1")),
            (7, "N2", "N3", vec![("p1", Prop::U64(2u64))], Some("layer2")),
            (8, "N3", "N4", vec![("p1", Prop::U64(1u64))], Some("layer1")),
            (9, "N4", "N5", vec![("p1", Prop::U64(1u64))], Some("layer1")),
            (5, "N5", "N6", vec![("p1", Prop::U64(1u64))], Some("layer1")),
            (6, "N5", "N6", vec![("p1", Prop::U64(2u64))], Some("layer2")),
            (5, "N6", "N7", vec![("p1", Prop::U64(1u64))], Some("layer1")),
            (6, "N6", "N7", vec![("p1", Prop::U64(1u64))], Some("layer2")),
            (3, "N7", "N8", vec![("p1", Prop::U64(1u64))], Some("layer1")),
            (5, "N7", "N8", vec![("p1", Prop::U64(1u64))], Some("layer2")),
            (3, "N8", "N1", vec![("p1", Prop::U64(1u64))], Some("layer1")),
            (4, "N8", "N1", vec![("p1", Prop::U64(2u64))], Some("layer2")),
            // Tests secondary indexes
            (3, "N9", "N2", vec![("p1", Prop::U64(1u64))], Some("layer1")),
            (3, "N9", "N2", vec![("p1", Prop::U64(2u64))], Some("layer2")),
        ];

        for (id, src, dst, props, layer) in edges {
            graph.add_edge(id, src, dst, props, layer).unwrap();
        }

        graph
    }

    #[cfg(test)]
    mod test_node_history_filter_event_graph {
        use crate::{
            db::api::{
                storage::graph::storage_ops::time_semantics::test_graph_storage::init_graph_for_nodes_tests,
                view::internal::NodeHistoryFilter,
            },
            prelude::{Graph, GraphViewOps},
        };
        use raphtory_api::core::storage::timeindex::TimeIndexEntry;
        use raphtory_storage::core_ops::CoreGraphOps;

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
            db::api::{
                storage::graph::storage_ops::time_semantics::test_graph_storage::init_graph_for_edges_tests,
                view::internal::{EdgeHistoryFilter, InternalLayerOps},
            },
            prelude::{Graph, GraphViewOps},
        };
        use raphtory_api::core::storage::timeindex::TimeIndexEntry;
        use raphtory_storage::core_ops::CoreGraphOps;

        #[test]
        fn test_is_edge_prop_update_latest() {
            let g = Graph::new();
            let g = init_graph_for_edges_tests(g);

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
                g.layer_ids(),
                g.get_layer_id("layer2").unwrap(),
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
            assert!(!bool);

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
                api::view::SearchableGraphOps,
                graph::views::filter::model::{NodeFilter, PropertyFilterOps},
            },
            prelude::{Graph, IndexMutationOps, NodeViewOps},
        };

        #[test]
        fn test_search_nodes_latest() {
            let g = Graph::new();
            let g = init_graph_for_nodes_tests(g);
            g.create_index().unwrap();
            let filter = NodeFilter::property("p1").eq(1u64);
            let mut results = g
                .search_nodes(filter, 10, 0)
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
                api::view::SearchableGraphOps,
                graph::views::filter::model::{EdgeFilter, PropertyFilterOps},
            },
            prelude::{EdgeViewOps, Graph, IndexMutationOps, NodeViewOps},
        };

        #[test]
        fn test_search_edges_latest() {
            let g = Graph::new();
            let g = init_graph_for_edges_tests(g);
            g.create_index().unwrap();
            let filter = EdgeFilter::property("p1").eq(1u64);
            let mut results = g
                .search_edges(filter, 10, 0)
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
