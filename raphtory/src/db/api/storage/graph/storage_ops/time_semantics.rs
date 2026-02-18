use super::GraphStorage;
use crate::{
    core::storage::timeindex::TimeIndexOps,
    db::api::view::internal::{GraphTimeSemanticsOps, TimeSemantics},
    prelude::Prop,
};
use raphtory_api::{
    core::{entities::properties::tprop::TPropOps, storage::timeindex::EventTime},
    iter::{BoxedLIter, IntoDynBoxed},
};
use raphtory_core::utils::iter::GenLockedIter;
use raphtory_storage::graph::{locked::LockedGraph, nodes::node_storage_ops::NodeStorageOps};
use rayon::iter::ParallelIterator;
use std::ops::Range;
use storage::{
    api::graph_props::{GraphPropEntryOps, GraphPropRefOps},
    gen_ts::ALL_LAYERS,
};

impl GraphTimeSemanticsOps for GraphStorage {
    fn node_time_semantics(&self) -> TimeSemantics {
        TimeSemantics::event()
    }

    fn edge_time_semantics(&self) -> TimeSemantics {
        TimeSemantics::event()
    }

    fn view_start(&self) -> Option<EventTime> {
        None
    }

    fn view_end(&self) -> Option<EventTime> {
        None
    }

    #[inline]
    fn earliest_time_global(&self) -> Option<i64> {
        match self {
            GraphStorage::Mem(LockedGraph { graph, .. }) | GraphStorage::Unlocked(graph) => {
                graph.graph_earliest_time()
            }
        }
    }

    #[inline]
    fn latest_time_global(&self) -> Option<i64> {
        match self {
            GraphStorage::Mem(LockedGraph { graph, .. }) | GraphStorage::Unlocked(graph) => {
                graph.graph_latest_time()
            }
        }
    }

    fn earliest_time_window(&self, start: EventTime, end: EventTime) -> Option<i64> {
        self.nodes()
            .par_iter()
            .flat_map_iter(|node| {
                node.additions()
                    .range(start..end)
                    .first_t()
                    .into_iter()
                    .chain(
                        node.node_edge_additions(ALL_LAYERS)
                            .range(start..end)
                            .first_t(),
                    )
            })
            .min()
    }

    fn latest_time_window(&self, start: EventTime, end: EventTime) -> Option<i64> {
        self.nodes()
            .par_iter()
            .flat_map_iter(|node| {
                node.additions()
                    .range(start..end)
                    .last_t()
                    .into_iter()
                    .chain(
                        node.node_edge_additions(ALL_LAYERS)
                            .range(start..end)
                            .last_t(),
                    )
            })
            .max()
    }

    fn has_temporal_prop(&self, prop_id: usize) -> bool {
        self.graph_props_meta()
            .temporal_prop_mapper()
            .has_id(prop_id)
    }

    fn temporal_prop_iter(&self, prop_id: usize) -> BoxedLIter<'_, (EventTime, Prop)> {
        let graph_entry = self.graph_entry();

        GenLockedIter::from(graph_entry, |entry| {
            entry
                .as_ref()
                .get_temporal_prop(prop_id)
                .iter()
                .into_dyn_boxed()
        })
        .into_dyn_boxed()
    }

    fn has_temporal_prop_window(&self, prop_id: usize, w: Range<EventTime>) -> bool {
        let graph_entry = self.graph_entry();

        graph_entry.as_ref().get_temporal_prop(prop_id).active(w)
    }

    fn temporal_prop_iter_window(
        &self,
        prop_id: usize,
        start: EventTime,
        end: EventTime,
    ) -> BoxedLIter<'_, (EventTime, Prop)> {
        let graph_entry = self.graph_entry();

        GenLockedIter::from(graph_entry, move |entry| {
            entry
                .as_ref()
                .get_temporal_prop(prop_id)
                .iter_window(start..end)
                .into_dyn_boxed()
        })
        .into_dyn_boxed()
    }

    fn temporal_prop_iter_window_rev(
        &self,
        prop_id: usize,
        start: EventTime,
        end: EventTime,
    ) -> BoxedLIter<'_, (EventTime, Prop)> {
        let graph_entry = self.graph_entry();

        GenLockedIter::from(graph_entry, move |entry| {
            entry
                .as_ref()
                .get_temporal_prop(prop_id)
                .iter_window_rev(start..end)
                .into_dyn_boxed()
        })
        .into_dyn_boxed()
    }

    fn temporal_prop_last_at(&self, prop_id: usize, t: EventTime) -> Option<(EventTime, Prop)> {
        let graph_entry = self.graph_entry();

        graph_entry
            .as_ref()
            .get_temporal_prop(prop_id)
            .last_before(t.next())
    }

    fn temporal_prop_last_at_window(
        &self,
        prop_id: usize,
        t: EventTime,
        w: Range<EventTime>,
    ) -> Option<(EventTime, Prop)> {
        if w.contains(&t) {
            let graph_entry = self.graph_entry();

            graph_entry
                .as_ref()
                .get_temporal_prop(prop_id)
                .last_before(t.next())
                .filter(|(prop_time, _)| w.contains(prop_time))
        } else {
            None
        }
    }
}

#[cfg(all(test, feature = "search"))]
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
            graph.add_node(id, name, props, None, None).unwrap();
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
            // Tests event ids
            (3, "N9", "N2", vec![("p1", Prop::U64(1u64))], Some("layer1")),
            (3, "N9", "N2", vec![("p1", Prop::U64(2u64))], Some("layer2")),
        ];

        for (id, src, dst, props, layer) in edges {
            graph.add_edge(id, src, dst, props, layer).unwrap();
        }

        graph
    }

    mod search_nodes {
        use super::*;
        use crate::{
            db::{
                api::view::SearchableGraphOps,
                graph::views::filter::model::{
                    node_filter::NodeFilter, property_filter::ops::PropertyFilterOps,
                    PropertyFilterFactory,
                },
            },
            prelude::{Graph, IndexMutationOps, NodeViewOps},
        };

        #[test]
        fn test_search_nodes_latest() {
            let g = Graph::new();
            let g = init_graph_for_nodes_tests(g);
            g.create_index().unwrap();
            let filter = NodeFilter.property("p1").eq(1u64);
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

    mod search_edges {
        use super::*;
        use crate::{
            db::{
                api::view::SearchableGraphOps,
                graph::views::filter::model::{
                    edge_filter::EdgeFilter, property_filter::ops::PropertyFilterOps,
                    PropertyFilterFactory,
                },
            },
            prelude::{EdgeViewOps, Graph, IndexMutationOps, NodeViewOps},
        };

        #[test]
        #[ignore = "TODO: #2372"]
        fn test_search_edges_latest() {
            let g = Graph::new();
            let g = init_graph_for_edges_tests(g);
            g.create_index().unwrap();
            let filter = EdgeFilter.property("p1").eq(1u64);
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
