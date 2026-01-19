use itertools::Itertools;
use proptest::prelude::*;
use raphtory::{
    algorithms::motifs::triangle_count::triangle_count, db::graph::graph::assert_graph_equal,
    prelude::*, test_storage,
};
use raphtory_api::core::storage::timeindex::AsTime;

#[test]
fn empty_graph() {
    let graph = Graph::new();
    test_storage!(&graph, |graph| {
        let sg = graph.cache_view();
        assert_graph_equal(&sg, &graph);
    });
}

#[test]
fn empty_window() {
    let graph = Graph::new();
    graph.add_edge(1, 1, 1, NO_PROPS, None).unwrap();
    test_storage!(&graph, |graph| {
        let window = graph.window(2, 3);
        let sg = window.cache_view();
        assert_graph_equal(&window, &sg);
    });
}

#[test]
fn test_materialize_no_edges() {
    let graph = Graph::new();

    graph.add_node(1, 1, NO_PROPS, None).unwrap();
    graph.add_node(2, 2, NO_PROPS, None).unwrap();

    test_storage!(&graph, |graph| {
        let sg = graph.cache_view();

        let actual = sg.materialize().unwrap().into_events().unwrap();
        assert_graph_equal(&actual, &sg);
    });
}

#[test]
fn test_mask_the_window_50pc() {
    let graph = Graph::new();
    let edges = vec![
        (1, 2, 1),
        (1, 3, 2),
        (1, 4, 3),
        (3, 1, 4),
        (3, 4, 5),
        (3, 5, 6),
        (4, 5, 7),
        (5, 6, 8),
        (5, 8, 9),
        (7, 5, 10),
        (8, 5, 11),
        (1, 9, 12),
        (9, 1, 13),
        (6, 3, 14),
        (4, 8, 15),
        (8, 3, 16),
        (5, 10, 17),
        (10, 5, 18),
        (10, 8, 19),
        (1, 11, 20),
        (11, 1, 21),
        (9, 11, 22),
        (11, 9, 23),
    ];
    for (src, dst, ts) in edges {
        graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
    }
    test_storage!(&graph, |graph| {
        let window = graph.window(12, 24);
        let mask = window.cache_view();
        let ts = triangle_count(&mask, None);
        let tg = triangle_count(&window, None);
        assert_eq!(ts, tg);
    });
}

#[test]
fn masked_always_equals() {
    fn check(edge_list: &[(u8, u8, i16, u8)]) {
        let graph = Graph::new();
        for (src, dst, ts, layer) in edge_list {
            graph
                .add_edge(
                    *ts as i64,
                    *src as u64,
                    *dst as u64,
                    NO_PROPS,
                    Some(&layer.to_string()),
                )
                .unwrap();
        }

        test_storage!(&graph, |graph| {
            let layers = graph
                .unique_layers()
                .take(graph.unique_layers().count() / 2)
                .collect_vec();

            let earliest = graph.earliest_time().unwrap().t();
            let latest = graph.latest_time().unwrap().t();
            let middle = earliest + (latest - earliest) / 2;

            if !layers.is_empty() && earliest < middle && middle < latest {
                let subgraph = graph.layers(layers).unwrap().window(earliest, middle);
                let masked = subgraph.cache_view();
                assert_graph_equal(&subgraph, &masked);
            }
        });
    }

    proptest!(|(edge_list in any::<Vec<(u8, u8, i16, u8)>>().prop_filter("greater than 3",|v| !v.is_empty() ))| {
        check(&edge_list);
    })
}

#[cfg(test)]
mod test_filters_cached_view {
    use raphtory::{
        db::{
            api::view::StaticGraphViewOps,
            graph::{
                assertions::GraphTransformer,
                views::{cached_view::CachedView, window_graph::WindowedGraph},
            },
        },
        prelude::{GraphViewOps, TimeOps},
    };
    use std::ops::Range;

    struct CachedGraphTransformer;

    impl GraphTransformer for CachedGraphTransformer {
        type Return<G: StaticGraphViewOps> = CachedView<G>;
        fn apply<G: StaticGraphViewOps>(&self, graph: G) -> Self::Return<G> {
            graph.cache_view()
        }
    }

    struct WindowedCachedGraphTransformer(Range<i64>);

    impl GraphTransformer for WindowedCachedGraphTransformer {
        type Return<G: StaticGraphViewOps> = WindowedGraph<CachedView<G>>;
        fn apply<G: StaticGraphViewOps>(&self, graph: G) -> Self::Return<G> {
            graph.cache_view().window(self.0.start, self.0.end)
        }
    }

    mod test_nodes_filters_cached_view_graph {
        use raphtory::{
            db::{
                api::view::StaticGraphViewOps,
                graph::{
                    assertions::{
                        assert_filter_nodes_results, assert_search_nodes_results,
                        TestGraphVariants, TestVariants,
                    },
                    views::filter::model::{
                        node_filter::NodeFilter, property_filter::ops::PropertyFilterOps,
                        PropertyFilterFactory,
                    },
                },
            },
            prelude::AdditionOps,
        };
        use raphtory_api::core::entities::properties::prop::Prop;

        use crate::test_filters_cached_view::{
            CachedGraphTransformer, WindowedCachedGraphTransformer,
        };

        fn init_graph<G: StaticGraphViewOps + AdditionOps>(graph: G) -> G {
            let node_data = vec![
                (6, "N1", 2u64, "air_nomad"),
                (7, "N1", 1u64, "air_nomad"),
                (6, "N2", 1u64, "water_tribe"),
                (7, "N2", 2u64, "water_tribe"),
                (8, "N3", 1u64, "air_nomad"),
                (9, "N4", 1u64, "air_nomad"),
                (5, "N5", 1u64, "air_nomad"),
                (6, "N5", 2u64, "air_nomad"),
                (5, "N6", 1u64, "fire_nation"),
                (6, "N6", 1u64, "fire_nation"),
                (3, "N7", 1u64, "air_nomad"),
                (5, "N7", 1u64, "air_nomad"),
                (3, "N8", 1u64, "fire_nation"),
                (4, "N8", 2u64, "fire_nation"),
            ];

            for (ts, name, value, kind) in node_data {
                graph
                    .add_node(ts, name, [("p1", Prop::U64(value))], Some(kind))
                    .unwrap();
            }

            graph
        }

        #[test]
        fn test_nodes_filters() {
            let filter = NodeFilter.property("p1").eq(1u64);
            let expected_results = vec!["N1", "N3", "N4", "N6", "N7"];
            assert_filter_nodes_results(
                init_graph,
                CachedGraphTransformer,
                filter.clone(),
                &expected_results,
                TestVariants::EventOnly,
            );
            assert_search_nodes_results(
                init_graph,
                CachedGraphTransformer,
                filter,
                &expected_results,
                TestVariants::EventOnly,
            );
        }

        #[test]
        fn test_nodes_filters_w() {
            // TODO: Enable event_disk_graph for filter_nodes once bug fixed: https://github.com/Pometry/Raphtory/issues/2098
            let filter = NodeFilter.property("p1").eq(1u64);
            let expected_results = vec!["N1", "N3", "N6"];
            assert_filter_nodes_results(
                init_graph,
                WindowedCachedGraphTransformer(6..9),
                filter.clone(),
                &expected_results,
                vec![TestGraphVariants::Graph],
            );
            assert_search_nodes_results(
                init_graph,
                WindowedCachedGraphTransformer(6..9),
                filter,
                &expected_results,
                TestVariants::EventOnly,
            );
        }

        #[test]
        fn test_nodes_filters_pg_w() {
            let filter = NodeFilter.property("p1").ge(2u64);
            let expected_results = vec!["N2", "N5", "N8"];
            assert_filter_nodes_results(
                init_graph,
                WindowedCachedGraphTransformer(6..9),
                filter.clone(),
                &expected_results,
                TestVariants::PersistentOnly,
            );
            assert_search_nodes_results(
                init_graph,
                WindowedCachedGraphTransformer(6..9),
                filter,
                &expected_results,
                TestVariants::PersistentOnly,
            );
        }
    }

    mod test_edges_filter_cached_view_graph {
        use raphtory::{
            db::{
                api::view::StaticGraphViewOps,
                graph::assertions::{
                    assert_filter_edges_results, assert_search_edges_results, TestVariants,
                },
            },
            prelude::{AdditionOps, EdgeFilter},
        };
        use raphtory_api::core::entities::properties::prop::Prop;

        use crate::test_filters_cached_view::{
            CachedGraphTransformer, WindowedCachedGraphTransformer,
        };
        use raphtory::db::graph::views::filter::model::{
            property_filter::ops::PropertyFilterOps, PropertyFilterFactory,
        };

        fn init_graph<G: StaticGraphViewOps + AdditionOps>(graph: G) -> G {
            let edge_data = vec![
                (6, "N1", "N2", 2u64),
                (7, "N1", "N2", 1u64),
                (6, "N2", "N3", 1u64),
                (7, "N2", "N3", 2u64),
                (8, "N3", "N4", 1u64),
                (9, "N4", "N5", 1u64),
                (5, "N5", "N6", 1u64),
                (6, "N5", "N6", 2u64),
                (5, "N6", "N7", 1u64),
                (6, "N6", "N7", 1u64),
                (3, "N7", "N8", 1u64),
                (5, "N7", "N8", 1u64),
                (3, "N8", "N1", 1u64),
                (4, "N8", "N1", 2u64),
            ];

            for (ts, src, dst, p1_val) in edge_data {
                graph
                    .add_edge(ts, src, dst, [("p1", Prop::U64(p1_val))], None)
                    .unwrap();
            }

            graph
        }

        #[test]
        fn test_edges_filters() {
            // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
            let filter = EdgeFilter.property("p1").eq(1u64);
            let expected_results = vec!["N1->N2", "N3->N4", "N4->N5", "N6->N7", "N7->N8"];
            assert_filter_edges_results(
                init_graph,
                CachedGraphTransformer,
                filter.clone(),
                &expected_results,
                TestVariants::EventOnly,
            );
            assert_search_edges_results(
                init_graph,
                CachedGraphTransformer,
                filter,
                &expected_results,
                TestVariants::All,
            );
        }

        #[test]
        fn test_edges_filter_w() {
            let filter = EdgeFilter.property("p1").eq(1u64);
            let expected_results = vec!["N1->N2", "N3->N4", "N6->N7"];
            assert_filter_edges_results(
                init_graph,
                WindowedCachedGraphTransformer(6..9),
                filter.clone(),
                &expected_results,
                TestVariants::EventOnly,
            );
            assert_search_edges_results(
                init_graph,
                WindowedCachedGraphTransformer(6..9),
                filter,
                &expected_results,
                TestVariants::EventOnly,
            );
        }

        #[test]
        fn test_edges_filters_pg_w() {
            // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
            let filter = EdgeFilter.property("p1").ge(2u64);
            let expected_results = vec!["N2->N3", "N5->N6", "N8->N1"];
            assert_filter_edges_results(
                init_graph,
                WindowedCachedGraphTransformer(6..9),
                filter.clone(),
                &expected_results,
                vec![],
            );
            assert_search_edges_results(
                init_graph,
                WindowedCachedGraphTransformer(6..9),
                filter,
                &expected_results,
                TestVariants::PersistentOnly,
            );
        }
    }
}
