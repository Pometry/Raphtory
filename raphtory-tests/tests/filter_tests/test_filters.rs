use raphtory::{db::api::view::StaticGraphViewOps, prelude::*};

mod test_composite_filters {
    use raphtory::{
        db::graph::views::filter::model::{
            edge_filter::EdgeFilter, filter::Filter, node_filter::NodeFilter,
            property_filter::ops::PropertyFilterOps, PropertyFilterFactory,
        },
        prelude::IntoProp,
    };
    use raphtory_api::core::{entities::properties::prop::Prop, storage::arc_str::ArcStr};

    #[test]
    fn test_fuzzy_search() {
        let filter = Filter::fuzzy_search("name", "pomet", 2, false);
        assert!(filter.matches(Some("pometry")));

        let filter = Filter::fuzzy_search("name", "shivam_kapoor", 2, false);
        assert!(filter.matches(Some("shivam_kapoor2")));

        let filter = Filter::fuzzy_search("name", "shivam kapoor", 2, false);
        assert!(filter.matches(Some("shivam_kapoor2")));

        let filter = Filter::fuzzy_search("name", "shivam kapoor", 2, false);
        assert!(filter.matches(Some("shivam_kapoor2")));

        let filter = Filter::fuzzy_search("name", "shivam kapoor", 2, false);
        assert!(!filter.matches(Some("shivam1_kapoor2")));

        let filter = Filter::fuzzy_search("name", "khivam sapoor", 2, false);
        assert!(!filter.matches(Some("shivam1_kapoor2")));
    }

    #[test]
    fn test_fuzzy_search_prefix_match() {
        let filter = Filter::fuzzy_search("name", "pome", 2, false);
        assert!(!filter.matches(Some("pometry")));

        let filter = Filter::fuzzy_search("name", "pome", 2, true);
        assert!(filter.matches(Some("pometry")));
    }

    #[test]
    fn test_fuzzy_search_property() {
        let filter = NodeFilter.property("prop").fuzzy_search("pomet", 2, false);
        assert!(filter.matches(Some(&Prop::Str(ArcStr::from("pometry")))));
    }

    #[test]
    fn test_fuzzy_search_property_prefix_match() {
        let filter = EdgeFilter.property("prop").fuzzy_search("pome", 2, false);
        assert!(!filter.matches(Some(&Prop::Str(ArcStr::from("pometry")))));

        let filter = EdgeFilter.property("prop").fuzzy_search("pome", 2, true);
        assert!(filter.matches(Some(&Prop::Str(ArcStr::from("pometry")))));
    }

    #[test]
    fn test_contains_match() {
        let filter = EdgeFilter.property("prop").contains("shivam");
        let res = filter.matches(Some(&Prop::Str(ArcStr::from("shivam_kapoor"))));
        assert!(res);
        let res = filter.matches(None);
        assert!(!res);

        let filter = EdgeFilter.property("prop").contains("am_ka");
        let res = filter.matches(Some(&Prop::Str(ArcStr::from("shivam_kapoor"))));
        assert!(res);
    }

    #[test]
    fn test_contains_not_match() {
        let filter = NodeFilter.property("prop").not_contains("shivam");
        let res = filter.matches(Some(&Prop::Str(ArcStr::from("shivam_kapoor"))));
        assert!(!res);
        let res = filter.matches(None);
        assert!(!res);
    }

    #[test]
    fn test_is_in_match() {
        let filter = NodeFilter
            .property("prop")
            .is_in(vec!["shivam".into_prop()]);
        let res = filter.matches(Some(&Prop::Str(ArcStr::from("shivam"))));
        assert!(res);
        let res = filter.matches(None);
        assert!(!res);
    }

    #[test]
    fn test_is_not_in_match() {
        let filter = EdgeFilter
            .property("prop")
            .is_not_in(vec!["shivam".into_prop()]);
        let res = filter.matches(Some(&Prop::Str(ArcStr::from("shivam"))));
        assert!(!res);
        let res = filter.matches(None);
        assert!(!res);
    }
}

use raphtory_api::core::entities::properties::prop::IntoProp;
use raphtory_storage::mutation::{
    addition_ops::InternalAdditionOps, property_addition_ops::InternalPropertyAdditionOps,
};
use raphtory_tests::assertions::GraphTransformer;

struct IdentityGraphTransformer;

impl GraphTransformer for IdentityGraphTransformer {
    type Return<G: StaticGraphViewOps> = G;
    fn apply<G: StaticGraphViewOps>(&self, graph: G) -> Self::Return<G> {
        graph
    }
}

mod test_property_semantics {
    mod test_node_property_filter_semantics {
        use crate::filter_tests::test_filters::IdentityGraphTransformer;
        use raphtory::{
            db::{
                api::view::{filter_ops::Filter, StaticGraphViewOps},
                graph::views::filter::model::{
                    node_filter::NodeFilter, property_filter::ops::PropertyFilterOps,
                    PropertyFilterFactory, TemporalPropertyFilterFactory,
                },
            },
            errors::GraphError,
            prelude::*,
        };
        use raphtory_api::core::entities::properties::prop::Prop;
        use raphtory_storage::mutation::{
            addition_ops::InternalAdditionOps, property_addition_ops::InternalPropertyAdditionOps,
        };
        use raphtory_tests::assertions::{assert_filter_nodes_results, TestVariants};

        fn init_graph<G: StaticGraphViewOps + AdditionOps + PropertyAdditionOps>(graph: G) -> G {
            let nodes = [
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
                (2, "N9", vec![("p1", Prop::U64(2u64))]),
                (2, "N10", vec![("q1", Prop::U64(0u64))]),
                (2, "N10", vec![("p1", Prop::U64(3u64))]),
                (2, "N11", vec![("p1", Prop::U64(3u64))]),
                (2, "N11", vec![("q1", Prop::U64(0u64))]),
                (2, "N12", vec![("q1", Prop::U64(0u64))]),
                (3, "N12", vec![("p1", Prop::U64(3u64))]),
                (2, "N13", vec![("q1", Prop::U64(0u64))]),
                (3, "N13", vec![("p1", Prop::U64(3u64))]),
                (2, "N14", vec![("q1", Prop::U64(0u64))]),
                (2, "N15", vec![]),
            ];

            for (id, label, props) in nodes.iter() {
                graph
                    .add_node(*id, label, props.clone(), None, None)
                    .unwrap();
            }

            let metadata = [
                ("N1", [("p1", Prop::U64(1u64))]),
                ("N4", [("p1", Prop::U64(2u64))]),
                ("N9", [("p1", Prop::U64(1u64))]),
                ("N10", [("p1", Prop::U64(1u64))]),
                ("N11", [("p1", Prop::U64(1u64))]),
                ("N12", [("p1", Prop::U64(1u64))]),
                ("N13", [("p1", Prop::U64(1u64))]),
                ("N14", [("p1", Prop::U64(1u64))]),
                ("N15", [("p1", Prop::U64(1u64))]),
            ];

            for (node, props) in metadata.iter() {
                graph
                    .node(node)
                    .unwrap()
                    .add_metadata(props.clone())
                    .unwrap();
            }

            graph
        }

        fn init_graph_for_event_ids<
            G: StaticGraphViewOps
                + AdditionOps
                + InternalAdditionOps
                + InternalPropertyAdditionOps
                + PropertyAdditionOps,
        >(
            graph: G,
        ) -> G {
            let graph: G = init_graph(graph);
            let nodes = [
                (1, "N16", vec![("p1", Prop::U64(2u64))]),
                (1, "N16", vec![("p1", Prop::U64(1u64))]),
                (1, "N17", vec![("p1", Prop::U64(1u64))]),
                (1, "N17", vec![("p1", Prop::U64(2u64))]),
            ];

            for (id, label, props) in nodes.iter() {
                graph
                    .add_node(*id, label, props.clone(), None, None)
                    .unwrap();
            }

            graph
        }

        #[test]
        fn test_metadata_semantics() {
            let filter = NodeFilter.metadata("p1").eq(1u64);
            let expected_results = vec!["N1", "N10", "N11", "N12", "N13", "N14", "N15", "N9"];
            assert_filter_nodes_results(
                init_graph,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                TestVariants::All,
            );
        }

        #[test]
        fn test_temporal_any_semantics() {
            let filter = NodeFilter.property("p1").temporal().any().eq(1u64);
            let expected_results = vec!["N1", "N2", "N3", "N4", "N5", "N6", "N7", "N8"];
            assert_filter_nodes_results(
                init_graph,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                TestVariants::All,
            );
        }

        #[test]
        fn test_temporal_any_semantics_for_event_ids() {
            let filter = NodeFilter.property("p1").temporal().any().eq(1u64);
            let expected_results =
                vec!["N1", "N16", "N17", "N2", "N3", "N4", "N5", "N6", "N7", "N8"];
            assert_filter_nodes_results(
                init_graph_for_event_ids,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                TestVariants::All,
            );
        }

        #[test]
        fn test_temporal_latest_semantics() {
            let filter = NodeFilter.property("p1").temporal().last().eq(1u64);
            let expected_results = vec!["N1", "N3", "N4", "N6", "N7"];
            assert_filter_nodes_results(
                init_graph,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                TestVariants::All,
            );
        }

        #[test]
        fn test_temporal_latest_semantics_for_event_ids() {
            let filter = NodeFilter.property("p1").temporal().last().eq(1u64);
            let expected_results = vec!["N1", "N16", "N3", "N4", "N6", "N7"];
            assert_filter_nodes_results(
                init_graph_for_event_ids,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                TestVariants::All,
            );
        }

        #[test]
        fn test_property_semantics() {
            // TODO: Const properties not supported for disk_graph.
            let filter = NodeFilter.property("p1").eq(1u64);
            let expected_results = vec!["N1", "N3", "N4", "N6", "N7"];
            assert_filter_nodes_results(
                init_graph,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                TestVariants::All,
            );
        }

        #[test]
        fn test_property_semantics_for_event_ids() {
            let filter = NodeFilter.property("p1").eq(1u64);
            let expected_results = vec!["N1", "N16", "N3", "N4", "N6", "N7"];
            assert_filter_nodes_results(
                init_graph_for_event_ids,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                TestVariants::All,
            );
        }

        #[test]
        fn test_property_semantics_only_metadata() {
            // For this graph there won't be any temporal property index for property name "p1".
            fn init_graph<
                G: StaticGraphViewOps
                    + AdditionOps
                    + InternalAdditionOps
                    + InternalPropertyAdditionOps
                    + PropertyAdditionOps,
            >(
                graph: G,
            ) -> G {
                let nodes = [(2, "N1", vec![("q1", Prop::U64(0u64))]), (2, "N2", vec![])];

                for (id, label, props) in nodes.iter() {
                    graph
                        .add_node(*id, label, props.clone(), None, None)
                        .unwrap();
                }

                let metadata = [
                    ("N1", [("p1", Prop::U64(1u64))]),
                    ("N2", [("p1", Prop::U64(1u64))]),
                ];

                for (node, props) in metadata.iter() {
                    graph
                        .node(node)
                        .unwrap()
                        .add_metadata(props.clone())
                        .unwrap();
                }

                graph
            }

            let filter = NodeFilter.property("p1").ge(1u64);
            let graph = init_graph(Graph::new());
            assert!(matches!(
                graph.filter(filter.clone()).unwrap_err(),
                GraphError::PropertyMissingError(ref name) if name == "p1"
            ));
            assert!(matches!(
                graph.persistent_graph().filter(filter).unwrap_err(),
                GraphError::PropertyMissingError(ref name) if name == "p1"
            ));
        }

        #[test]
        fn test_property_semantics_only_temporal() {
            // For this graph there won't be any metadata index for property name "p1".
            fn init_graph<
                G: StaticGraphViewOps
                    + AdditionOps
                    + InternalAdditionOps
                    + InternalPropertyAdditionOps
                    + PropertyAdditionOps,
            >(
                graph: G,
            ) -> G {
                let nodes = [
                    (1, "N1", vec![("p1", Prop::U64(1u64))]),
                    (2, "N2", vec![("p1", Prop::U64(1u64))]),
                    (3, "N2", vec![("p1", Prop::U64(2u64))]),
                    (2, "N3", vec![("p1", Prop::U64(2u64))]),
                    (3, "N3", vec![("p1", Prop::U64(1u64))]),
                    (3, "N4", vec![]),
                ];

                for (id, label, props) in nodes.iter() {
                    graph
                        .add_node(*id, label, props.clone(), None, None)
                        .unwrap();
                }

                graph
            }

            let filter = NodeFilter.property("p1").le(1u64);
            let expected_results = vec!["N1", "N3"];
            assert_filter_nodes_results(
                init_graph,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                TestVariants::All,
            );
        }
    }

    mod test_edge_property_filter_semantics {
        use crate::filter_tests::test_filters::IdentityGraphTransformer;
        use raphtory::{
            db::{
                api::view::{filter_ops::Filter, EdgeViewOps, StaticGraphViewOps},
                graph::views::filter::{
                    model::{
                        edge_filter::EdgeFilter, property_filter::ops::PropertyFilterOps,
                        PropertyFilterFactory, TemporalPropertyFilterFactory,
                    },
                    CreateFilter,
                },
            },
            errors::GraphError,
            prelude::*,
        };
        use raphtory_api::core::entities::properties::prop::Prop;
        use raphtory_storage::mutation::{
            addition_ops::InternalAdditionOps, property_addition_ops::InternalPropertyAdditionOps,
        };
        use raphtory_tests::assertions::{
            assert_filter_edges_results, TestVariants, WindowGraphTransformer,
        };

        fn init_graph<
            G: StaticGraphViewOps
                + AdditionOps
                + InternalAdditionOps
                + InternalPropertyAdditionOps
                + PropertyAdditionOps,
        >(
            graph: G,
        ) -> G {
            let edges = [
                (6, "N1", "N2", vec![("p1", Prop::U64(2u64))]),
                (7, "N1", "N2", vec![("p1", Prop::U64(1u64))]),
                (6, "N2", "N3", vec![("p1", Prop::U64(1u64))]),
                (7, "N2", "N3", vec![("p1", Prop::U64(2u64))]),
                (8, "N3", "N4", vec![("p1", Prop::U64(1u64))]),
                (9, "N4", "N5", vec![("p1", Prop::U64(1u64))]),
                (5, "N5", "N6", vec![("p1", Prop::U64(1u64))]),
                (6, "N5", "N6", vec![("p1", Prop::U64(2u64))]),
                (5, "N6", "N7", vec![("p1", Prop::U64(1u64))]),
                (6, "N6", "N7", vec![("p1", Prop::U64(1u64))]),
                (3, "N7", "N8", vec![("p1", Prop::U64(1u64))]),
                (5, "N7", "N8", vec![("p1", Prop::U64(1u64))]),
                (3, "N8", "N9", vec![("p1", Prop::U64(1u64))]),
                (4, "N8", "N9", vec![("p1", Prop::U64(2u64))]),
                (2, "N9", "N10", vec![("p1", Prop::U64(2u64))]),
                (2, "N10", "N11", vec![("q1", Prop::U64(0u64))]),
                (2, "N10", "N11", vec![("p1", Prop::U64(3u64))]),
                (2, "N11", "N12", vec![("p1", Prop::U64(3u64))]),
                (2, "N11", "N12", vec![("q1", Prop::U64(0u64))]),
                (2, "N12", "N13", vec![("q1", Prop::U64(0u64))]),
                (3, "N12", "N13", vec![("p1", Prop::U64(3u64))]),
                (2, "N13", "N14", vec![("q1", Prop::U64(0u64))]),
                (3, "N13", "N14", vec![("p1", Prop::U64(3u64))]),
                (2, "N14", "N15", vec![("q1", Prop::U64(0u64))]),
                (2, "N15", "N1", vec![]),
            ];

            for (time, src, dst, props) in edges {
                graph.add_edge(time, src, dst, props, None).unwrap();
            }

            let metadata_edges = [
                ("N1", "N2", vec![("p1", Prop::U64(1u64))]),
                ("N4", "N5", vec![("p1", Prop::U64(2u64))]),
                ("N9", "N10", vec![("p1", Prop::U64(1u64))]),
                ("N10", "N11", vec![("p1", Prop::U64(1u64))]),
                ("N11", "N12", vec![("p1", Prop::U64(1u64))]),
                ("N12", "N13", vec![("p1", Prop::U64(1u64))]),
                ("N13", "N14", vec![("p1", Prop::U64(1u64))]),
                ("N14", "N15", vec![("p1", Prop::U64(1u64))]),
                ("N15", "N1", vec![("p1", Prop::U64(1u64))]),
            ];

            for (src, dst, props) in metadata_edges {
                graph
                    .edge(src, dst)
                    .unwrap()
                    .add_metadata(props.clone(), None)
                    .unwrap();
            }

            graph
        }

        fn init_graph_for_event_ids<
            G: StaticGraphViewOps
                + AdditionOps
                + InternalAdditionOps
                + InternalPropertyAdditionOps
                + PropertyAdditionOps,
        >(
            graph: G,
        ) -> G {
            let graph: G = init_graph(graph);
            let edge_data = [
                (1, "N16", "N15", vec![("p1", Prop::U64(2u64))]),
                (1, "N16", "N15", vec![("p1", Prop::U64(1u64))]),
                (1, "N17", "N16", vec![("p1", Prop::U64(1u64))]),
                (1, "N17", "N16", vec![("p1", Prop::U64(2u64))]),
            ];

            for (time, src, dst, props) in edge_data {
                graph.add_edge(time, src, dst, props, None).unwrap();
            }

            graph
        }

        #[test]
        fn test_persistent_graph_first_window() {
            fn init_graph<
                G: StaticGraphViewOps
                    + AdditionOps
                    + InternalAdditionOps
                    + InternalPropertyAdditionOps
                    + PropertyAdditionOps,
            >(
                graph: G,
            ) -> G {
                graph
                    .add_edge(0, 1, 2, [("p1", Prop::U64(1u64))], None)
                    .unwrap();
                graph
                    .add_edge(2, 1, 2, [("p1", Prop::U64(2u64))], None)
                    .unwrap();
                graph
                    .add_edge(5, 1, 2, [("p1", Prop::U64(5u64))], None)
                    .unwrap();
                graph
                    .add_edge(10, 1, 2, [("p1", Prop::U64(10u64))], None)
                    .unwrap();
                graph
            }

            let filter = EdgeFilter.property("p1").temporal().first().eq(2u64);

            // No window; means the first update is at time 0 and the value of p1 is expected to be 1u64.
            let expected_empty = [];
            let expected_found = ["1->2"];

            assert_filter_edges_results(
                init_graph,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_empty,
                TestVariants::PersistentOnly,
            );

            // Window(1,10); Expected emtpy because the first update is at time 0 and the value of p1 is expected to be 1u64.
            assert_filter_edges_results(
                init_graph,
                WindowGraphTransformer(1..10),
                filter.clone(),
                &expected_empty,
                TestVariants::PersistentOnly,
            );

            // Window(2,10); Expected update at time 2 and the value of p1 is expected to be 2u64.
            assert_filter_edges_results(
                init_graph,
                WindowGraphTransformer(2..10),
                filter.clone(),
                &expected_found,
                TestVariants::PersistentOnly,
            );

            // Window(3,10); Expected update at time 2 (even if it is outside the window) and the value of p1 is expected to be 2u64.
            assert_filter_edges_results(
                init_graph,
                WindowGraphTransformer(3..10),
                filter.clone(),
                &expected_found,
                TestVariants::PersistentOnly,
            );

            // Window(4,10); Expected update at time 2 (even if it is outside the window) and the value of p1 is expected to be 2u64.
            assert_filter_edges_results(
                init_graph,
                WindowGraphTransformer(4..10),
                filter.clone(),
                &expected_found,
                TestVariants::PersistentOnly,
            );

            // Window(5,10); Expected update at time 5 (even if it is outside the window) and the value of p1 is expected to be 5u64.
            assert_filter_edges_results(
                init_graph,
                WindowGraphTransformer(5..10),
                filter.clone(),
                &expected_empty,
                TestVariants::PersistentOnly,
            );
        }

        #[test]
        fn test_metadata_semantics() {
            let filter = EdgeFilter.metadata("p1").eq(1u64);
            let expected_results = vec![
                "N1->N2", "N10->N11", "N11->N12", "N12->N13", "N13->N14", "N14->N15", "N15->N1",
                "N9->N10",
            ];
            assert_filter_edges_results(
                init_graph,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                TestVariants::All,
            );
        }

        #[test]
        fn test_metadata_semantics2() {
            fn filter_edges(graph: &Graph, filter: impl CreateFilter) -> Vec<String> {
                let mut results = graph
                    .filter(filter)
                    .unwrap()
                    .edges()
                    .iter()
                    .map(|e| format!("{}->{}", e.src().name(), e.dst().name()))
                    .collect::<Vec<_>>();
                results.sort();
                results
            }

            let graph = init_graph(Graph::new());

            let filter = EdgeFilter.metadata("p1").eq(1u64);
            assert_eq!(
                filter_edges(&graph, filter.clone()),
                vec![
                    "N1->N2", "N10->N11", "N11->N12", "N12->N13", "N13->N14", "N14->N15",
                    "N15->N1", "N9->N10"
                ]
            );

            let edge = graph
                .add_edge(1, "shivam", "kapoor", [("p1", 100u64)], Some("fire_nation"))
                .unwrap();
            edge.add_metadata([("z", true)], Some("fire_nation"))
                .unwrap();
            let prop = graph.edge("shivam", "kapoor").unwrap().metadata().get("z");
            assert_eq!(prop, Some(Prop::map([("fire_nation", true)])));

            let filter2 = EdgeFilter
                .metadata("z")
                .eq(Prop::map([("fire_nation", true)]));
            assert_eq!(filter_edges(&graph, filter2), vec!["shivam->kapoor"]);

            let filter = EdgeFilter
                .metadata("p1")
                .eq(Prop::map([("_default", 1u64)]));
            assert_eq!(
                filter_edges(&graph, filter),
                vec![
                    "N1->N2", "N10->N11", "N11->N12", "N12->N13", "N13->N14", "N14->N15",
                    "N15->N1", "N9->N10"
                ]
            );
        }

        #[test]
        fn test_temporal_any_semantics() {
            let filter = EdgeFilter.property("p1").temporal().any().eq(1u64);
            let expected_results = vec![
                "N1->N2", "N2->N3", "N3->N4", "N4->N5", "N5->N6", "N6->N7", "N7->N8", "N8->N9",
            ];
            assert_filter_edges_results(
                init_graph,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                TestVariants::All,
            );
        }

        #[test]
        fn test_temporal_any_semantics_for_event_ids() {
            let filter = EdgeFilter.property("p1").temporal().any().lt(2u64);
            let expected_results = vec![
                "N1->N2", "N16->N15", "N17->N16", "N2->N3", "N3->N4", "N4->N5", "N5->N6", "N6->N7",
                "N7->N8", "N8->N9",
            ];
            assert_filter_edges_results(
                init_graph_for_event_ids,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                TestVariants::All,
            );
        }

        #[test]
        fn test_temporal_latest_semantics() {
            let filter = EdgeFilter.property("p1").temporal().last().eq(1u64);
            let expected_results = vec!["N1->N2", "N3->N4", "N4->N5", "N6->N7", "N7->N8"];
            assert_filter_edges_results(
                init_graph,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                TestVariants::All,
            );
        }

        #[test]
        fn test_temporal_latest_semantics_for_event_ids() {
            let filter = EdgeFilter.property("p1").temporal().last().eq(1u64);
            let expected_results =
                vec!["N1->N2", "N16->N15", "N3->N4", "N4->N5", "N6->N7", "N7->N8"];
            assert_filter_edges_results(
                init_graph_for_event_ids,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                TestVariants::All,
            );
        }

        #[test]
        fn test_property_semantics() {
            let filter = EdgeFilter.property("p1").ge(2u64);
            let expected_results = vec![
                "N10->N11", "N11->N12", "N12->N13", "N13->N14", "N2->N3", "N5->N6", "N8->N9",
                "N9->N10",
            ];
            assert_filter_edges_results(
                init_graph,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                TestVariants::All,
            );
        }

        #[test]
        fn test_property_semantics_for_event_ids() {
            let filter = EdgeFilter.property("p1").eq(1u64);
            let expected_results =
                vec!["N1->N2", "N16->N15", "N3->N4", "N4->N5", "N6->N7", "N7->N8"];
            assert_filter_edges_results(
                init_graph_for_event_ids,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                TestVariants::All,
            );
        }

        #[test]
        fn test_property_semantics_only_metadata() {
            // For this graph there won't be any temporal property index for property name "p1".
            fn init_graph<
                G: StaticGraphViewOps
                    + AdditionOps
                    + InternalAdditionOps
                    + InternalPropertyAdditionOps
                    + PropertyAdditionOps,
            >(
                graph: G,
            ) -> G {
                let edges = [
                    (2, "N1", "N2", vec![("q1", Prop::U64(0u64))]),
                    (2, "N2", "N3", vec![]),
                ];

                for (time, src, dst, props) in edges {
                    graph.add_edge(time, src, dst, props, None).unwrap();
                }

                let metadata_edges = [
                    ("N1", "N2", vec![("p1", Prop::U64(1u64))]),
                    ("N2", "N3", vec![("p1", Prop::U64(1u64))]),
                ];

                for (src, dst, props) in metadata_edges {
                    graph
                        .edge(src, dst)
                        .unwrap()
                        .add_metadata(props.clone(), None)
                        .unwrap();
                }

                graph
            }

            let filter = EdgeFilter.property("p1").eq(1u64);
            let graph = init_graph(Graph::new());
            assert!(matches!(
                graph.filter(filter.clone()).unwrap_err(),
                GraphError::PropertyMissingError(ref name) if name == "p1"
            ));
            assert!(matches!(
                graph.persistent_graph().filter(filter).unwrap_err(),
                GraphError::PropertyMissingError(ref name) if name == "p1"
            ));
        }

        #[test]
        fn test_property_semantics_only_temporal() {
            // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
            // For this graph there won't be any metadata index for property name "p1".
            fn init_graph<
                G: StaticGraphViewOps
                    + AdditionOps
                    + InternalAdditionOps
                    + InternalPropertyAdditionOps
                    + PropertyAdditionOps,
            >(
                graph: G,
            ) -> G {
                let edges = [
                    (1, "N1", "N2", vec![("p1", Prop::U64(1u64))]),
                    (2, "N2", "N3", vec![("p1", Prop::U64(1u64))]),
                    (3, "N2", "N3", vec![("p1", Prop::U64(2u64))]),
                    (2, "N3", "N4", vec![("p1", Prop::U64(2u64))]),
                    (3, "N3", "N4", vec![("p1", Prop::U64(1u64))]),
                    (2, "N4", "N5", vec![]),
                ];

                for (time, src, dst, props) in edges {
                    graph.add_edge(time, src, dst, props, None).unwrap();
                }

                graph
            }

            let filter = EdgeFilter.property("p1").eq(1u64);
            let expected_results = vec!["N1->N2", "N3->N4"];
            assert_filter_edges_results(
                init_graph,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                TestVariants::All,
            );
        }
    }
}

fn init_nodes_graph<
    G: StaticGraphViewOps
        + AdditionOps
        + InternalAdditionOps
        + InternalPropertyAdditionOps
        + PropertyAdditionOps,
>(
    graph: G,
) -> G {
    let nodes = [
        (
            1,
            "1",
            vec![
                ("p1", "shivam_kapoor".into_prop()),
                ("p9", 5u64.into_prop()),
                ("p10", "Paper_airplane".into_prop()),
                ("p20", "Gold_ship".into_prop()),
                ("p30", "Gold_ship".into_prop()),
                ("p40", 5u64.into_prop()),
            ],
            Some("fire_nation"),
        ),
        (
            2,
            "2",
            vec![
                ("p1", "prop12".into_prop()),
                ("p2", 2u64.into_prop()),
                ("p10", "Paper_ship".into_prop()),
                ("p20", "Gold_boat".into_prop()),
                ("p30", "Old_boat".into_prop()),
                ("p40", 10u64.into_prop()),
            ],
            Some("air_nomads"),
        ),
        (
            3,
            "2",
            vec![
                ("p20", "Gold_ship".into_prop()),
                ("p30", "Gold_ship".into_prop()),
                ("p40", 15u64.into_prop()),
            ],
            Some("air_nomads"),
        ),
        (
            4,
            "2",
            vec![
                ("p20", "Gold_ship".into_prop()),
                ("p30", "Gold_ship".into_prop()),
                ("p40", 20u64.into_prop()),
            ],
            Some("air_nomads"),
        ),
        (
            3,
            "1",
            vec![
                ("p1", "shivam_kapoor".into_prop()),
                ("p9", 5u64.into_prop()),
                ("p20", "Gold_ship".into_prop()),
                ("p30", "Gold_ship".into_prop()),
                ("p40", 10u64.into_prop()),
            ],
            Some("fire_nation"),
        ),
        (
            3,
            "3",
            vec![
                ("p2", 6u64.into_prop()),
                ("p3", 1u64.into_prop()),
                ("p10", "Paper_airplane".into_prop()),
            ],
            Some("fire_nation"),
        ),
        (
            4,
            "1",
            vec![
                ("p1", "shivam_kapoor".into_prop()),
                ("p9", 5u64.into_prop()),
                ("p20", "Gold_ship".into_prop()),
                ("p30", "Gold_ship".into_prop()),
                ("p40", 15u64.into_prop()),
            ],
            Some("fire_nation"),
        ),
        (
            3,
            "4",
            vec![
                ("p4", "pometry".into_prop()),
                ("p20", "Gold_ship".into_prop()),
                ("p30", "Gold_ship".into_prop()),
            ],
            None,
        ),
        (
            4,
            "4",
            vec![
                ("p5", 12u64.into_prop()),
                ("p20", "Gold_boat".into_prop()),
                ("p30", "Old_ship".into_prop()),
            ],
            None,
        ),
    ];

    for (time, id, props, node_type) in nodes {
        graph.add_node(time, id, props, node_type, None).unwrap();
    }

    let metadata = [
        (
            "1",
            vec![
                ("m1", "pometry".into_prop()),
                ("m2", "raphtory".into_prop()),
            ],
        ),
        ("2", vec![("m1", "raphtory".into_prop())]),
        (
            "3",
            vec![
                ("m2", "pometry".into_prop()),
                ("m3", "raphtory".into_prop()),
            ],
        ),
        (
            "4",
            vec![
                ("m3", "pometry".into_prop()),
                ("m4", "raphtory".into_prop()),
            ],
        ),
    ];

    for (node_id, md) in metadata {
        graph.node(node_id).unwrap().add_metadata(md).unwrap();
    }

    graph
}

fn init_nodes_layers_graph<
    G: StaticGraphViewOps
        + AdditionOps
        + InternalAdditionOps
        + InternalPropertyAdditionOps
        + PropertyAdditionOps,
>(
    graph: G,
) -> G {
    let nodes = [
        (
            1,
            "1",
            vec![
                ("p1", "shivam_kapoor".into_prop()),
                ("p9", 5u64.into_prop()),
                ("p10", "Paper_airplane".into_prop()),
                ("p20", "Gold_ship".into_prop()),
                ("p30", "Gold_ship".into_prop()),
                ("p40", 5u64.into_prop()),
            ],
            Some("fire_nation"),
        ),
        (
            2,
            "2",
            vec![
                ("p1", "prop12".into_prop()),
                ("p2", 2u64.into_prop()),
                ("p10", "Paper_ship".into_prop()),
                ("p20", "Gold_boat".into_prop()),
                ("p30", "Old_boat".into_prop()),
                ("p40", 10u64.into_prop()),
            ],
            Some("air_nomads"),
        ),
        (
            3,
            "2",
            vec![
                ("p20", "Gold_ship".into_prop()),
                ("p30", "Gold_ship".into_prop()),
                ("p40", 15u64.into_prop()),
            ],
            Some("air_nomads"),
        ),
        (
            4,
            "2",
            vec![
                ("p20", "Gold_ship".into_prop()),
                ("p30", "Gold_ship".into_prop()),
                ("p40", 20u64.into_prop()),
            ],
            Some("air_nomads"),
        ),
        (
            3,
            "1",
            vec![
                ("p1", "shivam_kapoor".into_prop()),
                ("p9", 5u64.into_prop()),
                ("p20", "Gold_ship".into_prop()),
                ("p30", "Gold_ship".into_prop()),
                ("p40", 10u64.into_prop()),
            ],
            Some("fire_nation"),
        ),
        (
            3,
            "3",
            vec![
                ("p2", 6u64.into_prop()),
                ("p3", 1u64.into_prop()),
                ("p10", "Paper_airplane".into_prop()),
            ],
            Some("fire_nation"),
        ),
        (
            4,
            "1",
            vec![
                ("p1", "shivam_kapoor".into_prop()),
                ("p9", 5u64.into_prop()),
                ("p20", "Gold_ship".into_prop()),
                ("p30", "Gold_ship".into_prop()),
                ("p40", 15u64.into_prop()),
            ],
            Some("fire_nation"),
        ),
        (
            3,
            "4",
            vec![
                ("p4", "pometry".into_prop()),
                ("p20", "Gold_ship".into_prop()),
                ("p30", "Gold_ship".into_prop()),
            ],
            None,
        ),
        (
            4,
            "4",
            vec![
                ("p5", 12u64.into_prop()),
                ("p20", "Gold_boat".into_prop()),
                ("p30", "Old_ship".into_prop()),
            ],
            None,
        ),
    ];

    for (time, id, props, node_type) in nodes {
        graph.add_node(time, id, props, None, node_type).unwrap();
    }

    let metadata = [
        (
            "1",
            vec![
                ("m1", "pometry".into_prop()),
                ("m2", "raphtory".into_prop()),
            ],
        ),
        ("2", vec![("m1", "raphtory".into_prop())]),
        (
            "3",
            vec![
                ("m2", "pometry".into_prop()),
                ("m3", "raphtory".into_prop()),
            ],
        ),
        (
            "4",
            vec![
                ("m3", "pometry".into_prop()),
                ("m4", "raphtory".into_prop()),
            ],
        ),
    ];

    for (node_id, md) in metadata {
        graph.node(node_id).unwrap().add_metadata(md).unwrap();
    }

    graph
}

fn init_nodes_graph_with_num_ids<
    G: StaticGraphViewOps
        + AdditionOps
        + InternalAdditionOps
        + InternalPropertyAdditionOps
        + PropertyAdditionOps,
>(
    graph: G,
) -> G {
    let nodes = [
        (
            1,
            1,
            vec![
                ("p1", "shivam_kapoor".into_prop()),
                ("p9", 5u64.into_prop()),
                ("p10", "Paper_airplane".into_prop()),
                ("p20", "Gold_ship".into_prop()),
                ("p30", "Gold_ship".into_prop()),
                ("p40", 5u64.into_prop()),
            ],
            Some("fire_nation"),
        ),
        (
            2,
            2,
            vec![
                ("p1", "prop12".into_prop()),
                ("p2", 2u64.into_prop()),
                ("p10", "Paper_ship".into_prop()),
                ("p20", "Gold_boat".into_prop()),
                ("p30", "Old_boat".into_prop()),
                ("p40", 10u64.into_prop()),
            ],
            Some("air_nomads"),
        ),
        (
            3,
            2,
            vec![
                ("p20", "Gold_ship".into_prop()),
                ("p30", "Gold_ship".into_prop()),
                ("p40", 15u64.into_prop()),
            ],
            Some("air_nomads"),
        ),
        (
            4,
            2,
            vec![
                ("p20", "Gold_ship".into_prop()),
                ("p30", "Gold_ship".into_prop()),
                ("p40", 20u64.into_prop()),
            ],
            Some("air_nomads"),
        ),
        (
            3,
            1,
            vec![
                ("p1", "shivam_kapoor".into_prop()),
                ("p9", 5u64.into_prop()),
                ("p20", "Gold_ship".into_prop()),
                ("p30", "Gold_ship".into_prop()),
                ("p40", 10u64.into_prop()),
            ],
            Some("fire_nation"),
        ),
        (
            3,
            3,
            vec![
                ("p2", 6u64.into_prop()),
                ("p3", 1u64.into_prop()),
                ("p10", "Paper_airplane".into_prop()),
            ],
            Some("fire_nation"),
        ),
        (
            4,
            1,
            vec![
                ("p1", "shivam_kapoor".into_prop()),
                ("p9", 5u64.into_prop()),
                ("p20", "Gold_ship".into_prop()),
                ("p30", "Gold_ship".into_prop()),
                ("p40", 15u64.into_prop()),
            ],
            Some("fire_nation"),
        ),
        (
            3,
            4,
            vec![
                ("p4", "pometry".into_prop()),
                ("p20", "Gold_ship".into_prop()),
                ("p30", "Gold_ship".into_prop()),
            ],
            None,
        ),
        (
            4,
            4,
            vec![
                ("p5", 12u64.into_prop()),
                ("p20", "Gold_boat".into_prop()),
                ("p30", "Old_ship".into_prop()),
            ],
            None,
        ),
    ];

    for (time, id, props, node_type) in nodes {
        graph.add_node(time, id, props, node_type, None).unwrap();
    }

    graph
}

fn init_nodes_graph_with_str_ids<
    G: StaticGraphViewOps
        + AdditionOps
        + InternalAdditionOps
        + InternalPropertyAdditionOps
        + PropertyAdditionOps,
>(
    graph: G,
) -> G {
    let nodes = [
        (1, "London", Some("fire_nation")),
        (2, "Two", Some("air_nomads")),
        (3, "Two", Some("air_nomads")),
        (4, "Two", Some("air_nomads")),
        (3, "London", Some("fire_nation")),
        (3, "Tokyo", Some("fire_nation")),
        (4, "London", Some("fire_nation")),
        (3, "France Paris", None),
        (4, "France Paris", None),
    ];

    for (time, id, node_type) in nodes {
        graph.add_node(time, id, NO_PROPS, node_type, None).unwrap();
    }

    graph
}

fn init_edges_graph<
    G: StaticGraphViewOps
        + AdditionOps
        + InternalAdditionOps
        + InternalPropertyAdditionOps
        + PropertyAdditionOps,
>(
    graph: G,
) -> G {
    let edges = [
        (
            1,
            "1",
            "2",
            vec![
                ("p1", "shivam_kapoor".into_prop()),
                ("p10", "Paper_airplane".into_prop()),
                ("p20", "Gold_ship".into_prop()),
                ("p30", "Gold_ship".into_prop()),
            ],
            Some("fire_nation"),
        ),
        (
            2,
            "1",
            "2",
            vec![
                ("p1", "shivam_kapoor".into_prop()),
                ("p2", 4u64.into_prop()),
                ("p20", "Gold_ship".into_prop()),
                ("p30", "Gold_ship".into_prop()),
            ],
            Some("fire_nation"),
        ),
        (
            2,
            "2",
            "3",
            vec![
                ("p1", "prop12".into_prop()),
                ("p2", 2u64.into_prop()),
                ("p10", "Paper_ship".into_prop()),
                ("p20", "Gold_boat".into_prop()),
                ("p30", "Old_boat".into_prop()),
            ],
            Some("air_nomads"),
        ),
        (
            3,
            "2",
            "3",
            vec![
                ("p20", "Gold_ship".into_prop()),
                ("p30", "Gold_boat".into_prop()),
            ],
            Some("air_nomads"),
        ),
        (
            3,
            "3",
            "1",
            vec![("p2", 6u64.into_prop()), ("p3", 1u64.into_prop())],
            Some("fire_nation"),
        ),
        (
            3,
            "2",
            "1",
            vec![
                ("p2", 6u64.into_prop()),
                ("p3", 1u64.into_prop()),
                ("p10", "Paper_airplane".into_prop()),
            ],
            None,
        ),
        (
            4,
            "David Gilmour",
            "John Mayer",
            vec![("p2", 6u64.into_prop()), ("p3", 1u64.into_prop())],
            None,
        ),
        (
            4,
            "John Mayer",
            "Jimmy Page",
            vec![("p2", 6u64.into_prop()), ("p3", 1u64.into_prop())],
            None,
        ),
    ];

    for (time, src, dst, props, edge_type) in edges {
        graph.add_edge(time, src, dst, props, edge_type).unwrap();
    }

    graph
}

fn init_edges_graph2<
    G: StaticGraphViewOps
        + AdditionOps
        + InternalAdditionOps
        + InternalPropertyAdditionOps
        + PropertyAdditionOps,
>(
    graph: G,
) -> G {
    let edges = [
        (
            1,
            "1",
            "2",
            vec![
                ("p1", "shivam_kapoor".into_prop()),
                ("p2", 6u64.into_prop()),
                ("p10", "Paper_airplane".into_prop()),
                ("p20", "Gold_ship".into_prop()),
            ],
            Some("fire_nation"),
        ),
        (
            2,
            "1",
            "2",
            vec![
                ("p1", "shivam_kapoor".into_prop()),
                ("p2", 7u64.into_prop()),
                ("p10", "Gold_ship".into_prop()),
                ("p20", "Gold_ship".into_prop()),
            ],
            Some("fire_nation"),
        ),
        (
            2,
            "1",
            "2",
            vec![
                ("p1", "shivam_kapoor".into_prop()),
                ("p2", 4u64.into_prop()),
                ("p20", "Gold_ship".into_prop()),
            ],
            Some("air_nomads"),
        ),
        (
            2,
            "2",
            "3",
            vec![
                ("p1", "prop12".into_prop()),
                ("p2", 2u64.into_prop()),
                ("p10", "Paper_ship".into_prop()),
                ("p20", "Gold_boat".into_prop()),
            ],
            Some("air_nomads"),
        ),
        (
            3,
            "2",
            "3",
            vec![
                ("p20", "Gold_ship".into_prop()),
                ("p30", "Gold_boat".into_prop()),
            ],
            Some("air_nomads"),
        ),
        (
            3,
            "3",
            "1",
            vec![("p2", 6u64.into_prop()), ("p3", 1u64.into_prop())],
            Some("air_nomads"),
        ),
        (
            3,
            "2",
            "1",
            vec![
                ("p2", 6u64.into_prop()),
                ("p3", 1u64.into_prop()),
                ("p10", "Paper_airplane".into_prop()),
            ],
            None,
        ),
    ];

    for (time, src, dst, props, edge_type) in edges {
        graph.add_edge(time, src, dst, props, edge_type).unwrap();
    }

    graph
}

fn init_edges_graph_with_num_ids<
    G: StaticGraphViewOps
        + AdditionOps
        + InternalAdditionOps
        + InternalPropertyAdditionOps
        + PropertyAdditionOps,
>(
    graph: G,
) -> G {
    let edges = [
        (
            1,
            1,
            2,
            vec![
                ("p1", "shivam_kapoor".into_prop()),
                ("p10", "Paper_airplane".into_prop()),
                ("p20", "Gold_ship".into_prop()),
                ("p30", "Gold_ship".into_prop()),
            ],
            Some("fire_nation"),
        ),
        (
            2,
            1,
            2,
            vec![
                ("p1", "shivam_kapoor".into_prop()),
                ("p2", 4u64.into_prop()),
                ("p20", "Gold_ship".into_prop()),
                ("p30", "Gold_ship".into_prop()),
            ],
            Some("fire_nation"),
        ),
        (
            2,
            2,
            3,
            vec![
                ("p1", "prop12".into_prop()),
                ("p2", 2u64.into_prop()),
                ("p10", "Paper_ship".into_prop()),
                ("p20", "Gold_boat".into_prop()),
                ("p30", "Old_boat".into_prop()),
            ],
            Some("air_nomads"),
        ),
        (
            3,
            2,
            3,
            vec![
                ("p20", "Gold_ship".into_prop()),
                ("p30", "Gold_boat".into_prop()),
            ],
            Some("air_nomads"),
        ),
        (
            3,
            3,
            1,
            vec![("p2", 6u64.into_prop()), ("p3", 1u64.into_prop())],
            Some("fire_nation"),
        ),
        (
            3,
            2,
            1,
            vec![
                ("p2", 6u64.into_prop()),
                ("p3", 1u64.into_prop()),
                ("p10", "Paper_airplane".into_prop()),
            ],
            None,
        ),
    ];

    for (time, src, dst, props, edge_type) in edges {
        graph.add_edge(time, src, dst, props, edge_type).unwrap();
    }

    graph
}

fn init_edges_graph_with_str_ids<
    G: StaticGraphViewOps
        + AdditionOps
        + InternalAdditionOps
        + InternalPropertyAdditionOps
        + PropertyAdditionOps,
>(
    graph: G,
) -> G {
    let edges = [
        (1, "London", "Paris", Some("fire_nation")),
        (2, "London", "Paris", Some("fire_nation")),
        (2, "Two", "Three", Some("air_nomads")),
        (3, "Two", "Three", Some("air_nomads")),
        (3, "Three", "One", Some("fire_nation")),
        (3, "Two", "One", None),
        (4, "David Gilmour", "John Mayer", None),
        (4, "John Mayer", "Jimmy Page", None),
    ];

    for (time, src, dst, edge_type) in edges {
        graph.add_edge(time, src, dst, NO_PROPS, edge_type).unwrap();
    }

    graph
}

fn init_edges_graph_with_str_ids_del<
    G: StaticGraphViewOps
        + AdditionOps
        + DeletionOps
        + InternalAdditionOps
        + InternalPropertyAdditionOps
        + PropertyAdditionOps,
>(
    graph: G,
) -> G {
    let edges = [
        (1, "London", "Paris", Some("fire_nation")),
        (2, "London", "Paris", Some("fire_nation")),
        (2, "Two", "Three", Some("air_nomads")),
        (3, "Two", "Three", Some("air_nomads")),
        (3, "Three", "One", Some("fire_nation")),
        (3, "Two", "One", None),
        (4, "David Gilmour", "John Mayer", None),
        (4, "John Mayer", "Jimmy Page", None),
    ];

    for (time, src, dst, edge_type) in edges {
        graph.add_edge(time, src, dst, NO_PROPS, edge_type).unwrap();
    }

    graph
        .delete_edge(3, "London", "Paris", Some("fire_nation"))
        .unwrap();

    graph
        .add_edge(5, "Bangalore", "Bangalore", NO_PROPS, None)
        .unwrap();

    graph
}

mod test_node_filter {
    use crate::filter_tests::test_filters::{
        init_nodes_graph, init_nodes_graph_with_num_ids, init_nodes_graph_with_str_ids,
        IdentityGraphTransformer,
    };
    use proptest::proptest;
    use raphtory::{
        algorithms::alternating_mask::alternating_mask,
        core::entities::VID,
        db::{
            api::view::{filter_ops::Select, Filter},
            graph::views::filter::{
                model::{
                    degree_filter::DegreeFilterFactory,
                    node_filter::ops::{NodeFilterOps, NodeIdFilterOps},
                    property_filter::ops::{ElemQualifierOps, ListAggOps, PropertyFilterOps},
                    ComposableFilter, CompositeNodeFilter, NodeViewFilterOps, TryAsCompositeFilter,
                    ViewWrapOps,
                },
                CreateFilter,
            },
        },
        errors::GraphError,
        prelude::{
            AdditionOps, Graph, GraphViewOps, IntoProp, NodeFilter, NodeStateOps, NodeViewOps,
            TimeOps, NO_PROPS,
        },
    };
    use raphtory_api::core::{entities::properties::prop::Prop, Direction};
    use raphtory_tests::assertions::{
        assert_filter_nodes_results, assert_select_nodes_results, TestVariants,
    };

    fn sort_vids(mut vids: Vec<VID>) -> Vec<VID> {
        vids.sort();
        vids
    }

    fn candidates_with_history_after_filtering<'a, G: GraphViewOps<'a>>(
        graph: &G,
        candidate_nodes: Vec<VID>,
    ) -> Vec<VID> {
        let subgraph = graph.subgraph(candidate_nodes);
        sort_vids(
            subgraph
                .nodes()
                .into_iter()
                .filter(|n| !n.history().is_empty())
                .map(|n| n.node)
                .collect(),
        )
    }

    fn assert_filter<CF, F>(
        graph: &Graph,
        filter: CF,
        metric: Direction,
        manual_expr: F,
        context: &str,
    ) where
        CF: CreateFilter + TryAsCompositeFilter + Clone,
        F: Fn(usize) -> bool + Copy,
    {
        let expected_select_nodes = graph
            .nodes()
            .into_iter()
            .filter(|n| {
                manual_expr(match metric {
                    Direction::BOTH => n.degree(),
                    Direction::IN => n.in_degree(),
                    Direction::OUT => n.out_degree(),
                })
            })
            .map(|n| n.node)
            .collect::<Vec<_>>();

        let expected_filter_nodes =
            candidates_with_history_after_filtering(graph, expected_select_nodes.clone());

        let filtered_event_graph = graph.filter(filter.clone()).unwrap();
        let filtered_event_nodes = sort_vids(
            filtered_event_graph
                .nodes()
                .into_iter()
                .map(|n| n.node)
                .collect(),
        );
        assert_eq!(
            filtered_event_nodes, expected_filter_nodes,
            "{} failed for event graph",
            context
        );

        let selected_event_nodes = sort_vids(
            graph
                .nodes()
                .select(filter.clone())
                .unwrap()
                .into_iter()
                .map(|n| n.node)
                .collect(),
        );
        assert_eq!(
            selected_event_nodes, expected_select_nodes,
            "{} failed for event graph select",
            context
        );

        let filtered_persistent_graph = graph.persistent_graph().filter(filter.clone()).unwrap();
        let filtered_persistent_nodes = sort_vids(
            filtered_persistent_graph
                .nodes()
                .into_iter()
                .map(|n| n.node)
                .collect(),
        );
        assert_eq!(
            filtered_persistent_nodes, expected_filter_nodes,
            "{} failed for persistent graph",
            context
        );

        let selected_persistent_nodes = sort_vids(
            graph
                .persistent_graph()
                .nodes()
                .select(filter)
                .unwrap()
                .into_iter()
                .map(|n| n.node)
                .collect(),
        );
        assert_eq!(
            selected_persistent_nodes, expected_select_nodes,
            "{} failed for persistent graph select",
            context
        );
    }

    fn degree_graph_with_add_node_and_add_edge() -> Graph {
        let graph = degree_graph_with_add_edge_only();
        let add_nodes = [
            (0, "1", Some("layer_a")),
            (0, "7", None),
            (0, "8", None),
            (3, "9", Some("layer_a")),
            (4, "9", Some("layer_c")),
            (5, "10", Some("layer_b")),
            (6, "10", Some("layer_e")),
            (7, "11", Some("layer_d")),
            (8, "12", Some("layer_f")),
            (9, "12", Some("layer_c")),
        ];
        for (t, id, layer) in add_nodes {
            graph.add_node(t, id, NO_PROPS, None, layer).unwrap();
        }
        graph
    }

    fn degree_graph_with_add_edge_only() -> Graph {
        let graph = Graph::new();

        let edges = [
            (1, "1", "2", "layer_a"),
            (1, "1", "3", "layer_b"),
            (1, "1", "4", "layer_a"),
            (1, "1", "5", "layer_b"),
            (1, "1", "6", "layer_a"),
            (2, "2", "1", "layer_b"),
            (2, "2", "3", "layer_a"),
            (2, "2", "4", "layer_b"),
            (2, "2", "5", "layer_a"),
            (3, "3", "1", "layer_a"),
            (3, "3", "4", "layer_b"),
            (3, "3", "5", "layer_a"),
            (4, "4", "1", "layer_b"),
            (4, "4", "2", "layer_a"),
            (5, "5", "1", "layer_b"),
            (6, "6", "1", "layer_a"),
            (6, "4", "3", "layer_b"),
            (6, "5", "2", "layer_a"),
            (6, "6", "2", "layer_b"),
            (6, "5", "3", "layer_a"),
            (7, "2", "6", "layer_c"),
            (7, "3", "6", "layer_d"),
            (7, "6", "4", "layer_e"),
            (7, "1", "5", "layer_f"),
            (8, "3", "2", "layer_c"),
            (8, "4", "6", "layer_d"),
            (8, "2", "5", "layer_e"),
            (8, "6", "3", "layer_f"),
            (9, "5", "4", "layer_c"),
            (9, "4", "5", "layer_d"),
            (9, "2", "4", "layer_e"),
            (9, "3", "1", "layer_f"),
        ];
        for (t, src, dst, layer) in edges {
            graph.add_edge(t, src, dst, NO_PROPS, Some(layer)).unwrap();
        }

        graph
    }

    // Property-based tests for degree filtering
    proptest! {
        #[test]
        fn prop_degree_filter_both_direction_comparison(threshold in 0u64..15) {
            let graph = degree_graph_with_add_node_and_add_edge();

            assert_filter(
                &graph,
                NodeFilter.degree().lt(threshold),
                Direction::BOTH,
                |d| d < threshold as usize,
                &format!("BOTH < {}", threshold),
            );

            assert_filter(
                &graph,
                NodeFilter.degree().le(threshold),
                Direction::BOTH,
                |d| d <= threshold as usize,
                &format!("BOTH <= {}", threshold),
            );

            assert_filter(
                &graph,
                NodeFilter.degree().eq(threshold),
                Direction::BOTH,
                |d| d == threshold as usize,
                &format!("BOTH == {}", threshold),
            );

            assert_filter(
                &graph,
                NodeFilter.degree().ne(threshold),
                Direction::BOTH,
                |d| d != threshold as usize,
                &format!("BOTH != {}", threshold),
            );

            assert_filter(
                &graph,
                NodeFilter.degree().ge(threshold),
                Direction::BOTH,
                |d| d >= threshold as usize,
                &format!("BOTH >= {}", threshold),
            );

            assert_filter(
                &graph,
                NodeFilter.degree().gt(threshold),
                Direction::BOTH,
                |d| d > threshold as usize,
                &format!("BOTH > {}", threshold),
            );
        }

        #[test]
        fn prop_degree_filter_in_direction_comparison(threshold in 0u64..15) {
            let graph = degree_graph_with_add_node_and_add_edge();

            assert_filter(
                &graph,
                NodeFilter.in_degree().lt(threshold),
                Direction::IN,
                |d| d < threshold as usize,
                &format!("IN < {}", threshold),
            );

            assert_filter(
                &graph,
                NodeFilter.in_degree().le(threshold),
                Direction::IN,
                |d| d <= threshold as usize,
                &format!("IN <= {}", threshold),
            );

            assert_filter(
                &graph,
                NodeFilter.in_degree().eq(threshold),
                Direction::IN,
                |d| d == threshold as usize,
                &format!("IN == {}", threshold),
            );

            assert_filter(
                &graph,
                NodeFilter.in_degree().ne(threshold),
                Direction::IN,
                |d| d != threshold as usize,
                &format!("IN != {}", threshold),
            );

            assert_filter(
                &graph,
                NodeFilter.in_degree().ge(threshold),
                Direction::IN,
                |d| d >= threshold as usize,
                &format!("IN >= {}", threshold),
            );

            assert_filter(
                &graph,
                NodeFilter.in_degree().gt(threshold),
                Direction::IN,
                |d| d > threshold as usize,
                &format!("IN > {}", threshold),
            );
        }

        #[test]
        fn prop_degree_filter_out_direction_comparison(threshold in 0u64..15) {
            let graph = degree_graph_with_add_node_and_add_edge();

            assert_filter(
                &graph,
                NodeFilter.out_degree().lt(threshold),
                Direction::OUT,
                |d| d < threshold as usize,
                &format!("OUT < {}", threshold),
            );

            assert_filter(
                &graph,
                NodeFilter.out_degree().le(threshold),
                Direction::OUT,
                |d| d <= threshold as usize,
                &format!("OUT <= {}", threshold),
            );

            assert_filter(
                &graph,
                NodeFilter.out_degree().eq(threshold),
                Direction::OUT,
                |d| d == threshold as usize,
                &format!("OUT == {}", threshold),
            );

            assert_filter(
                &graph,
                NodeFilter.out_degree().ne(threshold),
                Direction::OUT,
                |d| d != threshold as usize,
                &format!("OUT != {}", threshold),
            );

            assert_filter(
                &graph,
                NodeFilter.out_degree().ge(threshold),
                Direction::OUT,
                |d| d >= threshold as usize,
                &format!("OUT >= {}", threshold),
            );

            assert_filter(
                &graph,
                NodeFilter.out_degree().gt(threshold),
                Direction::OUT,
                |d| d > threshold as usize,
                &format!("OUT > {}", threshold),
            );
        }

        #[test]
        fn prop_degree_filter_and(threshold in 0u64..15) {
            let graph = degree_graph_with_add_node_and_add_edge();

            assert_filter(
                &graph,
                NodeFilter.degree().gt(threshold).and(NodeFilter.degree().lt(threshold + 5)),
                Direction::BOTH,
                |d| d > threshold as usize && d < (threshold + 5) as usize,
                &format!("BOTH > {} AND BOTH < {}", threshold, threshold + 5),
            );

            assert_filter(
                &graph,
                NodeFilter.in_degree().gt(threshold).and(NodeFilter.in_degree().lt(threshold + 5)),
                Direction::IN,
                |d| d > threshold as usize && d < (threshold + 5) as usize,
                &format!("IN > {} AND IN < {}", threshold, threshold + 5),
            );

            assert_filter(
                &graph,
                NodeFilter.out_degree().gt(threshold).and(NodeFilter.out_degree().lt(threshold + 5)),
                Direction::OUT,
                |d| d > threshold as usize && d < (threshold + 5) as usize,
                &format!("OUT > {} AND OUT < {}", threshold, threshold + 5),
            );
        }

        #[test]
        fn prop_degree_filter_or(threshold in 0u64..15) {
            let graph = degree_graph_with_add_node_and_add_edge();

            assert_filter(
                &graph,
                NodeFilter.degree().lt(threshold).or(NodeFilter.degree().gt(threshold + 5)),
                Direction::BOTH,
                |d| d < threshold as usize || d > (threshold + 5) as usize,
                &format!("BOTH < {} OR BOTH > {}", threshold, threshold + 5),
            );

            assert_filter(
                &graph,
                NodeFilter.in_degree().lt(threshold).or(NodeFilter.in_degree().gt(threshold + 5)),
                Direction::IN,
                |d| d < threshold as usize || d > (threshold + 5) as usize,
                &format!("IN < {} OR IN > {}", threshold, threshold + 5),
            );

            assert_filter(
                &graph,
                NodeFilter.out_degree().lt(threshold).or(NodeFilter.out_degree().gt(threshold + 5)),
                Direction::OUT,
                |d| d < threshold as usize || d > (threshold + 5) as usize,
                &format!("OUT < {} OR OUT > {}", threshold, threshold + 5),
            );
        }

        #[test]
        fn prop_degree_filter_not(threshold in 0u64..15) {
            let graph = degree_graph_with_add_node_and_add_edge();

            assert_filter(
                &graph,
                NodeFilter.degree().lt(threshold).or(NodeFilter.degree().gt(threshold + 5).not()),
                Direction::BOTH,
                |d| d < threshold as usize || d <= (threshold + 5) as usize,
                &format!("BOTH < {} OR BOTH > {}", threshold, threshold + 5),
            );

            assert_filter(
                &graph,
                NodeFilter.in_degree().lt(threshold).or(NodeFilter.in_degree().gt(threshold + 5).not()),
                Direction::IN,
                |d| d < threshold as usize || d <= (threshold + 5) as usize,
                &format!("IN < {} OR IN > {}", threshold, threshold + 5),
            );

            assert_filter(
                &graph,
                NodeFilter.out_degree().lt(threshold).or(NodeFilter.out_degree().gt(threshold + 5).not()),
                Direction::OUT,
                |d| d < threshold as usize || d <= (threshold + 5) as usize,
                &format!("OUT < {} OR OUT > {}", threshold, threshold + 5),
            );
        }

        #[test]
        fn prop_degree_filter_is_in(val1 in 0u64..15, val2 in 0u64..15) {
            let graph = degree_graph_with_add_node_and_add_edge();
            let set = [val1, val2];

            assert_filter(
                &graph,
                NodeFilter.degree().is_in(vec![Prop::U64(val1), Prop::U64(val2)]),
                Direction::BOTH,
                |d| set.contains(&(d as u64)),
                &format!("BOTH is_in({}, {})", val1, val2),
            );

            assert_filter(
                &graph,
                NodeFilter.in_degree().is_in(vec![Prop::U64(val1), Prop::U64(val2)]),
                Direction::IN,
                |d| set.contains(&(d as u64)),
                &format!("IN is_in({}, {})", val1, val2),
            );

            assert_filter(
                &graph,
                NodeFilter.out_degree().is_in(vec![Prop::U64(val1), Prop::U64(val2)]),
                Direction::OUT,
                |d| set.contains(&(d as u64)),
                &format!("OUT is_in({}, {})", val1, val2),
            );
        }

        #[test]
        fn prop_degree_filter_is_not_in(val1 in 0u64..15, val2 in 0u64..15) {
            let graph = degree_graph_with_add_node_and_add_edge();
            let set = [val1, val2];

            assert_filter(
                &graph,
                NodeFilter
                    .degree()
                    .is_not_in(vec![Prop::U64(val1), Prop::U64(val2)]),
                Direction::BOTH,
                |d| !set.contains(&(d as u64)),
                &format!("BOTH is_not_in({}, {})", val1, val2),
            );

            assert_filter(
                &graph,
                NodeFilter
                    .in_degree()
                    .is_not_in(vec![Prop::U64(val1), Prop::U64(val2)]),
                Direction::IN,
                |d| !set.contains(&(d as u64)),
                &format!("IN is_not_in({}, {})", val1, val2),
            );

            assert_filter(
                &graph,
                NodeFilter
                    .out_degree()
                    .is_not_in(vec![Prop::U64(val1), Prop::U64(val2)]),
                Direction::OUT,
                |d| !set.contains(&(d as u64)),
                &format!("OUT is_not_in({}, {})", val1, val2),
            );
        }
    }

    #[test]
    fn test_degree_filter_with_invalid_expressions() {
        let graph = degree_graph_with_add_node_and_add_edge();
        let invalid_filters = vec![
            NodeFilter.degree().is_none(),
            NodeFilter.degree().is_some(),
            NodeFilter.degree().starts_with("1"),
            NodeFilter.degree().ends_with("1"),
            NodeFilter.degree().contains("1"),
            NodeFilter.degree().not_contains("1"),
            NodeFilter.degree().fuzzy_search("1", 1, false),
            NodeFilter.in_degree().is_none(),
            NodeFilter.in_degree().is_some(),
            NodeFilter.in_degree().starts_with("1"),
            NodeFilter.in_degree().ends_with("1"),
            NodeFilter.in_degree().contains("1"),
            NodeFilter.in_degree().not_contains("1"),
            NodeFilter.in_degree().fuzzy_search("1", 1, false),
            NodeFilter.out_degree().is_none(),
            NodeFilter.out_degree().is_some(),
            NodeFilter.out_degree().starts_with("1"),
            NodeFilter.out_degree().ends_with("1"),
            NodeFilter.out_degree().contains("1"),
            NodeFilter.out_degree().not_contains("1"),
            NodeFilter.out_degree().fuzzy_search("1", 1, false),
            NodeFilter.degree().any().eq(1u64),
            NodeFilter.degree().all().eq(1u64),
            NodeFilter.degree().len().gt(0u64),
            NodeFilter.degree().sum().eq(1u64),
            NodeFilter.degree().avg().eq(1u64),
            NodeFilter.degree().min().eq(1u64),
            NodeFilter.degree().max().eq(1u64),
            NodeFilter.degree().first().eq(1u64),
            NodeFilter.degree().last().eq(1u64),
            NodeFilter.in_degree().any().eq(1u64),
            NodeFilter.in_degree().all().eq(1u64),
            NodeFilter.in_degree().len().gt(0u64),
            NodeFilter.in_degree().sum().eq(1u64),
            NodeFilter.in_degree().avg().eq(1u64),
            NodeFilter.in_degree().min().eq(1u64),
            NodeFilter.in_degree().max().eq(1u64),
            NodeFilter.in_degree().first().eq(1u64),
            NodeFilter.in_degree().last().eq(1u64),
            NodeFilter.out_degree().any().eq(1u64),
            NodeFilter.out_degree().all().eq(1u64),
            NodeFilter.out_degree().len().gt(0u64),
            NodeFilter.out_degree().sum().eq(1u64),
            NodeFilter.out_degree().avg().eq(1u64),
            NodeFilter.out_degree().min().eq(1u64),
            NodeFilter.out_degree().max().eq(1u64),
            NodeFilter.out_degree().first().eq(1u64),
            NodeFilter.out_degree().last().eq(1u64),
        ];

        for filter in invalid_filters {
            assert!(
                matches!(graph.filter(filter), Err(GraphError::InvalidFilter(_))),
                "expected InvalidFilter for unsupported degree filter operation"
            );
        }
    }

    proptest! {
        #[test]
        fn prop_degree_filter_with_string_threshold(threshold in 0u64..15) {
            let graph = degree_graph_with_add_node_and_add_edge();
            let threshold_str = threshold.to_string();
            let parsed_str = threshold_str.parse::<u64>().unwrap();

            assert_filter(&graph, NodeFilter.degree().lt(threshold_str.clone()), Direction::BOTH, |d| d < parsed_str as usize, "BOTH < string threshold parsed to u64");
            assert_filter(&graph, NodeFilter.degree().le(threshold_str.clone()), Direction::BOTH, |d| d <= parsed_str as usize, "BOTH <= string threshold parsed to u64");
            assert_filter(&graph, NodeFilter.degree().eq(threshold_str.clone()), Direction::BOTH, |d| d == parsed_str as usize, "BOTH == string threshold parsed to u64");
            assert_filter(&graph, NodeFilter.degree().ne(threshold_str.clone()), Direction::BOTH, |d| d != parsed_str as usize, "BOTH != string threshold parsed to u64");
            assert_filter(&graph, NodeFilter.degree().ge(threshold_str.clone()), Direction::BOTH, |d| d >= parsed_str as usize, "BOTH >= string threshold parsed to u64");
            assert_filter(&graph, NodeFilter.degree().gt(threshold_str.clone()), Direction::BOTH, |d| d > parsed_str as usize, "BOTH > string threshold parsed to u64");

            assert_filter(&graph, NodeFilter.in_degree().lt(threshold_str.clone()), Direction::IN, |d| d < parsed_str as usize, "IN < string threshold parsed to u64");
            assert_filter(&graph, NodeFilter.in_degree().le(threshold_str.clone()), Direction::IN, |d| d <= parsed_str as usize, "IN <= string threshold parsed to u64");
            assert_filter(&graph, NodeFilter.in_degree().eq(threshold_str.clone()), Direction::IN, |d| d == parsed_str as usize, "IN == string threshold parsed to u64");
            assert_filter(&graph, NodeFilter.in_degree().ne(threshold_str.clone()), Direction::IN, |d| d != parsed_str as usize, "IN != string threshold parsed to u64");
            assert_filter(&graph, NodeFilter.in_degree().ge(threshold_str.clone()), Direction::IN, |d| d >= parsed_str as usize, "IN >= string threshold parsed to u64");
            assert_filter(&graph, NodeFilter.in_degree().gt(threshold_str.clone()), Direction::IN, |d| d > parsed_str as usize, "IN > string threshold parsed to u64");

            assert_filter(&graph, NodeFilter.out_degree().lt(threshold_str.clone()), Direction::OUT, |d| d < parsed_str as usize, "OUT < string threshold parsed to u64");
            assert_filter(&graph, NodeFilter.out_degree().le(threshold_str.clone()), Direction::OUT, |d| d <= parsed_str as usize, "OUT <= string threshold parsed to u64");
            assert_filter(&graph, NodeFilter.out_degree().eq(threshold_str.clone()), Direction::OUT, |d| d == parsed_str as usize, "OUT == string threshold parsed to u64");
            assert_filter(&graph, NodeFilter.out_degree().ne(threshold_str.clone()), Direction::OUT, |d| d != parsed_str as usize, "OUT != string threshold parsed to u64");
            assert_filter(&graph, NodeFilter.out_degree().ge(threshold_str.clone()), Direction::OUT, |d| d >= parsed_str as usize, "OUT >= string threshold parsed to u64");
            assert_filter(&graph, NodeFilter.out_degree().gt(threshold_str), Direction::OUT, |d| d > parsed_str as usize, "OUT > string threshold parsed to u64");
        }

        #[test]
        fn prop_degree_filter_with_float_threshold(threshold in 0u64..15) {
            let graph = degree_graph_with_add_node_and_add_edge();
            let threshold_float = threshold as f64;
            let parsed_float = threshold_float as u64;

            assert_filter(&graph, NodeFilter.degree().lt(threshold_float), Direction::BOTH, |d| d < parsed_float as usize, "BOTH < float threshold cast to u64");
            assert_filter(&graph, NodeFilter.degree().le(threshold_float), Direction::BOTH, |d| d <= parsed_float as usize, "BOTH <= float threshold cast to u64");
            assert_filter(&graph, NodeFilter.degree().eq(threshold_float), Direction::BOTH, |d| d == parsed_float as usize, "BOTH == float threshold cast to u64");
            assert_filter(&graph, NodeFilter.degree().ne(threshold_float), Direction::BOTH, |d| d != parsed_float as usize, "BOTH != float threshold cast to u64");
            assert_filter(&graph, NodeFilter.degree().ge(threshold_float), Direction::BOTH, |d| d >= parsed_float as usize, "BOTH >= float threshold cast to u64");
            assert_filter(&graph, NodeFilter.degree().gt(threshold_float), Direction::BOTH, |d| d > parsed_float as usize, "BOTH > float threshold cast to u64");

            assert_filter(&graph, NodeFilter.in_degree().lt(threshold_float), Direction::IN, |d| d < parsed_float as usize, "IN < float threshold cast to u64");
            assert_filter(&graph, NodeFilter.in_degree().le(threshold_float), Direction::IN, |d| d <= parsed_float as usize, "IN <= float threshold cast to u64");
            assert_filter(&graph, NodeFilter.in_degree().eq(threshold_float), Direction::IN, |d| d == parsed_float as usize, "IN == float threshold cast to u64");
            assert_filter(&graph, NodeFilter.in_degree().ne(threshold_float), Direction::IN, |d| d != parsed_float as usize, "IN != float threshold cast to u64");
            assert_filter(&graph, NodeFilter.in_degree().ge(threshold_float), Direction::IN, |d| d >= parsed_float as usize, "IN >= float threshold cast to u64");
            assert_filter(&graph, NodeFilter.in_degree().gt(threshold_float), Direction::IN, |d| d > parsed_float as usize, "IN > float threshold cast to u64");

            assert_filter(&graph, NodeFilter.out_degree().lt(threshold_float), Direction::OUT, |d| d < parsed_float as usize, "OUT < float threshold cast to u64");
            assert_filter(&graph, NodeFilter.out_degree().le(threshold_float), Direction::OUT, |d| d <= parsed_float as usize, "OUT <= float threshold cast to u64");
            assert_filter(&graph, NodeFilter.out_degree().eq(threshold_float), Direction::OUT, |d| d == parsed_float as usize, "OUT == float threshold cast to u64");
            assert_filter(&graph, NodeFilter.out_degree().ne(threshold_float), Direction::OUT, |d| d != parsed_float as usize, "OUT != float threshold cast to u64");
            assert_filter(&graph, NodeFilter.out_degree().ge(threshold_float), Direction::OUT, |d| d >= parsed_float as usize, "OUT >= float threshold cast to u64");
            assert_filter(&graph, NodeFilter.out_degree().gt(threshold_float), Direction::OUT, |d| d > parsed_float as usize, "OUT > float threshold cast to u64");
        }

        #[test]
        fn prop_degree_filter_with_string_is_in(threshold_a in 0u64..15, threshold_b in 0u64..15) {
            let graph = degree_graph_with_add_node_and_add_edge();
            let threshold_a_str = threshold_a.to_string();
            let threshold_b_str = threshold_b.to_string();
            let parsed_a = threshold_a_str.parse::<u64>().unwrap();
            let parsed_b = threshold_b_str.parse::<u64>().unwrap();
            let set = [parsed_a, parsed_b];

            assert_filter(&graph, NodeFilter.degree().is_in(vec![threshold_a_str.clone().into_prop(), threshold_b_str.clone().into_prop()]), Direction::BOTH, |d| set.contains(&(d as u64)), "BOTH is_in(string thresholds parsed to u64)");
            assert_filter(&graph, NodeFilter.in_degree().is_in(vec![threshold_a_str.clone().into_prop(), threshold_b_str.clone().into_prop()]), Direction::IN, |d| set.contains(&(d as u64)), "IN is_in(string thresholds parsed to u64)");
            assert_filter(&graph, NodeFilter.out_degree().is_in(vec![threshold_a_str.into_prop(), threshold_b_str.into_prop()]), Direction::OUT, |d| set.contains(&(d as u64)), "OUT is_in(string thresholds parsed to u64)");
        }

        #[test]
        fn prop_degree_filter_with_string_is_not_in(threshold_a in 0u64..15, threshold_b in 0u64..15) {
            let graph = degree_graph_with_add_node_and_add_edge();
            let threshold_a_str = threshold_a.to_string();
            let threshold_b_str = threshold_b.to_string();
            let parsed_a = threshold_a_str.parse::<u64>().unwrap();
            let parsed_b = threshold_b_str.parse::<u64>().unwrap();
            let set = [parsed_a, parsed_b];

            assert_filter(&graph, NodeFilter.degree().is_not_in(vec![threshold_a_str.clone().into_prop(), threshold_b_str.clone().into_prop()]), Direction::BOTH, |d| !set.contains(&(d as u64)), "BOTH is_not_in(string thresholds parsed to u64)");
            assert_filter(&graph, NodeFilter.in_degree().is_not_in(vec![threshold_a_str.clone().into_prop(), threshold_b_str.clone().into_prop()]), Direction::IN, |d| !set.contains(&(d as u64)), "IN is_not_in(string thresholds parsed to u64)");
            assert_filter(&graph, NodeFilter.out_degree().is_not_in(vec![threshold_a_str.into_prop(), threshold_b_str.into_prop()]), Direction::OUT, |d| !set.contains(&(d as u64)), "OUT is_not_in(string thresholds parsed to u64)");
        }

        #[test]
        fn prop_degree_filter_with_float_is_in(threshold_a in 0u64..15, threshold_b in 0u64..15) {
            let graph = degree_graph_with_add_node_and_add_edge();
            let threshold_a_float = threshold_a as f64;
            let threshold_b_float = threshold_b as f64;
            let parsed_a = threshold_a_float as u64;
            let parsed_b = threshold_b_float as u64;
            let set = [parsed_a, parsed_b];

            assert_filter(&graph, NodeFilter.degree().is_in(vec![threshold_a_float.into_prop(), threshold_b_float.into_prop()]), Direction::BOTH, |d| set.contains(&(d as u64)), "BOTH is_in(float thresholds cast to u64)");
            assert_filter(&graph, NodeFilter.in_degree().is_in(vec![threshold_a_float.into_prop(), threshold_b_float.into_prop()]), Direction::IN, |d| set.contains(&(d as u64)), "IN is_in(float thresholds cast to u64)");
            assert_filter(&graph, NodeFilter.out_degree().is_in(vec![threshold_a_float.into_prop(), threshold_b_float.into_prop()]), Direction::OUT, |d| set.contains(&(d as u64)), "OUT is_in(float thresholds cast to u64)");
        }

        #[test]
        fn prop_degree_filter_with_float_is_not_in(threshold_a in 0u64..15, threshold_b in 0u64..15) {
            let graph = degree_graph_with_add_node_and_add_edge();
            let threshold_a_float = threshold_a as f64;
            let threshold_b_float = threshold_b as f64;
            let parsed_a = threshold_a_float as u64;
            let parsed_b = threshold_b_float as u64;
            let set = [parsed_a, parsed_b];

            assert_filter(&graph, NodeFilter.degree().is_not_in(vec![threshold_a_float.into_prop(), threshold_b_float.into_prop()]), Direction::BOTH, |d| !set.contains(&(d as u64)), "BOTH is_not_in(float thresholds cast to u64)");
            assert_filter(&graph, NodeFilter.in_degree().is_not_in(vec![threshold_a_float.into_prop(), threshold_b_float.into_prop()]), Direction::IN, |d| !set.contains(&(d as u64)), "IN is_not_in(float thresholds cast to u64)");
            assert_filter(&graph, NodeFilter.out_degree().is_not_in(vec![threshold_a_float.into_prop(), threshold_b_float.into_prop()]), Direction::OUT, |d| !set.contains(&(d as u64)), "OUT is_not_in(float thresholds cast to u64)");
        }

        #[test]
        fn prop_degree_filter_invalid_non_numeric_string_values(value_a in "[a-zA-Z]{1,8}", value_b in "[a-zA-Z]{1,8}") {
            let graph = degree_graph_with_add_node_and_add_edge();

            let invalid_filters = vec![
                NodeFilter.degree().lt(value_a.clone()),
                NodeFilter.degree().le(value_a.clone()),
                NodeFilter.degree().eq(value_a.clone()),
                NodeFilter.degree().ne(value_a.clone()),
                NodeFilter.degree().ge(value_a.clone()),
                NodeFilter.degree().gt(value_a.clone()),
                NodeFilter.in_degree().lt(value_a.clone()),
                NodeFilter.in_degree().le(value_a.clone()),
                NodeFilter.in_degree().eq(value_a.clone()),
                NodeFilter.in_degree().ne(value_a.clone()),
                NodeFilter.in_degree().ge(value_a.clone()),
                NodeFilter.in_degree().gt(value_a.clone()),
                NodeFilter.out_degree().lt(value_a.clone()),
                NodeFilter.out_degree().le(value_a.clone()),
                NodeFilter.out_degree().eq(value_a.clone()),
                NodeFilter.out_degree().ne(value_a.clone()),
                NodeFilter.out_degree().ge(value_a.clone()),
                NodeFilter.out_degree().gt(value_a.clone()),
                NodeFilter.degree().is_in(vec![value_a.clone().into_prop(), value_b.clone().into_prop()]),
                NodeFilter.degree().is_not_in(vec![value_a.clone().into_prop(), value_b.clone().into_prop()]),
                NodeFilter.in_degree().is_in(vec![value_a.clone().into_prop(), value_b.clone().into_prop()]),
                NodeFilter.in_degree().is_not_in(vec![value_a.clone().into_prop(), value_b.clone().into_prop()]),
                NodeFilter.out_degree().is_in(vec![value_a.clone().into_prop(), value_b.clone().into_prop()]),
                NodeFilter.out_degree().is_not_in(vec![value_a.clone().into_prop(), value_b.clone().into_prop()]),
            ];

            for filter in invalid_filters {
                assert!(
                    matches!(graph.filter(filter), Err(GraphError::InvalidFilter(_))),
                    "expected InvalidFilter for non-numeric string values"
                );
            }
        }
    }

    #[test]
    fn test_node_list_is_preserved() {
        let graph = init_nodes_graph(Graph::new());
        let nodes = graph
            .nodes()
            .after(5)
            .select(NodeFilter::node_type().contains("x"))
            .unwrap();
        let degrees = nodes.degree();
        let degrees_collected = degrees.compute();
        assert_eq!(degrees, degrees_collected);
    }

    #[test]
    fn test_filter_nodes_for_node_name_eq() {
        let filter = NodeFilter::name().eq("3");
        let expected_results = vec!["3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_node_name_ne() {
        let filter = NodeFilter::name().ne("2");
        let expected_results = vec!["1", "3", "4"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_node_name_in() {
        let filter = NodeFilter::name().is_in(vec!["1"]);
        let expected_results = vec!["1"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter::name().is_in(vec![""]);
        let expected_results = Vec::<&str>::new();
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter::name().is_in(vec!["2", "3"]);
        let expected_results = vec!["2", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_node_name_not_in() {
        let filter = NodeFilter::name().is_not_in(vec!["1"]);
        let expected_results = vec!["2", "3", "4"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter::name().is_not_in(vec![""]);
        let expected_results = vec!["1", "2", "3", "4"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_node_type_eq() {
        let filter = NodeFilter::node_type().eq("fire_nation");
        let expected_results = vec!["1", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_node_type_ne() {
        let filter = NodeFilter::node_type().ne("fire_nation");
        let expected_results = vec!["2", "4"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_node_type_in() {
        let filter = NodeFilter::node_type().is_in(vec!["fire_nation"]);
        let expected_results = vec!["1", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter::node_type().is_in(vec!["fire_nation", "air_nomads"]);
        let expected_results = vec!["1", "2", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_node_type_not_in() {
        let filter = NodeFilter::node_type().is_not_in(vec!["fire_nation"]);
        let expected_results = vec!["2", "4"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_node_type_starts_with() {
        let filter = NodeFilter::node_type().starts_with("fire");
        let expected_results = vec!["1", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter::node_type().starts_with("rocket");
        let expected_results = vec![];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_node_type_ends_with() {
        let filter = NodeFilter::node_type().ends_with("nomads");
        let expected_results = vec!["2"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter::node_type().ends_with("circle");
        let expected_results = vec![];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_node_type_contains() {
        let filter = NodeFilter::node_type().contains("fire");
        let expected_results = vec!["1", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_node_type_contains_not() {
        let filter = NodeFilter::node_type().not_contains("fire");
        let expected_results = vec!["2", "4"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_fuzzy_search() {
        let filter = NodeFilter::node_type().fuzzy_search("fire", 2, true);
        let expected_results: Vec<&str> = vec!["1", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter::node_type().fuzzy_search("fire", 2, false);
        let expected_results: Vec<&str> = vec![];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter::node_type().fuzzy_search("air_noma", 2, false);
        let expected_results: Vec<&str> = vec!["2"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_not_node_type() {
        let filter = NodeFilter::node_type().is_not_in(vec!["fire_nation"]).not();
        let expected_results = vec!["1", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_eq_node_id() {
        let filter = NodeFilter::id().eq("1");
        let expected_results = vec!["1"];

        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter::id().eq(1);
        let expected_results = vec!["1"];

        assert_filter_nodes_results(
            init_nodes_graph_with_num_ids,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_filter_nodes_results(
            init_nodes_graph_with_num_ids,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_ne_node_id() {
        let filter = NodeFilter::id().ne("1");
        let expected_results = vec!["2", "3", "4"];

        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter::id().ne(1);
        let expected_results = vec!["2", "3", "4"];

        assert_filter_nodes_results(
            init_nodes_graph_with_num_ids,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_is_in_node_id() {
        let filter = NodeFilter::id().is_in(vec!["1", "3", "6"]);
        let expected_results = vec!["1", "3"];

        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter::id().is_in(vec![1, 3, 6]);
        let expected_results = vec!["1", "3"];

        assert_filter_nodes_results(
            init_nodes_graph_with_num_ids,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_is_not_in_node_id() {
        let filter = NodeFilter::id().is_not_in(vec!["1", "3", "6"]);
        let expected_results = vec!["2", "4"];

        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter::id().is_not_in(vec![1, 3, 6]);
        let expected_results = vec!["2", "4"];

        assert_filter_nodes_results(
            init_nodes_graph_with_num_ids,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_lt_node_id() {
        let filter = NodeFilter::id().lt(2);
        let expected_results = vec!["1"];

        assert_filter_nodes_results(
            init_nodes_graph_with_num_ids,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_le_node_id() {
        let filter = NodeFilter::id().le(3);
        let expected_results = vec!["1", "2", "3"];

        assert_filter_nodes_results(
            init_nodes_graph_with_num_ids,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_gt_node_id() {
        let filter = NodeFilter::id().gt(2);
        let expected_results = vec!["3", "4"];

        assert_filter_nodes_results(
            init_nodes_graph_with_num_ids,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_ge_node_id() {
        let filter = NodeFilter::id().ge(2);
        let expected_results = vec!["2", "3", "4"];

        assert_filter_nodes_results(
            init_nodes_graph_with_num_ids,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_starts_with_node_id() {
        let filter = NodeFilter::id().starts_with("France");
        let expected_results = vec!["France Paris"];
        assert_filter_nodes_results(
            init_nodes_graph_with_str_ids,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_ends_with_node_id() {
        let filter = NodeFilter::id().ends_with("wo");
        let expected_results = vec!["Two"];
        assert_filter_nodes_results(
            init_nodes_graph_with_str_ids,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_contains_node_id() {
        let filter = NodeFilter::id().contains("o");
        let expected_results = vec!["London", "Tokyo", "Two"];
        assert_filter_nodes_results(
            init_nodes_graph_with_str_ids,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_not_contains_node_id() {
        let filter = NodeFilter::id().not_contains("o");
        let expected_results = vec!["France Paris"];
        assert_filter_nodes_results(
            init_nodes_graph_with_str_ids,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_is_in_node_id_str() {
        let filter = NodeFilter::id().is_in(vec!["London", "Tokyo"]);
        let expected_results = vec!["London", "Tokyo"];
        assert_filter_nodes_results(
            init_nodes_graph_with_str_ids,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_is_not_in_node_id_str() {
        let filter = NodeFilter::id().is_not_in(vec!["London", "Tokyo"]);
        let expected_results = vec!["France Paris", "Two"];
        assert_filter_nodes_results(
            init_nodes_graph_with_str_ids,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_is_active_node_window() {
        let filter = NodeFilter.window(1, 10).is_active();
        let expected_results = vec!["1", "2", "3", "4"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_is_active_node_window_not() {
        let filter = NodeFilter
            .window(1, 10)
            .is_active()
            .try_as_composite_node_filter()
            .unwrap();
        let filter = CompositeNodeFilter::Not(Box::new(filter));
        let expected_results = vec![];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_is_active_node_latest() {
        let filter = NodeFilter.latest().is_active();
        let expected_results = vec!["1", "2", "4"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_filter_by_column() {
        let graph = Graph::new();
        graph.add_node(1, 1, NO_PROPS, None, None).unwrap();
        graph.add_node(1, 2, NO_PROPS, None, None).unwrap();
        graph.add_node(1, 3, NO_PROPS, None, None).unwrap();
        graph.add_node(1, 4, NO_PROPS, None, None).unwrap();
        graph.add_node(1, 5, NO_PROPS, None, None).unwrap();

        let mask = alternating_mask(&graph);
        let expected_nodes: Vec<_> = graph
            .nodes()
            .name()
            .iter_values()
            .skip(1)
            .step_by(2)
            .collect();

        let filtered = graph
            .filter(NodeFilter::by_column(&mask, "bool_col").unwrap())
            .unwrap();

        let names = filtered
            .nodes()
            .iter()
            .map(|n| n.id().to_string())
            .collect::<Vec<_>>();

        assert_eq!(names, expected_nodes);

        let filtered = graph
            .nodes()
            .select(NodeFilter::by_column(&mask, "bool_col").unwrap())
            .unwrap();

        let names = filtered
            .iter()
            .map(|n| n.id().to_string())
            .collect::<Vec<_>>();

        assert_eq!(names, expected_nodes);
    }

    #[test]
    fn test_is_active_node_snapshot_at() {
        let filter = NodeFilter.snapshot_at(2).is_active();
        let expected_results = vec!["2"];
        assert_select_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }
}

mod test_node_property_filter {
    use crate::filter_tests::test_filters::{
        init_nodes_graph, init_nodes_layers_graph, IdentityGraphTransformer,
    };
    use raphtory::db::graph::views::filter::model::{
        graph_filter::GraphFilter,
        node_filter::NodeFilter,
        not_filter::NotFilter,
        property_filter::ops::{ElemQualifierOps, ListAggOps, PropertyFilterOps},
        ComposableFilter, PropertyFilterFactory, TemporalPropertyFilterFactory, ViewWrapOps,
    };
    use raphtory_api::core::entities::properties::prop::Prop;
    use raphtory_tests::assertions::{assert_filter_nodes_results, TestVariants};
    use std::vec;

    #[test]
    fn test_exact_match() {
        // let filter = NodeFilter.degree > 5
        let filter = NodeFilter.property("p10").eq("Paper_airplane");
        let expected_results = vec!["1", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter.property("p10").eq("");
        let expected_results = Vec::<&str>::new();
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_not_exact_match() {
        let filter = NodeFilter.property("p10").eq("Paper");
        let expected_results: Vec<&str> = vec![];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_property_eq() {
        let filter = NodeFilter.property("p2").eq(2u64);
        let expected_results = vec!["2"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter.property("p30").temporal().first().eq("Old_boat");
        let expected_results = vec!["2"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter.property("p20").temporal().all().eq("Gold_ship");
        let expected_results = vec!["1"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_property_ne() {
        let filter = NodeFilter.property("p2").ne(2u64);
        let expected_results = vec!["3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter.property("p30").temporal().first().ne("Old_boat");
        let expected_results = vec!["1", "4"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter.property("p1").temporal().all().ne("Gold_ship");
        let expected_results = vec!["1"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_property_lt() {
        let filter = NodeFilter.property("p2").lt(10u64);
        let expected_results = vec!["2", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter.property("p40").temporal().first().lt(10u64);
        let expected_results = vec!["1"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter.property("p9").temporal().all().lt(10u64);
        let expected_results = vec!["1"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_property_le() {
        let filter = NodeFilter.property("p2").le(6u64);
        let expected_results = vec!["2", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter.property("p9").temporal().first().le(10u64);
        let expected_results = vec!["1"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter.property("p2").temporal().all().le(10u64);
        let expected_results = vec!["3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_property_gt() {
        let filter = NodeFilter.property("p2").gt(2u64);
        let expected_results = vec!["3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter.property("p40").temporal().first().gt(5u64);
        let expected_results = vec!["2"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter.property("p9").temporal().all().gt(1u64);
        let expected_results = vec!["1"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_property_ge() {
        let filter = NodeFilter.property("p2").ge(2u64);
        let expected_results = vec!["2", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter.property("p40").temporal().first().ge(5u64);
        let expected_results = vec!["1", "2"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter.property("p40").temporal().all().ge(5u64);
        let expected_results = vec!["1", "2"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_property_in() {
        let filter = NodeFilter.property("p2").is_in(vec![Prop::U64(6)]);
        let expected_results = vec!["3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter
            .property("p2")
            .is_in(vec![Prop::U64(2), Prop::U64(6)]);
        let expected_results = vec!["2", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter
            .property("p40")
            .temporal()
            .first()
            .is_in(vec![Prop::U64(5)]);
        let expected_results = vec!["1"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter
            .property("p2")
            .temporal()
            .any()
            .is_in(vec![Prop::U64(2)]);
        let expected_results = vec!["2"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_property_not_in() {
        let filter = NodeFilter.property("p2").is_not_in(vec![Prop::U64(6)]);
        let expected_results = vec!["2"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter.property("p40").is_not_in(vec![Prop::U64(6)]);
        let expected_results = vec!["1", "2"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter
            .property("p2")
            .temporal()
            .all()
            .is_not_in(vec![Prop::U64(2)]);
        let expected_results = vec!["3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_property_is_some() {
        let filter = NodeFilter.property("p2").is_some();
        let expected_results = vec!["2", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter.property("p40").is_some();
        let expected_results = vec!["1", "2"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_property_is_none() {
        let filter = NodeFilter.property("p2").is_none();
        let expected_results = vec!["1", "4"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter.property("p40").is_none();
        let expected_results = vec!["3", "4"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_property_starts_with() {
        let filter = NodeFilter.property("p10").starts_with("Pa");
        let expected_results: Vec<&str> = vec!["1", "2", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter
            .property("p10")
            .temporal()
            .any()
            .starts_with("Pap");
        let expected_results: Vec<&str> = vec!["1", "2", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter
            .property("p10")
            .temporal()
            .last()
            .starts_with("Pape");
        let expected_results: Vec<&str> = vec!["1", "2", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter
            .property("p10")
            .temporal()
            .last()
            .starts_with("Yohan");
        let expected_results: Vec<&str> = vec![];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter
            .property("p30")
            .temporal()
            .first()
            .starts_with("Gold");
        let expected_results: Vec<&str> = vec!["1", "4"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter
            .property("p20")
            .temporal()
            .all()
            .starts_with("Gold");
        let expected_results: Vec<&str> = vec!["1", "2", "4"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_property_ends_with() {
        let filter = NodeFilter.property("p10").ends_with("lane");
        let expected_results: Vec<&str> = vec!["1", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter
            .property("p10")
            .temporal()
            .any()
            .ends_with("ship");
        let expected_results: Vec<&str> = vec!["2"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter
            .property("p10")
            .temporal()
            .last()
            .ends_with("ane");
        let expected_results: Vec<&str> = vec!["1", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter
            .property("p10")
            .temporal()
            .last()
            .ends_with("Jerry");
        let expected_results: Vec<&str> = vec![];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter
            .property("p20")
            .temporal()
            .first()
            .ends_with("boat");
        let expected_results: Vec<&str> = vec!["2"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter
            .property("p20")
            .temporal()
            .all()
            .ends_with("ship");
        let expected_results: Vec<&str> = vec!["1"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_property_contains() {
        let filter = NodeFilter.property("p10").contains("Paper");
        let expected_results: Vec<&str> = vec!["1", "2", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter
            .property("p10")
            .temporal()
            .any()
            .contains("Paper");
        let expected_results: Vec<&str> = vec!["1", "2", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter
            .property("p10")
            .temporal()
            .last()
            .contains("Paper");
        let expected_results: Vec<&str> = vec!["1", "2", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter
            .property("p30")
            .temporal()
            .first()
            .contains("Old");
        let expected_results: Vec<&str> = vec!["2"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter.property("p30").temporal().all().contains("Gold");
        let expected_results: Vec<&str> = vec!["1"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_property_contains_not() {
        let filter = NodeFilter.property("p10").not_contains("ship");
        let expected_results: Vec<&str> = vec!["1", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter
            .property("p10")
            .temporal()
            .any()
            .not_contains("ship");
        let expected_results: Vec<&str> = vec!["1", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter
            .property("p10")
            .temporal()
            .last()
            .not_contains("ship");
        let expected_results: Vec<&str> = vec!["1", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter
            .property("p30")
            .temporal()
            .first()
            .not_contains("Old");
        let expected_results: Vec<&str> = vec!["1", "4"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter
            .property("p30")
            .temporal()
            .all()
            .not_contains("boat");
        let expected_results: Vec<&str> = vec!["1", "4"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_not_property() {
        let filter = NotFilter(NodeFilter.property("p10").contains("Paper"));
        let expected_results: Vec<&str> = vec!["4"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter.property("p10").contains("Paper").not();
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_temporal_property_sum() {
        let filter = NodeFilter.property("p9").temporal().sum().eq(15u64);
        let expected_results: Vec<&str> = vec!["1"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_temporal_property_avg() {
        let filter = NodeFilter.property("p2").temporal().avg().le(10f64);
        let expected_results: Vec<&str> = vec!["2", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_temporal_property_min() {
        let filter = NodeFilter.property("p40").temporal().min().is_in(vec![
            Prop::U64(5),
            Prop::U64(10),
            Prop::U64(20),
        ]);
        let expected_results: Vec<&str> = vec!["1", "2"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_temporal_property_max() {
        let filter = NodeFilter.property("p3").temporal().max().is_not_in(vec![
            Prop::U64(5),
            Prop::U64(10),
            Prop::U64(20),
        ]);
        let expected_results: Vec<&str> = vec!["3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_temporal_property_len() {
        let filter = NodeFilter.property("p2").temporal().len().le(5u64);
        let expected_results: Vec<&str> = vec!["1", "2", "3", "4"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_nodes_window_filter() {
        let filter = NodeFilter
            .window(1, 3)
            .property("p2")
            .temporal()
            .sum()
            .ge(2u64);

        let expected_results = vec!["2"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        // Wider window includes node 3
        let filter = NodeFilter
            .window(1, 5)
            .property("p2")
            .temporal()
            .sum()
            .ge(2u64);

        let expected_results = vec!["2", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_nodes_window_filter_on_non_temporal_property() {
        let filter1 = NodeFilter.window(1, 2).property("p1").eq("shivam_kapoor");
        let filter2 = NodeFilter
            .window(100, 200)
            .property("p1")
            .eq("shivam_kapoor");

        let expected_results = vec!["1"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter1.clone(),
            &expected_results,
            TestVariants::All,
        );

        let expected_results = vec![];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter2.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = NodeFilter
            .window(100, 200)
            .property("p1")
            .eq("shivam_kapoor");

        let expected_results = vec!["1"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_nodes_window_filter_any_all_over_window() {
        let filter = NodeFilter
            .window(3, 5)
            .property("p20")
            .temporal()
            .any()
            .eq("Gold_boat");

        let expected_results = vec!["4"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter
            .window(3, 5)
            .property("p20")
            .temporal()
            .all()
            .eq("Gold_boat");

        let expected_results = vec![];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_nodes_window_filter_and() {
        // Filters both node 1 and 3
        let filter1 = NodeFilter
            .window(1, 4)
            .property("p10")
            .temporal()
            .any()
            .eq("Paper_airplane");

        // Filters only node 3
        let filter2 = NodeFilter
            .window(3, 6)
            .property("p2")
            .temporal()
            .sum()
            .eq(6u64);

        let filter = filter1.and(filter2);

        let expected_results = vec!["3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_nodes_at_filter() {
        // Only time=2 contributes; node 2 has p2=2 at t=2
        let filter = NodeFilter.at(2).property("p2").temporal().sum().eq(2u64);

        let expected_results = vec!["2"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        // Only time=3 contributes; node 3 has p2=6 at t=3
        let filter = NodeFilter.at(3).property("p2").temporal().sum().eq(6u64);

        let expected_results = vec!["3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_nodes_after_filter() {
        // after(2) means t >= 3
        let filter = NodeFilter.after(2).property("p2").temporal().sum().ge(6u64);

        let expected_results = vec!["3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_nodes_before_filter() {
        // before(3) means t <= 2
        let filter = NodeFilter
            .before(3)
            .property("p2")
            .temporal()
            .sum()
            .eq(2u64);

        let expected_results = vec!["2"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        // And node 3 shouldn't match, because its p2=6 lives at t=3.
        let filter = NodeFilter
            .before(3)
            .property("p2")
            .temporal()
            .sum()
            .eq(6u64);

        let expected_results = vec![];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_nodes_latest_filter() {
        // At latest time (currently t=4), only node 4 has p5=12
        let filter = NodeFilter.latest().property("p5").eq(12u64);

        let expected_results = vec!["4"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_nodes_snapshot_at_semantics_event_graph() {
        let t = 2;

        let filter_snapshot = NodeFilter.snapshot_at(t).property("p2").eq(2u64);

        let filter_before = NodeFilter.before(t + 1).property("p2").eq(2u64);

        let expected_results = vec!["2"];

        // snapshot_at
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter_snapshot.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        // before(t+1)
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter_before.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_nodes_snapshot_at_semantics_persistent_graph() {
        let t = 2;

        let filter_snapshot = NodeFilter.snapshot_at(t).property("p2").eq(2u64);

        let filter_at = NodeFilter.at(t).property("p2").eq(2u64);

        let expected_results = vec!["2"];

        // snapshot_at
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter_snapshot.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        // at(t)
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter_at.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_nodes_snapshot_latest_semantics_event_graph() {
        let filter_snapshot_latest = NodeFilter
            .snapshot_latest()
            .property("p2")
            .temporal()
            .sum()
            .ge(2u64);

        let filter_noop = NodeFilter.property("p2").temporal().sum().ge(2u64);

        // From your earlier window test, "2" and "3" are the ones with p2 values across time.
        // If your underlying dataset changes, adjust this accordingly.
        let expected_results = vec!["2", "3"];

        // snapshot_latest
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter_snapshot_latest.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        // no-op baseline
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter_noop.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_nodes_snapshot_latest_semantics_persistent_graph() {
        let filter_snapshot_latest = NodeFilter
            .snapshot_latest()
            .property("p1")
            .eq("shivam_kapoor");

        let filter_latest = NodeFilter.latest().property("p1").eq("shivam_kapoor");

        let expected_results = vec!["1"];

        // snapshot_latest
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter_snapshot_latest.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        // latest
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter_latest.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    #[ignore] // TODO: Enable this when node layer is supported
    fn test_nodes_layer_filter() {
        let filter = NodeFilter
            .layer("_default")
            .property("p2")
            .temporal()
            .sum()
            .ge(2u64);

        let expected_results = vec!["2"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    #[ignore] // TODO: Enable this when node layer is supported
    fn test_nodes_layer_then_window_ordering() {
        // In layer "fire_nation" within window [1,4), node "1" matches p1 == "shivam_kapoor".
        let filter = NodeFilter
            .layer("fire_nation")
            .window(1, 4)
            .property("p1")
            .eq("shivam_kapoor");

        let expected_results = vec!["1"];

        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    #[ignore] // TODO: Enable this when node layer is supported
    fn test_nodes_window_then_layer_ordering() {
        // Same semantics as above, but reversed chaining order.
        let filter = NodeFilter
            .window(1, 4)
            .layer("fire_nation")
            .property("p1")
            .eq("shivam_kapoor");

        let expected_results = vec!["1"];

        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_graph_filter_window() {
        let filter = GraphFilter.window(1, 2);
        let expected_results = vec!["1"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = GraphFilter.window(1, 3);
        let expected_results = vec!["1", "2"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = GraphFilter.window(4, 6);
        let expected_results = vec!["1", "2", "4"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = GraphFilter.window(4, 6);
        let expected_results = vec!["1", "2", "3", "4"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_graph_filter_layer() {
        // Note: Default layer is currently always included for nodes!
        let filter = GraphFilter.layer("fire_nation");
        let expected_results = vec!["1", "3", "4"];
        assert_filter_nodes_results(
            init_nodes_layers_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = GraphFilter.layer("air_nomads");
        let expected_results = vec!["2", "4"];
        assert_filter_nodes_results(
            init_nodes_layers_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_graph_filter_window_then_layer() {
        let filter = GraphFilter.window(1, 3).layer("fire_nation");
        let expected_results = vec!["1"];
        assert_filter_nodes_results(
            init_nodes_layers_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = GraphFilter.window(2, 3).layer("air_nomads");
        let expected_results = vec!["2"];
        assert_filter_nodes_results(
            init_nodes_layers_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_graph_filter_layer_then_window() {
        let filter = GraphFilter.layer("fire_nation").window(1, 3);
        let expected_results = vec!["1"];
        assert_filter_nodes_results(
            init_nodes_layers_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = GraphFilter.layer("air_nomads").window(2, 3);
        let expected_results = vec!["2"];
        assert_filter_nodes_results(
            init_nodes_layers_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_graph_filter_at() {
        let filter = GraphFilter.at(1);
        let expected_results = vec!["1"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = GraphFilter.at(2);
        let expected_results = vec!["2"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = GraphFilter.at(2);
        let expected_results = vec!["1", "2"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = GraphFilter.at(3);
        let expected_results = vec!["1", "2", "3", "4"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_graph_filter_after() {
        let filter = GraphFilter.after(3);
        let expected_results = vec!["1", "2", "4"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = GraphFilter.after(3);
        let expected_results = vec!["1", "2", "3", "4"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_graph_filter_before() {
        let filter = GraphFilter.before(2);
        let expected_results = vec!["1"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = GraphFilter.before(3);
        let expected_results = vec!["1", "2"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_graph_filter_snapshot_at() {
        let filter = GraphFilter.snapshot_at(1);
        let expected_results = vec!["1"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = GraphFilter.snapshot_at(3);
        let expected_results = vec!["1", "2", "3", "4"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = GraphFilter.snapshot_at(4);
        let expected_results = vec!["1", "2", "3", "4"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_graph_filter_snapshot_latest() {
        let filter = GraphFilter.snapshot_latest();
        let expected_results = vec!["1", "2", "3", "4"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_graph_filter_latest() {
        let filter = GraphFilter.latest();
        let expected_results = vec!["1", "2", "4"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = GraphFilter.latest();
        let expected_results = vec!["1", "2", "3", "4"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }
}

mod test_node_composite_filter {
    use crate::filter_tests::test_filters::{
        init_edges_graph, init_nodes_graph, IdentityGraphTransformer,
    };
    use raphtory::{
        db::graph::views::filter::model::{
            node_filter::ops::NodeFilterOps, property_filter::ops::PropertyFilterOps,
            ComposableFilter, PropertyFilterFactory, TryAsCompositeFilter,
        },
        prelude::NodeFilter,
    };
    use raphtory_api::core::Direction;
    use raphtory_tests::assertions::{
        assert_filter_neighbours_results, assert_filter_nodes_results, TestVariants,
    };

    #[test]
    fn test_filter_nodes_by_props_added_at_different_times() {
        let filter = NodeFilter
            .property("p4")
            .eq("pometry")
            .and(NodeFilter.property("p5").eq(12u64));
        let expected_results = vec!["4"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_unique_results_from_composite_filters() {
        let filter = NodeFilter
            .property("p2")
            .ge(2u64)
            .and(NodeFilter.property("p2").ge(1u64));
        let expected_results = vec!["2", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter
            .property("p2")
            .ge(2u64)
            .or(NodeFilter.property("p2").ge(5u64));
        let expected_results = vec!["2", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_composite_filter_nodes() {
        let filter = NodeFilter
            .property("p2")
            .eq(2u64)
            .and(NodeFilter.property("p1").eq("kapoor"));
        let expected_results = Vec::<&str>::new();
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        let filter = filter.try_as_composite_node_filter().unwrap();
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter
            .property("p2")
            .eq(2u64)
            .or(NodeFilter.property("p1").eq("shivam_kapoor"));
        let expected_results = vec!["1", "2"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = filter.try_as_composite_node_filter().unwrap();
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter.property("p1").eq("pometry").or(NodeFilter
            .property("p2")
            .eq(6u64)
            .and(NodeFilter.property("p3").eq(1u64)));
        let expected_results = vec!["3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        let filter = filter.try_as_composite_node_filter().unwrap();
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter::node_type()
            .eq("fire_nation")
            .and(NodeFilter.property("p1").eq("prop1"));
        let expected_results = Vec::<&str>::new();
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = filter.try_as_composite_node_filter().unwrap();
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter
            .property("p9")
            .eq(5u64)
            .and(NodeFilter.property("p1").eq("shivam_kapoor"));
        let expected_results = vec!["1"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = filter.try_as_composite_node_filter().unwrap();
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter::node_type()
            .eq("fire_nation")
            .and(NodeFilter.property("p1").eq("shivam_kapoor"));
        let expected_results = vec!["1"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = filter.try_as_composite_node_filter().unwrap();
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter::name()
            .eq("2")
            .and(NodeFilter.property("p2").eq(2u64));
        let expected_results = vec!["2"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = filter.try_as_composite_node_filter().unwrap();
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter::name()
            .eq("2")
            .and(NodeFilter.property("p2").eq(2u64))
            .or(NodeFilter.property("p9").eq(5u64));
        let expected_results = vec!["1", "2"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = filter.try_as_composite_node_filter().unwrap();
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_not_composite_filter_nodes() {
        let filter = NodeFilter::name()
            .eq("2")
            .and(NodeFilter.property("p2").eq(2u64))
            .or(NodeFilter.property("p9").eq(5u64))
            .not();
        let expected_results = vec!["3", "4"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter::name()
            .eq("2")
            .not()
            .and(NodeFilter.property("p2").eq(2u64))
            .or(NodeFilter.property("p9").eq(5u64));
        let expected_results = vec!["1"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_out_neighbours_filter() {
        let filter = NodeFilter::name()
            .eq("2")
            .and(NodeFilter.property("p2").eq(2u64));
        let expected_results = vec!["2"];
        assert_filter_neighbours_results(
            |graph| init_edges_graph(init_nodes_graph(graph)),
            IdentityGraphTransformer,
            "1",
            Direction::OUT,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_in_neighbours_filter() {
        let filter = NodeFilter.property("p9").ge(1u64);
        let expected_results = vec!["1"];
        assert_filter_neighbours_results(
            |graph| init_edges_graph(init_nodes_graph(graph)),
            IdentityGraphTransformer,
            "2",
            Direction::IN,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_neighbours_filter() {
        let filter = NodeFilter.property("p10").contains("Paper");
        let expected_results = vec!["1", "3"];
        assert_filter_neighbours_results(
            |graph| init_edges_graph(init_nodes_graph(graph)),
            IdentityGraphTransformer,
            "2",
            Direction::BOTH,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }
}

mod test_node_property_filter_agg {
    use crate::filter_tests::test_filters::IdentityGraphTransformer;
    use raphtory::{
        db::{
            api::view::StaticGraphViewOps,
            graph::views::filter::{
                model::{
                    node_filter::NodeFilter,
                    property_filter::ops::{ElemQualifierOps, ListAggOps, PropertyFilterOps},
                    PropertyFilterFactory, TemporalPropertyFilterFactory, TryAsCompositeFilter,
                },
                CreateFilter,
            },
        },
        prelude::{AdditionOps, GraphViewOps, PropertyAdditionOps},
    };
    use raphtory_api::core::{
        entities::properties::prop::{IntoProp, Prop},
        storage::arc_str::ArcStr,
    };
    use raphtory_storage::mutation::{
        addition_ops::InternalAdditionOps, property_addition_ops::InternalPropertyAdditionOps,
    };
    use raphtory_tests::assertions::{
        assert_filter_nodes_err, assert_filter_nodes_results, TestVariants::All,
    };

    fn list_u8(xs: &[u8]) -> Prop {
        Prop::list(xs.iter().copied().map(Prop::U8))
    }
    fn list_u16(xs: &[u16]) -> Prop {
        Prop::list(xs.iter().copied().map(Prop::U16))
    }
    fn list_u32(xs: &[u32]) -> Prop {
        Prop::list(xs.iter().copied().map(Prop::U32))
    }
    fn list_u64(xs: &[u64]) -> Prop {
        Prop::list(xs.iter().copied().map(Prop::U64))
    }
    fn list_i32(xs: &[i32]) -> Prop {
        Prop::list(xs.iter().copied().map(Prop::I32))
    }
    fn list_i64(xs: &[i64]) -> Prop {
        Prop::list(xs.iter().copied().map(Prop::I64))
    }
    fn list_f32(xs: &[f32]) -> Prop {
        Prop::list(xs.iter().copied().map(Prop::F32))
    }
    fn list_f64(xs: &[f64]) -> Prop {
        Prop::list(xs.iter().copied().map(Prop::F64))
    }
    fn list_str(xs: &[&str]) -> Prop {
        Prop::list(xs.iter().map(|s| Prop::Str(ArcStr::from(*s))))
    }
    fn list_bool(xs: &[bool]) -> Prop {
        Prop::list(xs.iter().copied().map(Prop::Bool))
    }

    #[inline]
    fn list(v: Vec<Prop>) -> Prop {
        Prop::List(v.into())
    }

    /// Writes a set of node temporal properties and node metadata to the given graph.
    pub fn init_nodes_graph<
        G: StaticGraphViewOps
            + AdditionOps
            + InternalAdditionOps
            + InternalPropertyAdditionOps
            + PropertyAdditionOps,
    >(
        graph: G,
    ) -> G {
        // Each tuple represents (timestamp, node_name, properties).
        let nodes: [(i64, &str, Vec<(&str, Prop)>); 12] = [
            (
                1,
                "n1",
                vec![
                    ("p_strs", list_str(&["a", "b", "c"])), // min: None, max: None, sum: None, avg: None, len: 3
                    ("p_bools", list_bool(&[true, false])), // min: None, max: None, sum: None, avg: None, len: 2
                    ("p_u8s", list_u8(&[1, 2, 3])), // min: 1,  max: 3,  sum: 6,    avg: 2.0,  len: 3
                    ("p_u8s_max", list_u8(&[u8::MAX, u8::MAX])), // min: u8::MAX,  max: u8::MAX,  sum: 510
                    ("p_u16s", list_u16(&[1, 2, 3])), // min: 1,  max: 3,  sum: 6,    avg: 2.0,  len: 3
                    ("p_u16s_max", list_u16(&[u16::MAX, u16::MAX])), // min: u16::MAX,  max: u16::MAX,  sum: 131070
                    ("p_u32s", list_u32(&[1, 2, 3])), // min: 1,  max: 3,  sum: 6,    avg: 2.0,  len: 3
                    ("p_u32s_max", list_u32(&[u32::MAX, u32::MAX])), // min: 1,  max: 3,  sum: 8589934590
                    ("p_u64s", list_u64(&[1, 2, 3])), // min: 1,  max: 3,  sum: 6,    avg: 2.0,  len: 3
                    ("p_u64s_max", list_u64(&[u64::MAX, u64::MAX])), // min: u64::MAX,  max: u64::MAX,  sum: OVERFLOW
                    ("p_i32s", list_i32(&[1, 2, 3])), // min: 1,  max: 3,  sum: 6,    avg: 2.0,  len: 3
                    ("p_i64s", list_i64(&[1, 2, 3])), // min: 1,  max: 3,  sum: 6,    avg: 2.0,  len: 3
                    ("p_f32s", list_f32(&[1.0, 2.0, 3.5])), // min: 1.0, max: 3.5, sum: 6.5,  avg: 2.1666666666666665, len: 3
                    ("p_f64s", list_f64(&[50.0, 40.0])), // min: 40.0, max: 50.0, sum: 90.0, avg: 45.0, len: 2
                    (
                        "nested_list",
                        list(vec![
                            list(vec![
                                list(vec![
                                    list(vec![50.0.into_prop(), 40.0.into_prop()]),
                                    list(vec![60.0.into_prop()]),
                                ]),
                                list(vec![list(vec![46.0.into_prop()])]),
                            ]),
                            list(vec![list(vec![list(vec![90.0.into_prop()])])]),
                        ]),
                    ),
                ],
            ),
            (
                2,
                "n1",
                vec![
                    ("p_strs", list_str(&["a", "b", "c", "d"])), // min: None, max: None, sum: None, avg: None, len: 4
                    ("p_bools", list_bool(&[true, true])), // min: None, max: None, sum: None, avg: None, len: 2
                    ("p_u8s", list_u8(&[1, 2, 3, 4])), // min: 1,  max: 4,  sum: 10,   avg: 2.5,  len: 4
                    ("p_u16s", list_u16(&[1, 2, 3, 4])), // min: 1,  max: 4,  sum: 10,   avg: 2.5,  len: 4
                    ("p_u32s", list_u32(&[1, 2, 3, 4])), // min: 1,  max: 4,  sum: 10,   avg: 2.5,  len: 4
                    ("p_u64s", list_u64(&[1, 2, 3, 4])), // min: 1,  max: 4,  sum: 10,   avg: 2.5,  len: 4
                    ("p_i32s", list_i32(&[1, 2, 3, 4])), // min: 1,  max: 4,  sum: 10,   avg: 2.5,  len: 4
                    ("p_i64s", list_i64(&[1, 2, 3, 4])), // min: 1,  max: 4,  sum: 10,   avg: 2.5,  len: 4
                    ("p_f32s", list_f32(&[1.0, 2.0, 3.5, 4.5])), // min: 1.0, max: 4.5, sum: 11.0, avg: 2.75, len: 4
                    ("p_f64s", list_f64(&[30.0, 50.0, 40.0])), // min: 30.0, max: 50.0, sum: 120.0, avg: 40.0, len: 3
                ],
            ),
            (
                1,
                "n2",
                vec![
                    ("p_strs", list_str(&["a", "b", "c", "d"])), // min: None, max: None, sum: None, avg: None, len: 4
                    ("p_u64s", list_u64(&[1, 2, 3, 4])), // min: 1,  max: 4,  sum: 10,   avg: 2.5,  len: 4
                    ("p_f64s", list_f64(&[30.0, 50.0, 40.0])), // min: 30.0, max: 50.0, sum: 120.0, avg: 40.0, len: 3
                    ("p_bools", list_bool(&[false, false])),
                ],
            ),
            (
                2,
                "n2",
                vec![
                    ("p_strs", list_str(&["a", "b", "c", "d"])), // min: None, max: None, sum: None, avg: None, len: 4
                    ("p_u64s", list_u64(&[1, 2, 3, 4])), // min: 1,  max: 4,  sum: 10,   avg: 2.5,  len: 4
                    ("p_f64s", list_f64(&[30.0, 50.0, 40.0])), // min: 30.0, max: 50.0, sum: 120.0, avg: 40.0, len: 3
                ],
            ),
            (
                1,
                "n3",
                vec![
                    ("p_strs", list_str(&["a", "b", "c"])), // min: None, max: None, sum: None, avg: None, len: 3
                    ("p_bools", list_bool(&[true, false])), // min: None, max: None, sum: None, avg: None, len: 2
                    ("p_u8s", list_u8(&[1, 1, 4])), // min: 1,  max: 4,  sum: 6,   avg: 2.0,  len: 3
                    ("p_u16s", list_u16(&[1, 0, 5])), // min: 0,  max: 5,  sum: 6,   avg: 2.0,  len: 3
                    ("p_u32s", list_u32(&[2, 2, 2])), // min: 2,  max: 2,  sum: 6,   avg: 2.0,  len: 3
                    ("p_u64s", list_u64(&[0, 3, 3])), // min: 0,  max: 3,  sum: 6,   avg: 2.0,  len: 3
                    ("p_i32s", list_i32(&[-1, 4, 3])), // min: -1, max: 4,  sum: 6,   avg: 2.0,  len: 3
                    ("p_i64s", list_i64(&[0, 3, -3])), // min: -3, max: 3,  sum: 0,   avg: 0.0,  len: 3
                    ("p_f32s", list_f32(&[1.0, 2.5, 3.0])), // min: 1.0, max: 3.0, sum: 6.5, avg: 2.1666666666666665, len: 3
                    ("p_f64s", list_f64(&[30.0, 60.0])), // min: 30.0, max: 60.0, sum: 90.0, avg: 45.0, len: 2
                    (
                        "nested_list",
                        list(vec![
                            list(vec![
                                list(vec![
                                    list(vec![50.0.into_prop(), 40.0.into_prop()]),
                                    list(vec![60.0.into_prop()]),
                                ]),
                                list(vec![list(vec![46.0.into_prop()])]),
                            ]),
                            list(vec![list(vec![list(vec![90.0.into_prop()])])]),
                        ]),
                    ),
                ],
            ),
            (
                2,
                "n3",
                vec![
                    ("p_strs", list_str(&["a", "b", "c"])), // min: None, max: None, sum: None, avg: None, len: 3
                    ("p_bools", list_bool(&[true, false])), // min: None, max: None, sum: None, avg: None, len: 2
                    ("p_u8s", list_u8(&[1, 2, 3])), // min: 1,  max: 3,  sum: 6,   avg: 2.0,  len: 3
                    ("p_u16s", list_u16(&[1, 2, 3])), // min: 1,  max: 3,  sum: 6,   avg: 2.0,  len: 3
                    ("p_u32s", list_u32(&[1, 2, 3])), // min: 1,  max: 3,  sum: 6,   avg: 2.0,  len: 3
                    ("p_u64s", list_u64(&[1, 2, 3])), // min: 1,  max: 3,  sum: 6,   avg: 2.0,  len: 3
                    ("p_i32s", list_i32(&[1, 2, 3])), // min: 1,  max: 3,  sum: 6,   avg: 2.0,  len: 3
                    ("p_i64s", list_i64(&[1, 2, -3])), // min: -3, max: 2,  sum: 0,   avg: 0.0,  len: 3
                    ("p_f32s", list_f32(&[1.0, 2.0, 3.5])), // min: 1.0, max: 3.5, sum: 6.5, avg: 2.1666666666666665, len: 3
                    ("p_f64s", list_f64(&[50.0, 40.0])), // min: 40.0, max: 50.0, sum: 90.0, avg: 45.0, len: 2
                    (
                        "nested_list",
                        list(vec![
                            list(vec![
                                list(vec![
                                    list(vec![50.0.into_prop(), 40.0.into_prop()]),
                                    list(vec![60.0.into_prop()]),
                                ]),
                                list(vec![list(vec![46.0.into_prop()])]),
                            ]),
                            list(vec![list(vec![list(vec![90.0.into_prop()])])]),
                        ]),
                    ),
                ],
            ),
            (
                1,
                "n4",
                vec![
                    ("p_strs", list_str(&["a", "b", "c"])), // min: None, max: None, sum: None, avg: None, len: 3
                    ("p_bools", list_bool(&[true, false])), // min: None, max: None, sum: None, avg: None, len: 2
                    ("p_u64s", list_u64(&[1, 2, 3])), // min: 1,  max: 3,  sum: 6,    avg: 2.0,  len: 3
                    ("p_i32s", list_i32(&[1, 2, 3])), // min: 1,  max: 3,  sum: 6,    avg: 2.0,  len: 3
                    ("p_f32s", list_f32(&[1.0, 2.0, 3.5])), // min: 1.0, max: 3.5, sum: 6.5,  avg: 2.1666666666666665, len: 3
                    ("p_bools_all", list_bool(&[true, true])),
                ],
            ),
            (
                2,
                "n4",
                vec![
                    ("p_strs", list_str(&["x", "y", "z"])), // min: None, max: None, sum: None, avg: None, len: 3
                    ("p_bools", list_bool(&[false, false])), // min: None, max: None, sum: None, avg: None, len: 2
                    ("p_u64s", list_u64(&[10, 20, 30])), // min: 10, max: 30, sum: 60,   avg: 20.0, len: 3
                    ("p_i32s", list_i32(&[10, 20, 30])), // min: 10, max: 30, sum: 60,   avg: 20.0, len: 3
                    ("p_f32s", list_f32(&[10.0, 20.0, 30.0])), // min: 10.0, max: 30.0, sum: 60.0, avg: 20.0, len: 3
                    ("p_bools_all", list_bool(&[true, true])),
                ],
            ),
            (
                2,
                "n5",
                vec![
                    ("p_u64s", list_u64(&[u64::MAX, 1])), // min: 1,  max: u64::MAX, sum: None (overflow), avg: 9223372036854775808.0, len: 2
                    ("p_u64s_max", list_u64(&[u64::MAX, 1])), // min: 1,  max: u64::MAX, sum: None (overflow), avg: 9223372036854775808.0, len: 2
                    ("p_u64s_min", list_u64(&[u64::MIN, 1])), // min: 1,  max: u64::MAX, sum: None (overflow), avg: 9223372036854775808.0, len: 2
                    ("p_i64s", list_i64(&[i64::MAX, 1])), // min: 1,  max: i64::MAX, sum: None (overflow), avg: 4611686018427387904.0, len: 2
                    ("p_i64s_max", list_i64(&[i64::MAX, 1])), // min: 1,  max: i64::MAX, sum: None (overflow), avg: 4611686018427387904.0, len: 2
                    ("p_i64s_min", list_i64(&[i64::MIN, 1])), // min: 1,  max: i64::MAX, sum: None (overflow), avg: 4611686018427387904.0, len: 2
                ],
            ),
            (
                2,
                "n6",
                vec![
                    ("p_i32s", list_i32(&[-2, 1, 3])), // min: -2, max: 3, sum: 2, avg: 0.6666666666666666, len: 3
                ],
            ),
            (
                1,
                "n7",
                vec![
                    ("p_u64s", list_u64(&[])), // min: None, max: None, sum: None, avg: None, len: 0
                ],
            ),
            (
                2,
                "n10",
                vec![
                    ("p_strs", list_str(&["a", "b", "c"])), // min: None, max: None, sum: None, avg: None, len: 3
                    ("p_bools", list_bool(&[true, false])), // min: None, max: None, sum: None, avg: None, len: 2
                    ("p_u8s", list_u8(&[1, 2, 3])), // min: 1,  max: 3,  sum: 6,   avg: 2.0,  len: 3
                    ("p_u16s", list_u16(&[1, 2, 3])), // min: 1,  max: 3,  sum: 6,   avg: 2.0,  len: 3
                    ("p_u32s", list_u32(&[1, 2, 3])), // min: 1,  max: 3,  sum: 6,   avg: 2.0,  len: 3
                    ("p_u64s", list_u64(&[1, 2, 3])), // min: 1,  max: 3,  sum: 6,   avg: 2.0,  len: 3
                    ("p_i32s", list_i32(&[1, 2, 3])), // min: 1,  max: 3,  sum: 6,   avg: 2.0,  len: 3
                    ("p_i64s", list_i64(&[1, 2, -3])), // min: -3, max: 2,  sum: 0,   avg: 0.0,  len: 3
                    ("p_f32s", list_f32(&[1.0, 2.0, 3.5])), // min: 1.0, max: 3.5, sum: 6.5, avg: 2.1666666666666665, len: 3
                    ("p_f64s", list_f64(&[50.0, 40.0])), // min: 40.0, max: 50.0, sum: 90.0, avg: 45.0, len: 2
                    ("p_bools_all", list_bool(&[true, true])),
                ],
            ),
        ];

        for (t, id, props) in nodes {
            graph.add_node(t, id, props, None, None).unwrap();
        }

        // Each tuple represents (node_name, properties).
        let metadata: [(&str, Vec<(&str, Prop)>); 8] = [
            (
                "n1",
                vec![
                    ("p_u8s", list_u8(&[2, 9])), // min: 2,  max: 9,  sum: 11,  avg: 5.5,  len: 2
                    ("p_u16s", list_u16(&[3, 5])), // min: 3,  max: 5,  sum: 8,   avg: 4.0,  len: 2
                    ("p_u32s", list_u32(&[4, 9])), // min: 4,  max: 9,  sum: 13,  avg: 6.5,  len: 2
                ],
            ),
            (
                "n2",
                vec![
                    ("p_u64s", list_u64(&[2, 3, 7])), // min: 2,  max: 7,  sum: 12,  avg: 4.0,  len: 3
                ],
            ),
            (
                "n3",
                vec![
                    ("p_i32s", list_i32(&[10, 2, -3])), // min: -3, max: 10, sum: 9,   avg: 3.0,  len: 3
                    ("p_i64s", list_i64(&[1, 12, 3, 4])), // min: 1,  max: 12, sum: 20,  avg: 5.0,  len: 4
                ],
            ),
            (
                "n4",
                vec![
                    ("p_f32s", list_f32(&[1.5, 2.5])), // min: 1.5, max: 2.5, sum: 4.0,  avg: 2.0,  len: 2
                    ("p_f64s", list_f64(&[0.5, 1.5])), // min: 0.5, max: 1.5, sum: 2.0,  avg: 1.0,  len: 2
                ],
            ),
            (
                "n5",
                vec![
                    ("p_strs", list_str(&["m1", "m2", "m3"])), // min: None, max: None, sum: None, avg: None, len: 3
                ],
            ),
            (
                "n6",
                vec![
                    ("p_u64s", list_u64(&[])), // min: None, max: None, sum: None, avg: None, len: 0
                    ("p_strs", list_str(&["a", "a"])),
                ],
            ),
            (
                "n7",
                vec![
                    ("p_u64s", list_u64(&[u64::MAX, 1])), // min: 1, max: u64::MAX, sum: None (overflow), avg: ~9.22e18, len: 2
                    ("p_strs", list_str(&["a"])),
                ],
            ),
            (
                "n10",
                vec![
                    ("p_u64s", list_u64(&[1, 2, 3])), // min: 1,  max: 3,  sum: 6,   avg: 2.0,  len: 3
                    ("p_strs", list_str(&["a", "b", "c"])), // min: None, max: None, sum: None, avg: None, len: 3
                ],
            ),
        ];

        for (node_id, md) in metadata {
            graph.node(node_id).unwrap().add_metadata(md).unwrap();
        }

        graph
    }

    #[track_caller]
    fn apply_assertion(
        filter: impl TryAsCompositeFilter + CreateFilter + Clone,
        expected: &[&str],
    ) {
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected,
            All,
        );
    }

    #[track_caller]
    fn apply_assertion_err(
        filter: impl TryAsCompositeFilter + CreateFilter + Clone,
        expected: &str,
    ) {
        assert_filter_nodes_err(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected,
            All,
        );

        // assert_search_nodes_err(
        //     init_nodes_graph,
        //     IdentityGraphTransformer,
        //     filter,
        //     expected,
        //     All,
        // );
    }

    // ------ Property: SUM ----
    #[test]
    fn test_node_property_sum_u8s() {
        let filter = NodeFilter.property("p_u8s").sum().eq(Prop::U64(10));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_sum_u16s() {
        let filter = NodeFilter.property("p_u16s").sum().eq(Prop::U64(6));
        let expected = vec!["n10", "n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_sum_u32s() {
        let filter = NodeFilter.property("p_u32s").sum().eq(Prop::U64(10));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_sum_u64s() {
        let filter = NodeFilter.property("p_u64s").sum().eq(Prop::U64(6));
        let expected = vec!["n10", "n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_sum_i32s() {
        let filter = NodeFilter.property("p_i32s").sum().eq(Prop::I64(2));
        let expected = vec!["n6"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_sum_i64s() {
        let filter = NodeFilter.property("p_i64s").sum().eq(Prop::I64(0));
        let expected = vec!["n10", "n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_sum_f32s() {
        let filter = NodeFilter.property("p_f32s").sum().eq(Prop::F64(6.5));
        let expected = vec!["n10", "n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_sum_f64s() {
        let filter = NodeFilter.property("p_f64s").sum().eq(Prop::F64(120.0));
        let expected = vec!["n1", "n2"];
        apply_assertion(filter, &expected);
    }

    // ------ Property: AVG ----
    #[test]
    fn test_node_property_avg_u8s() {
        let filter = NodeFilter.property("p_u8s").avg().eq(Prop::F64(2.5));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_avg_u16s() {
        let filter = NodeFilter.property("p_u16s").avg().eq(Prop::F64(2.0));
        let expected = vec!["n10", "n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_avg_u32s() {
        let filter = NodeFilter.property("p_u32s").avg().eq(Prop::F64(2.5));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_avg_u64s() {
        let filter = NodeFilter.property("p_u64s").avg().eq(Prop::F64(2.0));
        let expected = vec!["n10", "n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_avg_i32s() {
        let filter = NodeFilter
            .property("p_i32s")
            .avg()
            .eq(Prop::F64(0.6666666666666666));
        let expected = vec!["n6"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_avg_i64s() {
        let filter = NodeFilter.property("p_i64s").avg().eq(Prop::F64(0.0));
        let expected = vec!["n10", "n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_avg_f32s() {
        let filter = NodeFilter
            .property("p_f32s")
            .avg()
            .eq(Prop::F64(2.1666666666666665));
        let expected = vec!["n10", "n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_avg_f64s() {
        let filter = NodeFilter.property("p_f64s").avg().eq(Prop::F64(40.0));
        let expected = vec!["n1", "n2"];
        apply_assertion(filter, &expected);
    }

    // ------ Property: LEN ------
    #[test]
    fn test_node_property_len_u8s() {
        let filter = NodeFilter.property("p_u8s").len().eq(Prop::U64(4));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_len_u16s() {
        let filter = NodeFilter.property("p_u16s").len().eq(Prop::U64(3));
        let expected = vec!["n10", "n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_len_u32s() {
        let filter = NodeFilter.property("p_u32s").len().eq(Prop::U64(4));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_len_u64s() {
        let filter = NodeFilter.property("p_u64s").len().eq(Prop::U64(3));
        let expected = vec!["n10", "n3", "n4"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_len_i32s() {
        let filter = NodeFilter.property("p_i32s").len().eq(Prop::U64(3));
        let expected = vec!["n10", "n3", "n4", "n6"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_len_i64s() {
        let filter = NodeFilter.property("p_i64s").len().eq(Prop::U64(3));
        let expected = vec!["n10", "n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_len_f32s() {
        let filter = NodeFilter.property("p_f32s").len().eq(Prop::U64(3));
        let expected = vec!["n10", "n3", "n4"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_len_f64s() {
        let filter = NodeFilter.property("p_f64s").len().eq(Prop::U64(3));
        let expected = vec!["n1", "n2"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_len_strs() {
        let filter = NodeFilter.property("p_strs").len().eq(Prop::U64(3));
        let expected = vec!["n10", "n3", "n4"];
        apply_assertion(filter, &expected);
    }

    // ------ Property: MIN ------
    #[test]
    fn test_node_property_min_u8s() {
        let filter = NodeFilter.property("p_u8s").min().eq(Prop::U8(1));
        let expected = vec!["n1", "n10", "n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_min_u16s() {
        let filter = NodeFilter.property("p_u16s").min().eq(Prop::U16(1));
        let expected = vec!["n1", "n10", "n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_min_u32s() {
        let filter = NodeFilter.property("p_u32s").min().eq(Prop::U32(1));
        let expected = vec!["n1", "n10", "n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_min_u64s() {
        let filter = NodeFilter.property("p_u64s").min().eq(Prop::U64(1));
        let expected = vec!["n1", "n10", "n2", "n3", "n5"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_min_i32s() {
        let filter = NodeFilter.property("p_i32s").min().eq(Prop::I32(-2));
        let expected = vec!["n6"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_min_i64s() {
        let filter = NodeFilter.property("p_i64s").min().eq(Prop::I64(-3));
        let expected = vec!["n10", "n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_min_f32s() {
        let filter = NodeFilter.property("p_f32s").min().eq(Prop::F32(10.0));
        let expected = vec!["n4"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_min_f64s() {
        let filter = NodeFilter.property("p_f64s").min().eq(Prop::F64(40.0));
        let expected = vec!["n10", "n3"];
        apply_assertion(filter, &expected);
    }

    // ------ Property: MAX ------
    #[test]
    fn test_node_property_max_u8s() {
        let filter = NodeFilter.property("p_u8s").max().eq(Prop::U8(4));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_max_u16s() {
        let filter = NodeFilter.property("p_u16s").max().eq(Prop::U16(3));
        let expected = vec!["n10", "n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_max_u32s() {
        let filter = NodeFilter.property("p_u32s").max().eq(Prop::U32(4));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_max_u64s() {
        let filter = NodeFilter.property("p_u64s").max().eq(Prop::U64(3));
        let expected = vec!["n10", "n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_max_i32s() {
        let filter = NodeFilter.property("p_i32s").max().eq(Prop::I32(3));
        let expected = vec!["n10", "n3", "n6"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_max_i64s() {
        let filter = NodeFilter.property("p_i64s").max().eq(Prop::I64(2));
        let expected = vec!["n10", "n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_max_f32s() {
        let filter = NodeFilter.property("p_f32s").max().eq(Prop::F32(30.0));
        let expected = vec!["n4"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_max_f64s() {
        let filter = NodeFilter.property("p_f64s").max().eq(Prop::F64(50.0));
        let expected = vec!["n1", "n10", "n2", "n3"];
        apply_assertion(filter, &expected);
    }

    // ------ Metadata: SUM ------
    #[test]
    fn test_node_property_metadata_sum_u8s() {
        let filter = NodeFilter.metadata("p_u8s").sum().eq(Prop::U64(11));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_metadata_sum_u16s() {
        let filter = NodeFilter.metadata("p_u16s").sum().eq(Prop::U64(8));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_metadata_sum_u32s() {
        let filter = NodeFilter.metadata("p_u32s").sum().eq(Prop::U64(13));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_metadata_sum_u64s() {
        let filter = NodeFilter.metadata("p_u64s").sum().eq(Prop::U64(12));
        let expected = vec!["n2"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_metadata_sum_i32s() {
        let filter = NodeFilter.metadata("p_i32s").sum().eq(Prop::I64(9));
        let expected = vec!["n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_metadata_sum_i64s() {
        let filter = NodeFilter.metadata("p_i64s").sum().eq(Prop::I64(20));
        let expected = vec!["n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_metadata_sum_f32s() {
        let filter = NodeFilter.metadata("p_f32s").sum().eq(Prop::F64(4.0));
        let expected = vec!["n4"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_metadata_sum_f64s() {
        let filter = NodeFilter.metadata("p_f64s").sum().eq(Prop::F64(2.0));
        let expected = vec!["n4"];
        apply_assertion(filter, &expected);
    }

    // ------ Metadata: AVG ------
    #[test]
    fn test_node_property_metadata_avg_u8s() {
        let filter = NodeFilter.metadata("p_u8s").avg().eq(Prop::F64(5.5));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_metadata_avg_u16s() {
        let filter = NodeFilter.metadata("p_u16s").avg().eq(Prop::F64(4.0));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_metadata_avg_u32s() {
        let filter = NodeFilter.metadata("p_u32s").avg().eq(Prop::F64(6.5));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_metadata_avg_u64s() {
        let filter = NodeFilter.metadata("p_u64s").avg().eq(Prop::F64(4.0));
        let expected = vec!["n2"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_metadata_avg_i32s() {
        let filter = NodeFilter.metadata("p_i32s").avg().eq(Prop::F64(3.0));
        let expected = vec!["n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_metadata_avg_i64s() {
        let filter = NodeFilter.metadata("p_i64s").avg().eq(Prop::F64(5.0));
        let expected = vec!["n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_metadata_avg_f32s() {
        let filter = NodeFilter.metadata("p_f32s").avg().eq(Prop::F64(2.0));
        let expected = vec!["n4"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_metadata_avg_f64s() {
        let filter = NodeFilter.metadata("p_f64s").avg().eq(Prop::F64(1.0));
        let expected = vec!["n4"];
        apply_assertion(filter, &expected);
    }

    // ------ Metadata: MIN ------
    #[test]
    fn test_node_property_metadata_min_u8s() {
        let filter = NodeFilter.metadata("p_u8s").min().eq(Prop::U8(2));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_metadata_min_u16s() {
        let filter = NodeFilter.metadata("p_u16s").min().eq(Prop::U16(3));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_metadata_min_u32s() {
        let filter = NodeFilter.metadata("p_u32s").min().eq(Prop::U32(4));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_metadata_min_u64s() {
        let filter = NodeFilter.metadata("p_u64s").min().eq(Prop::U64(2));
        let expected = vec!["n2"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_metadata_min_i32s() {
        let filter = NodeFilter.metadata("p_i32s").min().eq(Prop::I32(-3));
        let expected = vec!["n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_metadata_min_i64s() {
        let filter = NodeFilter.metadata("p_i64s").min().eq(Prop::I64(1));
        let expected = vec!["n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_metadata_min_f32s() {
        let filter = NodeFilter.metadata("p_f32s").min().eq(Prop::F32(1.5));
        let expected = vec!["n4"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_metadata_min_f64s() {
        let filter = NodeFilter.metadata("p_f64s").min().eq(Prop::F64(0.5));
        let expected = vec!["n4"];
        apply_assertion(filter, &expected);
    }

    // ------ Metadata: MAX ------
    #[test]
    fn test_node_property_metadata_max_u8s() {
        let filter = NodeFilter.metadata("p_u8s").max().eq(Prop::U8(9));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_metadata_max_u16s() {
        let filter = NodeFilter.metadata("p_u16s").max().eq(Prop::U16(5));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_metadata_max_u32s() {
        let filter = NodeFilter.metadata("p_u32s").max().eq(Prop::U32(9));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_metadata_max_u64s() {
        let filter = NodeFilter.metadata("p_u64s").max().eq(Prop::U64(7));
        let expected = vec!["n2"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_metadata_max_i32s() {
        let filter = NodeFilter.metadata("p_i32s").max().eq(Prop::I32(10));
        let expected = vec!["n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_metadata_max_i64s() {
        let filter = NodeFilter.metadata("p_i64s").max().eq(Prop::I64(12));
        let expected = vec!["n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_metadata_max_f32s() {
        let filter = NodeFilter.metadata("p_f32s").max().eq(Prop::F32(2.5));
        let expected = vec!["n4"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_metadata_max_f64s() {
        let filter = NodeFilter.metadata("p_f64s").max().eq(Prop::F64(1.5));
        let expected = vec!["n4"];
        apply_assertion(filter, &expected);
    }

    // ------ Metadata: Len ------
    #[test]
    fn test_node_property_metadata_len_u8s() {
        let filter = NodeFilter.metadata("p_u8s").len().eq(Prop::U64(2));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_metadata_len_u16s() {
        let filter = NodeFilter.metadata("p_u16s").len().eq(Prop::U64(2));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_metadata_len_u32s() {
        let filter = NodeFilter.metadata("p_u32s").len().eq(Prop::U64(2));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_metadata_len_u64s() {
        let filter = NodeFilter.metadata("p_u64s").len().eq(Prop::U64(3));
        let expected = vec!["n10", "n2"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_metadata_len_i32s() {
        let filter = NodeFilter.metadata("p_i32s").len().eq(Prop::U64(3));
        let expected = vec!["n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_metadata_len_i64s() {
        let filter = NodeFilter.metadata("p_i64s").len().eq(Prop::U64(4));
        let expected = vec!["n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_metadata_len_f32s() {
        let filter = NodeFilter.metadata("p_f32s").len().eq(Prop::U64(2));
        let expected = vec!["n4"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_metadata_len_f64s() {
        let filter = NodeFilter.metadata("p_f64s").len().eq(Prop::U64(2));
        let expected = vec!["n4"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_metadata_len_strs() {
        let filter = NodeFilter.metadata("p_strs").len().eq(Prop::U64(3));
        let expected = vec!["n10", "n5"];
        apply_assertion(filter, &expected);
    }

    // ------ Temporal last: SUM ------
    #[test]
    fn test_node_property_temporal_last_sum_u8s() {
        let filter = NodeFilter
            .property("p_u8s")
            .temporal()
            .last()
            .sum()
            .eq(Prop::U64(10));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_last_sum_u16s() {
        let filter = NodeFilter
            .property("p_u16s")
            .temporal()
            .last()
            .sum()
            .eq(Prop::U64(6));
        let expected = vec!["n3", "n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_last_sum_u32s() {
        let filter = NodeFilter
            .property("p_u32s")
            .temporal()
            .last()
            .sum()
            .eq(Prop::U64(10));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_last_sum_u64s() {
        let filter = NodeFilter
            .property("p_u64s")
            .temporal()
            .last()
            .sum()
            .eq(Prop::U64(60));
        let expected = vec!["n4"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_last_sum_i32s() {
        let filter = NodeFilter
            .property("p_i32s")
            .temporal()
            .last()
            .sum()
            .eq(Prop::I64(60));
        let expected = vec!["n4"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_last_sum_i64s() {
        let filter = NodeFilter
            .property("p_i64s")
            .temporal()
            .last()
            .sum()
            .eq(Prop::I64(0));
        let expected = vec!["n3", "n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_last_sum_f32s() {
        let filter = NodeFilter
            .property("p_f32s")
            .temporal()
            .last()
            .sum()
            .eq(Prop::F64(6.5));
        let expected = vec!["n3", "n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_last_sum_f64s() {
        let filter = NodeFilter
            .property("p_f64s")
            .temporal()
            .last()
            .sum()
            .eq(Prop::F64(90.0));
        let expected = vec!["n3", "n10"];
        apply_assertion(filter, &expected);
    }

    // ------ Temporal last: AVG ------
    #[test]
    fn test_node_property_temporal_last_avg_u8s() {
        let filter = NodeFilter
            .property("p_u8s")
            .temporal()
            .last()
            .avg()
            .eq(Prop::F64(2.5));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_last_avg_u16s() {
        let filter = NodeFilter
            .property("p_u16s")
            .temporal()
            .last()
            .avg()
            .eq(Prop::F64(2.0));
        let expected = vec!["n3", "n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_last_avg_u32s() {
        let filter = NodeFilter
            .property("p_u32s")
            .temporal()
            .last()
            .avg()
            .eq(Prop::F64(2.5));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_last_avg_u64s() {
        let filter = NodeFilter
            .property("p_u64s")
            .temporal()
            .last()
            .avg()
            .eq(Prop::F64(20.0));
        let expected = vec!["n4"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_last_avg_i32s() {
        let filter = NodeFilter
            .property("p_i32s")
            .temporal()
            .last()
            .avg()
            .eq(Prop::F64(0.6666666666666666));
        let expected = vec!["n6"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_last_avg_i64s() {
        let filter = NodeFilter
            .property("p_i64s")
            .temporal()
            .last()
            .avg()
            .eq(Prop::F64(0.0));
        let expected = vec!["n3", "n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_last_avg_f32s() {
        let filter = NodeFilter
            .property("p_f32s")
            .temporal()
            .last()
            .avg()
            .eq(Prop::F64(20.0));
        let expected = vec!["n4"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_last_avg_f64s() {
        let filter = NodeFilter
            .property("p_f64s")
            .temporal()
            .last()
            .avg()
            .eq(Prop::F64(45.0));
        let expected = vec!["n3", "n10"];
        apply_assertion(filter, &expected);
    }

    // ------ Temporal last: MIN ------
    #[test]
    fn test_node_property_temporal_last_min_u8s() {
        let filter = NodeFilter
            .property("p_u8s")
            .temporal()
            .last()
            .min()
            .eq(Prop::U8(1));
        let expected = vec!["n1", "n3", "n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_last_min_u16s() {
        let filter = NodeFilter
            .property("p_u16s")
            .temporal()
            .last()
            .min()
            .eq(Prop::U16(1));
        let expected = vec!["n1", "n3", "n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_last_min_u32s() {
        let filter = NodeFilter
            .property("p_u32s")
            .temporal()
            .last()
            .min()
            .eq(Prop::U32(1));
        let expected = vec!["n1", "n3", "n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_last_min_u64s() {
        let filter = NodeFilter
            .property("p_u64s")
            .temporal()
            .last()
            .min()
            .eq(Prop::U64(10));
        let expected = vec!["n4"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_last_min_i32s() {
        let filter = NodeFilter
            .property("p_i32s")
            .temporal()
            .last()
            .min()
            .eq(Prop::I32(-2));
        let expected = vec!["n6"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_last_min_i64s() {
        let filter = NodeFilter
            .property("p_i64s")
            .temporal()
            .last()
            .min()
            .eq(Prop::I64(-3));
        let expected = vec!["n3", "n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_last_min_f32s() {
        let filter = NodeFilter
            .property("p_f32s")
            .temporal()
            .last()
            .min()
            .eq(Prop::F32(10.0));
        let expected = vec!["n4"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_last_min_f64s() {
        let filter = NodeFilter
            .property("p_f64s")
            .temporal()
            .last()
            .min()
            .eq(Prop::F64(40.0));
        let expected = vec!["n3", "n10"];
        apply_assertion(filter, &expected);
    }

    // ------ Temporal last: MAX ------
    #[test]
    fn test_node_property_temporal_last_max_u8s() {
        let filter = NodeFilter
            .property("p_u8s")
            .temporal()
            .last()
            .max()
            .eq(Prop::U8(4));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_last_max_u16s() {
        let filter = NodeFilter
            .property("p_u16s")
            .temporal()
            .last()
            .max()
            .eq(Prop::U16(3));
        let expected = vec!["n3", "n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_last_max_u32s() {
        let filter = NodeFilter
            .property("p_u32s")
            .temporal()
            .last()
            .max()
            .eq(Prop::U32(4));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_last_max_u64s() {
        let filter = NodeFilter
            .property("p_u64s")
            .temporal()
            .last()
            .max()
            .eq(Prop::U64(30));
        let expected = vec!["n4"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_last_max_i32s() {
        let filter = NodeFilter
            .property("p_i32s")
            .temporal()
            .last()
            .max()
            .eq(Prop::I32(3));
        let expected = vec!["n3", "n6", "n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_last_max_i64s() {
        let filter = NodeFilter
            .property("p_i64s")
            .temporal()
            .last()
            .max()
            .eq(Prop::I64(2));
        let expected = vec!["n3", "n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_last_max_f32s() {
        let filter = NodeFilter
            .property("p_f32s")
            .temporal()
            .last()
            .max()
            .eq(Prop::F32(3.5));
        let expected = vec!["n3", "n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_last_max_f64s() {
        let filter = NodeFilter
            .property("p_f64s")
            .temporal()
            .last()
            .max()
            .eq(Prop::F64(50.0));
        let expected = vec!["n1", "n2", "n3", "n10"];
        apply_assertion(filter, &expected);
    }

    // ------ Temporal last: LEN ------
    #[test]
    fn test_node_property_temporal_last_len_u8s() {
        let filter = NodeFilter
            .property("p_u8s")
            .temporal()
            .last()
            .len()
            .eq(Prop::U64(3));
        let expected = vec!["n3", "n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_last_len_u16s() {
        let filter = NodeFilter
            .property("p_u16s")
            .temporal()
            .last()
            .len()
            .eq(Prop::U64(3));
        let expected = vec!["n3", "n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_last_len_u32s() {
        let filter = NodeFilter
            .property("p_u32s")
            .temporal()
            .last()
            .len()
            .eq(Prop::U64(3));
        let expected = vec!["n3", "n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_last_len_u64s() {
        let filter = NodeFilter
            .property("p_u64s")
            .temporal()
            .last()
            .len()
            .eq(Prop::U64(3));
        let expected = vec!["n3", "n4", "n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_last_len_i32s() {
        let filter = NodeFilter
            .property("p_i32s")
            .temporal()
            .last()
            .len()
            .eq(Prop::U64(3));
        let expected = vec!["n3", "n4", "n6", "n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_last_len_i64s() {
        let filter = NodeFilter
            .property("p_i64s")
            .temporal()
            .last()
            .len()
            .eq(Prop::U64(3));
        let expected = vec!["n3", "n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_last_len_f32s() {
        let filter = NodeFilter
            .property("p_f32s")
            .temporal()
            .last()
            .len()
            .eq(Prop::U64(3));
        let expected = vec!["n3", "n4", "n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_last_len_f64s() {
        let filter = NodeFilter
            .property("p_f64s")
            .temporal()
            .last()
            .len()
            .eq(Prop::U64(2));
        let expected = vec!["n3", "n10"];
        apply_assertion(filter, &expected);
    }

    // ------ Temporal all: SUM ------
    #[test]
    fn test_node_property_temporal_all_sum_u8s() {
        let filter = NodeFilter
            .property("p_u8s")
            .temporal()
            .all()
            .sum()
            .eq(Prop::U64(6));
        let expected = vec!["n3", "n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_all_sum_u16s() {
        let filter = NodeFilter
            .property("p_u16s")
            .temporal()
            .all()
            .sum()
            .eq(Prop::U64(6));
        let expected = vec!["n3", "n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_all_sum_u32s() {
        let filter = NodeFilter
            .property("p_u32s")
            .temporal()
            .all()
            .sum()
            .eq(Prop::U64(6));
        let expected = vec!["n3", "n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_all_sum_u64s() {
        let filter = NodeFilter
            .property("p_u64s")
            .temporal()
            .all()
            .sum()
            .eq(Prop::U64(6));
        let expected = vec!["n3", "n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_all_sum_i32s() {
        let filter = NodeFilter
            .property("p_i32s")
            .temporal()
            .all()
            .sum()
            .eq(Prop::I64(6));
        let expected = vec!["n3", "n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_all_sum_i64s() {
        let filter = NodeFilter
            .property("p_i64s")
            .temporal()
            .all()
            .sum()
            .eq(Prop::I64(0));
        let expected = vec!["n3", "n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_all_sum_f32s() {
        let filter = NodeFilter
            .property("p_f32s")
            .temporal()
            .all()
            .sum()
            .eq(Prop::F64(6.5));
        let expected = vec!["n3", "n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_all_sum_f64s() {
        let filter = NodeFilter
            .property("p_f64s")
            .temporal()
            .all()
            .sum()
            .eq(Prop::F64(90.0));
        let expected = vec!["n3", "n10"];
        apply_assertion(filter, &expected);
    }

    // ------ Temporal all: AVG ------
    #[test]
    fn test_node_property_temporal_all_avg_u8s() {
        let filter = NodeFilter
            .property("p_u8s")
            .temporal()
            .all()
            .avg()
            .eq(Prop::F64(2.0));
        let expected = vec!["n3", "n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_all_avg_u16s() {
        let filter = NodeFilter
            .property("p_u16s")
            .temporal()
            .all()
            .avg()
            .eq(Prop::F64(2.0));
        let expected = vec!["n3", "n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_all_avg_u32s() {
        let filter = NodeFilter
            .property("p_u32s")
            .temporal()
            .all()
            .avg()
            .eq(Prop::F64(2.0));
        let expected = vec!["n3", "n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_all_avg_u64s() {
        let filter = NodeFilter
            .property("p_u64s")
            .temporal()
            .all()
            .avg()
            .eq(Prop::F64(2.0));
        let expected = vec!["n3", "n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_all_avg_i32s() {
        let filter = NodeFilter
            .property("p_i32s")
            .temporal()
            .all()
            .avg()
            .eq(Prop::F64(2.0));
        let expected = vec!["n3", "n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_all_avg_i64s() {
        let filter = NodeFilter
            .property("p_i64s")
            .temporal()
            .all()
            .avg()
            .eq(Prop::F64(0.0));
        let expected = vec!["n3", "n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_all_avg_f32s() {
        let filter = NodeFilter
            .property("p_f32s")
            .temporal()
            .all()
            .avg()
            .eq(Prop::F64(2.1666666666666665));
        let expected = vec!["n10", "n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_all_avg_f64s() {
        let filter = NodeFilter
            .property("p_f64s")
            .temporal()
            .all()
            .avg()
            .eq(Prop::F64(45.0));
        let expected = vec!["n10", "n3"];
        apply_assertion(filter, &expected);
    }

    // ------ Temporal all: MIN ------
    #[test]
    fn test_node_property_temporal_all_min_u8s() {
        let filter = NodeFilter
            .property("p_u8s")
            .temporal()
            .all()
            .min()
            .eq(Prop::U8(1));
        let expected = vec!["n1", "n10", "n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_all_min_u16s() {
        let filter = NodeFilter
            .property("p_u16s")
            .temporal()
            .all()
            .min()
            .eq(Prop::U16(1));
        let expected = vec!["n1", "n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_all_min_u32s() {
        let filter = NodeFilter
            .property("p_u32s")
            .temporal()
            .all()
            .min()
            .eq(Prop::U32(1));
        let expected = vec!["n1", "n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_all_min_u64s() {
        let filter = NodeFilter
            .property("p_u64s")
            .temporal()
            .all()
            .min()
            .eq(Prop::U64(1));
        let expected = vec!["n1", "n10", "n2", "n5"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_all_min_i32s() {
        let filter = NodeFilter
            .property("p_i32s")
            .temporal()
            .all()
            .min()
            .eq(Prop::I32(-2));
        let expected = vec!["n6"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_all_min_i64s() {
        let filter = NodeFilter
            .property("p_i64s")
            .temporal()
            .all()
            .min()
            .eq(Prop::I64(-3));
        let expected = vec!["n10", "n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_all_min_f32s() {
        let filter = NodeFilter
            .property("p_f32s")
            .temporal()
            .all()
            .min()
            .eq(Prop::F32(1.0));
        let expected = vec!["n1", "n10", "n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_all_min_f64s() {
        let filter = NodeFilter
            .property("p_f64s")
            .temporal()
            .all()
            .min()
            .eq(Prop::F64(30.0));
        let expected = vec!["n2"];
        apply_assertion(filter, &expected);
    }

    // ------ Temporal all: MAX ------
    #[test]
    fn test_node_property_temporal_all_max_u8s() {
        let filter = NodeFilter
            .property("p_u8s")
            .temporal()
            .all()
            .max()
            .eq(Prop::U8(3));
        let expected = vec!["n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_all_max_u16s() {
        let filter = NodeFilter
            .property("p_u16s")
            .temporal()
            .all()
            .max()
            .eq(Prop::U16(3));
        let expected = vec!["n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_all_max_u32s() {
        let filter = NodeFilter
            .property("p_u32s")
            .temporal()
            .all()
            .max()
            .eq(Prop::U32(3));
        let expected = vec!["n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_all_max_u64s() {
        let filter = NodeFilter
            .property("p_u64s")
            .temporal()
            .all()
            .max()
            .eq(Prop::U64(4));
        let expected = vec!["n2"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_all_max_i32s() {
        let filter = NodeFilter
            .property("p_i32s")
            .temporal()
            .all()
            .max()
            .eq(Prop::I32(3));
        let expected = vec!["n10", "n6"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_all_max_i64s() {
        let filter = NodeFilter
            .property("p_i64s")
            .temporal()
            .all()
            .max()
            .eq(Prop::I64(2));
        let expected = vec!["n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_all_max_f32s() {
        let filter = NodeFilter
            .property("p_f32s")
            .temporal()
            .all()
            .max()
            .eq(Prop::F32(3.5));
        let expected = vec!["n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_all_max_f64s() {
        let filter = NodeFilter
            .property("p_f64s")
            .temporal()
            .all()
            .max()
            .eq(Prop::F64(50.0));
        let expected = vec!["n1", "n10", "n2"];
        apply_assertion(filter, &expected);
    }

    // ------ Temporal all: LEN ------
    #[test]
    fn test_node_property_temporal_all_len_u8s() {
        let filter = NodeFilter
            .property("p_u8s")
            .temporal()
            .all()
            .len()
            .eq(Prop::U64(3));
        let expected = vec!["n10", "n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_all_len_u16s() {
        let filter = NodeFilter
            .property("p_u16s")
            .temporal()
            .all()
            .len()
            .eq(Prop::U64(3));
        let expected = vec!["n10", "n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_all_len_u32s() {
        let filter = NodeFilter
            .property("p_u32s")
            .temporal()
            .all()
            .len()
            .eq(Prop::U64(3));
        let expected = vec!["n10", "n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_all_len_u64s() {
        let filter = NodeFilter
            .property("p_u64s")
            .temporal()
            .all()
            .len()
            .eq(Prop::U64(4));
        let expected = vec!["n2"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_all_len_i32s() {
        let filter = NodeFilter
            .property("p_i32s")
            .temporal()
            .all()
            .len()
            .eq(Prop::U64(3));
        let expected = vec!["n10", "n3", "n4", "n6"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_all_len_i64s() {
        let filter = NodeFilter
            .property("p_i64s")
            .temporal()
            .all()
            .len()
            .eq(Prop::U64(2));
        let expected = vec!["n5"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_all_len_f32s() {
        let filter = NodeFilter
            .property("p_f32s")
            .temporal()
            .all()
            .len()
            .eq(Prop::U64(3));
        let expected = vec!["n10", "n3", "n4"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_all_len_f64s() {
        let filter = NodeFilter
            .property("p_f64s")
            .temporal()
            .all()
            .len()
            .eq(Prop::U64(2));
        let expected = vec!["n10", "n3"];
        apply_assertion(filter, &expected);
    }

    // ------ Temporal first: SUM ------
    #[test]
    fn test_node_property_temporal_first_sum_u8s() {
        let filter = NodeFilter
            .property("p_u8s")
            .temporal()
            .first()
            .sum()
            .eq(Prop::U64(6));
        let expected = vec!["n1", "n10", "n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_first_sum_u16s() {
        let filter = NodeFilter
            .property("p_u16s")
            .temporal()
            .first()
            .sum()
            .eq(Prop::U64(6));
        let expected = vec!["n1", "n10", "n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_first_sum_u32s() {
        let filter = NodeFilter
            .property("p_u32s")
            .temporal()
            .first()
            .sum()
            .eq(Prop::U64(6));
        let expected = vec!["n1", "n10", "n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_first_sum_u64s() {
        let filter = NodeFilter
            .property("p_u64s")
            .temporal()
            .first()
            .sum()
            .eq(Prop::U64(6));
        let expected = vec!["n1", "n10", "n3", "n4"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_first_sum_i32s() {
        let filter = NodeFilter
            .property("p_i32s")
            .temporal()
            .first()
            .sum()
            .eq(Prop::I64(6));
        let expected = vec!["n1", "n10", "n3", "n4"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_first_sum_i64s() {
        let filter = NodeFilter
            .property("p_i64s")
            .temporal()
            .first()
            .sum()
            .eq(Prop::I64(0));
        let expected = vec!["n3", "n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_first_sum_f32s() {
        let filter = NodeFilter
            .property("p_f32s")
            .temporal()
            .first()
            .sum()
            .eq(Prop::F64(6.5));
        let expected = vec!["n1", "n10", "n3", "n4"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_first_sum_f64s() {
        let filter = NodeFilter
            .property("p_f64s")
            .temporal()
            .first()
            .sum()
            .eq(Prop::F64(90.0));
        let expected = vec!["n1", "n10", "n3"];
        apply_assertion(filter, &expected);
    }

    // ------ Temporal first: AVG ------
    #[test]
    fn test_node_property_temporal_first_avg_u8s() {
        let filter = NodeFilter
            .property("p_u8s")
            .temporal()
            .first()
            .avg()
            .eq(Prop::F64(2.0));
        let expected = vec!["n1", "n10", "n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_first_avg_u16s() {
        let filter = NodeFilter
            .property("p_u16s")
            .temporal()
            .first()
            .avg()
            .eq(Prop::F64(2.0));
        let expected = vec!["n1", "n10", "n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_first_avg_u32s() {
        let filter = NodeFilter
            .property("p_u32s")
            .temporal()
            .first()
            .avg()
            .eq(Prop::F64(2.0));
        let expected = vec!["n1", "n10", "n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_first_avg_u64s() {
        let filter = NodeFilter
            .property("p_u64s")
            .temporal()
            .first()
            .avg()
            .eq(Prop::F64(2.0));
        let expected = vec!["n1", "n10", "n3", "n4"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_first_avg_i32s() {
        let filter = NodeFilter
            .property("p_i32s")
            .temporal()
            .first()
            .avg()
            .eq(Prop::F64(2.0));
        let expected = vec!["n1", "n10", "n3", "n4"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_first_avg_i64s() {
        let filter = NodeFilter
            .property("p_i64s")
            .temporal()
            .first()
            .avg()
            .eq(Prop::F64(0.0));
        let expected = vec!["n3", "n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_first_avg_f32s() {
        let filter = NodeFilter
            .property("p_f32s")
            .temporal()
            .first()
            .avg()
            .eq(Prop::F64(2.1666666666666665));
        let expected = vec!["n1", "n10", "n3", "n4"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_first_avg_f64s() {
        let filter = NodeFilter
            .property("p_f64s")
            .temporal()
            .first()
            .avg()
            .eq(Prop::F64(45.0));
        let expected = vec!["n1", "n10", "n3"];
        apply_assertion(filter, &expected);
    }

    // ------ Temporal first: MIN ------
    #[test]
    fn test_node_property_temporal_first_min_u8s() {
        let filter = NodeFilter
            .property("p_u8s")
            .temporal()
            .first()
            .min()
            .eq(Prop::U8(1));
        let expected = vec!["n1", "n10", "n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_first_min_u16s() {
        let filter = NodeFilter
            .property("p_u16s")
            .temporal()
            .first()
            .min()
            .eq(Prop::U16(1));
        let expected = vec!["n1", "n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_first_min_u32s() {
        let filter = NodeFilter
            .property("p_u32s")
            .temporal()
            .first()
            .min()
            .eq(Prop::U32(1));
        let expected = vec!["n1", "n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_first_min_u64s() {
        let filter = NodeFilter
            .property("p_u64s")
            .temporal()
            .first()
            .min()
            .eq(Prop::U64(1));
        let expected = vec!["n1", "n10", "n2", "n4", "n5"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_first_min_i32s() {
        let filter = NodeFilter
            .property("p_i32s")
            .temporal()
            .first()
            .min()
            .eq(Prop::I32(-2));
        let expected = vec!["n6"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_first_min_i64s() {
        let filter = NodeFilter
            .property("p_i64s")
            .temporal()
            .first()
            .min()
            .eq(Prop::I64(-3));
        let expected = vec!["n10", "n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_first_min_f32s() {
        let filter = NodeFilter
            .property("p_f32s")
            .temporal()
            .first()
            .min()
            .eq(Prop::F32(1.0));
        let expected = vec!["n1", "n10", "n3", "n4"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_first_min_f64s() {
        let filter = NodeFilter
            .property("p_f64s")
            .temporal()
            .first()
            .min()
            .eq(Prop::F64(30.0));
        let expected = vec!["n2", "n3"];
        apply_assertion(filter, &expected);
    }

    // ------ Temporal first: MAX ------
    #[test]
    fn test_node_property_temporal_first_max_u8s() {
        let filter = NodeFilter
            .property("p_u8s")
            .temporal()
            .first()
            .max()
            .eq(Prop::U8(3));
        let expected = vec!["n1", "n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_first_max_u16s() {
        let filter = NodeFilter
            .property("p_u16s")
            .temporal()
            .first()
            .max()
            .eq(Prop::U16(3));
        let expected = vec!["n1", "n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_first_max_u32s() {
        let filter = NodeFilter
            .property("p_u32s")
            .temporal()
            .first()
            .max()
            .eq(Prop::U32(3));
        let expected = vec!["n1", "n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_first_max_u64s() {
        let filter = NodeFilter
            .property("p_u64s")
            .temporal()
            .first()
            .max()
            .eq(Prop::U64(4));
        let expected = vec!["n2"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_first_max_i32s() {
        let filter = NodeFilter
            .property("p_i32s")
            .temporal()
            .first()
            .max()
            .eq(Prop::I32(3));
        let expected = vec!["n1", "n10", "n4", "n6"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_first_max_i64s() {
        let filter = NodeFilter
            .property("p_i64s")
            .temporal()
            .first()
            .max()
            .eq(Prop::I64(2));
        let expected = vec!["n10"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_first_max_f32s() {
        let filter = NodeFilter
            .property("p_f32s")
            .temporal()
            .first()
            .max()
            .eq(Prop::F32(3.5));
        let expected = vec!["n1", "n10", "n4"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_first_max_f64s() {
        let filter = NodeFilter
            .property("p_f64s")
            .temporal()
            .first()
            .max()
            .eq(Prop::F64(50.0));
        let expected = vec!["n1", "n10", "n2"];
        apply_assertion(filter, &expected);
    }

    // ------ Temporal first: LEN ------
    #[test]
    fn test_node_property_temporal_first_len_u8s() {
        let filter = NodeFilter
            .property("p_u8s")
            .temporal()
            .first()
            .len()
            .eq(Prop::U64(3));
        let expected = vec!["n1", "n10", "n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_first_len_u16s() {
        let filter = NodeFilter
            .property("p_u16s")
            .temporal()
            .first()
            .len()
            .eq(Prop::U64(3));
        let expected = vec!["n1", "n10", "n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_first_len_u32s() {
        let filter = NodeFilter
            .property("p_u32s")
            .temporal()
            .first()
            .len()
            .eq(Prop::U64(3));
        let expected = vec!["n1", "n10", "n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_first_len_u64s() {
        let filter = NodeFilter
            .property("p_u64s")
            .temporal()
            .first()
            .len()
            .eq(Prop::U64(4));
        let expected = vec!["n2"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_first_len_i32s() {
        let filter = NodeFilter
            .property("p_i32s")
            .temporal()
            .first()
            .len()
            .eq(Prop::U64(3));
        let expected = vec!["n1", "n10", "n3", "n4", "n6"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_first_len_i64s() {
        let filter = NodeFilter
            .property("p_i64s")
            .temporal()
            .first()
            .len()
            .eq(Prop::U64(2));
        let expected = vec!["n5"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_first_len_f32s() {
        let filter = NodeFilter
            .property("p_f32s")
            .temporal()
            .first()
            .len()
            .eq(Prop::U64(3));
        let expected = vec!["n1", "n10", "n3", "n4"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_first_len_f64s() {
        let filter = NodeFilter
            .property("p_f64s")
            .temporal()
            .first()
            .len()
            .eq(Prop::U64(2));
        let expected = vec!["n1", "n10", "n3"];
        apply_assertion(filter, &expected);
    }

    // ------ Temporal any: SUM ------
    #[test]
    fn test_node_property_temporal_any_sum_u8s() {
        let filter = NodeFilter
            .property("p_u8s")
            .temporal()
            .any()
            .sum()
            .eq(Prop::U64(6));
        let expected = vec!["n1", "n10", "n3"];
        apply_assertion(filter, &expected);

        let filter = NodeFilter
            .property("p_u8s")
            .temporal()
            .any()
            .sum()
            .eq(Prop::U64(10));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_any_sum_u16s() {
        let filter = NodeFilter
            .property("p_u16s")
            .temporal()
            .any()
            .sum()
            .eq(Prop::U64(6));
        let expected = vec!["n1", "n10", "n3"];
        apply_assertion(filter, &expected);

        let filter = NodeFilter
            .property("p_u16s")
            .temporal()
            .any()
            .sum()
            .eq(Prop::U64(10));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_any_sum_u32s() {
        let filter = NodeFilter
            .property("p_u32s")
            .temporal()
            .any()
            .sum()
            .eq(Prop::U64(6));
        let expected = vec!["n1", "n10", "n3"];
        apply_assertion(filter, &expected);

        let filter = NodeFilter
            .property("p_u32s")
            .temporal()
            .any()
            .sum()
            .eq(Prop::U64(10));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_any_sum_u64s() {
        let filter = NodeFilter
            .property("p_u64s")
            .temporal()
            .any()
            .sum()
            .eq(Prop::U64(6));
        let expected = vec!["n1", "n10", "n3", "n4"];
        apply_assertion(filter, &expected);

        let filter = NodeFilter
            .property("p_u64s")
            .temporal()
            .any()
            .sum()
            .eq(Prop::U64(10));
        let expected = vec!["n1", "n2"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_any_sum_i32s() {
        let filter = NodeFilter
            .property("p_i32s")
            .temporal()
            .any()
            .sum()
            .eq(Prop::I64(6));
        let expected = vec!["n1", "n10", "n3", "n4"];
        apply_assertion(filter, &expected);

        let filter = NodeFilter
            .property("p_i32s")
            .temporal()
            .any()
            .sum()
            .eq(Prop::I64(60));
        let expected = vec!["n4"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_any_sum_i64s() {
        let filter = NodeFilter
            .property("p_i64s")
            .temporal()
            .any()
            .sum()
            .eq(Prop::I64(0));
        let expected = vec!["n3", "n10"];
        apply_assertion(filter, &expected);

        let filter = NodeFilter
            .property("p_i64s")
            .temporal()
            .any()
            .sum()
            .eq(Prop::I64(10));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_any_sum_f32s() {
        let filter = NodeFilter
            .property("p_f32s")
            .temporal()
            .any()
            .sum()
            .eq(Prop::F64(6.5));
        let expected = vec!["n1", "n10", "n3", "n4"];
        apply_assertion(filter, &expected);

        let filter = NodeFilter
            .property("p_f32s")
            .temporal()
            .any()
            .sum()
            .eq(Prop::F64(60.0));
        let expected = vec!["n4"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_any_sum_f64s() {
        let filter = NodeFilter
            .property("p_f64s")
            .temporal()
            .any()
            .sum()
            .eq(Prop::F64(90.0));
        let expected = vec!["n1", "n10", "n3"];
        apply_assertion(filter, &expected);

        let filter = NodeFilter
            .property("p_f64s")
            .temporal()
            .any()
            .sum()
            .eq(Prop::F64(120.0));
        let expected = vec!["n1", "n2"];
        apply_assertion(filter, &expected);
    }

    // ------ Temporal any: AVG ------
    #[test]
    fn test_node_property_temporal_any_avg_u8s() {
        let filter = NodeFilter
            .property("p_u8s")
            .temporal()
            .any()
            .avg()
            .eq(Prop::F64(2.0));
        let expected = vec!["n1", "n10", "n3"];
        apply_assertion(filter, &expected);

        let filter = NodeFilter
            .property("p_u8s")
            .temporal()
            .any()
            .avg()
            .eq(Prop::F64(2.5));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_any_avg_u16s() {
        let filter = NodeFilter
            .property("p_u16s")
            .temporal()
            .any()
            .avg()
            .eq(Prop::F64(2.0));
        let expected = vec!["n1", "n10", "n3"];
        apply_assertion(filter, &expected);

        let filter = NodeFilter
            .property("p_u16s")
            .temporal()
            .any()
            .avg()
            .eq(Prop::F64(2.5));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_any_avg_u32s() {
        let filter = NodeFilter
            .property("p_u32s")
            .temporal()
            .any()
            .avg()
            .eq(Prop::F64(2.0));
        let expected = vec!["n1", "n10", "n3"];
        apply_assertion(filter, &expected);

        let filter = NodeFilter
            .property("p_u32s")
            .temporal()
            .any()
            .avg()
            .eq(Prop::F64(2.5));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_any_avg_u64s() {
        let filter = NodeFilter
            .property("p_u64s")
            .temporal()
            .any()
            .avg()
            .eq(Prop::F64(2.0));
        let expected = vec!["n1", "n10", "n3", "n4"];
        apply_assertion(filter, &expected);

        let filter = NodeFilter
            .property("p_u64s")
            .temporal()
            .any()
            .avg()
            .eq(Prop::F64(2.5));
        let expected = vec!["n1", "n2"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_any_avg_i32s() {
        let filter = NodeFilter
            .property("p_i32s")
            .temporal()
            .any()
            .avg()
            .eq(Prop::F64(2.0));
        let expected = vec!["n1", "n10", "n3", "n4"];
        apply_assertion(filter, &expected);

        let filter = NodeFilter
            .property("p_i32s")
            .temporal()
            .any()
            .avg()
            .eq(Prop::F64(2.5));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_any_avg_i64s() {
        let filter = NodeFilter
            .property("p_i64s")
            .temporal()
            .any()
            .avg()
            .eq(Prop::F64(0.0));
        let expected = vec!["n3", "n10"];
        apply_assertion(filter, &expected);

        let filter = NodeFilter
            .property("p_i64s")
            .temporal()
            .any()
            .avg()
            .eq(Prop::F64(2.5));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_any_avg_f32s() {
        let filter = NodeFilter
            .property("p_f32s")
            .temporal()
            .any()
            .avg()
            .eq(Prop::F64(2.1666666666666665));
        let expected = vec!["n1", "n10", "n3", "n4"];
        apply_assertion(filter, &expected);

        let filter = NodeFilter
            .property("p_f32s")
            .temporal()
            .any()
            .avg()
            .eq(Prop::F64(20.0));
        let expected = vec!["n4"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_any_avg_f64s() {
        let filter = NodeFilter
            .property("p_f64s")
            .temporal()
            .any()
            .avg()
            .eq(Prop::F64(45.0));
        let expected = vec!["n1", "n10", "n3"];
        apply_assertion(filter, &expected);

        let filter = NodeFilter
            .property("p_f64s")
            .temporal()
            .any()
            .avg()
            .eq(Prop::F64(40.0));
        let expected = vec!["n1", "n2"];
        apply_assertion(filter, &expected);
    }

    // ------ Temporal any: MIN ------
    #[test]
    fn test_node_property_temporal_any_min_u8s() {
        let filter = NodeFilter
            .property("p_u8s")
            .temporal()
            .any()
            .min()
            .eq(Prop::U8(1));
        let expected = vec!["n1", "n10", "n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_any_min_u16s() {
        let filter = NodeFilter
            .property("p_u16s")
            .temporal()
            .any()
            .min()
            .eq(Prop::U16(1));
        let expected = vec!["n1", "n10", "n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_any_min_u32s() {
        let filter = NodeFilter
            .property("p_u32s")
            .temporal()
            .any()
            .min()
            .eq(Prop::U32(1));
        let expected = vec!["n1", "n10", "n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_any_min_u64s() {
        let filter = NodeFilter
            .property("p_u64s")
            .temporal()
            .any()
            .min()
            .eq(Prop::U64(1));
        let expected = vec!["n1", "n10", "n2", "n3", "n4", "n5"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_any_min_i32s() {
        let filter = NodeFilter
            .property("p_i32s")
            .temporal()
            .any()
            .min()
            .eq(Prop::I32(-2));
        let expected = vec!["n6"];
        apply_assertion(filter, &expected);

        let filter = NodeFilter
            .property("p_i32s")
            .temporal()
            .any()
            .min()
            .eq(Prop::I32(10));
        let expected = vec!["n4"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_any_min_i64s() {
        let filter = NodeFilter
            .property("p_i64s")
            .temporal()
            .any()
            .min()
            .eq(Prop::I64(-3));
        let expected = vec!["n10", "n3"];
        apply_assertion(filter, &expected);

        let filter = NodeFilter
            .property("p_i64s")
            .temporal()
            .any()
            .min()
            .eq(Prop::I64(1));
        let expected = vec!["n1", "n5"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_any_min_f32s() {
        let filter = NodeFilter
            .property("p_f32s")
            .temporal()
            .any()
            .min()
            .eq(Prop::F32(1.0));
        let expected = vec!["n1", "n10", "n3", "n4"];
        apply_assertion(filter, &expected);

        let filter = NodeFilter
            .property("p_f32s")
            .temporal()
            .any()
            .min()
            .eq(Prop::F32(10.0));
        let expected = vec!["n4"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_any_min_f64s() {
        let filter = NodeFilter
            .property("p_f64s")
            .temporal()
            .any()
            .min()
            .eq(Prop::F64(30.0));
        let expected = vec!["n1", "n2", "n3"];
        apply_assertion(filter, &expected);

        let filter = NodeFilter
            .property("p_f64s")
            .temporal()
            .any()
            .min()
            .eq(Prop::F64(40.0));
        let expected = vec!["n1", "n10", "n3"];
        apply_assertion(filter, &expected);
    }

    // ------ Temporal any: MAX ------
    #[test]
    fn test_node_property_temporal_any_max_u8s() {
        let filter = NodeFilter
            .property("p_u8s")
            .temporal()
            .any()
            .max()
            .eq(Prop::U8(3));
        let expected = vec!["n1", "n10", "n3"];
        apply_assertion(filter, &expected);

        let filter = NodeFilter
            .property("p_u8s")
            .temporal()
            .any()
            .max()
            .eq(Prop::U8(4));
        let expected = vec!["n1", "n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_any_max_u16s() {
        let filter = NodeFilter
            .property("p_u16s")
            .temporal()
            .any()
            .max()
            .eq(Prop::U16(3));
        let expected = vec!["n1", "n10", "n3"];
        apply_assertion(filter, &expected);

        let filter = NodeFilter
            .property("p_u16s")
            .temporal()
            .any()
            .max()
            .eq(Prop::U16(4));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_any_max_u32s() {
        let filter = NodeFilter
            .property("p_u32s")
            .temporal()
            .any()
            .max()
            .eq(Prop::U32(3));
        let expected = vec!["n1", "n10", "n3"];
        apply_assertion(filter, &expected);

        let filter = NodeFilter
            .property("p_u32s")
            .temporal()
            .any()
            .max()
            .eq(Prop::U32(4));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_any_max_u64s() {
        let filter = NodeFilter
            .property("p_u64s")
            .temporal()
            .any()
            .max()
            .eq(Prop::U64(4));
        let expected = vec!["n1", "n2"];
        apply_assertion(filter, &expected);

        let filter = NodeFilter
            .property("p_u64s")
            .temporal()
            .any()
            .max()
            .eq(Prop::U64(3));
        let expected = vec!["n1", "n10", "n3", "n4"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_any_max_i32s() {
        let filter = NodeFilter
            .property("p_i32s")
            .temporal()
            .any()
            .max()
            .eq(Prop::I32(3));
        let expected = vec!["n1", "n10", "n3", "n4", "n6"];
        apply_assertion(filter, &expected);

        let filter = NodeFilter
            .property("p_i32s")
            .temporal()
            .any()
            .max()
            .eq(Prop::I32(30));
        let expected = vec!["n4"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_any_max_i64s() {
        let filter = NodeFilter
            .property("p_i64s")
            .temporal()
            .any()
            .max()
            .eq(Prop::I64(2));
        let expected = vec!["n10", "n3"];
        apply_assertion(filter, &expected);

        let filter = NodeFilter
            .property("p_i64s")
            .temporal()
            .any()
            .max()
            .eq(Prop::I64(2));
        let expected = vec!["n10", "n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_any_max_f32s() {
        let filter = NodeFilter
            .property("p_f32s")
            .temporal()
            .any()
            .max()
            .eq(Prop::F32(3.5));
        let expected = vec!["n1", "n10", "n3", "n4"];
        apply_assertion(filter, &expected);

        let filter = NodeFilter
            .property("p_f32s")
            .temporal()
            .any()
            .max()
            .eq(Prop::F32(30.0));
        let expected = vec!["n4"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_any_max_f64s() {
        let filter = NodeFilter
            .property("p_f64s")
            .temporal()
            .any()
            .max()
            .eq(Prop::F64(50.0));
        let expected = vec!["n1", "n10", "n2", "n3"];
        apply_assertion(filter, &expected);
    }

    // ------ Temporal any: LEN ------
    #[test]
    fn test_node_property_temporal_any_len_u8s() {
        let filter = NodeFilter
            .property("p_u8s")
            .temporal()
            .any()
            .len()
            .is_in(vec![Prop::U64(3)]);
        let expected = vec!["n1", "n10", "n3"];
        apply_assertion(filter, &expected);

        let filter = NodeFilter
            .property("p_u8s")
            .temporal()
            .any()
            .len()
            .eq(Prop::U64(4));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_any_len_u16s() {
        let filter = NodeFilter
            .property("p_u16s")
            .temporal()
            .any()
            .len()
            .eq(Prop::U64(3));
        let expected = vec!["n1", "n10", "n3"];
        apply_assertion(filter, &expected);

        let filter = NodeFilter
            .property("p_u16s")
            .temporal()
            .any()
            .len()
            .eq(Prop::U64(4));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_any_len_u32s() {
        let filter = NodeFilter
            .property("p_u32s")
            .temporal()
            .any()
            .len()
            .eq(Prop::U64(3));
        let expected = vec!["n1", "n10", "n3"];
        apply_assertion(filter, &expected);

        let filter = NodeFilter
            .property("p_u32s")
            .temporal()
            .any()
            .len()
            .eq(Prop::U64(4));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_any_len_u64s() {
        let filter = NodeFilter
            .property("p_u64s")
            .temporal()
            .any()
            .len()
            .eq(Prop::U64(4));
        let expected = vec!["n1", "n2"];
        apply_assertion(filter, &expected);

        let filter = NodeFilter
            .property("p_u64s")
            .temporal()
            .any()
            .len()
            .eq(Prop::U64(3));
        let expected = vec!["n1", "n10", "n3", "n4"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_any_len_i32s() {
        let filter = NodeFilter
            .property("p_i32s")
            .temporal()
            .any()
            .len()
            .eq(Prop::U64(3));
        let expected = vec!["n1", "n10", "n3", "n4", "n6"];
        apply_assertion(filter, &expected);

        let filter = NodeFilter
            .property("p_i32s")
            .temporal()
            .any()
            .len()
            .eq(Prop::U64(4));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_any_len_i64s() {
        let filter = NodeFilter
            .property("p_i64s")
            .temporal()
            .any()
            .len()
            .eq(Prop::U64(2));
        let expected = vec!["n5"];
        apply_assertion(filter, &expected);

        let filter = NodeFilter
            .property("p_i64s")
            .temporal()
            .any()
            .len()
            .eq(Prop::U64(4));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_any_len_f32s() {
        let filter = NodeFilter
            .property("p_f32s")
            .temporal()
            .any()
            .len()
            .eq(Prop::U64(3));
        let expected = vec!["n1", "n10", "n3", "n4"];
        apply_assertion(filter, &expected);

        let filter = NodeFilter
            .property("p_f32s")
            .temporal()
            .any()
            .len()
            .eq(Prop::U64(4));
        let expected = vec!["n1"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_property_temporal_any_len_f64s() {
        let filter = NodeFilter
            .property("p_f64s")
            .temporal()
            .any()
            .len()
            .eq(Prop::U64(2));
        let expected = vec!["n1", "n10", "n3"];
        apply_assertion(filter, &expected);

        let filter = NodeFilter
            .property("p_f64s")
            .temporal()
            .any()
            .len()
            .eq(Prop::U64(3));
        let expected = vec!["n1", "n2"];
        apply_assertion(filter, &expected);
    }

    // ------ EMPTY LISTS ------
    #[test]
    fn test_empty_list_agg() {
        let filter = NodeFilter.property("p_u64s").sum().eq(Prop::U64(0));
        let expected: Vec<&str> = vec![];
        apply_assertion(filter, &expected);

        let filter = NodeFilter.property("p_u64s").avg().eq(Prop::F64(0.0));
        let expected: Vec<&str> = vec![];
        apply_assertion(filter, &expected);

        let filter = NodeFilter
            .property("p_u64s")
            .temporal()
            .last()
            .min()
            .eq(Prop::U64(0));
        let expected: Vec<&str> = vec![];
        apply_assertion(filter, &expected);

        let filter = NodeFilter
            .property("p_u64s")
            .temporal()
            .first()
            .max()
            .eq(Prop::U64(0));
        let expected: Vec<&str> = vec![];
        apply_assertion(filter, &expected);

        let filter = NodeFilter.property("p_u64s").len().eq(Prop::U64(0));
        let expected: Vec<&str> = vec!["n7"];
        apply_assertion(filter, &expected);

        let filter = NodeFilter.metadata("p_u64s").len().eq(Prop::U64(0));
        let expected: Vec<&str> = vec!["n6"];
        apply_assertion(filter, &expected);
    }

    // ------ Unsupported filter operations ------
    #[test]
    fn test_unsupported_filter_ops_agg() {
        let filter = NodeFilter.property("p_u64s").sum().starts_with("abc");
        let expected: &str = "Operator STARTS_WITH is not supported with list aggregation";
        apply_assertion_err(filter, expected);

        let filter = NodeFilter.property("p_u64s").avg().ends_with("abc");
        let expected: &str = "Operator ENDS_WITH is not supported with list aggregation";
        apply_assertion_err(filter, expected);

        let filter = NodeFilter.property("p_u64s").min().is_none();
        let expected: &str = "Operator IS_NONE is not supported with list aggregation";
        apply_assertion_err(filter, expected);

        let filter = NodeFilter.property("p_u64s").max().is_some();
        let expected: &str = "Operator IS_SOME is not supported with list aggregation";
        apply_assertion_err(filter, expected);

        let filter = NodeFilter.property("p_u64s").len().contains("abc");
        let expected: &str = "Operator CONTAINS is not supported with list aggregation";
        apply_assertion_err(filter, expected);

        let filter = NodeFilter.property("p_u64s").sum().not_contains("abc");
        let expected: &str = "Operator NOT_CONTAINS is not supported with list aggregation";
        apply_assertion_err(filter, expected);
    }

    // --------------- OVERFLOW HANDLING ---------------
    #[test]
    fn test_max_value_agg() {
        let filter = NodeFilter
            .property("p_u64s_max")
            .max()
            .eq(Prop::U64(u64::MAX));
        let expected: Vec<&str> = vec!["n5", "n1"];
        apply_assertion(filter, &expected);

        let filter = NodeFilter
            .property("p_u64s_min")
            .min()
            .eq(Prop::U64(u64::MIN));
        let expected: Vec<&str> = vec!["n5"];
        apply_assertion(filter, &expected);

        let filter = NodeFilter.property("p_u8s_max").sum().eq(Prop::U64(510));
        let expected: Vec<&str> = vec!["n1"];
        apply_assertion(filter, &expected);

        let filter = NodeFilter
            .property("p_u16s_max")
            .sum()
            .eq(Prop::U64(131070));
        let expected: Vec<&str> = vec!["n1"];
        apply_assertion(filter, &expected);

        let filter = NodeFilter
            .property("p_u32s_max")
            .sum()
            .eq(Prop::U64(8589934590));
        let expected: Vec<&str> = vec!["n1"];
        apply_assertion(filter, &expected);

        let filter = NodeFilter.property("p_u64s_max").sum().gt(Prop::U64(0));
        let expected: Vec<&str> = vec!["n1", "n5"];
        apply_assertion(filter, &expected);

        // AVG is computed in f64 even if SUM overflowed.
        let avg = (u64::MAX as f64 + 1.0) / 2.0;
        let filter = NodeFilter.property("p_u64s_max").avg().eq(avg);
        let expected = vec!["n5"];
        apply_assertion(filter, &expected);

        // Overflow is handled by promoting to Decimal which still compares
        let filter = NodeFilter.property("p_i64s_max").sum().gt(Prop::I64(0));
        let expected: Vec<&str> = vec!["n5"];
        apply_assertion(filter, &expected);

        // AVG is computed in f64 even if SUM overflowed.
        let avg = (i64::MAX as f64 + 1.0) / 2.0;
        let filter = NodeFilter.property("p_i64s_max").avg().eq(avg);
        let expected = vec!["n5"];
        apply_assertion(filter, &expected);
    }

    // ------ Property: any ------
    #[test]
    fn test_node_property_any() {
        let filter = NodeFilter.property("p_u8s").any().eq(Prop::U8(3));
        let expected = vec!["n1", "n10", "n3"];
        apply_assertion(filter, &expected);
    }

    // ------ Property: all ------
    #[test]
    fn test_node_property_all() {
        let filter = NodeFilter
            .property("p_bools_all")
            .all()
            .eq(Prop::Bool(true));
        let expected = vec!["n10", "n4"];
        apply_assertion(filter, &expected);
    }

    // ------ Metadata: any ------
    #[test]
    fn test_node_metadata_any() {
        let filter = NodeFilter.metadata("p_u64s").any().eq(Prop::U64(1));
        let expected = vec!["n10", "n7"];
        apply_assertion(filter, &expected);
    }

    // ------ Metadata: all ------
    #[test]
    fn test_node_metadata_all() {
        let filter = NodeFilter.metadata("p_strs").all().eq("a");
        let expected = vec!["n6", "n7"];
        apply_assertion(filter, &expected);
    }

    // ------ Temporal First: any ------
    #[test]
    fn test_node_temporal_property_first_any() {
        let filter = NodeFilter
            .property("p_bools")
            .temporal()
            .first()
            .any()
            .eq(false);
        let expected = vec!["n1", "n10", "n2", "n3", "n4"];
        apply_assertion(filter, &expected);
    }

    // ------ Temporal First: all ------
    #[test]
    fn test_node_temporal_property_first_all() {
        let filter = NodeFilter
            .property("p_bools_all")
            .temporal()
            .first()
            .all()
            .eq(true);
        let expected = vec!["n10", "n4"];
        apply_assertion(filter, &expected);
    }

    // ------ Temporal last: any ------
    #[test]
    fn test_node_temporal_property_last_any() {
        let filter = NodeFilter
            .property("p_f32s")
            .temporal()
            .last()
            .any()
            .eq(Prop::F32(3.5));
        let expected = vec!["n1", "n10", "n3"];
        apply_assertion(filter, &expected);
    }

    // ------ Temporal last: all ------
    #[test]
    fn test_node_temporal_property_last_all() {
        let filter = NodeFilter
            .property("p_bools_all")
            .temporal()
            .last()
            .all()
            .eq(true);
        let expected = vec!["n10", "n4"];
        apply_assertion(filter, &expected);
    }

    // ------ Temporal Any: any ------
    #[test]
    fn test_node_temporal_property_any_any() {
        let filter = NodeFilter
            .property("p_f32s")
            .temporal()
            .any()
            .any()
            .eq(Prop::F32(3.5));
        let expected = vec!["n1", "n10", "n3", "n4"];
        apply_assertion(filter, &expected);

        let filter = NodeFilter
            .property("p_f32s")
            .temporal()
            .any()
            .any()
            .eq(Prop::F32(30.0));
        let expected = vec!["n4"];
        apply_assertion(filter, &expected);
    }

    // ------ Temporal Any: all ------
    #[test]
    fn test_node_temporal_property_any_all() {
        let filter = NodeFilter
            .property("p_bools")
            .temporal()
            .any()
            .all()
            .eq(false);
        let expected = vec!["n2", "n4"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_nested_list_property_all_all_all_any() {
        let filter = NodeFilter
            .property("nested_list")
            .all()
            .all()
            .all()
            .any()
            .gt(45.0);

        let expected = vec!["n1", "n3"];
        apply_assertion(filter, &expected);
    }

    #[test]
    fn test_node_nested_list_temporal_property_all_all_all_all_any() {
        let filter = NodeFilter
            .property("nested_list")
            .temporal()
            .all()
            .all()
            .all()
            .all()
            .any()
            .gt(45.0);

        let expected = vec!["n3"];
        apply_assertion(filter, &expected);
    }

    // ------ Temporal All: any ------
    #[test]
    fn test_node_temporal_property_all_any() {
        let filter = NodeFilter
            .property("p_bools")
            .temporal()
            .all()
            .any()
            .eq(true);
        let expected = vec!["n1", "n10", "n3"];
        apply_assertion(filter, &expected);
    }

    // ------ Temporal All: all ------
    #[test]
    fn test_node_temporal_property_all_all() {
        let filter = NodeFilter
            .property("p_bools_all")
            .temporal()
            .all()
            .all()
            .eq(true);
        let expected = vec!["n4", "n10"];
        apply_assertion(filter, &expected);
    }
}

mod test_edge_filter {
    use crate::filter_tests::test_filters::{
        init_edges_graph, init_edges_graph_with_num_ids, init_edges_graph_with_str_ids,
        init_edges_graph_with_str_ids_del, init_nodes_graph, IdentityGraphTransformer,
    };
    use raphtory::db::graph::views::filter::model::{
        edge_filter::EdgeFilter,
        node_filter::ops::{NodeFilterOps, NodeIdFilterOps},
        property_filter::ops::{ListAggOps, PropertyFilterOps},
        ComposableFilter, EdgeViewFilterOps, PropertyFilterFactory, TemporalPropertyFilterFactory,
        ViewWrapOps,
    };
    use raphtory_tests::assertions::{
        assert_filter_edges_results, assert_select_edges_results, TestGraphVariants, TestVariants,
    };

    #[test]
    fn test_filter_edges_src_property_eq() {
        let filter = EdgeFilter::src().property("p10").eq("Paper_airplane");
        let expected_results = vec!["1->2", "3->1"];
        let g = |g| init_edges_graph(init_nodes_graph(g));
        assert_filter_edges_results(
            g,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_src_property_temporal_eq() {
        let filter = EdgeFilter::src()
            .property("p30")
            .temporal()
            .first()
            .eq("Old_boat");
        let expected_results = vec!["2->1", "2->3"];
        let g = |g| init_edges_graph(init_nodes_graph(g));
        assert_filter_edges_results(
            g,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_src_metadata_eq() {
        let filter = EdgeFilter::src().metadata("m1").eq("pometry");
        let expected_results = vec!["1->2"];
        let g = |g| init_edges_graph(init_nodes_graph(g));
        assert_filter_edges_results(
            g,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_src_eq() {
        let filter = EdgeFilter::src().name().eq("3");
        let expected_results = vec!["3->1"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_src_ne() {
        let filter = EdgeFilter::src().name().ne("1");
        let expected_results = vec![
            "2->1",
            "2->3",
            "3->1",
            "David Gilmour->John Mayer",
            "John Mayer->Jimmy Page",
        ];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_src_in() {
        let filter = EdgeFilter::src().name().is_in(vec!["1"]);
        let expected_results = vec!["1->2"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter::src().name().is_in(vec!["1", "2"]);
        let expected_results = vec!["1->2", "2->1", "2->3"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_src_not_in() {
        let filter = EdgeFilter::src().name().is_not_in(vec!["1"]);
        let expected_results = vec![
            "2->1",
            "2->3",
            "3->1",
            "David Gilmour->John Mayer",
            "John Mayer->Jimmy Page",
        ];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_dst_eq() {
        let filter = EdgeFilter::dst().name().eq("2");
        let expected_results = vec!["1->2"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_dst_ne() {
        let filter = EdgeFilter::dst().name().ne("2");
        let expected_results = vec![
            "2->1",
            "2->3",
            "3->1",
            "David Gilmour->John Mayer",
            "John Mayer->Jimmy Page",
        ];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_dst_in() {
        let filter = EdgeFilter::dst().name().is_in(vec!["2"]);
        let expected_results = vec!["1->2"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter::dst().name().is_in(vec!["2", "3"]);
        let expected_results = vec!["1->2", "2->3"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_dst_not_in() {
        let filter = EdgeFilter::dst().name().is_not_in(vec!["1"]);
        let expected_results = vec![
            "1->2",
            "2->3",
            "David Gilmour->John Mayer",
            "John Mayer->Jimmy Page",
        ];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_src_dst_starts_with() {
        let filter = EdgeFilter::src().name().starts_with("Joh");
        let expected_results: Vec<&str> = vec!["John Mayer->Jimmy Page"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter::src().name().starts_with("Joker");
        let expected_results: Vec<&str> = vec![];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter::dst().name().starts_with("Jimmy");
        let expected_results: Vec<&str> = vec!["John Mayer->Jimmy Page"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter::dst().name().starts_with("Tango");
        let expected_results: Vec<&str> = vec![];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_src_dst_ends_with() {
        let filter = EdgeFilter::src().name().ends_with("Mayer");
        let expected_results: Vec<&str> = vec!["John Mayer->Jimmy Page"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter::src().name().ends_with("Cruise");
        let expected_results: Vec<&str> = vec![];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter::dst().name().ends_with("Page");
        let expected_results: Vec<&str> = vec!["John Mayer->Jimmy Page"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter::dst().name().ends_with("Cruise");
        let expected_results: Vec<&str> = vec![];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_src_contains() {
        let filter = EdgeFilter::src().name().contains("Mayer");
        let expected_results: Vec<&str> = vec!["John Mayer->Jimmy Page"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_src_contains_not() {
        let filter = EdgeFilter::src().name().not_contains("Mayer");
        let expected_results: Vec<&str> =
            vec!["1->2", "2->1", "2->3", "3->1", "David Gilmour->John Mayer"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_fuzzy_search() {
        let filter = EdgeFilter::src().name().fuzzy_search("John", 2, true);
        let expected_results: Vec<&str> = vec!["John Mayer->Jimmy Page"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter::src().name().fuzzy_search("John", 2, false);
        let expected_results: Vec<&str> = vec![];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter::src().name().fuzzy_search("John May", 2, false);
        let expected_results: Vec<&str> = vec!["John Mayer->Jimmy Page"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_not_src() {
        let filter = EdgeFilter::src().name().is_not_in(vec!["1"]).not();
        let expected_results = vec!["1->2"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_src_id_eq() {
        let filter = EdgeFilter::src().id().eq("3");
        let expected_results = vec!["3->1"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter::src().id().eq(3);
        let expected_results = vec!["3->1"];
        assert_filter_edges_results(
            init_edges_graph_with_num_ids,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_dst_id_eq() {
        let filter = EdgeFilter::dst().id().eq("3");
        let expected_results = vec!["2->3"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter::dst().id().eq(3);
        let expected_results = vec!["2->3"];
        assert_filter_edges_results(
            init_edges_graph_with_num_ids,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_src_id_ne() {
        let filter = EdgeFilter::src().id().ne("3");
        let expected_results = vec![
            "1->2",
            "2->1",
            "2->3",
            "David Gilmour->John Mayer",
            "John Mayer->Jimmy Page",
        ];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter::src().id().ne(3);
        let expected_results = vec!["1->2", "2->1", "2->3"];
        assert_filter_edges_results(
            init_edges_graph_with_num_ids,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_dst_id_ne() {
        let filter = EdgeFilter::dst().id().ne("3");
        let expected_results = vec![
            "1->2",
            "2->1",
            "3->1",
            "David Gilmour->John Mayer",
            "John Mayer->Jimmy Page",
        ];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter::dst().id().ne(3);
        let expected_results = vec!["1->2", "2->1", "3->1"];
        assert_filter_edges_results(
            init_edges_graph_with_num_ids,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_id_src_is_in() {
        let filter = EdgeFilter::src().id().is_in(vec!["3"]);
        let expected_results = vec!["3->1"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter::src().id().is_in(vec![3]);
        let expected_results = vec!["3->1"];
        assert_filter_edges_results(
            init_edges_graph_with_num_ids,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_id_dst_is_in() {
        let filter = EdgeFilter::dst().id().is_in(vec!["3"]);
        let expected_results = vec!["2->3"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter::dst().id().is_in(vec![3]);
        let expected_results = vec!["2->3"];
        assert_filter_edges_results(
            init_edges_graph_with_num_ids,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_id_src_is_not_in() {
        let filter = EdgeFilter::src().id().is_not_in(vec!["3"]);
        let expected_results = vec![
            "1->2",
            "2->1",
            "2->3",
            "David Gilmour->John Mayer",
            "John Mayer->Jimmy Page",
        ];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter::src().id().is_not_in(vec![3]);
        let expected_results = vec!["1->2", "2->1", "2->3"];
        assert_filter_edges_results(
            init_edges_graph_with_num_ids,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_id_dst_is_not_in() {
        let filter = EdgeFilter::dst().id().is_not_in(vec!["3"]);
        let expected_results = vec![
            "1->2",
            "2->1",
            "3->1",
            "David Gilmour->John Mayer",
            "John Mayer->Jimmy Page",
        ];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter::dst().id().is_not_in(vec![3]);
        let expected_results = vec!["1->2", "2->1", "3->1"];
        assert_filter_edges_results(
            init_edges_graph_with_num_ids,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_src_id_lt() {
        let filter = EdgeFilter::src().id().lt(3);
        let expected_results = vec!["1->2", "2->1", "2->3"];
        assert_filter_edges_results(
            init_edges_graph_with_num_ids,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_dst_id_lt() {
        let filter = EdgeFilter::dst().id().lt(3);
        let expected_results = vec!["1->2", "2->1", "3->1"];
        assert_filter_edges_results(
            init_edges_graph_with_num_ids,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_src_id_le() {
        let filter = EdgeFilter::src().id().le(3);
        let expected_results = vec!["1->2", "2->1", "2->3", "3->1"];
        assert_filter_edges_results(
            init_edges_graph_with_num_ids,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_dst_id_le() {
        let filter = EdgeFilter::dst().id().le(3);
        let expected_results = vec!["1->2", "2->1", "2->3", "3->1"];
        assert_filter_edges_results(
            init_edges_graph_with_num_ids,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_src_id_gt() {
        let filter = EdgeFilter::src().id().gt(1);
        let expected_results = vec!["2->1", "2->3", "3->1"];
        assert_filter_edges_results(
            init_edges_graph_with_num_ids,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_dst_id_gt() {
        let filter = EdgeFilter::dst().id().gt(1);
        let expected_results = vec!["1->2", "2->3"];
        assert_filter_edges_results(
            init_edges_graph_with_num_ids,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_src_id_ge() {
        let filter = EdgeFilter::src().id().ge(1);
        let expected_results = vec!["1->2", "2->1", "2->3", "3->1"];
        assert_filter_edges_results(
            init_edges_graph_with_num_ids,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_dst_id_ge() {
        let filter = EdgeFilter::dst().id().ge(1);
        let expected_results = vec!["1->2", "2->1", "2->3", "3->1"];
        assert_filter_edges_results(
            init_edges_graph_with_num_ids,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_src_name_starts_with() {
        let filter = EdgeFilter::src().name().starts_with("Tw");
        let expected_results = vec!["Two->One", "Two->Three"];
        assert_filter_edges_results(
            init_edges_graph_with_str_ids,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_src_id_ends_with() {
        let filter = EdgeFilter::src().id().ends_with("don");
        let expected_results = vec!["London->Paris"];
        assert_filter_edges_results(
            init_edges_graph_with_str_ids,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_src_id_contains() {
        let filter = EdgeFilter::src().id().contains("don");
        let expected_results = vec!["London->Paris"];
        assert_filter_edges_results(
            init_edges_graph_with_str_ids,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_dst_id_contains() {
        let filter = EdgeFilter::dst().id().contains("Par");
        let expected_results = vec!["London->Paris"];
        assert_filter_edges_results(
            init_edges_graph_with_str_ids,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_dst_name_not_contains() {
        let filter = EdgeFilter::dst().name().not_contains("Par");
        let expected_results = vec![
            "David Gilmour->John Mayer",
            "John Mayer->Jimmy Page",
            "Three->One",
            "Two->One",
            "Two->Three",
        ];
        assert_filter_edges_results(
            init_edges_graph_with_str_ids,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_src_id_is_in() {
        let filter = EdgeFilter::src().id().is_in(["Two"]);
        let expected_results = vec!["Two->One", "Two->Three"];
        assert_filter_edges_results(
            init_edges_graph_with_str_ids,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_dst_id_is_not_in() {
        let filter = EdgeFilter::dst().id().is_not_in(["One"]);
        let expected_results = vec![
            "David Gilmour->John Mayer",
            "John Mayer->Jimmy Page",
            "London->Paris",
            "Two->Three",
        ];
        assert_filter_edges_results(
            init_edges_graph_with_str_ids,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_is_active_edge_window() {
        let filter = EdgeFilter.window(1, 3).is_active();
        let expected_results = vec!["London->Paris", "Two->Three"];
        assert_filter_edges_results(
            init_edges_graph_with_str_ids_del,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_is_active_edge_after() {
        let filter = EdgeFilter.after(3).is_active();
        let expected_results = vec![
            "Bangalore->Bangalore",
            "David Gilmour->John Mayer",
            "John Mayer->Jimmy Page",
        ];
        assert_filter_edges_results(
            init_edges_graph_with_str_ids_del,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_is_active_edge_before() {
        let filter = EdgeFilter.before(3).is_active();
        let expected_results = vec!["London->Paris", "Two->Three"];
        assert_filter_edges_results(
            init_edges_graph_with_str_ids_del,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_is_active_edge_snapshot_latest() {
        let filter = EdgeFilter.snapshot_latest().is_active();
        let expected_results = vec!["Bangalore->Bangalore"];
        assert_filter_edges_results(
            init_edges_graph_with_str_ids_del,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_is_valid_edge_window() {
        let filter = EdgeFilter.window(1, 3).is_valid();
        let expected_results = vec!["London->Paris", "Two->Three"];
        assert_filter_edges_results(
            init_edges_graph_with_str_ids_del,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestGraphVariants::PersistentGraph,
        );
        assert_select_edges_results(
            init_edges_graph_with_str_ids_del,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestGraphVariants::PersistentGraph,
        );

        let filter = EdgeFilter.window(1, 4).is_valid();
        let expected_results = vec!["Three->One", "Two->One", "Two->Three"];
        assert_filter_edges_results(
            init_edges_graph_with_str_ids_del,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestGraphVariants::PersistentGraph,
        );
        assert_select_edges_results(
            init_edges_graph_with_str_ids_del,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestGraphVariants::PersistentGraph,
        );
    }

    #[test]
    fn test_is_valid_edge_snapshot_at() {
        let filter = EdgeFilter.snapshot_at(2).is_valid();
        let expected_results = vec!["London->Paris", "Two->Three"];
        assert_filter_edges_results(
            init_edges_graph_with_str_ids_del,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestGraphVariants::PersistentGraph,
        );
        assert_select_edges_results(
            init_edges_graph_with_str_ids_del,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestGraphVariants::PersistentGraph,
        );

        let filter = EdgeFilter.snapshot_at(3).is_valid();
        let expected_results = vec!["Three->One", "Two->One", "Two->Three"];
        assert_filter_edges_results(
            init_edges_graph_with_str_ids_del,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestGraphVariants::PersistentGraph,
        );
        assert_select_edges_results(
            init_edges_graph_with_str_ids_del,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestGraphVariants::PersistentGraph,
        );
    }

    #[test]
    fn test_is_valid_edge_snapshot_latest() {
        let filter = EdgeFilter.snapshot_latest().is_valid();
        let expected_results = vec![
            "Bangalore->Bangalore",
            "David Gilmour->John Mayer",
            "John Mayer->Jimmy Page",
            "Three->One",
            "Two->One",
            "Two->Three",
        ];
        assert_filter_edges_results(
            init_edges_graph_with_str_ids_del,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestGraphVariants::PersistentGraph,
        );
        assert_select_edges_results(
            init_edges_graph_with_str_ids_del,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestGraphVariants::PersistentGraph,
        );
    }

    // Disk graph doesn't support deletions
    #[test]
    fn test_is_deleted_edge_after() {
        let filter = EdgeFilter.after(1).is_deleted();
        let expected_results = vec!["London->Paris"];
        assert_filter_edges_results(
            init_edges_graph_with_str_ids_del,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_select_edges_results(
            init_edges_graph_with_str_ids_del,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_is_deleted_edge_before() {
        let filter = EdgeFilter.before(4).is_deleted();
        let expected_results = vec!["London->Paris"];
        assert_filter_edges_results(
            init_edges_graph_with_str_ids_del,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_select_edges_results(
            init_edges_graph_with_str_ids_del,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_is_self_loop_edge_window() {
        // window has no effect on is_self_loop and because we are using an `EdgeFilter` as the
        // entrypoint, the window is only applied to the edges, not the graph
        let filter = EdgeFilter.window(1, 3).is_self_loop();
        let expected_results_self_loop = vec!["Bangalore->Bangalore"];
        assert_filter_edges_results(
            init_edges_graph_with_str_ids_del,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results_self_loop,
            TestVariants::All,
        );

        // window doesn't make a difference for `is_self_loop`
        assert_select_edges_results(
            init_edges_graph_with_str_ids_del,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results_self_loop,
            TestVariants::All,
        );

        let filter = EdgeFilter.window(1, 6).is_self_loop();
        assert_filter_edges_results(
            init_edges_graph_with_str_ids_del,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results_self_loop,
            TestVariants::All,
        );
        assert_select_edges_results(
            init_edges_graph_with_str_ids_del,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results_self_loop,
            TestVariants::All,
        );
    }
}

mod test_edge_property_filter {
    use crate::filter_tests::test_filters::{
        init_edges_graph, init_edges_graph2, IdentityGraphTransformer,
    };
    use raphtory::db::graph::views::filter::model::{
        edge_filter::EdgeFilter,
        property_filter::ops::{ElemQualifierOps, ListAggOps, PropertyFilterOps},
        ComposableFilter, PropertyFilterFactory, TemporalPropertyFilterFactory, ViewWrapOps,
    };

    use raphtory_api::core::entities::properties::prop::Prop;
    use raphtory_tests::assertions::{assert_filter_edges_results, TestVariants};

    #[test]
    fn test_filter_edges_for_property_eq() {
        let filter = EdgeFilter.property("p2").eq(2u64);
        let expected_results = vec!["2->3"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter.property("p30").temporal().first().eq("Old_boat");
        let expected_results = vec!["2->3"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter.property("p20").temporal().all().eq("Gold_ship");
        let expected_results = vec!["1->2"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_property_ne() {
        let filter = EdgeFilter.property("p2").ne(2u64);
        let expected_results = vec![
            "1->2",
            "2->1",
            "3->1",
            "David Gilmour->John Mayer",
            "John Mayer->Jimmy Page",
        ];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter.property("p30").temporal().first().ne("Old_boat");
        let expected_results = vec!["1->2"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter.property("p30").temporal().all().ne("Classic");
        let expected_results = vec!["1->2", "2->3"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_property_lt() {
        let filter = EdgeFilter.property("p2").lt(10u64);
        let expected_results = vec![
            "1->2",
            "2->1",
            "2->3",
            "3->1",
            "David Gilmour->John Mayer",
            "John Mayer->Jimmy Page",
        ];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter.property("p2").temporal().first().lt(5u64);
        let expected_results = vec!["1->2", "2->3"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter.property("p2").temporal().all().lt(10u64);
        let expected_results = vec![
            "1->2",
            "2->1",
            "2->3",
            "3->1",
            "David Gilmour->John Mayer",
            "John Mayer->Jimmy Page",
        ];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_property_le() {
        let filter = EdgeFilter.property("p2").le(6u64);
        let expected_results = vec![
            "1->2",
            "2->1",
            "2->3",
            "3->1",
            "David Gilmour->John Mayer",
            "John Mayer->Jimmy Page",
        ];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter.property("p2").temporal().first().le(3u64);
        let expected_results = vec!["2->3"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter.property("p2").temporal().all().le(5u64);
        let expected_results = vec!["1->2", "2->3"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_property_gt() {
        let filter = EdgeFilter.property("p2").gt(2u64);
        let expected_results = vec![
            "1->2",
            "2->1",
            "3->1",
            "David Gilmour->John Mayer",
            "John Mayer->Jimmy Page",
        ];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter.property("p2").temporal().first().gt(5u64);
        let expected_results = vec![
            "2->1",
            "3->1",
            "David Gilmour->John Mayer",
            "John Mayer->Jimmy Page",
        ];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter.property("p2").temporal().all().gt(5u64);
        let expected_results = vec![
            "2->1",
            "3->1",
            "David Gilmour->John Mayer",
            "John Mayer->Jimmy Page",
        ];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_property_ge() {
        let filter = EdgeFilter.property("p2").ge(2u64);
        let expected_results = vec![
            "1->2",
            "2->1",
            "2->3",
            "3->1",
            "David Gilmour->John Mayer",
            "John Mayer->Jimmy Page",
        ];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter.property("p2").temporal().first().ge(6u64);
        let expected_results = vec![
            "2->1",
            "3->1",
            "David Gilmour->John Mayer",
            "John Mayer->Jimmy Page",
        ];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter.property("p2").temporal().all().ge(6u64);
        let expected_results = vec![
            "2->1",
            "3->1",
            "David Gilmour->John Mayer",
            "John Mayer->Jimmy Page",
        ];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_property_in() {
        let filter = EdgeFilter.property("p2").is_in(vec![Prop::U64(6)]);
        let expected_results = vec![
            "2->1",
            "3->1",
            "David Gilmour->John Mayer",
            "John Mayer->Jimmy Page",
        ];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter
            .property("p2")
            .is_in(vec![Prop::U64(2), Prop::U64(6)]);
        let expected_results = vec![
            "2->1",
            "2->3",
            "3->1",
            "David Gilmour->John Mayer",
            "John Mayer->Jimmy Page",
        ];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter
            .property("p2")
            .temporal()
            .first()
            .is_in(vec![Prop::U64(6)]);
        let expected_results = vec![
            "2->1",
            "3->1",
            "David Gilmour->John Mayer",
            "John Mayer->Jimmy Page",
        ];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter
            .property("p2")
            .temporal()
            .all()
            .is_in(vec![Prop::U64(6)]);
        let expected_results = vec![
            "2->1",
            "3->1",
            "David Gilmour->John Mayer",
            "John Mayer->Jimmy Page",
        ];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_property_not_in() {
        let filter = EdgeFilter.property("p2").is_not_in(vec![Prop::U64(6)]);
        let expected_results = vec!["1->2", "2->3"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter
            .property("p2")
            .temporal()
            .first()
            .is_not_in(vec![Prop::U64(6)]);
        let expected_results = vec!["1->2", "2->3"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter
            .property("p2")
            .temporal()
            .all()
            .is_not_in(vec![Prop::U64(6)]);
        let expected_results = vec!["1->2", "2->3"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_property_is_some() {
        let filter = EdgeFilter.property("p2").is_some();
        let expected_results = vec![
            "1->2",
            "2->1",
            "2->3",
            "3->1",
            "David Gilmour->John Mayer",
            "John Mayer->Jimmy Page",
        ];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter.property("p2").temporal().first().is_some();
        let expected_results = vec![
            "1->2",
            "2->1",
            "2->3",
            "3->1",
            "David Gilmour->John Mayer",
            "John Mayer->Jimmy Page",
        ];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_property_is_none() {
        let filter = EdgeFilter.property("p2").is_none();
        let expected_results = Vec::<&str>::new();
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter.property("p2").temporal().first().is_none();
        let expected_results = vec![];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_property_starts_with() {
        let filter = EdgeFilter.property("p10").starts_with("Pa");
        let expected_results: Vec<&str> = vec!["1->2", "2->1", "2->3"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter
            .property("p10")
            .temporal()
            .any()
            .starts_with("Pape");
        let expected_results: Vec<&str> = vec!["1->2", "2->1", "2->3"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter
            .property("p10")
            .temporal()
            .last()
            .starts_with("Paper");
        let expected_results: Vec<&str> = vec!["1->2", "2->1", "2->3"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter
            .property("p10")
            .temporal()
            .last()
            .starts_with("Traffic");
        let expected_results: Vec<&str> = vec![];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter
            .property("p30")
            .temporal()
            .first()
            .starts_with("Old");
        let expected_results: Vec<&str> = vec!["2->3"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter
            .property("p20")
            .temporal()
            .all()
            .starts_with("Gold");
        let expected_results: Vec<&str> = vec!["1->2", "2->3"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_property_ends_with() {
        let filter = EdgeFilter.property("p10").ends_with("lane");
        let expected_results: Vec<&str> = vec!["1->2", "2->1"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter
            .property("p10")
            .temporal()
            .any()
            .ends_with("ship");
        let expected_results: Vec<&str> = vec!["2->3"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter
            .property("p10")
            .temporal()
            .last()
            .ends_with("ane");
        let expected_results: Vec<&str> = vec!["1->2", "2->1"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter
            .property("p10")
            .temporal()
            .last()
            .ends_with("marcus");
        let expected_results: Vec<&str> = vec![];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter
            .property("p20")
            .temporal()
            .first()
            .ends_with("boat");
        let expected_results: Vec<&str> = vec!["2->3"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter
            .property("p20")
            .temporal()
            .all()
            .ends_with("ship");
        let expected_results: Vec<&str> = vec!["1->2"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_property_contains() {
        let filter = EdgeFilter.property("p10").contains("Paper");
        let expected_results: Vec<&str> = vec!["1->2", "2->1", "2->3"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter
            .property("p10")
            .temporal()
            .any()
            .contains("Paper");
        let expected_results: Vec<&str> = vec!["1->2", "2->1", "2->3"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter
            .property("p10")
            .temporal()
            .last()
            .contains("Paper");
        let expected_results: Vec<&str> = vec!["1->2", "2->1", "2->3"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter
            .property("p20")
            .temporal()
            .first()
            .contains("boat");
        let expected_results: Vec<&str> = vec!["2->3"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter.property("p20").temporal().all().contains("ship");
        let expected_results: Vec<&str> = vec!["1->2"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_property_contains_not() {
        let filter = EdgeFilter.property("p10").not_contains("ship");
        let expected_results: Vec<&str> = vec!["1->2", "2->1"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter
            .property("p10")
            .temporal()
            .any()
            .not_contains("ship");
        let expected_results: Vec<&str> = vec!["1->2", "2->1"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter
            .property("p10")
            .temporal()
            .last()
            .not_contains("ship");
        let expected_results: Vec<&str> = vec!["1->2", "2->1"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter
            .property("p20")
            .temporal()
            .first()
            .not_contains("boat");
        let expected_results: Vec<&str> = vec!["1->2"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter
            .property("p30")
            .temporal()
            .all()
            .not_contains("ship");
        let expected_results: Vec<&str> = vec!["2->3"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_by_fuzzy_search() {
        let filter = EdgeFilter.property("p1").fuzzy_search("shiv", 2, true);
        let expected_results: Vec<&str> = vec!["1->2"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter.property("p1").fuzzy_search("ShiV", 2, true);
        let expected_results: Vec<&str> = vec!["1->2"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter.property("p1").fuzzy_search("shiv", 2, false);
        let expected_results: Vec<&str> = vec![];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_not_property() {
        let filter = EdgeFilter.property("p2").ne(2u64).not();
        let expected_results = vec!["2->3"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_edges_window_filter() {
        let filter = EdgeFilter
            .window(1, 3)
            .property("p2")
            .temporal()
            .sum()
            .ge(2u64);

        let expected_results = vec!["1->2", "2->3"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter
            .window(1, 5)
            .property("p2")
            .temporal()
            .sum()
            .ge(2u64);

        let expected_results = vec![
            "1->2",
            "2->3",
            "3->1",
            "2->1",
            "David Gilmour->John Mayer",
            "John Mayer->Jimmy Page",
        ];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_edges_window_filter_on_non_temporal_property() {
        let filter1 = EdgeFilter.window(1, 2).property("p1").eq("shivam_kapoor");
        let filter2 = EdgeFilter
            .window(100, 200)
            .property("p1")
            .eq("shivam_kapoor");

        let expected_results = vec!["1->2"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter1.clone(),
            &expected_results,
            TestVariants::All,
        );

        let expected_results = vec![];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter2.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter2 = EdgeFilter
            .window(100, 200)
            .property("p1")
            .eq("shivam_kapoor");
        let expected_results = vec!["1->2"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter2.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_edges_window_filter_any_all_over_window() {
        let filter_any = EdgeFilter
            .window(2, 4)
            .property("p20")
            .temporal()
            .any()
            .eq("Gold_boat");

        let expected_any = vec!["2->3"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter_any.clone(),
            &expected_any,
            TestVariants::All,
        );

        let filter_all = EdgeFilter
            .window(2, 4)
            .property("p20")
            .temporal()
            .all()
            .eq("Gold_boat");

        let expected_all: Vec<&str> = vec![];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter_all.clone(),
            &expected_all,
            TestVariants::All,
        );
    }

    #[test]
    fn test_edges_window_filter_and() {
        let filter1 = EdgeFilter
            .window(3, 6)
            .property("p10")
            .temporal()
            .any()
            .eq("Paper_airplane");

        let filter2 = EdgeFilter
            .window(3, 6)
            .property("p2")
            .temporal()
            .sum()
            .eq(6u64);

        let filter = filter1.and(filter2);

        let expected_results = vec!["2->1"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_edges_layer_filter() {
        let filter = EdgeFilter
            .layer("fire_nation")
            .property("p2")
            .temporal()
            .sum()
            .ge(2u64);

        let expected_results = vec!["1->2", "3->1"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_edges_at_filter() {
        // Only time=2 contributes; edge 2->3 has p2=2 at t=2
        let filter = EdgeFilter.at(2).property("p2").temporal().sum().eq(2u64);

        let expected_results = vec!["2->3"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        // Only time=3 contributes; edge 3->1 has p2=6 at t=3
        let filter = EdgeFilter.at(3).property("p2").temporal().sum().eq(6u64);

        let expected_results = vec!["3->1", "2->1"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_edges_after_filter() {
        // after(2) means t >= 3
        let filter = EdgeFilter.after(2).property("p2").temporal().sum().ge(6u64);

        // At t=3: 3->1 and 2->1 have p2=6
        // At t=4: David->John and John->Jimmy have p2=6
        let expected_results = vec![
            "3->1",
            "2->1",
            "David Gilmour->John Mayer",
            "John Mayer->Jimmy Page",
        ];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_edges_before_filter() {
        // before(3) means t <= 2
        let filter = EdgeFilter
            .before(3)
            .property("p2")
            .temporal()
            .sum()
            .eq(2u64);

        // Only t=2 contributes for p2=2 -> 2->3
        let expected_results = vec!["2->3"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        // And p2=6 edges shouldn't match, because their p2=6 lives at t=3+.
        let filter = EdgeFilter
            .before(3)
            .property("p2")
            .temporal()
            .sum()
            .eq(6u64);

        let expected_results = vec![];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_edges_latest_filter() {
        // At latest time (currently t=4), only the t=4 edges exist in the Event graph.
        // Use EventOnly so the expectation is stable and matches node-style.
        let filter = EdgeFilter.latest().property("p2").eq(6u64);

        let expected_results = vec!["David Gilmour->John Mayer", "John Mayer->Jimmy Page"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_edges_snapshot_at_semantics_event_graph() {
        let t = 2;

        let filter_snapshot = EdgeFilter
            .snapshot_at(t)
            .property("p2")
            .temporal()
            .sum()
            .eq(2u64);

        let filter_before = EdgeFilter
            .before(t + 1)
            .property("p2")
            .temporal()
            .sum()
            .eq(2u64);

        let expected_results = vec!["2->3"];

        // snapshot_at
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter_snapshot.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        // before(t+1)
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter_before.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_edges_snapshot_at_semantics_persistent_graph() {
        let t = 2;

        let filter_snapshot = EdgeFilter
            .snapshot_at(t)
            .property("p2")
            .temporal()
            .sum()
            .eq(2u64);

        let filter_at = EdgeFilter.at(t).property("p2").temporal().sum().eq(2u64);

        let expected_results = vec!["2->3"];

        // snapshot_at
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter_snapshot.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        // at(t)
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter_at.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_edges_snapshot_latest_semantics_event_graph() {
        let filter_snapshot_latest = EdgeFilter
            .snapshot_latest()
            .property("p2")
            .temporal()
            .sum()
            .ge(6u64);

        let filter_noop = EdgeFilter.property("p2").temporal().sum().ge(6u64);

        // Across the whole event history, p2=6 appears at t=3 and t=4.
        let expected_results = vec![
            "3->1",
            "2->1",
            "David Gilmour->John Mayer",
            "John Mayer->Jimmy Page",
        ];

        // snapshot_latest
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter_snapshot_latest.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        // no-op baseline
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter_noop.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_edges_snapshot_latest_semantics_persistent_graph() {
        let filter_snapshot_latest = EdgeFilter.snapshot_latest().property("p2").eq(6u64);

        let filter_latest = EdgeFilter.latest().property("p2").eq(6u64);

        // In persistent latest state at t=4, these edges have p2=6:
        // - t=3 edges: 3->1, 2->1
        // - t=4 edges: David->John, John->Jimmy
        let expected_results = vec![
            "3->1",
            "2->1",
            "David Gilmour->John Mayer",
            "John Mayer->Jimmy Page",
        ];

        // snapshot_latest
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter_snapshot_latest.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        // latest
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter_latest.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_edges_layer_then_window_ordering() {
        // In layer "fire_nation" within window [1,3), edge 1->2 matches p1 == "shivam_kapoor".
        let filter = EdgeFilter
            .layer("fire_nation")
            .window(1, 3)
            .property("p1")
            .eq("shivam_kapoor");

        let expected_results = vec!["1->2"];

        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_edges_window_then_layer_ordering() {
        // Same semantics, reversed chaining order.
        let filter = EdgeFilter
            .window(1, 3)
            .layer("fire_nation")
            .property("p1")
            .eq("shivam_kapoor");

        let expected_results = vec!["1->2"];

        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_edges_latest_layer() {
        let filter = EdgeFilter
            .latest()
            .layer("fire_nation")
            .property("p2")
            .temporal()
            .last()
            .eq(7u64);

        let expected_results = vec![];

        assert_filter_edges_results(
            init_edges_graph2,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_edges_layer_latest() {
        let filter = EdgeFilter
            .layer("fire_nation")
            .latest()
            .property("p2")
            .temporal()
            .last()
            .eq(7u64);

        let expected_results = vec!["1->2"];

        assert_filter_edges_results(
            init_edges_graph2,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }
}

mod test_edge_composite_filter {
    use raphtory::db::graph::views::filter::model::{
        edge_filter::EdgeFilter, node_filter::ops::NodeFilterOps,
        property_filter::ops::PropertyFilterOps, ComposableFilter, PropertyFilterFactory,
        TryAsCompositeFilter,
    };
    use raphtory_tests::assertions::{assert_filter_edges_results, TestVariants};

    use crate::filter_tests::test_filters::{init_edges_graph, IdentityGraphTransformer};

    #[test]
    fn test_filter_edge_for_src_dst() {
        let filter = EdgeFilter::src()
            .name()
            .eq("3")
            .and(EdgeFilter::dst().name().eq("1"));
        let expected_results = vec!["3->1"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_unique_results_from_composite_filters() {
        let filter = EdgeFilter
            .property("p2")
            .ge(2u64)
            .and(EdgeFilter.property("p2").ge(1u64));
        let expected_results = vec![
            "1->2",
            "2->1",
            "2->3",
            "3->1",
            "David Gilmour->John Mayer",
            "John Mayer->Jimmy Page",
        ];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter
            .property("p2")
            .ge(2u64)
            .or(EdgeFilter.property("p2").ge(5u64));
        let expected_results = vec![
            "1->2",
            "2->1",
            "2->3",
            "3->1",
            "David Gilmour->John Mayer",
            "John Mayer->Jimmy Page",
        ];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_composite_filter_edges() {
        let filter = EdgeFilter
            .property("p2")
            .eq(2u64)
            .and(EdgeFilter.property("p1").eq("kapoor"));
        let expected_results = Vec::<&str>::new();
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        let filter = filter.try_as_composite_edge_filter().unwrap();
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter
            .property("p2")
            .eq(2u64)
            .or(EdgeFilter.property("p1").eq("shivam_kapoor"));
        let expected_results = vec!["1->2", "2->3"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = filter.try_as_composite_edge_filter().unwrap();
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter.property("p1").eq("pometry").or(EdgeFilter
            .property("p2")
            .eq(6u64)
            .and(EdgeFilter.property("p3").eq(1u64)));
        let expected_results = vec![
            "2->1",
            "3->1",
            "David Gilmour->John Mayer",
            "John Mayer->Jimmy Page",
        ];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = filter.try_as_composite_edge_filter().unwrap();
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter::src()
            .name()
            .eq("13")
            .and(EdgeFilter.property("p1").eq("prop1"));
        let expected_results = Vec::<&str>::new();
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = filter.try_as_composite_edge_filter().unwrap();
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter
            .property("p2")
            .eq(4u64)
            .and(EdgeFilter.property("p1").eq("shivam_kapoor"));
        let expected_results = vec!["1->2"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = filter.try_as_composite_edge_filter().unwrap();
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter::src()
            .name()
            .eq("1")
            .and(EdgeFilter.property("p1").eq("shivam_kapoor"));
        let expected_results = vec!["1->2"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = filter.try_as_composite_edge_filter().unwrap();
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter::dst()
            .name()
            .eq("1")
            .and(EdgeFilter.property("p2").eq(6u64));
        let expected_results = vec!["2->1", "3->1"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = filter.try_as_composite_edge_filter().unwrap();
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter::src()
            .name()
            .eq("1")
            .and(EdgeFilter.property("p1").eq("shivam_kapoor"))
            .or(EdgeFilter.property("p3").eq(5u64));
        let expected_results = vec!["1->2"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = filter.try_as_composite_edge_filter().unwrap();
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_not_composite_filter_edges() {
        let filter = EdgeFilter::src()
            .name()
            .eq("13")
            .and(EdgeFilter.property("p1").eq("prop1"))
            .not();
        let expected_results = vec![
            "1->2",
            "2->1",
            "2->3",
            "3->1",
            "David Gilmour->John Mayer",
            "John Mayer->Jimmy Page",
        ];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter::src()
            .name()
            .eq("13")
            .and(EdgeFilter.property("p1").eq("prop1").not())
            .not();
        let expected_results = vec![
            "1->2",
            "2->1",
            "2->3",
            "3->1",
            "David Gilmour->John Mayer",
            "John Mayer->Jimmy Page",
        ];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }
}

/// How `&`, `|` and `~` of graph-level views compose. Every expectation is
/// computed by chaining the views directly, never through the filter path
/// under test.
///
/// ```text
/// time:  0    1    2    3    4    5    6    7    8    9   10
/// a→b         ●                             ●               events 1 and 7
/// c→d                        ●                              event 4
/// e→f                                  ●                    event 6
///
/// Graph.window(0,5)  [==================)
/// Graph.window(3,8)            [==================)
/// Graph.window(6,10)                          [==========)
/// ```
mod test_view_composition {
    use raphtory::{
        db::{
            api::view::filter_ops::{Filter, Select},
            graph::views::filter::model::{
                edge_filter::EdgeFilter,
                graph_filter::{GraphFilter, ViewFilter},
                property_filter::ops::PropertyFilterOps,
                ComposableFilter, PropertyFilterFactory, TemporalPropertyFilterFactory,
            },
        },
        errors::GraphError,
        prelude::*,
    };
    use raphtory_api::core::storage::timeindex::AsTime;
    use std::collections::BTreeSet;

    fn graph() -> Graph {
        let g = Graph::new();
        g.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
        g.add_edge(7, "a", "b", NO_PROPS, None).unwrap();
        g.add_edge(4, "c", "d", NO_PROPS, None).unwrap();
        g.add_edge(6, "e", "f", NO_PROPS, None).unwrap();
        g
    }

    fn edges<'a, G: GraphViewOps<'a>>(g: &G) -> BTreeSet<String> {
        g.edges()
            .iter()
            .map(|e| format!("{}->{}", e.src().name(), e.dst().name()))
            .collect()
    }

    fn history<'a, G: GraphViewOps<'a>>(g: &G, src: &str, dst: &str) -> Vec<i64> {
        g.edge(src, dst)
            .map(|e| e.history().iter().map(|t| t.t()).collect())
            .unwrap_or_default()
    }

    /// Each time op, then each layer op: applying the filter must equal the
    /// chained view, and negating it must equal the views either side of what
    /// the chain resolved to — read off the applied view's own bounds, so an
    /// op whose meaning depends on the graph model is checked as that model
    /// resolved it.
    ///
    /// A macro rather than a function because each chained view has its own
    /// type, and the graph models differ.
    macro_rules! check_every_view_op {
        ($g:expr) => {{
            let g = $g;
            let time_ops: Vec<(&str, ViewFilter, BTreeSet<String>)> = vec![
                ("window", GraphFilter.window(3, 12), edges(&g.window(3, 12))),
                ("at", GraphFilter.at(10), edges(&g.at(10))),
                ("before", GraphFilter.before(10), edges(&g.before(10))),
                ("after", GraphFilter.after(8), edges(&g.after(8))),
                ("latest", GraphFilter.latest(), edges(&g.latest())),
                (
                    "snapshot_at",
                    GraphFilter.snapshot_at(10),
                    edges(&g.snapshot_at(10)),
                ),
                (
                    "snapshot_latest",
                    GraphFilter.snapshot_latest(),
                    edges(&g.snapshot_latest()),
                ),
            ];
            for (label, filter, chained) in time_ops {
                let applied = g.filter(filter.clone()).unwrap();
                assert_eq!(edges(&applied), chained, "{}: applied", label);

                // The views either side of the applied window.
                let mut want = BTreeSet::new();
                if let Some(start) = applied.start() {
                    want.extend(edges(&g.before(start.t())));
                }
                if let Some(end) = applied.end() {
                    want.extend(edges(&g.after(end.t().saturating_sub(1))));
                }
                let negated = g.filter(filter.not()).unwrap();
                assert_eq!(edges(&negated), want, "{}: negated", label);
            }

            let layer_ops: Vec<(&str, ViewFilter, BTreeSet<String>, BTreeSet<String>)> = vec![
                (
                    "layer",
                    GraphFilter.layer("work"),
                    edges(&g.layers(["work"]).unwrap()),
                    edges(&g.exclude_layers(["work"]).unwrap()),
                ),
                (
                    "layers",
                    GraphFilter.layer(vec!["work", "friends"]),
                    edges(&g.layers(["work", "friends"]).unwrap()),
                    edges(&g.exclude_layers(["work", "friends"]).unwrap()),
                ),
            ];
            for (label, filter, chained, complement) in layer_ops {
                let applied = g.filter(filter.clone()).unwrap();
                assert_eq!(edges(&applied), chained, "{}: applied", label);
                let negated = g.filter(filter.not()).unwrap();
                assert_eq!(edges(&negated), complement, "{}: negated", label);
            }
        }};
    }

    #[test]
    fn a_conjunction_of_windows_is_their_overlap() {
        let g = graph();
        let expected = g.window(0, 5).window(3, 8);
        let got = g
            .filter(GraphFilter.window(0, 5).and(GraphFilter.window(3, 8)))
            .unwrap();
        assert_eq!(edges(&got), edges(&expected));
        assert_eq!(edges(&got), BTreeSet::from(["c->d".to_string()]));
        assert_eq!(got.earliest_time(), expected.earliest_time());
        assert_eq!(got.latest_time(), expected.latest_time());
    }

    #[test]
    fn a_disjunction_of_windows_is_their_union_with_the_gap_left_out() {
        let g = graph();
        let left = g.window(0, 5);
        let right = g.window(6, 10);
        let got = g
            .filter(GraphFilter.window(0, 5).or(GraphFilter.window(6, 10)))
            .unwrap();

        let expected: BTreeSet<_> = edges(&left).union(&edges(&right)).cloned().collect();
        assert_eq!(edges(&got), expected);
        // e->f at 6 is in the right window, so the union holds it; nothing
        // sits in the gap [5, 6) to be wrongly admitted.
        assert!(edges(&got).contains("e->f"));

        // An edge with an event in each window keeps both, and only those.
        let mut want = history(&left, "a", "b");
        want.extend(history(&right, "a", "b"));
        assert_eq!(history(&got, "a", "b"), want);
        assert_eq!(history(&got, "a", "b"), vec![1, 7]);
    }

    #[test]
    fn a_negated_window_is_the_ranges_either_side_of_it() {
        let g = graph();
        let got = g.filter(GraphFilter.window(3, 8).not()).unwrap();

        // Everything before 3 or from 8 on: a->b keeps its event at 1 and
        // drops the one at 7, which is what chaining the two sides gives.
        let mut want = history(&g.window(i64::MIN, 3), "a", "b");
        want.extend(history(&g.window(8, i64::MAX), "a", "b"));
        assert_eq!(history(&got, "a", "b"), want);
        assert_eq!(history(&got, "a", "b"), vec![1]);
        assert_eq!(edges(&got), BTreeSet::from(["a->b".to_string()]));
    }

    #[test]
    fn a_predicate_beside_a_window_is_evaluated_inside_it() {
        let g = Graph::new();
        g.add_edge(1, "a", "b", [("score", 1i64)], None).unwrap();
        g.add_edge(9, "a", "b", [("score", 9i64)], None).unwrap();

        let inside = GraphFilter
            .window(0, 5)
            .and(EdgeFilter.property("score").temporal().last().eq(9i64));
        // The edge's last value inside [0, 5) is 1, so the predicate fails
        // there even though the edge does reach 9 later.
        assert!(edges(&g.filter(inside).unwrap()).is_empty());

        let matching = GraphFilter
            .window(0, 5)
            .and(EdgeFilter.property("score").temporal().last().eq(1i64));
        assert_eq!(
            edges(&g.filter(matching).unwrap()),
            BTreeSet::from(["a->b".to_string()])
        );
    }

    #[test]
    fn a_conjunction_carries_the_composed_view_not_the_base_graph() {
        let g = Graph::new();
        g.add_edge(1, "a", "b", [("score", 1i64)], None).unwrap();
        g.add_edge(7, "a", "b", [("score", 7i64)], None).unwrap();
        let expected = g.window(0, 5);
        let got = g
            .filter(
                GraphFilter
                    .window(0, 5)
                    .and(EdgeFilter.property("score").temporal().last().ne(404i64)),
            )
            .unwrap();
        assert_eq!(history(&got, "a", "b"), history(&expected, "a", "b"));
        assert_eq!(history(&got, "a", "b"), vec![1]);
        assert_eq!(got.latest_time(), expected.latest_time());
    }

    // The edge-collection path lowers through `select`, which is where a
    // window used to reach the result but not the graph the predicate beside
    // it was read from.
    #[test]
    fn an_edge_collection_reads_a_predicate_inside_the_window_beside_it() {
        let g = Graph::new();
        g.add_edge(1, "a", "b", [("score", 1i64)], None).unwrap();
        g.add_edge(9, "a", "b", [("score", 9i64)], None).unwrap();

        let inside = GraphFilter
            .window(0, 5)
            .and(EdgeFilter.property("score").temporal().last().eq(9i64));
        assert!(g.edges().select(inside).unwrap().iter().next().is_none());

        let matching = GraphFilter
            .window(0, 5)
            .and(EdgeFilter.property("score").temporal().last().eq(1i64));
        let selected = g.edges().select(matching).unwrap();
        assert_eq!(
            selected
                .iter()
                .map(|e| format!("{}->{}", e.src().name(), e.dst().name()))
                .collect::<Vec<_>>(),
            vec!["a->b".to_string()]
        );
        // Membership is what the window decides here; each edge in the
        // collection still reports the base graph's history, which is how
        // collections have always behaved and is not what this filter changes.
    }

    // Layer ids are not a contiguous range from zero, so a complement built by
    // counting layers picks ids the graph never issued and misses real ones.
    #[test]
    fn a_negated_layer_view_is_the_other_layers() {
        let g = Graph::new();
        g.add_edge(5, "a", "b", NO_PROPS, Some("work")).unwrap();
        g.add_edge(15, "c", "a", NO_PROPS, Some("friends")).unwrap();
        g.add_edge(12, "d", "d", NO_PROPS, None).unwrap();

        let expected = g.layers(["_default", "friends"]).unwrap();
        let got = g.filter(GraphFilter.layer("work").not()).unwrap();
        assert_eq!(edges(&got), edges(&expected));
        assert_eq!(
            edges(&got),
            BTreeSet::from(["c->a".to_string(), "d->d".to_string()])
        );
    }

    // `after` and `before` are open-ended, so their complements are single
    // ranges. Encoding them as windows bounded by the largest timestamp left a
    // sliver of time beyond the bound, which a negation picked up — and on a
    // persistent graph that sliver admits every edge still alive at the end of
    // time, so the complement returned the whole graph.
    #[test]
    fn a_negated_open_ended_view_has_no_sliver_at_the_end_of_time() {
        for persistent in [false, true] {
            let g = Graph::new();
            g.add_edge(5, "a", "b", NO_PROPS, None).unwrap();
            g.add_edge(10, "b", "c", NO_PROPS, None).unwrap();
            g.add_edge(15, "c", "d", NO_PROPS, None).unwrap();

            let after = GraphFilter.after(8);
            let before = GraphFilter.before(9);
            if persistent {
                let g = g.persistent_graph();
                assert_eq!(
                    edges(&g.filter(after.clone().not()).unwrap()),
                    edges(&g.before(9))
                );
                assert_eq!(
                    edges(&g.filter(before.clone().not()).unwrap()),
                    edges(&g.after(8))
                );
            } else {
                assert_eq!(edges(&g.filter(after.not()).unwrap()), edges(&g.before(9)));
                assert_eq!(edges(&g.filter(before.not()).unwrap()), edges(&g.after(8)));
            }
        }
    }

    // Every view op, applied and negated, against the chained view it is
    // meant to equal. The two bugs this covers were both op-specific — a
    // layer complement that assumed contiguous layer ids, and `after`
    // encoded as a window ending at the largest timestamp — so each op is
    // checked rather than a representative few.
    #[test]
    fn every_view_op_applies_and_negates_like_its_chained_view() {
        let g = Graph::new();
        g.add_edge(1, "a", "b", NO_PROPS, Some("work")).unwrap();
        g.add_edge(11, "a", "b", NO_PROPS, Some("work")).unwrap();
        g.add_edge(4, "c", "d", NO_PROPS, Some("work")).unwrap();
        g.add_edge(10, "e", "f", NO_PROPS, Some("friends")).unwrap();
        g.add_edge(15, "g", "h", NO_PROPS, None).unwrap();
        g.delete_edge(20, "c", "d", Some("work")).unwrap();

        check_every_view_op!(g.clone());
        check_every_view_op!(g.persistent_graph());
    }

    #[test]
    fn mixing_time_and_layers_under_a_union_or_a_negation_is_refused() {
        let g = Graph::new();
        g.add_edge(1, "a", "b", NO_PROPS, Some("x")).unwrap();
        g.add_edge(2, "c", "d", NO_PROPS, Some("y")).unwrap();

        let mixed = GraphFilter.window(0, 5).or(GraphFilter.layer("x"));
        assert!(matches!(
            g.filter(mixed),
            Err(GraphError::InvalidGqlFilter(_))
        ));
        let both = GraphFilter.window(0, 5).layer("x");
        assert!(matches!(
            g.filter(both.not()),
            Err(GraphError::InvalidGqlFilter(_))
        ));
        // Agreeing on the layer dimension is fine.
        let same_layer = GraphFilter
            .window(0, 5)
            .layer("x")
            .or(GraphFilter.window(6, 9).layer("x"));
        assert!(g.filter(same_layer).is_ok());
    }
}

/// Algebraic laws of graph-level view composition, checked against references
/// built by chaining the graph's own view methods — never through the filter
/// path under test.
///
/// The two bugs these cover were both op-specific and only showed on one
/// graph model or one op, which a handful of hand-picked cases missed: a layer
/// complement that assumed contiguous layer ids, and `after` encoded as a
/// window ending at the largest timestamp.
mod test_view_composition_properties {
    use proptest::prelude::*;
    use raphtory::{
        db::{
            api::view::{filter_ops::Filter, DynamicGraph, IntoDynamic},
            graph::views::filter::model::{
                graph_filter::{GraphFilter, ViewFilter},
                ComposableFilter,
            },
        },
        prelude::*,
    };
    use raphtory_api::core::storage::timeindex::AsTime;
    use std::collections::BTreeSet;

    /// The view ops, each one a restriction of the time axis alone. Layer ops
    /// are covered separately: mixing the two axes under `|` or `~` has no
    /// single answer and is refused, so a generator over both would spend its
    /// time on the refusal rather than on the algebra.
    #[derive(Clone, Debug)]
    enum TimeOp {
        Window(i64, i64),
        At(i64),
        Before(i64),
        After(i64),
        Latest,
        SnapshotAt(i64),
        SnapshotLatest,
    }

    impl TimeOp {
        /// The op as a filter — the thing under test.
        fn filter(&self) -> ViewFilter {
            match *self {
                TimeOp::Window(start, end) => GraphFilter.window(start, end),
                TimeOp::At(t) => GraphFilter.at(t),
                TimeOp::Before(t) => GraphFilter.before(t),
                TimeOp::After(t) => GraphFilter.after(t),
                TimeOp::Latest => GraphFilter.latest(),
                TimeOp::SnapshotAt(t) => GraphFilter.snapshot_at(t),
                TimeOp::SnapshotLatest => GraphFilter.snapshot_latest(),
            }
        }

        /// The op as the graph's own view — the reference.
        fn chain(&self, graph: &DynamicGraph) -> DynamicGraph {
            match *self {
                TimeOp::Window(start, end) => graph.window(start, end).into_dynamic(),
                TimeOp::At(t) => graph.at(t).into_dynamic(),
                TimeOp::Before(t) => graph.before(t).into_dynamic(),
                TimeOp::After(t) => graph.after(t).into_dynamic(),
                TimeOp::Latest => graph.latest().into_dynamic(),
                TimeOp::SnapshotAt(t) => graph.snapshot_at(t).into_dynamic(),
                TimeOp::SnapshotLatest => graph.snapshot_latest().into_dynamic(),
            }
        }
    }

    fn op() -> impl Strategy<Value = TimeOp> {
        prop_oneof![
            (0i64..14, 0i64..14).prop_map(|(a, b)| TimeOp::Window(a.min(b), a.max(b))),
            (0i64..14).prop_map(TimeOp::At),
            (0i64..14).prop_map(TimeOp::Before),
            (0i64..14).prop_map(TimeOp::After),
            Just(TimeOp::Latest),
            (0i64..14).prop_map(TimeOp::SnapshotAt),
            Just(TimeOp::SnapshotLatest),
        ]
    }

    /// Edge additions, so an event graph's membership is unambiguous.
    fn graph() -> impl Strategy<Value = Graph> {
        proptest::collection::vec((0i64..12, 0u64..4, 0u64..4), 1..8).prop_map(|events| {
            let g = Graph::new();
            for (time, src, dst) in events {
                g.add_edge(time, src, dst, NO_PROPS, None).unwrap();
            }
            g
        })
    }

    const LAYERS: [&str; 3] = ["work", "friends", "family"];

    /// The same, spread over named layers and the default one, so a layer
    /// complement has real ids to enumerate — they are not a contiguous range
    /// from zero, which is what a counted complement got wrong.
    fn layered_graph() -> impl Strategy<Value = Graph> {
        proptest::collection::vec((0i64..12, 0u64..4, 0u64..4, 0usize..4), 1..8).prop_map(
            |events| {
                // Two layers are always present, so a proper subset exists.
                let g = Graph::new();
                g.add_edge(0, 90, 91, NO_PROPS, Some(LAYERS[0])).unwrap();
                g.add_edge(0, 92, 93, NO_PROPS, Some(LAYERS[1])).unwrap();
                for (time, src, dst, layer) in events {
                    let layer = LAYERS.get(layer).copied();
                    g.add_edge(time, src, dst, NO_PROPS, layer).unwrap();
                }
                g
            },
        )
    }

    /// A layered graph paired with a proper, non-empty subset of the layers it
    /// actually has.
    ///
    /// Drawn from the graph because `GraphFilter.layer` rejects a name the
    /// graph never issued, and *proper* because naming every layer restricts
    /// nothing — the view's layer set is then all of them, which composes with
    /// a time view rather than being refused.
    fn graph_and_layers() -> impl Strategy<Value = (Graph, Vec<String>)> {
        layered_graph().prop_flat_map(|g| {
            let all: Vec<String> = g.unique_layers().map(|l| l.to_string()).collect();
            let count = all.len();
            (Just(g), proptest::collection::vec(0..count, 1..count)).prop_map(move |(g, picks)| {
                let names: BTreeSet<String> = picks.into_iter().map(|i| all[i].clone()).collect();
                (g, names.into_iter().collect())
            })
        })
    }

    fn ids<'a, G: GraphViewOps<'a>>(graph: &G) -> BTreeSet<(u64, u64)> {
        graph
            .edges()
            .iter()
            .map(|e| {
                (
                    e.src().id().as_u64().unwrap(),
                    e.dst().id().as_u64().unwrap(),
                )
            })
            .collect()
    }

    fn history<'a, G: GraphViewOps<'a>>(graph: &G, edge: (u64, u64)) -> BTreeSet<i64> {
        graph
            .edge(edge.0, edge.1)
            .map(|e| e.history().iter().map(|t| t.t()).collect())
            .unwrap_or_default()
    }

    /// The views either side of what `applied` resolved to.
    fn outside(base: &DynamicGraph, applied: &DynamicGraph) -> BTreeSet<(u64, u64)> {
        let mut out = BTreeSet::new();
        if let Some(start) = applied.start() {
            out.extend(ids(&base.before(start.t())));
        }
        if let Some(end) = applied.end() {
            out.extend(ids(&base.after(end.t().saturating_sub(1))));
        }
        out
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// One op: the filter is the graph's own view, events and all.
        #[test]
        fn a_single_view_is_its_chained_view(g in graph(), a in op()) {
            let base = g.clone().into_dynamic();
            let got = g.filter(a.filter())?.into_dynamic();
            let want = a.chain(&base);
            prop_assert_eq!(ids(&got), ids(&want));
            for edge in ids(&want) {
                prop_assert_eq!(history(&got, edge), history(&want, edge));
            }
        }

        /// `&` applies its operands in sequence, so it is the two views chained
        /// — which is what `latest` and `snapshot_at` need, since they resolve
        /// against the graph they are applied to.
        #[test]
        fn a_conjunction_is_the_two_views_chained(g in graph(), a in op(), b in op()) {
            let base = g.clone().into_dynamic();
            let got = g.filter(a.filter().and(b.filter()))?.into_dynamic();
            let want = b.chain(&a.chain(&base));
            prop_assert_eq!(ids(&got), ids(&want));
            for edge in ids(&want) {
                prop_assert_eq!(history(&got, edge), history(&want, edge));
            }
        }

        /// `|` is the union: an entity belongs if either view holds it, and it
        /// keeps the events of both — the multi-window semantics.
        #[test]
        fn a_disjunction_is_the_union_of_both_views(g in graph(), a in op(), b in op()) {
            let base = g.clone().into_dynamic();
            let got = g.filter(a.filter().or(b.filter()))?.into_dynamic();
            let (left, right) = (a.chain(&base), b.chain(&base));

            let want: BTreeSet<_> = ids(&left).union(&ids(&right)).cloned().collect();
            prop_assert_eq!(ids(&got), want.clone());
            for edge in want {
                let events: BTreeSet<i64> = history(&left, edge)
                    .union(&history(&right, edge))
                    .cloned()
                    .collect();
                prop_assert_eq!(history(&got, edge), events);
            }
        }

        /// `~` keeps what lies outside the view, so an entity with events on
        /// both sides survives with the outside ones.
        #[test]
        fn a_negation_is_what_lies_outside_the_view(g in graph(), a in op()) {
            let base = g.clone().into_dynamic();
            let applied = a.chain(&base);
            let got = g.filter(a.filter().not())?.into_dynamic();
            prop_assert_eq!(ids(&got), outside(&base, &applied));
        }

        /// Negating twice comes back, and a view meets itself unchanged.
        #[test]
        fn negation_is_an_involution_and_a_view_is_idempotent(g in graph(), a in op()) {
            let base = g.clone().into_dynamic();
            let once = ids(&g.filter(a.filter())?.into_dynamic());
            prop_assert_eq!(
                ids(&g.filter(a.filter().not().not())?.into_dynamic()),
                once.clone()
            );
            prop_assert_eq!(ids(&g.filter(a.filter().and(a.filter()))?.into_dynamic()), once.clone());
            prop_assert_eq!(ids(&g.filter(a.filter().or(a.filter()))?.into_dynamic()), once);
            let _ = base;
        }

        /// A union does not depend on the order of its operands, where a
        /// conjunction may: `latest` resolves against what precedes it.
        #[test]
        fn a_disjunction_is_commutative(g in graph(), a in op(), b in op()) {
            let left = ids(&g.filter(a.filter().or(b.filter()))?.into_dynamic());
            let right = ids(&g.filter(b.filter().or(a.filter()))?.into_dynamic());
            prop_assert_eq!(left, right);
        }

        /// A layer view is the graph's own layer view, and its negation is the
        /// graph's own exclusion of those layers — over any subset, since the
        /// ids a graph issues are not a contiguous range.
        #[test]
        fn a_layer_view_and_its_negation_match_the_graph((g, names) in graph_and_layers()) {
            let filter = GraphFilter.layer(names.clone());
            let kept = g.filter(filter.clone())?.into_dynamic();
            prop_assert_eq!(ids(&kept), ids(&g.layers(names.clone())?));

            let dropped = g.filter(filter.not())?.into_dynamic();
            prop_assert_eq!(ids(&dropped), ids(&g.exclude_layers(names)?));
        }

        /// Restricting time on one side and layers on the other has no single
        /// (time, layers) answer, so a union or negation of the two is
        /// refused rather than widened into a hull that admits what neither
        /// side does. The conjunction of the same pair is always fine.
        #[test]
        fn mixing_the_axes_is_refused_under_a_union_but_not_a_conjunction(
            (g, names) in graph_and_layers(),
            a in op(),
        ) {
            let layer = GraphFilter.layer(names);
            prop_assert!(g.filter(a.filter().or(layer.clone())).is_err());
            prop_assert!(g.filter(a.filter().and(layer.clone()).not()).is_err());
            prop_assert!(g.filter(a.filter().and(layer)).is_ok());
        }
    }
}
