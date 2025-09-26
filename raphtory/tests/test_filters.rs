use raphtory::{db::api::view::StaticGraphViewOps, prelude::*};
use raphtory_api::core::entities::properties::prop::IntoProp;
use raphtory_storage::mutation::{
    addition_ops::InternalAdditionOps, property_addition_ops::InternalPropertyAdditionOps,
};

pub struct IdentityGraphTransformer;

impl GraphTransformer for IdentityGraphTransformer {
    type Return<G: StaticGraphViewOps> = G;
    fn apply<G: StaticGraphViewOps>(&self, graph: G) -> Self::Return<G> {
        graph
    }
}

#[cfg(test)]
mod test_property_semantics {
    #[cfg(test)]
    mod test_node_property_filter_semantics {
        use raphtory::{
            db::{
                api::view::StaticGraphViewOps,
                graph::{
                    assertions::{
                        assert_filter_nodes_results, assert_search_nodes_results, TestVariants,
                    },
                    views::filter::model::PropertyFilterOps,
                },
            },
            prelude::*,
        };
        use raphtory_api::core::entities::properties::prop::Prop;
        use raphtory_storage::mutation::{
            addition_ops::InternalAdditionOps, property_addition_ops::InternalPropertyAdditionOps,
        };

        use crate::IdentityGraphTransformer;

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
                graph.add_node(*id, label, props.clone(), None).unwrap();
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

        fn init_graph_for_secondary_indexes<
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
                graph.add_node(*id, label, props.clone(), None).unwrap();
            }

            graph
        }

        #[test]
        fn test_metadata_semantics() {
            let filter = PropertyFilter::metadata("p1").eq(1u64);
            let expected_results = vec!["N1", "N10", "N11", "N12", "N13", "N14", "N15", "N9"];
            assert_filter_nodes_results(
                init_graph,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                TestVariants::All,
            );
            assert_search_nodes_results(
                init_graph,
                IdentityGraphTransformer,
                filter,
                &expected_results,
                TestVariants::All,
            );
        }

        #[test]
        fn test_temporal_any_semantics() {
            let filter = PropertyFilter::property("p1").temporal().any().eq(1u64);
            let expected_results = vec!["N1", "N2", "N3", "N4", "N5", "N6", "N7", "N8"];
            assert_filter_nodes_results(
                init_graph,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                TestVariants::All,
            );
            assert_search_nodes_results(
                init_graph,
                IdentityGraphTransformer,
                filter,
                &expected_results,
                TestVariants::All,
            );
        }

        #[test]
        fn test_temporal_any_semantics_for_secondary_indexes() {
            let filter = PropertyFilter::property("p1").temporal().any().eq(1u64);
            let expected_results =
                vec!["N1", "N16", "N17", "N2", "N3", "N4", "N5", "N6", "N7", "N8"];
            assert_filter_nodes_results(
                init_graph_for_secondary_indexes,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                TestVariants::All,
            );
            assert_search_nodes_results(
                init_graph_for_secondary_indexes,
                IdentityGraphTransformer,
                filter,
                &expected_results,
                TestVariants::All,
            );
        }

        #[test]
        fn test_temporal_latest_semantics() {
            let filter = PropertyFilter::property("p1").temporal().latest().eq(1u64);
            let expected_results = vec!["N1", "N3", "N4", "N6", "N7"];
            assert_filter_nodes_results(
                init_graph,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                TestVariants::All,
            );
            assert_search_nodes_results(
                init_graph,
                IdentityGraphTransformer,
                filter,
                &expected_results,
                TestVariants::All,
            );
        }

        #[test]
        fn test_temporal_latest_semantics_for_secondary_indexes() {
            let filter = PropertyFilter::property("p1").temporal().latest().eq(1u64);
            let expected_results = vec!["N1", "N16", "N3", "N4", "N6", "N7"];
            assert_filter_nodes_results(
                init_graph_for_secondary_indexes,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                TestVariants::All,
            );
            assert_search_nodes_results(
                init_graph_for_secondary_indexes,
                IdentityGraphTransformer,
                filter,
                &expected_results,
                TestVariants::All,
            );
        }

        #[test]
        fn test_property_semantics() {
            // TODO: Const properties not supported for disk_graph.
            let filter = PropertyFilter::property("p1").eq(1u64);
            let expected_results = vec!["N1", "N3", "N4", "N6", "N7"];
            assert_filter_nodes_results(
                init_graph,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                TestVariants::NonDiskOnly,
            );
            assert_search_nodes_results(
                init_graph,
                IdentityGraphTransformer,
                filter,
                &expected_results,
                TestVariants::NonDiskOnly,
            );
        }

        #[test]
        fn test_property_semantics_for_secondary_indexes() {
            let filter = PropertyFilter::property("p1").eq(1u64);
            let expected_results = vec!["N1", "N16", "N3", "N4", "N6", "N7"];
            assert_filter_nodes_results(
                init_graph_for_secondary_indexes,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                TestVariants::All,
            );
            assert_search_nodes_results(
                init_graph_for_secondary_indexes,
                IdentityGraphTransformer,
                filter,
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
                    graph.add_node(*id, label, props.clone(), None).unwrap();
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

            let filter = PropertyFilter::property("p1").ge(1u64);
            let expected_results = vec![];
            assert_filter_nodes_results(
                init_graph,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                TestVariants::NonDiskOnly,
            );
            assert_search_nodes_results(
                init_graph,
                IdentityGraphTransformer,
                filter,
                &expected_results,
                TestVariants::NonDiskOnly,
            );
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
                    graph.add_node(*id, label, props.clone(), None).unwrap();
                }

                graph
            }

            let filter = PropertyFilter::property("p1").le(1u64);
            let expected_results = vec!["N1", "N3"];
            assert_filter_nodes_results(
                init_graph,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                TestVariants::NonDiskOnly,
            );
            assert_search_nodes_results(
                init_graph,
                IdentityGraphTransformer,
                filter,
                &expected_results,
                TestVariants::NonDiskOnly,
            );
        }
    }

    #[cfg(test)]
    mod test_edge_property_filter_semantics {
        use raphtory::{
            db::{
                api::view::{EdgeViewOps, StaticGraphViewOps},
                graph::{
                    assertions::{
                        assert_filter_edges_results, assert_search_edges_results,
                        TestGraphVariants, TestVariants,
                    },
                    views::filter::{
                        internal::CreateEdgeFilter,
                        model::{property_filter::PropertyFilter, PropertyFilterOps},
                    },
                },
            },
            prelude::*,
        };
        use raphtory_api::core::entities::properties::prop::Prop;
        use raphtory_storage::mutation::{
            addition_ops::InternalAdditionOps, property_addition_ops::InternalPropertyAdditionOps,
        };

        use crate::IdentityGraphTransformer;

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

        fn init_graph_for_secondary_indexes<
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
        fn test_metadata_semantics() {
            // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
            // TODO: Const properties not supported for disk_graph.
            let filter = PropertyFilter::metadata("p1").eq(1u64);
            let expected_results = vec![
                "N1->N2", "N10->N11", "N11->N12", "N12->N13", "N13->N14", "N14->N15", "N15->N1",
                "N9->N10",
            ];
            assert_filter_edges_results(
                init_graph,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                vec![TestGraphVariants::Graph],
            );
            assert_search_edges_results(
                init_graph,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                TestVariants::NonDiskOnly,
            );
        }

        #[test]
        fn test_metadata_semantics2() {
            fn filter_edges(graph: &Graph, filter: impl CreateEdgeFilter) -> Vec<String> {
                let mut results = graph
                    .filter_edges(filter)
                    .unwrap()
                    .edges()
                    .iter()
                    .map(|e| format!("{}->{}", e.src().name(), e.dst().name()))
                    .collect::<Vec<_>>();
                results.sort();
                results
            }

            let graph = init_graph(Graph::new());

            let filter = PropertyFilter::metadata("p1").eq(1u64);
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

            let filter2 = PropertyFilter::metadata("z").eq(Prop::map([("fire_nation", true)]));
            assert_eq!(filter_edges(&graph, filter2), vec!["shivam->kapoor"]);

            let filter = PropertyFilter::metadata("p1").eq(Prop::map([("_default", 1u64)]));
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
            // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
            let filter = PropertyFilter::property("p1").temporal().any().eq(1u64);
            let expected_results = vec![
                "N1->N2", "N2->N3", "N3->N4", "N4->N5", "N5->N6", "N6->N7", "N7->N8", "N8->N9",
            ];
            assert_filter_edges_results(
                init_graph,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                TestVariants::EventOnly,
            );
            assert_search_edges_results(
                init_graph,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                TestVariants::All,
            );
        }

        #[test]
        fn test_temporal_any_semantics_for_secondary_indexes() {
            // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
            let filter = PropertyFilter::property("p1").temporal().any().lt(2u64);
            let expected_results = vec![
                "N1->N2", "N16->N15", "N17->N16", "N2->N3", "N3->N4", "N4->N5", "N5->N6", "N6->N7",
                "N7->N8", "N8->N9",
            ];
            assert_filter_edges_results(
                init_graph_for_secondary_indexes,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                TestVariants::EventOnly,
            );
            assert_search_edges_results(
                init_graph_for_secondary_indexes,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                TestVariants::All,
            );
        }

        #[test]
        fn test_temporal_latest_semantics() {
            // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
            let filter = PropertyFilter::property("p1").temporal().latest().eq(1u64);
            let expected_results = vec!["N1->N2", "N3->N4", "N4->N5", "N6->N7", "N7->N8"];
            assert_filter_edges_results(
                init_graph,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                TestVariants::EventOnly,
            );
            assert_search_edges_results(
                init_graph,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                TestVariants::All,
            );
        }

        #[test]
        fn test_temporal_latest_semantics_for_secondary_indexes() {
            // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
            let filter = PropertyFilter::property("p1").temporal().latest().eq(1u64);
            let expected_results =
                vec!["N1->N2", "N16->N15", "N3->N4", "N4->N5", "N6->N7", "N7->N8"];
            assert_filter_edges_results(
                init_graph_for_secondary_indexes,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                TestVariants::EventOnly,
            );
            assert_search_edges_results(
                init_graph_for_secondary_indexes,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                TestVariants::All,
            );
        }

        #[test]
        fn test_property_semantics() {
            // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
            let filter = PropertyFilter::property("p1").ge(2u64);
            let expected_results = vec![
                "N10->N11", "N11->N12", "N12->N13", "N13->N14", "N2->N3", "N5->N6", "N8->N9",
                "N9->N10",
            ];
            assert_filter_edges_results(
                init_graph,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                TestVariants::EventOnly,
            );
            assert_search_edges_results(
                init_graph,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                TestVariants::All,
            );
        }

        #[test]
        fn test_property_semantics_for_secondary_indexes() {
            // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
            // TODO: Const properties not supported for disk_graph.
            let filter = PropertyFilter::property("p1").eq(1u64);
            let expected_results =
                vec!["N1->N2", "N16->N15", "N3->N4", "N4->N5", "N6->N7", "N7->N8"];
            assert_filter_edges_results(
                init_graph_for_secondary_indexes,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                vec![TestGraphVariants::Graph],
            );
            assert_search_edges_results(
                init_graph_for_secondary_indexes,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                TestVariants::NonDiskOnly,
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

            let filter = PropertyFilter::property("p1").eq(1u64);
            let expected_results = vec![];
            assert_filter_edges_results(
                init_graph,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                vec![TestGraphVariants::Graph],
            );
            assert_search_edges_results(
                init_graph,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                TestVariants::NonDiskOnly,
            );
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

            let filter = PropertyFilter::property("p1").eq(1u64);
            let expected_results = vec!["N1->N2", "N3->N4"];
            assert_filter_edges_results(
                init_graph,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                TestVariants::EventOnly,
            );
            assert_search_edges_results(
                init_graph,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                TestVariants::All,
            );
        }
    }
}

use raphtory::db::graph::assertions::GraphTransformer;

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
            ],
            Some("air_nomads"),
        ),
        (
            3,
            "1",
            vec![
                ("p1", "shivam_kapoor".into_prop()),
                ("p9", 5u64.into_prop()),
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
            ],
            Some("fire_nation"),
        ),
        (3, "4", vec![("p4", "pometry".into_prop())], None),
        (4, "4", vec![("p5", 12u64.into_prop())], None),
    ];

    for (time, id, props, node_type) in nodes {
        graph.add_node(time, id, props, node_type).unwrap();
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

#[cfg(test)]
mod test_node_property_filter {
    use raphtory::db::graph::views::filter::model::PropertyFilterOps;
    use raphtory_api::core::entities::properties::prop::Prop;

    use raphtory::db::graph::{
        assertions::{assert_filter_nodes_results, assert_search_nodes_results, TestVariants},
        views::filter::model::{property_filter::PropertyFilter, ComposableFilter, NotFilter},
    };

    use crate::{init_nodes_graph, IdentityGraphTransformer};

    #[test]
    fn test_exact_match() {
        let filter = PropertyFilter::property("p10").eq("Paper_airplane");
        let expected_results = vec!["1", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter,
            &expected_results,
            TestVariants::All,
        );

        let filter = PropertyFilter::property("p10").eq("");
        let expected_results = Vec::<&str>::new();
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter,
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_not_exact_match() {
        let filter = PropertyFilter::property("p10").eq("Paper");
        let expected_results: Vec<&str> = vec![];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter,
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_property_eq() {
        let filter = PropertyFilter::property("p2").eq(2u64);
        let expected_results = vec!["2"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter,
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_property_ne() {
        let filter = PropertyFilter::property("p2").ne(2u64);
        let expected_results = vec!["3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter,
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_property_lt() {
        let filter = PropertyFilter::property("p2").lt(10u64);
        let expected_results = vec!["2", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter,
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_property_le() {
        let filter = PropertyFilter::property("p2").le(6u64);
        let expected_results = vec!["2", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter,
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_property_gt() {
        let filter = PropertyFilter::property("p2").gt(2u64);
        let expected_results = vec!["3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter,
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_property_ge() {
        let filter = PropertyFilter::property("p2").ge(2u64);
        let expected_results = vec!["2", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter,
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_property_in() {
        let filter = PropertyFilter::property("p2").is_in(vec![Prop::U64(6)]);
        let expected_results = vec!["3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter,
            &expected_results,
            TestVariants::All,
        );

        let filter = PropertyFilter::property("p2").is_in(vec![Prop::U64(2), Prop::U64(6)]);
        let expected_results = vec!["2", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter,
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_property_not_in() {
        let filter = PropertyFilter::property("p2").is_not_in(vec![Prop::U64(6)]);
        let expected_results = vec!["2"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter,
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_property_is_some() {
        let filter = PropertyFilter::property("p2").is_some();
        let expected_results = vec!["2", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter,
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_property_is_none() {
        let filter = PropertyFilter::property("p2").is_none();
        let expected_results = vec!["1", "4"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter,
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_property_contains() {
        let filter = PropertyFilter::property("p10").contains("Paper");
        let expected_results: Vec<&str> = vec!["1", "2", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter,
            &expected_results,
            TestVariants::All,
        );

        let filter = PropertyFilter::property("p10")
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
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter,
            &expected_results,
            TestVariants::All,
        );

        let filter = PropertyFilter::property("p10")
            .temporal()
            .latest()
            .contains("Paper");
        let expected_results: Vec<&str> = vec!["1", "2", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter,
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_property_contains_not() {
        let filter = PropertyFilter::property("p10").not_contains("ship");
        let expected_results: Vec<&str> = vec!["1", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter,
            &expected_results,
            TestVariants::All,
        );

        let filter = PropertyFilter::property("p10")
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
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter,
            &expected_results,
            TestVariants::All,
        );

        let filter = PropertyFilter::property("p10")
            .temporal()
            .latest()
            .not_contains("ship");
        let expected_results: Vec<&str> = vec!["1", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter,
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_not_property() {
        let filter = NotFilter(PropertyFilter::property("p10").contains("Paper"));
        let expected_results: Vec<&str> = vec!["4"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter,
            &expected_results,
            TestVariants::All,
        );

        let filter = PropertyFilter::property("p10").contains("Paper").not();
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter,
            &expected_results,
            TestVariants::All,
        );
    }
}

#[cfg(test)]
mod test_edge_property_filter {
    use raphtory::db::graph::{
        assertions::{
            assert_filter_edges_results, assert_search_edges_results, TestGraphVariants,
            TestVariants,
        },
        views::filter::model::{
            property_filter::PropertyFilter, ComposableFilter, PropertyFilterOps,
        },
    };
    use raphtory_api::core::entities::properties::prop::Prop;

    use crate::{init_edges_graph, IdentityGraphTransformer};

    #[test]
    fn test_filter_edges_for_property_eq() {
        // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
        let filter = PropertyFilter::property("p2").eq(2u64);
        let expected_results = vec!["2->3"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_property_ne() {
        // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
        let filter = PropertyFilter::property("p2").ne(2u64);
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
            TestVariants::EventOnly,
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_property_lt() {
        // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
        let filter = PropertyFilter::property("p2").lt(10u64);
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
            TestVariants::EventOnly,
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_property_le() {
        // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
        let filter = PropertyFilter::property("p2").le(6u64);
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
            TestVariants::EventOnly,
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_property_gt() {
        // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
        let filter = PropertyFilter::property("p2").gt(2u64);
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
            TestVariants::EventOnly,
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_property_ge() {
        // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
        let filter = PropertyFilter::property("p2").ge(2u64);
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
            TestVariants::EventOnly,
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_property_in() {
        // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
        let filter = PropertyFilter::property("p2").is_in(vec![Prop::U64(6)]);
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
            TestVariants::EventOnly,
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = PropertyFilter::property("p2").is_in(vec![Prop::U64(2), Prop::U64(6)]);
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
            TestVariants::EventOnly,
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_property_not_in() {
        // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
        let filter = PropertyFilter::property("p2").is_not_in(vec![Prop::U64(6)]);
        let expected_results = vec!["1->2", "2->3"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_property_is_some() {
        // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
        let filter = PropertyFilter::property("p2").is_some();
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
            TestVariants::EventOnly,
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_property_is_none() {
        // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for both filter_edges and search_edges. Search API uses filter API internally for this filter.
        let filter = PropertyFilter::property("p2").is_none();
        let expected_results = Vec::<&str>::new();
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_filter_edges_for_property_contains() {
        // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
        let filter = PropertyFilter::property("p10").contains("Paper");
        let expected_results: Vec<&str> = vec!["1->2", "2->1", "2->3"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = PropertyFilter::property("p10")
            .temporal()
            .any()
            .contains("Paper");
        let expected_results: Vec<&str> = vec!["1->2", "2->1", "2->3"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = PropertyFilter::property("p10")
            .temporal()
            .latest()
            .contains("Paper");
        let expected_results: Vec<&str> = vec!["1->2", "2->1", "2->3"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_property_contains_not() {
        // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
        let filter = PropertyFilter::property("p10").not_contains("ship");
        let expected_results: Vec<&str> = vec!["1->2", "2->1"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = PropertyFilter::property("p10")
            .temporal()
            .any()
            .not_contains("ship");
        let expected_results: Vec<&str> = vec!["1->2", "2->1"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = PropertyFilter::property("p10")
            .temporal()
            .latest()
            .not_contains("ship");
        let expected_results: Vec<&str> = vec!["1->2", "2->1"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_by_fuzzy_search() {
        // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for both filter_edges and search_edges.
        // TODO: Enable these test for event_disk_graph, persistent_disk_graph once string property is fixed.
        let filter = PropertyFilter::property("p1").fuzzy_search("shiv", 2, true);
        let expected_results: Vec<&str> = vec!["1->2"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            vec![TestGraphVariants::Graph],
        );

        let filter = PropertyFilter::property("p1").fuzzy_search("ShiV", 2, true);
        let expected_results: Vec<&str> = vec!["1->2"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            vec![TestGraphVariants::Graph],
        );

        let filter = PropertyFilter::property("p1").fuzzy_search("shiv", 2, false);
        let expected_results: Vec<&str> = vec![];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            vec![TestGraphVariants::Graph],
        );
    }

    #[test]
    fn test_filter_edges_for_not_property() {
        // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for both filter_edges and search_edges. Search API uses filter API internally for this filter.
        let filter = PropertyFilter::property("p2").ne(2u64).not();
        let expected_results = vec!["2->3"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }
}

#[cfg(test)]
mod test_node_filter {
    use raphtory::db::graph::{
        assertions::{assert_filter_nodes_results, assert_search_nodes_results, TestVariants},
        views::filter::model::{ComposableFilter, NodeFilter, NodeFilterBuilderOps},
    };

    use crate::{init_nodes_graph, IdentityGraphTransformer};

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
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter,
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
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter,
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_node_name_in() {
        let filter = NodeFilter::name().is_in(vec!["1".into()]);
        let expected_results = vec!["1"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter,
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter::name().is_in(vec!["".into()]);
        let expected_results = Vec::<&str>::new();
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter,
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter::name().is_in(vec!["2".into(), "3".into()]);
        let expected_results = vec!["2", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter,
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_node_name_not_in() {
        let filter = NodeFilter::name().is_not_in(vec!["1".into()]);
        let expected_results = vec!["2", "3", "4"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter,
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter::name().is_not_in(vec!["".into()]);
        let expected_results = vec!["1", "2", "3", "4"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter,
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
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter,
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
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter,
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_node_type_in() {
        let filter = NodeFilter::node_type().is_in(vec!["fire_nation".into()]);
        let expected_results = vec!["1", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter,
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter::node_type().is_in(vec!["fire_nation".into(), "air_nomads".into()]);
        let expected_results = vec!["1", "2", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter,
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_nodes_for_node_type_not_in() {
        let filter = NodeFilter::node_type().is_not_in(vec!["fire_nation".into()]);
        let expected_results = vec!["2", "4"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter,
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
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter,
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
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter,
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
        let filter = NodeFilter::node_type()
            .is_not_in(vec!["fire_nation".into()])
            .not();
        let expected_results = vec!["1", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter,
            &expected_results,
            TestVariants::All,
        );
    }
}

#[cfg(test)]
mod test_node_composite_filter {
    use raphtory_api::core::Direction;

    use raphtory::db::graph::views::filter::model::{
        property_filter::PropertyFilter, AsNodeFilter, ComposableFilter, NodeFilter,
        NodeFilterBuilderOps, PropertyFilterOps,
    };

    use raphtory::db::graph::assertions::{
        assert_filter_neighbours_results, assert_filter_nodes_results, assert_search_nodes_results,
        TestVariants,
    };

    use crate::{init_edges_graph, init_nodes_graph, IdentityGraphTransformer};

    #[test]
    fn test_filter_nodes_by_props_added_at_different_times() {
        let filter = PropertyFilter::property("p4")
            .eq("pometry")
            .and(PropertyFilter::property("p5").eq(12u64));
        let expected_results = vec!["4"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter,
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_unique_results_from_composite_filters() {
        let filter = PropertyFilter::property("p2")
            .ge(2u64)
            .and(PropertyFilter::property("p2").ge(1u64));
        let expected_results = vec!["2", "3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = PropertyFilter::property("p2")
            .ge(2u64)
            .or(PropertyFilter::property("p2").ge(5u64));
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
        let filter = PropertyFilter::property("p2")
            .eq(2u64)
            .and(PropertyFilter::property("p1").eq("kapoor"));
        let expected_results = Vec::<&str>::new();
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        let filter = filter.as_node_filter();
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = PropertyFilter::property("p2")
            .eq(2u64)
            .or(PropertyFilter::property("p1").eq("shivam_kapoor"));
        let expected_results = vec!["1", "2"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        let filter = filter.as_node_filter();
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = PropertyFilter::property("p1")
            .eq("pometry")
            .or(PropertyFilter::property("p2")
                .eq(6u64)
                .and(PropertyFilter::property("p3").eq(1u64)));
        let expected_results = vec!["3"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        let filter = filter.as_node_filter();
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter::node_type()
            .eq("fire_nation")
            .and(PropertyFilter::property("p1").eq("prop1"));
        let expected_results = Vec::<&str>::new();
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        let filter = filter.as_node_filter();
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = PropertyFilter::property("p9")
            .eq(5u64)
            .and(PropertyFilter::property("p1").eq("shivam_kapoor"));
        let expected_results = vec!["1"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        let filter = filter.as_node_filter();
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter::node_type()
            .eq("fire_nation")
            .and(PropertyFilter::property("p1").eq("shivam_kapoor"));
        let expected_results = vec!["1"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        let filter = filter.as_node_filter();
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter::name()
            .eq("2")
            .and(PropertyFilter::property("p2").eq(2u64));
        let expected_results = vec!["2"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        let filter = filter.as_node_filter();
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter::name()
            .eq("2")
            .and(PropertyFilter::property("p2").eq(2u64))
            .or(PropertyFilter::property("p9").eq(5u64));
        let expected_results = vec!["1", "2"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        let filter = filter.as_node_filter();
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
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
            .and(PropertyFilter::property("p2").eq(2u64))
            .or(PropertyFilter::property("p9").eq(5u64))
            .not();
        let expected_results = vec!["3", "4"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = NodeFilter::name()
            .eq("2")
            .not()
            .and(PropertyFilter::property("p2").eq(2u64))
            .or(PropertyFilter::property("p9").eq(5u64));
        let expected_results = vec!["1"];
        assert_filter_nodes_results(
            init_nodes_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        assert_search_nodes_results(
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
            .and(PropertyFilter::property("p2").eq(2u64));
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
        let filter = PropertyFilter::property("p9").ge(1u64);
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
        let filter = PropertyFilter::property("p10").contains("Paper");
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

#[cfg(test)]
mod test_edge_filter {

    use raphtory::db::graph::{
        assertions::{assert_filter_edges_results, assert_search_edges_results, TestVariants},
        views::filter::model::{ComposableFilter, EdgeFilter, EdgeFilterOps},
    };

    use crate::{init_edges_graph, IdentityGraphTransformer};

    #[test]
    fn test_filter_edges_for_src_eq() {
        // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
        let filter = EdgeFilter::src().name().eq("3");
        let expected_results = vec!["3->1"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_src_ne() {
        // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
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
            TestVariants::EventOnly,
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_src_in() {
        // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
        let filter = EdgeFilter::src().name().is_in(vec!["1".into()]);
        let expected_results = vec!["1->2"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter::src().name().is_in(vec!["1".into(), "2".into()]);
        let expected_results = vec!["1->2", "2->1", "2->3"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_src_not_in() {
        // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
        let filter = EdgeFilter::src().name().is_not_in(vec!["1".into()]);
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
            TestVariants::EventOnly,
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_dst_eq() {
        // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
        let filter = EdgeFilter::dst().name().eq("2");
        let expected_results = vec!["1->2"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_dst_ne() {
        // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
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
            TestVariants::EventOnly,
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_dst_in() {
        // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
        let filter = EdgeFilter::dst().name().is_in(vec!["2".into()]);
        let expected_results = vec!["1->2"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter::dst().name().is_in(vec!["2".into(), "3".into()]);
        let expected_results = vec!["1->2", "2->3"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_dst_not_in() {
        // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
        let filter = EdgeFilter::dst().name().is_not_in(vec!["1".into()]);
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
            TestVariants::EventOnly,
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_src_contains() {
        // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
        let filter = EdgeFilter::src().name().contains("Mayer");
        let expected_results: Vec<&str> = vec!["John Mayer->Jimmy Page"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_src_contains_not() {
        // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
        let filter = EdgeFilter::src().name().not_contains("Mayer");
        let expected_results: Vec<&str> =
            vec!["1->2", "2->1", "2->3", "3->1", "David Gilmour->John Mayer"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_filter_edges_for_fuzzy_search() {
        // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
        let filter = EdgeFilter::src().name().fuzzy_search("John", 2, true);
        let expected_results: Vec<&str> = vec!["John Mayer->Jimmy Page"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = EdgeFilter::src().name().fuzzy_search("John", 2, false);
        let expected_results: Vec<&str> = vec![];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = EdgeFilter::src().name().fuzzy_search("John May", 2, false);
        let expected_results: Vec<&str> = vec!["John Mayer->Jimmy Page"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_filter_edges_for_not_src() {
        // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for both filter_edges and search_edges. Search API uses filter API internally for this filter.
        let filter = EdgeFilter::src().name().is_not_in(vec!["1".into()]).not();
        let expected_results = vec!["1->2"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }
}

#[cfg(test)]
mod test_edge_composite_filter {
    use raphtory::db::graph::{
        assertions::{
            assert_filter_edges_results, assert_search_edges_results, TestGraphVariants,
            TestVariants,
        },
        views::filter::model::{
            edge_filter::EdgeFieldFilter, property_filter::PropertyFilter, AndFilter, AsEdgeFilter,
            ComposableFilter, EdgeFilter, EdgeFilterOps, PropertyFilterOps,
        },
    };

    use crate::{init_edges_graph, IdentityGraphTransformer};

    #[test]
    fn test_filter_edge_for_src_dst() {
        // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
        let filter: AndFilter<EdgeFieldFilter, EdgeFieldFilter> = EdgeFilter::src()
            .name()
            .eq("3")
            .and(EdgeFilter::dst().name().eq("1"));
        let expected_results = vec!["3->1"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_unique_results_from_composite_filters() {
        // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
        let filter = PropertyFilter::property("p2")
            .ge(2u64)
            .and(PropertyFilter::property("p2").ge(1u64));
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
            TestVariants::EventOnly,
        );

        let filter = PropertyFilter::property("p2")
            .ge(2u64)
            .or(PropertyFilter::property("p2").ge(5u64));
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
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_composite_filter_edges() {
        // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for both filter_edges and search_edges.
        // TODO: Enable these test for event_disk_graph, persistent_disk_graph once string property is fixed.
        let filter = PropertyFilter::property("p2")
            .eq(2u64)
            .and(PropertyFilter::property("p1").eq("kapoor"));
        let expected_results = Vec::<&str>::new();
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            vec![TestGraphVariants::Graph],
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::NonDiskOnly,
        );
        let filter = filter.as_edge_filter();
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            vec![TestGraphVariants::Graph],
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::NonDiskOnly,
        );

        let filter = PropertyFilter::property("p2")
            .eq(2u64)
            .or(PropertyFilter::property("p1").eq("shivam_kapoor"));
        let expected_results = vec!["1->2", "2->3"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            vec![TestGraphVariants::Graph],
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::NonDiskOnly,
        );
        let filter = filter.as_edge_filter();
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            vec![TestGraphVariants::Graph],
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::NonDiskOnly,
        );

        let filter = PropertyFilter::property("p1")
            .eq("pometry")
            .or(PropertyFilter::property("p2")
                .eq(6u64)
                .and(PropertyFilter::property("p3").eq(1u64)));
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
            vec![TestGraphVariants::Graph],
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::NonDiskOnly,
        );
        let filter = filter.as_edge_filter();
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            vec![TestGraphVariants::Graph],
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::NonDiskOnly,
        );

        let filter = EdgeFilter::src()
            .name()
            .eq("13")
            .and(PropertyFilter::property("p1").eq("prop1"));
        let expected_results = Vec::<&str>::new();
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            vec![TestGraphVariants::Graph],
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::NonDiskOnly,
        );
        let filter = filter.as_edge_filter();
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            vec![TestGraphVariants::Graph],
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::NonDiskOnly,
        );

        let filter = PropertyFilter::property("p2")
            .eq(4u64)
            .and(PropertyFilter::property("p1").eq("shivam_kapoor"));
        let expected_results = vec!["1->2"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            vec![TestGraphVariants::Graph],
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::NonDiskOnly,
        );
        let filter = filter.as_edge_filter();
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            vec![TestGraphVariants::Graph],
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::NonDiskOnly,
        );

        let filter = EdgeFilter::src()
            .name()
            .eq("1")
            .and(PropertyFilter::property("p1").eq("shivam_kapoor"));
        let expected_results = vec!["1->2"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            vec![TestGraphVariants::Graph],
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::NonDiskOnly,
        );
        let filter = filter.as_edge_filter();
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            vec![TestGraphVariants::Graph],
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::NonDiskOnly,
        );

        let filter = EdgeFilter::dst()
            .name()
            .eq("1")
            .and(PropertyFilter::property("p2").eq(6u64));
        let expected_results = vec!["2->1", "3->1"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
        let filter = filter.as_edge_filter();
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );

        let filter = EdgeFilter::src()
            .name()
            .eq("1")
            .and(PropertyFilter::property("p1").eq("shivam_kapoor"))
            .or(PropertyFilter::property("p3").eq(5u64));
        let expected_results = vec!["1->2"];
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            vec![TestGraphVariants::Graph],
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::NonDiskOnly,
        );

        let filter = filter.as_edge_filter();
        assert_filter_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            vec![TestGraphVariants::Graph],
        );
        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::NonDiskOnly,
        );
    }

    #[test]
    fn test_not_composite_filter_edges() {
        // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for both filter_edges and search_edges. Search API uses filter API internally for this filter.
        let filter = EdgeFilter::src()
            .name()
            .eq("13")
            .and(PropertyFilter::property("p1").eq("prop1"))
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
            TestVariants::EventOnly,
        );

        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = EdgeFilter::src()
            .name()
            .eq("13")
            .and(PropertyFilter::property("p1").eq("prop1").not())
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
            TestVariants::EventOnly,
        );

        assert_search_edges_results(
            init_edges_graph,
            IdentityGraphTransformer,
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }
}
