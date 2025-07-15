use crate::db::graph::views::filter::model::{
    edge_filter::EdgeFieldFilter,
    node_filter::{NodeNameFilter, NodeTypeFilter},
    property_filter::PropertyFilter,
};

pub mod and_filtered_graph;
pub mod edge_field_filtered_graph;
pub mod edge_property_filtered_graph;
pub mod exploded_edge_property_filter;
pub(crate) mod internal;
pub mod model;
pub mod node_name_filtered_graph;
pub mod node_property_filtered_graph;
pub mod node_type_filtered_graph;
pub mod not_filtered_graph;
pub mod or_filtered_graph;

#[cfg(test)]
mod test_fluent_builder_apis {
    use crate::db::graph::views::filter::model::{
        edge_filter::CompositeEdgeFilter,
        node_filter::CompositeNodeFilter,
        property_filter::{PropertyFilter, PropertyRef, Temporal},
        AsEdgeFilter, AsNodeFilter, ComposableFilter, EdgeFilter, EdgeFilterOps, Filter,
        NodeFilter, NodeFilterBuilderOps, PropertyFilterOps,
    };

    #[test]
    fn test_node_property_filter_build() {
        let filter_expr = NodeFilter::property("p").eq("raphtory");
        let node_property_filter = filter_expr.as_node_filter();
        let node_property_filter2 = CompositeNodeFilter::Property(PropertyFilter::eq(
            PropertyRef::Property("p".to_string()),
            "raphtory",
        ));
        assert_eq!(node_property_filter, node_property_filter2);
    }

    #[test]
    fn test_node_const_property_filter_build() {
        let filter_expr = NodeFilter::property("p").constant().eq("raphtory");
        let node_property_filter = filter_expr.as_node_filter();
        let node_property_filter2 = CompositeNodeFilter::Property(PropertyFilter::eq(
            PropertyRef::ConstantProperty("p".to_string()),
            "raphtory",
        ));
        assert_eq!(node_property_filter, node_property_filter2);
    }

    #[test]
    fn test_node_any_temporal_property_filter_build() {
        let filter_expr = NodeFilter::property("p").temporal().any().eq("raphtory");
        let node_property_filter = filter_expr.as_node_filter();
        let node_property_filter2 = CompositeNodeFilter::Property(PropertyFilter::eq(
            PropertyRef::TemporalProperty("p".to_string(), Temporal::Any),
            "raphtory",
        ));
        assert_eq!(node_property_filter, node_property_filter2);
    }

    #[test]
    fn test_node_latest_temporal_property_filter_build() {
        let filter_expr = NodeFilter::property("p").temporal().latest().eq("raphtory");
        let node_property_filter = filter_expr.as_node_filter();
        let node_property_filter2 = CompositeNodeFilter::Property(PropertyFilter::eq(
            PropertyRef::TemporalProperty("p".to_string(), Temporal::Latest),
            "raphtory",
        ));
        assert_eq!(node_property_filter, node_property_filter2);
    }

    #[test]
    fn test_node_name_filter_build() {
        let filter_expr = NodeFilter::name().eq("raphtory");
        let node_property_filter = filter_expr.as_node_filter();
        let node_property_filter2 = CompositeNodeFilter::Node(Filter::eq("node_name", "raphtory"));
        assert_eq!(node_property_filter, node_property_filter2);
    }

    #[test]
    fn test_node_type_filter_build() {
        let filter_expr = NodeFilter::node_type().eq("raphtory");
        let node_property_filter = filter_expr.as_node_filter();
        let node_property_filter2 = CompositeNodeFilter::Node(Filter::eq("node_type", "raphtory"));
        assert_eq!(node_property_filter, node_property_filter2);
    }

    #[test]
    fn test_node_filter_composition() {
        let node_composite_filter = NodeFilter::name()
            .eq("fire_nation")
            .and(NodeFilter::property("p2").constant().eq(2u64))
            .and(NodeFilter::property("p1").eq(1u64))
            .and(
                NodeFilter::property("p3")
                    .temporal()
                    .any()
                    .eq(5u64)
                    .or(NodeFilter::property("p4").temporal().latest().eq(7u64)),
            )
            .or(NodeFilter::node_type().eq("raphtory"))
            .or(NodeFilter::property("p5").eq(9u64))
            .as_node_filter();

        let node_composite_filter2 = CompositeNodeFilter::Or(
            Box::new(CompositeNodeFilter::Or(
                Box::new(CompositeNodeFilter::And(
                    Box::new(CompositeNodeFilter::And(
                        Box::new(CompositeNodeFilter::And(
                            Box::new(CompositeNodeFilter::Node(Filter::eq(
                                "node_name",
                                "fire_nation",
                            ))),
                            Box::new(CompositeNodeFilter::Property(PropertyFilter::eq(
                                PropertyRef::ConstantProperty("p2".to_string()),
                                2u64,
                            ))),
                        )),
                        Box::new(CompositeNodeFilter::Property(PropertyFilter::eq(
                            PropertyRef::Property("p1".to_string()),
                            1u64,
                        ))),
                    )),
                    Box::new(CompositeNodeFilter::Or(
                        Box::new(CompositeNodeFilter::Property(PropertyFilter::eq(
                            PropertyRef::TemporalProperty("p3".to_string(), Temporal::Any),
                            5u64,
                        ))),
                        Box::new(CompositeNodeFilter::Property(PropertyFilter::eq(
                            PropertyRef::TemporalProperty("p4".to_string(), Temporal::Latest),
                            7u64,
                        ))),
                    )),
                )),
                Box::new(CompositeNodeFilter::Node(Filter::eq(
                    "node_type",
                    "raphtory",
                ))),
            )),
            Box::new(CompositeNodeFilter::Property(PropertyFilter::eq(
                PropertyRef::Property("p5".to_string()),
                9u64,
            ))),
        );

        assert_eq!(node_composite_filter, node_composite_filter2);
    }

    #[test]
    fn test_edge_src_filter_build() {
        let filter_expr = EdgeFilter::src().name().eq("raphtory");
        let edge_property_filter = filter_expr.as_edge_filter();
        let edge_property_filter2 = CompositeEdgeFilter::Edge(Filter::eq("src", "raphtory"));
        assert_eq!(edge_property_filter, edge_property_filter2);
    }

    #[test]
    fn test_edge_dst_filter_build() {
        let filter_expr = EdgeFilter::dst().name().eq("raphtory");
        let edge_property_filter = filter_expr.as_edge_filter();
        let edge_property_filter2 = CompositeEdgeFilter::Edge(Filter::eq("dst", "raphtory"));
        assert_eq!(edge_property_filter, edge_property_filter2);
    }

    #[test]
    fn test_edge_filter_composition() {
        let edge_composite_filter = EdgeFilter::src()
            .name()
            .eq("fire_nation")
            .and(EdgeFilter::property("p2").constant().eq(2u64))
            .and(EdgeFilter::property("p1").eq(1u64))
            .and(
                EdgeFilter::property("p3")
                    .temporal()
                    .any()
                    .eq(5u64)
                    .or(EdgeFilter::property("p4").temporal().latest().eq(7u64)),
            )
            .or(EdgeFilter::src().name().eq("raphtory"))
            .or(EdgeFilter::property("p5").eq(9u64))
            .as_edge_filter();

        let edge_composite_filter2 = CompositeEdgeFilter::Or(
            Box::new(CompositeEdgeFilter::Or(
                Box::new(CompositeEdgeFilter::And(
                    Box::new(CompositeEdgeFilter::And(
                        Box::new(CompositeEdgeFilter::And(
                            Box::new(CompositeEdgeFilter::Edge(Filter::eq("src", "fire_nation"))),
                            Box::new(CompositeEdgeFilter::Property(PropertyFilter::eq(
                                PropertyRef::ConstantProperty("p2".into()),
                                2u64,
                            ))),
                        )),
                        Box::new(CompositeEdgeFilter::Property(PropertyFilter::eq(
                            PropertyRef::Property("p1".into()),
                            1u64,
                        ))),
                    )),
                    Box::new(CompositeEdgeFilter::Or(
                        Box::new(CompositeEdgeFilter::Property(PropertyFilter::eq(
                            PropertyRef::TemporalProperty("p3".into(), Temporal::Any),
                            5u64,
                        ))),
                        Box::new(CompositeEdgeFilter::Property(PropertyFilter::eq(
                            PropertyRef::TemporalProperty("p4".into(), Temporal::Latest),
                            7u64,
                        ))),
                    )),
                )),
                Box::new(CompositeEdgeFilter::Edge(Filter::eq("src", "raphtory"))),
            )),
            Box::new(CompositeEdgeFilter::Property(PropertyFilter::eq(
                PropertyRef::Property("p5".into()),
                9u64,
            ))),
        );

        assert_eq!(edge_composite_filter, edge_composite_filter2);
    }
}

#[cfg(test)]
mod test_composite_filters {
    use crate::{
        db::graph::views::filter::model::{
            edge_filter::CompositeEdgeFilter,
            node_filter::CompositeNodeFilter,
            property_filter::{PropertyFilter, PropertyRef},
            EdgeFilter, Filter, NodeFilter, PropertyFilterOps,
        },
        prelude::IntoProp,
    };
    use raphtory_api::core::{entities::properties::prop::Prop, storage::arc_str::ArcStr};

    #[test]
    fn test_composite_node_filter() {
        assert_eq!(
            "p2 == 2",
            CompositeNodeFilter::Property(PropertyFilter::eq(
                PropertyRef::Property("p2".to_string()),
                2u64
            ))
            .to_string()
        );

        assert_eq!(
            "((((node_type NOT_IN [fire_nation, water_tribe] AND p2 == 2) AND p1 == 1) AND (p3 <= 5 OR p4 IN [2, 10])) OR (node_name == pometry OR p5 == 9))",
            CompositeNodeFilter::Or(Box::new(CompositeNodeFilter::And(
                Box::new(CompositeNodeFilter::And(
                    Box::new(CompositeNodeFilter::And(
                        Box::new(CompositeNodeFilter::Node(Filter::is_not_in(
                            "node_type",
                            vec!["fire_nation".into(), "water_tribe".into()],
                        ))),
                        Box::new(CompositeNodeFilter::Property(PropertyFilter::eq(
                            PropertyRef::Property("p2".to_string()),
                            2u64,
                        ))),
                    )),
                    Box::new(CompositeNodeFilter::Property(PropertyFilter::eq(
                        PropertyRef::Property("p1".to_string()),
                        1u64,
                    ))),
                )),
                Box::new(CompositeNodeFilter::Or(
                    Box::new(CompositeNodeFilter::Property(PropertyFilter::le(
                        PropertyRef::Property("p3".to_string()),
                        5u64,
                    ))),
                    Box::new(CompositeNodeFilter::Property(PropertyFilter::is_in(
                        PropertyRef::Property("p4".to_string()),
                        vec![Prop::U64(10), Prop::U64(2)],
                    ))),
                )),
            )),
                                    Box::new(CompositeNodeFilter::Or(
                                        Box::new(CompositeNodeFilter::Node(Filter::eq("node_name", "pometry"))),
                                        Box::new(CompositeNodeFilter::Property(PropertyFilter::eq(
                                            PropertyRef::Property("p5".to_string()),
                                            9u64,
                                        ))),
                                    )),
            ).to_string()
        );

        assert_eq!(
            "(name FUZZY_SEARCH(1,true) shivam AND nation FUZZY_SEARCH(1,false) air_nomad)",
            CompositeNodeFilter::And(
                Box::from(CompositeNodeFilter::Node(Filter::fuzzy_search(
                    "name", "shivam", 1, true
                ))),
                Box::from(CompositeNodeFilter::Property(PropertyFilter::fuzzy_search(
                    PropertyRef::Property("nation".to_string()),
                    "air_nomad",
                    1,
                    false,
                ))),
            )
            .to_string()
        );
    }

    #[test]
    fn test_composite_edge_filter() {
        assert_eq!(
            "p2 == 2",
            CompositeEdgeFilter::Property(PropertyFilter::eq(
                PropertyRef::Property("p2".to_string()),
                2u64
            ))
            .to_string()
        );

        assert_eq!(
            "((((edge_type NOT_IN [fire_nation, water_tribe] AND p2 == 2) AND p1 == 1) AND (p3 <= 5 OR p4 IN [2, 10])) OR (src == pometry OR p5 == 9))",
            CompositeEdgeFilter::Or(
                Box::new(CompositeEdgeFilter::And(
                    Box::new(CompositeEdgeFilter::And(
                        Box::new(CompositeEdgeFilter::And(
                            Box::new(CompositeEdgeFilter::Edge(Filter::is_not_in(
                                "edge_type",
                                vec!["fire_nation".into(), "water_tribe".into()],
                            ))),
                            Box::new(CompositeEdgeFilter::Property(PropertyFilter::eq(
                                PropertyRef::Property("p2".to_string()),
                                2u64,
                            ))),
                        )),
                        Box::new(CompositeEdgeFilter::Property(PropertyFilter::eq(
                            PropertyRef::Property("p1".to_string()),
                            1u64,
                        ))),
                    )),
                    Box::new(CompositeEdgeFilter::Or(
                        Box::new(CompositeEdgeFilter::Property(PropertyFilter::le(
                            PropertyRef::Property("p3".to_string()),
                            5u64,
                        ))),
                        Box::new(CompositeEdgeFilter::Property(PropertyFilter::is_in(
                            PropertyRef::Property("p4".to_string()),
                            vec![Prop::U64(10), Prop::U64(2)],
                        ))),
                    )),
                )),
                Box::new(CompositeEdgeFilter::Or(
                    Box::new(CompositeEdgeFilter::Edge(Filter::eq("src", "pometry"))),
                    Box::new(CompositeEdgeFilter::Property(PropertyFilter::eq(
                        PropertyRef::Property("p5".to_string()),
                        9u64,
                    ))),
                )),
            )
                .to_string()
        );

        assert_eq!(
            "(name FUZZY_SEARCH(1,true) shivam AND nation FUZZY_SEARCH(1,false) air_nomad)",
            CompositeEdgeFilter::And(
                Box::from(CompositeEdgeFilter::Edge(Filter::fuzzy_search(
                    "name", "shivam", 1, true
                ))),
                Box::from(CompositeEdgeFilter::Property(PropertyFilter::fuzzy_search(
                    PropertyRef::Property("nation".to_string()),
                    "air_nomad",
                    1,
                    false,
                ))),
            )
            .to_string()
        );
    }

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
        let filter = NodeFilter::property("prop").fuzzy_search("pomet", 2, false);
        assert!(filter.matches(Some(&Prop::Str(ArcStr::from("pometry")))));
    }

    #[test]
    fn test_fuzzy_search_property_prefix_match() {
        let filter = EdgeFilter::property("prop").fuzzy_search("pome", 2, false);
        assert!(!filter.matches(Some(&Prop::Str(ArcStr::from("pometry")))));

        let filter = EdgeFilter::property("prop").fuzzy_search("pome", 2, true);
        assert!(filter.matches(Some(&Prop::Str(ArcStr::from("pometry")))));
    }

    #[test]
    fn test_contains_match() {
        let filter = EdgeFilter::property("prop").contains("shivam");
        let res = filter.matches(Some(&Prop::Str(ArcStr::from("shivam_kapoor"))));
        assert!(res);
        let res = filter.matches(None);
        assert!(!res);

        let filter = EdgeFilter::property("prop").contains("am_ka");
        let res = filter.matches(Some(&Prop::Str(ArcStr::from("shivam_kapoor"))));
        assert!(res);
    }

    #[test]
    fn test_contains_not_match() {
        let filter = NodeFilter::property("prop").not_contains("shivam");
        let res = filter.matches(Some(&Prop::Str(ArcStr::from("shivam_kapoor"))));
        assert!(!res);
        let res = filter.matches(None);
        assert!(!res);
    }

    #[test]
    fn test_is_in_match() {
        let filter = NodeFilter::property("prop").is_in(vec!["shivam".into_prop()]);
        let res = filter.matches(Some(&Prop::Str(ArcStr::from("shivam"))));
        assert!(res);
        let res = filter.matches(None);
        assert!(!res);
    }

    #[test]
    fn test_is_not_in_match() {
        let filter = EdgeFilter::property("prop").is_not_in(vec!["shivam".into_prop()]);
        let res = filter.matches(Some(&Prop::Str(ArcStr::from("shivam"))));
        assert!(!res);
        let res = filter.matches(None);
        assert!(!res);
    }
}

#[cfg(test)]
pub(crate) mod test_filters {
    use crate::{
        db::api::view::StaticGraphViewOps,
        prelude::{AdditionOps, PropertyAdditionOps},
    };
    use raphtory_api::core::entities::properties::prop::IntoProp;
    use raphtory_storage::mutation::{
        addition_ops::InternalAdditionOps, property_addition_ops::InternalPropertyAdditionOps,
    };

    struct IdentityGraphTransformer;

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
            use crate::{
                db::{
                    api::view::StaticGraphViewOps,
                    graph::{
                        assertions::{
                            assert_filter_nodes_results, assert_search_nodes_results, TestVariants,
                        },
                        views::filter::{
                            model::{NodeFilter, PropertyFilterOps},
                            test_filters::IdentityGraphTransformer,
                        },
                    },
                },
                prelude::{AdditionOps, GraphViewOps, PropertyAdditionOps},
            };
            use raphtory_api::core::entities::properties::prop::Prop;
            use raphtory_storage::mutation::{
                addition_ops::InternalAdditionOps,
                property_addition_ops::InternalPropertyAdditionOps,
            };

            fn init_graph<G: StaticGraphViewOps + AdditionOps + PropertyAdditionOps>(
                graph: G,
            ) -> G {
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

                let constant_properties = [
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

                for (node, props) in constant_properties.iter() {
                    graph
                        .node(node)
                        .unwrap()
                        .add_constant_properties(props.clone())
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
            fn test_constant_semantics() {
                let filter = NodeFilter::property("p1").constant().eq(1u64);
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
                let filter = NodeFilter::property("p1").temporal().any().eq(1u64);
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
                let filter = NodeFilter::property("p1").temporal().any().eq(1u64);
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
                let filter = NodeFilter::property("p1").temporal().latest().eq(1u64);
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
                let filter = NodeFilter::property("p1").temporal().latest().eq(1u64);
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
                let filter = NodeFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1", "N14", "N15", "N3", "N4", "N6", "N7"];
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
                let filter = NodeFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1", "N14", "N15", "N16", "N3", "N4", "N6", "N7"];
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
            fn test_property_semantics_only_constant() {
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

                    let constant_properties = [
                        ("N1", [("p1", Prop::U64(1u64))]),
                        ("N2", [("p1", Prop::U64(1u64))]),
                    ];

                    for (node, props) in constant_properties.iter() {
                        graph
                            .node(node)
                            .unwrap()
                            .add_constant_properties(props.clone())
                            .unwrap();
                    }

                    graph
                }

                let filter = NodeFilter::property("p1").ge(1u64);
                let expected_results = vec!["N1", "N2"];
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
                // For this graph there won't be any constant property index for property name "p1".
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

                let filter = NodeFilter::property("p1").le(1u64);
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
            use crate::{
                db::{
                    api::view::{filter_ops::BaseFilterOps, EdgeViewOps, StaticGraphViewOps},
                    graph::{
                        assertions::{
                            assert_filter_edges_results, assert_search_edges_results,
                            TestGraphVariants, TestVariants,
                        },
                        views::filter::{
                            internal::CreateFilter,
                            model::{EdgeFilter, PropertyFilterOps},
                            test_filters::IdentityGraphTransformer,
                        },
                    },
                },
                prelude::{AdditionOps, Graph, GraphViewOps, NodeViewOps, PropertyAdditionOps},
            };
            use raphtory_api::core::entities::properties::prop::Prop;
            use raphtory_storage::mutation::{
                addition_ops::InternalAdditionOps,
                property_addition_ops::InternalPropertyAdditionOps,
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

                let constant_edges = [
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

                for (src, dst, props) in constant_edges {
                    graph
                        .edge(src, dst)
                        .unwrap()
                        .add_constant_properties(props.clone(), None)
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
            fn test_constant_semantics() {
                // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
                // TODO: Const properties not supported for disk_graph.
                let filter = EdgeFilter::property("p1").constant().eq(1u64);
                let expected_results = vec![
                    "N1->N2", "N10->N11", "N11->N12", "N12->N13", "N13->N14", "N14->N15",
                    "N15->N1", "N9->N10",
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
            fn test_constant_semantics2() {
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

                let filter = EdgeFilter::property("p1").constant().eq(1u64);
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
                edge.add_constant_properties([("z", true)], Some("fire_nation"))
                    .unwrap();
                let prop = graph
                    .edge("shivam", "kapoor")
                    .unwrap()
                    .properties()
                    .constant()
                    .get("z")
                    .unwrap();
                assert_eq!("{\"fire_nation\": true}", prop.to_string());

                let filter2 = EdgeFilter::property("z")
                    .constant()
                    .eq(Prop::map([("fire_nation", true)]));
                assert_eq!(filter_edges(&graph, filter2), vec!["shivam->kapoor"]);

                let filter = EdgeFilter::property("p1")
                    .constant()
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
                // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
                let filter = EdgeFilter::property("p1").temporal().any().eq(1u64);
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
                let filter = EdgeFilter::property("p1").temporal().any().lt(2u64);
                let expected_results = vec![
                    "N1->N2", "N16->N15", "N17->N16", "N2->N3", "N3->N4", "N4->N5", "N5->N6",
                    "N6->N7", "N7->N8", "N8->N9",
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
                let filter = EdgeFilter::property("p1").temporal().latest().eq(1u64);
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
                let filter = EdgeFilter::property("p1").temporal().latest().eq(1u64);
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
                let filter = EdgeFilter::property("p1").ge(2u64);
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
                let filter = EdgeFilter::property("p1").eq(1u64);
                let expected_results = vec![
                    "N1->N2", "N14->N15", "N15->N1", "N16->N15", "N3->N4", "N4->N5", "N6->N7",
                    "N7->N8",
                ];
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
            fn test_property_semantics_only_constant() {
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

                    let constant_edges = [
                        ("N1", "N2", vec![("p1", Prop::U64(1u64))]),
                        ("N2", "N3", vec![("p1", Prop::U64(1u64))]),
                    ];

                    for (src, dst, props) in constant_edges {
                        graph
                            .edge(src, dst)
                            .unwrap()
                            .add_constant_properties(props.clone(), None)
                            .unwrap();
                    }

                    graph
                }

                let filter = EdgeFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1->N2", "N2->N3"];
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
                // For this graph there won't be any constant property index for property name "p1".
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

                let filter = EdgeFilter::property("p1").eq(1u64);
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

    use crate::db::graph::assertions::GraphTransformer;

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
        use crate::db::graph::views::filter::{
            model::PropertyFilterOps, test_filters::init_nodes_graph,
        };
        use raphtory_api::core::entities::properties::prop::Prop;

        use crate::db::graph::{
            assertions::{assert_filter_nodes_results, assert_search_nodes_results, TestVariants},
            views::filter::{
                model::{ComposableFilter, NodeFilter, NotFilter},
                test_filters::IdentityGraphTransformer,
            },
        };

        #[test]
        fn test_exact_match() {
            let filter = NodeFilter::property("p10").eq("Paper_airplane");
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

            let filter = NodeFilter::property("p10").eq("");
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
            let filter = NodeFilter::property("p10").eq("Paper");
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
            let filter = NodeFilter::property("p2").eq(2u64);
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
            let filter = NodeFilter::property("p2").ne(2u64);
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
            let filter = NodeFilter::property("p2").lt(10u64);
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
            let filter = NodeFilter::property("p2").le(6u64);
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
            let filter = NodeFilter::property("p2").gt(2u64);
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
            let filter = NodeFilter::property("p2").ge(2u64);
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
            let filter = NodeFilter::property("p2").is_in(vec![Prop::U64(6)]);
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

            let filter = NodeFilter::property("p2").is_in(vec![Prop::U64(2), Prop::U64(6)]);
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
            let filter = NodeFilter::property("p2").is_not_in(vec![Prop::U64(6)]);
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
            let filter = NodeFilter::property("p2").is_some();
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
            let filter = NodeFilter::property("p2").is_none();
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
            let filter = NodeFilter::property("p10").contains("Paper");
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

            let filter = NodeFilter::property("p10")
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

            let filter = NodeFilter::property("p10")
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
            let filter = NodeFilter::property("p10").not_contains("ship");
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

            let filter = NodeFilter::property("p10")
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

            let filter = NodeFilter::property("p10")
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
            let filter = NotFilter(NodeFilter::property("p10").contains("Paper"));
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

            let filter = NodeFilter::property("p10").contains("Paper").not();
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
        use crate::db::graph::{
            assertions::{
                assert_filter_edges_results, assert_search_edges_results, TestGraphVariants,
                TestVariants,
            },
            views::filter::{
                model::{ComposableFilter, EdgeFilter, PropertyFilterOps},
                test_filters::{init_edges_graph, IdentityGraphTransformer},
            },
        };
        use raphtory_api::core::entities::properties::prop::Prop;

        #[test]
        fn test_filter_edges_for_property_eq() {
            // TODO: PropertyFilteringNotImplemented for variants persistent_graph, persistent_disk_graph for filter_edges.
            let filter = EdgeFilter::property("p2").eq(2u64);
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
            let filter = EdgeFilter::property("p2").ne(2u64);
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
            let filter = EdgeFilter::property("p2").lt(10u64);
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
            let filter = EdgeFilter::property("p2").le(6u64);
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
            let filter = EdgeFilter::property("p2").gt(2u64);
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
            let filter = EdgeFilter::property("p2").ge(2u64);
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
            let filter = EdgeFilter::property("p2").is_in(vec![Prop::U64(6)]);
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

            let filter = EdgeFilter::property("p2").is_in(vec![Prop::U64(2), Prop::U64(6)]);
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
            let filter = EdgeFilter::property("p2").is_not_in(vec![Prop::U64(6)]);
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
            let filter = EdgeFilter::property("p2").is_some();
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
            let filter = EdgeFilter::property("p2").is_none();
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
            let filter = EdgeFilter::property("p10").contains("Paper");
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

            let filter = EdgeFilter::property("p10")
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

            let filter = EdgeFilter::property("p10")
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
            let filter = EdgeFilter::property("p10").not_contains("ship");
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

            let filter = EdgeFilter::property("p10")
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

            let filter = EdgeFilter::property("p10")
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
            let filter = EdgeFilter::property("p1").fuzzy_search("shiv", 2, true);
            let expected_results: Vec<&str> = vec!["1->2"];
            assert_filter_edges_results(
                init_edges_graph,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                vec![TestGraphVariants::Graph],
            );

            let filter = EdgeFilter::property("p1").fuzzy_search("ShiV", 2, true);
            let expected_results: Vec<&str> = vec!["1->2"];
            assert_filter_edges_results(
                init_edges_graph,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                vec![TestGraphVariants::Graph],
            );

            let filter = EdgeFilter::property("p1").fuzzy_search("shiv", 2, false);
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
            let filter = EdgeFilter::property("p2").ne(2u64).not();
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
        use crate::db::graph::views::filter::test_filters::init_nodes_graph;

        use crate::db::graph::{
            assertions::{assert_filter_nodes_results, assert_search_nodes_results, TestVariants},
            views::filter::{
                model::{ComposableFilter, NodeFilter, NodeFilterBuilderOps},
                test_filters::IdentityGraphTransformer,
            },
        };

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

            let filter =
                NodeFilter::node_type().is_in(vec!["fire_nation".into(), "air_nomads".into()]);
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
        use crate::db::graph::views::filter::test_filters::{init_edges_graph, init_nodes_graph};
        use raphtory_api::core::Direction;

        use crate::db::graph::views::filter::model::{
            AsNodeFilter, ComposableFilter, NodeFilter, NodeFilterBuilderOps, PropertyFilterOps,
        };

        use crate::db::graph::{
            assertions::{
                assert_filter_neighbours_results, assert_filter_nodes_results,
                assert_search_nodes_results, TestVariants,
            },
            views::filter::test_filters::IdentityGraphTransformer,
        };

        #[test]
        fn test_filter_nodes_by_props_added_at_different_times() {
            let filter = NodeFilter::property("p4")
                .eq("pometry")
                .and(NodeFilter::property("p5").eq(12u64));
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
            let filter = NodeFilter::property("p2")
                .ge(2u64)
                .and(NodeFilter::property("p2").ge(1u64));
            let expected_results = vec!["2", "3"];
            assert_filter_nodes_results(
                init_nodes_graph,
                IdentityGraphTransformer,
                filter.clone(),
                &expected_results,
                TestVariants::All,
            );

            let filter = NodeFilter::property("p2")
                .ge(2u64)
                .or(NodeFilter::property("p2").ge(5u64));
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
            let filter = NodeFilter::property("p2")
                .eq(2u64)
                .and(NodeFilter::property("p1").eq("kapoor"));
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

            let filter = NodeFilter::property("p2")
                .eq(2u64)
                .or(NodeFilter::property("p1").eq("shivam_kapoor"));
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

            let filter = NodeFilter::property("p1")
                .eq("pometry")
                .or(NodeFilter::property("p2")
                    .eq(6u64)
                    .and(NodeFilter::property("p3").eq(1u64)));
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
                .and(NodeFilter::property("p1").eq("prop1"));
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

            let filter = NodeFilter::property("p9")
                .eq(5u64)
                .and(NodeFilter::property("p1").eq("shivam_kapoor"));
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
                .and(NodeFilter::property("p1").eq("shivam_kapoor"));
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
                .and(NodeFilter::property("p2").eq(2u64));
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
                .and(NodeFilter::property("p2").eq(2u64))
                .or(NodeFilter::property("p9").eq(5u64));
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
                .and(NodeFilter::property("p2").eq(2u64))
                .or(NodeFilter::property("p9").eq(5u64))
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
                .and(NodeFilter::property("p2").eq(2u64))
                .or(NodeFilter::property("p9").eq(5u64));
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
                .and(NodeFilter::property("p2").eq(2u64));
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
            let filter = NodeFilter::property("p9").ge(1u64);
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
            let filter = NodeFilter::property("p10").contains("Paper");
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
        use crate::db::graph::views::filter::test_filters::init_edges_graph;

        use crate::db::graph::{
            assertions::{assert_filter_edges_results, assert_search_edges_results, TestVariants},
            views::filter::{
                model::{ComposableFilter, EdgeFilter, EdgeFilterOps},
                test_filters::IdentityGraphTransformer,
            },
        };

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
        use crate::db::graph::{
            assertions::{
                assert_filter_edges_results, assert_search_edges_results, TestGraphVariants,
                TestVariants,
            },
            views::filter::{
                model::{
                    AndFilter, AsEdgeFilter, ComposableFilter, EdgeFilter, EdgeFilterOps,
                    PropertyFilterOps,
                },
                test_filters::init_edges_graph,
                EdgeFieldFilter,
            },
        };

        use crate::db::graph::views::filter::test_filters::IdentityGraphTransformer;

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
            let filter = EdgeFilter::property("p2")
                .ge(2u64)
                .and(EdgeFilter::property("p2").ge(1u64));
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

            let filter = EdgeFilter::property("p2")
                .ge(2u64)
                .or(EdgeFilter::property("p2").ge(5u64));
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
            let filter = EdgeFilter::property("p2")
                .eq(2u64)
                .and(EdgeFilter::property("p1").eq("kapoor"));
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

            let filter = EdgeFilter::property("p2")
                .eq(2u64)
                .or(EdgeFilter::property("p1").eq("shivam_kapoor"));
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

            let filter = EdgeFilter::property("p1")
                .eq("pometry")
                .or(EdgeFilter::property("p2")
                    .eq(6u64)
                    .and(EdgeFilter::property("p3").eq(1u64)));
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
                .and(EdgeFilter::property("p1").eq("prop1"));
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

            let filter = EdgeFilter::property("p2")
                .eq(4u64)
                .and(EdgeFilter::property("p1").eq("shivam_kapoor"));
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
                .and(EdgeFilter::property("p1").eq("shivam_kapoor"));
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
                .and(EdgeFilter::property("p2").eq(6u64));
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
                .and(EdgeFilter::property("p1").eq("shivam_kapoor"))
                .or(EdgeFilter::property("p3").eq(5u64));
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
                .and(EdgeFilter::property("p1").eq("prop1"))
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
                .and(EdgeFilter::property("p1").eq("prop1").not())
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
}
