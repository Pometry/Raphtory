#[cfg(test)]
mod test_fluent_builder_apis {
    use raphtory::db::graph::views::filter::model::{
        edge_filter::CompositeEdgeFilter,
        node_filter::CompositeNodeFilter,
        property_filter::{PropertyFilter, PropertyRef, Temporal},
        AsEdgeFilter, AsNodeFilter, ComposableFilter, EdgeFilter, EdgeFilterOps, Filter,
        NodeFilter, NodeFilterBuilderOps, PropertyFilterOps,
    };

    #[test]
    fn test_node_property_filter_build() {
        let filter_expr = PropertyFilter::property("p").eq("raphtory");
        let node_property_filter = filter_expr.as_node_filter();
        let node_property_filter2 = CompositeNodeFilter::Property(PropertyFilter::eq(
            PropertyRef::Property("p".to_string()),
            "raphtory",
        ));
        assert_eq!(node_property_filter, node_property_filter2);
    }

    #[test]
    fn test_node_metadata_filter_build() {
        let filter_expr = PropertyFilter::metadata("p").eq("raphtory");
        let node_property_filter = filter_expr.as_node_filter();
        let node_property_filter2 = CompositeNodeFilter::Property(PropertyFilter::eq(
            PropertyRef::Metadata("p".to_string()),
            "raphtory",
        ));
        assert_eq!(node_property_filter, node_property_filter2);
    }

    #[test]
    fn test_node_any_temporal_property_filter_build() {
        let filter_expr = PropertyFilter::property("p")
            .temporal()
            .any()
            .eq("raphtory");
        let node_property_filter = filter_expr.as_node_filter();
        let node_property_filter2 = CompositeNodeFilter::Property(PropertyFilter::eq(
            PropertyRef::TemporalProperty("p".to_string(), Temporal::Any),
            "raphtory",
        ));
        assert_eq!(node_property_filter, node_property_filter2);
    }

    #[test]
    fn test_node_latest_temporal_property_filter_build() {
        let filter_expr = PropertyFilter::property("p")
            .temporal()
            .latest()
            .eq("raphtory");
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
            .and(PropertyFilter::metadata("p2").eq(2u64))
            .and(PropertyFilter::property("p1").eq(1u64))
            .and(
                PropertyFilter::property("p3")
                    .temporal()
                    .any()
                    .eq(5u64)
                    .or(PropertyFilter::property("p4").temporal().latest().eq(7u64)),
            )
            .or(NodeFilter::node_type().eq("raphtory"))
            .or(PropertyFilter::property("p5").eq(9u64))
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
                                PropertyRef::Metadata("p2".to_string()),
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
            .and(PropertyFilter::metadata("p2").eq(2u64))
            .and(PropertyFilter::property("p1").eq(1u64))
            .and(
                PropertyFilter::property("p3")
                    .temporal()
                    .any()
                    .eq(5u64)
                    .or(PropertyFilter::property("p4").temporal().latest().eq(7u64)),
            )
            .or(EdgeFilter::src().name().eq("raphtory"))
            .or(PropertyFilter::property("p5").eq(9u64))
            .as_edge_filter();

        let edge_composite_filter2 = CompositeEdgeFilter::Or(
            Box::new(CompositeEdgeFilter::Or(
                Box::new(CompositeEdgeFilter::And(
                    Box::new(CompositeEdgeFilter::And(
                        Box::new(CompositeEdgeFilter::And(
                            Box::new(CompositeEdgeFilter::Edge(Filter::eq("src", "fire_nation"))),
                            Box::new(CompositeEdgeFilter::Property(PropertyFilter::eq(
                                PropertyRef::Metadata("p2".into()),
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
    use raphtory::{
        db::graph::views::filter::model::{
            edge_filter::CompositeEdgeFilter,
            node_filter::CompositeNodeFilter,
            property_filter::{PropertyFilter, PropertyRef},
            Filter,
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
        let filter = PropertyFilter::fuzzy_search(
            PropertyRef::Property("prop".to_string()),
            "pomet",
            2,
            false,
        );
        assert!(filter.matches(Some(&Prop::Str(ArcStr::from("pometry")))));
    }

    #[test]
    fn test_fuzzy_search_property_prefix_match() {
        let filter = PropertyFilter::fuzzy_search(
            PropertyRef::Property("prop".to_string()),
            "pome",
            2,
            false,
        );
        assert!(!filter.matches(Some(&Prop::Str(ArcStr::from("pometry")))));

        let filter = PropertyFilter::fuzzy_search(
            PropertyRef::Property("prop".to_string()),
            "pome",
            2,
            true,
        );
        assert!(filter.matches(Some(&Prop::Str(ArcStr::from("pometry")))));
    }

    #[test]
    fn test_contains_match() {
        let filter = PropertyFilter::contains(PropertyRef::Property("prop".to_string()), "shivam");

        let res = filter.matches(Some(&Prop::Str(ArcStr::from("shivam_kapoor"))));
        assert!(res);

        let res = filter.matches(None);
        assert!(!res);

        let filter = PropertyFilter::contains(PropertyRef::Property("prop".to_string()), "am_ka");

        let res = filter.matches(Some(&Prop::Str(ArcStr::from("shivam_kapoor"))));
        assert!(res);
    }

    #[test]
    fn test_contains_not_match() {
        let filter =
            PropertyFilter::not_contains(PropertyRef::Property("prop".to_string()), "shivam");

        let res = filter.matches(Some(&Prop::Str(ArcStr::from("shivam_kapoor"))));
        assert!(!res);

        let res = filter.matches(None);
        assert!(!res);
    }

    #[test]
    fn test_is_in_match() {
        let filter = PropertyFilter::is_in(
            PropertyRef::Property("prop".to_string()),
            ["shivam".into_prop()],
        );

        let res = filter.matches(Some(&Prop::Str(ArcStr::from("shivam"))));
        assert!(res);

        let res = filter.matches(None);
        assert!(!res);
    }

    #[test]
    fn test_is_not_in_match() {
        let filter = PropertyFilter::is_not_in(
            PropertyRef::Property("prop".to_string()),
            ["shivam".into_prop()],
        );

        let res = filter.matches(Some(&Prop::Str(ArcStr::from("shivam"))));
        assert!(!res);

        let res = filter.matches(None);
        assert!(!res);
    }
}
