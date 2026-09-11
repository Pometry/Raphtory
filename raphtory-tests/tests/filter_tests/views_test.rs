mod test_nodes_filters_window_graph {
    use raphtory::{
        db::{
            api::view::{filter_ops::Filter, StaticGraphViewOps},
            graph::views::filter::model::{
                node_filter::{NodeFilter, NodeFilterFactory},
                ComposableFilter, EntityExprFilterOps, PropertyExprFactory,
            },
        },
        errors::GraphError,
        prelude::{AdditionOps, Graph, GraphViewOps, PropertyAdditionOps, TimeOps},
    };
    use raphtory_api::core::{entities::properties::prop::Prop, storage::arc_str::ArcStr};
    use raphtory_storage::mutation::{
        addition_ops::InternalAdditionOps, property_addition_ops::InternalPropertyAdditionOps,
    };
    use raphtory_tests::assertions::{
        assert_filter_nodes_results, TestVariants, WindowGraphTransformer,
    };

    fn init_graph<G: StaticGraphViewOps + AdditionOps + PropertyAdditionOps>(graph: G) -> G {
        let nodes = vec![
            (
                6,
                "N1",
                vec![
                    ("p1", Prop::U64(2u64)),
                    ("k1", Prop::I64(2i64)),
                    ("k2", Prop::Str(ArcStr::from("Paper_Airplane"))),
                    ("k3", Prop::Bool(true)),
                    ("k4", Prop::F64(6.0f64)),
                ],
                Some("air_nomad"),
            ),
            (
                7,
                "N1",
                vec![
                    ("p1", Prop::U64(1u64)),
                    ("k1", Prop::I64(5i64)),
                    ("k3", Prop::Bool(false)),
                ],
                Some("air_nomad"),
            ),
            (
                6,
                "N2",
                vec![("p1", Prop::U64(1u64)), ("k4", Prop::F64(6.0f64))],
                Some("water_tribe"),
            ),
            (
                7,
                "N2",
                vec![
                    ("p1", Prop::U64(2u64)),
                    ("k1", Prop::I64(2i64)),
                    ("k2", Prop::Str(ArcStr::from("Paper_Ship"))),
                    ("k3", Prop::Bool(true)),
                    ("k4", Prop::F64(10.0f64)),
                ],
                Some("water_tribe"),
            ),
            (8, "N3", vec![("p1", Prop::U64(1u64))], Some("air_nomad")),
            (9, "N4", vec![("p1", Prop::U64(1u64))], Some("air_nomad")),
            (
                5,
                "N5",
                vec![
                    ("p1", Prop::U64(1u64)),
                    ("k1", Prop::I64(2i64)),
                    ("k2", Prop::Str(ArcStr::from("Paper_Airplane"))),
                    ("k3", Prop::Bool(true)),
                    ("k4", Prop::F64(6.0f64)),
                ],
                Some("air_nomad"),
            ),
            (
                6,
                "N5",
                vec![
                    ("p1", Prop::U64(2u64)),
                    ("k2", Prop::Str(ArcStr::from("Pometry"))),
                    ("k4", Prop::F64(1.0f64)),
                ],
                Some("air_nomad"),
            ),
            (5, "N6", vec![("p1", Prop::U64(1u64))], Some("fire_nation")),
            (
                6,
                "N6",
                vec![("p1", Prop::U64(1u64)), ("k4", Prop::F64(1.0f64))],
                Some("fire_nation"),
            ),
            (
                3,
                "N7",
                vec![
                    ("p1", Prop::U64(1u64)),
                    ("k1", Prop::I64(2i64)),
                    ("k2", Prop::Str(ArcStr::from("Paper_Ship"))),
                    ("k3", Prop::Bool(true)),
                    ("k4", Prop::F64(10.0f64)),
                ],
                Some("air_nomad"),
            ),
            (5, "N7", vec![("p1", Prop::U64(1u64))], Some("air_nomad")),
            (3, "N8", vec![("p1", Prop::U64(1u64))], Some("fire_nation")),
            (
                4,
                "N8",
                vec![
                    ("p1", Prop::U64(2u64)),
                    ("k1", Prop::I64(2i64)),
                    ("k2", Prop::Str(ArcStr::from("Sand_Clown"))),
                    ("k3", Prop::Bool(true)),
                    ("k4", Prop::F64(10.0f64)),
                ],
                Some("fire_nation"),
            ),
            (2, "N9", vec![("p1", Prop::U64(2u64))], None),
            (2, "N10", vec![("q1", Prop::U64(0u64))], None),
            (2, "N10", vec![("p1", Prop::U64(3u64))], None),
            (2, "N11", vec![("p1", Prop::U64(3u64))], None),
            (2, "N11", vec![("q1", Prop::U64(0u64))], None),
            (2, "N12", vec![("q1", Prop::U64(0u64))], None),
            (
                3,
                "N12",
                vec![
                    ("p1", Prop::U64(3u64)),
                    ("k1", Prop::I64(2i64)),
                    ("k2", Prop::Str(ArcStr::from("Sand_Clown"))),
                    ("k3", Prop::Bool(true)),
                    ("k4", Prop::F64(10.0f64)),
                ],
                None,
            ),
            (2, "N13", vec![("q1", Prop::U64(0u64))], None),
            (3, "N13", vec![("p1", Prop::U64(3u64))], None),
            (2, "N14", vec![("q1", Prop::U64(0u64))], None),
            (2, "N15", vec![], None),
        ];

        // Add nodes to the graph
        for (id, name, props, layer) in &nodes {
            graph
                .add_node(*id, name, props.clone(), *layer, None)
                .unwrap();
        }

        // Metadata property assignments
        let metadata = vec![
            (
                "N1",
                vec![
                    ("p1", Prop::U64(1u64)),
                    ("k1", Prop::I64(3i64)),
                    ("k2", Prop::Str(ArcStr::from("Paper_Airplane"))),
                    ("k3", Prop::Bool(true)),
                    ("k4", Prop::F64(6.0f64)),
                ],
            ),
            ("N4", vec![("p1", Prop::U64(2u64))]),
            ("N9", vec![("p1", Prop::U64(1u64))]),
            ("N10", vec![("p1", Prop::U64(1u64))]),
            ("N11", vec![("p1", Prop::U64(1u64))]),
            ("N12", vec![("p1", Prop::U64(1u64))]),
            (
                "N13",
                vec![
                    ("p1", Prop::U64(1u64)),
                    ("k1", Prop::I64(2i64)),
                    ("k2", Prop::Str(ArcStr::from("Sand_Clown"))),
                    ("k3", Prop::Bool(true)),
                    ("k4", Prop::F64(10.0f64)),
                ],
            ),
            ("N14", vec![("p1", Prop::U64(1u64))]),
            ("N15", vec![("p1", Prop::U64(1u64))]),
        ];

        // Apply metadata
        for (node, props) in metadata {
            graph.node(node).unwrap().add_metadata(props).unwrap();
        }

        graph
    }

    fn init_graph2<
        G: StaticGraphViewOps
            + AdditionOps
            + InternalAdditionOps
            + InternalPropertyAdditionOps
            + PropertyAdditionOps,
    >(
        graph: G,
    ) -> G {
        let nodes = vec![(
            2,
            "N14",
            vec![
                ("q1", Prop::U64(0u64)),
                (
                    "x",
                    Prop::list(vec![Prop::U64(1), Prop::U64(6), Prop::U64(9)]),
                ),
            ],
            None,
        )];

        // Add nodes to the graph
        for (id, name, props, layer) in &nodes {
            graph
                .add_node(*id, name, props.clone(), *layer, None)
                .unwrap();
        }

        graph
    }

    #[test]
    fn test_nodes_filters_for_node_name_eq() {
        let filter = NodeFilter.name().eq("N2");
        let expected_results = vec!["N2"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_nodes_filters_pg_for_node_name_eq() {
        let filter = NodeFilter.name().eq("N2");
        let expected_results = vec!["N2"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_nodes_filters_for_node_name_ne() {
        let filter = NodeFilter.name().ne("N2");
        let expected_results = vec!["N1", "N3", "N5", "N6"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_nodes_filters_pg_for_node_name_ne() {
        let filter = NodeFilter.name().ne("N2");
        let expected_results = vec![
            "N1", "N10", "N11", "N12", "N13", "N14", "N15", "N3", "N5", "N6", "N7", "N8", "N9",
        ];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_nodes_filters_for_node_name_in() {
        let filter = NodeFilter.name().is_in(vec!["N2"]);
        let expected_results = vec!["N2"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = NodeFilter.name().is_in(vec!["N2", "N5"]);
        let expected_results = vec!["N2", "N5"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_nodes_filters_pg_for_node_name_in() {
        let filter = NodeFilter.name().is_in(vec!["N2"]);
        let expected_results = vec!["N2"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = NodeFilter.name().is_in(vec!["N2", "N5"]);
        let expected_results = vec!["N2", "N5"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_nodes_filters_for_node_name_not_in() {
        let filter = NodeFilter.name().is_not_in(vec!["N5"]);
        let expected_results = vec!["N1", "N2", "N3", "N6"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_nodes_filters_pg_for_node_name_not_in() {
        let filter = NodeFilter.name().is_not_in(vec!["N5"]);
        let expected_results = vec![
            "N1", "N10", "N11", "N12", "N13", "N14", "N15", "N2", "N3", "N6", "N7", "N8", "N9",
        ];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_nodes_filters_for_node_type_eq() {
        let filter = NodeFilter.node_type().eq("fire_nation");
        let expected_results = vec!["N6"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_nodes_filters_pg_for_node_type_eq() {
        let filter = NodeFilter.node_type().eq("fire_nation");
        let expected_results = vec!["N6", "N8"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_nodes_filters_for_node_type_ne() {
        let filter = NodeFilter.node_type().ne("fire_nation");
        let expected_results = vec!["N1", "N2", "N3", "N5"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_nodes_filters_pg_for_node_type_ne() {
        let filter = NodeFilter.node_type().ne("fire_nation");
        let expected_results = vec![
            "N1", "N10", "N11", "N12", "N13", "N14", "N15", "N2", "N3", "N5", "N7", "N9",
        ];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_nodes_filters_for_node_type_in() {
        let filter = NodeFilter.node_type().is_in(vec!["fire_nation"]);
        let expected_results = vec!["N6"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = NodeFilter
            .node_type()
            .is_in(vec!["fire_nation", "air_nomad"]);
        let expected_results = vec!["N1", "N3", "N5", "N6"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_nodes_filters_pg_for_node_type_in() {
        let filter = NodeFilter.node_type().is_in(vec!["fire_nation"]);
        let expected_results = vec!["N6", "N8"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = NodeFilter
            .node_type()
            .is_in(vec!["fire_nation", "air_nomad"]);
        let expected_results = vec!["N1", "N3", "N5", "N6", "N7", "N8"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_nodes_filters_for_node_type_not_in() {
        // TODO: Enable event_disk_graph once bug fixed: https://github.com/Pometry/Raphtory/issues/2098
        let filter = NodeFilter.node_type().is_not_in(vec!["fire_nation"]);
        let expected_results = vec!["N1", "N2", "N3", "N5"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_nodes_filters_pg_for_node_type_not_in() {
        let filter = NodeFilter.node_type().is_not_in(vec!["fire_nation"]);
        let expected_results = vec![
            "N1", "N10", "N11", "N12", "N13", "N14", "N15", "N2", "N3", "N5", "N7", "N9",
        ];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_nodes_filters_for_property_eq() {
        let filter = NodeFilter.property("p1").eq(1u64);
        let expected_results = vec!["N1", "N3", "N6"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = NodeFilter.property("k1").eq(2i64);
        let expected_results = vec!["N2"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = NodeFilter.property("k2").eq("Paper_Airplane");
        let expected_results = vec!["N1"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = NodeFilter.property("k3").eq(true);
        let expected_results = vec!["N2"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = NodeFilter.property("k4").eq(6.0f64);
        let expected_results = vec!["N1"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter =
            NodeFilter
                .property("x")
                .eq(Prop::list(vec![Prop::U64(1), Prop::U64(6), Prop::U64(9)]));
        let expected_results = vec!["N14"];
        assert_filter_nodes_results(
            init_graph2,
            WindowGraphTransformer(1..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_nodes_filters_pg_for_property_eq() {
        let filter = NodeFilter.property("p1").eq(1u64);
        let expected_results = vec!["N1", "N3", "N6", "N7"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = NodeFilter.property("k1").eq(2i64);
        let expected_results = vec!["N12", "N2", "N5", "N7", "N8"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = NodeFilter.property("k2").eq("Paper_Airplane");
        let expected_results = vec!["N1"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = NodeFilter.property("k3").eq(true);
        let expected_results = vec!["N12", "N2", "N5", "N7", "N8"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = NodeFilter.property("k4").eq(6.0f64);
        let expected_results = vec!["N1"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter =
            NodeFilter
                .property("x")
                .eq(Prop::list(vec![Prop::U64(1), Prop::U64(6), Prop::U64(9)]));
        let expected_results = vec!["N14"];
        assert_filter_nodes_results(
            init_graph2,
            WindowGraphTransformer(1..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_nodes_filters_for_property_ne() {
        let filter = NodeFilter.property("p1").ne(1u64);
        let expected_results = vec!["N2", "N5"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = NodeFilter.property("k1").ne(2i64);
        let expected_results = vec!["N1"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = NodeFilter.property("k2").ne("Paper_Airplane");
        let expected_results = vec!["N2", "N5"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = NodeFilter.property("k3").ne(true);
        let expected_results = vec!["N1"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = NodeFilter.property("k4").ne(6.0f64);
        let expected_results = vec!["N2", "N5", "N6"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_nodes_filters_pg_for_property_ne() {
        let filter = NodeFilter.property("p1").ne(1u64);
        let expected_results = vec!["N10", "N11", "N12", "N13", "N2", "N5", "N8", "N9"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = NodeFilter.property("k1").ne(2i64);
        let expected_results = vec!["N1"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = NodeFilter.property("k2").ne("Paper_Airplane");
        let expected_results = vec!["N12", "N2", "N5", "N7", "N8"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = NodeFilter.property("k3").ne(true);
        let expected_results = vec!["N1"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = NodeFilter.property("k4").ne(6.0f64);
        let expected_results = vec!["N12", "N2", "N5", "N6", "N7", "N8"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_nodes_filters_for_property_lt() {
        let filter = NodeFilter.property("p1").lt(3u64);
        let expected_results = vec!["N1", "N2", "N3", "N5", "N6"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = NodeFilter.property("k1").lt(3i64);
        let expected_results = vec!["N2"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = NodeFilter.property("k4").lt(10.0f64);
        let expected_results = vec!["N1", "N5", "N6"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_nodes_filters_pg_for_property_lt() {
        let filter = NodeFilter.property("p1").lt(3u64);
        let expected_results = vec!["N1", "N2", "N3", "N5", "N6", "N7", "N8", "N9"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = NodeFilter.property("k1").lt(3i64);
        let expected_results = vec!["N12", "N2", "N5", "N7", "N8"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = NodeFilter.property("k4").lt(10.0f64);
        let expected_results = vec!["N1", "N5", "N6"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_nodes_filters_for_property_le() {
        let filter = NodeFilter.property("p1").le(1u64);
        let expected_results = vec!["N1", "N3", "N6"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = NodeFilter.property("k1").le(2i64);
        let expected_results = vec!["N2"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = NodeFilter.property("k4").le(6.0f64);
        let expected_results = vec!["N1", "N5", "N6"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_nodes_filters_pg_for_property_le() {
        let filter = NodeFilter.property("p1").le(1u64);
        let expected_results = vec!["N1", "N3", "N6", "N7"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = NodeFilter.property("k1").le(2i64);
        let expected_results = vec!["N12", "N2", "N5", "N7", "N8"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = NodeFilter.property("k4").le(6.0f64);
        let expected_results = vec!["N1", "N5", "N6"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_nodes_filters_for_property_gt() {
        let filter = NodeFilter.property("p1").gt(1u64);
        let expected_results = vec!["N2", "N5"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = NodeFilter.property("k1").gt(2i64);
        let expected_results = vec!["N1"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = NodeFilter.property("k4").gt(6.0f64);
        let expected_results = vec!["N2"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = NodeFilter.property("x").gt(Prop::List(
            vec![Prop::U64(1), Prop::U64(6), Prop::U64(9)].into(),
        ));
        let graph = init_graph(Graph::new());
        assert!(matches!(
            graph.window(1, 9).filter(filter.clone()).map(|_| ()).unwrap_err(),
            GraphError::PropertyMissingError(ref name) if name == "x"
        ));
        assert!(matches!(
            graph.persistent_graph().window(1, 9).filter(filter).map(|_| ()).unwrap_err(),
            GraphError::PropertyMissingError(ref name) if name == "x"
        ));
    }

    #[test]
    fn test_nodes_filters_pg_for_property_gt() {
        let filter = NodeFilter.property("p1").gt(1u64);
        let expected_results = vec!["N10", "N11", "N12", "N13", "N2", "N5", "N8", "N9"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = NodeFilter.property("k1").gt(2i64);
        let expected_results = vec!["N1"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = NodeFilter.property("k4").gt(6.0f64);
        let expected_results = vec!["N12", "N2", "N7", "N8"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_nodes_filters_for_property_ge() {
        let filter = NodeFilter.property("p1").ge(1u64);
        let expected_results = vec!["N1", "N2", "N3", "N5", "N6"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = NodeFilter.property("k1").ge(2i64);
        let expected_results = vec!["N1", "N2"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = NodeFilter.property("k4").ge(6.0f64);
        let expected_results = vec!["N1", "N2"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_nodes_filters_pg_for_property_ge() {
        let filter = NodeFilter.property("p1").ge(1u64);
        let expected_results = vec![
            "N1", "N10", "N11", "N12", "N13", "N2", "N3", "N5", "N6", "N7", "N8", "N9",
        ];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = NodeFilter.property("k1").ge(2i64);
        let expected_results = vec!["N1", "N12", "N2", "N5", "N7", "N8"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = NodeFilter.property("k4").ge(6.0f64);
        let expected_results = vec!["N1", "N12", "N2", "N7", "N8"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_nodes_filters_for_property_in() {
        let filter = NodeFilter.property("p1").is_in(vec![2u64]);
        let expected_results = vec!["N2", "N5"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = NodeFilter.property("k1").is_in(vec![2i64]);
        let expected_results = vec!["N2"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = NodeFilter.property("k2").is_in(vec!["Paper_Airplane"]);
        let expected_results = vec!["N1"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = NodeFilter.property("k3").is_in(vec![true]);
        let expected_results = vec!["N2"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = NodeFilter.property("k4").is_in(vec![6.0f64]);
        let expected_results = vec!["N1"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_nodes_filters_pg_for_property_in() {
        let filter = NodeFilter.property("p1").is_in(vec![2u64]);
        let expected_results = vec!["N2", "N5", "N8", "N9"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = NodeFilter.property("k1").is_in(vec![2i64]);
        let expected_results = vec!["N12", "N2", "N5", "N7", "N8"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = NodeFilter.property("k2").is_in(vec!["Paper_Airplane"]);
        let expected_results = vec!["N1"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = NodeFilter.property("k3").is_in(vec![true]);
        let expected_results = vec!["N12", "N2", "N5", "N7", "N8"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = NodeFilter.property("k4").is_in(vec![6.0f64]);
        let expected_results = vec!["N1"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_nodes_filters_for_property_not_in() {
        let filter = NodeFilter.property("p1").is_not_in(vec![1u64]);
        let expected_results = vec!["N2", "N5"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = NodeFilter.property("k1").is_not_in(vec![2i64]);
        let expected_results = vec!["N1"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = NodeFilter.property("k2").is_not_in(vec!["Paper_Airplane"]);
        let expected_results = vec!["N2", "N5"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = NodeFilter.property("k3").is_not_in(vec![true]);
        let expected_results = vec!["N1"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = NodeFilter.property("k4").is_not_in(vec![6.0f64]);
        let expected_results = vec!["N2", "N5", "N6"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_nodes_filters_pg_for_property_not_in() {
        let filter = NodeFilter.property("p1").is_not_in(vec![1u64]);
        let expected_results = vec!["N10", "N11", "N12", "N13", "N2", "N5", "N8", "N9"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = NodeFilter.property("k1").is_not_in(vec![2i64]);
        let expected_results = vec!["N1"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = NodeFilter.property("k2").is_not_in(vec!["Paper_Airplane"]);
        let expected_results = vec!["N12", "N2", "N5", "N7", "N8"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = NodeFilter.property("k3").is_not_in(vec![true]);
        let expected_results = vec!["N1"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = NodeFilter.property("k4").is_not_in(vec![6.0f64]);
        let expected_results = vec!["N12", "N2", "N5", "N6", "N7", "N8"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_nodes_filters_for_property_is_some() {
        let filter = NodeFilter.property("p1").is_some();
        let expected_results = vec!["N1", "N2", "N3", "N5", "N6"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let expected_results = Vec::<&str>::new();
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(1..2),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(10..12),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_nodes_filters_pg_for_property_is_some() {
        let filter = NodeFilter.property("p1").is_some();
        let expected_results = vec![
            "N1", "N10", "N11", "N12", "N13", "N2", "N3", "N5", "N6", "N7", "N8", "N9",
        ];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let expected_results = Vec::<&str>::new();
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(1..2),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let expected_results = vec![
            "N1", "N10", "N11", "N12", "N13", "N2", "N3", "N4", "N5", "N6", "N7", "N8", "N9",
        ];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(10..12),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_nodes_filters_for_props_added_at_different_times() {
        let filter = NodeFilter
            .property("q1")
            .eq(0u64)
            .and(NodeFilter.property("p1").eq(3u64));
        let expected_results = vec!["N10", "N11", "N12", "N13"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(1..4),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_nodes_filters_pg_for_props_added_at_different_times() {
        let filter = NodeFilter
            .property("q1")
            .eq(0u64)
            .and(NodeFilter.property("p1").eq(3u64));
        let expected_results = vec!["N10", "N11", "N12", "N13"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_nodes_filters_fuzzy_search() {
        let filter = NodeFilter
            .property("k2")
            .fuzzy_search("Paper_Airpla", 2, false);
        let expected_results = vec!["N1"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_nodes_filters_pg_fuzzy_search() {
        let filter = NodeFilter
            .property("k2")
            .fuzzy_search("Paper_Air", 5, false);
        let expected_results = vec!["N1", "N2", "N7"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_nodes_filters_fuzzy_search_prefix_match() {
        let filter = NodeFilter.property("k2").fuzzy_search("Pa", 2, true);
        let expected_results = vec!["N1", "N2"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = NodeFilter.property("k2").fuzzy_search("Pa", 2, false);
        let expected_results = Vec::<&str>::new();
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_nodes_filters_pg_fuzzy_search_prefix_match() {
        let filter = NodeFilter.property("k2").fuzzy_search("Pa", 2, true);
        let expected_results = vec!["N1", "N2", "N7"];
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = NodeFilter.property("k2").fuzzy_search("Pa", 2, false);
        let expected_results = Vec::<&str>::new();
        assert_filter_nodes_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }
}

mod test_edges_filters_window_graph {
    use raphtory::{
        db::{
            api::view::{filter_ops::Filter, StaticGraphViewOps},
            graph::views::filter::model::{
                edge_filter::EdgeFilter, node_filter::NodeFilterFactory, ComposableFilter,
                EntityExprFilterOps, PropertyExprFactory,
            },
        },
        errors::GraphError,
        prelude::{AdditionOps, Graph, GraphViewOps, PropertyAdditionOps, TimeOps, NO_PROPS},
    };
    use raphtory_api::core::{entities::properties::prop::Prop, storage::arc_str::ArcStr};
    use raphtory_tests::assertions::{
        assert_filter_edges_results, TestVariants, WindowGraphTransformer,
    };

    fn init_graph<G: StaticGraphViewOps + AdditionOps + PropertyAdditionOps>(graph: G) -> G {
        let edges = vec![
            (
                6,
                "N1",
                "N2",
                vec![
                    ("p1", Prop::U64(2u64)),
                    ("k1", Prop::I64(2i64)),
                    ("k2", Prop::Str(ArcStr::from("Paper_Airplane"))),
                    ("k3", Prop::Bool(true)),
                    ("k4", Prop::F64(6.0f64)),
                ],
                Some("air_nomad"),
            ),
            (
                7,
                "N1",
                "N2",
                vec![
                    ("p1", Prop::U64(1u64)),
                    ("k1", Prop::I64(5i64)),
                    ("k3", Prop::Bool(false)),
                ],
                Some("air_nomad"),
            ),
            (
                6,
                "N2",
                "N3",
                vec![("p1", Prop::U64(1u64)), ("k4", Prop::F64(6.0f64))],
                Some("water_tribe"),
            ),
            (
                7,
                "N2",
                "N3",
                vec![
                    ("p1", Prop::U64(2u64)),
                    ("k1", Prop::I64(2i64)),
                    ("k2", Prop::Str(ArcStr::from("Paper_Ship"))),
                    ("k3", Prop::Bool(true)),
                    ("k4", Prop::F64(10.0f64)),
                ],
                Some("water_tribe"),
            ),
            (
                8,
                "N3",
                "N4",
                vec![("p1", Prop::U64(1u64))],
                Some("air_nomad"),
            ),
            (
                9,
                "N4",
                "N5",
                vec![("p1", Prop::U64(1u64))],
                Some("air_nomad"),
            ),
            (
                5,
                "N5",
                "N6",
                vec![
                    ("p1", Prop::U64(1u64)),
                    ("k1", Prop::I64(2i64)),
                    ("k2", Prop::Str(ArcStr::from("Paper_Airplane"))),
                    ("k3", Prop::Bool(true)),
                    ("k4", Prop::F64(6.0f64)),
                ],
                Some("air_nomad"),
            ),
            (
                6,
                "N5",
                "N6",
                vec![
                    ("p1", Prop::U64(2u64)),
                    ("k2", Prop::Str(ArcStr::from("Pometry"))),
                    ("k4", Prop::F64(1.0f64)),
                ],
                Some("air_nomad"),
            ),
            (
                5,
                "N6",
                "N7",
                vec![("p1", Prop::U64(1u64))],
                Some("fire_nation"),
            ),
            (
                6,
                "N6",
                "N7",
                vec![("p1", Prop::U64(1u64)), ("k4", Prop::F64(1.0f64))],
                Some("fire_nation"),
            ),
            (
                3,
                "N7",
                "N8",
                vec![
                    ("p1", Prop::U64(1u64)),
                    ("k1", Prop::I64(2i64)),
                    ("k2", Prop::Str(ArcStr::from("Paper_Ship"))),
                    ("k3", Prop::Bool(true)),
                    ("k4", Prop::F64(10.0f64)),
                ],
                Some("air_nomad"),
            ),
            (
                5,
                "N7",
                "N8",
                vec![("p1", Prop::U64(1u64))],
                Some("air_nomad"),
            ),
            (
                3,
                "N8",
                "N9",
                vec![("p1", Prop::U64(1u64))],
                Some("fire_nation"),
            ),
            (
                4,
                "N8",
                "N9",
                vec![
                    ("p1", Prop::U64(2u64)),
                    ("k1", Prop::I64(2i64)),
                    ("k2", Prop::Str(ArcStr::from("Sand_Clown"))),
                    ("k3", Prop::Bool(true)),
                    ("k4", Prop::F64(10.0f64)),
                ],
                Some("fire_nation"),
            ),
            (2, "N9", "N10", vec![("p1", Prop::U64(2u64))], None),
            (2, "N10", "N11", vec![("q1", Prop::U64(0u64))], None),
            (2, "N10", "N11", vec![("p1", Prop::U64(3u64))], None),
            (2, "N11", "N12", vec![("p1", Prop::U64(3u64))], None),
            (2, "N11", "N12", vec![("q1", Prop::U64(0u64))], None),
            (2, "N12", "N13", vec![("q1", Prop::U64(0u64))], None),
            (
                3,
                "N12",
                "N13",
                vec![
                    ("p1", Prop::U64(3u64)),
                    ("k1", Prop::I64(2i64)),
                    ("k2", Prop::Str(ArcStr::from("Sand_Clown"))),
                    ("k3", Prop::Bool(true)),
                    ("k4", Prop::F64(10.0f64)),
                ],
                None,
            ),
            (2, "N13", "N14", vec![("q1", Prop::U64(0u64))], None),
            (3, "N13", "N14", vec![("p1", Prop::U64(3u64))], None),
            (2, "N14", "N15", vec![("q1", Prop::U64(0u64))], None),
            (2, "N15", "N1", vec![], None),
        ];

        for (id, src, dst, props, layer) in &edges {
            graph
                .add_edge(*id, src, dst, props.clone(), *layer)
                .unwrap();
        }

        // Metadata property assignments
        let metadata = vec![
            (
                "N1",
                "N2",
                vec![
                    ("p1", Prop::U64(1u64)),
                    ("k1", Prop::I64(3i64)),
                    ("k2", Prop::Str(ArcStr::from("Paper_Airplane"))),
                    ("k3", Prop::Bool(true)),
                    ("k4", Prop::F64(6.0f64)),
                ],
                Some("air_nomad"),
            ),
            ("N4", "N5", vec![("p1", Prop::U64(2u64))], Some("air_nomad")),
            ("N9", "N10", vec![("p1", Prop::U64(1u64))], None),
            ("N10", "N11", vec![("p1", Prop::U64(1u64))], None),
            ("N11", "N12", vec![("p1", Prop::U64(1u64))], None),
            ("N12", "N13", vec![("p1", Prop::U64(1u64))], None),
            (
                "N13",
                "N14",
                vec![
                    ("p1", Prop::U64(1u64)),
                    ("k1", Prop::I64(2i64)),
                    ("k2", Prop::Str(ArcStr::from("Sand_Clown"))),
                    ("k3", Prop::Bool(true)),
                    ("k4", Prop::F64(10.0f64)),
                ],
                None,
            ),
            ("N14", "N15", vec![("p1", Prop::U64(1u64))], None),
            ("N15", "N1", vec![("p1", Prop::U64(1u64))], None),
        ];

        for (src, dst, props, layer) in metadata {
            graph
                .edge(src, dst)
                .unwrap()
                .add_metadata(props, layer)
                .unwrap();
        }

        graph.add_node(1, "N1", NO_PROPS, None, None).unwrap();
        graph.add_node(2, "N2", NO_PROPS, None, None).unwrap();
        graph.add_node(3, "N3", NO_PROPS, None, None).unwrap();

        graph
    }

    fn init_graph2<G: StaticGraphViewOps + AdditionOps + PropertyAdditionOps>(graph: G) -> G {
        let edges = vec![(
            2,
            "N14",
            "N15",
            vec![
                ("q1", Prop::U64(0u64)),
                (
                    "x",
                    Prop::list(vec![Prop::U64(1), Prop::U64(6), Prop::U64(9)]),
                ),
            ],
            None,
        )];

        for (id, src, dst, props, layer) in &edges {
            graph
                .add_edge(*id, src, dst, props.clone(), *layer)
                .unwrap();
        }

        graph
    }

    #[test]
    fn test_edges_filters_for_src_eq() {
        let filter = EdgeFilter::src().name().eq("N2");
        let expected_results = vec!["N2->N3"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_edges_filters_pg_for_src_eq() {
        let filter = EdgeFilter::src().name().eq("N2");
        let expected_results = vec!["N2->N3"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_edges_filters_for_src_ne() {
        let filter = EdgeFilter::src().name().ne("N2");
        let expected_results = vec!["N1->N2", "N3->N4", "N5->N6", "N6->N7"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_edges_filters_pg_for_src_ne() {
        let filter = EdgeFilter::src().name().ne("N2");
        let expected_results = vec![
            "N1->N2", "N10->N11", "N11->N12", "N12->N13", "N13->N14", "N14->N15", "N15->N1",
            "N3->N4", "N5->N6", "N6->N7", "N7->N8", "N8->N9", "N9->N10",
        ];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_edges_filters_for_dst_in() {
        let filter = EdgeFilter::dst().name().is_in(vec!["N2"]);
        let expected_results = vec!["N1->N2"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = EdgeFilter::dst().name().is_in(vec!["N2", "N5"]);
        let expected_results = vec!["N1->N2"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_edges_filters_pg_for_dst_in() {
        let filter = EdgeFilter::dst().name().is_in(vec!["N2"]);
        let expected_results = vec!["N1->N2"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = EdgeFilter::dst().name().is_in(vec!["N2", "N5"]);
        let expected_results = vec!["N1->N2"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_edges_filters_for_dst_not_in() {
        let filter = EdgeFilter::dst().name().is_not_in(vec!["N5"]);
        let expected_results = vec!["N1->N2", "N2->N3", "N3->N4", "N5->N6", "N6->N7"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_edges_filters_pg_for_dst_not_in() {
        let filter = EdgeFilter::dst().name().is_not_in(vec!["N5"]);
        let expected_results = vec![
            "N1->N2", "N10->N11", "N11->N12", "N12->N13", "N13->N14", "N14->N15", "N15->N1",
            "N2->N3", "N3->N4", "N5->N6", "N6->N7", "N7->N8", "N8->N9", "N9->N10",
        ];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_edges_filters_for_property_eq() {
        let filter = EdgeFilter.property("p1").eq(1u64);
        let expected_results = vec!["N1->N2", "N3->N4", "N6->N7"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = EdgeFilter.property("k1").eq(2i64);
        let expected_results = vec!["N2->N3"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = EdgeFilter.property("k2").eq("Paper_Airplane");
        let expected_results = vec!["N1->N2"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = EdgeFilter.property("k3").eq(true);
        let expected_results = vec!["N2->N3"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = EdgeFilter.property("k4").eq(6.0f64);
        let expected_results = vec!["N1->N2"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter =
            EdgeFilter
                .property("x")
                .eq(Prop::list(vec![Prop::U64(1), Prop::U64(6), Prop::U64(9)]));
        let expected_results = vec!["N14->N15"];
        assert_filter_edges_results(
            init_graph2,
            WindowGraphTransformer(1..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_edges_filters_pg_for_property_eq() {
        let filter = EdgeFilter.property("p1").eq(1u64);
        let expected_results = vec!["N1->N2", "N3->N4", "N6->N7", "N7->N8"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = EdgeFilter.property("k1").eq(2i64);

        let expected_results = vec!["N12->N13", "N2->N3", "N5->N6", "N7->N8", "N8->N9"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = EdgeFilter.property("k2").eq("Paper_Airplane");
        let expected_results = vec!["N1->N2"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = EdgeFilter.property("k3").eq(true);
        let expected_results = vec!["N12->N13", "N2->N3", "N5->N6", "N7->N8", "N8->N9"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = EdgeFilter.property("k4").eq(6.0f64);
        let expected_results = vec!["N1->N2"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter =
            EdgeFilter
                .property("x")
                .eq(Prop::list(vec![Prop::U64(1), Prop::U64(6), Prop::U64(9)]));
        let expected_results = vec!["N14->N15"];
        assert_filter_edges_results(
            init_graph2,
            WindowGraphTransformer(1..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_edges_filters_for_property_ne() {
        let filter = EdgeFilter.property("p1").ne(1u64);
        let expected_results = vec!["N2->N3", "N5->N6"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = EdgeFilter.property("k1").ne(2i64);
        let expected_results = vec!["N1->N2"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = EdgeFilter.property("k2").ne("Paper_Airplane");
        let expected_results = vec!["N2->N3", "N5->N6"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = EdgeFilter.property("k3").ne(true);
        let expected_results = vec!["N1->N2"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = EdgeFilter.property("k4").ne(6.0f64);
        let expected_results = vec!["N2->N3", "N5->N6", "N6->N7"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_edges_filters_pg_for_property_ne() {
        let filter = EdgeFilter.property("p1").ne(1u64);
        let expected_results = vec![
            "N10->N11", "N11->N12", "N12->N13", "N13->N14", "N2->N3", "N5->N6", "N8->N9", "N9->N10",
        ];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = EdgeFilter.property("k1").ne(2i64);
        let expected_results = vec!["N1->N2"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = EdgeFilter.property("k2").ne("Paper_Airplane");
        let expected_results = vec!["N12->N13", "N2->N3", "N5->N6", "N7->N8", "N8->N9"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = EdgeFilter.property("k3").ne(true);
        let expected_results = vec!["N1->N2"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = EdgeFilter.property("k4").ne(6.0f64);
        let expected_results = vec!["N12->N13", "N2->N3", "N5->N6", "N6->N7", "N7->N8", "N8->N9"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter =
            EdgeFilter
                .property("x")
                .ne(Prop::list(vec![Prop::U64(1), Prop::U64(6), Prop::U64(9)]));
        let expected_results = Vec::<&str>::new();
        assert_filter_edges_results(
            init_graph2,
            WindowGraphTransformer(1..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_edges_filters_for_property_lt() {
        let filter = EdgeFilter.property("p1").lt(3u64);
        let expected_results = vec!["N1->N2", "N2->N3", "N3->N4", "N5->N6", "N6->N7"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = EdgeFilter.property("k1").lt(3i64);
        let expected_results = vec!["N2->N3"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = EdgeFilter.property("k4").lt(10.0f64);
        let expected_results = vec!["N1->N2", "N5->N6", "N6->N7"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_edges_filters_pg_for_property_lt() {
        // TODO: Const properties not supported for disk_graph.
        let filter = EdgeFilter.property("p1").lt(3u64);
        let expected_results = vec![
            "N1->N2", "N2->N3", "N3->N4", "N5->N6", "N6->N7", "N7->N8", "N8->N9", "N9->N10",
        ];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = EdgeFilter.property("k1").lt(3i64);
        let expected_results = vec!["N12->N13", "N2->N3", "N5->N6", "N7->N8", "N8->N9"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = EdgeFilter.property("k4").lt(10.0f64);
        let expected_results = vec!["N1->N2", "N5->N6", "N6->N7"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_edges_filters_for_property_le() {
        let filter = EdgeFilter.property("p1").le(1u64);
        let expected_results = vec!["N1->N2", "N3->N4", "N6->N7"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = EdgeFilter.property("k1").le(2i64);
        let expected_results = vec!["N2->N3"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = EdgeFilter.property("k4").le(6.0f64);
        let expected_results = vec!["N1->N2", "N5->N6", "N6->N7"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_edges_filters_pg_for_property_le() {
        let filter = EdgeFilter.property("p1").le(1u64);
        let expected_results = vec!["N1->N2", "N3->N4", "N6->N7", "N7->N8"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = EdgeFilter.property("k1").le(2i64);
        let expected_results = vec!["N12->N13", "N2->N3", "N5->N6", "N7->N8", "N8->N9"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = EdgeFilter.property("k4").le(6.0f64);
        let expected_results = vec!["N1->N2", "N5->N6", "N6->N7"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_edges_filters_for_property_gt() {
        let filter = EdgeFilter.property("p1").gt(1u64);
        let expected_results = vec!["N2->N3", "N5->N6"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = EdgeFilter.property("k1").gt(2i64);
        let expected_results = vec!["N1->N2"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = EdgeFilter.property("k4").gt(6.0f64);
        let expected_results = vec!["N2->N3"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = EdgeFilter.property("x").gt(Prop::List(
            vec![Prop::U64(1), Prop::U64(6), Prop::U64(9)].into(),
        ));
        let graph = init_graph(Graph::new());
        assert!(matches!(
            graph.window(1, 9).filter(filter.clone()).map(|_| ()).unwrap_err(),
            GraphError::PropertyMissingError(ref name) if name == "x"
        ));
        assert!(matches!(
            graph.persistent_graph().window(1, 9).filter(filter).map(|_| ()).unwrap_err(),
            GraphError::PropertyMissingError(ref name) if name == "x"
        ));
    }

    #[test]
    fn test_edges_filters_pg_for_property_gt() {
        let filter = EdgeFilter.property("p1").gt(1u64);
        let expected_results = vec![
            "N10->N11", "N11->N12", "N12->N13", "N13->N14", "N2->N3", "N5->N6", "N8->N9", "N9->N10",
        ];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = EdgeFilter.property("k1").gt(2i64);
        let expected_results = vec!["N1->N2"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = EdgeFilter.property("k4").gt(6.0f64);
        let expected_results = vec!["N12->N13", "N2->N3", "N7->N8", "N8->N9"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_edges_filters_for_property_ge() {
        let filter = EdgeFilter.property("p1").ge(1u64);
        let expected_results = vec!["N1->N2", "N2->N3", "N3->N4", "N5->N6", "N6->N7"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = EdgeFilter.property("k1").ge(2i64);
        let expected_results = vec!["N1->N2", "N2->N3"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = EdgeFilter.property("k4").ge(6.0f64);
        let expected_results = vec!["N1->N2", "N2->N3"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_edges_filters_pg_for_property_ge() {
        let filter = EdgeFilter.property("p1").ge(1u64);
        let expected_results = vec![
            "N1->N2", "N10->N11", "N11->N12", "N12->N13", "N13->N14", "N2->N3", "N3->N4", "N5->N6",
            "N6->N7", "N7->N8", "N8->N9", "N9->N10",
        ];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = EdgeFilter.property("k1").ge(2i64);
        let expected_results = vec!["N1->N2", "N12->N13", "N2->N3", "N5->N6", "N7->N8", "N8->N9"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = EdgeFilter.property("k4").ge(6.0f64);
        let expected_results = vec!["N1->N2", "N12->N13", "N2->N3", "N7->N8", "N8->N9"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_edges_filters_for_property_in() {
        let filter = EdgeFilter.property("p1").is_in(vec![2u64]);
        let expected_results = vec!["N2->N3", "N5->N6"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = EdgeFilter.property("k1").is_in(vec![2i64]);
        let expected_results = vec!["N2->N3"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = EdgeFilter.property("k2").is_in(vec!["Paper_Airplane"]);
        let expected_results = vec!["N1->N2"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = EdgeFilter.property("k3").is_in(vec![true]);
        let expected_results = vec!["N2->N3"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = EdgeFilter.property("k4").is_in(vec![6.0f64]);
        let expected_results = vec!["N1->N2"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_edges_filters_pg_for_property_in() {
        let filter = EdgeFilter.property("p1").is_in(vec![2u64]);
        let expected_results = vec!["N2->N3", "N5->N6", "N8->N9", "N9->N10"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = EdgeFilter.property("k1").is_in(vec![2i64]);
        let expected_results = vec!["N12->N13", "N2->N3", "N5->N6", "N7->N8", "N8->N9"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = EdgeFilter.property("k2").is_in(vec!["Paper_Airplane"]);
        let expected_results = vec!["N1->N2"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = EdgeFilter.property("k3").is_in(vec![true]);
        let expected_results = vec!["N12->N13", "N2->N3", "N5->N6", "N7->N8", "N8->N9"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = EdgeFilter.property("k4").is_in(vec![6.0f64]);
        let expected_results = vec!["N1->N2"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_edges_filters_for_property_not_in() {
        let filter = EdgeFilter.property("p1").is_not_in(vec![1u64]);
        let expected_results = vec!["N2->N3", "N5->N6"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = EdgeFilter.property("k1").is_not_in(vec![2i64]);
        let expected_results = vec!["N1->N2"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = EdgeFilter.property("k2").is_not_in(vec!["Paper_Airplane"]);
        let expected_results = vec!["N2->N3", "N5->N6"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = EdgeFilter.property("k3").is_not_in(vec![true]);
        let expected_results = vec!["N1->N2"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = EdgeFilter.property("k4").is_not_in(vec![6.0f64]);
        let expected_results = vec!["N2->N3", "N5->N6", "N6->N7"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_edges_filters_pg_for_property_not_in() {
        let filter = EdgeFilter.property("p1").is_not_in(vec![1u64]);
        let expected_results = vec![
            "N10->N11", "N11->N12", "N12->N13", "N13->N14", "N2->N3", "N5->N6", "N8->N9", "N9->N10",
        ];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = EdgeFilter.property("k1").is_not_in(vec![2i64]);
        let expected_results = vec!["N1->N2"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = EdgeFilter.property("k2").is_not_in(vec!["Paper_Airplane"]);
        let expected_results = vec!["N12->N13", "N2->N3", "N5->N6", "N7->N8", "N8->N9"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = EdgeFilter.property("k3").is_not_in(vec![true]);
        let expected_results = vec!["N1->N2"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = EdgeFilter.property("k4").is_not_in(vec![6.0f64]);
        let expected_results = vec!["N12->N13", "N2->N3", "N5->N6", "N6->N7", "N7->N8", "N8->N9"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_edges_filters_for_property_is_some() {
        let filter = EdgeFilter.property("p1").is_some();
        let expected_results = vec!["N1->N2", "N2->N3", "N3->N4", "N5->N6", "N6->N7"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_edges_filters_pg_for_property_is_some() {
        let filter = EdgeFilter.property("p1").is_some();
        let expected_results = vec![
            "N1->N2", "N10->N11", "N11->N12", "N12->N13", "N13->N14", "N2->N3", "N3->N4", "N5->N6",
            "N6->N7", "N7->N8", "N8->N9", "N9->N10",
        ];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_edges_filters_for_src_dst() {
        let filter = EdgeFilter::src()
            .name()
            .eq("N1")
            .and(EdgeFilter::dst().name().eq("N2"));
        let expected_results = vec!["N1->N2"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::All,
        );
    }

    #[test]
    fn test_edges_filters_fuzzy_search() {
        let filter = EdgeFilter
            .property("k2")
            .fuzzy_search("Paper_Airpla", 2, false);
        let expected_results = vec!["N1->N2"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    #[ignore]
    fn test_edges_filters_pg_fuzzy_search() {
        let filter = EdgeFilter.property("k2").fuzzy_search("Paper_", 2, false);
        let expected_results = vec!["N1->N2", "N2->N3", "N7->N8"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }

    #[test]
    fn test_edges_filters_fuzzy_search_prefix_match() {
        let filter = EdgeFilter.property("k2").fuzzy_search("Pa", 2, true);
        let expected_results = vec!["N1->N2", "N2->N3"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );

        let filter = EdgeFilter.property("k2").fuzzy_search("Pa", 2, true);
        let expected_results = vec!["N1->N2", "N2->N3"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter.clone(),
            &expected_results,
            TestVariants::EventOnly,
        );
    }

    #[test]
    fn test_edges_filters_pg_fuzzy_search_prefix_match() {
        let filter = EdgeFilter.property("k2").fuzzy_search("Pa", 2, true);
        let expected_results = vec!["N1->N2", "N2->N3", "N7->N8"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter,
            &expected_results,
            TestVariants::PersistentOnly,
        );

        let filter = EdgeFilter
            .property("k2")
            .fuzzy_search("Paper_Airplan", 2, false);
        let expected_results = vec!["N1->N2"];
        assert_filter_edges_results(
            init_graph,
            WindowGraphTransformer(6..9),
            filter,
            &expected_results,
            TestVariants::PersistentOnly,
        );
    }
}
