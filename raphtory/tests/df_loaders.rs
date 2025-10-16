#[cfg(feature = "io")]
mod io_tests {
    use arrow::array::builder::{
        ArrayBuilder, Int64Builder, LargeStringBuilder, StringViewBuilder, UInt64Builder,
    };
    use itertools::Itertools;
    use proptest::proptest;
    use raphtory::{
        db::graph::graph::assert_graph_equal,
        errors::GraphError,
        io::arrow::{
            dataframe::{DFChunk, DFView},
            df_loaders::load_edges_from_df,
        },
        prelude::*,
        test_utils::{build_edge_list, build_edge_list_str},
    };
    use raphtory_storage::core_ops::CoreGraphOps;
    use tempfile::TempDir;

    fn build_df(
        chunk_size: usize,
        edges: &[(u64, u64, i64, String, i64)],
    ) -> DFView<impl Iterator<Item = Result<DFChunk, GraphError>>> {
        let chunks = edges.iter().chunks(chunk_size);
        let mut src_col = UInt64Builder::new();
        let mut dst_col = UInt64Builder::new();
        let mut time_col = Int64Builder::new();
        let mut str_prop_col = LargeStringBuilder::new();
        let mut int_prop_col = Int64Builder::new();
        let chunks = chunks
            .into_iter()
            .map(|chunk| {
                for (src, dst, time, str_prop, int_prop) in chunk {
                    src_col.append_value(*src);
                    dst_col.append_value(*dst);
                    time_col.append_value(*time);
                    str_prop_col.append_value(str_prop);
                    int_prop_col.append_value(*int_prop);
                }
                let chunk = vec![
                    ArrayBuilder::finish(&mut src_col),
                    ArrayBuilder::finish(&mut dst_col),
                    ArrayBuilder::finish(&mut time_col),
                    ArrayBuilder::finish(&mut str_prop_col),
                    ArrayBuilder::finish(&mut int_prop_col),
                ];
                Ok(DFChunk { chunk })
            })
            .collect_vec();
        DFView {
            names: vec![
                "src".to_owned(),
                "dst".to_owned(),
                "time".to_owned(),
                "str_prop".to_owned(),
                "int_prop".to_owned(),
            ],
            chunks: chunks.into_iter(),
            num_rows: edges.len(),
        }
    }

    fn build_df_str(
        chunk_size: usize,
        edges: &[(String, String, i64, String, i64)],
    ) -> DFView<impl Iterator<Item = Result<DFChunk, GraphError>>> {
        let chunks = edges.iter().chunks(chunk_size);
        let mut src_col = LargeStringBuilder::new();
        let mut dst_col = StringViewBuilder::new();
        let mut time_col = Int64Builder::new();
        let mut str_prop_col = StringViewBuilder::new();
        let mut int_prop_col = Int64Builder::new();
        let chunks = chunks
            .into_iter()
            .map(|chunk| {
                for (src, dst, time, str_prop, int_prop) in chunk {
                    src_col.append_value(src);
                    dst_col.append_value(dst);
                    time_col.append_value(*time);
                    str_prop_col.append_value(str_prop);
                    int_prop_col.append_value(*int_prop);
                }
                let chunk = vec![
                    ArrayBuilder::finish(&mut src_col),
                    ArrayBuilder::finish(&mut dst_col),
                    ArrayBuilder::finish(&mut time_col),
                    ArrayBuilder::finish(&mut str_prop_col),
                    ArrayBuilder::finish(&mut int_prop_col),
                ];
                Ok(DFChunk { chunk })
            })
            .collect_vec();
        DFView {
            names: vec![
                "src".to_owned(),
                "dst".to_owned(),
                "time".to_owned(),
                "str_prop".to_owned(),
                "int_prop".to_owned(),
            ],
            chunks: chunks.into_iter(),
            num_rows: edges.len(),
        }
    }

    #[test]
    fn test_load_edges() {
        proptest!(|(edges in build_edge_list(1000, 100), chunk_size in 1usize..=1000)| {
            let distinct_edges = edges.iter().map(|(src, dst, _, _, _)| (src, dst)).collect::<std::collections::HashSet<_>>().len();
            let df_view = build_df(chunk_size, &edges);
            let g = Graph::new();
            let props = ["str_prop", "int_prop"];
            load_edges_from_df(df_view, "time", "src", "dst", &props, &[], None, None, None, &g).unwrap();
            let g2 = Graph::new();
            for (src, dst, time, str_prop, int_prop) in edges {
                g2.add_edge(time, src, dst, [("str_prop", str_prop.clone().into_prop()), ("int_prop", int_prop.into_prop())], None).unwrap();
                let edge = g2.edge(src, dst).unwrap().at(time);
                assert_eq!(edge.properties().get("str_prop").unwrap_str(), str_prop);
                assert_eq!(edge.properties().get("int_prop").unwrap_i64(), int_prop);
            }
            assert_eq!(g.unfiltered_num_edges(), distinct_edges);
            assert_eq!(g2.unfiltered_num_edges(), distinct_edges);
            assert_graph_equal(&g, &g2);
        })
    }

    #[test]
    fn test_simultaneous_edge_update() {
        let edges = [(0, 1, 0, "".to_string(), 0), (0, 1, 0, "".to_string(), 1)];

        let distinct_edges = edges
            .iter()
            .map(|(src, dst, _, _, _)| (src, dst))
            .collect::<std::collections::HashSet<_>>()
            .len();
        let df_view = build_df(1, &edges);
        let g = Graph::new();
        let props = ["str_prop", "int_prop"];
        load_edges_from_df(
            df_view,
            "time",
            "src",
            "dst",
            &props,
            &[],
            None,
            None,
            None,
            &g,
        )
        .unwrap();
        let g2 = Graph::new();
        for (src, dst, time, str_prop, int_prop) in edges {
            g2.add_edge(
                time,
                src,
                dst,
                [
                    ("str_prop", str_prop.clone().into_prop()),
                    ("int_prop", int_prop.into_prop()),
                ],
                None,
            )
            .unwrap();
            let edge = g2.edge(src, dst).unwrap().at(time);
            assert_eq!(edge.properties().get("str_prop").unwrap_str(), str_prop);
            assert_eq!(edge.properties().get("int_prop").unwrap_i64(), int_prop);
        }
        assert_eq!(g.unfiltered_num_edges(), distinct_edges);
        assert_eq!(g2.unfiltered_num_edges(), distinct_edges);
        assert_graph_equal(&g, &g2);
    }

    #[test]
    fn test_load_edges_str() {
        proptest!(|(edges in build_edge_list_str(100, 100), chunk_size in 1usize..=100)| {
            let distinct_edges = edges.iter().map(|(src, dst, _, _, _)| (src, dst)).collect::<std::collections::HashSet<_>>().len();
            let df_view = build_df_str(chunk_size, &edges);
            let g = Graph::new();
            let props = ["str_prop", "int_prop"];
            load_edges_from_df(df_view, "time", "src", "dst", &props, &[], None, None, None, &g).unwrap();
            let g2 = Graph::new();
            for (src, dst, time, str_prop, int_prop) in edges {
                g2.add_edge(time, &src, &dst, [("str_prop", str_prop.clone().into_prop()), ("int_prop", int_prop.into_prop())], None).unwrap();
                let edge = g.edge(&src, &dst).unwrap().at(time);
                assert_eq!(edge.properties().get("str_prop").unwrap_str(), str_prop);
                assert_eq!(edge.properties().get("int_prop").unwrap_i64(), int_prop);
            }
            assert_eq!(g.unfiltered_num_edges(), distinct_edges);
            assert_eq!(g2.unfiltered_num_edges(), distinct_edges);
            assert_graph_equal(&g, &g2);
        })
    }

    #[test]
    fn test_load_edges_str_fail() {
        let edges = [("0".to_string(), "1".to_string(), 0, "".to_string(), 0)];
        let df_view = build_df_str(1, &edges);
        let g = Graph::new();
        let props = ["str_prop", "int_prop"];
        load_edges_from_df(
            df_view,
            "time",
            "src",
            "dst",
            &props,
            &[],
            None,
            None,
            None,
            &g,
        )
        .unwrap();
        assert!(g.has_edge("0", "1"))
    }

    fn check_load_edges_layers(
        mut edges: Vec<(u64, u64, i64, String, i64, Option<String>)>,
        chunk_size: usize,
    ) {
        let distinct_edges = edges
            .iter()
            .map(|(src, dst, _, _, _, _)| (src, dst))
            .collect::<std::collections::HashSet<_>>()
            .len();
        edges.sort_by(|(_, _, _, _, _, l1), (_, _, _, _, _, l2)| l1.cmp(l2));
        let g = Graph::new();
        let g2 = Graph::new();

        for edges in edges.chunk_by(|(_, _, _, _, _, l1), (_, _, _, _, _, l2)| l1 < l2) {
            let layer = edges[0].5.clone();
            let edges = edges
                .iter()
                .map(|(src, dst, time, str_prop, int_prop, _)| {
                    (*src, *dst, *time, str_prop.clone(), *int_prop)
                })
                .collect_vec();
            let df_view = build_df(chunk_size, &edges);
            let props = ["str_prop", "int_prop"];
            load_edges_from_df(
                df_view,
                "time",
                "src",
                "dst",
                &props,
                &[],
                None,
                layer.as_deref(),
                None,
                &g,
            )
            .unwrap();
            for (src, dst, time, str_prop, int_prop) in edges {
                g2.add_edge(
                    time,
                    src,
                    dst,
                    [
                        ("str_prop", str_prop.clone().into_prop()),
                        ("int_prop", int_prop.into_prop()),
                    ],
                    layer.as_deref(),
                )
                .unwrap();
                let edge = g.edge(src, dst).unwrap().at(time);
                assert_eq!(edge.properties().get("str_prop").unwrap_str(), str_prop);
                assert_eq!(edge.properties().get("int_prop").unwrap_i64(), int_prop);
                if let Some(layer) = &layer {
                    assert!(edge.has_layer(layer))
                }
            }
            assert_eq!(g.count_edges(), distinct_edges);
            assert_eq!(g2.count_edges(), distinct_edges);
            assert_graph_equal(&g, &g2);
        }
    }
}

#[cfg(test)]
#[cfg(feature = "io")]
mod parquet_tests {
    use bigdecimal::BigDecimal;
    use chrono::{DateTime, Utc};
    use proptest::prelude::*;
    use raphtory::{
        db::graph::{graph::assert_graph_equal, views::deletion_graph::PersistentGraph},
        prelude::*,
        test_utils::{
            build_edge_list_dyn, build_graph, build_graph_strat, build_nodes_dyn, build_props_dyn,
            EdgeFixture, EdgeUpdatesFixture, GraphFixture, NodeFixture, NodeUpdatesFixture,
            PropUpdatesFixture,
        },
    };
    use std::str::FromStr;

    #[test]
    fn node_temp_props() {
        let nodes: NodeFixture = [(0, 0, vec![("a".to_string(), Prop::U8(5))])].into();
        build_and_check_parquet_encoding(nodes.into());
    }

    #[test]
    fn node_temp_props_decimal() {
        let nodes = NodeFixture(
            [(
                0,
                NodeUpdatesFixture {
                    props: PropUpdatesFixture {
                        t_props: vec![(
                            0,
                            vec![(
                                "Y".to_string(),
                                Prop::List(
                                    vec![
                                        Prop::List(
                                            vec![
                                                Prop::Decimal(
                                                    BigDecimal::from_str("13e-13").unwrap(),
                                                ),
                                                Prop::Decimal(
                                                    BigDecimal::from_str("13e-13").unwrap(),
                                                ),
                                                Prop::Decimal(
                                                    BigDecimal::from_str("13e-13").unwrap(),
                                                ),
                                                Prop::Decimal(
                                                    BigDecimal::from_str("13e-13").unwrap(),
                                                ),
                                                Prop::Decimal(
                                                    BigDecimal::from_str("13e-13").unwrap(),
                                                ),
                                                Prop::Decimal(
                                                    BigDecimal::from_str("13e-13").unwrap(),
                                                ),
                                            ]
                                            .into(),
                                        ),
                                        Prop::List(
                                            vec![
                                                Prop::Decimal(
                                                    BigDecimal::from_str("13e-13").unwrap(),
                                                ),
                                                Prop::Decimal(
                                                    BigDecimal::from_str(
                                                        "191558628130262966499e-13",
                                                    )
                                                    .unwrap(),
                                                ),
                                            ]
                                            .into(),
                                        ),
                                        Prop::List(
                                            vec![
                                    Prop::Decimal(
                                        BigDecimal::from_str(
                                            "87897464368906578673545214461637064026e-13",
                                        )
                                            .unwrap(),
                                    ),
                                    Prop::Decimal(
                                        BigDecimal::from_str(
                                            "94016349560001117444902279806303521844e-13",
                                        )
                                            .unwrap(),
                                    ),
                                    Prop::Decimal(
                                        BigDecimal::from_str(
                                            "84910690243002010022611521070762324633e-13",
                                        )
                                            .unwrap(),
                                    ),
                                    Prop::Decimal(
                                        BigDecimal::from_str(
                                            "31555839249842363263204026650232450040e-13",
                                        )
                                            .unwrap(),
                                    ),
                                    Prop::Decimal(
                                        BigDecimal::from_str(
                                            "86230621933535017744166139882102600331e-13",
                                        )
                                            .unwrap(),
                                    ),
                                    Prop::Decimal(
                                        BigDecimal::from_str(
                                            "8814065867434113836260276824023976656e-13",
                                        )
                                            .unwrap(),
                                    ),
                                    Prop::Decimal(
                                        BigDecimal::from_str(
                                            "5911907249021330427648764706320440531e-13",
                                        )
                                            .unwrap(),
                                    ),
                                    Prop::Decimal(
                                        BigDecimal::from_str(
                                            "86835517758183724431483793853154818250e-13",
                                        )
                                            .unwrap(),
                                    ),
                                    Prop::Decimal(
                                        BigDecimal::from_str(
                                            "89347387369804528029924787786630755616e-13",
                                        )
                                            .unwrap(),
                                    ),
                                ]
                                            .into(),
                                        ),
                                    ]
                                    .into(),
                                ),
                            )],
                        )],
                        c_props: vec![(
                            "x".to_string(),
                            Prop::Decimal(
                                BigDecimal::from_str("47852687012008324212654110188753175619e-22")
                                    .unwrap(),
                            ),
                        )],
                    },
                    node_type: None,
                },
            )]
            .into(),
        );

        build_and_check_parquet_encoding(nodes.into());
    }

    #[test]
    fn edge_metadata_maps() {
        let edges = EdgeFixture(
            [
                (
                    (1, 1, None),
                    EdgeUpdatesFixture {
                        props: PropUpdatesFixture {
                            t_props: vec![(1, vec![])],
                            c_props: vec![],
                        },
                        deletions: vec![],
                    },
                ),
                (
                    (0, 0, None),
                    EdgeUpdatesFixture {
                        props: PropUpdatesFixture {
                            t_props: vec![(0, vec![])],
                            c_props: vec![("x".to_string(), Prop::map([("n", Prop::U64(23))]))],
                        },
                        deletions: vec![],
                    },
                ),
                (
                    (0, 1, None),
                    EdgeUpdatesFixture {
                        props: PropUpdatesFixture {
                            t_props: vec![(0, vec![])],
                            c_props: vec![(
                                "a".to_string(),
                                Prop::map([("a", Prop::U8(1)), ("b", Prop::str("baa"))]),
                            )],
                        },
                        deletions: vec![],
                    },
                ),
            ]
            .into(),
        );

        build_and_check_parquet_encoding(edges.into());
    }

    #[test]
    fn write_edges_to_parquet() {
        let dt = "2012-12-12 12:12:12+00:00"
            .parse::<DateTime<Utc>>()
            .unwrap();
        build_and_check_parquet_encoding(
            [
                (0, 1, 12, vec![("one".to_string(), Prop::DTime(dt))], None),
                (
                    1,
                    2,
                    12,
                    vec![
                        ("two".to_string(), Prop::I32(2)),
                        ("three".to_string(), Prop::I64(3)),
                        (
                            "four".to_string(),
                            Prop::List(vec![Prop::I32(1), Prop::I32(2)].into()),
                        ),
                    ],
                    Some("b"),
                ),
                (
                    2,
                    3,
                    12,
                    vec![
                        ("three".to_string(), Prop::I64(3)),
                        ("one".to_string(), Prop::DTime(dt)),
                        ("five".to_string(), Prop::List(vec![Prop::str("a")].into())),
                    ],
                    Some("a"),
                ),
            ]
            .into(),
        );
    }

    #[test]
    fn write_edges_empty_prop_first() {
        build_and_check_parquet_encoding(
            [
                (
                    0,
                    1,
                    12,
                    vec![("a".to_string(), Prop::List(vec![].into()))],
                    None,
                ),
                (
                    1,
                    2,
                    12,
                    vec![("a".to_string(), Prop::List(vec![Prop::str("aa")].into()))],
                    None,
                ),
            ]
            .into(),
        );
    }

    #[test]
    fn edges_dates() {
        let dt = "2012-12-12 12:12:12+00:00"
            .parse::<DateTime<Utc>>()
            .unwrap();
        build_and_check_parquet_encoding(
            [(
                0,
                0,
                0,
                vec![("a".to_string(), Prop::List(vec![Prop::DTime(dt)].into()))],
                None,
            )]
            .into(),
        );
    }
    #[test]
    fn edges_maps() {
        let dt = "2012-12-12 12:12:12+00:00"
            .parse::<DateTime<Utc>>()
            .unwrap();
        build_and_check_parquet_encoding(
            [(
                0,
                0,
                0,
                vec![(
                    "a".to_string(),
                    Prop::map([("a", Prop::DTime(dt)), ("b", Prop::str("s"))]),
                )],
                None,
            )]
            .into(),
        );
    }

    #[test]
    fn edges_maps2() {
        build_and_check_parquet_encoding(
            [
                (
                    0,
                    0,
                    0,
                    vec![("a".to_string(), Prop::map([("a", Prop::I32(1))]))],
                    None,
                ),
                (
                    0,
                    0,
                    0,
                    vec![("a".to_string(), Prop::map([("b", Prop::str("x"))]))],
                    None,
                ),
            ]
            .into(),
        );
    }

    #[test]
    fn edges_maps3() {
        build_and_check_parquet_encoding(
            [
                (0, 0, 0, vec![("a".to_string(), Prop::U8(5))], None),
                (
                    0,
                    0,
                    0,
                    vec![("b".to_string(), Prop::map([("c", Prop::U8(66))]))],
                    None,
                ),
            ]
            .into(),
        );
    }

    #[test]
    fn edges_map4() {
        let edges = EdgeFixture(
            [(
                (0, 0, None),
                EdgeUpdatesFixture {
                    props: PropUpdatesFixture {
                        t_props: vec![(0, vec![("a".to_string(), Prop::U8(5))])],
                        c_props: vec![(
                            "x".to_string(),
                            Prop::List(
                                vec![
                                    Prop::map([("n", Prop::I64(23))]),
                                    Prop::map([("b", Prop::F64(0.2))]),
                                ]
                                .into(),
                            ),
                        )],
                    },
                    deletions: vec![1],
                },
            )]
            .into(),
        );

        build_and_check_parquet_encoding(edges.into())
    }

    // proptest
    fn build_and_check_parquet_encoding(edges: GraphFixture) {
        let g = Graph::from(build_graph(&edges));
        check_parquet_encoding(g);
    }

    fn check_parquet_encoding(g: Graph) {
        let temp_dir = tempfile::tempdir().unwrap();
        g.encode_parquet(&temp_dir).unwrap();
        let g2 = Graph::decode_parquet(&temp_dir).unwrap();
        assert_graph_equal(&g, &g2);
    }

    fn check_parquet_encoding_deletions(g: PersistentGraph) {
        let temp_dir = tempfile::tempdir().unwrap();
        g.encode_parquet(&temp_dir).unwrap();
        let g2 = PersistentGraph::decode_parquet(&temp_dir).unwrap();
        assert_graph_equal(&g, &g2);
    }

    #[test]
    fn nodes_props_1() {
        let dt = "2012-12-12 12:12:12+00:00"
            .parse::<DateTime<Utc>>()
            .unwrap();
        let nodes = NodeFixture(
            [(
                0,
                NodeUpdatesFixture {
                    props: PropUpdatesFixture {
                        t_props: vec![(
                            0,
                            vec![
                                ("a".to_string(), Prop::U8(5)),
                                ("a".to_string(), Prop::U8(5)),
                            ],
                        )],
                        c_props: vec![("b".to_string(), Prop::DTime(dt))],
                    },
                    node_type: None,
                },
            )]
            .into(),
        );

        build_and_check_parquet_encoding(nodes.into());
    }

    fn check_graph_props(nf: PropUpdatesFixture) {
        let g = Graph::new();
        let temp_dir = tempfile::tempdir().unwrap();
        for (t, props) in nf.t_props {
            g.add_properties(t, props).unwrap()
        }

        g.add_metadata(nf.c_props).unwrap();
        g.encode_parquet(&temp_dir).unwrap();
        let g2 = Graph::decode_parquet(&temp_dir).unwrap();
        assert_graph_equal(&g, &g2);
    }

    #[test]
    fn graph_props() {
        let props = PropUpdatesFixture {
            t_props: vec![(0, vec![("a".to_string(), Prop::U8(5))])],
            c_props: vec![("b".to_string(), Prop::str("baa"))],
        };
        check_graph_props(props)
    }

    #[test]
    fn node_metadata() {
        let nf = NodeFixture(
            [(
                1,
                NodeUpdatesFixture {
                    props: PropUpdatesFixture {
                        t_props: vec![],
                        c_props: vec![("b".to_string(), Prop::str("baa"))],
                    },
                    node_type: None,
                },
            )]
            .into(),
        );
        build_and_check_parquet_encoding(nf.into())
    }

    #[test]
    fn write_graph_props_to_parquet() {
        proptest!(|(props in build_props_dyn(10))| {
            check_graph_props(props);
        });
    }

    #[test]
    fn write_nodes_no_props_to_parquet() {
        let nf = PropUpdatesFixture {
            t_props: vec![(1, vec![])],
            c_props: vec![],
        };
        check_graph_props(nf);
    }

    #[test]
    fn write_nodes_any_props_to_parquet() {
        proptest!(|(nodes in build_nodes_dyn(10, 10))| {
            build_and_check_parquet_encoding(nodes.into());
        });
    }
    #[test]
    fn write_edges_any_props_to_parquet() {
        proptest!(|(edges in build_edge_list_dyn(10, 10, true))| {
            build_and_check_parquet_encoding(edges.into());
        });
    }

    #[test]
    fn write_graph_to_parquet() {
        proptest!(|(edges in build_graph_strat(10, 10, true))| {
            build_and_check_parquet_encoding(edges);
        })
    }

    #[test]
    fn test_deletion() {
        let graph = PersistentGraph::new();
        graph.delete_edge(0, 0, 0, Some("a")).unwrap();
        check_parquet_encoding_deletions(graph);
    }

    #[test]
    fn test_empty_map() {
        let graph = Graph::new();
        graph
            .add_edge(0, 0, 1, [("test", Prop::map(NO_PROPS))], None)
            .unwrap();
        check_parquet_encoding(graph);
    }
}
