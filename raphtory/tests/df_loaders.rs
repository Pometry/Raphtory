pub mod test_utils;

#[cfg(feature = "io")]
mod io_tests {

    use itertools::Itertools;
    use polars_arrow::array::{MutableArray, MutablePrimitiveArray, MutableUtf8Array};
    use proptest::proptest;
    use raphtory::{
        db::graph::graph::assert_graph_equal,
        errors::GraphError,
        io::arrow::{
            dataframe::{DFChunk, DFView},
            df_loaders::load_edges_from_df,
        },
        prelude::*,
    };
    use tempfile::TempDir;

    use crate::test_utils::build_edge_list;

    #[cfg(feature = "storage")]
    mod load_multi_layer {
        use std::{
            fs::File,
            path::{Path, PathBuf},
        };

        use crate::test_utils::build_edge_list;
        use polars_arrow::{
            array::{PrimitiveArray, Utf8Array},
            types::NativeType,
        };
        use polars_core::{frame::DataFrame, prelude::*};
        use polars_io::prelude::{ParquetCompression, ParquetWriter};
        use pometry_storage::{graph::TemporalGraph, load::ExternalEdgeList};
        use prop::sample::SizeRange;
        use proptest::prelude::*;
        use raphtory::{
            db::graph::graph::{assert_graph_equal, assert_graph_equal_timestamps},
            io::parquet_loaders::load_edges_from_parquet,
            prelude::*,
        };
        use raphtory_storage::{disk::DiskGraphStorage, graph::graph::GraphStorage};
        use tempfile::TempDir;

        fn build_edge_list_df(
            len: usize,
            num_nodes: impl Strategy<Value = u64>,
            num_layers: impl Into<SizeRange>,
        ) -> impl Strategy<Value = Vec<DataFrame>> {
            let layer = num_nodes
                .prop_flat_map(move |num_nodes| {
                    build_edge_list(len, num_nodes)
                        .prop_filter("no empty edge lists", |el| !el.is_empty())
                })
                .prop_map(move |mut rows| {
                    rows.sort_by_key(|(src, dst, time, _, _)| (*src, *dst, *time));
                    new_df_from_rows(&rows)
                });
            proptest::collection::vec(layer, num_layers)
        }

        fn new_df_from_rows(rows: &[(u64, u64, i64, String, i64)]) -> DataFrame {
            let src = native_series("src", rows.iter().map(|(src, _, _, _, _)| *src));
            let dst = native_series("dst", rows.iter().map(|(_, dst, _, _, _)| *dst));
            let time = native_series("time", rows.iter().map(|(_, _, time, _, _)| *time));
            let int_prop = native_series(
                "int_prop",
                rows.iter().map(|(_, _, _, _, int_prop)| *int_prop),
            );

            let str_prop = Series::from_arrow(
                "str_prop",
                Utf8Array::<i64>::from_iter(
                    rows.iter()
                        .map(|(_, _, _, str_prop, _)| Some(str_prop.clone())),
                )
                .boxed(),
            )
            .unwrap();

            DataFrame::new(vec![src, dst, time, str_prop, int_prop]).unwrap()
        }

        fn native_series<T: NativeType>(name: &str, is: impl IntoIterator<Item = T>) -> Series {
            let is = PrimitiveArray::from_vec(is.into_iter().collect());
            Series::from_arrow(name, is.boxed()).unwrap()
        }

        fn check_layers_from_df(input: Vec<DataFrame>, num_threads: usize) {
            let root_dir = TempDir::new().unwrap();
            let graph_dir = TempDir::new().unwrap();
            let layers = input
                .into_iter()
                .enumerate()
                .map(|(i, df)| (i.to_string(), df))
                .collect::<Vec<_>>();
            let edge_lists = write_layers(&layers, root_dir.path());

            let expected = Graph::new();
            for edge_list in &edge_lists {
                load_edges_from_parquet(
                    &expected,
                    &edge_list.path,
                    "time",
                    "src",
                    "dst",
                    &["int_prop", "str_prop"],
                    &[],
                    None,
                    Some(edge_list.layer),
                    None,
                )
                .unwrap();
            }

            let g = TemporalGraph::from_parquets(
                num_threads,
                13,
                23,
                graph_dir.path(),
                edge_lists,
                &[],
                None,
                None,
                None,
            )
            .unwrap();
            let actual: Graph = GraphStorage::from(DiskGraphStorage::from(g)).into();

            assert_graph_equal(&expected, &actual);

            let g = TemporalGraph::new(graph_dir.path()).unwrap();

            for edge in g.edges_iter() {
                assert!(g.find_edge(edge.src_id(), edge.dst_id()).is_some());
            }

            let actual: Graph = GraphStorage::from(DiskGraphStorage::from(g)).into();
            assert_graph_equal(&expected, &actual);
        }

        // DiskGraph appears to have different event ids on time entries
        fn check_layers_from_df_timestamps(input: Vec<DataFrame>, num_threads: usize) {
            let root_dir = TempDir::new().unwrap();
            let graph_dir = TempDir::new().unwrap();
            let layers = input
                .into_iter()
                .enumerate()
                .map(|(i, df)| (i.to_string(), df))
                .collect::<Vec<_>>();
            let edge_lists = write_layers(&layers, root_dir.path());

            let expected = Graph::new();
            for edge_list in &edge_lists {
                load_edges_from_parquet(
                    &expected,
                    &edge_list.path,
                    "time",
                    "src",
                    "dst",
                    &["int_prop", "str_prop"],
                    &[],
                    None,
                    Some(edge_list.layer),
                    None,
                )
                .unwrap();
            }

            let g = TemporalGraph::from_parquets(
                num_threads,
                13,
                23,
                graph_dir.path(),
                edge_lists,
                &[],
                None,
                None,
                None,
            )
            .unwrap();
            let actual: Graph = GraphStorage::from(DiskGraphStorage::from(g)).into();

            assert_graph_equal_timestamps(&expected, &actual);

            let g = TemporalGraph::new(graph_dir.path()).unwrap();

            for edge in g.edges_iter() {
                assert!(g.find_edge(edge.src_id(), edge.dst_id()).is_some());
            }

            let actual: Graph = GraphStorage::from(DiskGraphStorage::from(g)).into();
            assert_graph_equal_timestamps(&expected, &actual);
        }

        #[test]
        fn load_from_multiple_layers() {
            proptest!(|(input in build_edge_list_df(50, 1u64..23, 1..10,  ), num_threads in 1usize..2)| {
                check_layers_from_df_timestamps(input, num_threads)
            });
        }

        #[test]
        fn single_layer_single_edge() {
            let df = new_df_from_rows(&[(0, 0, 1, "".to_owned(), 2)]);
            check_layers_from_df(vec![df], 1)
        }

        fn write_layers<'a>(
            layers: &'a [(String, DataFrame)],
            root_dir: &Path,
        ) -> Vec<ExternalEdgeList<'a, PathBuf>> {
            let mut paths = vec![];
            for (name, df) in layers.iter() {
                let layer_dir = root_dir.join(name);
                std::fs::create_dir_all(&layer_dir).unwrap();
                let layer_path = layer_dir.join("edges.parquet");

                paths.push(
                    ExternalEdgeList::new(
                        name,
                        layer_path.to_path_buf(),
                        "src",
                        "dst",
                        "time",
                        vec![],
                    )
                    .unwrap(),
                );

                let file = File::create(layer_path).unwrap();
                let mut df = df.clone();
                ParquetWriter::new(file)
                    .with_compression(ParquetCompression::Snappy)
                    .finish(&mut df)
                    .unwrap();
            }
            paths
        }
    }

    fn build_df(
        chunk_size: usize,
        edges: &[(u64, u64, i64, String, i64)],
    ) -> DFView<impl Iterator<Item = Result<DFChunk, GraphError>>> {
        let chunks = edges.iter().chunks(chunk_size);
        let chunks = chunks
            .into_iter()
            .map(|chunk| {
                let mut src_col = MutablePrimitiveArray::new();
                let mut dst_col = MutablePrimitiveArray::new();
                let mut time_col = MutablePrimitiveArray::new();
                let mut str_prop_col = MutableUtf8Array::<i64>::new();
                let mut int_prop_col = MutablePrimitiveArray::new();
                for (src, dst, time, str_prop, int_prop) in chunk {
                    src_col.push_value(*src);
                    dst_col.push_value(*dst);
                    time_col.push_value(*time);
                    str_prop_col.push(Some(str_prop));
                    int_prop_col.push_value(*int_prop);
                }
                let chunk = vec![
                    src_col.as_box(),
                    dst_col.as_box(),
                    time_col.as_box(),
                    str_prop_col.as_box(),
                    int_prop_col.as_box(),
                ];
                Ok(DFChunk::new(chunk))
            })
            .collect_vec();
        DFView::new(
            vec![
                "src".to_owned(),
                "dst".to_owned(),
                "time".to_owned(),
                "str_prop".to_owned(),
                "int_prop".to_owned(),
            ],
            chunks.into_iter(),
            edges.len(),
        )
    }
    #[test]
    fn test_load_edges() {
        proptest!(|(edges in build_edge_list(1000, 100), chunk_size in 1usize..=1000)| {
            let df_view = build_df(chunk_size, &edges);
            let g = Graph::new();
            let props = ["str_prop", "int_prop"];
            load_edges_from_df(df_view, "time", "src", "dst", &props, &[], None, None, None, &g).unwrap();
            let g2 = Graph::new();
            for (src, dst, time, str_prop, int_prop) in edges {
                g2.add_edge(time, src, dst, [("str_prop", str_prop.clone().into_prop()), ("int_prop", int_prop.into_prop())], None).unwrap();
                let edge = g.edge(src, dst).unwrap().at(time);
                assert_eq!(edge.properties().get("str_prop").unwrap_str(), str_prop);
                assert_eq!(edge.properties().get("int_prop").unwrap_i64(), int_prop);
            }
            assert_graph_equal(&g, &g2);
        })
    }

    #[test]
    fn test_load_edges_with_cache() {
        proptest!(|(edges in build_edge_list(100, 100), chunk_size in 1usize..=100)| {
            let df_view = build_df(chunk_size, &edges);
            let g = Graph::new();
            let cache_file = TempDir::new().unwrap();
            g.cache(cache_file.path()).unwrap();
            let props = ["str_prop", "int_prop"];
            load_edges_from_df(df_view, "time", "src", "dst", &props, &[], None, None, None, &g).unwrap();
            let g = Graph::load_cached(cache_file.path()).unwrap();
            let g2 = Graph::new();
            for (src, dst, time, str_prop, int_prop) in edges {
                g2.add_edge(time, src, dst, [("str_prop", str_prop.clone().into_prop()), ("int_prop", int_prop.into_prop())], None).unwrap();
                let edge = g.edge(src, dst).unwrap().at(time);
                assert_eq!(edge.properties().get("str_prop").unwrap_str(), str_prop);
                assert_eq!(edge.properties().get("int_prop").unwrap_i64(), int_prop);
            }
            assert_graph_equal(&g, &g2);
        })
    }

    #[test]
    fn load_single_edge_with_cache() {
        let edges = [(0, 0, 0, "".to_string(), 0)];
        let df_view = build_df(1, &edges);
        let g = Graph::new();
        let cache_file = TempDir::new().unwrap();
        g.cache(cache_file.path()).unwrap();
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
        let g = Graph::load_cached(cache_file.path()).unwrap();
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
            let edge = g.edge(src, dst).unwrap().at(time);
            assert_eq!(edge.properties().get("str_prop").unwrap_str(), str_prop);
            assert_eq!(edge.properties().get("int_prop").unwrap_i64(), int_prop);
        }
        assert_graph_equal(&g, &g2);
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
    };

    use crate::test_utils::{
        build_edge_list_dyn, build_graph, build_graph_strat, build_nodes_dyn, build_props_dyn,
        EdgeFixture, EdgeUpdatesFixture, GraphFixture, NodeFixture, NodeUpdatesFixture,
        PropUpdatesFixture,
    };
    use raphtory::db::graph::graph::assert_graph_equal_timestamps;
    use std::str::FromStr;

    #[test]
    fn node_temp_props() {
        let nodes: NodeFixture = [(0, 0, vec![("a".to_string(), Prop::U8(5))])].into();
        build_and_check_parquet_encoding(nodes.into());
    }

    #[test]
    #[ignore = "This is broken because of polars-parquet"]
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
        assert_graph_equal_timestamps(&g, &g2);
    }

    fn check_parquet_encoding_deletions(g: PersistentGraph) {
        let temp_dir = tempfile::tempdir().unwrap();
        g.encode_parquet(&temp_dir).unwrap();
        let g2 = PersistentGraph::decode_parquet(&temp_dir).unwrap();
        assert_graph_equal_timestamps(&g, &g2);
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

    fn check_graph_props(nf: PropUpdatesFixture, only_timestamps: bool) {
        let g = Graph::new();
        let temp_dir = tempfile::tempdir().unwrap();
        for (t, props) in nf.t_props {
            g.add_properties(t, props).unwrap()
        }

        g.add_metadata(nf.c_props).unwrap();
        g.encode_parquet(&temp_dir).unwrap();
        let g2 = Graph::decode_parquet(&temp_dir).unwrap();
        if only_timestamps {
            assert_graph_equal_timestamps(&g, &g2)
        } else {
            assert_graph_equal(&g, &g2);
        }
    }

    #[test]
    fn graph_props() {
        let props = PropUpdatesFixture {
            t_props: vec![(0, vec![("a".to_string(), Prop::U8(5))])],
            c_props: vec![("b".to_string(), Prop::str("baa"))],
        };
        check_graph_props(props, true)
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
            check_graph_props(props, true);
        });
    }

    #[test]
    fn write_nodes_no_props_to_parquet() {
        let nf = PropUpdatesFixture {
            t_props: vec![(1, vec![])],
            c_props: vec![],
        };
        check_graph_props(nf, false);
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
