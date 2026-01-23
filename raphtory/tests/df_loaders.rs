#[cfg(feature = "io")]
mod io_tests {
    use std::any::Any;

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
            df_loaders::{
                edges::{load_edges_from_df, ColumnNames},
                nodes::{load_node_props_from_df, load_nodes_from_df},
            },
        },
        prelude::*,
        test_utils::{build_edge_list, build_edge_list_str, build_edge_list_with_secondary_index},
    };
    use raphtory_api::core::storage::arc_str::ArcStr;
    use raphtory_core::storage::timeindex::EventTime;
    use raphtory_storage::{
        core_ops::CoreGraphOps,
        mutation::addition_ops::{InternalAdditionOps, SessionAdditionOps},
    };

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
            num_rows: Some(edges.len()),
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
            num_rows: Some(edges.len()),
        }
    }

    fn build_df_with_secondary_index(
        chunk_size: usize,
        edges: &[(u64, u64, i64, u64, String, i64)],
    ) -> DFView<impl Iterator<Item = Result<DFChunk, GraphError>>> {
        let chunks = edges.iter().chunks(chunk_size);
        let mut src_col = UInt64Builder::new();
        let mut dst_col = UInt64Builder::new();
        let mut time_col = Int64Builder::new();
        let mut secondary_index_col = UInt64Builder::new();
        let mut str_prop_col = LargeStringBuilder::new();
        let mut int_prop_col = Int64Builder::new();
        let chunks = chunks
            .into_iter()
            .map(|chunk| {
                for (src, dst, time, secondary_index, str_prop, int_prop) in chunk {
                    src_col.append_value(*src);
                    dst_col.append_value(*dst);
                    time_col.append_value(*time);
                    secondary_index_col.append_value(*secondary_index);
                    str_prop_col.append_value(str_prop);
                    int_prop_col.append_value(*int_prop);
                }

                let chunk = vec![
                    ArrayBuilder::finish(&mut src_col),
                    ArrayBuilder::finish(&mut dst_col),
                    ArrayBuilder::finish(&mut time_col),
                    ArrayBuilder::finish(&mut secondary_index_col),
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
                "secondary_index".to_owned(),
                "str_prop".to_owned(),
                "int_prop".to_owned(),
            ],
            chunks: chunks.into_iter(),
            num_rows: Some(edges.len()),
        }
    }

    fn build_nodes_df_with_secondary_index(
        chunk_size: usize,
        nodes: &[(u64, i64, u64, &str, i64, &str)],
    ) -> DFView<impl Iterator<Item = Result<DFChunk, GraphError>>> {
        let chunks = nodes.iter().chunks(chunk_size);
        let mut node_id_col = UInt64Builder::new();
        let mut time_col = Int64Builder::new();
        let mut secondary_index_col = UInt64Builder::new();
        let mut str_prop_col = LargeStringBuilder::new();
        let mut int_prop_col = Int64Builder::new();
        let mut node_type_col = StringViewBuilder::new();
        let chunks = chunks
            .into_iter()
            .map(|chunk| {
                for (node_id, time, secondary_index, str_prop, int_prop, node_type) in chunk {
                    node_id_col.append_value(*node_id);
                    time_col.append_value(*time);
                    secondary_index_col.append_value(*secondary_index);
                    str_prop_col.append_value(str_prop);
                    int_prop_col.append_value(*int_prop);
                    node_type_col.append_value(node_type);
                }
                let chunk = vec![
                    ArrayBuilder::finish(&mut node_id_col),
                    ArrayBuilder::finish(&mut time_col),
                    ArrayBuilder::finish(&mut secondary_index_col),
                    ArrayBuilder::finish(&mut str_prop_col),
                    ArrayBuilder::finish(&mut int_prop_col),
                    ArrayBuilder::finish(&mut node_type_col),
                ];
                Ok(DFChunk { chunk })
            })
            .collect_vec();
        DFView {
            names: vec![
                "node_id".to_owned(),
                "time".to_owned(),
                "secondary_index".to_owned(),
                "str_prop".to_owned(),
                "int_prop".to_owned(),
                "node_type".to_owned(),
            ],
            chunks: chunks.into_iter(),
            num_rows: Some(nodes.len()),
        }
    }

    #[test]
    fn test_load_edges() {
        proptest!(|(edges in build_edge_list(1000, 100), chunk_size in 1usize..=1000)| {
            let distinct_edges = edges.iter().map(|(src, dst, _, _, _)| (src, dst)).collect::<std::collections::HashSet<_>>().len();
            let df_view = build_df(chunk_size, &edges);
            let g = Graph::new();
            let props = ["str_prop", "int_prop"];
            let secondary_index = None;
            load_edges_from_df(df_view,
                ColumnNames::new("time", secondary_index, "src", "dst", None),
                true,
                &props, &[], None, None, &g, false).unwrap();

            let g2 = Graph::new();

            for (src, dst, time, str_prop, int_prop) in edges {
                g2.add_edge(time, src, dst, [("str_prop", str_prop.clone().into_prop()), ("int_prop", int_prop.into_prop())], None).unwrap();
                let edge = g2.edge(src, dst).unwrap().at(time);
                assert_eq!(edge.properties().get("str_prop").unwrap_str(), str_prop);
                assert_eq!(edge.properties().get("int_prop").unwrap_i64(), int_prop);
            }

            let count_edges = g.core_edges().iter(&raphtory_core::entities::LayerIds::All).count();
            assert_eq!(g.unfiltered_num_edges(), distinct_edges);
            assert_eq!(g2.unfiltered_num_edges(), distinct_edges);
            assert_eq!(count_edges, distinct_edges);
            assert_graph_equal(&g, &g2);
        })
    }

    // def test_load_from_pandas():
    #[test]
    fn load_some_edges_as_in_python() {
        use arrow::array::builder::{Float64Builder, LargeStringBuilder};

        // Create the dataframe equivalent to the pandas DataFrame
        let edges = vec![
            (1u64, 2u64, 1i64, 1.0f64, "red".to_string()),
            (2, 3, 2, 2.0, "blue".to_string()),
            (3, 4, 3, 3.0, "green".to_string()),
            (4, 5, 4, 4.0, "yellow".to_string()),
            (5, 6, 5, 5.0, "purple".to_string()),
        ];

        // Build the dataframe
        let mut src_col = UInt64Builder::new();
        let mut dst_col = UInt64Builder::new();
        let mut time_col = Int64Builder::new();
        let mut weight_col = Float64Builder::new();
        let mut marbles_col = LargeStringBuilder::new();

        for (src, dst, time, weight, marbles) in &edges {
            src_col.append_value(*src);
            dst_col.append_value(*dst);
            time_col.append_value(*time);
            weight_col.append_value(*weight);
            marbles_col.append_value(marbles);
        }

        let chunk = vec![
            ArrayBuilder::finish(&mut src_col),
            ArrayBuilder::finish(&mut dst_col),
            ArrayBuilder::finish(&mut time_col),
            ArrayBuilder::finish(&mut weight_col),
            ArrayBuilder::finish(&mut marbles_col),
        ];

        let df_view = DFView {
            names: vec![
                "src".to_owned(),
                "dst".to_owned(),
                "time".to_owned(),
                "weight".to_owned(),
                "marbles".to_owned(),
            ],
            chunks: vec![Ok(DFChunk { chunk })].into_iter(),
            num_rows: Some(edges.len()),
        };

        // Load edges into graph
        let g = Graph::new();
        let props = ["weight", "marbles"];
        load_edges_from_df(
            df_view,
            ColumnNames::new("time", None, "src", "dst", None),
            true,
            &props,
            &[],
            None,
            None,
            &g,
            false,
        )
        .unwrap();

        // Expected values
        let expected_nodes = vec![1u64, 2, 3, 4, 5, 6];
        let mut expected_edges = vec![
            (1u64, 2u64, 1.0f64, "red".to_string()),
            (2, 3, 2.0, "blue".to_string()),
            (3, 4, 3.0, "green".to_string()),
            (4, 5, 4.0, "yellow".to_string()),
            (5, 6, 5.0, "purple".to_string()),
        ];

        // Collect actual nodes
        let mut actual_nodes: Vec<u64> = g
            .nodes()
            .id()
            .into_iter()
            .flat_map(|(_, id)| id.as_u64())
            .collect();
        actual_nodes.sort();

        // Collect actual edges
        let mut actual_edges: Vec<(u64, u64, f64, String)> = g
            .edges()
            .iter()
            .filter_map(|e| {
                let weight = e.properties().get("weight").unwrap_f64();
                let marbles = e.properties().get("marbles").unwrap_str().to_string();
                Some((
                    e.src().id().as_u64()?,
                    e.dst().id().as_u64()?,
                    weight,
                    marbles,
                ))
            })
            .collect();
        actual_edges.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
        expected_edges.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

        // Assertions
        assert_eq!(actual_nodes, expected_nodes);
        assert_eq!(actual_edges, expected_edges);
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
        let secondary_index = None;

        load_edges_from_df(
            df_view,
            ColumnNames::new("time", secondary_index, "src", "dst", None),
            true,
            &props,
            &[],
            None,
            None,
            &g,
            false,
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
            load_edges_from_df(df_view, ColumnNames::new("time", None, "src", "dst", None), true, &props, &[], None, None, &g, false).unwrap();

            let g2 = Graph::new();

            for (src, dst, time, str_prop, int_prop) in edges {
                g2.add_edge(time, &src, &dst, [("str_prop", str_prop.clone().into_prop()), ("int_prop", int_prop.into_prop())], None).unwrap();
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
            ColumnNames::new("time", None, "src", "dst", None),
            true,
            &props,
            &[],
            None,
            None,
            &g,
            false,
        )
        .unwrap();

        assert!(g.has_edge("0", "1"))
    }

    #[test]
    fn test_load_edges_with_secondary_index() {
        // Create edges with the same timestamp but different secondary_index values
        // Edge format: (src, dst, time, secondary_index, str_prop, int_prop)
        let edges = [
            (1, 2, 100, 2, "secondary_index_2".to_string(), 1),
            (1, 2, 100, 0, "secondary_index_0".to_string(), 2),
            (1, 2, 100, 1, "secondary_index_1".to_string(), 3),
            (2, 3, 200, 1, "secondary_index_1".to_string(), 4),
            (2, 3, 200, 0, "secondary_index_0".to_string(), 5),
            (3, 4, 300, 10, "secondary_index_10".to_string(), 6),
            (3, 4, 300, 5, "secondary_index_5".to_string(), 7),
            (4, 5, 400, 0, "secondary_index_0".to_string(), 8),
            (4, 5, 500, 0, "secondary_index_0".to_string(), 9),
        ];

        let chunk_size = 50;
        let df_view = build_df_with_secondary_index(chunk_size, &edges);
        let g = Graph::new();
        let props = ["str_prop", "int_prop"];
        let secondary_index = Some("secondary_index");

        // Load edges from DataFrame with secondary_index
        load_edges_from_df(
            df_view,
            ColumnNames::new("time", secondary_index, "src", "dst", None),
            true,
            &props,
            &[],
            None,
            None,
            &g,
            false,
        )
        .unwrap();

        let g2 = Graph::new();

        for (src, dst, time, secondary_index, str_prop, int_prop) in edges {
            let time_with_secondary_index = EventTime::new(time, secondary_index as usize);

            g2.add_edge(
                time_with_secondary_index,
                src,
                dst,
                [
                    ("str_prop", str_prop.clone().into_prop()),
                    ("int_prop", int_prop.into_prop()),
                ],
                None,
            )
            .unwrap();
        }

        // Internally checks whether temporal props are sorted by
        // secondary index.
        assert_graph_equal(&g, &g2);

        // Both graphs should have the same event_id / secondary_index
        assert_eq!(
            g.write_session().unwrap().read_event_id().unwrap(),
            g2.write_session().unwrap().read_event_id().unwrap(),
        );

        assert_eq!(
            g.write_session().unwrap().read_event_id().unwrap(),
            10 // max secondary index in edges
        );
    }

    #[test]
    fn test_load_edges_with_secondary_index_proptest() {
        let len = 1000;
        let num_nodes = 100;

        proptest!(|(edges in build_edge_list_with_secondary_index(len, num_nodes), chunk_size in 1usize..=len)| {
            let distinct_edges = edges.iter().map(|(src, dst, _, _, _, _)| (src, dst)).collect::<std::collections::HashSet<_>>().len();
            let df_view = build_df_with_secondary_index(chunk_size, &edges);
            let g = Graph::new();
            let props = ["str_prop", "int_prop"];
            let secondary_index = Some("secondary_index");

            load_edges_from_df(
                df_view,
                ColumnNames::new("time", secondary_index, "src", "dst", None),
                true,
                &props,
                &[],
                None,
                None,
                &g,
                false,
            ).unwrap();

            let g2 = Graph::new();
            let mut max_secondary_index = 0;

            for (src, dst, time, secondary_index_val, str_prop, int_prop) in edges {
                let time_with_secondary_index = EventTime(time, secondary_index_val as usize);

                g2.add_edge(
                    time_with_secondary_index,
                    src,
                    dst,
                    [
                        ("str_prop", str_prop.clone().into_prop()),
                        ("int_prop", int_prop.into_prop()),
                    ],
                    None,
                ).unwrap();

                let edge = g.edge(src, dst).unwrap().at(time);
                assert_eq!(edge.properties().get("str_prop").unwrap_str(), str_prop);
                assert_eq!(edge.properties().get("int_prop").unwrap_i64(), int_prop);

                // Track the maximum secondary_index value to compare later
                max_secondary_index = max_secondary_index.max(secondary_index_val as usize);
            }

            assert_eq!(g.unfiltered_num_edges(), distinct_edges);
            assert_eq!(g2.unfiltered_num_edges(), distinct_edges);
            assert_graph_equal(&g, &g2);

            // Both graphs should have the same event_id / secondary_index
            assert_eq!(
                g.write_session().unwrap().read_event_id().unwrap(),
                g2.write_session().unwrap().read_event_id().unwrap(),
            );

            assert_eq!(
                g.write_session().unwrap().read_event_id().unwrap(),
                max_secondary_index
            );
        })
    }

    #[test]
    fn test_load_nodes_with_secondary_index() {
        // Create nodes with the same timestamp but different secondary_index values
        // Node format: (node_id, time, secondary_index, str_prop, int_prop)
        let nodes = [
            (1, 100, 2, "secondary_index_2", 1, "TypeA"),
            (1, 100, 0, "secondary_index_0", 2, "TypeA"),
            (1, 100, 1, "secondary_index_1", 3, "TypeA"),
            (2, 200, 1, "secondary_index_1", 4, "TypeA"),
            (2, 200, 0, "secondary_index_0", 5, "TypeA"),
            (3, 300, 10, "secondary_index_10", 6, "TypeC"),
            (3, 300, 5, "secondary_index_5", 7, "TypeC"),
            (4, 400, 0, "secondary_index_0", 8, "TypeA"),
            (4, 500, 0, "secondary_index_0", 9, "TypeA"),
        ];

        let nodes_no_dupes = [
            (1, 100, 2, "secondary_index_2", 1, "TypeA"),
            (2, 200, 1, "secondary_index_1", 4, "TypeA"),
            (4, 400, 0, "secondary_index_0", 8, "TypeA"),
            (3, 300, 5, "secondary_index_5", 7, "TypeC"),
        ];

        let chunk_size = 50;
        let df_view = build_nodes_df_with_secondary_index(chunk_size, &nodes);
        let g = Graph::new();
        let props = ["str_prop", "int_prop"];
        let secondary_index = Some("secondary_index");

        // Load nodes from DataFrame with secondary_index
        load_nodes_from_df(
            df_view,
            "time",
            secondary_index,
            "node_id",
            &props,
            &[],
            None,
            None,
            None,
            &g,
            true,
        )
        .unwrap();

        let df_view = build_nodes_df_with_secondary_index(chunk_size, &nodes_no_dupes);

        load_node_props_from_df(
            df_view,
            "node_id",
            None,
            Some("node_type"),
            None,
            None,
            &[],
            None,
            &g,
        )
        .unwrap();

        let g2 = Graph::new();

        for (node_id, time, secondary_index, str_prop, int_prop, node_type) in nodes {
            let time_with_secondary_index = EventTime(time, secondary_index as usize);

            g2.add_node(
                time_with_secondary_index,
                node_id,
                [
                    ("str_prop", str_prop.into_prop()),
                    ("int_prop", int_prop.into_prop()),
                ],
                Some(node_type),
            )
            .unwrap();
        }

        // Internally checks whether temporal props are sorted by
        // secondary index.
        assert_graph_equal(&g, &g2);

        // Both graphs should have the same event_id / secondary_index
        assert_eq!(
            g.write_session().unwrap().read_event_id().unwrap(),
            g2.write_session().unwrap().read_event_id().unwrap(),
        );

        assert_eq!(
            g.write_session().unwrap().read_event_id().unwrap(),
            10 // max secondary index in nodes
        );

        let mut act_node_types = g
            .nodes()
            .node_type()
            .compute()
            .into_iter()
            .filter_map(|(node, val)| Some((node.id().as_u64()?, val)))
            .collect_vec();
        act_node_types.sort();
        let exp_node_types = vec![
            (1u64, Some(ArcStr::from("TypeA"))),
            (2u64, Some(ArcStr::from("TypeA"))),
            (3u64, Some(ArcStr::from("TypeC"))),
            (4u64, Some(ArcStr::from("TypeA"))),
        ];
        assert_eq!(act_node_types, exp_node_types);

        let mut act_node_types = g.nodes().node_type().iter_values().collect_vec();
        act_node_types.sort();
        let exp_node_types = vec![
            Some(ArcStr::from("TypeA")),
            Some(ArcStr::from("TypeA")),
            Some(ArcStr::from("TypeA")),
            Some(ArcStr::from("TypeC")),
        ];
        assert_eq!(act_node_types, exp_node_types);
    }
}

#[cfg(test)]
#[cfg(feature = "io")]
mod parquet_tests {
    use bigdecimal::BigDecimal;
    use chrono::{DateTime, Utc};
    use proptest::prelude::*;
    use raphtory::{
        db::graph::{
            graph::{assert_graph_equal, assert_graph_equal_timestamps},
            views::deletion_graph::PersistentGraph,
        },
        prelude::*,
        test_utils::{
            build_edge_list_dyn, build_graph, build_graph_strat, build_nodes_dyn, build_props_dyn,
            EdgeFixture, EdgeUpdatesFixture, GraphFixture, NodeFixture, NodeUpdatesFixture,
            PropUpdatesFixture,
        },
    };
    use std::{io::Cursor, str::FromStr};
    use zip::{ZipArchive, ZipWriter};

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
        let g2 = Graph::decode_parquet(&temp_dir, None).unwrap();
        assert_graph_equal_timestamps(&g, &g2);
    }

    fn check_parquet_encoding_deletions(g: PersistentGraph) {
        let temp_dir = tempfile::tempdir().unwrap();
        g.encode_parquet(&temp_dir).unwrap();
        let g2 = PersistentGraph::decode_parquet(&temp_dir, None).unwrap();
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
        let g2 = Graph::decode_parquet(&temp_dir, None).unwrap();
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
        proptest!(|(props in build_props_dyn(0..=10))| {
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
        proptest!(|(nodes in build_nodes_dyn(0..10, 0..=10, 0..=10))| {
            build_and_check_parquet_encoding(nodes.into());
        });
    }
    #[test]
    fn write_edges_any_props_to_parquet() {
        proptest!(|(edges in build_edge_list_dyn(0..=10, 0..10, 0..=10, 0..=10, true))| {
            build_and_check_parquet_encoding(edges.into());
        });
    }

    #[test]
    fn write_graph_to_parquet() {
        proptest!(|(edges in build_graph_strat(10, 10, 10, 10, true))| {
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

    #[test]
    fn test_parquet_zip_simple() {
        let g = Graph::new();

        g.add_edge(0, 0, 1, [("test prop 1", Prop::map(NO_PROPS))], None)
            .unwrap();
        g.add_edge(
            1,
            2,
            3,
            [("test prop 1", Prop::map([("key", "value")]))],
            Some("layer_a"),
        )
        .unwrap();
        g.add_edge(2, 3, 4, [("test prop 2", "value")], Some("layer_b"))
            .unwrap();
        g.add_edge(3, 1, 4, [("test prop 3", 10.0)], None).unwrap();
        g.add_edge(4, 1, 3, [("test prop 4", true)], None).unwrap();

        let temp_dir = tempfile::tempdir().unwrap();
        let zip_path = temp_dir.path().join("test_graph.zip");

        // Test writing to a file
        let file = std::fs::File::create(&zip_path).unwrap();
        let mut writer = ZipWriter::new(file);
        g.encode_parquet_to_zip(&mut writer, "graph").unwrap();
        writer.finish().unwrap();

        let mut reader = ZipArchive::new(std::fs::File::open(&zip_path).unwrap()).unwrap();
        let g2 =
            Graph::decode_parquet_from_zip(&mut reader, None::<&std::path::Path>, "graph").unwrap();
        assert_graph_equal(&g, &g2);
    }

    #[test]
    fn test_parquet_bytes_simple() {
        let g = Graph::new();

        g.add_edge(0, 0, 1, [("test prop 1", Prop::map(NO_PROPS))], None)
            .unwrap();
        g.add_edge(
            1,
            2,
            3,
            [("test prop 1", Prop::map([("key", "value")]))],
            Some("layer_a"),
        )
        .unwrap();
        g.add_edge(2, 3, 4, [("test prop 2", "value")], Some("layer_b"))
            .unwrap();
        g.add_edge(3, 1, 4, [("test prop 3", 10.0)], None).unwrap();
        g.add_edge(4, 1, 3, [("test prop 4", true)], None).unwrap();

        let mut bytes = Vec::new();
        let mut writer = ZipWriter::new(Cursor::new(&mut bytes));
        g.encode_parquet_to_zip(&mut writer, "graph").unwrap();
        writer.finish().unwrap();
        let g2 =
            Graph::decode_parquet_from_bytes(&bytes, None::<&std::path::Path>, "graph").unwrap();
        assert_graph_equal(&g, &g2);
    }

    #[test]
    fn test_parquet_bytes_proptest() {
        proptest!(|(edges in build_graph_strat(30, 30, 10, 10, true))| {
            let g = Graph::from(build_graph(&edges));
            let mut bytes = Vec::new();
            let mut writer = ZipWriter::new(Cursor::new(&mut bytes));
            g.encode_parquet_to_zip(&mut writer, "graph").unwrap();
            writer.finish().unwrap();
            let g2 = Graph::decode_parquet_from_bytes(&bytes, None::<&std::path::Path>, "graph").unwrap();

            assert_graph_equal(&g, &g2);
        })
    }
}
