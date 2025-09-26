mod test_utils;

#[cfg(feature = "storage")]
#[cfg(test)]
mod test {
    use bigdecimal::BigDecimal;
    use itertools::Itertools;
    use polars_arrow::array::Utf8Array;
    use std::{path::PathBuf, str::FromStr};
    use tempfile::TempDir;

    use pometry_storage::{graph::TemporalGraph, properties::Properties};
    use raphtory::{db::graph::graph::assert_graph_equal, prelude::*};
    use raphtory_api::core::entities::properties::prop::Prop;
    use raphtory_storage::disk::ParquetLayerCols;

    #[test]
    fn test_no_prop_nodes() {
        let test_dir = TempDir::new().unwrap();
        let g = Graph::new();
        g.add_node(0, 0, NO_PROPS, None).unwrap();
        // g.add_node(1, 1, [("test", "test")], None).unwrap();
        let disk_g = g.persist_as_disk_graph(test_dir.path()).unwrap();
        assert_eq!(disk_g.node(0).unwrap().earliest_time(), Some(0));
        assert_graph_equal(&g, &disk_g);
    }

    #[test]
    fn test_mem_to_disk_graph() {
        let mem_graph = Graph::new();
        mem_graph.add_edge(0, 0, 1, [("test", 0u64)], None).unwrap();
        let test_dir = TempDir::new().unwrap();
        let disk_graph =
            TemporalGraph::from_graph(&mem_graph, test_dir.path(), || Ok(Properties::default()))
                .unwrap();
        assert_eq!(disk_graph.num_nodes(), 2);
        assert_eq!(disk_graph.num_edges(), 1);
    }

    #[test]
    fn test_node_properties() {
        let mem_graph = Graph::new();
        let node = mem_graph
            .add_node(
                0,
                0,
                [
                    ("test_num", 0u64.into_prop()),
                    ("test_str", "test".into_prop()),
                ],
                None,
            )
            .unwrap();
        node.add_metadata([
            ("const_str", "test_c".into_prop()),
            ("const_float", 0.314f64.into_prop()),
        ])
        .unwrap();
        let test_dir = TempDir::new().unwrap();
        let disk_graph = mem_graph.persist_as_disk_graph(test_dir.path()).unwrap();
        assert_eq!(disk_graph.count_nodes(), 1);
        let props = disk_graph.node(0).unwrap().properties();
        let metadata = disk_graph.node(0).unwrap().metadata();
        assert_eq!(props.get("test_num").unwrap_u64(), 0);
        assert_eq!(props.get("test_str").unwrap_str(), "test");
        assert_eq!(metadata.get("const_str").unwrap_str(), "test_c");
        assert_eq!(metadata.get("const_float").unwrap_f64(), 0.314);

        let temp = disk_graph.node(0).unwrap().properties().temporal();
        assert_eq!(
            temp.get("test_num").unwrap().latest().unwrap(),
            0u64.into_prop()
        );
        assert_eq!(
            temp.get("test_str").unwrap().latest().unwrap(),
            "test".into_prop()
        );

        drop(disk_graph);

        let disk_graph: Graph = DiskGraphStorage::load_from_dir(test_dir.path())
            .unwrap()
            .into();
        let props = disk_graph.node(0).unwrap().properties();
        let metadata = disk_graph.node(0).unwrap().metadata();
        assert_eq!(props.get("test_num").unwrap_u64(), 0);
        assert_eq!(props.get("test_str").unwrap_str(), "test");
        assert_eq!(metadata.get("const_str").unwrap_str(), "test_c");
        assert_eq!(metadata.get("const_float").unwrap_f64(), 0.314);

        let temp = disk_graph.node(0).unwrap().properties().temporal();
        assert_eq!(
            temp.get("test_num").unwrap().latest().unwrap(),
            0u64.into_prop()
        );
        assert_eq!(
            temp.get("test_str").unwrap().latest().unwrap(),
            "test".into_prop()
        );
    }

    #[test]
    fn test_node_properties_2() {
        let g = Graph::new();
        g.add_edge(1, 1u64, 1u64, NO_PROPS, None).unwrap();
        let props_t1 = [
            ("prop 1", 1u64.into_prop()),
            ("prop 3", "hi".into_prop()),
            ("prop 4", true.into_prop()),
        ];
        let v = g.add_node(1, 1u64, props_t1, None).unwrap();
        let props_t2 = [
            ("prop 1", 2u64.into_prop()),
            ("prop 2", 0.6.into_prop()),
            ("prop 4", false.into_prop()),
        ];
        v.add_updates(2, props_t2).unwrap();
        let props_t3 = [
            ("prop 2", 0.9.into_prop()),
            ("prop 3", "hello".into_prop()),
            ("prop 4", true.into_prop()),
        ];
        v.add_updates(3, props_t3).unwrap();
        v.add_metadata([("static prop", 123)]).unwrap();

        let test_dir = TempDir::new().unwrap();
        let disk_graph = g.persist_as_disk_graph(test_dir.path()).unwrap();

        let actual = disk_graph
            .at(2)
            .node(1u64)
            .unwrap()
            .properties()
            .temporal()
            .into_iter()
            .map(|(key, t_view)| (key.to_string(), t_view.into_iter().collect::<Vec<_>>()))
            .filter(|(_, v)| !v.is_empty())
            .collect::<Vec<_>>();

        let expected = vec![
            ("prop 1".to_string(), vec![(2, 2u64.into_prop())]),
            ("prop 4".to_string(), vec![(2, false.into_prop())]),
            ("prop 2".to_string(), vec![(2, 0.6.into_prop())]),
        ];

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_only_const_node_properties() {
        let g = Graph::new();
        let v = g.add_node(0, 1, NO_PROPS, None).unwrap();
        v.add_metadata([("test", "test")]).unwrap();
        let test_dir = TempDir::new().unwrap();
        let disk_graph = g.persist_as_disk_graph(test_dir.path()).unwrap();
        assert_eq!(
            disk_graph
                .node(1)
                .unwrap()
                .metadata()
                .get("test")
                .unwrap_str(),
            "test"
        );
        let disk_graph = DiskGraphStorage::load_from_dir(test_dir.path())
            .unwrap()
            .into_graph();
        assert_eq!(
            disk_graph
                .node(1)
                .unwrap()
                .metadata()
                .get("test")
                .unwrap_str(),
            "test"
        );
    }

    #[test]
    fn test_type_filter_disk_graph_loaded_from_parquets() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let graph_dir = tmp_dir.path();
        let chunk_size = 268_435_456;
        let num_threads = 4;
        let t_props_chunk_size = chunk_size / 8;

        let netflow_layer_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .map(|p| p.join("pometry-storage-private/resources/test/netflow.parquet"))
            .unwrap();

        let v1_layer_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .map(|p| p.join("pometry-storage-private/resources/test/wls.parquet"))
            .unwrap();

        let node_properties = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .map(|p| p.join("pometry-storage-private/resources/test/node_types.parquet"))
            .unwrap();

        let layer_parquet_cols = vec![
            ParquetLayerCols {
                parquet_dir: netflow_layer_path.to_str().unwrap(),
                layer: "netflow",
                src_col: "source",
                dst_col: "destination",
                time_col: "time",
                exclude_edge_props: vec![],
            },
            ParquetLayerCols {
                parquet_dir: v1_layer_path.to_str().unwrap(),
                layer: "wls",
                src_col: "src",
                dst_col: "dst",
                time_col: "Time",
                exclude_edge_props: vec![],
            },
        ];

        let node_type_col = Some("node_type");

        let g = DiskGraphStorage::load_from_parquets(
            graph_dir,
            layer_parquet_cols,
            Some(&node_properties),
            chunk_size,
            t_props_chunk_size,
            num_threads,
            node_type_col,
            None,
        )
        .unwrap()
        .into_graph();

        assert_eq!(
            g.nodes().type_filter(["A"]).name().collect_vec(),
            vec!["Comp710070", "Comp844043"]
        );

        assert_eq!(
            g.nodes()
                .type_filter(Vec::<String>::new())
                .name()
                .collect_vec(),
            Vec::<String>::new()
        );

        assert_eq!(
            g.nodes().type_filter([""]).name().collect_vec(),
            Vec::<String>::new()
        );

        assert_eq!(
            g.nodes()
                .type_filter(["A"])
                .neighbours()
                .name()
                .map(|n| { n.collect::<Vec<_>>() })
                .collect_vec(),
            vec![vec!["Comp844043"], vec!["Comp710070"]]
        );

        assert_eq!(
            g.nodes()
                .type_filter(["A", "B"])
                .neighbours()
                .name()
                .map(|n| { n.collect::<Vec<_>>() })
                .collect_vec(),
            vec![vec!["Comp244393"], vec!["Comp844043"], vec!["Comp710070"]]
        );

        assert_eq!(
            g.nodes()
                .type_filter(["C"])
                .neighbours()
                .name()
                .map(|n| { n.collect::<Vec<_>>() })
                .collect_vec(),
            Vec::<Vec<&str>>::new()
        );

        assert_eq!(
            g.nodes()
                .type_filter(["A"])
                .neighbours()
                .type_filter(["A"])
                .name()
                .map(|n| { n.collect::<Vec<_>>() })
                .collect_vec(),
            vec![vec!["Comp844043"], vec!["Comp710070"]]
        );

        assert_eq!(
            g.nodes()
                .type_filter(["A"])
                .neighbours()
                .type_filter(Vec::<&str>::new())
                .name()
                .map(|n| { n.collect::<Vec<_>>() })
                .collect_vec(),
            vec![vec![], Vec::<&str>::new()]
        );

        let w = g.window(6415659, 7387801);

        assert_eq!(
            w.nodes().type_filter(["A"]).name().collect_vec(),
            vec!["Comp710070", "Comp844043"]
        );

        assert_eq!(
            w.nodes()
                .type_filter(Vec::<String>::new())
                .name()
                .collect_vec(),
            Vec::<String>::new()
        );

        assert_eq!(
            w.nodes().type_filter([""]).name().collect_vec(),
            Vec::<String>::new()
        );

        let l = g.layers(["netflow"]).unwrap();

        assert_eq!(
            l.nodes().type_filter(["A"]).name().collect_vec(),
            vec!["Comp710070", "Comp844043"]
        );

        assert_eq!(
            l.nodes()
                .type_filter(Vec::<String>::new())
                .name()
                .collect_vec(),
            Vec::<String>::new()
        );

        assert_eq!(
            l.nodes().type_filter([""]).name().collect_vec(),
            Vec::<String>::new()
        );
    }

    #[test]
    fn test_type_filter_disk_graph_created_from_in_memory_graph() {
        let g = Graph::new();
        g.add_node(1, 1, NO_PROPS, Some("a")).unwrap();
        g.add_node(1, 2, NO_PROPS, Some("b")).unwrap();
        g.add_node(1, 3, NO_PROPS, Some("b")).unwrap();
        g.add_node(1, 4, NO_PROPS, Some("a")).unwrap();
        g.add_node(1, 5, NO_PROPS, Some("c")).unwrap();
        g.add_node(1, 6, NO_PROPS, Some("e")).unwrap();
        g.add_node(1, 7, NO_PROPS, None).unwrap();
        g.add_node(1, 8, NO_PROPS, None).unwrap();
        g.add_node(1, 9, NO_PROPS, None).unwrap();
        g.add_edge(2, 1, 2, NO_PROPS, Some("a")).unwrap();
        g.add_edge(2, 3, 2, NO_PROPS, Some("a")).unwrap();
        g.add_edge(2, 2, 4, NO_PROPS, Some("a")).unwrap();
        g.add_edge(2, 4, 5, NO_PROPS, Some("a")).unwrap();
        g.add_edge(2, 4, 5, NO_PROPS, Some("a")).unwrap();
        g.add_edge(2, 5, 6, NO_PROPS, Some("a")).unwrap();
        g.add_edge(2, 3, 6, NO_PROPS, Some("a")).unwrap();

        let tmp_dir = tempfile::tempdir().unwrap();
        let g = DiskGraphStorage::from_graph(&g, tmp_dir.path())
            .unwrap()
            .into_graph();

        assert_eq!(
            g.nodes()
                .type_filter(["a", "b", "c", "e"])
                .name()
                .collect_vec(),
            vec!["1", "2", "3", "4", "5", "6"]
        );

        assert_eq!(
            g.nodes()
                .type_filter(Vec::<String>::new())
                .name()
                .collect_vec(),
            Vec::<String>::new()
        );

        assert_eq!(
            g.nodes().type_filter([""]).name().collect_vec(),
            vec!["7", "8", "9"]
        );

        let g = DiskGraphStorage::load_from_dir(tmp_dir.path())
            .unwrap()
            .into_graph();

        assert_eq!(
            g.nodes()
                .type_filter(["a", "b", "c", "e"])
                .name()
                .collect_vec(),
            vec!["1", "2", "3", "4", "5", "6"]
        );

        assert_eq!(
            g.nodes()
                .type_filter(Vec::<String>::new())
                .name()
                .collect_vec(),
            Vec::<String>::new()
        );

        assert_eq!(
            g.nodes().type_filter([""]).name().collect_vec(),
            vec!["7", "8", "9"]
        );
    }

    #[test]
    fn test_reload() {
        let graph_dir = TempDir::new().unwrap();
        let graph = Graph::new();
        graph.add_edge(0, 0, 1, [("weight", 0.)], None).unwrap();
        graph.add_edge(1, 0, 1, [("weight", 1.)], None).unwrap();
        graph.add_edge(2, 0, 1, [("weight", 2.)], None).unwrap();
        graph.add_edge(3, 1, 2, [("weight", 3.)], None).unwrap();
        let disk_graph = graph.persist_as_disk_graph(graph_dir.path()).unwrap();
        assert_graph_equal(&disk_graph, &graph);

        let reloaded_graph = DiskGraphStorage::load_from_dir(graph_dir.path())
            .unwrap()
            .into_graph();
        assert_graph_equal(&reloaded_graph, &graph);
    }

    #[test]
    fn test_load_node_types() {
        let graph_dir = TempDir::new().unwrap();
        let graph = Graph::new();
        graph.add_edge(0, 0, 1, NO_PROPS, None).unwrap();
        let mut dg = DiskGraphStorage::from_graph(&graph, graph_dir.path()).unwrap();
        dg.load_node_types_from_arrays([Ok(Utf8Array::<i32>::from_slice(["1", "2"]).boxed())], 100)
            .unwrap();
        assert_eq!(
            dg.into_graph().nodes().node_type().collect_vec(),
            [Some("1".into()), Some("2".into())]
        );
    }

    #[test]
    fn test_node_type() {
        let graph_dir = TempDir::new().unwrap();
        let graph = Graph::new();
        graph.add_node(0, 0, NO_PROPS, Some("1")).unwrap();
        graph.add_node(0, 1, NO_PROPS, Some("2")).unwrap();
        graph.add_edge(0, 0, 1, NO_PROPS, None).unwrap();
        let dg = graph.persist_as_disk_graph(graph_dir.path()).unwrap();
        assert_eq!(
            dg.nodes().node_type().collect_vec(),
            [Some("1".into()), Some("2".into())]
        );
        let dg = DiskGraphStorage::load_from_dir(graph_dir.path()).unwrap();
        assert_eq!(
            dg.into_graph().nodes().node_type().collect_vec(),
            [Some("1".into()), Some("2".into())]
        );
    }
    mod addition_bounds {
        use proptest::prelude::*;
        use raphtory::{db::graph::graph::assert_graph_equal, prelude::*};
        use raphtory_storage::disk::DiskGraphStorage;
        use tempfile::TempDir;

        use crate::test_utils::{build_edge_list, build_graph_from_edge_list};

        #[test]
        fn test_load_from_graph_missing_edge() {
            let g = Graph::new();
            g.add_edge(0, 1, 2, [("test", "test1")], Some("1")).unwrap();
            g.add_edge(1, 2, 3, [("test", "test2")], Some("2")).unwrap();
            let test_dir = TempDir::new().unwrap();
            let disk_g = g.persist_as_disk_graph(test_dir.path()).unwrap();
            assert_graph_equal(&disk_g, &g);
        }

        #[test]
        fn disk_graph_persist_proptest() {
            proptest!(|(edges in build_edge_list(100, 10))| {
                let g = build_graph_from_edge_list(&edges);
                let test_dir = TempDir::new().unwrap();
                let disk_g = g.persist_as_disk_graph(test_dir.path()).unwrap();
                assert_graph_equal(&disk_g, &g);
                let reloaded_disk_g = DiskGraphStorage::load_from_dir(test_dir.path()).unwrap().into_graph();
                assert_graph_equal(&reloaded_disk_g, &g);
            } )
        }
    }

    #[test]
    fn load_decimal_column() {
        let parquet_file_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("resources/test/data_0.parquet")
            .to_string_lossy()
            .to_string();

        let graph_dir = tempfile::tempdir().unwrap();

        let layer_parquet_cols = vec![ParquetLayerCols {
            parquet_dir: parquet_file_path.as_ref(),
            layer: "large",
            src_col: "from_address",
            dst_col: "to_address",
            time_col: "block_timestamp",
            exclude_edge_props: vec![],
        }];
        let dgs = DiskGraphStorage::load_from_parquets(
            graph_dir.path(),
            layer_parquet_cols,
            None,
            100,
            100,
            1,
            None,
            None,
        )
        .unwrap();

        let g = dgs.into_graph();
        let (_, actual): (Vec<_>, Vec<_>) = g
            .edges()
            .properties()
            .flat_map(|props| props.temporal().into_iter())
            .flat_map(|(_, view)| view.into_iter())
            .unzip();

        let expected = [
            "20000000000000000000.000000000",
            "20000000000000000000.000000000",
            "20000000000000000000.000000000",
            "24000000000000000000.000000000",
            "20000000000000000000.000000000",
            "104447267751554560119.000000000",
            "42328815976923864739.000000000",
            "23073375143032303343.000000000",
            "23069234889247394908.000000000",
            "18729358881519682914.000000000",
        ]
        .into_iter()
        .map(|s| BigDecimal::from_str(s).map(Prop::Decimal))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

        assert_eq!(actual, expected);
    }
}

#[cfg(feature = "storage")]
#[cfg(test)]
mod storage_tests {
    use std::collections::BTreeSet;

    use itertools::Itertools;
    use proptest::prelude::*;
    use tempfile::TempDir;

    use raphtory::{
        db::graph::graph::assert_graph_equal,
        prelude::{AdditionOps, Graph, GraphViewOps, NodeViewOps, NO_PROPS, *},
    };
    use raphtory_api::core::storage::arc_str::OptionAsStr;
    use raphtory_core::entities::nodes::node_ref::AsNodeRef;
    use raphtory_storage::{disk::DiskGraphStorage, mutation::addition_ops::InternalAdditionOps};

    #[test]
    fn test_merge() {
        let g1 = Graph::new();
        g1.add_node(0, 0, [("node_prop", 0f64)], Some("1")).unwrap();
        g1.add_node(0, 1, NO_PROPS, None).unwrap();
        g1.add_node(0, 2, [("node_prop", 2f64)], Some("2")).unwrap();
        g1.add_edge(1, 0, 1, [("test", 1i32)], None).unwrap();
        g1.add_edge(2, 0, 1, [("test", 2i32)], Some("1")).unwrap();
        g1.add_edge(2, 1, 2, [("test2", "test")], None).unwrap();
        g1.node(1)
            .unwrap()
            .add_metadata([("const_str", "test")])
            .unwrap();
        g1.node(0)
            .unwrap()
            .add_updates(3, [("test", "test")])
            .unwrap();

        let g2 = Graph::new();
        g2.add_node(1, 0, [("node_prop", 1f64)], None).unwrap();
        g2.add_node(0, 1, NO_PROPS, None).unwrap();
        g2.add_node(3, 2, [("node_prop", 3f64)], Some("3")).unwrap();
        g2.add_edge(1, 0, 1, [("test", 2i32)], None).unwrap();
        g2.add_edge(3, 0, 1, [("test", 3i32)], Some("2")).unwrap();
        g2.add_edge(2, 1, 2, [("test2", "test")], None).unwrap();
        g2.node(1)
            .unwrap()
            .add_metadata([("const_str2", "test2")])
            .unwrap();
        g2.node(0)
            .unwrap()
            .add_updates(3, [("test", "test")])
            .unwrap();
        let g1_dir = TempDir::new().unwrap();
        let g2_dir = TempDir::new().unwrap();
        let gm_dir = TempDir::new().unwrap();

        let g1_a = DiskGraphStorage::from_graph(&g1, g1_dir.path()).unwrap();
        let g2_a = DiskGraphStorage::from_graph(&g2, g2_dir.path()).unwrap();

        let gm = g1_a
            .merge_by_sorted_gids(&g2_a, &gm_dir)
            .unwrap()
            .into_graph();

        let n0 = gm.node(0).unwrap();
        assert_eq!(
            n0.properties()
                .temporal()
                .get("node_prop")
                .unwrap()
                .iter()
                .collect_vec(),
            [(0, Prop::F64(0.)), (1, Prop::F64(1.))]
        );
        assert_eq!(
            n0.properties()
                .temporal()
                .get("test")
                .unwrap()
                .iter()
                .collect_vec(),
            [(3, Prop::str("test")), (3, Prop::str("test"))]
        );
        assert_eq!(n0.node_type().as_str(), Some("1"));
        let n1 = gm.node(1).unwrap();
        assert_eq!(n1.metadata().get("const_str"), Some(Prop::str("test")));
        assert_eq!(n1.metadata().get("const_str2").unwrap_str(), "test2");
        assert!(n1
            .properties()
            .temporal()
            .values()
            .all(|prop| prop.values().next().is_none()));
        let n2 = gm.node(2).unwrap();
        assert_eq!(n2.node_type().as_str(), Some("3")); // right has priority

        assert_eq!(
            gm.default_layer()
                .edges()
                .id()
                .filter_map(|(a, b)| a.as_u64().zip(b.as_u64()))
                .collect::<Vec<_>>(),
            [(0, 1), (1, 2)]
        );
        assert_eq!(
            gm.valid_layers("1")
                .edges()
                .id()
                .filter_map(|(a, b)| a.as_u64().zip(b.as_u64()))
                .collect::<Vec<_>>(),
            [(0, 1)]
        );
        assert_eq!(
            gm.valid_layers("2")
                .edges()
                .id()
                .filter_map(|(a, b)| a.as_u64().zip(b.as_u64()))
                .collect::<Vec<_>>(),
            [(0, 1)]
        );
    }

    fn add_edges(g: &Graph, edges: &[(i64, u64, u64)]) {
        let nodes: BTreeSet<_> = edges
            .iter()
            .flat_map(|(_, src, dst)| [*src, *dst])
            .collect();
        for n in nodes {
            g.resolve_node(n.as_node_ref()).unwrap();
        }
        for (t, src, dst) in edges {
            g.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
        }
    }

    fn inner_merge_test(left_edges: &[(i64, u64, u64)], right_edges: &[(i64, u64, u64)]) {
        let left_g = Graph::new();
        add_edges(&left_g, left_edges);
        let right_g = Graph::new();
        add_edges(&right_g, right_edges);
        let merged_g_expected = Graph::new();
        add_edges(&merged_g_expected, left_edges);
        add_edges(&merged_g_expected, right_edges);

        let left_dir = TempDir::new().unwrap();
        let right_dir = TempDir::new().unwrap();
        let merged_dir = TempDir::new().unwrap();

        let left_g_disk = DiskGraphStorage::from_graph(&left_g, left_dir.path()).unwrap();
        let right_g_disk = DiskGraphStorage::from_graph(&right_g, right_dir.path()).unwrap();

        let merged_g_disk = left_g_disk
            .merge_by_sorted_gids(&right_g_disk, &merged_dir)
            .unwrap();
        assert_graph_equal(&merged_g_disk.into_graph(), &merged_g_expected)
    }

    #[test]
    fn test_merge_proptest() {
        proptest!(|(left_edges in prop::collection::vec((0i64..10, 0u64..10, 0u64..10), 0..=100), right_edges in prop::collection::vec((0i64..10, 0u64..10, 0u64..10), 0..=100))| {
            inner_merge_test(&left_edges, &right_edges)
        })
    }

    #[test]
    fn test_merge_simple() {
        let left = [(4, 4, 2), (4, 4, 2)];
        let right = [];
        inner_merge_test(&left, &right);

        let left = [(0, 5, 5)];
        let right = [];
        inner_merge_test(&left, &right);

        let left = [(0, 0, 0), (0, 0, 0), (0, 0, 0)];
        let right = [];
        inner_merge_test(&left, &right);

        let left = [(0, 0, 0), (0, 0, 0), (0, 0, 0)];
        let right = [(0, 0, 0)];
        inner_merge_test(&left, &right);
    }

    #[test]
    fn test_one_empty_graph_non_zero_time() {
        inner_merge_test(&[], &[(1, 0, 0)])
    }
    #[test]
    fn test_empty_graphs() {
        inner_merge_test(&[], &[])
    }

    #[test]
    fn test_one_empty_graph() {
        inner_merge_test(&[], &[(0, 0, 0)])
    }

    #[test]
    fn inbounds_not_merging() {
        inner_merge_test(&[], &[(0, 0, 0), (0, 0, 1), (0, 0, 2)])
    }

    #[test]
    fn inbounds_not_merging_take2() {
        inner_merge_test(
            &[(0, 0, 2)],
            &[
                (0, 1, 0),
                (0, 0, 0),
                (0, 0, 0),
                (0, 0, 0),
                (0, 0, 0),
                (0, 0, 0),
                (0, 0, 0),
            ],
        )
    }

    #[test]
    fn offsets_panic_overflow() {
        inner_merge_test(
            &[
                (0, 0, 4),
                (0, 0, 4),
                (0, 0, 0),
                (0, 0, 4),
                (0, 1, 2),
                (0, 3, 4),
            ],
            &[(0, 0, 5), (0, 2, 0)],
        )
    }

    #[test]
    fn inbounds_not_merging_take3() {
        inner_merge_test(
            &[
                (0, 0, 4),
                (0, 0, 4),
                (0, 0, 0),
                (0, 0, 4),
                (0, 1, 2),
                (0, 3, 4),
            ],
            &[(0, 0, 3), (0, 0, 4), (0, 2, 2), (0, 0, 5), (0, 0, 6)],
        )
    }
}
