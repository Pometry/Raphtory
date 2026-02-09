use proptest::prelude::*;
use raphtory::{
    algorithms::community_detection::{
        label_propagation::label_propagation,
        louvain::louvain,
        modularity::{ComID, ConstModularity, ModularityFunction, ModularityUnDir, Partition},
    },
    logging::global_info_logger,
    prelude::*,
    test_storage,
};
use raphtory_core::entities::VID;
use std::collections::HashSet;
use tracing::info;

mod lpa {
    use super::*;
    #[test]
    fn lpa_test() {
        let graph: Graph = Graph::new();
        let edges = vec![
            (1, "R1", "R2"),
            (1, "R2", "R3"),
            (1, "R3", "G"),
            (1, "G", "B1"),
            (1, "G", "B3"),
            (1, "B1", "B2"),
            (1, "B2", "B3"),
            (1, "B2", "B4"),
            (1, "B3", "B4"),
            (1, "B3", "B5"),
            (1, "B4", "B5"),
        ];
        for (ts, src, dst) in edges {
            graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
        }
        test_storage!(&graph, |graph| {
            let seed = Some([5; 32]);
            let result = label_propagation(graph, seed).unwrap();

            let expected = vec![
                HashSet::from([
                    graph.node("R1").unwrap(),
                    graph.node("R2").unwrap(),
                    graph.node("R3").unwrap(),
                ]),
                HashSet::from([
                    graph.node("G").unwrap(),
                    graph.node("B1").unwrap(),
                    graph.node("B2").unwrap(),
                    graph.node("B3").unwrap(),
                    graph.node("B4").unwrap(),
                    graph.node("B5").unwrap(),
                ]),
            ];
            for hashset in expected {
                assert!(result.contains(&hashset));
            }
        });
    }
}

mod louvain {
    use super::*;
    use std::fs;
    #[test]
    fn test_louvain() {
        let edges = vec![
            (100, 200, 2.0f64),
            (100, 300, 3.0f64),
            (200, 300, 8.5f64),
            (300, 400, 1.0f64),
            (400, 500, 1.5f64),
            (600, 800, 0.5f64),
            (700, 900, 3.5f64),
            (100, 600, 1.5f64),
        ];
        test_all_nodes_assigned_inner(edges)
    }

    fn test_all_nodes_assigned_inner(edges: Vec<(u64, u64, f64)>) {
        let graph = Graph::new();
        for (src, dst, weight) in edges {
            graph
                .add_edge(1, src, dst, [("weight", weight)], None)
                .unwrap();
            graph
                .add_edge(1, dst, src, [("weight", weight)], None)
                .unwrap();
        }

        test_storage!(&graph, |graph| {
            let result = louvain::<ModularityUnDir, _>(graph, 1.0, Some("weight"), None);
            assert!(graph
                .nodes()
                .iter()
                .all(|n| result.get_by_node(n).is_some()));

            let result = louvain::<ConstModularity, _>(graph, 1.0, Some("weight"), None);
            assert!(graph
                .nodes()
                .iter()
                .all(|n| result.get_by_node(n).is_some()));
        });
    }

    fn test_all_nodes_assigned_inner_unweighted(edges: Vec<(u64, u64)>) {
        let graph = Graph::new();
        for (src, dst) in edges {
            graph.add_edge(1, src, dst, NO_PROPS, None).unwrap();
            graph.add_edge(1, dst, src, NO_PROPS, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let result = louvain::<ModularityUnDir, _>(graph, 1.0, None, None);
            assert!(graph
                .nodes()
                .iter()
                .all(|n| result.get_by_node(n).is_some()));

            let result = louvain::<ConstModularity, _>(graph, 1.0, None, None);
            assert!(graph
                .nodes()
                .iter()
                .all(|n| result.get_by_node(n).is_some()));
        });
    }

    proptest! {
        #[test]
        fn test_all_nodes_in_communities(edges in any::<Vec<(u64, u64, f64)>>().prop_map(|mut v| {v.iter_mut().for_each(|(_, _, w)| *w = w.abs()); v})) {
            test_all_nodes_assigned_inner(edges)
        }

        #[test]
        fn test_all_nodes_assigned_unweighted(edges in any::<Vec<(u8, u8)>>().prop_map(|v| v.into_iter().map(|(s, d)|  (s as u64, d as u64)).collect::<Vec<_>>())) {
            test_all_nodes_assigned_inner_unweighted(edges)
        }
    }

    #[cfg(feature = "io")]
    #[test]
    fn lfr_test() {
        use raphtory::io::csv_loader::CsvLoader;
        use raphtory_api::core::utils::logging::global_info_logger;
        use serde::{Deserialize, Serialize};
        use std::path::PathBuf;
        global_info_logger();
        let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("resources/test");
        let loader = CsvLoader::new(d.join("test.csv")).set_delimiter(",");
        let graph = Graph::new();

        #[derive(Deserialize, Serialize, Debug)]
        struct CsvEdge {
            src: u64,
            dst: u64,
        }

        loader
            .load_into_graph(&graph, |e: CsvEdge, g| {
                g.add_edge(1, e.src, e.dst, NO_PROPS, None).unwrap();
            })
            .unwrap();

        let expected_coms: Vec<Vec<u64>> =
            serde_json::from_str(&fs::read_to_string(d.join("communities.json")).unwrap()).unwrap();

        let expected_coms = Partition::from_coms(
            expected_coms
                .into_iter()
                .map(|com| {
                    com.into_iter()
                        .map(|id| graph.node(id).unwrap().node)
                        .collect()
                })
                .collect(),
        );

        test_storage!(&graph, |graph| {
            let coms = louvain::<ModularityUnDir, _>(graph, 1.0, None, None);
            let partition = Partition::from_iter(coms.iter_values().copied());
            let mi = partition.nmi(&expected_coms);
            assert!(mi > 0.5)
        });

        test_storage!(&graph, |graph| {
            let coms = louvain::<ConstModularity, _>(graph, 1.0, None, None);
            let partition = Partition::from_iter(coms.iter_values().copied());
            let mi = partition.nmi(&expected_coms);
            assert!(mi > 0.5)
        });
    }

    #[test]
    fn test_delta() {
        global_info_logger();
        let graph = Graph::new();
        graph.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        graph.add_edge(0, 2, 1, NO_PROPS, None).unwrap();

        test_storage!(&graph, |graph| {
            let mut m = ModularityUnDir::new(
                graph,
                None,
                1.0,
                Partition::new_singletons(graph.count_nodes()),
                1e-8,
            );
            let old_value = m.value();
            assert_eq!(old_value, -0.5);
            let delta = m.move_delta(&VID(0), ComID(1));
            info!("delta: {delta}");
            m.move_node(&VID(0), ComID(1));
            assert_eq!(m.value(), old_value + delta)
        });
    }

    #[test]
    fn test_delta_const() {
        global_info_logger();
        let graph = Graph::new();
        graph.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        graph.add_edge(0, 2, 1, NO_PROPS, None).unwrap();

        test_storage!(&graph, |graph| {
            let mut m = ConstModularity::new(
                graph,
                None,
                1.0,
                Partition::new_singletons(graph.count_nodes()),
                1e-8,
            );
            let old_value = m.value();
            assert_eq!(old_value, -0.5);
            let delta = m.move_delta(&VID(0), ComID(1));
            info!("delta: {delta}");
            m.move_node(&VID(0), ComID(1));
            assert_eq!(m.value(), old_value + delta)
        });
    }

    #[test]
    fn test_aggregation() {
        global_info_logger();
        let graph = Graph::new();
        graph.add_edge(0, 0, 1, NO_PROPS, None).unwrap();
        graph.add_edge(0, 1, 0, NO_PROPS, None).unwrap();
        graph.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        graph.add_edge(0, 2, 1, NO_PROPS, None).unwrap();
        graph.add_edge(0, 0, 3, NO_PROPS, None).unwrap();
        graph.add_edge(0, 3, 0, NO_PROPS, None).unwrap();

        test_storage!(&graph, |graph| {
            let partition = Partition::from_iter([0usize, 0, 1, 1]);
            let mut m = ModularityUnDir::new(graph, None, 1.0, partition, 1e-8);
            let value_before = m.value();
            let _ = m.aggregate();
            let value_after = m.value();
            info!("before: {value_before}, after: {value_after}");
            assert_eq!(value_after, value_before);
            let delta = m.move_delta(&VID(0), ComID(1));
            m.move_node(&VID(0), ComID(1));
            let value_merged = m.value();
            assert_eq!(value_merged, 0.0);
            assert!((value_merged - (value_after + delta)).abs() < 1e-8);
        });
    }

    #[test]
    fn test_aggregation_const() {
        global_info_logger();
        let graph = Graph::new();
        graph.add_edge(0, 0, 1, NO_PROPS, None).unwrap();
        graph.add_edge(0, 1, 0, NO_PROPS, None).unwrap();
        graph.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        graph.add_edge(0, 2, 1, NO_PROPS, None).unwrap();
        graph.add_edge(0, 0, 3, NO_PROPS, None).unwrap();
        graph.add_edge(0, 3, 0, NO_PROPS, None).unwrap();

        test_storage!(&graph, |graph| {
            let partition = Partition::from_iter([0usize, 0, 1, 1]);
            let mut m = ConstModularity::new(graph, None, 1.0, partition, 1e-8);
            let value_before = m.value();
            let _ = m.aggregate();
            let value_after = m.value();
            info!("before: {value_before}, after: {value_after}");
            assert_eq!(value_after, value_before);
            let delta = m.move_delta(&VID(0), ComID(1));
            m.move_node(&VID(0), ComID(1));
            let value_merged = m.value();
            assert_eq!(value_merged, 0.0);
            assert!((value_merged - (value_after + delta)).abs() < 1e-8);
        });
    }
}
