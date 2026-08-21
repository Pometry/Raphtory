mod dijkstra_tests {
    use itertools::Itertools;
    use raphtory::{
        algorithms::pathing::dijkstra::dijkstra_single_source_shortest_paths,
        db::{api::mutation::AdditionOps, graph::graph::Graph},
        prelude::*,
    };
    use raphtory_api::core::Direction;

    use raphtory_tests::test_storage;
    fn load_graph(edges: Vec<(i64, &str, &str, Vec<(&str, f32)>)>) -> Graph {
        let graph = Graph::new();

        for (t, src, dst, props) in edges {
            graph.add_edge(t, src, dst, props, None).unwrap();
        }
        graph
    }

    fn basic_graph() -> Graph {
        load_graph(vec![
            (0, "A", "B", vec![("weight", 4.0f32)]),
            (1, "A", "C", vec![("weight", 4.0f32)]),
            (2, "B", "C", vec![("weight", 2.0f32)]),
            (3, "C", "D", vec![("weight", 3.0f32)]),
            (4, "C", "E", vec![("weight", 1.0f32)]),
            (5, "C", "F", vec![("weight", 6.0f32)]),
            (6, "D", "F", vec![("weight", 2.0f32)]),
            (7, "E", "F", vec![("weight", 3.0f32)]),
        ])
    }

    #[test]
    fn test_dijkstra_multiple_targets() {
        let graph = basic_graph();

        test_storage!(&graph, |graph| {
            let targets: Vec<&str> = vec!["D", "F"];
            let results = dijkstra_single_source_shortest_paths(
                graph,
                "A",
                targets,
                Some("weight"),
                Direction::OUT,
            );

            let results = results.unwrap();

            assert_eq!(results.get_by_node("D").unwrap().distance, 7.0f64);
            assert_eq!(
                results
                    .get_by_node("D")
                    .unwrap()
                    .path
                    .into_iter()
                    .map(|value| graph.node(value).unwrap().name())
                    .collect_vec(),
                vec!["A", "C", "D"]
            );

            assert_eq!(results.get_by_node("F").unwrap().distance, 8.0f64);
            assert_eq!(
                results
                    .get_by_node("F")
                    .unwrap()
                    .path
                    .into_iter()
                    .map(|value| graph.node(value).unwrap().name())
                    .collect_vec(),
                vec!["A", "C", "E", "F"]
            );

            let targets: Vec<&str> = vec!["D", "E", "F"];
            let results = dijkstra_single_source_shortest_paths(
                graph,
                "B",
                targets,
                Some("weight"),
                Direction::OUT,
            );
            let results = results.unwrap();
            assert_eq!(results.get_by_node("D").unwrap().distance, 5.0f64);
            assert_eq!(results.get_by_node("E").unwrap().distance, 3.0f64);
            assert_eq!(results.get_by_node("F").unwrap().distance, 6.0f64);
            assert_eq!(
                results
                    .get_by_node("D")
                    .unwrap()
                    .path
                    .into_iter()
                    .map(|value| graph.node(value).unwrap().name())
                    .collect_vec(),
                vec!["B", "C", "D"]
            );
            assert_eq!(
                results
                    .get_by_node("E")
                    .unwrap()
                    .path
                    .into_iter()
                    .map(|value| graph.node(value).unwrap().name())
                    .collect_vec(),
                vec!["B", "C", "E"]
            );
            assert_eq!(
                results
                    .get_by_node("F")
                    .unwrap()
                    .path
                    .into_iter()
                    .map(|value| graph.node(value).unwrap().name())
                    .collect_vec(),
                vec!["B", "C", "E", "F"]
            );
        });
    }

    #[test]
    fn test_dijkstra_no_weight() {
        let graph = basic_graph();

        test_storage!(&graph, |graph| {
            let targets: Vec<&str> = vec!["C", "E", "F"];
            let results =
                dijkstra_single_source_shortest_paths(graph, "A", targets, None, Direction::OUT)
                    .unwrap();
            assert_eq!(
                results
                    .get_by_node("C")
                    .unwrap()
                    .path
                    .into_iter()
                    .map(|value| graph.node(value).unwrap().name())
                    .collect_vec(),
                vec!["A", "C"]
            );
            assert_eq!(
                results
                    .get_by_node("E")
                    .unwrap()
                    .path
                    .into_iter()
                    .map(|value| graph.node(value).unwrap().name())
                    .collect_vec(),
                vec!["A", "C", "E"]
            );
            assert_eq!(
                results
                    .get_by_node("F")
                    .unwrap()
                    .path
                    .into_iter()
                    .map(|value| graph.node(value).unwrap().name())
                    .collect_vec(),
                vec!["A", "C", "F"]
            );
        });
    }

    #[test]
    fn test_dijkstra_multiple_targets_node_ids() {
        let edges = vec![
            (0, 1, 2, vec![("weight", 4u64)]),
            (1, 1, 3, vec![("weight", 4u64)]),
            (2, 2, 3, vec![("weight", 2u64)]),
            (3, 3, 4, vec![("weight", 3u64)]),
            (4, 3, 5, vec![("weight", 1u64)]),
            (5, 3, 6, vec![("weight", 6u64)]),
            (6, 4, 6, vec![("weight", 2u64)]),
            (7, 5, 6, vec![("weight", 3u64)]),
        ];

        let graph = Graph::new();
        for (t, src, dst, props) in edges {
            graph.add_edge(t, src, dst, props, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let targets = vec![4, 6];
            let results = dijkstra_single_source_shortest_paths(
                graph,
                1,
                targets,
                Some("weight"),
                Direction::OUT,
            );
            let results = results.unwrap();
            assert_eq!(results.get_by_node("4").unwrap().distance, 7f64);
            assert_eq!(
                results
                    .get_by_node("4")
                    .unwrap()
                    .path
                    .into_iter()
                    .map(|value| graph.node(value).unwrap().name())
                    .collect_vec(),
                vec!["1", "3", "4"]
            );

            assert_eq!(results.get_by_node("6").unwrap().distance, 8f64);
            assert_eq!(
                results
                    .get_by_node("6")
                    .unwrap()
                    .path
                    .into_iter()
                    .map(|value| graph.node(value).unwrap().name())
                    .collect_vec(),
                vec!["1", "3", "5", "6"]
            );

            let targets = vec![4, 5, 6];
            let results = dijkstra_single_source_shortest_paths(
                graph,
                2,
                targets,
                Some("weight"),
                Direction::OUT,
            );
            let results = results.unwrap();
            assert_eq!(results.get_by_node("4").unwrap().distance, 5f64);
            assert_eq!(results.get_by_node("5").unwrap().distance, 3f64);
            assert_eq!(results.get_by_node("6").unwrap().distance, 6f64);
            assert_eq!(
                results
                    .get_by_node("4")
                    .unwrap()
                    .path
                    .into_iter()
                    .map(|value| graph.node(value).unwrap().name())
                    .collect_vec(),
                vec!["2", "3", "4"]
            );
            assert_eq!(
                results
                    .get_by_node("5")
                    .unwrap()
                    .path
                    .into_iter()
                    .map(|value| graph.node(value).unwrap().name())
                    .collect_vec(),
                vec!["2", "3", "5"]
            );
            assert_eq!(
                results
                    .get_by_node("6")
                    .unwrap()
                    .path
                    .into_iter()
                    .map(|value| graph.node(value).unwrap().name())
                    .collect_vec(),
                vec!["2", "3", "5", "6"]
            );
        });
    }

    #[test]
    fn test_dijkstra_multiple_targets_u64() {
        let edges = vec![
            (0, "A", "B", vec![("weight", 4u64)]),
            (1, "A", "C", vec![("weight", 4u64)]),
            (2, "B", "C", vec![("weight", 2u64)]),
            (3, "C", "D", vec![("weight", 3u64)]),
            (4, "C", "E", vec![("weight", 1u64)]),
            (5, "C", "F", vec![("weight", 6u64)]),
            (6, "D", "F", vec![("weight", 2u64)]),
            (7, "E", "F", vec![("weight", 3u64)]),
        ];

        let graph = Graph::new();

        for (t, src, dst, props) in edges {
            graph.add_edge(t, src, dst, props, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let targets: Vec<&str> = vec!["D", "F"];
            let results = dijkstra_single_source_shortest_paths(
                graph,
                "A",
                targets,
                Some("weight"),
                Direction::OUT,
            );
            let results = results.unwrap();
            assert_eq!(results.get_by_node("D").unwrap().distance, 7f64);
            assert_eq!(
                results
                    .get_by_node("D")
                    .unwrap()
                    .path
                    .into_iter()
                    .map(|value| graph.node(value).unwrap().name())
                    .collect_vec(),
                vec!["A", "C", "D"]
            );

            assert_eq!(results.get_by_node("F").unwrap().distance, 8f64);
            assert_eq!(
                results
                    .get_by_node("F")
                    .unwrap()
                    .path
                    .into_iter()
                    .map(|value| graph.node(value).unwrap().name())
                    .collect_vec(),
                vec!["A", "C", "E", "F"]
            );

            let targets: Vec<&str> = vec!["D", "E", "F"];
            let results = dijkstra_single_source_shortest_paths(
                graph,
                "B",
                targets,
                Some("weight"),
                Direction::OUT,
            );
            let results = results.unwrap();
            assert_eq!(results.get_by_node("D").unwrap().distance, 5f64);
            assert_eq!(results.get_by_node("E").unwrap().distance, 3f64);
            assert_eq!(results.get_by_node("F").unwrap().distance, 6f64);
            assert_eq!(
                results
                    .get_by_node("D")
                    .unwrap()
                    .path
                    .into_iter()
                    .map(|value| graph.node(value).unwrap().name())
                    .collect_vec(),
                vec!["B", "C", "D"]
            );
            assert_eq!(
                results
                    .get_by_node("E")
                    .unwrap()
                    .path
                    .into_iter()
                    .map(|value| graph.node(value).unwrap().name())
                    .collect_vec(),
                vec!["B", "C", "E"]
            );
            assert_eq!(
                results
                    .get_by_node("F")
                    .unwrap()
                    .path
                    .into_iter()
                    .map(|value| graph.node(value).unwrap().name())
                    .collect_vec(),
                vec!["B", "C", "E", "F"]
            );
        });
    }

    #[test]
    fn test_dijkstra_undirected() {
        let edges = vec![
            (0, "C", "A", vec![("weight", 4u64)]),
            (1, "A", "B", vec![("weight", 4u64)]),
            (3, "C", "D", vec![("weight", 3u64)]),
        ];

        let graph = Graph::new();

        for (t, src, dst, props) in edges {
            graph.add_edge(t, src, dst, props, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let targets: Vec<&str> = vec!["D"];
            let results = dijkstra_single_source_shortest_paths(
                graph,
                "A",
                targets,
                Some("weight"),
                Direction::BOTH,
            );

            let results = results.unwrap();
            assert_eq!(results.get_by_node("D").unwrap().distance, 7f64);
            assert_eq!(
                results
                    .get_by_node("D")
                    .unwrap()
                    .path
                    .into_iter()
                    .map(|value| graph.node(value).unwrap().name())
                    .collect_vec(),
                vec!["A", "C", "D"]
            );
        });
    }

    #[test]
    fn test_dijkstra_no_weight_undirected() {
        let edges = vec![
            (0, "C", "A", vec![("weight", 4u64)]),
            (1, "A", "B", vec![("weight", 4u64)]),
            (3, "C", "D", vec![("weight", 3u64)]),
        ];

        let graph = Graph::new();

        for (t, src, dst, props) in edges {
            graph.add_edge(t, src, dst, props, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let targets: Vec<&str> = vec!["D"];
            let results =
                dijkstra_single_source_shortest_paths(graph, "A", targets, None, Direction::BOTH)
                    .unwrap();
            assert_eq!(
                results
                    .get_by_node("D")
                    .unwrap()
                    .path
                    .into_iter()
                    .map(|value| graph.node(value).unwrap().name())
                    .collect_vec(),
                vec!["A", "C", "D"]
            );
        });
    }
}

mod sssp_tests {
    use itertools::Itertools;
    use raphtory::{
        algorithms::pathing::single_source_shortest_path::single_source_shortest_path,
        db::{api::mutation::AdditionOps, graph::graph::Graph},
        prelude::*,
    };
    use raphtory_api::core::utils::logging::global_info_logger;
    use raphtory_tests::test_storage;
    use std::collections::HashMap;

    fn load_graph(edges: Vec<(i64, u64, u64)>) -> Graph {
        let graph = Graph::new();
        for (t, src, dst) in edges {
            graph.add_edge(t, src, dst, NO_PROPS, None).unwrap();
        }
        graph
    }

    #[test]
    fn test_sssp_1() {
        global_info_logger();
        let graph = load_graph(vec![
            (0, 1, 2),
            (1, 1, 3),
            (2, 1, 4),
            (3, 2, 3),
            (4, 2, 4),
            (5, 3, 4),
            (6, 4, 4),
            (7, 4, 5),
            (8, 5, 6),
        ]);

        test_storage!(&graph, |graph| {
            let results = single_source_shortest_path(graph, 1, Some(4));
            let expected: HashMap<String, Vec<String>> = HashMap::from([
                ("1".to_string(), vec!["1".to_string()]),
                ("2".to_string(), vec!["1".to_string(), "2".to_string()]),
                ("3".to_string(), vec!["1".to_string(), "3".to_string()]),
                ("4".to_string(), vec!["1".to_string(), "4".to_string()]),
                (
                    "5".to_string(),
                    vec!["1".to_string(), "4".to_string(), "5".to_string()],
                ),
                (
                    "6".to_string(),
                    vec![
                        "1".to_string(),
                        "4".to_string(),
                        "5".to_string(),
                        "6".to_string(),
                    ],
                ),
            ]);
            assert_eq!(expected.len(), results.len());
            for (node, values) in expected {
                assert_eq!(
                    results
                        .get_by_node(node)
                        .unwrap()
                        .path
                        .into_iter()
                        .map(|value| graph.node(value).unwrap().name())
                        .collect_vec(),
                    values
                );
            }
            let _ = single_source_shortest_path(graph, 5, Some(4));
        });
    }
}

mod generic_taint_tests {

    use raphtory::{
        algorithms::pathing::temporal_reachability::temporally_reachable_nodes,
        core::entities::nodes::node_ref::AsNodeRef,
        db::{
            api::{mutation::AdditionOps, view::StaticGraphViewOps},
            graph::graph::Graph,
        },
        prelude::*,
    };
    use raphtory_tests::test_storage;
    use std::collections::HashMap;

    fn sort_inner_by_string(
        data: HashMap<String, Vec<(i64, String)>>,
    ) -> Vec<(String, Vec<(i64, String)>)> {
        let mut vec: Vec<_> = data.into_iter().collect();
        vec.sort_by(|a, b| a.0.cmp(&b.0));
        for (_, inner_vec) in &mut vec {
            inner_vec.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| b.1.cmp(&a.1)));
        }
        vec
    }

    fn load_graph(edges: Vec<(i64, u64, u64)>) -> Graph {
        let graph = Graph::new();

        for (t, src, dst) in edges {
            graph.add_edge(t, src, dst, NO_PROPS, None).unwrap();
        }
        graph
    }

    fn test_generic_taint<T: AsNodeRef, G: StaticGraphViewOps>(
        graph: &G,
        iter_count: usize,
        start_time: i64,
        infected_nodes: Vec<T>,
        stop_nodes: Option<Vec<T>>,
    ) -> HashMap<String, Vec<(i64, String)>> {
        temporally_reachable_nodes(
            graph,
            None,
            iter_count,
            start_time,
            infected_nodes,
            stop_nodes,
        )
        .into_iter()
        .map(|(n, v)| (n.name(), v.reachable_nodes))
        .collect()
    }

    #[test]
    fn test_generic_taint_1() {
        let graph = load_graph(vec![
            (10, 1, 3),
            (11, 1, 2),
            (12, 2, 4),
            (13, 2, 5),
            (14, 5, 5),
            (14, 5, 4),
            (5, 4, 6),
            (15, 4, 7),
            (10, 4, 7),
            (10, 5, 8),
        ]);

        test_storage!(&graph, |graph| {
            let results = sort_inner_by_string(test_generic_taint(graph, 20, 11, vec![2], None));
            let expected: Vec<(String, Vec<(i64, String)>)> = Vec::from([
                ("1".to_string(), vec![]),
                ("2".to_string(), vec![(11i64, "start".to_string())]),
                ("3".to_string(), vec![]),
                (
                    "4".to_string(),
                    vec![(12i64, "2".to_string()), (14i64, "5".to_string())],
                ),
                (
                    "5".to_string(),
                    vec![(13i64, "2".to_string()), (14i64, "5".to_string())],
                ),
                ("6".to_string(), vec![]),
                ("7".to_string(), vec![(15i64, "4".to_string())]),
                ("8".to_string(), vec![]),
            ]);
            assert_eq!(results, expected);
        });
    }

    #[test]
    fn test_generic_taint_1_multiple_start() {
        let graph = load_graph(vec![
            (10, 1, 3),
            (11, 1, 2),
            (12, 2, 4),
            (13, 2, 5),
            (14, 5, 5),
            (14, 5, 4),
            (5, 4, 6),
            (15, 4, 7),
            (10, 4, 7),
            (10, 5, 8),
        ]);

        test_storage!(&graph, |graph| {
            let results = sort_inner_by_string(test_generic_taint(graph, 20, 11, vec![1, 2], None));
            let expected: Vec<(String, Vec<(i64, String)>)> = Vec::from([
                ("1".to_string(), vec![(11i64, "start".to_string())]),
                (
                    "2".to_string(),
                    vec![(11i64, "start".to_string()), (11i64, "1".to_string())],
                ),
                ("3".to_string(), vec![]),
                (
                    "4".to_string(),
                    vec![(12i64, "2".to_string()), (14i64, "5".to_string())],
                ),
                (
                    "5".to_string(),
                    vec![(13i64, "2".to_string()), (14i64, "5".to_string())],
                ),
                ("6".to_string(), vec![]),
                ("7".to_string(), vec![(15i64, "4".to_string())]),
                ("8".to_string(), vec![]),
            ]);
            assert_eq!(results, expected);
        });
    }

    #[test]
    fn test_generic_taint_1_stop_nodes() {
        let graph = load_graph(vec![
            (10, 1, 3),
            (11, 1, 2),
            (12, 2, 4),
            (13, 2, 5),
            (14, 5, 5),
            (14, 5, 4),
            (5, 4, 6),
            (15, 4, 7),
            (10, 4, 7),
            (10, 5, 8),
        ]);

        test_storage!(&graph, |graph| {
            let results = sort_inner_by_string(test_generic_taint(
                graph,
                20,
                11,
                vec![1, 2],
                Some(vec![4, 5]),
            ));
            let expected: Vec<(String, Vec<(i64, String)>)> = Vec::from([
                ("1".to_string(), vec![(11i64, "start".to_string())]),
                (
                    "2".to_string(),
                    vec![(11i64, "start".to_string()), (11i64, "1".to_string())],
                ),
                ("3".to_string(), vec![]),
                ("4".to_string(), vec![(12i64, "2".to_string())]),
                ("5".to_string(), vec![(13i64, "2".to_string())]),
                ("6".to_string(), vec![]),
                ("7".to_string(), vec![]),
                ("8".to_string(), vec![]),
            ]);
            assert_eq!(results, expected);
        });
    }

    #[test]
    fn test_generic_taint_1_multiple_history_points() {
        let graph = load_graph(vec![
            (10, 1, 3),
            (11, 1, 2),
            (12, 1, 2),
            (9, 1, 2),
            (12, 2, 4),
            (13, 2, 5),
            (14, 5, 5),
            (14, 5, 4),
            (5, 4, 6),
            (15, 4, 7),
            (10, 4, 7),
            (10, 5, 8),
        ]);

        test_storage!(&graph, |graph| {
            let results = sort_inner_by_string(test_generic_taint(
                graph,
                20,
                11,
                vec![1, 2],
                Some(vec![4, 5]),
            ));
            let expected: Vec<(String, Vec<(i64, String)>)> = Vec::from([
                ("1".to_string(), vec![(11i64, "start".to_string())]),
                (
                    "2".to_string(),
                    vec![
                        (11i64, "start".to_string()),
                        (11i64, "1".to_string()),
                        (12i64, "1".to_string()),
                    ],
                ),
                ("3".to_string(), vec![]),
                ("4".to_string(), vec![(12i64, "2".to_string())]),
                ("5".to_string(), vec![(13i64, "2".to_string())]),
                ("6".to_string(), vec![]),
                ("7".to_string(), vec![]),
                ("8".to_string(), vec![]),
            ]);
            assert_eq!(results, expected);
        });
    }
}

mod scored_paths_tests {
    use raphtory::{
        algorithms::pathing::scored_paths::{
            top_scoring_paths, EntityScore, PropertyScore, ScoredPath, ScoringMap,
        },
        db::{api::mutation::AdditionOps, graph::graph::Graph},
        prelude::*,
    };
    use raphtory_api::core::Direction;
    use raphtory_tests::test_storage;
    use std::{collections::HashMap, num::NonZeroUsize};

    /// A warm-introduction graph: three routes from `Me` to `John`, one of them through an
    /// ex-partner.
    fn intro_graph() -> Graph {
        let graph = Graph::new();
        for name in ["Me", "Jenny", "James", "John"] {
            graph
                .add_node(0, name, NO_PROPS, Some("person"), None)
                .unwrap();
        }
        graph
            .add_node(0, "Priya", NO_PROPS, Some("recruiter"), None)
            .unwrap();

        graph
            .add_edge(1, "Me", "Jenny", [("closeness", "close")], Some("friend"))
            .unwrap();
        graph
            .add_edge(1, "Jenny", "John", NO_PROPS, Some("ex_partner"))
            .unwrap();
        graph
            .add_edge(1, "Me", "James", [("years", 4i64)], Some("colleague"))
            .unwrap();
        graph
            .add_edge(
                1,
                "James",
                "John",
                [("closeness", "distant")],
                Some("friend"),
            )
            .unwrap();
        graph
            .add_edge(1, "Me", "Priya", NO_PROPS, Some("colleague"))
            .unwrap();
        graph
            .add_edge(1, "Priya", "John", NO_PROPS, Some("colleague"))
            .unwrap();
        graph
    }

    fn layer_weights(weights: [(&str, f64); 3]) -> HashMap<String, EntityScore> {
        weights
            .into_iter()
            .map(|(layer, weight)| {
                (
                    layer.to_string(),
                    EntityScore {
                        weight,
                        properties: vec![],
                    },
                )
            })
            .collect()
    }

    fn relationship_scoring() -> ScoringMap {
        ScoringMap {
            layers: layer_weights([("friend", 5.0), ("colleague", 3.0), ("ex_partner", -10.0)]),
            ..Default::default()
        }
    }

    fn names(graph: &Graph, path: &ScoredPath) -> Vec<String> {
        path.nodes
            .iter()
            .map(|node| graph.node(*node).unwrap().name())
            .collect()
    }

    fn routes(graph: &Graph, paths: &[ScoredPath]) -> Vec<(Vec<String>, f64)> {
        paths
            .iter()
            .map(|path| (names(graph, path), path.score))
            .collect()
    }

    #[test]
    fn routes_around_a_negatively_scored_relationship() {
        let graph = intro_graph();

        test_storage!(&graph, |graph| {
            let paths = top_scoring_paths(
                graph,
                "John",
                Some(vec!["Me"]),
                &relationship_scoring(),
                Some(2),
                None,
                None,
                Direction::OUT,
            )
            .unwrap();

            // The direct-ish route through Jenny exists but scores worst, so it ranks last rather
            // than being excluded.
            assert_eq!(
                routes(graph, &paths),
                vec![
                    (vec!["Me".into(), "James".into(), "John".into()], 8.0),
                    (vec!["Me".into(), "Priya".into(), "John".into()], 6.0),
                    (vec!["Me".into(), "Jenny".into(), "John".into()], -5.0),
                ]
            );
        });
    }

    #[test]
    fn node_type_weights_apply_to_every_node_on_the_path() {
        let graph = intro_graph();

        test_storage!(&graph, |graph| {
            let scoring = ScoringMap {
                node_types: HashMap::from([(
                    "recruiter".to_string(),
                    EntityScore {
                        weight: -5.0,
                        properties: vec![],
                    },
                )]),
                ..relationship_scoring()
            };
            let paths = top_scoring_paths(
                graph,
                "John",
                Some(vec!["Me"]),
                &scoring,
                Some(2),
                None,
                None,
                Direction::OUT,
            )
            .unwrap();

            assert_eq!(
                routes(graph, &paths),
                vec![
                    (vec!["Me".into(), "James".into(), "John".into()], 8.0),
                    (vec!["Me".into(), "Priya".into(), "John".into()], 1.0),
                    (vec!["Me".into(), "Jenny".into(), "John".into()], -5.0),
                ]
            );
        });
    }

    #[test]
    fn property_scores_combine_categories_scale_and_defaults() {
        let graph = intro_graph();

        test_storage!(&graph, |graph| {
            let scoring = ScoringMap {
                layers: HashMap::from([
                    (
                        "friend".to_string(),
                        EntityScore {
                            weight: 5.0,
                            properties: vec![PropertyScore {
                                name: "closeness".to_string(),
                                categories: Some(HashMap::from([
                                    ("close".to_string(), 4.0),
                                    ("distant".to_string(), -1.0),
                                ])),
                                ..Default::default()
                            }],
                        },
                    ),
                    (
                        "colleague".to_string(),
                        EntityScore {
                            weight: 3.0,
                            // `Priya`'s edges have no `years`, so they fall back to the default.
                            properties: vec![PropertyScore {
                                name: "years".to_string(),
                                scale: 0.5,
                                ..Default::default()
                            }],
                        },
                    ),
                    (
                        "ex_partner".to_string(),
                        EntityScore {
                            weight: -10.0,
                            properties: vec![],
                        },
                    ),
                ]),
                ..Default::default()
            };
            let paths = top_scoring_paths(
                graph,
                "John",
                Some(vec!["Me"]),
                &scoring,
                Some(2),
                None,
                None,
                Direction::OUT,
            )
            .unwrap();

            assert_eq!(
                routes(graph, &paths),
                vec![
                    // (3 + 4 * 0.5) + (5 - 1)
                    (vec!["Me".into(), "James".into(), "John".into()], 9.0),
                    // (3 + 0) + (3 + 0)
                    (vec!["Me".into(), "Priya".into(), "John".into()], 6.0),
                    // (5 + 4) + (-10)
                    (vec!["Me".into(), "Jenny".into(), "John".into()], -1.0),
                ]
            );
        });
    }

    #[test]
    fn hop_cutoff_excludes_longer_routes() {
        let graph = intro_graph();

        test_storage!(&graph, |graph| {
            let one_hop = |source: &'static str| {
                top_scoring_paths(
                    graph,
                    "John",
                    Some(vec![source]),
                    &relationship_scoring(),
                    Some(1),
                    None,
                    None,
                    Direction::OUT,
                )
                .unwrap()
            };

            assert_eq!(routes(graph, &one_hop("Me")), vec![]);
            assert_eq!(
                routes(graph, &one_hop("James")),
                vec![(vec!["James".into(), "John".into()], 5.0)]
            );
        });
    }

    #[test]
    fn top_k_returns_only_the_best_paths() {
        let graph = intro_graph();

        test_storage!(&graph, |graph| {
            let paths = top_scoring_paths(
                graph,
                "John",
                Some(vec!["Me"]),
                &relationship_scoring(),
                Some(2),
                Some(1),
                None,
                Direction::OUT,
            )
            .unwrap();

            assert_eq!(
                routes(graph, &paths),
                vec![(vec!["Me".into(), "James".into(), "John".into()], 8.0)]
            );
        });
    }

    #[test]
    fn skipping_unscored_layers_makes_them_untraversable() {
        let graph = intro_graph();

        test_storage!(&graph, |graph| {
            let scoring = ScoringMap {
                layers: HashMap::from([(
                    "friend".to_string(),
                    EntityScore {
                        weight: 5.0,
                        properties: vec![],
                    },
                )]),
                skip_unscored_layers: true,
                ..Default::default()
            };
            let paths = top_scoring_paths(
                graph,
                "John",
                Some(vec!["Me"]),
                &scoring,
                Some(2),
                None,
                None,
                Direction::OUT,
            )
            .unwrap();

            // Every route to John needs a `colleague` or `ex_partner` hop.
            assert_eq!(routes(graph, &paths), vec![]);
        });
    }

    #[test]
    fn no_source_set_starts_from_every_node() {
        let graph = intro_graph();

        test_storage!(&graph, |graph| {
            let paths = top_scoring_paths(
                graph,
                "John",
                None,
                &relationship_scoring(),
                Some(2),
                Some(3),
                None,
                Direction::OUT,
            )
            .unwrap();

            assert_eq!(
                routes(graph, &paths),
                vec![
                    (vec!["Me".into(), "James".into(), "John".into()], 8.0),
                    (vec!["Me".into(), "Priya".into(), "John".into()], 6.0),
                    (vec!["James".into(), "John".into()], 5.0),
                ]
            );
        });
    }

    #[test]
    fn beam_width_keeps_the_best_partial_path_per_node() {
        let graph = intro_graph();

        test_storage!(&graph, |graph| {
            let paths = top_scoring_paths(
                graph,
                "John",
                Some(vec!["Me"]),
                &relationship_scoring(),
                Some(2),
                None,
                NonZeroUsize::new(1),
                Direction::OUT,
            )
            .unwrap();

            // A beam of 1 keeps a single partial path per node, so only the best route to `Me`
            // survives the second hop.
            assert_eq!(
                routes(graph, &paths),
                vec![(vec!["Me".into(), "James".into(), "John".into()], 8.0)]
            );
        });
    }

    #[test]
    fn positive_cycles_do_not_produce_repeated_nodes() {
        let graph = Graph::new();
        // A positive cycle: maximising over walks rather than simple paths would loop here forever.
        for (src, dst) in [("A", "B"), ("B", "C"), ("C", "A"), ("C", "D")] {
            graph
                .add_edge(1, src, dst, NO_PROPS, Some("friend"))
                .unwrap();
        }

        test_storage!(&graph, |graph| {
            let scoring = ScoringMap {
                layers: HashMap::from([(
                    "friend".to_string(),
                    EntityScore {
                        weight: 5.0,
                        properties: vec![],
                    },
                )]),
                ..Default::default()
            };
            let paths = top_scoring_paths(
                graph,
                "D",
                None,
                &scoring,
                Some(10),
                None,
                None,
                Direction::BOTH,
            )
            .unwrap();

            assert!(!paths.is_empty());
            for path in &paths {
                let route = names(graph, path);
                let unique: std::collections::HashSet<_> = route.iter().collect();
                assert_eq!(unique.len(), route.len(), "repeated node in {route:?}");
            }
            // The longest simple path into D covers all four nodes.
            assert_eq!(paths.iter().map(|path| path.nodes.len()).max(), Some(4));
        });
    }

    #[test]
    fn missing_destination_is_an_error() {
        let graph = intro_graph();

        test_storage!(&graph, |graph| {
            assert!(top_scoring_paths(
                graph,
                "Nobody",
                Some(vec!["Me"]),
                &relationship_scoring(),
                None,
                None,
                None,
                Direction::OUT,
            )
            .is_err());
        });
    }
}
