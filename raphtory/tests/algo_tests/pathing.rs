#[cfg(test)]
mod dijkstra_tests {
    use itertools::Itertools;
    use raphtory::{
        algorithms::pathing::dijkstra::dijkstra_single_source_shortest_paths,
        db::{api::mutation::AdditionOps, graph::graph::Graph},
        prelude::*,
        test_storage,
    };
    use raphtory_api::core::Direction;

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

#[cfg(test)]
mod bellman_ford_tests {
    use raphtory::{
        algorithms::pathing::bellman_ford::bellman_ford_single_source_shortest_paths,
        db::{api::mutation::AdditionOps, graph::graph::Graph},
        prelude::*,
        test_storage,
    };
    use raphtory_api::core::Direction;

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
            (4, "C", "E", vec![("weight", -2.0f32)]),
            (5, "C", "F", vec![("weight", 6.0f32)]),
            (6, "D", "F", vec![("weight", 2.0f32)]),
            (7, "E", "F", vec![("weight", 3.0f32)]),
        ])
    }

    #[test]
    fn test_bellman_ford_multiple_targets() {
        let graph = basic_graph();

        test_storage!(&graph, |graph| {
            let targets: Vec<&str> = vec!["D", "F"];
            let results = bellman_ford_single_source_shortest_paths(
                graph,
                "A",
                targets,
                Some("weight"),
                Direction::OUT,
            );

            let results = results.unwrap();

            assert_eq!(results.get_by_node("D").unwrap().0, 7.0f64);
            assert_eq!(
                results.get_by_node("D").unwrap().1.name(),
                vec!["A", "C", "D"]
            );

            assert_eq!(results.get_by_node("F").unwrap().0, 5.0f64);
            assert_eq!(
                results.get_by_node("F").unwrap().1.name(),
                vec!["A", "C", "E", "F"]
            );

            let targets: Vec<&str> = vec!["D", "E", "F"];
            let results = bellman_ford_single_source_shortest_paths(
                graph,
                "B",
                targets,
                Some("weight"),
                Direction::OUT,
            );
            let results = results.unwrap();
            assert_eq!(results.get_by_node("D").unwrap().0, 5.0f64);
            assert_eq!(results.get_by_node("E").unwrap().0, 0.0f64);
            assert_eq!(results.get_by_node("F").unwrap().0, 3.0f64);
            assert_eq!(
                results.get_by_node("D").unwrap().1.name(),
                vec!["B", "C", "D"]
            );
            assert_eq!(
                results.get_by_node("E").unwrap().1.name(),
                vec!["B", "C", "E"]
            );
            assert_eq!(
                results.get_by_node("F").unwrap().1.name(),
                vec!["B", "C", "E", "F"]
            );
        });
    }

    #[test]
    fn test_bellman_ford_no_weight() {
        let graph = basic_graph();

        test_storage!(&graph, |graph| {
            let targets: Vec<&str> = vec!["C", "E", "F"];
            let results =
                bellman_ford_single_source_shortest_paths(graph, "A", targets, None, Direction::OUT)
                    .unwrap();
            assert_eq!(results.get_by_node("C").unwrap().1.name(), vec!["A", "C"]);
            assert_eq!(
                results.get_by_node("E").unwrap().1.name(),
                vec!["A", "C", "E"]
            );
            assert_eq!(
                results.get_by_node("F").unwrap().1.name(),
                vec!["A", "C", "F"]
            );
        });
    }

    #[test]
    fn test_bellman_ford_multiple_targets_node_ids() {
        let edges = vec![
            (0, 1, 2, vec![("weight", 4i64)]),
            (1, 1, 3, vec![("weight", 4i64)]),
            (2, 2, 3, vec![("weight", 2i64)]),
            (3, 3, 4, vec![("weight", 3i64)]),
            (4, 3, 5, vec![("weight", -2i64)]),
            (5, 3, 6, vec![("weight", 6i64)]),
            (6, 4, 6, vec![("weight", 2i64)]),
            (7, 5, 6, vec![("weight", 3i64)]),
        ];

        let graph = Graph::new();
        for (t, src, dst, props) in edges {
            graph.add_edge(t, src, dst, props, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let targets = vec![4, 6];
            let results = bellman_ford_single_source_shortest_paths(
                graph,
                1,
                targets,
                Some("weight"),
                Direction::OUT,
            );
            let results = results.unwrap();
            assert_eq!(results.get_by_node("4").unwrap().0, 7f64);
            assert_eq!(
                results.get_by_node("4").unwrap().1.name(),
                vec!["1", "3", "4"]
            );

            assert_eq!(results.get_by_node("6").unwrap().0, 5f64);
            assert_eq!(
                results.get_by_node("6").unwrap().1.name(),
                vec!["1", "3", "5", "6"]
            );

            let targets = vec![4, 5, 6];
            let results = bellman_ford_single_source_shortest_paths(
                graph,
                2,
                targets,
                Some("weight"),
                Direction::OUT,
            );
            let results = results.unwrap();
            assert_eq!(results.get_by_node("4").unwrap().0, 5f64);
            assert_eq!(results.get_by_node("5").unwrap().0, 0f64);
            assert_eq!(results.get_by_node("6").unwrap().0, 3f64);
            assert_eq!(
                results.get_by_node("4").unwrap().1.name(),
                vec!["2", "3", "4"]
            );
            assert_eq!(
                results.get_by_node("5").unwrap().1.name(),
                vec!["2", "3", "5"]
            );
            assert_eq!(
                results.get_by_node("6").unwrap().1.name(),
                vec!["2", "3", "5", "6"]
            );
        });
    }

    #[test]
    fn test_bellman_ford_multiple_targets_i64() {
        let edges = vec![
            (0, "A", "B", vec![("weight", 4i64)]),
            (1, "A", "C", vec![("weight", 4i64)]),
            (2, "B", "C", vec![("weight", 2i64)]),
            (3, "C", "D", vec![("weight", 3i64)]),
            (4, "C", "E", vec![("weight", -2i64)]),
            (5, "C", "F", vec![("weight", 6i64)]),
            (6, "D", "F", vec![("weight", 2i64)]),
            (7, "E", "F", vec![("weight", 3i64)]),
        ];

        let graph = Graph::new();

        for (t, src, dst, props) in edges {
            graph.add_edge(t, src, dst, props, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let targets: Vec<&str> = vec!["D", "F"];
            let results = bellman_ford_single_source_shortest_paths(
                graph,
                "A",
                targets,
                Some("weight"),
                Direction::OUT,
            );
            let results = results.unwrap();
            assert_eq!(results.get_by_node("D").unwrap().0, 7f64);
            assert_eq!(
                results.get_by_node("D").unwrap().1.name(),
                vec!["A", "C", "D"]
            );

            assert_eq!(results.get_by_node("F").unwrap().0, 5f64);
            assert_eq!(
                results.get_by_node("F").unwrap().1.name(),
                vec!["A", "C", "E", "F"]
            );

            let targets: Vec<&str> = vec!["D", "E", "F"];
            let results = bellman_ford_single_source_shortest_paths(
                graph,
                "B",
                targets,
                Some("weight"),
                Direction::OUT,
            );
            let results = results.unwrap();
            assert_eq!(results.get_by_node("D").unwrap().0, 5f64);
            assert_eq!(results.get_by_node("E").unwrap().0, 0f64);
            assert_eq!(results.get_by_node("F").unwrap().0, 3f64);
            assert_eq!(
                results.get_by_node("D").unwrap().1.name(),
                vec!["B", "C", "D"]
            );
            assert_eq!(
                results.get_by_node("E").unwrap().1.name(),
                vec!["B", "C", "E"]
            );
            assert_eq!(
                results.get_by_node("F").unwrap().1.name(),
                vec!["B", "C", "E", "F"]
            );
        });
    }

    #[test]
    fn test_bellman_ford_undirected() {
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
            let results = bellman_ford_single_source_shortest_paths(
                graph,
                "A",
                targets,
                Some("weight"),
                Direction::BOTH,
            );

            let results = results.unwrap();
            assert_eq!(results.get_by_node("D").unwrap().0, 7f64);
            assert_eq!(
                results.get_by_node("D").unwrap().1.name(),
                vec!["A", "C", "D"]
            );
        });
    }

    #[test]
    fn test_bellman_ford_no_weight_undirected() {
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
                bellman_ford_single_source_shortest_paths(graph, "A", targets, None, Direction::BOTH)
                    .unwrap();
            assert_eq!(
                results.get_by_node("D").unwrap().1.name(),
                vec!["A", "C", "D"]
            );
        });
    }

    #[test]
    fn test_bellman_ford_negative_cycle() {
        let edges = vec![
            (0, "A", "B", vec![("weight", 1i64)]),
            (1, "B", "C", vec![("weight", -5i64)]),
            (2, "C", "A", vec![("weight", 2i64)]),
        ];

        let graph = Graph::new();
        for (t, src, dst, props) in edges {
            graph.add_edge(t, src, dst, props, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let targets: Vec<&str> = vec!["C"];
            let result = bellman_ford_single_source_shortest_paths(
                graph,
                "A",
                targets,
                Some("weight"),
                Direction::OUT,
            );
            assert!(result.is_err());
        });
    }
}

#[cfg(test)]
mod sssp_tests {
    use itertools::Itertools;
    use raphtory::{
        algorithms::pathing::single_source_shortest_path::single_source_shortest_path,
        db::{api::mutation::AdditionOps, graph::graph::Graph},
        prelude::*,
        test_storage,
    };
    use raphtory_api::core::utils::logging::global_info_logger;
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

#[cfg(test)]
mod generic_taint_tests {

    use raphtory::{
        algorithms::pathing::temporal_reachability::temporally_reachable_nodes,
        db::{
            api::{mutation::AdditionOps, view::StaticGraphViewOps},
            graph::graph::Graph,
        },
        prelude::*,
        test_storage,
    };
    use raphtory_core::entities::nodes::node_ref::AsNodeRef;
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
