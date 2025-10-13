#[cfg(test)]
mod cc_test {
    use pretty_assertions::assert_eq;
    use raphtory::{
        algorithms::metrics::clustering_coefficient::global_clustering_coefficient::global_clustering_coefficient,
        db::{api::mutation::AdditionOps, graph::graph::Graph},
        prelude::NO_PROPS,
    };

    use crate::test_storage;

    /// Test the global clustering coefficient
    #[test]
    fn test_global_cc() {
        let graph = Graph::new();

        // Graph has 2 triangles and 20 triplets
        let edges = vec![
            (1, 2),
            (1, 3),
            (1, 4),
            (2, 1),
            (2, 6),
            (2, 7),
            (3, 1),
            (3, 4),
            (3, 7),
            (4, 1),
            (4, 3),
            (4, 5),
            (4, 6),
            (5, 4),
            (5, 6),
            (6, 4),
            (6, 5),
            (6, 2),
            (7, 2),
            (7, 3),
        ];

        for (src, dst) in edges {
            graph.add_edge(0, src, dst, NO_PROPS, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let results = global_clustering_coefficient(graph);
            assert_eq!(results, 0.3);
        });
    }
}

#[cfg(test)]
mod clustering_coefficient_tests {
    use std::collections::HashMap;

    use raphtory::{
        algorithms::metrics::clustering_coefficient::{
            local_clustering_coefficient::local_clustering_coefficient,
            local_clustering_coefficient_batch::local_clustering_coefficient_batch,
        },
        db::{
            api::{mutation::AdditionOps, view::*},
            graph::graph::Graph,
        },
        prelude::NO_PROPS,
    };

    use crate::test_storage;

    #[test]
    fn clusters_of_triangles() {
        let graph = Graph::new();
        let vs = vec![
            (1, 1, 2),
            (2, 1, 3),
            (3, 2, 1),
            (4, 3, 2),
            (5, 1, 4),
            (6, 4, 5),
            (6, 1, 1),
        ];

        for (t, src, dst) in &vs {
            graph.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let expected = vec![0.3333333333333333, 1.0, 1.0, 0.0, 0.0];
            let windowed_graph = graph.window(0, 7);
            let actual = (1..=5)
                .map(|v| local_clustering_coefficient(&windowed_graph, v).unwrap())
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);
        });
    }

    #[test]
    fn clusters_of_triangles_batch() {
        /*
        let graph = Graph::new();
        let vs = vec![
            (1, 1, 2),
            (2, 1, 3),
            (3, 2, 1),
            (4, 3, 2),
            (5, 1, 4),
            (6, 4, 5),
            (6, 1, 1),
            (6, 5, 5),
        ];

        for (t, src, dst) in &vs {
            graph.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let expected = [0.3333333333333333, 1.0, 1.0, 0.0, 0.0];
            let expected: HashMap<String, f64> =
                (1..=5).map(|v| (v.to_string(), expected[v - 1])).collect();
            let windowed_graph = graph.window(0, 7);
            let actual = local_clustering_coefficient_batch(&windowed_graph, (1..=5).collect());
            let actual: HashMap<String, f64> = (1..=5)
                .map(|v| (v.to_string(), actual.values()[v - 1]))
                .collect();
            assert_eq!(expected, actual);
        });
        */
    }
}

#[cfg(test)]
mod sum_weight_test {
    use pretty_assertions::assert_eq;
    use raphtory::{
        algorithms::metrics::balance::balance,
        db::{api::mutation::AdditionOps, graph::graph::Graph},
        prelude::GraphViewOps,
    };
    use raphtory_api::core::{entities::properties::prop::Prop, Direction};
    use std::collections::HashMap;

    use crate::test_storage;

    #[test]
    fn test_sum_float_weights() {
        let graph = Graph::new();

        let vs = vec![
            ("1", "2", 10, 1),
            ("1", "4", 20, 2),
            ("2", "3", 5, 3),
            ("3", "2", 2, 4),
            ("3", "1", 1, 5),
            ("4", "3", 10, 6),
            ("4", "1", 5, 7),
            ("1", "5", 2, 8),
        ];

        for (src, dst, val, time) in &vs {
            graph
                .add_edge(
                    *time,
                    *src,
                    *dst,
                    [("value_dec".to_string(), Prop::I32(*val))],
                    None,
                )
                .expect("Couldnt add edge");
        }

        test_storage!(&graph, |graph| {
            let res = balance(graph, "value_dec".to_string(), Direction::BOTH).unwrap();
            let node_one = graph.node("1").unwrap();
            let node_two = graph.node("2").unwrap();
            let node_three = graph.node("3").unwrap();
            let node_four = graph.node("4").unwrap();
            let node_five = graph.node("5").unwrap();
            let expected = HashMap::from([
                (node_one.clone(), -26.0),
                (node_two.clone(), 7.0),
                (node_three.clone(), 12.0),
                (node_four.clone(), 5.0),
                (node_five.clone(), 2.0),
            ]);
            assert_eq!(res, expected);

            let res = balance(graph, "value_dec".to_string(), Direction::IN).unwrap();
            let expected = HashMap::from([
                (node_one.clone(), 6.0),
                (node_two.clone(), 12.0),
                (node_three.clone(), 15.0),
                (node_four.clone(), 20.0),
                (node_five.clone(), 2.0),
            ]);
            assert_eq!(res, expected);

            let res = balance(graph, "value_dec".to_string(), Direction::OUT).unwrap();
            let expected = HashMap::from([
                (node_one, -32.0),
                (node_two, -5.0),
                (node_three, -3.0),
                (node_four, -15.0),
                (node_five, 0.0),
            ]);
            assert_eq!(res, expected);
        });
    }
}

#[cfg(test)]
mod degree_test {
    use raphtory::{
        algorithms::metrics::degree::{
            average_degree, max_degree, max_in_degree, max_out_degree, min_degree, min_in_degree,
            min_out_degree,
        },
        db::{api::mutation::AdditionOps, graph::graph::Graph},
        prelude::NO_PROPS,
    };

    use crate::test_storage;

    #[test]
    fn degree_test() {
        let graph = Graph::new();
        let vs = vec![
            (1, 1, 2),
            (2, 1, 3),
            (3, 2, 1),
            (4, 3, 2),
            (5, 1, 4),
            (6, 4, 5),
        ];

        for (t, src, dst) in &vs {
            graph.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let expected_max_out_degree = 3;
            let actual_max_out_degree = max_out_degree(graph);

            let expected_max_in_degree = 2;
            let actual_max_in_degree = max_in_degree(graph);

            let expected_min_out_degree = 0;
            let actual_min_out_degree = min_out_degree(graph);

            let expected_min_in_degree = 1;
            let actual_min_in_degree = min_in_degree(graph);

            let expected_average_degree = 2.0;
            let actual_average_degree = average_degree(graph);

            let expected_max_degree = 3;
            let actual_max_degree = max_degree(graph);

            let expected_min_degree = 1;
            let actual_min_degree = min_degree(graph);

            assert_eq!(expected_max_out_degree, actual_max_out_degree);
            assert_eq!(expected_max_in_degree, actual_max_in_degree);
            assert_eq!(expected_min_out_degree, actual_min_out_degree);
            assert_eq!(expected_min_in_degree, actual_min_in_degree);
            assert_eq!(expected_average_degree, actual_average_degree);
            assert_eq!(expected_max_degree, actual_max_degree);
            assert_eq!(expected_min_degree, actual_min_degree);
        });
    }
}

#[cfg(test)]
mod directed_graph_density_tests {
    use raphtory::{
        algorithms::metrics::directed_graph_density::directed_graph_density,
        db::{api::mutation::AdditionOps, graph::graph::Graph},
        prelude::*,
    };

    use crate::test_storage;

    #[test]
    fn low_graph_density() {
        let graph = Graph::new();

        let vs = vec![
            (1, 1, 2),
            (2, 1, 3),
            (3, 2, 1),
            (4, 3, 2),
            (5, 1, 4),
            (6, 4, 5),
        ];

        for (t, src, dst) in &vs {
            graph.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let windowed_graph = graph.window(0, 7);
            let actual = directed_graph_density(&windowed_graph);
            let expected = 0.3;

            assert_eq!(actual, expected);
        });
    }

    #[test]
    fn complete_graph_has_graph_density_of_one() {
        let graph = Graph::new();

        let vs = vec![(1, 1, 2), (2, 2, 1)];

        for (t, src, dst) in &vs {
            graph.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let windowed_graph = graph.window(0, 3);
            let actual = directed_graph_density(&windowed_graph);
            let expected = 1.0;

            assert_eq!(actual, expected);
        });
    }
}

#[cfg(test)]
mod reciprocity_test {
    use pretty_assertions::assert_eq;
    use raphtory::{
        algorithms::metrics::reciprocity::{all_local_reciprocity, global_reciprocity},
        prelude::*,
    };
    use std::collections::HashMap;

    use crate::test_storage;

    #[test]
    fn test_global_recip() {
        let graph = Graph::new();

        let vs = vec![
            (1, 2),
            (1, 4),
            (2, 3),
            (3, 2),
            (3, 1),
            (4, 3),
            (4, 1),
            (1, 5),
        ];

        for (src, dst) in &vs {
            graph.add_edge(0, *src, *dst, NO_PROPS, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let actual = global_reciprocity(graph);
            assert_eq!(actual, 0.5);

            let mut hash_map_result: HashMap<String, f64> = HashMap::new();
            hash_map_result.insert("1".to_string(), 0.4);
            hash_map_result.insert("2".to_string(), 2.0 / 3.0);
            hash_map_result.insert("3".to_string(), 0.5);
            hash_map_result.insert("4".to_string(), 2.0 / 3.0);
            hash_map_result.insert("5".to_string(), 0.0);

            let res = all_local_reciprocity(graph);
            assert_eq!(res, hash_map_result);
        });
    }
}
