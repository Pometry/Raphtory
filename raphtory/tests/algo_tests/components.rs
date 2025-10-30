use ahash::HashSet;
use proptest::{prelude::Strategy, proptest, sample::Index};
use raphtory::{
    algorithms::components::weakly_connected_components,
    db::api::{mutation::AdditionOps, state::NodeState, view::internal::GraphView},
    prelude::*,
    test_storage,
};
use std::collections::BTreeSet;

fn assert_same_partition<G: GraphView, ID: Into<GID>>(
    left: NodeState<usize, G>,
    right: impl IntoIterator<Item = impl IntoIterator<Item = ID>>,
) {
    let left_groups: HashSet<BTreeSet<_>> = left
        .groups()
        .into_iter_groups()
        .map(|(_, nodes)| nodes.id().collect())
        .collect();
    let right_groups: HashSet<BTreeSet<_>> = right
        .into_iter()
        .map(|inner| inner.into_iter().map(|id| id.into()).collect())
        .collect();
    assert_eq!(left_groups, right_groups);
}

#[test]
fn run_loop_simple_connected_components() {
    let graph = Graph::new();

    let edges = vec![
        (1, 2, 1),
        (2, 3, 2),
        (3, 4, 3),
        (3, 5, 4),
        (6, 5, 5),
        (7, 8, 6),
        (8, 7, 7),
    ];

    for (src, dst, ts) in edges {
        graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
    }

    test_storage!(&graph, |graph| {
        for _ in 0..1000 {
            let results = weakly_connected_components(graph);
            assert_same_partition(results, [1..=6, 7..=8]);
        }
    });
}

#[test]
fn simple_connected_components_2() {
    let graph = Graph::new();

    let edges = vec![
        (1, 2, 1),
        (1, 3, 2),
        (1, 4, 3),
        (3, 1, 4),
        (3, 4, 5),
        (3, 5, 6),
        (4, 5, 7),
        (5, 6, 8),
        (5, 8, 9),
        (7, 5, 10),
        (8, 5, 11),
        (1, 9, 12),
        (9, 1, 13),
        (6, 3, 14),
        (4, 8, 15),
        (8, 3, 16),
        (5, 10, 17),
        (10, 5, 18),
        (10, 8, 19),
        (1, 11, 20),
        (11, 1, 21),
        (9, 11, 22),
        (11, 9, 23),
    ];

    for (src, dst, ts) in edges {
        graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
    }

    test_storage!(&graph, |graph| {
        let results = weakly_connected_components(graph);
        assert_same_partition(results, [1..=11]);
    });
}

#[test]
fn test_multiple_components() {
    let graph = Graph::new();
    let edges = vec![
        (1, 1, 2),
        (2, 2, 1),
        (3, 3, 1),
        (1, 10, 11),
        (2, 20, 21),
        (3, 30, 31),
    ];
    for (ts, src, dst) in edges {
        graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
    }
    for _ in 0..1000 {
        let result = weakly_connected_components(&graph);
        assert_same_partition(
            result,
            [vec![1, 2, 3], vec![10, 11], vec![20, 21], vec![30, 31]],
        )
    }
}

// connected community_detection on a graph with 1 node and a self loop
#[test]
fn simple_connected_components_3() {
    let graph = Graph::new();

    let edges = vec![(1, 1, 1)];

    for (src, dst, ts) in edges {
        graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
    }

    test_storage!(&graph, |graph| {
        for _ in 0..1000 {
            // loop to test for weird non-deterministic behaviour
            let results = weakly_connected_components(graph);
            assert_same_partition(results, [[1]]);
        }
    });
}

#[test]
fn windowed_connected_components() {
    let graph = Graph::new();
    graph.add_edge(0, 1, 2, NO_PROPS, None).expect("add edge");
    graph.add_edge(0, 2, 1, NO_PROPS, None).expect("add edge");
    graph.add_edge(9, 3, 4, NO_PROPS, None).expect("add edge");
    graph.add_edge(9, 4, 3, NO_PROPS, None).expect("add edge");

    test_storage!(&graph, |graph| {
        let results = weakly_connected_components(graph);
        assert_same_partition(results, [[1, 2], [3, 4]]);

        let wg = graph.window(0, 2);
        let results = weakly_connected_components(&wg);
        assert_same_partition(results, [[1, 2]]);
    });
}

fn random_component_edges(
    num_components: usize,
    num_nodes_per_component: usize,
) -> impl Strategy<Value = (Vec<(u64, u64)>, Vec<HashSet<u64>>)> {
    let vs = proptest::collection::vec(
        proptest::collection::vec(
            (
                0..num_nodes_per_component,
                proptest::arbitrary::any::<Index>(),
            ),
            2..=num_nodes_per_component,
        ),
        0..=num_components,
    );
    vs.prop_map(move |vs| {
        let mut edges = Vec::new();
        let mut components = Vec::new();
        for (ci, c) in vs.into_iter().enumerate() {
            let offset = num_nodes_per_component * ci;
            let component: Vec<_> = c.iter().map(|(i, _)| (*i + offset) as u64).collect();
            for i in 1..c.len() {
                let n = component[c[i].1.index(i)];
                edges.push((component[i], n));
            }
            components.push(component.into_iter().collect());
        }
        (edges, components)
    })
}

#[test]
fn weakly_connected_components_proptest() {
    proptest!(|(input in random_component_edges(10, 100))|{
        let (edges, components) = input;
        let g = Graph::new();
        for (src, dst) in edges {
            g.add_edge(0, src, dst, NO_PROPS, None).unwrap();
        }
        for _ in 0..10 {
            let result = weakly_connected_components(&g);
            assert_same_partition(result, &components);
        }
    })
}

mod in_component_test {
    use itertools::Itertools;
    use raphtory::{
        algorithms::components::{in_component, in_components},
        db::api::mutation::AdditionOps,
        prelude::*,
        test_storage,
    };
    use std::collections::HashMap;

    fn check_node(graph: &Graph, node_id: u64, mut correct: Vec<(u64, usize)>) {
        let mut results: Vec<_> = in_component(graph.node(node_id).unwrap())
            .iter()
            .map(|(n, d)| (n.id().as_u64().unwrap(), *d))
            .collect();
        results.sort();
        correct.sort();
        assert_eq!(results, correct);
    }

    #[test]
    fn in_component_test() {
        let graph = Graph::new();
        let edges = vec![
            (1, 1, 2),
            (1, 1, 3),
            (1, 2, 4),
            (1, 2, 5),
            (1, 5, 4),
            (1, 4, 6),
            (1, 4, 7),
            (1, 5, 8),
        ];

        for (ts, src, dst) in edges {
            graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
        }

        check_node(&graph, 1, vec![]);
        check_node(&graph, 2, vec![(1, 1)]);
        check_node(&graph, 3, vec![(1, 1)]);
        check_node(&graph, 4, vec![(1, 2), (2, 1), (5, 1)]);
        check_node(&graph, 5, vec![(1, 2), (2, 1)]);
        check_node(&graph, 6, vec![(1, 3), (2, 2), (4, 1), (5, 2)]);
        check_node(&graph, 7, vec![(1, 3), (2, 2), (4, 1), (5, 2)]);
        check_node(&graph, 8, vec![(1, 3), (2, 2), (5, 1)]);
    }

    #[test]
    fn test_distances() {
        let graph = Graph::new();
        graph.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        graph.add_edge(0, 2, 3, NO_PROPS, None).unwrap();
        graph.add_edge(0, 1, 4, NO_PROPS, None).unwrap();
        graph.add_edge(0, 4, 5, NO_PROPS, None).unwrap();
        graph.add_edge(0, 5, 3, NO_PROPS, None).unwrap();

        check_node(&graph, 3, vec![(1, 2), (2, 1), (4, 2), (5, 1)]);
    }

    #[test]
    fn in_components_test() {
        let graph = Graph::new();
        let edges = vec![
            (1, 1, 2),
            (1, 1, 3),
            (1, 2, 4),
            (1, 2, 5),
            (1, 5, 4),
            (1, 4, 6),
            (1, 4, 7),
            (1, 5, 8),
        ];

        for (ts, src, dst) in edges {
            graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let results = in_components(graph, None);
            let mut correct = HashMap::new();
            correct.insert("1".to_string(), vec![]);
            correct.insert("2".to_string(), vec![1]);
            correct.insert("3".to_string(), vec![1]);
            correct.insert("4".to_string(), vec![1, 2, 5]);
            correct.insert("5".to_string(), vec![1, 2]);
            correct.insert("6".to_string(), vec![1, 2, 4, 5]);
            correct.insert("7".to_string(), vec![1, 2, 4, 5]);
            correct.insert("8".to_string(), vec![1, 2, 5]);
            let map: HashMap<String, Vec<u64>> = results
                .into_iter()
                .map(|(k, v)| {
                    (
                        k.name(),
                        v.id()
                            .into_iter_values()
                            .filter_map(|v| v.as_u64())
                            .sorted()
                            .collect(),
                    )
                })
                .collect();
            assert_eq!(map, correct);
        });
    }
}
#[cfg(test)]
mod components_test {
    use itertools::Itertools;
    use raphtory::{
        algorithms::components::{out_component, out_components},
        db::api::mutation::AdditionOps,
        prelude::*,
        test_storage,
    };
    use std::collections::HashMap;

    fn check_node(graph: &Graph, node_id: u64, mut correct: Vec<(u64, usize)>) {
        let mut results: Vec<_> = out_component(graph.node(node_id).unwrap())
            .iter()
            .map(|(n, d)| (n.id().as_u64().unwrap(), *d))
            .collect();
        results.sort();
        correct.sort();
        assert_eq!(results, correct);
    }

    #[test]
    fn out_component_test() {
        let graph = Graph::new();
        let edges = vec![
            (1, 1, 2),
            (1, 1, 3),
            (1, 2, 3),
            (1, 2, 4),
            (1, 2, 5),
            (1, 5, 4),
            (1, 4, 6),
            (1, 4, 7),
            (1, 5, 8),
        ];

        for (ts, src, dst) in edges {
            graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
        }

        check_node(
            &graph,
            1,
            vec![(2, 1), (3, 1), (4, 2), (5, 2), (6, 3), (7, 3), (8, 3)],
        );
        check_node(
            &graph,
            2,
            vec![(3, 1), (4, 1), (5, 1), (6, 2), (7, 2), (8, 2)],
        );
        check_node(&graph, 3, vec![]);
        check_node(&graph, 4, vec![(6, 1), (7, 1)]);
        check_node(&graph, 5, vec![(4, 1), (6, 2), (7, 2), (8, 1)]);
        check_node(&graph, 6, vec![]);
        check_node(&graph, 7, vec![]);
        check_node(&graph, 8, vec![]);
    }

    #[test]
    fn test_distances() {
        let graph = Graph::new();
        graph.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        graph.add_edge(0, 2, 3, NO_PROPS, None).unwrap();
        graph.add_edge(0, 1, 4, NO_PROPS, None).unwrap();
        graph.add_edge(0, 4, 5, NO_PROPS, None).unwrap();
        graph.add_edge(0, 5, 3, NO_PROPS, None).unwrap();

        check_node(&graph, 1, vec![(2, 1), (3, 2), (4, 1), (5, 2)]);
    }

    #[test]
    fn out_components_test() {
        let graph = Graph::new();
        let edges = vec![
            (1, 1, 2),
            (1, 1, 3),
            (1, 2, 4),
            (1, 2, 5),
            (1, 5, 4),
            (1, 4, 6),
            (1, 4, 7),
            (1, 5, 8),
        ];

        for (ts, src, dst) in edges {
            graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let results = out_components(graph, None);
            let mut correct = HashMap::new();
            correct.insert("1".to_string(), vec![2, 3, 4, 5, 6, 7, 8]);
            correct.insert("2".to_string(), vec![4, 5, 6, 7, 8]);
            correct.insert("3".to_string(), vec![]);
            correct.insert("4".to_string(), vec![6, 7]);
            correct.insert("5".to_string(), vec![4, 6, 7, 8]);
            correct.insert("6".to_string(), vec![]);
            correct.insert("7".to_string(), vec![]);
            correct.insert("8".to_string(), vec![]);
            let map: HashMap<String, Vec<u64>> = results
                .into_iter()
                .map(|(k, v)| {
                    (
                        k.name(),
                        v.id()
                            .into_iter_values()
                            .filter_map(|v| v.as_u64())
                            .sorted()
                            .collect(),
                    )
                })
                .collect();
            assert_eq!(map, correct);
        });
    }
}

#[cfg(test)]
mod strongly_connected_components_tests {
    use itertools::Itertools;
    use raphtory::{
        algorithms::components::strongly_connected_components,
        prelude::{AdditionOps, Graph, NodeStateGroupBy, NodeStateOps, NodeViewOps, NO_PROPS},
        test_storage,
    };
    use std::collections::HashSet;

    #[test]
    fn scc_test() {
        /*
        let graph = Graph::new();
        let edges = vec![
            (1, 1, 2),
            (1, 2, 3),
            (1, 2, 5),
            (1, 3, 4),
            (1, 5, 6),
            (1, 6, 4),
            (1, 6, 7),
            (1, 7, 8),
            (1, 8, 6),
            (1, 6, 2),
        ];

        for (ts, src, dst) in edges {
            graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let scc_nodes: HashSet<Vec<String>> = strongly_connected_components(graph)
                .groups()
                .into_iter_groups()
                .map(|(_, v)| v.name().into_iter_values().sorted().collect())
                .collect();

            let expected: HashSet<Vec<String>> = [
                vec!["2", "5", "6", "7", "8"],
                vec!["1"],
                vec!["3"],
                vec!["4"],
            ]
            .into_iter()
            .map(|v| v.into_iter().map(|s| s.to_owned()).collect())
            .collect();
            assert_eq!(scc_nodes, expected);
        });
         */
    }

    #[test]
    fn scc_test_multiple_components() {
        /*
        let graph = Graph::new();
        let edges = [
            (1, 2),
            (2, 3),
            (2, 8),
            (3, 4),
            (3, 7),
            (4, 5),
            (5, 3),
            (5, 6),
            (7, 4),
            (7, 6),
            (8, 1),
            (8, 7),
        ];
        for (src, dst) in edges {
            graph.add_edge(0, src, dst, NO_PROPS, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let scc_nodes: HashSet<Vec<_>> = strongly_connected_components(graph)
                .groups()
                .into_iter_groups()
                .map(|(_, v)| v.name().into_iter_values().sorted().collect())
                .collect();

            let expected: HashSet<Vec<String>> =
                [vec!["3", "4", "5", "7"], vec!["1", "2", "8"], vec!["6"]]
                    .into_iter()
                    .map(|v| v.into_iter().map(|s| s.to_owned()).collect())
                    .collect();
            assert_eq!(scc_nodes, expected);
        });
         */
    }

    #[test]
    fn scc_test_multiple_components_2() {
        /*
        let graph = Graph::new();
        let edges = [(1, 2), (1, 3), (1, 4), (4, 2), (3, 4), (2, 3)];
        for (src, dst) in edges {
            graph.add_edge(0, src, dst, NO_PROPS, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let scc_nodes: HashSet<Vec<_>> = strongly_connected_components(graph)
                .groups()
                .into_iter_groups()
                .map(|(_, v)| v.name().into_iter_values().sorted().collect())
                .collect();

            let expected: HashSet<Vec<String>> = [vec!["2", "3", "4"], vec!["1"]]
                .into_iter()
                .map(|v| v.into_iter().map(|s| s.to_owned()).collect())
                .collect();
            assert_eq!(scc_nodes, expected);
        });
        */
    }

    #[test]
    fn scc_test_all_singletons() {
        /*
        let graph = Graph::new();
        let edges = [
            (0, 1),
            (1, 2),
            (1, 3),
            (2, 4),
            (2, 5),
            (3, 4),
            (3, 5),
            (4, 6),
        ];
        for (src, dst) in edges {
            graph.add_edge(0, src, dst, NO_PROPS, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let scc_nodes: HashSet<Vec<_>> = strongly_connected_components(graph)
                .groups()
                .into_iter_groups()
                .map(|(_, v)| v.name().into_iter_values().sorted().collect())
                .collect();

            let expected: HashSet<Vec<String>> = [
                vec!["0"],
                vec!["1"],
                vec!["2"],
                vec!["3"],
                vec!["4"],
                vec!["5"],
                vec!["6"],
            ]
            .into_iter()
            .map(|v| v.into_iter().map(|s| s.to_owned()).collect())
            .collect();
            assert_eq!(scc_nodes, expected);
        });
         */
    }
}
