use std::{borrow::Borrow, collections::HashMap};

use itertools::Itertools;
use raphtory::{
    algorithms::centrality::{
        betweenness::betweenness_centrality, degree_centrality::degree_centrality, hits::hits,
        pagerank::unweighted_page_rank,
    },
    prelude::*,
    test_storage,
};

#[test]
fn test_betweenness_centrality() {
    let graph = Graph::new();
    let vs = vec![
        (1, 2),
        (1, 3),
        (1, 4),
        (2, 3),
        (2, 4),
        (2, 5),
        (3, 4),
        (3, 5),
        (3, 6),
        (4, 3),
        (4, 2),
        (4, 4),
    ];
    for (src, dst) in &vs {
        graph.add_edge(0, *src, *dst, NO_PROPS, None).unwrap();
    }

    test_storage!(&graph, |graph| {
        let mut expected: HashMap<String, f64> = HashMap::new();
        expected.insert("1".to_string(), 0.0);
        expected.insert("2".to_string(), 1.0);
        expected.insert("3".to_string(), 4.0);
        expected.insert("4".to_string(), 1.0);
        expected.insert("5".to_string(), 0.0);
        expected.insert("6".to_string(), 0.0);

        let res = betweenness_centrality(graph, None, false);
        assert_eq!(res, expected);

        let mut expected: HashMap<String, f64> = HashMap::new();
        expected.insert("1".to_string(), 0.0);
        expected.insert("2".to_string(), 0.05);
        expected.insert("3".to_string(), 0.2);
        expected.insert("4".to_string(), 0.05);
        expected.insert("5".to_string(), 0.0);
        expected.insert("6".to_string(), 0.0);
        let res = betweenness_centrality(graph, None, true);
        assert_eq!(res, expected);
    });
}

#[test]
fn test_degree_centrality() {
    let graph = Graph::new();
    let vs = vec![(1, 2), (1, 3), (1, 4), (2, 3), (2, 4)];
    for (src, dst) in &vs {
        graph.add_edge(0, *src, *dst, NO_PROPS, None).unwrap();
    }
    test_storage!(&graph, |graph| {
        let mut expected: HashMap<String, f64> = HashMap::new();
        expected.insert("1".to_string(), 1.0);
        expected.insert("2".to_string(), 1.0);
        expected.insert("3".to_string(), 2.0 / 3.0);
        expected.insert("4".to_string(), 2.0 / 3.0);

        let res = degree_centrality(graph);
        assert_eq!(res, expected);
    });
}

#[test]
fn test_hits() {
    let edges = vec![
        (1, 4),
        (2, 3),
        (2, 5),
        (3, 1),
        (4, 2),
        (4, 3),
        (5, 2),
        (5, 3),
        (5, 4),
        (5, 6),
        (6, 3),
        (6, 8),
        (7, 1),
        (7, 3),
        (8, 1),
    ];
    let graph = Graph::new();

    for (src, dst) in edges {
        graph.add_edge(0, src, dst, NO_PROPS, None).unwrap();
    }
    test_storage!(&graph, |graph| {
        let results = hits(graph, 20, None);

        assert_eq!(
            results,
            HashMap::from([
                ("1".to_string(), (0.0431365, 0.096625775)),
                ("2".to_string(), (0.14359662, 0.18366566)),
                ("3".to_string(), (0.030866561, 0.36886504)),
                ("4".to_string(), (0.1865414, 0.12442485)),
                ("5".to_string(), (0.26667944, 0.05943252)),
                ("6".to_string(), (0.14359662, 0.10755368)),
                ("7".to_string(), (0.15471625, 0.0)),
                ("8".to_string(), (0.030866561, 0.05943252))
            ])
        );
    });
}

#[test]
fn test_page_rank() {
    let graph = Graph::new();

    let edges = vec![(1, 2), (1, 4), (2, 3), (3, 1), (4, 1)];

    for (src, dst) in edges {
        graph.add_edge(0, src, dst, NO_PROPS, None).unwrap();
    }

    test_storage!(&graph, |graph| {
        let results = unweighted_page_rank(graph, Some(1000), Some(1), None, true, None);

        assert_eq_f64(results.get_by_node("1"), Some(&0.38694), 5);
        assert_eq_f64(results.get_by_node("2"), Some(&0.20195), 5);
        assert_eq_f64(results.get_by_node("4"), Some(&0.20195), 5);
        assert_eq_f64(results.get_by_node("3"), Some(&0.20916), 5);
    });
}

#[test]
fn motif_page_rank() {
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

    let graph = Graph::new();

    for (src, dst, t) in edges {
        graph.add_edge(t, src, dst, NO_PROPS, None).unwrap();
    }

    test_storage!(&graph, |graph| {
        let results = unweighted_page_rank(graph, Some(1000), Some(4), None, true, None);

        assert_eq_f64(results.get_by_node("10"), Some(&0.072082), 5);
        assert_eq_f64(results.get_by_node("8"), Some(&0.136473), 5);
        assert_eq_f64(results.get_by_node("3"), Some(&0.15484), 5);
        assert_eq_f64(results.get_by_node("6"), Some(&0.07208), 5);
        assert_eq_f64(results.get_by_node("11"), Some(&0.06186), 5);
        assert_eq_f64(results.get_by_node("2"), Some(&0.03557), 5);
        assert_eq_f64(results.get_by_node("1"), Some(&0.11284), 5);
        assert_eq_f64(results.get_by_node("4"), Some(&0.07944), 5);
        assert_eq_f64(results.get_by_node("7"), Some(&0.01638), 5);
        assert_eq_f64(results.get_by_node("9"), Some(&0.06186), 5);
        assert_eq_f64(results.get_by_node("5"), Some(&0.19658), 5);
    });
}

#[test]
fn two_nodes_page_rank() {
    let edges = vec![(1, 2), (2, 1)];

    let graph = Graph::new();

    for (t, (src, dst)) in edges.into_iter().enumerate() {
        graph.add_edge(t as i64, src, dst, NO_PROPS, None).unwrap();
    }

    test_storage!(&graph, |graph| {
        let results = unweighted_page_rank(graph, Some(1000), Some(4), None, false, None);

        assert_eq_f64(results.get_by_node("1"), Some(&0.5), 3);
        assert_eq_f64(results.get_by_node("2"), Some(&0.5), 3);
    });
}

#[test]
fn three_nodes_page_rank_one_dangling() {
    let edges = vec![(1, 2), (2, 1), (2, 3)];

    let graph = Graph::new();

    for (t, (src, dst)) in edges.into_iter().enumerate() {
        graph.add_edge(t as i64, src, dst, NO_PROPS, None).unwrap();
    }

    test_storage!(&graph, |graph| {
        let results = unweighted_page_rank(graph, Some(10), Some(4), None, false, None);

        assert_eq_f64(results.get_by_node("1"), Some(&0.303), 3);
        assert_eq_f64(results.get_by_node("2"), Some(&0.393), 3);
        assert_eq_f64(results.get_by_node("3"), Some(&0.303), 3);
    });
}

#[test]
fn dangling_page_rank() {
    let edges = vec![
        (1, 2),
        (1, 3),
        (2, 3),
        (3, 1),
        (3, 2),
        (3, 4),
        // dangling from here
        (4, 5),
        (5, 6),
        (6, 7),
        (7, 8),
        (8, 9),
        (9, 10),
        (10, 11),
    ]
    .into_iter()
    .enumerate()
    .map(|(t, (src, dst))| (src, dst, t as i64))
    .collect_vec();

    let graph = Graph::new();

    for (src, dst, t) in edges {
        graph.add_edge(t, src, dst, NO_PROPS, None).unwrap();
    }
    test_storage!(&graph, |graph| {
        let results = unweighted_page_rank(graph, Some(1000), Some(4), None, true, None);

        assert_eq_f64(results.get_by_node("1"), Some(&0.055), 3);
        assert_eq_f64(results.get_by_node("2"), Some(&0.079), 3);
        assert_eq_f64(results.get_by_node("3"), Some(&0.113), 3);
        assert_eq_f64(results.get_by_node("4"), Some(&0.055), 3);
        assert_eq_f64(results.get_by_node("5"), Some(&0.070), 3);
        assert_eq_f64(results.get_by_node("6"), Some(&0.083), 3);
        assert_eq_f64(results.get_by_node("7"), Some(&0.093), 3);
        assert_eq_f64(results.get_by_node("8"), Some(&0.102), 3);
        assert_eq_f64(results.get_by_node("9"), Some(&0.110), 3);
        assert_eq_f64(results.get_by_node("10"), Some(&0.117), 3);
        assert_eq_f64(results.get_by_node("11"), Some(&0.122), 3);
    });
}

pub fn assert_eq_f64<T: Borrow<f64> + PartialEq + std::fmt::Debug>(
    a: Option<T>,
    b: Option<T>,
    decimals: u8,
) {
    if a.is_none() || b.is_none() {
        assert_eq!(a, b);
    } else {
        let factor = 10.0_f64.powi(decimals as i32);
        match (a, b) {
            (Some(a), Some(b)) => {
                let left = (a.borrow() * factor).round();
                let right = (b.borrow() * factor).round();
                assert_eq!(left, right,);
            }
            _ => unreachable!(),
        }
    }
}
