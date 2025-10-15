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

        let res = betweenness_centrality(graph, None, false)
            .to_hashmap(|value| value.betweenness_centrality);
        assert_eq!(res, expected);

        let mut expected: HashMap<String, f64> = HashMap::new();
        expected.insert("1".to_string(), 0.0);
        expected.insert("2".to_string(), 0.05);
        expected.insert("3".to_string(), 0.2);
        expected.insert("4".to_string(), 0.05);
        expected.insert("5".to_string(), 0.0);
        expected.insert("6".to_string(), 0.0);
        let res = betweenness_centrality(graph, None, true)
            .to_hashmap(|value| value.betweenness_centrality);
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

        let res = degree_centrality(graph).to_hashmap(|value| value.score);
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
        let results = hits(graph, 20, None).to_hashmap(|value| (value.hub_score, value.auth_score));

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
        let expected: HashMap<String, f64> = HashMap::from([
            ("1".to_string(), 0.38694),
            ("2".to_string(), 0.20195),
            ("3".to_string(), 0.20916),
            ("4".to_string(), 0.20195),
        ]);
        let results = unweighted_page_rank(graph, Some(1000), Some(1), None, true, None)
            .to_hashmap(|value| value.score);
        assert_eq_hashmaps_approx(&results, &expected, 1e-5);
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
        let expected: HashMap<String, f64> = HashMap::from([
            ("10".to_string(), 0.072082),
            ("8".to_string(), 0.136473),
            ("3".to_string(), 0.15484),
            ("6".to_string(), 0.07208),
            ("11".to_string(), 0.06186),
            ("2".to_string(), 0.03557),
            ("1".to_string(), 0.11284),
            ("4".to_string(), 0.07944),
            ("7".to_string(), 0.01638),
            ("9".to_string(), 0.06186),
            ("5".to_string(), 0.19658),
        ]);

        let results = unweighted_page_rank(graph, Some(1000), Some(4), None, true, None)
            .to_hashmap(|value| value.score);

        assert_eq_hashmaps_approx(&results, &expected, 1e-5);
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
        let expected: HashMap<String, f64> =
            HashMap::from([("1".to_string(), 0.5), ("2".to_string(), 0.5)]);

        let results = unweighted_page_rank(graph, Some(1000), Some(4), None, false, None)
            .to_hashmap(|value| value.score);

        assert_eq_hashmaps_approx(&results, &expected, 1e-3);
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
        let expected: HashMap<String, f64> = HashMap::from([
            ("1".to_string(), 0.303),
            ("2".to_string(), 0.393),
            ("3".to_string(), 0.303),
        ]);

        let results = unweighted_page_rank(graph, Some(10), Some(4), None, false, None)
            .to_hashmap(|value| value.score);

        assert_eq_hashmaps_approx(&results, &expected, 1e-3);
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
        let expected: HashMap<String, f64> = HashMap::from([
            ("1".to_string(), 0.055),
            ("2".to_string(), 0.079),
            ("3".to_string(), 0.113),
            ("4".to_string(), 0.055),
            ("5".to_string(), 0.070),
            ("6".to_string(), 0.083),
            ("7".to_string(), 0.093),
            ("8".to_string(), 0.102),
            ("9".to_string(), 0.110),
            ("10".to_string(), 0.117),
            ("11".to_string(), 0.122),
        ]);

        let results = unweighted_page_rank(graph, Some(1000), Some(4), None, true, None)
            .to_hashmap(|value| value.score);

        assert_eq_hashmaps_approx(&results, &expected, 1e-3);
    });
}
