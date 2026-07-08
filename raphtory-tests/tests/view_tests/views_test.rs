use itertools::Itertools;
use proptest::{prop_assert, prop_assert_eq, prop_assume, proptest};
use rand::{prelude::*, rng};
use raphtory::{
    algorithms::centrality::degree_centrality::degree_centrality,
    db::graph::{graph::assert_graph_equal, views::window_graph::WindowedGraph},
    prelude::*,
};
use raphtory_api::core::{
    entities::GID,
    storage::timeindex::AsTime,
    utils::{logging::global_info_logger, time::IntoTime},
};
use raphtory_tests::{test_storage, utils::test_graph};
use rayon::prelude::*;
use std::ops::Range;
use tracing::{error, info};

#[test]
fn test_non_restricted_window() {
    let g = Graph::new();
    g.add_edge(0, 0, 1, NO_PROPS, None).unwrap();

    for n in g.window(0, 1).nodes() {
        assert!(g.has_node(n));
    }

    assert_graph_equal(&g.window(0, 1), &g)
}

#[test]
fn windowed_graph_nodes_degree() {
    let vs = vec![
        (1, 1, 2),
        (2, 1, 3),
        (-1, 2, 1),
        (0, 1, 1),
        (7, 3, 2),
        (1, 1, 1),
    ];

    let graph = Graph::new();

    for (t, src, dst) in &vs {
        graph.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
    }
    test_storage!(&graph, |graph| {
        let wg = graph.window(-1, 1);

        let actual = wg
            .nodes()
            .iter()
            .map(|v| (v.id(), v.degree()))
            .collect::<Vec<_>>();

        let expected = vec![(GID::U64(1), 2), (GID::U64(2), 1)];

        assert_eq!(actual, expected);
    });
}

#[test]
fn windowed_graph_edge() {
    let vs = vec![
        (1, 1, 2),
        (2, 1, 3),
        (-1, 2, 1),
        (0, 1, 1),
        (7, 3, 2),
        (1, 1, 1),
    ];

    let graph = Graph::new();

    for (t, src, dst) in vs {
        graph.add_edge(t, src, dst, NO_PROPS, None).unwrap();
    }
    test_storage!(&graph, |graph| {
        let wg = graph.window(i64::MIN, i64::MAX);
        assert_eq!(wg.edge(1, 3).unwrap().src().id(), GID::U64(1));
        assert_eq!(wg.edge(1, 3).unwrap().dst().id(), GID::U64(3));
    });
}

#[test]
fn windowed_graph_node_edges() {
    let vs = vec![
        (1, 1, 2),
        (2, 1, 3),
        (-1, 2, 1),
        (0, 1, 1),
        (7, 3, 2),
        (1, 1, 1),
    ];

    let graph = Graph::new();

    for (t, src, dst) in &vs {
        graph.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
    }
    test_storage!(&graph, |graph| {
        let wg = graph.window(-1, 1);

        assert_eq!(wg.node(1).unwrap().id(), GID::U64(1));
    });
}

#[test]
fn graph_has_node_check_fail() {
    let vs: Vec<(i64, u64)> = vec![
        (1, 0),
        (-100, 262),
        // (327226439, 108748364996394682),
        (1, 9135428456135679950),
        // (0, 1),
        // (2, 2),
    ];
    let graph = Graph::new();

    for (t, v) in &vs {
        graph.add_node(*t, *v, NO_PROPS, None, None).unwrap();
    }

    // FIXME: Issue #46: arrow_test(&graph, test)
    test_graph(&graph, |graph| {
        let wg = graph.window(1, 2);
        assert!(!wg.has_node(262))
    });
}

#[test]
fn windowed_graph_has_node_proptest() {
    proptest!(|(mut vs: Vec<(i64, u64)>)| {
        global_info_logger();
        prop_assume!(!vs.is_empty());

        vs.sort_by_key(|v| v.1); // Sorted by node
        vs.dedup_by_key(|v| v.1); // Have each node only once to avoid headaches
        vs.sort_by_key(|v| v.0); // Sorted by time

        let rand_start_index = rng().random_range(0..vs.len());
        let rand_end_index = rng().random_range(rand_start_index..vs.len());

        let g = Graph::new();

        for (t, v) in &vs {
            g.add_node(*t, *v, NO_PROPS, None, None)
                .map_err(|err| error!("{:?}", err))
                .ok();
        }

        let start = vs.get(rand_start_index).expect("start index in range").0;
        let end = vs.get(rand_end_index).expect("end index in range").0;

        let wg = g.window(start, end);

        let rand_test_index: usize = rng().random_range(0..vs.len());

        let (i, v) = vs.get(rand_test_index).expect("test index in range");
        if (start..end).contains(i) {
            prop_assert!(wg.has_node(*v), "Node {:?} was not in window {:?}", (i, v), start..end);
        } else {
            prop_assert!(!wg.has_node(*v), "Node {:?} was in window {:?}", (i, v), start..end);
        }
    });
}

#[test]
fn windowed_graph_has_edge_proptest() {
    proptest!(|(mut edges: Vec<(i64, (u64, u64))>)| {
        prop_assume!(!edges.is_empty());

        edges.sort_by_key(|e| e.1); // Sorted by edge
        edges.dedup_by_key(|e| e.1); // Have each edge only once to avoid headaches
        edges.sort_by_key(|e| e.0); // Sorted by time

        let rand_start_index = rng().random_range(0..edges.len());
        let rand_end_index = rng().random_range(rand_start_index..edges.len());

        let g = Graph::new();

        for (t, e) in &edges {
            g.add_edge(*t, e.0, e.1, NO_PROPS, None).unwrap();
        }

        let start = edges.get(rand_start_index).expect("start index in range").0;
        let end = edges.get(rand_end_index).expect("end index in range").0;

        let wg = g.window(start, end);

        let rand_test_index: usize = rng().random_range(0..edges.len());

        let (i, e) = edges.get(rand_test_index).expect("test index in range");
        if (start..end).contains(i) {
            prop_assert!(wg.has_edge(e.0, e.1), "Edge {:?} was not in window {:?}", (i, e), start..end);
        } else {
            prop_assert!(!wg.has_edge(e.0, e.1), "Edge {:?} was in window {:?}", (i, e), start..end);
        }
    });
}

#[test]
fn windowed_graph_edge_count_proptest() {
    proptest!(|(mut edges: Vec<(i64, (u64, u64))>, window: Range<i64>)| {
        global_info_logger();
        prop_assume!(window.end >= window.start);

        edges.sort_by_key(|e| e.1); // Sorted by edge
        edges.dedup_by_key(|e| e.1); // Have each edge only once to avoid headaches

        let true_edge_count = edges.iter().filter(|e| window.contains(&e.0)).count();

        let g = Graph::new();

        for (t, e) in &edges {
            g.add_edge(*t, e.0, e.1, [("test".to_owned(), Prop::Bool(true))], None)
                .unwrap();
        }

        let wg = g.window(window.start, window.end);
        if wg.count_edges() != true_edge_count {
            info!(
                "failed, g.num_edges() = {}, true count = {}",
                wg.count_edges(),
                true_edge_count
            );
            info!("g.edges() = {:?}", wg.edges().iter().collect_vec());
        }
        prop_assert_eq!(wg.count_edges(), true_edge_count);
    });
}

#[test]
fn trivial_window_has_all_edges_proptest() {
    proptest!(|(edges: Vec<(i64, u64, u64)>)| {
        let g = Graph::new();
        edges
            .into_par_iter()
            .filter(|e| e.0 < i64::MAX)
            .for_each(|(t, src, dst)| {
                g.add_edge(t, src, dst, [("test".to_owned(), Prop::Bool(true))], None)
                    .unwrap();
            });
        let w = g.window(i64::MIN, i64::MAX);
        prop_assert!(g.edges()
            .iter()
            .all(|e| w.has_edge(e.src().id(), e.dst().id())));
    });
}

#[test]
fn large_node_in_window_proptest() {
    proptest!(|(dsts: Vec<u64>)| {
        let dsts: Vec<u64> = dsts.into_iter().unique().collect();
        let n = dsts.len();
        let g = Graph::new();

        for dst in dsts {
            let t = 1;
            g.add_edge(t, 0, dst, NO_PROPS, None).unwrap();
        }
        let w = g.window(i64::MIN, i64::MAX);
        prop_assert_eq!(w.count_edges(), n);
    });
}

#[test]
fn windowed_graph_node_ids() {
    let vs = vec![(1, 1, 2), (3, 3, 4), (5, 5, 6), (7, 7, 1)];

    let args = [(i64::MIN, 8), (i64::MIN, 2), (i64::MIN, 4), (3, 6)];

    let expected = vec![
        vec![1, 2, 3, 4, 5, 6, 7],
        vec![1, 2],
        vec![1, 2, 3, 4],
        vec![3, 4, 5, 6],
    ];

    let graph = Graph::new();

    for (t, src, dst) in &vs {
        graph.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
    }

    test_storage!(&graph, |graph| {
        let res: Vec<_> = (0..=3)
            .map(|i| {
                let wg = graph.window(args[i].0, args[i].1);
                let mut e = wg
                    .nodes()
                    .id()
                    .iter_values()
                    .filter_map(|id| id.to_u64())
                    .collect::<Vec<_>>();
                e.sort();
                e
            })
            .collect_vec();

        assert_eq!(res, expected);
    });

    let graph = Graph::new();
    for (src, dst, t) in &vs {
        graph.add_edge(*src, *dst, *t, NO_PROPS, None).unwrap();
    }
    test_storage!(&graph, |graph| {
        let res: Vec<_> = (0..=3)
            .map(|i| {
                let wg = graph.window(args[i].0, args[i].1);
                let mut e = wg
                    .nodes()
                    .id()
                    .iter_values()
                    .filter_map(|id| id.to_u64())
                    .collect::<Vec<_>>();
                e.sort();
                e
            })
            .collect_vec();
        assert_eq!(res, expected);
    });
}

#[test]
fn windowed_graph_nodes() {
    let vs = vec![
        (1, 1, 2),
        (2, 1, 3),
        (-1, 2, 1),
        (0, 1, 1),
        (7, 3, 2),
        (1, 1, 1),
    ];

    let graph = Graph::new();

    graph
        .add_node(
            0,
            1,
            [("type", "wallet".into_prop()), ("cost", 99.5.into_prop())],
            None,
            None,
        )
        .unwrap();

    graph
        .add_node(
            -1,
            2,
            [("type", "wallet".into_prop()), ("cost", 10.0.into_prop())],
            None,
            None,
        )
        .unwrap();

    graph
        .add_node(
            6,
            3,
            [("type", "wallet".into_prop()), ("cost", 76.2.into_prop())],
            None,
            None,
        )
        .unwrap();

    for (t, src, dst) in &vs {
        graph
            .add_edge(*t, *src, *dst, [("eprop", "commons")], None)
            .unwrap();
    }
    test_storage!(&graph, |graph| {
        let wg = graph.window(-2, 0);

        let actual = wg
            .nodes()
            .id()
            .iter_values()
            .filter_map(|id| id.to_u64())
            .collect::<Vec<_>>();

        let expected = vec![1, 2];

        assert_eq!(actual, expected);
    });
}

#[test]
fn test_reference() {
    let graph = Graph::new();
    graph.add_edge(0, 1, 2, NO_PROPS, None).unwrap();

    test_storage!(&graph, |graph| {
        let mut w = WindowedGraph::new(&graph, Some(0.into_time()), Some(1.into_time()));
        assert_eq!(w, graph);
        w = WindowedGraph::new(&graph, Some(1.into_time()), Some(2.into_time()));
        assert_eq!(w, Graph::new());
    });
}

#[test]
fn test_algorithm_on_windowed_graph() {
    global_info_logger();
    let graph = Graph::new();
    graph.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
    test_storage!(&graph, |graph| {
        let w = graph.window(0, 1);
        let _ = degree_centrality(&w);
    });
}

#[test]
fn test_view_resetting() {
    let graph = Graph::new();
    for t in 0..10 {
        let t1 = t * 3;
        let t2 = t * 3 + 1;
        let t3 = t * 3 + 2;
        graph.add_edge(t1, 1, 2, NO_PROPS, None).unwrap();
        graph.add_edge(t2, 2, 3, NO_PROPS, None).unwrap();
        graph.add_edge(t3, 3, 1, NO_PROPS, None).unwrap();
    }

    test_storage!(&graph, |graph| {
        assert_graph_equal(&graph.before(9).after(2), &graph.window(3, 9));
        let res = graph
            .window(3, 9)
            .nodes()
            .before(6)
            .edges()
            .window(1, 9)
            .earliest_time()
            .map(|it| it.map(|t_opt| t_opt.map(|t| t.t())).collect_vec())
            .collect_vec();
        assert_eq!(
            res,
            [[Some(3), Some(5)], [Some(3), Some(4)], [Some(5), Some(4)]]
        );
    });
}

#[test]
fn test_entity_history() {
    let graph = Graph::new();
    graph.add_node(0, 0, NO_PROPS, None, None).unwrap();
    graph.add_node(1, 0, NO_PROPS, None, None).unwrap();
    graph.add_node(2, 0, NO_PROPS, None, None).unwrap();
    graph.add_node(3, 0, NO_PROPS, None, None).unwrap();
    graph.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
    graph.add_edge(1, 1, 2, NO_PROPS, None).unwrap();
    graph.add_edge(2, 1, 2, NO_PROPS, None).unwrap();
    graph.add_edge(3, 1, 2, NO_PROPS, None).unwrap();
    graph.add_edge(4, 1, 3, NO_PROPS, None).unwrap();
    graph.add_edge(5, 1, 3, NO_PROPS, None).unwrap();
    graph.add_edge(6, 1, 3, NO_PROPS, None).unwrap();
    graph.add_edge(7, 1, 3, NO_PROPS, None).unwrap();

    // FIXME: Issue #46
    test_graph(&graph, |graph| {
        let e = graph.edge(1, 2).unwrap();
        let v = graph.node(0).unwrap();
        let full_history_1 = vec![0i64, 1, 2, 3];

        let full_history_2 = vec![4i64, 5, 6, 7];

        let windowed_history = vec![0i64, 1];

        assert_eq!(v.history(), full_history_1);

        assert_eq!(v.window(0, 2).history(), windowed_history);
        assert_eq!(e.history(), full_history_1);
        assert_eq!(e.window(0, 2).history(), windowed_history);

        assert_eq!(
            graph.edges().history().collect_vec(),
            [full_history_1.clone(), full_history_2.clone()]
        );
        assert_eq!(
            graph
                .nodes()
                .in_edges()
                .history()
                .map(|it| it.collect_vec())
                .collect_vec(),
            [vec![], vec![], vec![full_history_1], vec![full_history_2],]
        );

        assert_eq!(
            graph
                .nodes()
                .earliest_time()
                .iter_values()
                .flatten()
                .collect_vec(),
            [0, 0, 0, 4,]
        );

        assert_eq!(
            graph
                .nodes()
                .latest_time()
                .iter_values()
                .flatten()
                .collect_vec(),
            [3, 7, 3, 7]
        );

        assert_eq!(
            graph
                .nodes()
                .neighbours()
                .latest_time()
                .sorted_by_key(|(n, _)| n.id())
                .map(|(_, it)| it.flatten().collect_vec())
                .collect_vec(),
            [vec![], vec![3, 7], vec![7], vec![7],]
        );

        assert_eq!(
            graph
                .nodes()
                .neighbours()
                .earliest_time()
                .sorted_by_key(|(n, _)| n.id())
                .map(|(_, it)| it.flatten().collect_vec())
                .collect_vec(),
            [vec![], vec![0, 4], vec![0], vec![0],]
        );
    });
}
