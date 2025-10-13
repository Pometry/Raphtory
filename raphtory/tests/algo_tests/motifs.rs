#[cfg(test)]
mod global_motifs_test {
    use raphtory::{
        algorithms::motifs::global_temporal_three_node_motifs::temporal_three_node_motif_multi,
        db::{api::mutation::AdditionOps, graph::graph::Graph},
        prelude::*,
        test_storage,
    };

    fn load_graph(edges: Vec<(i64, u64, u64)>) -> Graph {
        let graph = Graph::new();

        for (t, src, dst) in edges {
            graph.add_edge(t, src, dst, NO_PROPS, None).unwrap();
        }
        graph
    }

    #[test]
    fn test_global() {
        let graph = load_graph(vec![
            (1, 1, 1),
            (1, 1, 1),
            (2, 1, 1),
            (1, 1, 2),
            (1, 1, 2),
            (2, 1, 3),
            (2, 1, 3),
            (3, 1, 4),
            (4, 3, 1),
            (5, 3, 4),
            (6, 3, 5),
            (7, 4, 5),
            (8, 5, 6),
            (9, 5, 8),
            (10, 7, 5),
            (11, 8, 5),
            (12, 1, 9),
            (13, 9, 1),
            (14, 6, 3),
            (15, 4, 8),
            (16, 8, 3),
            (17, 5, 10),
            (18, 10, 5),
            (19, 10, 8),
            (20, 1, 11),
            (21, 11, 1),
            (22, 9, 11),
            (23, 11, 9),
        ]);

        test_storage!(&graph, |graph| {
            let global_motifs = &temporal_three_node_motif_multi(graph, vec![10], None);

            let expected: [usize; 40] = vec![
                0, 2, 3, 8, 2, 4, 1, 5, 0, 0, 0, 0, 1, 0, 2, 0, 0, 1, 6, 0, 0, 1, 10, 2, 0, 1, 0,
                0, 0, 0, 1, 0, 2, 3, 2, 4, 1, 2, 4, 1,
            ]
            .into_iter()
            .map(|x| x as usize)
            .collect::<Vec<usize>>()
            .try_into()
            .unwrap();
            assert_eq!(global_motifs[0], expected);
        });
    }
}

#[cfg(test)]
mod local_motifs_test {
    use raphtory::{
        algorithms::motifs::local_temporal_three_node_motifs::temporal_three_node_motif,
        db::{api::mutation::AdditionOps, graph::graph::Graph},
        prelude::*,
        test_storage,
    };
    use raphtory_api::core::utils::logging::global_debug_logger;
    use std::collections::HashMap;
    use tracing::info;

    fn load_graph(edges: Vec<(i64, u64, u64)>) -> Graph {
        let graph = Graph::new();

        for (t, src, dst) in edges {
            graph.add_edge(t, src, dst, NO_PROPS, None).unwrap();
        }
        graph
    }

    fn load_sample_graph() -> Graph {
        let edges = vec![
            (1, 1, 1),
            (1, 1, 1),
            (2, 1, 1),
            (1, 1, 2),
            (2, 1, 3),
            (3, 1, 4),
            (4, 3, 1),
            (5, 3, 4),
            (6, 3, 5),
            (7, 4, 5),
            (8, 5, 6),
            (9, 5, 8),
            (10, 7, 5),
            (11, 8, 5),
            (12, 1, 9),
            (13, 9, 1),
            (14, 6, 3),
            (15, 4, 8),
            (16, 8, 3),
            (17, 5, 10),
            (18, 10, 5),
            (19, 10, 8),
            (20, 1, 11),
            (21, 11, 1),
            (22, 9, 11),
            (23, 11, 9),
        ];
        load_graph(edges)
    }

    #[ignore]
    #[test]
    fn test_triangle_motif() {
        global_debug_logger();
        let ij_kj_ik = vec![(1, 1, 2), (2, 3, 2), (3, 1, 3)];
        let g = load_graph(ij_kj_ik);
        let mc = temporal_three_node_motif(&g, 3, None);
        assert_eq!(
            *mc.get_by_node(3).unwrap(),
            [
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0
            ]
        );

        let ij_ki_jk = vec![(1, 1, 2), (2, 3, 1), (3, 2, 3)];
        let g = load_graph(ij_ki_jk);
        let mc = temporal_three_node_motif(&g, 3, None);
        assert_eq!(
            *mc.get_by_node(3).unwrap(),
            [
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0
            ]
        );

        let ij_jk_ik = vec![(1, 1, 2), (2, 2, 3), (3, 1, 3)];
        let g = load_graph(ij_jk_ik);
        let mc = temporal_three_node_motif(&g, 3, None);
        assert_eq!(
            *mc.get_by_node(3).unwrap(),
            [
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0
            ]
        );

        let ij_ik_jk = vec![(1, 1, 2), (2, 1, 3), (3, 2, 3)];
        let g = load_graph(ij_ik_jk);
        let mc = temporal_three_node_motif(&g, 3, None);
        assert_eq!(
            *mc.get_by_node(3).unwrap(),
            [
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0
            ]
        );

        let ij_kj_ki = vec![(1, 1, 2), (2, 3, 2), (3, 3, 1)];
        let g = load_graph(ij_kj_ki);
        let mc = temporal_three_node_motif(&g, 3, None);
        assert_eq!(
            *mc.get_by_node(3).unwrap(),
            [
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0
            ]
        );

        let ij_ki_kj = vec![(1, 1, 2), (2, 3, 1), (3, 3, 2)];
        let g = load_graph(ij_ki_kj);
        let mc = temporal_three_node_motif(&g, 3, None);
        assert_eq!(
            *mc.get_by_node(3).unwrap(),
            [
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0
            ]
        );

        let ij_jk_ki = vec![(1, 1, 2), (2, 2, 3), (3, 3, 1)];
        let g = load_graph(ij_jk_ki);
        let mc = temporal_three_node_motif(&g, 3, None);
        assert_eq!(
            *mc.get_by_node(3).unwrap(),
            [
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0
            ]
        );

        let ij_ik_kj = vec![(1, 1, 2), (2, 1, 3), (3, 3, 2)];
        let g = load_graph(ij_ik_kj);
        let mc = temporal_three_node_motif(&g, 3, None);
        assert_eq!(
            *mc.get_by_node(3).unwrap(),
            [
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1
            ]
        );
    }

    #[test]
    fn test_local_motif() {
        let graph = load_sample_graph();

        test_storage!(&graph, |graph| {
            let actual = temporal_three_node_motif(graph, 10, None);

            let expected: HashMap<String, Vec<usize>> = HashMap::from([
                (
                    "1".to_string(),
                    vec![
                        0, 0, 0, 0, 1, 2, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0,
                        0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 2, 0,
                    ],
                ),
                (
                    "10".to_string(),
                    vec![
                        0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1,
                    ],
                ),
                (
                    "11".to_string(),
                    vec![
                        0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0,
                        0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 0, 1, 0,
                    ],
                ),
                (
                    "2".to_string(),
                    vec![
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    ],
                ),
                (
                    "3".to_string(),
                    vec![
                        0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 2, 0, 1, 2, 0,
                    ],
                ),
                (
                    "4".to_string(),
                    vec![
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 1, 2, 0,
                    ],
                ),
                (
                    "5".to_string(),
                    vec![
                        0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 4, 0, 0, 0, 3, 0, 0,
                        0, 0, 0, 0, 0, 0, 0, 1, 2, 1, 3, 0, 1, 1, 1,
                    ],
                ),
                (
                    "6".to_string(),
                    vec![
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0,
                    ],
                ),
                (
                    "7".to_string(),
                    vec![
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    ],
                ),
                (
                    "8".to_string(),
                    vec![
                        0, 0, 2, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0, 0, 1, 2, 1, 2, 0, 1, 0, 1,
                    ],
                ),
                (
                    "9".to_string(),
                    vec![
                        0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0,
                        0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 0, 1, 0,
                    ],
                ),
            ]);
            assert_eq!(actual, expected);
        });
    }

    #[test]
    fn test_windowed_graph() {
        global_debug_logger();
        let g = load_sample_graph();
        let g_windowed = g.before(11).after(0);
        info! {"windowed graph has {:?} vertices",g_windowed.count_nodes()}

        let actual = temporal_three_node_motif(&g_windowed, 10, None);

        let expected: HashMap<String, Vec<usize>> = HashMap::from([
            (
                "1".to_string(),
                vec![
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0,
                ],
            ),
            (
                "2".to_string(),
                vec![
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                ],
            ),
            (
                "3".to_string(),
                vec![
                    0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 0,
                ],
            ),
            (
                "4".to_string(),
                vec![
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 0,
                ],
            ),
            (
                "5".to_string(),
                vec![
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0,
                ],
            ),
            (
                "6".to_string(),
                vec![
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                ],
            ),
            (
                "7".to_string(),
                vec![
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                ],
            ),
            (
                "8".to_string(),
                vec![
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                ],
            ),
        ]);

        assert_eq!(actual, expected);
    }
}

#[cfg(test)]
mod triangle_count_tests {
    use raphtory::{
        algorithms::motifs::local_triangle_count::local_triangle_count,
        db::{
            api::{mutation::AdditionOps, view::*},
            graph::graph::Graph,
        },
        prelude::*,
        test_storage,
    };

    #[test]
    fn counts_triangles() {
        let graph = Graph::new();
        let vs = vec![(1, 1, 2), (2, 1, 3), (3, 2, 1), (4, 3, 2)];

        for (t, src, dst) in &vs {
            graph.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let windowed_graph = graph.window(0, 5);
            let expected = vec![1, 1, 1];

            let actual = (1..=3)
                .map(|v| local_triangle_count(&windowed_graph, v).unwrap())
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);
        });
    }
}

#[cfg(test)]
mod rich_club_test {
    use raphtory::{
        algorithms::motifs::temporal_rich_club_coefficient::temporal_rich_club_coefficient,
        db::{api::mutation::AdditionOps, graph::graph::Graph},
        prelude::*,
    };

    use crate::algo_tests::centrality::assert_eq_f64;

    fn load_graph(edges: Vec<(i64, u64, u64)>) -> Graph {
        let graph = Graph::new();

        for (t, src, dst) in edges {
            graph.add_edge(t, src, dst, NO_PROPS, None).unwrap();
        }
        graph
    }

    fn load_sample_graph() -> Graph {
        let edges = vec![
            (1, 1, 2),
            (1, 1, 3),
            (1, 1, 4),
            (1, 2, 3),
            (1, 2, 4),
            (1, 3, 4),
            (1, 4, 5),
            (2, 1, 2),
            (2, 1, 3),
            (2, 1, 4),
            (2, 3, 4),
            (2, 2, 6),
            (3, 1, 2),
            (3, 2, 4),
            (3, 3, 4),
            (3, 1, 4),
            (3, 1, 3),
            (3, 1, 7),
            (4, 1, 2),
            (4, 1, 3),
            (4, 1, 4),
            (4, 2, 8),
            (5, 1, 2),
            (5, 1, 3),
            (5, 1, 4),
            (5, 2, 4),
            (5, 3, 9),
        ];
        load_graph(edges)
    }

    #[test]
    // Using the toy example from the paper
    fn toy_graph_test() {
        let g = load_sample_graph();
        let g_rolling = g.rolling(1, Some(1)).unwrap();

        let rc_coef_1 = temporal_rich_club_coefficient(&g, g_rolling.clone(), 3, 1);
        let rc_coef_3 = temporal_rich_club_coefficient(&g, g_rolling.clone(), 3, 3);
        let rc_coef_5 = temporal_rich_club_coefficient(&g, g_rolling.clone(), 3, 5);
        assert_eq_f64(Some(rc_coef_1), Some(1.0), 3);
        assert_eq_f64(Some(rc_coef_3), Some(0.66666), 3);
        assert_eq_f64(Some(rc_coef_5), Some(0.5), 3);
    }
}

#[cfg(test)]
mod triangle_count_tests_alt {
    use raphtory::{
        algorithms::motifs::triangle_count::triangle_count,
        db::{api::mutation::AdditionOps, graph::graph::Graph},
        prelude::NO_PROPS,
        test_storage,
    };
    use std::time::Instant;

    #[ignore]
    #[test]
    fn triangle_scale_test() {
        let graph = Graph::new();

        let n: u64 = 1500;
        for i in 1..n {
            for j in (i + 1)..n {
                graph.add_edge(i as i64, i, j, NO_PROPS, None).unwrap();
            }
        }

        let start = Instant::now();
        let tri_count = triangle_count(&graph.clone(), None);
        println!("Time elapsed: {:?}, {tri_count}", start.elapsed());
    }

    #[test]
    fn triangle_count_1() {
        let graph = Graph::new();

        let edges = vec![
            // triangle 1
            (1, 2, 1),
            (2, 3, 1),
            (3, 1, 1),
            //triangle 2
            (4, 5, 1),
            (5, 6, 1),
            (6, 4, 1),
            // triangle 4 and 5
            (7, 8, 2),
            (8, 9, 3),
            (9, 7, 4),
            (8, 10, 5),
            (10, 9, 6),
        ];

        for (src, dst, ts) in edges {
            graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let actual_tri_count = triangle_count(graph, Some(2));

            assert_eq!(actual_tri_count, 4)
        });
    }

    #[test]
    fn triangle_count_3() {
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
            let actual_tri_count = triangle_count(graph, None);
            assert_eq!(actual_tri_count, 8)
        });
    }
}

#[cfg(test)]
mod triplet_test {
    use pretty_assertions::assert_eq;
    use raphtory::{
        algorithms::motifs::triplet_count::triplet_count,
        db::{api::mutation::AdditionOps, graph::graph::Graph},
        prelude::*,
        test_storage,
    };

    /// Test the global clustering coefficient
    #[test]
    fn test_triplet_count() {
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
            let exp_triplet_count = 20;
            let results = triplet_count(graph, None);

            assert_eq!(results, exp_triplet_count);
        });
    }
}
