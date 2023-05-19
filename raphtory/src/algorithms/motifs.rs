use crate::algorithms::three_node_local::*;
use crate::core::state::accumulator_id::accumulators::val;
use crate::core::state::compute_state::ComputeStateVec;
use crate::db::task::context::Context;
use crate::db::task::eval_vertex::EvalVertexView;
use crate::db::task::task::{ATask, Job, Step};
use crate::db::task::task_runner::TaskRunner;
use crate::db::view_api::{GraphViewOps, VertexViewOps};
use num_traits::Zero;
use std::collections::HashMap;
use std::ops::Add;

#[derive(Eq, PartialEq, Clone, Debug, Default)]
pub struct MotifCounter {
    pub two_nodes: [usize; 8],
    pub star_nodes: [usize; 24],
    pub triangle: [usize; 8],
}

impl MotifCounter {
    fn new(two_nodes: [usize; 8], star_nodes: [usize; 24], triangle: [usize; 8]) -> Self {
        Self {
            two_nodes,
            star_nodes,
            triangle,
        }
    }

    pub(crate) fn from_triangle_counter(triangle: [usize; 8]) -> Self {
        Self {
            two_nodes: [0; 8],
            star_nodes: [0; 24],
            triangle,
        }
    }
}

impl Add for MotifCounter {
    type Output = MotifCounter;

    fn add(self, rhs: Self) -> Self::Output {
        rhs
    }
}

impl Zero for MotifCounter {
    fn zero() -> Self {
        MotifCounter {
            two_nodes: [0; 8],
            star_nodes: [0; 24],
            triangle: [0; 8],
        }
    }

    fn set_zero(&mut self) {
        *self = Zero::zero();
    }

    fn is_zero(&self) -> bool {
        self.two_nodes == [0; 8] && self.star_nodes == [0; 24] && self.triangle == [0; 8]
    }
}

pub fn global_temporal_three_node_motif<G: GraphViewOps>(
    graph: &G,
    threads: Option<usize>,
    delta: i64,
) -> Vec<usize> {
    let counts = temporal_three_node_motif(graph, threads, delta);
    let mut tmp_counts = counts.values().fold(vec![0; 40], |acc, x| {
        acc.iter().zip(x.iter()).map(|(x1, x2)| x1 + x2).collect()
    });
    for ind in 31..40 {
        tmp_counts[ind] = tmp_counts[ind] / 3;
    }
    tmp_counts
}

pub fn global_temporal_three_node_motif_from_local(
    counts: HashMap<String, Vec<usize>>,
) -> Vec<usize> {
    let mut tmp_counts = counts.values().fold(vec![0; 40], |acc, x| {
        acc.iter().zip(x.iter()).map(|(x1, x2)| x1 + x2).collect()
    });
    for ind in 31..40 {
        tmp_counts[ind] = tmp_counts[ind] / 3;
    }
    tmp_counts
}

pub fn temporal_three_node_motif<G: GraphViewOps>(
    g: &G,
    threads: Option<usize>,
    delta: i64,
) -> HashMap<String, Vec<usize>> {
    let ctx: Context<G, ComputeStateVec> = g.into();
    let motifs_counter = val::<MotifCounter>(0);

    let step1 = ATask::new(move |evv: &EvalVertexView<G, ComputeStateVec>| {
        let g = evv.g.as_ref();

        triangle_motif_count(g, evv, delta, motifs_counter);
        let two_nodes = twonode_motif_count(g, evv, delta);
        let star_nodes = star_motif_count(evv, delta);

        evv.update_local(
            &motifs_counter,
            MotifCounter::new(
                two_nodes,
                star_nodes,
                evv.read_local(&motifs_counter).triangle,
            ),
        );

        Step::Continue
    });

    let mut runner: TaskRunner<G, _> = TaskRunner::new(ctx);

    runner.run(
        vec![],
        vec![Job::new(step1)],
        |_, _, els| {
            els.finalize(&motifs_counter, |motifs_counter| {
                let triangles = motifs_counter.triangle.to_vec();
                let two_nodes = motifs_counter.two_nodes.to_vec();
                let tmp_stars = motifs_counter.star_nodes.to_vec();
                let stars: Vec<usize> = tmp_stars
                    .iter()
                    .zip(two_nodes.iter().cycle().take(24))
                    .map(|(&x1, &x2)| x1 - x2)
                    .collect();
                let mut final_cts = Vec::new();
                final_cts.extend(stars.into_iter());
                final_cts.extend(two_nodes.into_iter());
                final_cts.extend(triangles.into_iter());

                final_cts
            })
        },
        threads,
        1,
        None,
        None,
    )
}

#[cfg(test)]
mod motifs_test {
    use super::*;
    use crate::db::graph::Graph;

    fn load_graph(n_shards: usize, edges: Vec<(i64, u64, u64)>) -> Graph {
        let graph = Graph::new(n_shards);

        for (t, src, dst) in edges {
            graph.add_edge(t, src, dst, &vec![], None).unwrap();
        }
        graph
    }

    #[test]
    fn test_two_node_motif() {
        let g = load_graph(
            1,
            vec![
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
            ],
        );

        let actual = temporal_three_node_motif(&g, None, 10);

        let expected: HashMap<String, Vec<usize>> = HashMap::from([
            (
                "1".to_string(),
                vec![
                    0, 0, 0, 0, 1, 2, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 2, 0,
                ],
            ),
            (
                "10".to_string(),
                vec![
                    0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1,
                ],
            ),
            (
                "11".to_string(),
                vec![
                    0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 0, 1, 0,
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
                    0, 0, 0, 0, 0, 0, 0, 1, 0, 2, 0, 1, 2, 0,
                ],
            ),
            (
                "4".to_string(),
                vec![
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 1, 2, 0,
                ],
            ),
            (
                "5".to_string(),
                vec![
                    0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 4, 0, 0, 0, 3, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 1, 2, 1, 3, 0, 1, 1, 1,
                ],
            ),
            (
                "6".to_string(),
                vec![
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0,
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
                    0, 0, 2, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 1, 2, 1, 2, 0, 1, 0, 1,
                ],
            ),
            (
                "9".to_string(),
                vec![
                    0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 0, 1, 0,
                ],
            ),
        ]);

        for ind in 1..12 {
            assert_eq!(
                actual.get(&ind.to_string()).unwrap(),
                expected.get(&ind.to_string()).unwrap()
            );
        }
    }
}
