use std::collections::HashMap;
use std::slice::Iter;

use crate::core::state::accumulator_id::AccId;
use crate::core::state::agg::ValDef;
use crate::db::view_api::*;

use crate::algorithms::motifs::three_node_motifs::*;
use crate::core::state::accumulator_id::accumulators::val;
use crate::core::state::compute_state::ComputeStateVec;
use crate::db::task::context::Context;
use crate::db::task::eval_vertex::EvalVertexView;
use crate::db::task::task::{ATask, Job, Step};
use crate::db::task::task_runner::TaskRunner;
use crate::db::view_api::{GraphViewOps, VertexViewOps};
use num_traits::Zero;
use std::ops::Add;

pub fn star_motif_count<G: GraphViewOps>(
    evv: &EvalVertexView<G, ComputeStateVec, MotifCounter>,
    delta: i64,
) -> [usize; 24] {
    let neigh_map: HashMap<u64, usize> = evv
        .neighbours()
        .into_iter()
        .enumerate()
        .map(|(num, nb)| (nb.id(), num))
        .into_iter()
        .collect();
    let mut exploded_edges = evv
        .edges()
        .explode()
        .map(|edge| {
            if edge.src().id() == evv.id() {
                star_event(neigh_map[&edge.dst().id()], 1, edge.time().unwrap())
            } else {
                star_event(neigh_map[&edge.src().id()], 0, edge.time().unwrap())
            }
        })
        .collect::<Vec<StarEvent>>();
    exploded_edges.sort_by_key(|e| e.time);
    let mut star_count = init_star_count(neigh_map.len());
    star_count.execute(&exploded_edges, delta);
    star_count.return_counts()
}

pub fn twonode_motif_count<G: GraphViewOps>(
    graph: &G,
    evv: &EvalVertexView<G, ComputeStateVec, MotifCounter>,
    delta: i64,
) -> [usize; 8] {
    let mut counts = [0; 8];
    for nb in evv.neighbours().into_iter() {
        let nb_id = nb.id();
        let out = graph.edge(evv.id(), nb_id, None);
        let inc = graph.edge(nb_id, evv.id(), None);
        let mut all_exploded = match (out, inc) {
            (Some(o), Some(i)) => o
                .explode()
                .chain(i.explode())
                .map(|e| {
                    two_node_event(
                        if e.src().id() == evv.id() { 1 } else { 0 },
                        e.time().unwrap(),
                    )
                })
                .collect::<Vec<TwoNodeEvent>>(),
            (Some(o), None) => o
                .explode()
                .map(|e| two_node_event(1, e.time().unwrap()))
                .collect::<Vec<TwoNodeEvent>>(),
            (None, Some(i)) => i
                .explode()
                .map(|e| two_node_event(0, e.time().unwrap()))
                .collect::<Vec<TwoNodeEvent>>(),
            (None, None) => Vec::new(),
        };
        all_exploded.sort_by_key(|e| e.time);
        let mut two_node_counter = init_two_node_count();
        two_node_counter.execute(&all_exploded, delta);
        let two_node_result = two_node_counter.return_counts();
        for i in 0..8 {
            counts[i] += two_node_result[i];
        }
    }
    counts
}

pub fn triangle_motif_count<G: GraphViewOps>(
    graph: &G,
    evv: &EvalVertexView<G, ComputeStateVec, MotifCounter>,
    delta: i64,
    motif_counter: AccId<MotifCounter, MotifCounter, MotifCounter, ValDef<MotifCounter>>,
) {
    let u: u64 = evv.id();
    for v in evv.neighbours().into_iter().filter(|x| x.id() > u) {
        let mut nb_ct = 0;
        for nb in evv.neighbours().into_iter().filter(|x| x.id() > v.id()) {
            let u_to_v = match graph.edge(u, v.id(), None) {
                Some(edge) => {
                    let r = edge
                        .explode()
                        .map(|e| new_triangle_edge(true, 1, 0, 1, e.time().unwrap()))
                        .collect::<Vec<TriangleEdge>>();
                    r.into_iter()
                }
                None => vec![].into_iter(),
            };
            let v_to_u = match graph.edge(v.id(), u, None) {
                Some(edge) => {
                    let r = edge
                        .explode()
                        .map(|e| new_triangle_edge(true, 0, 0, 0, e.time().unwrap()))
                        .collect::<Vec<TriangleEdge>>();
                    r.into_iter()
                }
                None => vec![].into_iter(),
            };
            let mut tri_edges: Vec<TriangleEdge> = Vec::new();
            let out = graph.edge(v.id(), nb.id(), None);
            let inc = graph.edge(nb.id(), v.id(), None);
            // The following code checks for triangles
            match (out, inc) {
                (Some(o), Some(i)) => {
                    tri_edges.append(
                        &mut o
                            .explode()
                            .map(|e| new_triangle_edge(false, 1, nb_ct, 1, e.time().unwrap()))
                            .collect::<Vec<TriangleEdge>>(),
                    );
                    tri_edges.append(
                        &mut i
                            .explode()
                            .map(|e| new_triangle_edge(false, 1, nb_ct, 0, e.time().unwrap()))
                            .collect::<Vec<TriangleEdge>>(),
                    );
                }
                (Some(o), None) => {
                    tri_edges.append(
                        &mut o
                            .explode()
                            .map(|e| new_triangle_edge(false, 1, nb_ct, 1, e.time().unwrap()))
                            .collect::<Vec<TriangleEdge>>(),
                    );
                }
                (None, Some(i)) => {
                    tri_edges.append(
                        &mut i
                            .explode()
                            .map(|e| new_triangle_edge(false, 1, nb_ct, 0, e.time().unwrap()))
                            .collect::<Vec<TriangleEdge>>(),
                    );
                }
                (None, None) => {
                    continue;
                }
            }
            if !tri_edges.is_empty() {
                let uout = graph.edge(u, nb.id(), None);
                let uin = graph.edge(nb.id(), u, None);
                match (uout, uin) {
                    (Some(o), Some(i)) => {
                        tri_edges.append(
                            &mut o
                                .explode()
                                .map(|e| new_triangle_edge(false, 0, nb_ct, 1, e.time().unwrap()))
                                .collect::<Vec<TriangleEdge>>(),
                        );
                        tri_edges.append(
                            &mut i
                                .explode()
                                .map(|e| new_triangle_edge(false, 0, nb_ct, 0, e.time().unwrap()))
                                .collect::<Vec<TriangleEdge>>(),
                        );
                    }
                    (Some(o), None) => {
                        tri_edges.append(
                            &mut o
                                .explode()
                                .map(|e| new_triangle_edge(false, 0, nb_ct, 1, e.time().unwrap()))
                                .collect::<Vec<TriangleEdge>>(),
                        );
                    }
                    (None, Some(i)) => {
                        tri_edges.append(
                            &mut i
                                .explode()
                                .map(|e| new_triangle_edge(false, 0, nb_ct, 0, e.time().unwrap()))
                                .collect::<Vec<TriangleEdge>>(),
                        );
                    }
                    (None, None) => {
                        continue;
                    }
                }
                nb_ct += 1;
                // found triangle at this point!!
                tri_edges.append(&mut u_to_v.collect::<Vec<TriangleEdge>>());
                tri_edges.append(&mut v_to_u.collect::<Vec<TriangleEdge>>());
                tri_edges.sort_by_key(|e| e.time);

                let mut tri_count = init_tri_count(nb_ct);
                tri_count.execute(&tri_edges, delta);
                let tmp_counts: Iter<usize> = tri_count.return_counts().iter();

                update_counter(vec![evv, &v, &nb], motif_counter, tmp_counts);
            }
        }
    }
}

// works fine for 1 shard but breaks on more shard
// v1 - shard1, v2,v3 - shard2
// distributed acc
// v1 -> v2 (sending new count v2/)
// v2 -> v1 (sending new count v1) 5, 6 (motif 3 c)

// per vertex (motif counts)
// every iteration (sum them up)
// A -> B -> C-> A (motif 3)

// A: [1, 2, 3(1), 4, 5, 6, 7, 8], B: [1, 2, 3(1), 4, 5, 6, 7, 8], C: [1, 2, 3(1), 4, 5, 6, 7, 8] - shard1
// A: [1, 2, 3(0), 4, 5, 6, 7, 8], B: [1, 2, 3(0), 4, 5, 6, 7, 8], C: [1, 2, 3(0), 4, 5, 6, 7, 8] - shard2
// A: [1, 2, 3(0), 4, 5, 6, 7, 8], B: [1, 2, 3(0), 4, 5, 6, 7, 8], C: [1, 2, 3(0), 4, 5, 6, 7, 8] - shard3

// global acc
// 1, 2, 3, 4, 5, 6, 7, 8

fn update_counter<G: GraphViewOps>(
    vs: Vec<&EvalVertexView<G, ComputeStateVec, MotifCounter>>,
    motif_counter: AccId<MotifCounter, MotifCounter, MotifCounter, ValDef<MotifCounter>>,
    tmp_counts: Iter<usize>,
) {
    for v in vs {
        let mc = v.read(&motif_counter);
        let triangle: [usize; 8] = mc
            .triangle
            .iter()
            .zip(tmp_counts.clone())
            .map(|(&i1, &i2)| i1 + i2)
            .collect::<Vec<usize>>()
            .try_into()
            .unwrap();
        v.update(
            &motif_counter,
            MotifCounter::from_triangle_counter(triangle),
        );
    }
}

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
    let mut ctx: Context<G, ComputeStateVec> = g.into();
    let motifs_counter = val::<MotifCounter>(0);

    ctx.agg(motifs_counter);

    let step1 = ATask::new(
        move |evv: &mut EvalVertexView<G, ComputeStateVec, MotifCounter>| {
            let g = evv.graph;

            triangle_motif_count(g, evv, delta, motifs_counter);
            let two_nodes = twonode_motif_count(g, evv, delta);
            let star_nodes = star_motif_count(evv, delta);

            *evv.get_mut() = MotifCounter::new(two_nodes, star_nodes, evv.get().triangle);

            Step::Continue
        },
    );

    let mut runner: TaskRunner<G, _> = TaskRunner::new(ctx);

    runner.run(
        vec![],
        vec![Job::new(step1)],
        MotifCounter::zero(),
        |_, _, els, _| {
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
    use crate::db::mutation_api::AdditionOps;

    fn load_graph(edges: Vec<(i64, u64, u64)>) -> Graph {
        let graph = Graph::new();

        for (t, src, dst) in edges {
            graph.add_edge(t, src, dst, [], None).unwrap();
        }
        graph
    }

    #[test]
    #[ignore = "This is not correct, it needs a rethink of the algorithm to be parallel"]
    fn test_two_node_motif() {
        let g = load_graph(vec![
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
        ]);

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
