// Imports ///////////////////////////////////////////
use crate::{
    algorithms::{k_core::k_core_set, motifs::three_node_motifs::*},
    core::state::{
        accumulator_id::{
            accumulators::{self, val},
            AccId,
        },
        agg::ValDef,
        compute_state::ComputeStateVec,
    },
    db::{
        api::view::{GraphViewOps, VertexViewOps, *},
        graph::{views::vertex_subgraph::VertexSubgraph, edge::EdgeView},
        task::{
            context::Context,
            task::{ATask, Job, Step},
            task_runner::TaskRunner,
            vertex::eval_vertex::EvalVertexView,
        },
    },
};

use crate::core::entities::vertices::vertex_ref::VertexRef;
use rand::prelude::*;
use rand::{rngs::StdRng, SeedableRng, Rng, seq::SliceRandom};
use futures::io::empty;
use itertools::{enumerate, Combinations, Itertools};
use num_traits::Zero;
use rustc_hash::FxHashSet;
use std::{collections::HashMap, ops::Add, slice::Iter};
///////////////////////////////////////////////////////

// State objects for three node motifs
#[derive(Eq, PartialEq, Clone, Debug)]
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
}

impl Default for MotifCounter {
    fn default() -> Self {
        Self::zero()
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

///////////////////////////////////////////////////////

// fn update_counter<G: GraphViewOps>(
//     vs: &mut Vec<&EvalVertexView<G, ComputeStateVec, MotifCounter>>,
//     motif_counter: AccId<MotifCounter, MotifCounter, MotifCounter, ValDef<MotifCounter>>,
//     tmp_counts: Iter<usize>,
// ) {
//     for mut v in vs {
//         let mc = v.get_mut();
//         let triangle: [usize; 8] = mc
//             .triangle
//             .iter()
//             .zip(tmp_counts.clone())
//             .map(|(&i1, &i2)| i1 + i2)
//             .collect::<Vec<usize>>()
//             .try_into()
//             .unwrap();
//         v.update(
//             &motif_counter,
//             MotifCounter::from_triangle_counter(triangle),
//         );
//         println!("{:?}",v.read(&motif_counter))
//     }
// }

///////////////////////////////////////////////////////

pub fn star_motif_count<G: GraphViewOps>(
    evv: &EvalVertexView<G, ComputeStateVec, MotifCounter>,
    delta: i64,
) -> [usize; 24] {
    let mut rng = StdRng::from_entropy();
    let neigh_map: HashMap<u64, usize> = evv
        .neighbours()
        .into_iter()
        .enumerate()
        .map(|(num, nb)| (nb.id(), num))
        .into_iter()
        .collect();
    let exploded_edges = evv
        .edges()
        .explode()
        .sorted_by_key(|e| (e.time(), e.src().id(), e.dst().id()))
        // .sorted_unstable_by_key(|e| (e.time(), rng.gen_range(0..10000000)))
        .map(|edge| {
            if edge.src().id() == evv.id() {
                star_event(neigh_map[&edge.dst().id()], 1, edge.time().unwrap())
            } else {
                star_event(neigh_map[&edge.src().id()], 0, edge.time().unwrap())
            }
        })
        .collect::<Vec<StarEvent>>();
    let mut star_count = init_star_count(neigh_map.len());
    star_count.execute(&exploded_edges, delta);
    star_count.return_counts()
}

///////////////////////////////////////////////////////

pub fn twonode_motif_count<G: GraphViewOps>(
    graph: &G,
    evv: &EvalVertexView<G, ComputeStateVec, MotifCounter>,
    delta: i64,
) -> [usize; 8] {
    let mut rng = StdRng::from_entropy();
    let mut counts = [0; 8];
    for nb in evv.neighbours().into_iter() {
        let nb_id = nb.id();
        let out = graph.edge(evv.id(), nb_id);
        let inc = graph.edge(nb_id, evv.id());
        let all_exploded: Vec<TwoNodeEvent> = out
            .iter()
            .flat_map(|e| e.explode())
            .chain(inc.iter().flat_map(|e| e.explode()))
            .sorted_by_key(|e| (e.time(), e.src().id(), e.dst().id()))
            // .sorted_unstable_by_key(|e| (e.time(), rng.gen_range(0..10000000)))
            .map(|e| {
                two_node_event(
                    if e.src().id() == evv.id() { 1 } else { 0 },
                    e.time().unwrap(),
                )
            })
            .collect();
        let mut two_node_counter = init_two_node_count();
        two_node_counter.execute(&all_exploded, delta);
        let two_node_result = two_node_counter.return_counts();
        for i in 0..8 {
            counts[i] += two_node_result[i];
        }
    }
    counts
}

///////////////////////////////////////////////////////

pub fn triangle_motifs<G: GraphViewOps>(
    graph: &G,
    delta: i64,
    _motifs_count_id: AccId<MotifCounter, MotifCounter, MotifCounter, ValDef<MotifCounter>>,
    threads: Option<usize>,
) -> HashMap<String, [usize; 8]> {
    let vertex_set = k_core_set(graph, 2, usize::MAX, None);
    let g: VertexSubgraph<G> = graph.subgraph(vertex_set);
    let mut ctx: Context<VertexSubgraph<G>, ComputeStateVec> = Context::from(&g);

    let neighbours_set = accumulators::hash_set::<u64>(1);

    ctx.agg(neighbours_set);

    let step1 = ATask::new(
        move |u: &mut EvalVertexView<VertexSubgraph<G>, ComputeStateVec, MotifCounter>| {
            for v in u.neighbours() {
                if u.id() > v.id() {
                    v.update(&neighbours_set, u.id());
                }
            }
            Step::Continue
        },
    );

    let step2 = ATask::new(
        move |u: &mut EvalVertexView<VertexSubgraph<G>, ComputeStateVec, MotifCounter>| {
            let mut rng = StdRng::from_entropy();
            for v in u.neighbours() {
                // Find triangles on the UV edge
                if u.id() > v.id() {
                    let intersection_nbs = {
                        match (
                            u.entry(&neighbours_set)
                                .read_ref()
                                .unwrap_or(&FxHashSet::default()),
                            v.entry(&neighbours_set)
                                .read_ref()
                                .unwrap_or(&FxHashSet::default()),
                        ) {
                            (u_set, v_set) => {
                                let intersection =
                                    u_set.intersection(v_set).cloned().collect::<Vec<_>>();
                                intersection
                            }
                        }
                    };

                    if intersection_nbs.is_empty() {
                        continue;
                    }
                    // let mut nb_ct = 0;
                    intersection_nbs.iter().for_each(|w| {
                        // For each triangle, run the triangle count.

                        let mut tmp = vec![u.id(), v.id(), *w]
                            .into_iter()
                            .sorted()
                            .permutations(2)
                            .collect_vec();

                        // tmp.shuffle(&mut rng);

                        let all_exploded = tmp
                            .iter()
                            .map(|e| {
                                g
                                    .edge(e.get(0).unwrap().clone(), e.get(1).unwrap().clone())
                                    .iter()
                                    .flat_map(|edge| edge.explode())
                                    .collect::<Vec<_>>()
                            })
                            .kmerge_by(|e1,e2| e1.time() < e2.time())
                            .map(|e| {
                                let (src_id, dst_id) = (e.src().id(), e.dst().id());
                                let (uid, vid) = (u.id(), v.id());
                                if src_id==w.clone() {
                                    new_triangle_edge(false, if dst_id==uid {0} else {1}, 0, 0, e.time().unwrap())
                                } else if dst_id == w.clone() {
                                    new_triangle_edge(false, if src_id==uid {0} else {1}, 0, 1, e.time().unwrap())
                                } else if src_id == uid {
                                    new_triangle_edge(true, 1, 0, 1, e.time().unwrap())
                                } else {
                                    new_triangle_edge(true, 0, 0, 0, e.time().unwrap())
                                }
                                // let uv_edge = (e.src().id()==u.id() && e.dst().id()==v.id()) || (e.src().id() == v.id() && e.dst().id() == u.id());
                                // if uv_edge {
                                //     if e.src().id()==u.id() {
                                //     new_triangle_edge(true, 1, 0, 1, e.time().unwrap())
                                // } else {new_triangle_edge(true, 0, 0, 0, e.time().unwrap())}
                                // } else {
                                //     if e.src().id() == u.id() || e.dst().id() == u.id() {
                                //         new_triangle_edge(false, 0, 0, if e.src().id() == u.id() {1} else {0}, e.time().unwrap())
                                //     } else {new_triangle_edge(false, 1, 0, if e.src().id() == v.id() {1} else {0}, e.time().unwrap())}
                                // }
                            }).collect::<Vec<TriangleEdge>>();

                        let mut tri_count = init_tri_count(2);
                        tri_count.execute(&all_exploded, delta);
                        let tmp_counts: Iter<usize> = tri_count.return_counts().iter();

                        // Triangle counts are going to be WRONG without w
                        // update_counter(&mut vec![u, &v], motifs_count_id, tmp_counts);

                        let mc_u = u.get_mut();
                        let triangle_u = mc_u
                            .triangle
                            .iter()
                            .zip(tmp_counts.clone())
                            .map(|(&i1, &i2)| i1 + i2)
                            .collect::<Vec<usize>>()
                            .try_into()
                            .unwrap();
                        mc_u.triangle = triangle_u;

                    })
                }
            }
            Step::Continue
        },
    );

    let mut runner: TaskRunner<VertexSubgraph<G>, _> = TaskRunner::new(ctx);

    runner.run(
        vec![Job::new(step1)],
        vec![Job::read_only(step2)],
        None,
        |_, _, _els, local| {
            let mut tri_motifs = HashMap::new();
            let layers = graph.layer_ids();
            let edge_filter = graph.edge_filter();
            for (vref, mc) in enumerate(local) {
                if graph.has_vertex_ref(VertexRef::Internal(vref.into()), &layers, edge_filter) {
                    let v_gid = graph.vertex_name(vref.into());
                    tri_motifs.insert(v_gid.clone(), mc.triangle);
                }
            }
            tri_motifs
        },
        threads,
        1,
        None,
        None,
    )
}

///////////////////////////////////////////////////////
///////////////////////////////////////////////////////

pub fn temporal_three_node_motif<G: GraphViewOps>(
    g: &G,
    delta: i64,
    threads: Option<usize>,
) -> HashMap<String, Vec<usize>> {
    let mut ctx: Context<G, ComputeStateVec> = g.into();
    let motifs_counter = val::<MotifCounter>(0);

    ctx.agg(motifs_counter);

    let out1 = triangle_motifs(g, delta, motifs_counter, threads);
    let step1 = ATask::new(
        move |evv: &mut EvalVertexView<G, ComputeStateVec, MotifCounter>| {
            let g = evv.graph;

            let two_nodes = twonode_motif_count(g, evv, delta);
            let star_nodes = star_motif_count(evv, delta);

            *evv.get_mut() = MotifCounter::new(two_nodes, star_nodes, evv.get().triangle);

            Step::Continue
        },
    );

    let mut runner: TaskRunner<G, _> = TaskRunner::new(ctx);

    let out2 = runner.run(
        vec![Job::new(step1)],
        vec![],
        None,
        |_, _, _els, local| {
            let mut motifs = HashMap::new();
            for (vref, mc) in enumerate(local) {
                let v_gid = g.vertex_name(vref.into());
                let triangles = out1.get(&v_gid).unwrap_or_else(|| &[0; 8]).to_vec();
                let two_nodes = mc.two_nodes.to_vec();
                let tmp_stars = mc.star_nodes.to_vec();
                let stars: Vec<usize> = tmp_stars
                    .iter()
                    .zip(two_nodes.iter().cycle().take(24))
                    .map(|(&x1, &x2)| x1 - x2)
                    .collect();
                let mut final_cts = Vec::new();
                final_cts.extend(stars.into_iter());
                final_cts.extend(two_nodes.into_iter());
                final_cts.extend(triangles.into_iter());
                motifs.insert(v_gid.clone(), final_cts);
            }
            motifs
        },
        threads,
        1,
        None,
        None,
    );
    out2
}

pub fn global_temporal_three_node_motif_from_local(
    counts: HashMap<String, Vec<usize>>,
) -> Vec<usize> {
    let tmp_counts = counts.values().fold(vec![0; 40], |acc, x| {
        acc.iter().zip(x.iter()).map(|(x1, x2)| x1 + x2).collect()
    });
    // for ind in 31..40 {
    //     tmp_counts[ind] = tmp_counts[ind] / 2;
    // }
    tmp_counts
}

pub fn global_temporal_three_node_motif<G: GraphViewOps>(
    graph: &G,
    delta: i64,
    threads: Option<usize>,
) -> Vec<usize> {
    let counts = temporal_three_node_motif(graph, delta, threads);
    let tmp_counts = counts.values().fold(vec![0; 40], |acc, x| {
        acc.iter().zip(x.iter()).map(|(x1, x2)| x1 + x2).collect()
    });
    // for ind in 31..40 {
    //     tmp_counts[ind] = tmp_counts[ind] / 3;
    // }
    tmp_counts
}

#[cfg(test)]
mod motifs_test {
    use super::*;
    use crate::{
        db::{api::mutation::AdditionOps, graph::graph::Graph},
        prelude::NO_PROPS,
    };

    fn load_graph(edges: Vec<(i64, u64, u64)>) -> Graph {
        let graph = Graph::new();

        for (t, src, dst) in edges {
            graph.add_edge(t, src, dst, NO_PROPS, None).unwrap();
        }
        graph
    }

    #[test]
    #[ignore = "This is not correct, local version does not work"]
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

        let actual = temporal_three_node_motif(&g, 10, None);

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

        for ind in 3..12 {
            assert_eq!(
                actual.get(&ind.to_string()).unwrap(),
                expected.get(&ind.to_string()).unwrap()
            );
        }
    }

    #[test]
    fn test_global() {
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

        let global_motifs = global_temporal_three_node_motif(&g, 10, None);
        assert_eq!(
            global_motifs,
            vec![
                0, 0, 3, 6, 2, 3, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 1, 6, 0, 0, 1, 7, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 2, 3, 2, 4, 1, 2, 3, 1
            ]
        );
    }
}
