// Imports ///////////////////////////////////////////
use crate::{
    algorithms::{k_core::k_core_set, motifs::three_node_motifs::*},
    core::state::{
        accumulator_id::{
            accumulators::{self, val},
            AccId,
        },
        agg::{ArrConst, SumDef, ValDef},
        compute_state::ComputeStateVec,
    },
    db::{
        api::view::{GraphViewOps, VertexViewOps, *},
        graph::{edge::EdgeView, views::vertex_subgraph::VertexSubgraph},
        task::{
            context::Context,
            edge::eval_edge::EvalEdgeView,
            task::{ATask, Job, Step},
            task_runner::TaskRunner,
            vertex::eval_vertex::EvalVertexView,
        },
    },
};

use std::sync::Arc;

use crate::core::entities::vertices::vertex_ref::VertexRef;
use itertools::{enumerate, Itertools};
use num_traits::Zero;
use rand::{rngs::StdRng, Rng, SeedableRng};
use rustc_hash::FxHashSet;
use std::{cmp::Ordering, collections::HashMap, ops::Add, slice::Iter};

///////////////////////////////////////////////////////

pub fn star_motif_count<G>(
    graph: &G,
    evv: &EvalVertexView<G, ComputeStateVec, ()>,
    deltas: Vec<i64>,
) -> Vec<[usize; 32]>
where
    G: GraphViewOps,
{
    let two_n_c = twonode_motif_count(graph, evv, deltas.clone());
    let neigh_map: HashMap<u64, usize> = evv
        .neighbours()
        .into_iter()
        .enumerate()
        .map(|(num, nb)| (nb.id(), num))
        .collect();
    let events = evv
        .edges()
        .explode()
        .sorted_by_key(|e| e.time_and_index())
        .map(|edge| {
            if edge.src().id() == evv.id() {
                star_event(neigh_map[&edge.dst().id()], 1, edge.time().unwrap())
            } else {
                star_event(neigh_map[&edge.src().id()], 0, edge.time().unwrap())
            }
        })
        .collect::<Vec<StarEvent>>();

    deltas
        .into_iter()
        .map(|delta| {
            let mut star_count = init_star_count(evv.degree());
            star_count.execute(&events, delta);
            star_count.return_counts()
        })
        .zip(two_n_c)
        .map(|(c1, c2)| {
            let mut tmp = c1
                .iter()
                .zip(c2.iter().cycle().take(24))
                .map(|(x1, x2)| x1 - x2)
                .collect_vec();
            tmp.extend(c2.iter());
            let cts: [usize; 32] = tmp.try_into().unwrap();
            cts
        })
        .collect::<Vec<[usize; 32]>>()
}

///////////////////////////////////////////////////////

pub fn twonode_motif_count<G>(
    graph: &G,
    evv: &EvalVertexView<G, ComputeStateVec, ()>,
    deltas: Vec<i64>,
) -> Vec<[usize; 8]>
where
    G: GraphViewOps,
{
    let mut results = deltas.iter().map(|_| [0; 8]).collect::<Vec<[usize; 8]>>();

    for nb in evv.neighbours().into_iter() {
        let nb_id = nb.id();
        let out = graph.edge(evv.id(), nb_id);
        let inc = graph.edge(nb_id, evv.id());
        let events: Vec<TwoNodeEvent> = out
            .iter()
            .flat_map(|e| e.explode())
            .chain(inc.iter().flat_map(|e| e.explode()))
            .sorted_by_key(|e| e.time_and_index())
            .map(|e| {
                two_node_event(
                    if e.src().id() == evv.id() { 1 } else { 0 },
                    e.time().unwrap(),
                )
            })
            .collect();
        for j in 0..deltas.len() {
            let mut two_node_counter = init_two_node_count();
            two_node_counter.execute(&events, deltas[j]);
            let two_node_result = two_node_counter.return_counts();
            for i in 0..8 {
                results[j][i] += two_node_result[i];
            }
        }
    }
    results
}

///////////////////////////////////////////////////////

pub fn triangle_motifs<G>(graph: &G, deltas: Vec<i64>, threads: Option<usize>) -> Vec<[usize; 8]>
where
    G: GraphViewOps,
{
    let mut ctx_g: Context<G, ComputeStateVec> = graph.into();
    let tri_mcc = deltas
        .iter()
        .map(|d| accumulators::arr::<usize, SumDef<usize>, 8>(2 * *d as u32))
        .collect_vec();

    tri_mcc.iter().for_each(|mc| {
        ctx_g.agg::<[usize; 8], [usize; 8], [usize; 8], ArrConst<usize, SumDef<usize>, 8>>(*mc)
    });

    // Define a closure for sorting by time()
    let vertex_set = k_core_set(graph, 2, usize::MAX, None);
    let g: VertexSubgraph<G> = graph.subgraph(vertex_set);
    let mut ctx_sub: Context<VertexSubgraph<G>, ComputeStateVec> = Context::from(&g);

    let neighbours_set = accumulators::hash_set::<u64>(0);

    ctx_sub.agg(neighbours_set);

    let tri_arc = Arc::new(tri_mcc);
    let tri_arc_clone = Arc::clone(&tri_arc);

    let step1 = ATask::new(
        move |u: &mut EvalVertexView<'_, VertexSubgraph<G>, ComputeStateVec, ()>| {
            for v in u.neighbours() {
                if u.id() > v.id() {
                    v.update(&neighbours_set, u.id());
                }
            }
            Step::Continue
        },
    );

    let step2 = ATask::new(
        move |u: &mut EvalVertexView<'_, VertexSubgraph<G>, ComputeStateVec, ()>| {
            let mut triangle_u = vec![[0 as usize; 8]; deltas.len()];
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

                        let all_exploded = vec![u.id(), v.id(), *w]
                            .into_iter()
                            .sorted()
                            .permutations(2)
                            .flat_map(|e| {
                                g.edge(e.get(0).unwrap().clone(), e.get(1).unwrap().clone())
                                    .iter()
                                    .flat_map(|edge| edge.explode())
                                    .collect::<Vec<_>>()
                            })
                            .sorted_by_key(|e| e.time_and_index())
                            .map(|e| {
                                let (src_id, dst_id) = (e.src().id(), e.dst().id());
                                let (uid, vid) = (u.id(), v.id());
                                if src_id == w.clone() {
                                    new_triangle_edge(
                                        false,
                                        if dst_id == uid { 0 } else { 1 },
                                        0,
                                        0,
                                        e.time().unwrap(),
                                    )
                                } else if dst_id == w.clone() {
                                    new_triangle_edge(
                                        false,
                                        if src_id == uid { 0 } else { 1 },
                                        0,
                                        1,
                                        e.time().unwrap(),
                                    )
                                } else if src_id == uid {
                                    new_triangle_edge(true, 1, 0, 1, e.time().unwrap())
                                } else {
                                    new_triangle_edge(true, 0, 0, 0, e.time().unwrap())
                                }
                            })
                            .collect::<Vec<TriangleEdge>>();

                        deltas.iter().enumerate().for_each(|(i, delta)| {
                            let mut tri_count = init_tri_count(2);
                            tri_count.execute(&all_exploded, *delta);
                            let tmp_counts: [usize; 8] = *tri_count.return_counts();
                            for j in 0..8 {
                                triangle_u[i][j] += tmp_counts[j];
                            }
                        })
                    })
                }
            }
            triangle_u
                .iter()
                .zip(tri_arc_clone.iter())
                .for_each(|(cts, mc)| u.global_update(mc, *cts));
            Step::Continue
        },
    );

    let mut runner: TaskRunner<VertexSubgraph<G>, _> = TaskRunner::new(ctx_sub);

    runner.run(
        vec![],
        vec![Job::new(step1),Job::new(step2)],
        None,
        |egs, _, _, _| {
            tri_arc.iter().map(|mc| egs.finalize::<[usize; 8], [usize;8], [usize; 8], ArrConst<usize,SumDef<usize>,8>>(mc)).collect_vec()
        },
        threads,
        2,
        None,
        None,
    )
}

///////////////////////////////////////////////////////

pub fn temporal_three_node_motif_multi<G>(
    g: &G,
    deltas: Vec<i64>,
    threads: Option<usize>,
) -> Vec<[usize; 40]>
where
    G: GraphViewOps,
{
    let mut ctx: Context<G, ComputeStateVec> = g.into();
    let star_mc = deltas
        .iter()
        .map(|d| accumulators::arr::<usize, SumDef<usize>, 32>((2 * *d + 1 as i64) as u32))
        .collect_vec();
    star_mc.iter().for_each(|mc| {
        ctx.agg::<[usize; 32], [usize; 32], [usize; 32], ArrConst<usize, SumDef<usize>, 32>>(*mc)
    });
    let star_arc = Arc::new(star_mc);
    let star_arc_clone = Arc::clone(&star_arc);

    let out1 = triangle_motifs(g, deltas.clone(), threads);

    let step1 = ATask::new(move |evv: &mut EvalVertexView<'_, G, ComputeStateVec, _>| {
        let g = evv.graph;

        let star_nodes = star_motif_count(g, evv, deltas.clone());
        for (star, mc) in star_nodes.iter().zip(star_arc.iter()) {
            evv.global_update(&mc, *star)
        }
        Step::Continue
    });

    let mut runner: TaskRunner<G, _> = TaskRunner::new(ctx);

    let out2 = runner.run(
        vec![],
        vec![Job::new(step1)],
        None,
        |egs, _ , _ , _ | {
            star_arc_clone.iter().zip(out1.iter()).map(|(mc,tri)| {
                let mut tmp = egs.finalize::<[usize; 32], [usize;32], [usize; 32], ArrConst<usize,SumDef<usize>,32>>(mc)
                .iter()
                .map(|x| *x)
                .collect_vec();
                tmp.extend(tri.iter());
                let motifs : [usize;40] = tmp
                .try_into()
                .unwrap();
                motifs
            }).collect_vec()
        },
        threads,
        1,
        None,
        None,
    );
    out2.clone()
}

pub fn global_temporal_three_node_motif<G: GraphViewOps>(
    graph: &G,
    delta: i64,
    threads: Option<usize>,
) -> [usize; 40] {
    let counts = temporal_three_node_motif_multi(graph, vec![delta], threads);
    counts[0].clone()
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
    fn test_global() {
        let g = load_graph(vec![
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

        let global_motifs = &temporal_three_node_motif_multi(&g, Vec::from([3, 6, 10]), None);

        let expected: [usize; 40] = vec![
            0, 2, 3, 8, 2, 4, 1, 5, 0, 0, 0, 0, 1, 0, 2, 0, 0, 1, 6, 0, 0, 1, 10, 2, 0, 1, 0, 0, 0,
            0, 1, 0, 2, 3, 2, 4, 1, 2, 4, 1,
        ]
        .into_iter()
        .map(|x| x as usize)
        .collect::<Vec<usize>>()
        .try_into()
        .unwrap();
        assert_eq!(global_motifs[2], expected);
    }
}
