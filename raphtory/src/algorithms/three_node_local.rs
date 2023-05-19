use std::collections::HashMap;
use std::slice::Iter;

use crate::algorithms::motifs::MotifCounter;
use crate::algorithms::three_node_motifs::*;
use crate::core::agg::ValDef;
use crate::core::state::accumulator_id::AccId;
use crate::core::state::compute_state::ComputeStateVec;
use crate::db::task::eval_vertex::EvalVertexView;
use crate::db::view_api::*;

pub fn star_motif_count<G: GraphViewOps>(
    evv: &EvalVertexView<G, ComputeStateVec>,
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
    evv: &EvalVertexView<G, ComputeStateVec>,
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
    evv: &EvalVertexView<G, ComputeStateVec>,
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

fn update_counter<G: GraphViewOps>(
    vs: Vec<&EvalVertexView<G, ComputeStateVec>>,
    motif_counter: AccId<MotifCounter, MotifCounter, MotifCounter, ValDef<MotifCounter>>,
    tmp_counts: Iter<usize>,
) {
    for v in vs {
        let mc = v.read_local(&motif_counter);
        let triangle: [usize; 8] = mc
            .triangle
            .iter()
            .zip(tmp_counts.clone())
            .map(|(&i1, &i2)| i1 + i2)
            .collect::<Vec<usize>>()
            .try_into()
            .unwrap();
        v.update_local(
            &motif_counter,
            MotifCounter::from_triangle_counter(triangle),
        );
    }
}
