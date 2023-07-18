// Imports ///////////////////////////////////////////
use crate::{
    algorithms::{motifs::three_node_motifs::*, k_core::k_core_set},
    core::state::{accumulator_id::{accumulators::{self, val}, AccId}, compute_state::ComputeStateVec, agg::ValDef},
    db::{
        api::view::{GraphViewOps, VertexViewOps, *},
        task::{
            context::Context,
            task::{ATask, Job, Step},
            task_runner::TaskRunner,
            vertex::eval_vertex::EvalVertexView,
        }, graph::views::vertex_subgraph::{self, VertexSubgraph},
    },
};

use num_traits::Zero;
use rustc_hash::FxHashSet;
use std::{collections::HashMap, ops::Add, slice::Iter};
///////////////////////////////////////////////////////

// State objects for three node motifs
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

///////////////////////////////////////////////////////

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

///////////////////////////////////////////////////////

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

///////////////////////////////////////////////////////

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
        let mut all_exploded : Vec<TwoNodeEvent> = out.iter()
            .flat_map(|e| e.explode())
            .chain(inc.iter().flat_map(|e| e.explode()))
            .map(|e| {
                two_node_event(
                    if e.src().id() == evv.id() { 1 } else { 0 },
                    e.time().unwrap(),
                )
            })
            .collect();
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

///////////////////////////////////////////////////////

pub fn triangle_motifs<G: GraphViewOps>(graph: &G, delta:i64, threads: Option<usize>) {
    let vertex_set = k_core_set(graph,2,usize::MAX, None);
    let g = graph.subgraph(vertex_set);
    let mut ctx: Context<VertexSubgraph<G>, ComputeStateVec> = Context::from(&g);

    let motifs_counter = val::<MotifCounter>(0);
    ctx.agg(motifs_counter);

    let neighbours_set = accumulators::hash_set::<u64>(0);

    ctx.agg(neighbours_set);

    let step1 = ATask::new(move |u| {
        for v in u.neighbours() {
            if u.id() > v.id() {
                v.update(&neighbours_set, u.id());
            }
        }
        Step::Continue
    });

    let step2 = ATask::new(move |u| {
        for v in u.neighbours() {

            // Find triangles on the UV edge
            if u.id() > v.id() {
                let intersection_nbs = {
                    // when using entry() we need to make sure the reference is released before we can update the state, otherwise we break the Rc<RefCell<_>> invariant
                    // where there can either be one mutable or many immutable references

                    match (
                        u.entry(&neighbours_set)
                            .read_ref()
                            .unwrap_or(&FxHashSet::default()),
                        v.entry(&neighbours_set)
                            .read_ref()
                            .unwrap_or(&FxHashSet::default()),
                    ) {
                        (u_set, v_set) => {
                            let intersection = u_set.intersection(v_set);
                            intersection
                        }
                    }
                };

                if !intersection_nbs.peekable().peek().is_none() { continue; }
                let mut nb_ct = 0;
                intersection_nbs.for_each(|w| {
                    // For each triangle, run the triangle count.
                    let mut tri_edges: Vec<TriangleEdge> = Vec::new();

                    let u_to_v = match graph.edge(u.id(), v.id(), None) {
                        Some(edge) => {
                            let r = edge
                                .explode()
                                .map(|e| new_triangle_edge(true, 1, 0, 1, e.time().unwrap()))
                                .collect::<Vec<TriangleEdge>>();
                            r.into_iter()
                        }
                        None => vec![].into_iter(),
                    };
                    let v_to_u = match graph.edge(v.id(), u.id(), None) {
                        Some(edge) => {
                            let r = edge
                                .explode()
                                .map(|e| new_triangle_edge(true, 0, 0, 0, e.time().unwrap()))
                                .collect::<Vec<TriangleEdge>>();
                            r.into_iter()
                        }
                        None => vec![].into_iter(),
                    };

                    let uout = graph.edge(u.id(), *w, None);
                    let uin = graph.edge(*w, u.id(), None);
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
                        }
                    }

                    let vout = graph.edge(v.id(), *w, None);
                    let vin = graph.edge(*w, v.id(), None);
                    // The following code checks for triangles
                    match (vout, vin) {
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
                        (None, None) => {}
                    }
                    nb_ct += 1;

                    tri_edges.append(&mut u_to_v.collect::<Vec<TriangleEdge>>());
                    tri_edges.append(&mut v_to_u.collect::<Vec<TriangleEdge>>());
                    tri_edges.sort_by_key(|e| e.time);

                    let mut tri_count = init_tri_count(nb_ct);
                    tri_count.execute(&tri_edges, delta);
                    let tmp_counts: Iter<usize> = tri_count.return_counts().iter();

                    // Triangle counts are going to be WRONG without w
                    update_counter(vec![u, &v], motifs_counter, tmp_counts);
                })
            }
        }
        Step::Continue
    });

}