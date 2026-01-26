// Imports ///////////////////////////////////////////
use crate::{
    algorithms::{cores::k_core::k_core_set, motifs::three_node_motifs::*},
    core::state::{accumulator_id::accumulators, compute_state::ComputeStateVec},
    db::{
        api::{
            state::{GenericNodeState, TypedNodeState},
            view::{internal::GraphView, NodeViewOps, *},
        },
        graph::views::node_subgraph::NodeSubgraph,
        task::{
            context::Context,
            node::eval_node::EvalNodeView,
            task::{ATask, Job, Step},
            task_runner::TaskRunner,
        },
    },
};
use itertools::Itertools;
use num_traits::Zero;
use raphtory_api::core::{entities::VID, storage::timeindex::AsTime};
use rayon::prelude::*;
use rustc_hash::FxHashSet;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, mem, ops::Add, slice::Iter};
use tracing::debug;
///////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug, Default)]
pub struct MotifState {
    pub motif_counter: Vec<usize>,
}

// State objects for three node motifs
#[derive(Eq, PartialEq, Clone, Debug)]
pub struct MotifCounter {
    pub two_nodes: Vec<[usize; 8]>,
    pub star_nodes: Vec<[usize; 24]>,
    pub triangle: Vec<[usize; 8]>,
}

impl MotifCounter {
    fn new(
        size: usize,
        two_nodes: Vec<[usize; 8]>,
        star_nodes: Vec<[usize; 24]>,
        triangle: Vec<[usize; 8]>,
    ) -> Self {
        let _ = size;
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
            two_nodes: vec![],
            star_nodes: vec![],
            triangle: vec![],
        }
    }

    fn set_zero(&mut self) {
        *self = Zero::zero();
    }

    fn is_zero(&self) -> bool {
        self.two_nodes.is_empty() && self.star_nodes.is_empty() && self.triangle.is_empty()
    }
}

///////////////////////////////////////////////////////

pub fn star_motif_count<'graph, G>(
    evv: &EvalNodeView<'graph, '_, G, MotifCounter>,
    deltas: Vec<i64>,
) -> Vec<[usize; 24]>
where
    G: GraphView + 'graph,
{
    let neigh_map: HashMap<VID, usize> = evv
        .neighbours()
        .into_iter()
        .enumerate()
        .map(|(num, nb)| (nb.node, num))
        .collect();
    let events = evv
        .edges()
        .iter()
        .filter_map(|e| {
            if e.src().node != e.dst().node {
                Some(e.explode())
            } else {
                None
            }
        })
        .kmerge_by(|e1, e2| e1.time_and_event_id().unwrap() < e2.time_and_event_id().unwrap())
        .map(|edge| {
            if edge.src().node == evv.node {
                star_event(neigh_map[&edge.dst().node], 1, edge.time().unwrap().t())
            } else {
                star_event(neigh_map[&edge.src().node], 0, edge.time().unwrap().t())
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
        .collect::<Vec<[usize; 24]>>()
}

///////////////////////////////////////////////////////

pub fn twonode_motif_count<'a, 'b, G>(
    evv: &'a EvalNodeView<'b, '_, G, MotifCounter>,
    deltas: Vec<i64>,
) -> Vec<[usize; 8]>
where
    G: GraphViewOps<'b>,
    'b: 'a,
{
    let mut results = deltas.iter().map(|_| [0; 8]).collect::<Vec<[usize; 8]>>();

    for nb in evv.neighbours().into_iter() {
        let nb_id = nb.node;
        let out = evv.graph().edge(evv.node, nb_id);
        let inc = evv.graph().edge(nb_id, evv.node);
        let events: Vec<TwoNodeEvent> = out
            .iter()
            .flat_map(|e| e.explode())
            .merge_by(inc.iter().flat_map(|e| e.explode()), |e1, e2| {
                e1.time_and_event_id().unwrap() < e2.time_and_event_id().unwrap()
            })
            .filter_map(|e| {
                if e.src().node != e.dst().node {
                    Some(two_node_event(
                        if e.src().node == evv.node { 1 } else { 0 },
                        e.time().unwrap().t(),
                    ))
                } else {
                    None
                }
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

pub fn triangle_motifs<G>(
    graph: &G,
    deltas: Vec<i64>,
    threads: Option<usize>,
) -> HashMap<String, Vec<[usize; 8]>>
where
    G: StaticGraphViewOps,
{
    let delta_len = deltas.len();

    // Create K-Core graph to recursively remove nodes of degree < 2
    let node_set = k_core_set(graph, 2, usize::MAX, None);
    let kcore_subgraph: NodeSubgraph<G> = graph.subgraph(node_set);
    let mut ctx_subgraph: Context<NodeSubgraph<G>, ComputeStateVec> =
        Context::from(&kcore_subgraph);

    // Triangle Accumulator
    let neighbours_set = accumulators::hash_set::<VID>(1);
    ctx_subgraph.agg(neighbours_set);

    let neighbourhood_update_step = ATask::new(move |u: &mut EvalNodeView<_, MotifCounter>| {
        for v in u.neighbours() {
            v.update(&neighbours_set, u.node);
        }
        Step::Continue
    });

    let intersection_compute_step = ATask::new(move |u: &mut EvalNodeView<_, MotifCounter>| {
        let uu = u.get_mut();
        if uu.triangle.is_empty() {
            uu.triangle = vec![[0usize; 8]; delta_len];
        }
        for v in u.neighbours() {
            // Find triangles on the UV edge
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
                        let intersection = u_set.intersection(v_set).cloned().collect::<Vec<_>>();
                        intersection
                    }
                }
            };

            if intersection_nbs.is_empty() {
                continue;
            }

            intersection_nbs.iter().for_each(|w| {
                if w > &v.node {
                    // For each triangle, run the triangle count.

                    let all_exploded = vec![u.node, v.node, *w]
                        .into_iter()
                        .sorted()
                        .permutations(2)
                        .map(|e| {
                            u.graph()
                                .edge(*e.first().unwrap(), *e.get(1).unwrap())
                                .iter()
                                .flat_map(|edge| edge.explode())
                                .collect::<Vec<_>>()
                        })
                        .kmerge_by(|e1, e2| {
                            e1.time_and_event_id().unwrap() < e2.time_and_event_id().unwrap()
                        })
                        .map(|e| {
                            let (src_id, dst_id) = (e.src().node, e.dst().node);
                            let (uid, _vid) = (u.node, v.node);
                            if src_id == *w {
                                new_triangle_edge(
                                    false,
                                    if dst_id == uid { 0 } else { 1 },
                                    0,
                                    0,
                                    e.time().unwrap().t(),
                                )
                            } else if dst_id == *w {
                                new_triangle_edge(
                                    false,
                                    if src_id == uid { 0 } else { 1 },
                                    0,
                                    1,
                                    e.time().unwrap().t(),
                                )
                            } else if src_id == uid {
                                new_triangle_edge(true, 1, 0, 1, e.time().unwrap().t())
                            } else {
                                new_triangle_edge(true, 0, 0, 0, e.time().unwrap().t())
                            }
                        })
                        .collect::<Vec<TriangleEdge>>();

                    deltas.iter().enumerate().for_each(|(mc, delta)| {
                        let mut tri_count = init_tri_count(2);
                        tri_count.execute(&all_exploded, *delta);
                        let intermediate_counts: Iter<usize> = tri_count.return_counts().iter();
                        let mc_u = u.get_mut();
                        let triangle_u = mc_u.triangle[mc]
                            .iter()
                            .zip(intermediate_counts.clone())
                            .map(|(&i1, &i2)| i1 + i2)
                            .collect::<Vec<usize>>()
                            .try_into()
                            .unwrap();
                        mc_u.triangle[mc] = triangle_u;
                    })
                }
            })
        }
        Step::Continue
    });

    let mut runner: TaskRunner<NodeSubgraph<G>, _> = TaskRunner::new(ctx_subgraph);

    runner.run(
        vec![Job::new(neighbourhood_update_step)],
        vec![Job::new(intersection_compute_step)],
        None,
        |_, _, _els, mut local| {
            let mut tri_motifs = HashMap::new();
            for node in graph.nodes() {
                let v_gid = node.name();
                let triangle = mem::take(&mut local[node.node.0].triangle);
                if triangle.is_empty() {
                    tri_motifs.insert(v_gid.clone(), vec![[0; 8]; delta_len]);
                } else {
                    tri_motifs.insert(v_gid.clone(), triangle);
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
/// Computes the number of each type of motif that each node participates in. See global_temporal_three_node_motifs for a summary of the motifs involved.
///
/// # Arguments
/// - `g`: A directed raphtory graph
/// - `delta`: Maximum time difference between the first and last edge of the motif. NB if time for edges was given as a UNIX epoch, this should be given in seconds, otherwise milliseconds should be used (if edge times were given as string)
///
/// # Returns
/// An [AlgorithmResult] mapping each node to a 40d array of motif counts as values (in the same order as the global motif counts) with the number of each motif that node participates in.
///
/// # Notes
/// For this local count, a node is counted as participating in a motif in the following way. For star motifs, only the centre node counts
/// the motif. For two node motifs, both constituent nodes count the motif. For triangles, all three constituent nodes count the motif.
pub fn temporal_three_node_motif<G>(
    g: &G,
    delta: i64,
    threads: Option<usize>,
) -> TypedNodeState<'static, MotifState, G>
where
    G: StaticGraphViewOps,
{
    let ctx: Context<G, ComputeStateVec> = g.into();

    debug!("Running triangle step");
    let triadic_motifs = triangle_motifs(g, vec![delta], threads);
    debug!("Running rest of motifs");

    let star_motif_step = ATask::new(move |evv: &mut EvalNodeView<G, MotifCounter>| {
        let two_nodes = twonode_motif_count(evv, vec![delta]);
        let star_nodes = star_motif_count(evv, vec![delta]);

        *evv.get_mut() = MotifCounter::new(1, two_nodes, star_nodes, evv.get().triangle.clone());

        Step::Continue
    });

    let mut runner: TaskRunner<G, _> = TaskRunner::new(ctx);

    runner.run(
        vec![Job::new(star_motif_step)],
        vec![],
        None,
        |_, _, _, local| {
            let values: Vec<_> = g
                .nodes()
                .par_iter()
                .map(|n| {
                    let mc = &local[n.node.index()];
                    let v_gid = n.name();
                    let triangles = triadic_motifs
                        .get(&v_gid)
                        .map(|t| &t[0])
                        .unwrap_or_else(|| &[0usize; 8]);
                    let two_nodes = &mc.two_nodes[0];
                    let mut counts: Vec<_> = mc.star_nodes[0]
                        .iter()
                        .zip(two_nodes.iter().cycle().take(24))
                        .map(|(&x1, &x2)| x1 - x2)
                        .collect();
                    counts.extend_from_slice(two_nodes);
                    counts.extend_from_slice(triangles);
                    counts
                })
                .map(|value| MotifState {
                    motif_counter: value,
                })
                .collect();
            TypedNodeState::new(GenericNodeState::new_from_eval(g.clone(), values, None))
        },
        threads,
        1,
        None,
        None,
    )
}
