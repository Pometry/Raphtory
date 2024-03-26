// Imports ///////////////////////////////////////////
use crate::{
    algorithms::{cores::k_core::k_core_set, motifs::three_node_motifs::*},
    core::state::{
        accumulator_id::{
            accumulators::{self, val},
            AccId,
        },
        agg::ValDef,
        compute_state::ComputeStateVec,
    },
    db::{
        api::view::{NodeViewOps, *},
        graph::{edge::EdgeView, views::node_subgraph::NodeSubgraph},
        task::{
            context::Context,
            node::eval_node::EvalNodeView,
            task::{ATask, Job, Step},
            task_runner::TaskRunner,
        },
    },
};

use itertools::{enumerate, Itertools};
use num_traits::Zero;
use rustc_hash::FxHashSet;
use std::{cmp::Ordering, collections::HashMap, mem, ops::Add, slice::Iter};
///////////////////////////////////////////////////////

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

pub fn star_motif_count<'graph, G, GH>(
    evv: &EvalNodeView<'graph, '_, G, MotifCounter, GH>,
    deltas: Vec<i64>,
) -> Vec<[usize; 24]>
where
    G: GraphViewOps<'graph>,
    GH: GraphViewOps<'graph>,
{
    let neigh_map: HashMap<u64, usize> = evv
        .neighbours()
        .into_iter()
        .enumerate()
        .map(|(num, nb)| (nb.id(), num))
        .collect();
    let events = evv
        .edges()
        .explode()
        .iter()
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
        .collect::<Vec<[usize; 24]>>()
}

///////////////////////////////////////////////////////

pub fn twonode_motif_count<'a, 'b, G, GH>(
    graph: &'a G,
    evv: &'a EvalNodeView<'b, '_, G, MotifCounter, GH>,
    deltas: Vec<i64>,
) -> Vec<[usize; 8]>
where
    G: GraphViewOps<'b>,
    GH: GraphViewOps<'b>,
    'b: 'a,
{
    let mut results = deltas.iter().map(|_| [0; 8]).collect::<Vec<[usize; 8]>>();

    // Define a closure for sorting by time_and_index()
    let _sort_by_time_and_index = |e1: &EdgeView<G, GH>, e2: &EdgeView<G, GH>| -> Ordering {
        Ord::cmp(&e1.time_and_index(), &e2.time_and_index())
    };

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

pub fn triangle_motifs<G>(
    graph: &G,
    deltas: Vec<i64>,
    _motifs_count_id: AccId<MotifCounter, MotifCounter, MotifCounter, ValDef<MotifCounter>>,
    threads: Option<usize>,
) -> HashMap<String, Vec<[usize; 8]>>
where
    G: StaticGraphViewOps,
{
    let delta_len = deltas.len();

    // Define a closure for sorting by time_and_index()
    let _sort_by_time_and_index =
        |e1: &EdgeView<NodeSubgraph<G>, NodeSubgraph<G>>,
         e2: &EdgeView<NodeSubgraph<G>, NodeSubgraph<G>>|
         -> Ordering { Ord::cmp(&e1.time_and_index(), &e2.time_and_index()) };

    // Define a closure for sorting by time()
    let node_set = k_core_set(graph, 2, usize::MAX, None);
    let g: NodeSubgraph<G> = graph.subgraph(node_set);
    let mut ctx: Context<NodeSubgraph<G>, ComputeStateVec> = Context::from(&g);

    let neighbours_set = accumulators::hash_set::<u64>(1);

    ctx.agg(neighbours_set);

    let step1 = ATask::new(move |u: &mut EvalNodeView<NodeSubgraph<G>, MotifCounter>| {
        for v in u.neighbours() {
            if u.id() > v.id() {
                v.update(&neighbours_set, u.id());
            }
        }
        Step::Continue
    });

    let step2 = ATask::new(move |u: &mut EvalNodeView<NodeSubgraph<G>, MotifCounter>| {
        let uu = u.get_mut();
        if uu.triangle.is_empty() {
            uu.triangle = vec![[0usize; 8]; delta_len];
        }
        for v in u.neighbours() {
            // Find triangles on the UV edge
            let intersection_nbs: Vec<u64> = match v.entry(&neighbours_set).read_ref() {
                Some(v_set) => {
                    let u_set = FxHashSet::from_iter(u.neighbours().id());
                    let intersection = u_set.intersection(v_set).cloned().collect::<Vec<_>>();
                    intersection
                }
                None => vec![],
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
                        g.edge(*e.first().unwrap(), *e.get(1).unwrap())
                            .iter()
                            .flat_map(|edge| edge.explode())
                            .collect::<Vec<_>>()
                    })
                    .sorted_by_key(|e| e.time_and_index())
                    .map(|e| {
                        let (src_id, dst_id) = (e.src().id(), e.dst().id());
                        let (uid, _vid) = (u.id(), v.id());
                        if src_id == *w {
                            new_triangle_edge(
                                false,
                                if dst_id == uid { 0 } else { 1 },
                                0,
                                0,
                                e.time().unwrap(),
                            )
                        } else if dst_id == *w {
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

                for i in 0..deltas.len() {
                    let delta = deltas[i];
                    let mut tri_count = init_tri_count(2);
                    tri_count.execute(&all_exploded, delta);
                    let tmp_counts: Iter<usize> = tri_count.return_counts().iter();

                    // Triangle counts are going to be WRONG without w
                    // update_counter(&mut vec![u, &v], motifs_count_id, tmp_counts);

                    let mc_u = u.get_mut();
                    let triangle_u = mc_u.triangle[i]
                        .iter()
                        .zip(tmp_counts.clone())
                        .map(|(&i1, &i2)| i1 + i2)
                        .collect::<Vec<usize>>()
                        .try_into()
                        .unwrap();
                    mc_u.triangle[i] = triangle_u;
                }
            })
        }
        Step::Continue
    });

    let mut runner: TaskRunner<NodeSubgraph<G>, _> = TaskRunner::new(ctx);

    runner.run(
        vec![Job::new(step1)],
        vec![Job::read_only(step2)],
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

pub fn temporal_three_node_motif<G>(
    g: &G,
    deltas: Vec<i64>,
    threads: Option<usize>,
) -> HashMap<String, Vec<Vec<usize>>>
where
    G: StaticGraphViewOps,
{
    let mut ctx: Context<G, ComputeStateVec> = g.into();
    let motifs_counter = val::<MotifCounter>(0);
    let delta_len = deltas.len();

    ctx.agg(motifs_counter);

    let out1 = triangle_motifs(g, deltas.clone(), motifs_counter, threads);

    let step1 = ATask::new(move |evv: &mut EvalNodeView<G, MotifCounter>| {
        let g = evv.graph();
        let two_nodes = twonode_motif_count(g, evv, deltas.clone());
        let star_nodes = star_motif_count(evv, deltas.clone());

        *evv.get_mut() = MotifCounter::new(
            deltas.len(),
            two_nodes,
            star_nodes,
            evv.get().triangle.clone(),
        );

        Step::Continue
    });

    let mut runner: TaskRunner<G, _> = TaskRunner::new(ctx);

    runner.run(
        vec![Job::new(step1)],
        vec![],
        None,
        |_, _, _els, local| {
            let mut motifs = HashMap::new();
            for (vref, mc) in enumerate(local) {
                let v_gid = g.node_name(vref.into());
                let triangles = out1
                    .get(&v_gid)
                    .cloned()
                    .unwrap_or_else(|| vec![[0usize; 8]; delta_len]);
                let run_counts = (0..delta_len)
                    .map(|i| {
                        let two_nodes = mc.two_nodes[i].to_vec();
                        let tmp_stars = mc.star_nodes[i].to_vec();
                        let stars: Vec<usize> = tmp_stars
                            .iter()
                            .zip(two_nodes.iter().cycle().take(24))
                            .map(|(&x1, &x2)| x1 - x2)
                            .collect();
                        let mut final_cts = Vec::new();
                        final_cts.extend(stars.into_iter());
                        final_cts.extend(two_nodes.into_iter());
                        final_cts.extend(triangles[i].into_iter());
                        final_cts
                    })
                    .collect::<Vec<Vec<usize>>>();
                motifs.insert(v_gid.clone(), run_counts);
            }
            motifs
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
    fn test_local_motif() {
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

        let binding = temporal_three_node_motif(&g, Vec::from([10]), None);
        let actual = binding
            .iter()
            .map(|(k, v)| (k, v[0].clone()))
            .into_iter()
            .collect::<HashMap<&String, Vec<usize>>>();

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
}
