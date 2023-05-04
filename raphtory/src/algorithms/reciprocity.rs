//! Reciprocity - measure of the symmetry of relationships in a graph.
//! This calculates the number of reciprocal connections (edges that go in both directions) in a
//! graph and normalizes it by the total number of edges.
//!
//! In a social network context, reciprocity measures the likelihood that if person A is linked
//! to person B, then person B is linked to person A. This algorithm can be used to determine the
//! level of symmetry or balance in a social network. It can also reveal the power dynamics in a
//! group or community. For example, if one person has many connections that are not reciprocated,
//! it could indicate that this person has more power or influence in the network than others.
//!
//! In a business context, reciprocity can be used to study customer behavior. For instance, in a
//! transactional network, if a customer tends to make a purchase from a seller and then the seller
//! makes a purchase from the same customer, it can indicate a strong reciprocal relationship
//! between them. On the other hand, if the seller does not make a purchase from the same customer,
//! it could imply a less reciprocal or more one-sided relationship.
//!
//! There are three algorithms in this module:
//! - `all_local_reciprocity` - returns the reciprocity of every vertex in the graph as a tuple of
//! vector id and the reciprocity
//! - `global_reciprocity` - returns the global reciprocity of the entire graph
//!
//! # Examples
//!
//! ```rust
//! use raphtory::algorithms::reciprocity::{all_local_reciprocity, global_reciprocity};
//! use raphtory::db::graph::Graph;
//! use raphtory::db::view_api::*;
//! let g = Graph::new(1);
//! let vs = vec![
//!     (1, 1, 2),
//!     (1, 1, 4),
//!     (1, 2, 3),
//!     (1, 3, 2),
//!     (1, 3, 1),
//!     (1, 4, 3),
//!     (1, 4, 1),
//!     (1, 1, 5),
//! ];
//!
//! for (t, src, dst) in &vs {
//!     g.add_edge(*t, *src, *dst, &vec![], None);
//! }
//!
//! println!("all_local_reciprocity: {:?}", all_local_reciprocity(&g));
//! println!("global_reciprocity: {:?}", global_reciprocity(&g));
//! ```
use crate::core::state::compute_state::ComputeStateMap;
use crate::core::state::{self};
use crate::db::program::{EvalVertexView, GlobalEvalState, LocalState, Program};
use crate::db::view_api::GraphViewOps;
use std::collections::{HashMap, HashSet};

struct GlobalReciprocity {}

type CS = ComputeStateMap;
/// Gets the unique edge counts excluding cycles for a vertex. Returns a tuple of usize
/// (out neighbours, in neighbours, the intersection of the out and in neighbours)
fn get_reciprocal_edge_count<G: GraphViewOps>(v: &EvalVertexView<G>) -> (usize, usize, usize) {
    let out_neighbours: HashSet<u64> = v
        .neighbours_out()
        .map(|n| n.global_id())
        .filter(|x| *x != v.global_id())
        .collect();

    let in_neighbours = v
        .neighbours_in()
        .map(|n| n.global_id())
        .filter(|x| *x != v.global_id())
        .count();

    let out_inter_in = out_neighbours
        .intersection(
            &v.neighbours_in()
                .map(|n| n.global_id())
                .filter(|x| *x != v.global_id())
                .collect(),
        )
        .count();
    (out_neighbours.len(), in_neighbours, out_inter_in)
}

impl Program for GlobalReciprocity {
    type Out = f64;

    fn local_eval<G: GraphViewOps>(&self, c: &LocalState<G>) {
        let total_out_neighbours = c.agg(state::accumulator_id::def::sum::<usize>(0));
        let total_out_inter_in = c.agg(state::accumulator_id::def::sum::<usize>(1));

        c.step(|v| {
            let edge_counts = get_reciprocal_edge_count(&v);
            v.global_update(&total_out_neighbours, edge_counts.0);
            v.global_update(&total_out_inter_in, edge_counts.2);
        });
    }

    fn post_eval<G: GraphViewOps>(&self, c: &mut GlobalEvalState<G>) {
        let _ = c.global_agg(state::accumulator_id::def::sum::<usize>(0));
        let _ = c.global_agg(state::accumulator_id::def::sum::<usize>(1));
        c.step(|_| true);
    }

    #[allow(unused_variables)]
    fn produce_output<G: GraphViewOps>(&self, g: &G, gs: &GlobalEvalState<G>) -> Self::Out
    where
        Self: Sync,
    {
        gs.read_global_state(&state::accumulator_id::def::sum::<usize>(1))
            .unwrap_or(0) as f64
            / gs.read_global_state(&state::accumulator_id::def::sum::<usize>(0))
                .unwrap_or(0) as f64
    }
}

struct AllLocalReciprocity {}

impl Program for AllLocalReciprocity {
    type Out = HashMap<u64, f64>;

    fn local_eval<G: GraphViewOps>(&self, c: &LocalState<G>) {
        let min = c.agg(state::accumulator_id::def::sum(0));

        c.step(|v| {
            let edge_counts = get_reciprocal_edge_count(&v);
            let res = (2.0 * edge_counts.2 as f64) / (edge_counts.1 as f64 + edge_counts.0 as f64);
            if res.is_nan() {
                v.global_update(&min, 0.0);
            } else {
                v.update(&min, res);
            }
        });
    }

    fn post_eval<G: GraphViewOps>(&self, c: &mut GlobalEvalState<G>) {
        let _ = c.agg(state::accumulator_id::def::sum::<usize>(0));
        c.step(|_| true);
    }

    fn produce_output<G: GraphViewOps>(&self, g: &G, gs: &GlobalEvalState<G>) -> Self::Out
    where
        Self: Sync,
    {
        let agg = state::accumulator_id::def::sum::<f64>(0);

        let mut results: HashMap<u64, f64> = HashMap::default();

        (0..g.num_shards())
            .into_iter()
            .fold(&mut results, |res, part_id| {
                gs.fold_state(&agg, part_id, res, |res, v_id, cc| {
                    res.insert(*v_id, cc);
                    res
                })
            });

        results
    }
}

/// returns the global reciprocity of the entire graph
pub fn global_reciprocity<G: GraphViewOps>(g: &G) -> f64 {
    let mut gs = GlobalEvalState::new(g.clone(), false);
    let gr = GlobalReciprocity {};
    gr.run_step(g, &mut gs);
    gr.produce_output(g, &gs)
}

/// returns the reciprocity of every vertex in the graph as a tuple of
pub fn all_local_reciprocity<G: GraphViewOps>(g: &G) -> HashMap<u64, f64> {
    let mut gs = GlobalEvalState::new(g.clone(), false);
    let gr = AllLocalReciprocity {};
    gr.run_step(g, &mut gs);
    gr.produce_output(g, &gs)
}

#[cfg(test)]
mod reciprocity_test {
    use crate::algorithms::reciprocity::{all_local_reciprocity, global_reciprocity};
    use crate::db::graph::Graph;
    use pretty_assertions::assert_eq;
    use std::collections::HashMap;

    #[test]
    fn test_global_recip() {
        let graph = Graph::new(2);

        let vs = vec![
            (1, 2),
            (1, 4),
            (2, 3),
            (3, 2),
            (3, 1),
            (4, 3),
            (4, 1),
            (1, 5),
        ];

        for (src, dst) in &vs {
            graph.add_edge(0, *src, *dst, &vec![], None).unwrap();
        }

        let actual = global_reciprocity(&graph);
        assert_eq!(actual, 0.5);

        let expected_vec: Vec<(u64, f64)> =
            vec![(1, 0.4), (2, 2.0 / 3.0), (3, 0.5), (4, 2.0 / 3.0), (5, 0.0)];

        let map_names_by_id: HashMap<u64, f64> = expected_vec.iter().map(|x| (x.0, x.1)).collect();

        let actual = all_local_reciprocity(&graph);
        assert_eq!(actual, map_names_by_id);
    }
}
