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
//! println!("all_local_reciprocity: {:?}", all_local_reciprocity(&g, None));
//! println!("global_reciprocity: {:?}", global_reciprocity(&g, None));
//! ```
use crate::core::state::accumulator_id::accumulators;
use crate::db::program::{GlobalEvalState, LocalState, Program};
use crate::db::view_api::GraphViewOps;
use std::collections::{HashMap, HashSet};
use crate::core::state::accumulator_id::accumulators::sum;
use crate::core::state::compute_state::{ComputeState, ComputeStateVec};
use crate::db::task::context::Context;
use crate::db::task::eval_vertex::EvalVertexView;
use crate::db::task::task::{ATask, Job, Step};
use crate::db::task::task_runner::TaskRunner;

struct GlobalReciprocity {}

/// Gets the unique edge counts excluding cycles for a vertex. Returns a tuple of usize
/// (out neighbours, in neighbours, the intersection of the out and in neighbours)
fn get_reciprocal_edge_count<G: GraphViewOps, CS: ComputeState>(v: &EvalVertexView<G, CS>) -> (usize, usize, usize) {
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

/// returns the global reciprocity of the entire graph
pub fn global_reciprocity<G: GraphViewOps>(g: &G, threads: Option<usize>) -> f64 {
    let mut ctx: Context<G, ComputeStateVec> = g.into();

    let total_out_neighbours = sum::<usize>(0);
    ctx.global_agg(total_out_neighbours);
    let total_out_inter_in = sum::<usize>(1);
    ctx.global_agg(total_out_inter_in);

    let step1 = ATask::new(move |evv| {
        let edge_counts = get_reciprocal_edge_count(evv);
        evv.global_update(&total_out_neighbours, edge_counts.0);
        evv.global_update(&total_out_inter_in, edge_counts.2);
        Step::Continue
    });

    let mut runner: TaskRunner<G, _> = TaskRunner::new(ctx);

    let (_, global_state, _) = runner.run(
        vec![],
        vec![Job::new(step1)],
        threads,
        1,
        None,
        None,
    );

    global_state.inner().read_global(runner.ctx.ss() + 1, &total_out_inter_in)
        .unwrap_or(0) as f64
        / global_state.inner().read_global(runner.ctx.ss() + 1, &total_out_neighbours)
        .unwrap_or(0) as f64
}

/// returns the reciprocity of every vertex in the graph as a tuple of
pub fn all_local_reciprocity<G: GraphViewOps>(g: &G, threads: Option<usize>) -> HashMap<u64, f64> {
    let mut ctx: Context<G, ComputeStateVec> = g.into();

    let min = sum(0);
    ctx.agg(min);

    let step1 = ATask::new(move |evv| {
        let edge_counts = get_reciprocal_edge_count(&evv);
        let res = (2.0 * edge_counts.2 as f64) / (edge_counts.1 as f64 + edge_counts.0 as f64);
        if res.is_nan() {
            evv.global_update(&min, 0.0);
        } else {
            evv.update(&min, res);
        }
        Step::Continue
    });

    let mut runner: TaskRunner<G, _> = TaskRunner::new(ctx);

    let (shards_states, _, _) = runner.run(
        vec![],
        vec![Job::new(step1)],
        threads,
        1,
        None,
        None,
    );

    let mut results: HashMap<u64, f64> = HashMap::default();

    shards_states.inner().fold_state_internal(
        runner.ctx.ss(),
        &mut results,
        &min,
        |res, shard, pid, min| {
            // println!("v0 = {}, taint_history0 = {:?}", pid, taint_history);
            if let Some(v_ref) = g.lookup_by_pid_and_shard(pid, shard) {
                res.insert(v_ref.g_id, min);
            }
            res
        },
    );
    results
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

        let actual = global_reciprocity(&graph, None);
        assert_eq!(actual, 0.5);

        let expected_vec: Vec<(u64, f64)> =
            vec![(1, 0.4), (2, 2.0 / 3.0), (3, 0.5), (4, 2.0 / 3.0), (5, 0.0)];

        let map_names_by_id: HashMap<u64, f64> = expected_vec.iter().map(|x| (x.0, x.1)).collect();

        let actual = all_local_reciprocity(&graph, None);
        assert_eq!(actual, map_names_by_id);
    }
}
