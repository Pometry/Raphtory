//! Reciprocity - measure of the symmetry of relationships in a graph.
//! This calculates the number of reciprocal connections (edges that go in both directions) in a
//! graph and normalizes it by the total number of edges.
//!
//! In a social network context, reciprocity measures the likelihood that if person A is linked
//! to person B, then person B is linked to person A. This algorithm can be used to determine the
//! level of symmetry or balance in a social network. It can also reveal the power dynamics in a
//! group or community_detection. For example, if one person has many connections that are not reciprocated,
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
//! use raphtory::algorithms::metrics::reciprocity::{all_local_reciprocity, global_reciprocity};
//! use raphtory::prelude::*;
//! let g = Graph::new();
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
//!     g.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
//! }
//!
//! println!("all_local_reciprocity: {:?}", all_local_reciprocity(&g, None));
//! println!("global_reciprocity: {:?}", global_reciprocity(&g, None));
//! ```
use crate::{
    algorithms::algorithm_result_old::AlgorithmResultOLD,
    core::state::{
        accumulator_id::accumulators::sum,
        compute_state::{ComputeState, ComputeStateVec},
    },
    db::{
        api::view::{GraphViewOps, VertexViewOps},
        task::{
            context::Context,
            task::{ATask, Job, Step},
            task_runner::TaskRunner,
            vertex::eval_vertex::EvalVertexView,
        },
    },
};
use ordered_float::OrderedFloat;
use std::collections::{HashMap, HashSet};

/// Gets the unique edge counts excluding cycles for a vertex. Returns a tuple of usize
/// (out neighbours, in neighbours, the intersection of the out and in neighbours)
fn get_reciprocal_edge_count<G: GraphViewOps, CS: ComputeState>(
    v: &EvalVertexView<G, CS, ()>,
) -> (usize, usize, usize) {
    let id = v.id();
    let out_neighbours: HashSet<u64> = v.out_neighbours().id().filter(|x| *x != id).collect();

    let in_neighbours = v.in_neighbours().id().filter(|x| *x != id).count();

    let out_inter_in = out_neighbours
        .intersection(&v.in_neighbours().id().filter(|x| *x != id).collect())
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

    runner.run(
        vec![],
        vec![Job::new(step1)],
        None,
        |egs, _, _, _| {
            (egs.finalize(&total_out_inter_in) as f64)
                / (egs.finalize(&total_out_neighbours) as f64)
        },
        threads,
        1,
        None,
        None,
    )
}

/// returns the reciprocity of every vertex in the graph as a tuple of
/// vector id and the reciprocity
pub fn all_local_reciprocity<G: GraphViewOps>(
    g: &G,
    threads: Option<usize>,
) -> AlgorithmResultOLD<String, f64, OrderedFloat<f64>> {
    let mut ctx: Context<G, ComputeStateVec> = g.into();

    let min = sum(0);
    ctx.agg(min);

    let step1 = ATask::new(move |evv| {
        let edge_counts = get_reciprocal_edge_count(evv);
        let res = (2.0 * edge_counts.2 as f64) / (edge_counts.1 as f64 + edge_counts.0 as f64);
        if res.is_nan() {
            evv.global_update(&min, 0.0);
        } else {
            evv.update(&min, res);
        }
        Step::Continue
    });

    let mut runner: TaskRunner<G, _> = TaskRunner::new(ctx);
    let results_type = std::any::type_name::<HashMap<String, f64>>();

    AlgorithmResultOLD::new(
        "Reciprocity",
        results_type,
        runner.run(
            vec![],
            vec![Job::new(step1)],
            None,
            |_, ess, _, _| ess.finalize(&min, |min| min),
            threads,
            1,
            None,
            None,
        ),
    )
}

#[cfg(test)]
mod reciprocity_test {
    use crate::{
        algorithms::metrics::reciprocity::{all_local_reciprocity, global_reciprocity},
        db::{api::mutation::AdditionOps, graph::graph::Graph},
        prelude::NO_PROPS,
    };
    use pretty_assertions::assert_eq;
    use std::collections::HashMap;

    #[test]
    fn test_global_recip() {
        let graph = Graph::new();

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
            graph.add_edge(0, *src, *dst, NO_PROPS, None).unwrap();
        }

        let actual = global_reciprocity(&graph, None);
        assert_eq!(actual, 0.5);

        let mut hash_map_result: HashMap<String, f64> = HashMap::new();
        hash_map_result.insert("1".to_string(), 0.4);
        hash_map_result.insert("2".to_string(), 2.0 / 3.0);
        hash_map_result.insert("3".to_string(), 0.5);
        hash_map_result.insert("4".to_string(), 2.0 / 3.0);
        hash_map_result.insert("5".to_string(), 0.0);

        let res = all_local_reciprocity(&graph, None);
        assert_eq!(res.get("1"), hash_map_result.get("1"));
    }
}
