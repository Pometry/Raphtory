use crate::{
    algorithms::{algorithm_result::AlgorithmResult, metrics::degree::max_degree},
    core::state::{accumulator_id::accumulators::sum, compute_state::ComputeStateVec},
    db::{
        api::view::StaticGraphViewOps,
        task::{
            context::Context,
            node::eval_node::EvalNodeView,
            task::{ATask, Job, Step},
            task_runner::TaskRunner,
        },
    },
    prelude::*,
};
use ordered_float::OrderedFloat;

/// Computes the degree centrality of all nodes in the graph. The values are normalized
/// by dividing each result with the maximum possible degree. Graphs with self-loops can have
/// values of centrality greater than 1.
///
/// # Arguments
///
/// - `g`: A reference to the graph.
/// - `threads` - Number of threads to use
///
/// # Returns
///
/// An [AlgorithmResult] containing the degree centrality of each node.
pub fn degree_centrality<G: StaticGraphViewOps>(
    g: &G,
    threads: Option<usize>,
) -> AlgorithmResult<G, f64, OrderedFloat<f64>> {
    let max_degree = max_degree(g);

    let mut ctx: Context<G, ComputeStateVec> = g.into();

    let min = sum(0);

    ctx.agg(min);

    let step1 = ATask::new(move |evv: &mut EvalNodeView<_, ()>| {
        // The division below is fine as floating point division of 0.0
        // causes the result to be an NaN
        let res = evv.degree() as f64 / max_degree as f64;
        if res.is_nan() || res.is_infinite() {
            evv.global_update(&min, 0.0);
        } else {
            evv.update(&min, res);
        }
        Step::Done
    });

    let mut runner: TaskRunner<G, _> = TaskRunner::new(ctx);
    let runner_result = runner.run(
        vec![],
        vec![Job::new(step1)],
        None,
        |_, ess, _, _| ess.finalize(&min, |min| min),
        threads,
        1,
        None,
        None,
    );
    let results_type = std::any::type_name::<f64>();
    AlgorithmResult::new(g.clone(), "Degree Centrality", results_type, runner_result)
}

#[cfg(test)]
mod degree_centrality_test {
    use crate::{
        algorithms::centrality::degree_centrality::degree_centrality,
        db::{api::mutation::AdditionOps, graph::graph::Graph},
        prelude::NO_PROPS,
        test_storage,
    };
    use std::collections::HashMap;

    #[test]
    fn test_degree_centrality() {
        let graph = Graph::new();
        let vs = vec![(1, 2), (1, 3), (1, 4), (2, 3), (2, 4)];
        for (src, dst) in &vs {
            graph.add_edge(0, *src, *dst, NO_PROPS, None).unwrap();
        }
        test_storage!(&graph, |graph| {
            let mut hash_map_result: HashMap<String, f64> = HashMap::new();
            hash_map_result.insert("1".to_string(), 1.0);
            hash_map_result.insert("2".to_string(), 1.0);
            hash_map_result.insert("3".to_string(), 2.0 / 3.0);
            hash_map_result.insert("4".to_string(), 2.0 / 3.0);

            let binding = degree_centrality(graph, None);
            let res = binding.get_all_with_names();
            assert_eq!(res, hash_map_result);
        });
    }
}
