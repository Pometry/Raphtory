//! # Weight Accumulation
//!
//! This algorithm provides functionality to accumulate (or sum) weights on vertices
//! in a graph.
use crate::{
    algorithms::algorithm_result::AlgorithmResult,
    core::{
        state::{
            accumulator_id::accumulators::sum,
            compute_state::{ComputeState, ComputeStateVec},
        },
        Direction,
    },
    db::{
        api::view::GraphViewOps,
        task::{
            context::Context,
            task::{ATask, Job, Step},
            task_runner::TaskRunner,
            vertex::eval_vertex::EvalVertexView,
        },
    },
    prelude::{EdgeViewOps, PropUnwrap, VertexViewOps},
};
use ordered_float::OrderedFloat;

/// Computes the net sum of weights for a given vertex based on edge direction.
///
/// For every edge connected to the vertex, this function checks the source of the edge
/// against the vertex itself to determine the directionality. The weight can be treated
/// as negative or positive based on the edge's source and the specified direction:
///
/// - If the edge's source is the vertex itself and the direction is either `OUT` or `BOTH`,
///   the weight is treated as negative.
/// - If the edge's source is not the vertex and the direction is either `IN` or `BOTH`,
///   the weight is treated as positive.
/// - In all other cases, the weight contribution is zero.
///
/// # Parameters
/// - `v`: The vertex for which we want to compute the weight sum.
/// - `name`: The name of the property which holds the edge weight.
/// - `direction`: Specifies the direction of edges to consider (`IN`, `OUT`, or `BOTH`).
///
/// # Returns
/// Returns a `f64` which is the net sum of weights for the vertex considering the specified direction.
fn sum_weight_for_vertex<G: GraphViewOps, CS: ComputeState>(
    v: &EvalVertexView<G, CS, ()>,
    name: String,
    direction: Direction,
) -> f64 {
    v.edges()
        .map(|edge| {
            if edge.src().name() == v.name()
                && ((direction == Direction::OUT) | (direction == Direction::BOTH))
            {
                -edge.properties().get(name.clone()).unwrap_f64()
            } else if edge.src().name() != v.name()
                && ((direction == Direction::IN) | (direction == Direction::BOTH))
            {
                edge.properties().get(name.clone()).unwrap_f64()
            } else {
                0.0
            }
        })
        .sum()
}

/// Computes the sum of weights for all vertices in the graph.
///
/// This function iterates over all vertices and calculates the net sum of weights.
/// Incoming edges have a positive sum and outgoing edges have a negative sum
/// It uses a compute context and tasks to achieve this.
///
/// # Parameters
/// - `graph`: The graph on which the operation is to be performed.
/// - `name`: The name of the property which holds the edge weight.
/// - `threads`: An optional parameter to specify the number of threads to use.
///              If `None`, it defaults to a suitable number.
///
/// # Returns
/// Returns an `AlgorithmResult` which maps each vertex to its corresponding net weight sum.
pub fn sum_weights_edges<G: GraphViewOps>(
    graph: &G,
    name: String,
    direction: Direction,
    threads: Option<usize>,
) -> AlgorithmResult<String, OrderedFloat<f64>> {
    let mut ctx: Context<G, ComputeStateVec> = graph.into();
    let min = sum(0);
    ctx.agg(min);
    let step1 = ATask::new(move |evv| {
        let res = sum_weight_for_vertex(evv, name.clone(), direction);
        evv.update(&min, res);
        Step::Continue
    });
    let mut runner: TaskRunner<G, _> = TaskRunner::new(ctx);
    AlgorithmResult::new_with_float(runner.run(
        vec![],
        vec![Job::new(step1)],
        (),
        |_, ess, _, _| ess.finalize(&min, |min| min),
        threads,
        1,
        None,
        None,
    ))
}

#[cfg(test)]
mod sum_weight_test {
    use crate::{
        algorithms::weight_accum::sum_weights_edges,
        core::{Direction, Prop},
        db::{api::mutation::AdditionOps, graph::graph::Graph},
    };
    use ordered_float::OrderedFloat;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_sum_float_weights() {
        let graph = Graph::new();

        let vs = vec![
            ("1", "2", 10.0, 1),
            ("1", "4", 20.0, 2),
            ("2", "3", 5.0, 3),
            ("3", "2", 2.0, 4),
            ("3", "1", 1.0, 5),
            ("4", "3", 10.0, 6),
            ("4", "1", 5.0, 7),
            ("1", "5", 2.0, 8),
        ];

        for (src, dst, val, time) in &vs {
            graph
                .add_edge(
                    *time,
                    *src,
                    *dst,
                    [("value_dec".to_string(), Prop::F64(*val))],
                    None,
                )
                .expect("Couldnt add edge");
        }

        let res = sum_weights_edges(&graph, "value_dec".to_string(), Direction::BOTH, None);
        let expected = vec![
            ("1".to_string(), OrderedFloat(-26.0)),
            ("2".to_string(), OrderedFloat(7.0)),
            ("3".to_string(), OrderedFloat(12.0)),
            ("4".to_string(), OrderedFloat(5.0)),
            ("5".to_string(), OrderedFloat(2.0)),
        ];
        assert_eq!(res.sort_by_key(false), expected);

        let res = sum_weights_edges(&graph, "value_dec".to_string(), Direction::IN, None);
        let expected = vec![
            ("1".to_string(), OrderedFloat(6.0)),
            ("2".to_string(), OrderedFloat(12.0)),
            ("3".to_string(), OrderedFloat(15.0)),
            ("4".to_string(), OrderedFloat(20.0)),
            ("5".to_string(), OrderedFloat(2.0)),
        ];
        assert_eq!(res.sort_by_key(false), expected);

        let res = sum_weights_edges(&graph, "value_dec".to_string(), Direction::OUT, None);
        let expected = vec![
            ("1".to_string(), OrderedFloat(-32.0)),
            ("2".to_string(), OrderedFloat(-5.0)),
            ("3".to_string(), OrderedFloat(-3.0)),
            ("4".to_string(), OrderedFloat(-15.0)),
            ("5".to_string(), OrderedFloat(0.0)),
        ];
        assert_eq!(res.sort_by_key(false), expected);
    }
}
