#[cfg(test)]
mod task_tests {
    use raphtory::{
        core::state::{self, compute_state::ComputeStateVec},
        db::{api::mutation::AdditionOps, task::node::eval_node::EvalNodeView},
        prelude::*,
    };

    use raphtory::db::task::{
        context::Context,
        task::{ATask, Job, Step},
        task_runner::TaskRunner,
    };

    // count all the nodes with a global state
    #[test]
    fn count_all_nodes_with_global_state() {
        let graph = Graph::new();

        let edges = vec![
            (1, 2, 1),
            (2, 3, 2),
            (3, 4, 3),
            (3, 5, 4),
            (6, 5, 5),
            (7, 8, 6),
            (8, 7, 7),
        ];

        for (src, dst, ts) in edges {
            graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
        }

        let mut ctx: Context<Graph, ComputeStateVec> = (&graph).into();

        let count = state::accumulator_id::accumulators::sum::<usize>(0);

        ctx.global_agg(count);

        let step1 = ATask::new(move |vv: &mut EvalNodeView<_, ()>| {
            vv.global_update(&count, 1);
            Step::Done
        });

        let mut runner = TaskRunner::new(ctx);

        let actual = runner.run(
            vec![],
            vec![Job::new(step1)],
            None,
            |egs, _, _, _, _| egs.finalize(&count),
            Some(2),
            1,
            None,
            None,
        );

        assert_eq!(actual, 8);
    }
}
