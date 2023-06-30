use std::sync::Arc;

use once_cell::sync::Lazy;
use rayon::{ThreadPool, ThreadPoolBuilder};

pub mod context;
mod edge;
pub mod task;
pub mod task_runner;
pub(crate) mod task_state;
pub(crate) mod vertex;

pub static POOL: Lazy<Arc<ThreadPool>> = Lazy::new(|| {
    let num_threads = std::env::var("DOCBROWN_MAX_THREADS")
        .map(|s| {
            s.parse::<usize>()
                .expect("DOCBROWN_MAX_THREADS must be a number")
        })
        .unwrap_or_else(|_| {
            std::thread::available_parallelism()
                .unwrap_or(std::num::NonZeroUsize::new(1).unwrap())
                .get()
        });

    let pool = ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build()
        .unwrap();

    Arc::new(pool)
});

pub fn custom_pool(n_threads: usize) -> Arc<ThreadPool> {
    let pool = ThreadPoolBuilder::new()
        .num_threads(n_threads)
        .build()
        .unwrap();

    Arc::new(pool)
}

#[cfg(test)]
mod task_tests {
    use crate::db::api::mutation::AdditionOps;
    use crate::{
        core::state::{self, compute_state::ComputeStateVec},
        db::graph::Graph,
    };

    use super::{
        context::Context,
        task::{ATask, Job, Step},
        task_runner::TaskRunner,
    };

    // count all the vertices with a global state
    #[test]
    fn count_all_vertices_with_global_state() {
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
            graph.add_edge(ts, src, dst, [], None).unwrap();
        }

        let mut ctx: Context<Graph, ComputeStateVec> = (&graph).into();

        let count = state::accumulator_id::accumulators::sum::<usize>(0);

        ctx.global_agg(count.clone());

        let step1 = ATask::new(move |vv| {
            vv.global_update(&count, 1);
            Step::Done
        });

        let mut runner = TaskRunner::new(ctx);

        let actual = runner.run(
            vec![],
            vec![Job::new(step1)],
            (),
            |egs, _, _, _| egs.finalize(&count),
            Some(2),
            1,
            None,
            None,
        );

        assert_eq!(actual, 8);
    }
}
