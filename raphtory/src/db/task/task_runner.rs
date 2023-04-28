use std::{
    borrow::Cow,
    cell::RefCell,
    cmp::min,
    rc::Rc,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use rayon::{prelude::*, ThreadPool};

use crate::{
    core::state::{ComputeState, ShuffleComputeState},
    db::view_api::internal::GraphViewInternalOps,
};

use super::{
    context::Context,
    custom_pool,
    eval_vertex::EvalVertexView,
    task::{Job, Step, Task},
    POOL,
};

pub struct TaskRunner<G: GraphViewInternalOps + Send + Sync + Clone + 'static, CS: ComputeState> {
    pub(crate) ctx: Context<G, CS>,
}

impl<G: GraphViewInternalOps + Send + Sync + Clone + 'static, CS: ComputeState> TaskRunner<G, CS> {
    pub fn new(ctx: Context<G, CS>) -> Self {
        Self { ctx }
    }

    fn merge_states(
        &self,
        a: Arc<ShuffleComputeState<CS>>,
        b: Arc<ShuffleComputeState<CS>>,
    ) -> Arc<ShuffleComputeState<CS>> {
        // println!("merging states \n {:?} \n {:?}", a, b);
        let a = self.ctx.run_merge(a, b);

        // println!("merged states  \n {:?}", a);
        a
    }

    fn make_total_state<B: Clone, F: Fn() -> B>(
        &self,
        num_threads: usize,
        f: F,
    ) -> Vec<(usize, B)> {
        vec![f(); num_threads]
            .into_iter()
            .enumerate()
            .collect::<Vec<_>>()
    }

    fn run_task(
        &self,
        state: &Arc<ShuffleComputeState<CS>>,
        num_shards: usize,
        num_tasks: usize,
        job_id: &usize,
        done_v2: &AtomicBool,
        task: &Box<dyn Task<G, CS> + Send + Sync>,
    ) -> Arc<ShuffleComputeState<CS>> {
        let rc_local_state = Rc::new(RefCell::new(Cow::Borrowed(state.as_ref())));

        let g = self.ctx.graph();

        let mut done = true;
        for shard in 0..num_shards {
            if shard % num_tasks == *job_id {
                for vertex in self.ctx.graph().vertices_shard(shard) {
                    let vv = EvalVertexView::new(
                        self.ctx.ss(),
                        vertex,
                        g.clone(),
                        rc_local_state.clone(),
                    );

                    match task.run(&vv) {
                        Step::Continue => {
                            done = false;
                        }
                        Step::Done => {}
                    }
                }
            }
        }
        if !done {
            done_v2.store(false, Ordering::Relaxed);
        }

        let cow_state: Cow<ShuffleComputeState<CS>> =
            Rc::try_unwrap(rc_local_state).unwrap().into_inner();
        match cow_state {
            Cow::Owned(state) => {
                // the state was changed in some way so we need to update the arc
                Arc::new(state)
            }
            Cow::Borrowed(_) => {
                // the state was only read, so we can just return the original arc
                state.clone()
            }
        }
    }

    pub fn run_task_list(
        &mut self,
        tasks: &[Job<G, CS>],
        pool: &ThreadPool,
        total_state: Arc<ShuffleComputeState<CS>>,
        num_threads: usize,
        num_shards: usize,
    ) -> (bool, Arc<ShuffleComputeState<CS>>) {
        pool.install(move || {
            let mut new_total_state = total_state;
            let mut done = false;

            let num_tasks = min(num_shards, num_threads);

            for task in tasks.iter() {
                let done_v2 = AtomicBool::new(true);

                let updated_state = {
                    let task_states = self.make_total_state(num_tasks, || new_total_state.clone());

                    let out_state = match task {
                        Job::Write(task) => task_states
                            .par_iter()
                            .map(|(job_id, state)| {
                                self.run_task(state, num_shards, num_tasks, job_id, &done_v2, task)
                            })
                            .reduce_with(|a, b| self.merge_states(a, b)),
                        Job::Read(task) => {
                            task_states.par_iter().for_each(|(job_id, state)| {
                                self.run_task(state, num_shards, num_tasks, job_id, &done_v2, task);
                            });
                            None
                        }
                    };
                    out_state
                };

                if let Some(arc_state) = updated_state {
                    new_total_state = arc_state;
                }

                if done_v2.load(Ordering::Relaxed) {
                    done = true;
                    break;
                }
            }

            (done, new_total_state)
        })
    }

    pub fn run(
        &mut self,
        init_tasks: Vec<Job<G, CS>>,
        tasks: Vec<Job<G, CS>>,
        num_threads: Option<usize>,
        steps: usize,
        initial_state: Option<Arc<ShuffleComputeState<CS>>>,
    ) -> Arc<ShuffleComputeState<CS>> {
        let graph_shards = self.ctx.graph().num_shards();

        let pool = num_threads
            .map(|nt| custom_pool(nt))
            .unwrap_or_else(|| POOL.clone());

        let num_threads = pool.current_num_threads();

        let mut total_state =
            initial_state.unwrap_or_else(|| Arc::new(ShuffleComputeState::new(graph_shards)));

        let mut done = false;

        (done, total_state) =
            self.run_task_list(&init_tasks, &pool, total_state, num_threads, graph_shards);

        while !done && self.ctx.ss() < steps && tasks.len() > 0 {
            (done, total_state) =
                self.run_task_list(&tasks, &pool, total_state, num_threads, graph_shards);
            // copy and reset the state from the step that just ended

            Arc::get_mut(&mut total_state).map(|s| {
                s.copy_over_next_ss(self.ctx.ss());
                s.reset_states(self.ctx.ss(), self.ctx.resetable_states());
            });

            self.ctx.increment_ss();
        }

        total_state
    }
}
