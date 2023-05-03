use std::{
    borrow::Cow,
    cell::RefCell,
    cmp::min,
    iter::zip,
    rc::Rc,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use itertools::Itertools;
use rayon::{prelude::*, ThreadPool};

use crate::{
    core::state::{ComputeState, ShuffleComputeState},
    db::view_api::internal::GraphViewInternalOps,
};

use super::{
    context::{Context, GlobalState},
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
        a: (Arc<ShuffleComputeState<CS>>, Arc<ShuffleComputeState<CS>>),
        b: (Arc<ShuffleComputeState<CS>>, Arc<ShuffleComputeState<CS>>),
    ) -> (Arc<ShuffleComputeState<CS>>, Arc<ShuffleComputeState<CS>>) {
        let shard = self.ctx.run_merge(a.0, b.0);
        let global = self.ctx.run_merge(a.1, b.1);
        (shard, global)
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
        shard_state: &Arc<ShuffleComputeState<CS>>,
        global_state: &Arc<ShuffleComputeState<CS>>,
        local_state: &mut Arc<Option<ShuffleComputeState<CS>>>,
        num_shards: usize,
        num_tasks: usize,
        job_id: &usize,
        atomic_done: &AtomicBool,
        task: &Box<dyn Task<G, CS> + Send + Sync>,
    ) -> (Arc<ShuffleComputeState<CS>>, Arc<ShuffleComputeState<CS>>) {
        // the view for this task of the global state
        let shard_state_view = Rc::new(RefCell::new(Cow::Borrowed(shard_state.as_ref())));
        let global_state_view = Rc::new(RefCell::new(Cow::Borrowed(global_state.as_ref())));

        let owned_local_state = Arc::get_mut(local_state).and_then(|x| x.take()).unwrap();
        let rc_local_state = Rc::new(RefCell::new(owned_local_state));

        let g = self.ctx.graph();

        let mut done = true;
        for shard in 0..num_shards {
            if shard % num_tasks == *job_id {
                for vertex in self.ctx.graph().vertices_shard(shard) {
                    let vv = EvalVertexView::new(
                        self.ctx.ss(),
                        vertex,
                        g.clone(),
                        shard_state_view.clone(),
                        global_state_view.clone(),
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
        // put the local state back
        *local_state = Arc::new(Some(Rc::try_unwrap(rc_local_state).unwrap().into_inner()));

        if !done {
            atomic_done.store(false, Ordering::Relaxed);
        }

        let cow_shard_state: Cow<ShuffleComputeState<CS>> =
            Rc::try_unwrap(shard_state_view).unwrap().into_inner();

        let cow_global_state = Rc::try_unwrap(global_state_view).unwrap().into_inner();

        match (cow_shard_state, cow_global_state) {
            (Cow::Owned(state), Cow::Owned(global_state)) => {
                // the state was changed in some way so we need to update the arc
                (Arc::new(state), Arc::new(global_state))
            }
            (Cow::Borrowed(_), Cow::Borrowed(_)) => {
                // the state was only read, so we can just return the original arc
                (shard_state.clone(), global_state.clone())
            }
            (Cow::Owned(state), Cow::Borrowed(_)) => {
                // the state was changed in some way so we need to update the arc
                (Arc::new(state), global_state.clone())
            }
            (Cow::Borrowed(_), Cow::Owned(global_state)) => {
                // the state was changed in some way so we need to update the arc
                (shard_state.clone(), Arc::new(global_state))
            }
        }
    }

    pub fn run_task_list(
        &mut self,
        tasks: &[Job<G, CS>],
        pool: &ThreadPool,
        shard_state: Arc<ShuffleComputeState<CS>>,
        global_state: Arc<ShuffleComputeState<CS>>,
        mut local_state: Vec<Arc<Option<ShuffleComputeState<CS>>>>,
        num_threads: usize,
        num_shards: usize,
    ) -> (
        bool,
        Arc<ShuffleComputeState<CS>>,
        Arc<ShuffleComputeState<CS>>,
        Vec<Arc<Option<ShuffleComputeState<CS>>>>,
    ) {
        pool.install(move || {
            let mut new_shard_state = shard_state;
            let mut new_global_state = global_state;

            let mut done = false;

            let num_tasks = min(num_shards, num_threads);

            for task in tasks.iter() {
                let atomic_done = AtomicBool::new(true);

                let updated_state = {
                    let task_shard_states =
                        self.make_total_state(num_tasks, || new_shard_state.clone());

                    let task_global_states =
                        self.make_total_state(num_shards, || new_global_state.clone());

                    let mut task_states = zip(
                        zip(local_state.iter_mut(), task_shard_states.into_iter()),
                        task_global_states.into_iter(),
                    )
                    .map(
                        |((local_state, (_, shard_state)), (job_id, global_state))| {
                            (job_id, local_state, shard_state, global_state)
                        },
                    )
                    .collect_vec();

                    let out_state = match task {
                        Job::Write(task) => task_states
                            .par_iter_mut()
                            .map(|(job_id, local_state, shard_state, global_state)| {
                                self.run_task(
                                    shard_state,
                                    global_state,
                                    local_state,
                                    num_shards,
                                    num_tasks,
                                    job_id,
                                    &atomic_done,
                                    task,
                                )
                            })
                            .reduce_with(|a, b| self.merge_states(a, b)),
                        Job::Read(task) => {
                            task_states.par_iter_mut().for_each(
                                |(job_id, local_state, shard_state, global_state)| {
                                    self.run_task(
                                        shard_state,
                                        global_state,
                                        local_state,
                                        num_shards,
                                        num_tasks,
                                        job_id,
                                        &atomic_done,
                                        task,
                                    );
                                },
                            );
                            None
                        }
                        Job::Check(task) => {
                            match task(&GlobalState::new(new_global_state.clone(), self.ctx.ss())) {
                                Step::Continue => {
                                    atomic_done.store(false, Ordering::Relaxed);
                                }
                                Step::Done => {}
                            };
                            None
                        }
                    };
                    out_state
                };

                if let Some((shard_state, global_state)) = updated_state {
                    new_shard_state = shard_state;
                    new_global_state = global_state;
                }

                if atomic_done.load(Ordering::Relaxed) {
                    done = true;
                    break;
                }
            }

            (done, new_shard_state, new_global_state, local_state)
        })
    }

    pub fn run(
        &mut self,
        init_tasks: Vec<Job<G, CS>>,
        tasks: Vec<Job<G, CS>>,
        num_threads: Option<usize>,
        steps: usize,
        shard_initial_state: Option<Arc<ShuffleComputeState<CS>>>,
        global_initial_state: Option<Arc<ShuffleComputeState<CS>>>,
    ) -> (
        Arc<ShuffleComputeState<CS>>,
        Vec<Arc<Option<ShuffleComputeState<CS>>>>,
    ) {
        let graph_shards = self.ctx.graph().num_shards();

        let pool = num_threads
            .map(|nt| custom_pool(nt))
            .unwrap_or_else(|| POOL.clone());

        let num_threads = pool.current_num_threads();

        let mut shard_state =
            shard_initial_state.unwrap_or_else(|| Arc::new(ShuffleComputeState::new(graph_shards)));

        let mut global_state =
            global_initial_state.unwrap_or_else(|| Arc::new(ShuffleComputeState::new(0)));

        let mut local_state: Vec<Arc<Option<ShuffleComputeState<CS>>>> = (0..num_threads)
            .into_iter()
            .map(|_| Arc::new(Some(ShuffleComputeState::new(graph_shards))))
            .collect_vec();

        let mut done = false;

        (done, shard_state, global_state, local_state) = self.run_task_list(
            &init_tasks,
            &pool,
            shard_state,
            global_state,
            local_state,
            num_threads,
            graph_shards,
        );

        while !done && self.ctx.ss() < steps && tasks.len() > 0 {
            (done, shard_state, global_state, local_state) = self.run_task_list(
                &tasks,
                &pool,
                shard_state,
                global_state,
                local_state,
                num_threads,
                graph_shards,
            );

            // copy and reset the state from the step that just ended
            Arc::get_mut(&mut shard_state).map(|s| {
                s.copy_over_next_ss(self.ctx.ss());
                s.reset_states(self.ctx.ss(), self.ctx.resetable_states());
            });

            // Copy and reset the local states from the step that just ended

            for local_state in local_state.iter_mut() {
                Arc::get_mut(local_state).map(|s| {
                    s.as_mut().map(|s| {
                        s.copy_over_next_ss(self.ctx.ss());
                        s.reset_states(self.ctx.ss(), self.ctx.resetable_states());
                    });
                });
            }

            self.ctx.increment_ss();
        }

        (shard_state, local_state)
    }
}
