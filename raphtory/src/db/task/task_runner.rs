use std::{
    borrow::Cow,
    rc::Rc,
    sync::atomic::{AtomicBool, Ordering},
};

use rayon::{prelude::*, ThreadPool};

use crate::core:: state::shuffle_state::{EvalLocalState, EvalShardState};
use crate::{core::state::compute_state::ComputeState, db::view_api::GraphViewOps};

use super::{
    context::{Context, GlobalState},
    custom_pool,
    eval_vertex::EvalVertexView,
    eval_vertex_state::EVState,
    task::{Job, Step, Task},
    task_state::{Global, Local2, Shard},
    POOL,
};

pub struct TaskRunner<G: GraphViewOps, CS: ComputeState> {
    pub(crate) ctx: Context<G, CS>,
}

impl<G: GraphViewOps, CS: ComputeState> TaskRunner<G, CS> {
    pub fn new(ctx: Context<G, CS>) -> Self {
        Self { ctx }
    }

    fn merge_states(
        &self,
        a: (Shard<CS>, Global<CS>),
        b: (Shard<CS>, Global<CS>),
    ) -> (Shard<CS>, Global<CS>) {
        let shard = self.ctx.run_merge_shard(a.0, b.0);
        let global = self.ctx.run_merge_global(a.1, b.1);
        (shard, global)
    }

    fn run_task_v2<S: 'static>(
        &self,
        shard_state: &Shard<CS>,
        global_state: &Global<CS>,
        morcel: &mut [S],
        prev_local_state: &Vec<S>,
        atomic_done: &AtomicBool,
        morcel_size: usize,
        morcel_id: usize,
        task: &Box<dyn Task<G, CS, S> + Send + Sync>,
    ) -> (Shard<CS>, Global<CS>) {
        // the view for this task of the global state
        let shard_state_view = shard_state.as_cow();
        let global_state_view = global_state.as_cow();

        let g = self.ctx.graph();

        let mut done = true;

        let vertex_state = EVState::rc_from(shard_state_view, global_state_view);

        let local = Local2::new(prev_local_state);
        let mut v_ref = morcel_id * morcel_size;
        for local_state in morcel {
            let mut vv = EvalVertexView::new_local(
                self.ctx.ss(),
                v_ref.into(),
                &g,
                Some(local_state),
                &local,
                vertex_state.clone(),
            );
            v_ref += 1;

            match task.run(&mut vv) {
                Step::Continue => {
                    done = false;
                }
                Step::Done => {}
            }
        }

        if !done {
            atomic_done.store(false, Ordering::Relaxed);
        }

        let vertex_state: EVState<CS> = Rc::try_unwrap(vertex_state).unwrap().into_inner();
        let (shard_state_view, global_state_view) = vertex_state.restore_states();

        match (shard_state_view, global_state_view) {
            (Cow::Owned(state), Cow::Owned(global_state)) => {
                // the state was changed in some way so we need to update the arc
                (Shard::from_state(state), Global::from_state(global_state))
            }
            (Cow::Borrowed(_), Cow::Borrowed(_)) => {
                // the state was only read, so we can just return the original arc
                (shard_state.clone(), global_state.clone())
            }
            (Cow::Owned(state), Cow::Borrowed(_)) => {
                // the state was changed in some way so we need to update the arc
                (Shard::from_state(state), global_state.clone())
            }
            (Cow::Borrowed(_), Cow::Owned(global_state)) => {
                // the state was changed in some way so we need to update the arc
                (shard_state.clone(), Global::from_state(global_state))
            }
        }
    }

    pub fn run_task_list<S: Send + Sync + 'static>(
        &mut self,
        tasks: &[Job<G, CS, S>],
        pool: &ThreadPool,
        morcel_size: usize,
        shard_state: Shard<CS>,
        global_state: Global<CS>,
        mut local_state: Vec<S>,
        prev_local_state: &Vec<S>,
    ) -> (bool, Shard<CS>, Global<CS>, Vec<S>) {
        pool.install(move || {
            let mut new_shard_state = shard_state;
            let mut new_global_state = global_state;

            let mut done = false;

            for task in tasks.iter() {
                let atomic_done = AtomicBool::new(true);

                let updated_state: Option<(Shard<CS>, Global<CS>)> = match task {
                    Job::Write(task) => local_state
                        .par_chunks_mut(morcel_size)
                        .enumerate()
                        .map(|(morcel_id, morcel)| {
                            self.run_task_v2(
                                &new_shard_state,
                                &new_global_state,
                                morcel,
                                prev_local_state,
                                &atomic_done,
                                morcel_size,
                                morcel_id,
                                task,
                            )
                        })
                        .reduce_with(|a, b| self.merge_states(a, b)),
                    Job::Read(task) => {
                        local_state
                            .par_chunks_mut(morcel_size)
                            .enumerate()
                            .for_each(|(morcel_id, morcel)| {
                                self.run_task_v2(
                                    &new_shard_state,
                                    &new_global_state,
                                    morcel,
                                    prev_local_state,
                                    &atomic_done,
                                    morcel_size,
                                    morcel_id,
                                    task,
                                );
                            });
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

    fn make_cur_and_prev_states<S: Clone>(&self, init: S) -> (Vec<S>, Vec<S>) {
        let g = self.ctx.graph();

        let states: Vec<S> = vec![init; g.num_vertices()]; 

        (states.clone(), states)
    }

    pub fn run<
        B: std::fmt::Debug,
        F: FnOnce(
                GlobalState<CS>,
                EvalShardState<G, CS>,
                EvalLocalState<G, CS>,
                &Vec<S>,
            ) -> B
            + std::marker::Copy,
        S: Send + Sync + Clone + 'static + std::fmt::Debug,
    >(
        &mut self,
        init_tasks: Vec<Job<G, CS, S>>,
        tasks: Vec<Job<G, CS, S>>,
        init: S,
        f: F,
        num_threads: Option<usize>,
        steps: usize,
        shard_initial_state: Option<Shard<CS>>,
        global_initial_state: Option<Global<CS>>,
    ) -> B {
        let pool = num_threads
            .map(|nt| custom_pool(nt))
            .unwrap_or_else(|| POOL.clone());

        let morcel_size = self.ctx.graph().num_vertices().min(16_000);
        let num_chunks = self.ctx.graph().num_vertices() / morcel_size ;

        let mut shard_state = shard_initial_state.unwrap_or_else(|| Shard::new(self.ctx.graph().num_vertices(), num_chunks, morcel_size));

        let mut global_state = global_initial_state.unwrap_or_else(|| Global::new());

        let (mut cur_local_state, mut prev_local_state) = self.make_cur_and_prev_states::<S>(init);

        let mut _done = false;

        (_done, shard_state, global_state, cur_local_state) = self.run_task_list(
            &init_tasks,
            &pool,
            morcel_size,
            shard_state,
            global_state,
            cur_local_state,
            &prev_local_state,
        );

        // To allow the init step to cache stuff we will copy everything from cur_local_state to prev_local_state
        prev_local_state.clone_from_slice(&cur_local_state);

        while !_done && self.ctx.ss() < steps && tasks.len() > 0 {
            (_done, shard_state, global_state, cur_local_state) = self.run_task_list(
                &tasks,
                &pool,
                morcel_size,
                shard_state,
                global_state,
                cur_local_state,
                &prev_local_state,
            );

            // copy and reset the state from the step that just ended
            shard_state.reset(self.ctx.ss(), self.ctx.resetable_states());
            global_state.reset(self.ctx.ss(), self.ctx.resetable_states());

            // swap the two local states
            prev_local_state.clone_from_slice(&cur_local_state);
            std::mem::swap(&mut cur_local_state, &mut prev_local_state);

            // Copy and reset the local states from the step that just ended
            self.ctx.increment_ss();
        }

        let ss: usize = self.ctx.ss();
        let last_local_state = if ss % 2 == 0 {
            cur_local_state
        } else {
            prev_local_state
        };

        f(
            GlobalState::new(global_state, ss),
            EvalShardState::new(ss, self.ctx.graph(), shard_state),
            EvalLocalState::new(ss, self.ctx.graph(), vec![]),
            &last_local_state,
        )
    }
}
