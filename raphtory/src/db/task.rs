// the main execution unit of an algorithm

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use once_cell::sync::Lazy;
use rayon::prelude::*;
use rayon::{ThreadPool, ThreadPoolBuilder};

use crate::core::agg::{Accumulator, AndDef};
use crate::core::state::{self, AccId, ComputeState, ShuffleComputeState, StateType};
use crate::core::utils::get_shard_id_from_global_vid;

use super::vertex::VertexView;
use super::view_api::{GraphViewOps, VertexViewOps};

#[derive(thiserror::Error, Debug, PartialEq)]
#[error("Failure to execute Task")]

pub enum Step {
    Done,
    Continue,
}

pub trait Task<G: GraphViewOps, CS: ComputeState> {
    fn run(&self, vv: &EvalVertexView<G, CS>) -> Step;
}

pub struct ATask<G: GraphViewOps, CS: ComputeState, F: Fn(&EvalVertexView<G, CS>) -> Step> {
    f: F,
    task_type: TaskType,
    _g: std::marker::PhantomData<G>,
    _cs: std::marker::PhantomData<CS>,
}

// determines if the task is executed for all vertices or only for updated vertices (vertices that had a state change since last sync)
pub enum TaskType {
    ALL,
    UPDATED_ONLY,
}

impl<G: GraphViewOps, CS: ComputeState, F: Fn(&EvalVertexView<G, CS>) -> Step> ATask<G, CS, F> {
    fn new(task_type: TaskType, f: F) -> Self {
        Self {
            f,
            task_type,
            _g: std::marker::PhantomData,
            _cs: std::marker::PhantomData,
        }
    }
}

impl<G: GraphViewOps, CS: ComputeState, F: Fn(&EvalVertexView<G, CS>) -> Step> Task<G, CS>
    for ATask<G, CS, F>
{
    fn run(&self, vv: &EvalVertexView<G, CS>) -> Step {
        (self.f)(vv)
    }
}

type MergeFn<CS> =
    Arc<dyn Fn(&mut ShuffleComputeState<CS>, &ShuffleComputeState<CS>, usize) + Send + Sync>;

// state for the execution of a task
pub struct Context<G: GraphViewOps, CS: ComputeState> {
    ss: usize,
    g: Arc<G>,
    merge_fns: Vec<MergeFn<CS>>,
}

impl<G: GraphViewOps, CS: ComputeState> Context<G, CS> {
    fn agg<A: StateType, IN: 'static, OUT: 'static, ACC: Accumulator<A, IN, OUT>>(
        &mut self,
        id: AccId<A, IN, OUT, ACC>,
    ) {
        let fn_merge: MergeFn<CS> = Arc::new(move |a, b, ss| a.merge_mut_2(b, id, ss));

        self.merge_fns.push(fn_merge);
    }

    fn global_agg<A: StateType, IN: 'static, OUT: 'static, ACC: Accumulator<A, IN, OUT>>(
        &mut self,
        id: AccId<A, IN, OUT, ACC>,
    ) {
        let fn_merge: MergeFn<CS> = Arc::new(move |a, b, ss| a.merge_mut_global(b, id, ss));

        self.merge_fns.push(fn_merge);
    }
}

impl<G: GraphViewOps, CS: ComputeState> From<G> for Context<G, CS> {
    fn from(g: G) -> Self {
        Self {
            ss: 0,
            g: Arc::new(g),
            merge_fns: vec![],
        }
    }
}

pub struct TaskRunner<G: GraphViewOps, CS: ComputeState> {
    tasks: Vec<Box<dyn Task<G, CS> + Sync + Send>>,
    ctx: Context<G, CS>,
}

impl<G: GraphViewOps, CS: ComputeState> TaskRunner<G, CS> {
    pub fn new(tasks: Vec<Box<dyn Task<G, CS> + Sync + Send>>, ctx: Context<G, CS>) -> Self {
        Self { tasks, ctx: ctx }
    }

    fn merge_states(
        &self,
        mut a: Arc<ShuffleComputeState<CS>>,
        b: Arc<ShuffleComputeState<CS>>,
    ) -> Arc<ShuffleComputeState<CS>> {
        let left = Arc::get_mut(&mut a)
            .expect("merge_states: get_mut should always work, no other reference can exist");
        for merge_fn in self.ctx.merge_fns.iter() {
            merge_fn(left, &b, self.ctx.ss);
        }
        a
    }

    fn make_total_state<F: Fn() -> Arc<Option<ShuffleComputeState<CS>>>>(
        &self,
        num_threads: usize,
        f: F,
    ) -> Vec<(usize, Arc<Option<ShuffleComputeState<CS>>>)> {
        vec![f(); num_threads]
            .into_iter()
            .enumerate()
            .collect::<Vec<_>>()
    }

    pub fn run_task_list(
        &mut self,
        total_state: Vec<(usize, Arc<Option<ShuffleComputeState<CS>>>)>,
        num_threads: usize,
        all_stop: &AccId<bool, bool, bool, AndDef>,
    ) -> (bool, Vec<(usize, Arc<Option<ShuffleComputeState<CS>>>)>) {
        POOL.install(move || {
            let mut new_total_state = total_state;
            let mut done = false;

            for task in self.tasks.iter() {
                let updated_state = new_total_state
                    .par_iter_mut()
                    .map(|(job_id, state)| {
                        let rc_local_state =
                            Rc::new(RefCell::new(Arc::make_mut(state).take().unwrap())); // make_mut clones the state

                        for vertex in self.ctx.g.vertices() {
                            if self.is_vertex_in_job(&vertex, num_threads, *job_id) {
                                let vv = EvalVertexView::new(
                                    self.ctx.ss,
                                    vertex,
                                    rc_local_state.clone(),
                                );

                                match task.run(&vv) {
                                    Step::Done => {
                                        rc_local_state.borrow_mut().accumulate_global(
                                            self.ctx.ss,
                                            true,
                                            all_stop,
                                        );
                                    }
                                    _ => {}
                                }
                            }
                        }

                        let state: ShuffleComputeState<CS> =
                            Rc::try_unwrap(rc_local_state).unwrap().into_inner();

                        Arc::new(state)
                    })
                    .reduce_with(|a, b| self.merge_states(a, b));

                let full_stop = updated_state
                    .as_ref()
                    .and_then(|state| state.read_global(self.ctx.ss, &all_stop))
                    .unwrap_or(false);

                // restore the shape of new_total_state Vec<(usize, Arc<Option<ShuffleComputeState<CS>>>)> from updated state using num_threads for vec len

                if let Some(arc_state) = updated_state {
                    let mut state = Arc::try_unwrap(arc_state)
                        .expect("should be able to unwrap Arc, no other reference can exist");

                    state.copy_over_next_ss(self.ctx.ss);
                    let arc_state = Arc::new(Some(state));
                    new_total_state = self.make_total_state(num_threads, || arc_state.clone())
                }

                if full_stop {
                    done = true;
                    break;
                }
            }

            (done, new_total_state)
        })
    }

    pub fn run(&mut self, num_threads: Option<usize>, steps: usize) -> Vec<(usize, Arc<Option<ShuffleComputeState<CS>>>)> {
        // say we go over all the vertices on all the threads but we partition the vertices
        // on each thread and we only run the function if the vertex is in the partition

        let graph_shards = self.ctx.g.num_shards();

        let pool = num_threads
            .map(|nt| custom_pool(nt))
            .unwrap_or_else(|| POOL.clone());

        let num_threads = pool.current_num_threads();

        let all_stop = state::def::and(u32::MAX);
        self.ctx.global_agg(all_stop);

        let mut total_state = self.make_total_state(num_threads, || {
            Arc::new(Some(ShuffleComputeState::new(graph_shards)))
        });

        // the only benefit in this case is that we do not clone when we run with 1 thread

        let mut done = false;
        while !done && self.ctx.ss < steps {
            (done, total_state) = self.run_task_list(total_state, num_threads, &all_stop);
            self.ctx.ss += 1;
        }

        total_state
    }

    // use the global id of the vertex to determine if it's present in the partition
    fn is_vertex_in_job(&self, vertex: &VertexView<G>, n_jobs: usize, job_id: usize) -> bool {
        get_shard_id_from_global_vid(vertex.vertex.g_id, n_jobs) == job_id
    }
}

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

pub struct EvalVertexView<G: GraphViewOps, CS: ComputeState> {
    ss: usize,
    vv: VertexView<G>,
    state: Rc<RefCell<ShuffleComputeState<CS>>>,
}

impl<G: GraphViewOps, CS: ComputeState> EvalVertexView<G, CS> {
    fn new(ss: usize, vertex: VertexView<G>, state: Rc<RefCell<ShuffleComputeState<CS>>>) -> Self {
        Self {
            ss,
            vv: vertex,
            state,
        }
    }

    fn global_id(&self) -> u64 {
        self.vv.vertex.g_id
    }

    // TODO: do we always look-up the pid in the graph? or when calling neighbours we look-it up?
    fn pid(&self) -> usize {
        if let Some(pid) = self.vv.vertex.pid {
            pid
        } else {
            self.vv
                .graph
                .vertex(self.global_id())
                .unwrap()
                .vertex
                .pid
                .unwrap()
        }
    }

    pub fn neighbours(&self) -> impl Iterator<Item = Self> + '_ {
        self.vv
            .neighbours()
            .iter()
            .map(move |vv| EvalVertexView::new(self.ss, vv, self.state.clone()))
    }

    pub fn update<A: StateType, IN: 'static, OUT: 'static, ACC: Accumulator<A, IN, OUT>>(
        &self,
        id: &AccId<A, IN, OUT, ACC>,
        a: IN,
    ) {
        self.state
            .borrow_mut()
            .accumulate_into_pid(self.ss, self.global_id(), self.pid(), a, id)
    }

    /// Read the current value of the vertex state using the given accumulator.
    /// Returns a default value if the value is not present.
    pub fn read<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        agg_r: &AccId<A, IN, OUT, ACC>,
    ) -> OUT
    where
        A: StateType,
        OUT: std::fmt::Debug,
    {
        self.state
            .borrow()
            .read(self.ss, self.vv.id() as usize, agg_r)
            .unwrap_or(ACC::finish(&ACC::zero()))
    }

    /// Try to read the previous value of the vertex state using the given accumulator.
    pub fn try_read_prev<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        acc_id: &AccId<A, IN, OUT, ACC>,
    ) -> Result<OUT, OUT>
    where
        A: StateType,
        OUT: std::fmt::Debug,
    {
        self.state
            .borrow()
            .read(self.ss + 1, self.vv.id() as usize, acc_id)
            .ok_or(ACC::finish(&ACC::zero()))
    }

    /// Read the previous value of the vertex state using the given accumulator.
    pub fn read_prev<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        acc_id: &AccId<A, IN, OUT, ACC>,
    ) -> OUT
    where
        A: StateType,
        OUT: std::fmt::Debug,
    {
        self.try_read_prev::<A, IN, OUT, ACC>(acc_id)
            .or_else(|v| Ok::<OUT, OUT>(v))
            .unwrap()
    }
}

#[cfg(test)]
mod tasks_tests {
    use crate::{
        core::state::{self, ComputeStateVec},
        db::graph::Graph,
    };

    use super::*;

    #[test]
    fn connected_components() {
        let graph = Graph::new(4);

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
            graph.add_edge(ts, src, dst, &vec![], None).unwrap();
        }

        let mut ctx: Context<Graph, ComputeStateVec> = graph.into();

        let min = state::def::min::<u64>(0);

        // setup the aggregator to be merged post execution
        ctx.agg(min.clone());

        let step1 = ATask::new(TaskType::ALL, move |vv| {
            vv.update(&min, vv.global_id());

            for n in vv.neighbours() {
                n.update(&min, vv.global_id())
            }

            Step::Continue
        });

        let step2 = ATask::new(TaskType::ALL, move |vv| {
            let current = vv.read(&min);
            let prev = vv.read_prev(&min);

            if current == prev {
                Step::Done
            } else {
                Step::Continue
            }
        });

        let mut runner: TaskRunner<Graph, ComputeStateVec> =
            TaskRunner::new(vec![Box::new(step1), Box::new(step2)], ctx);

        let results = runner.run(Some(2), 10);

        println!("{:?}", results[0]);
    }
}
