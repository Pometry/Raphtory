// the main execution unit of an algorithm

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use once_cell::sync::Lazy;
use rayon::prelude::*;
use rayon::{ThreadPool, ThreadPoolBuilder};

use crate::core::agg::Accumulator;
use crate::core::state::{AccId, ComputeState, ShuffleComputeState, StateType};
use crate::core::utils::get_shard_id_from_global_vid;

use super::vertex::VertexView;
use super::view_api::{GraphViewOps, VertexViewOps};

#[derive(thiserror::Error, Debug, PartialEq)]
#[error("Failure to execute Task")]
struct TaskError {}

trait Task<G: GraphViewOps, CS: ComputeState> {
    fn run(&self, vv: &EvalVertexView<G, CS>) -> Result<(), TaskError>;
}

struct AnonymousTask<
    G: GraphViewOps,
    CS: ComputeState,
    F: Fn(&EvalVertexView<G, CS>) -> Result<(), TaskError>,
> {
    f: F,
    _g: std::marker::PhantomData<G>,
    _cs: std::marker::PhantomData<CS>,
}

impl<G: GraphViewOps, CS: ComputeState, F: Fn(&EvalVertexView<G, CS>) -> Result<(), TaskError>>
    AnonymousTask<G, CS, F>
{
    fn new(f: F) -> Self {
        Self {
            f,
            _g: std::marker::PhantomData,
            _cs: std::marker::PhantomData,
        }
    }
}

impl<G: GraphViewOps, CS: ComputeState, F: Fn(&EvalVertexView<G, CS>) -> Result<(), TaskError>>
    Task<G, CS> for AnonymousTask<G, CS, F>
{
    fn run(&self, vv: &EvalVertexView<G, CS>) -> Result<(), TaskError> {
        (self.f)(vv)
    }
}

type MergeFn<CS> =
    Arc<dyn Fn(&mut ShuffleComputeState<CS>, &ShuffleComputeState<CS>, usize) + Send + Sync>;

// state for the execution of a task
struct Context<G: GraphViewOps, CS: ComputeState> {
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

struct TaskRunner<G: GraphViewOps, CS: ComputeState> {
    tasks: Vec<Box<dyn Task<G, CS> + Sync + Send>>,
    ctx: Context<G, CS>,
}

impl<G: GraphViewOps, CS: ComputeState> TaskRunner<G, CS> {
    fn new(tasks: Vec<Box<dyn Task<G, CS> + Sync + Send>>, ctx: Context<G, CS>) -> Self {
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

    fn run(&mut self) {
        // say we go over all the vertices on all the threads but we partition the vertices
        // on each thread and we only run the function if the vertex is in the partition
        let num_threads = POOL.current_num_threads();

        // the only benefit in this case is that we do not clone when we run with 1 thread

        let mut total_state =
            vec![Arc::new(Some(ShuffleComputeState::new(num_threads))); num_threads]
                .into_iter()
                .enumerate()
                .collect::<Vec<_>>();

        let new_total_state = POOL.install(|| {
            total_state
                .par_iter_mut()
                .map(|(job_id, state)| {
                    let rc_local_state =
                        Rc::new(RefCell::new(Arc::make_mut(state).take().unwrap())); // make_mut clones the state

                    for vertex in self.ctx.g.vertices() {
                        if self.is_vertex_in_partition(&vertex, num_threads, *job_id) {
                            let vv = EvalVertexView::new(self.ctx.ss, vertex, rc_local_state.clone());
                            for task in self.tasks.iter() {
                                task.run(&vv).expect("task run");
                            }
                        }
                    }

                    let state: ShuffleComputeState<CS> =
                        Rc::try_unwrap(rc_local_state).unwrap().into_inner();

                    Arc::new(state)
                })
                .reduce_with(|a, b| self.merge_states(a, b))
        });

        println!("new_total_state: {:?}", new_total_state);
    }

    // use the global id of the vertex to determine if it's present in the partition
    fn is_vertex_in_partition(&self, vertex: &VertexView<G>, n_jobs: usize, job_id: usize) -> bool {
        get_shard_id_from_global_vid(vertex.vertex.g_id, n_jobs) == job_id
    }
}

pub static POOL: Lazy<ThreadPool> = Lazy::new(|| {
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

    ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build()
        .unwrap()
});

struct EvalVertexView<G: GraphViewOps, CS: ComputeState> {
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

    fn update<A: StateType, IN: 'static, OUT: 'static, ACC: Accumulator<A, IN, OUT>>(
        &self,
        id: &AccId<A, IN, OUT, ACC>,
        a: IN,
    ) {
        self.state
            .borrow_mut()
            .accumulate_into_pid(self.ss, self.global_id(), self.pid(), a, id)
    }
}

#[cfg(test)]
mod tasks_tests {
    use crate::{
        core::state::{self, ComputeStateMap},
        db::graph::Graph,
    };

    use super::*;

    #[test]
    fn run_tasks_over_a_graph() {
        let graph = Graph::new(4);

        graph.add_edge(1, 2, 0, &vec![], None).expect("add edge");

        let mut ctx: Context<Graph, ComputeStateMap> = graph.into();

        let min = state::def::min::<u64>(0);

        // setup the aggregator to be merged post execution
        ctx.agg(min.clone());

        let ann = AnonymousTask::new(move |vv| {
            let t_id = std::thread::current().id();
            println!("vertex: {} {t_id:?}", vv.global_id());

            vv.update(&min, vv.global_id());

            for n in vv.neighbours() {
                n.update(&min, vv.global_id())
            }

            Ok(())
        });

        let mut runner: TaskRunner<Graph, ComputeStateMap> =
            TaskRunner::new(vec![Box::new(ann)], ctx);

        runner.run();
    }
}
