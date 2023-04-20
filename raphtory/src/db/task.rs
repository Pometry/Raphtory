// the main execution unit of an algorithm

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use itertools::Merge;
use once_cell::sync::Lazy;
use rayon::prelude::*;
use rayon::{prelude::IntoParallelRefIterator, ThreadPool, ThreadPoolBuilder};

use crate::core::agg::Accumulator;
use crate::core::state::{AccId, ComputeState, ShuffleComputeState, StateType};

use super::vertex::VertexView;
use super::{program::EvalVertexView, view_api::GraphViewOps};

#[derive(thiserror::Error, Debug, PartialEq)]
#[error("Failure to execute Task")]
struct TaskError {}

trait Task<G: GraphViewOps, CS: ComputeState> {
    fn run(&self, vv: &EvalVertexView<G, CS>, ctx: &Context<G, CS>) -> Result<(), TaskError>;
}

struct AnonymousTask<
    G: GraphViewOps,
    CS: ComputeState,
    F: Fn(&EvalVertexView<G, CS>, &Context<G, CS>) -> Result<(), TaskError>,
> {
    f: F,
    _g: std::marker::PhantomData<G>,
    _cs: std::marker::PhantomData<CS>,
}

impl<
        G: GraphViewOps,
        CS: ComputeState,
        F: Fn(&EvalVertexView<G, CS>, &Context<G, CS>) -> Result<(), TaskError>,
    > AnonymousTask<G, CS, F>
{
    fn new(f: F) -> Self {
        Self {
            f,
            _g: std::marker::PhantomData,
            _cs: std::marker::PhantomData,
        }
    }
}

impl<
        G: GraphViewOps,
        CS: ComputeState,
        F: Fn(&EvalVertexView<G, CS>, &Context<G, CS>) -> Result<(), TaskError>,
    > Task<G, CS> for AnonymousTask<G, CS, F>
{
    fn run(&self, vv: &EvalVertexView<G, CS>, ctx: &Context<G, CS>) -> Result<(), TaskError> {
        (self.f)(vv, ctx)
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

    fn run(&mut self) {
        // say we go over all the vertices on all the threads but we partition the vertices
        // on each thread and we only run the function if the vertex is in the partition
        let job_ids: Vec<usize> = (0..POOL.current_num_threads()).into_iter().collect();

        POOL.install(|| {
            job_ids.par_iter().for_each(|job_id| {
                let local_state = Rc::new(RefCell::new(ShuffleComputeState::new(job_ids.len())));
                for vertex in self.ctx.g.vertices() {
                    if self.is_vertex_in_partition(&vertex, job_ids.len(), *job_id) {
                        let vv = EvalVertexView::new(self.ctx.ss, vertex, local_state.clone());
                        for task in self.tasks.iter() {
                            task.run(&vv, &self.ctx).expect("task run");
                        }
                    }
                }
            })
        })
    }

    // use the global id of the vertex to determine if it's present in the partition
    fn is_vertex_in_partition(&self, vertex: &VertexView<G>, n_jobs: usize, job_id: usize) -> bool {
        (vertex.vertex.g_id as usize) % n_jobs == job_id
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

        ctx.agg(state::def::sum::<usize>(0));

        let ann = AnonymousTask::new(|vv, ctx| {
            let t_id = std::thread::current().id();
            println!("vertex: {} {t_id:?}", vv.global_id());
            Ok(())
        });

        let mut runner: TaskRunner<Graph, ComputeStateMap> =
            TaskRunner::new(vec![Box::new(ann)], ctx);

        runner.run();
    }
}
