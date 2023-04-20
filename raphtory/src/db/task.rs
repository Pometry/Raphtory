// the main execution unit of an algorithm

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use once_cell::sync::Lazy;
use rayon::prelude::*;
use rayon::{prelude::IntoParallelRefIterator, ThreadPool, ThreadPoolBuilder};

use crate::core::state::ShuffleComputeState;

use super::vertex::VertexView;
use super::{program::EvalVertexView, view_api::GraphViewOps};

#[derive(thiserror::Error, Debug, PartialEq)]
#[error("Failure to execute Task")]
struct TaskError {}

trait Task<G: GraphViewOps> {
    fn run(&self, vv: &EvalVertexView<G>, ctx: &Context<G>) -> Result<(), TaskError>;
}

struct AnonymousTask<
    G: GraphViewOps,
    F: Fn(&EvalVertexView<G>, &Context<G>) -> Result<(), TaskError>,
> {
    f: F,
    _g: std::marker::PhantomData<G>,
}

impl<G: GraphViewOps, F: Fn(&EvalVertexView<G>, &Context<G>) -> Result<(), TaskError>>
    AnonymousTask<G, F>
{
    fn new(f: F) -> Self {
        Self {
            f,
            _g: std::marker::PhantomData,
        }
    }
}

impl<G: GraphViewOps, F: Fn(&EvalVertexView<G>, &Context<G>) -> Result<(), TaskError>> Task<G>
    for AnonymousTask<G, F>
{
    fn run(&self, vv: &EvalVertexView<G>, ctx: &Context<G>) -> Result<(), TaskError> {
        (self.f)(vv, ctx)
    }
}

// state for the execution of a task
struct Context<G: GraphViewOps> {
    ss: usize,
    g: G,
}

struct TaskRunner<G: GraphViewOps> {
    tasks: Vec<Box<dyn Task<G> + Sync + Send>>,
    ctx: Arc<Context<G>>,
}

impl<G: GraphViewOps> TaskRunner<G> {
    fn new(tasks: Vec<Box<dyn Task<G> + Sync + Send>>, ctx: Context<G>) -> Self {
        Self {
            tasks,
            ctx: Arc::new(ctx),
        }
    }

    fn run(&self) {
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
    fn is_vertex_in_partition(
        &self,
        vertex: &VertexView<G>,
        n_jobs: usize,
        job_id: usize,
    ) -> bool {
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
    use crate::db::graph::Graph;

    use super::*;

    #[test]
    fn run_tasks_over_a_graph() {
        let graph = Graph::new(4);

        graph.add_edge(1, 2, 0, &vec![], None).expect("add edge");

        let ann = AnonymousTask::new(|vv, ctx| {
            let t_id = std::thread::current().id();
            println!("vertex: {} {t_id:?}", vv.global_id());
            Ok(())
        });

        let runner = TaskRunner::new(vec![Box::new(ann)], Context { ss: 0, g: graph });

        runner.run();


    }
}
