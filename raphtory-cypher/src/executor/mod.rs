use std::{num::NonZeroUsize, sync::Arc};

use arrow2::array::Array;
use raphtory::arrow::graph_impl::Graph2;
use rayon::{Scope, ThreadPool};

pub mod expr;
mod operators;
use operators::PhysicalOperator;

struct DataBlock {
    cols: Vec<Box<dyn Array>>,
}

#[derive(thiserror::Error, Debug)]
pub enum ExecError {
    #[error("Failed to create thread pool: {0}")]
    TPBuildErr(#[from] rayon::ThreadPoolBuildError),
}

struct Context<'graph, 's, 'b> {
    graph: &'graph Graph2,
    scope: &'b Scope<'s>,
}

impl<'graph, 's, 'b> Context<'graph, 's, 'b> {
    fn new(graph: &'graph Graph2, scope: &'b Scope<'s>) -> Self {
        Self { graph, scope }
    }
}

struct Executor {
    graph: Graph2,
    pipeline: Pipeline,
}

struct Pipeline {
    source: Box<dyn Source>,
    operators: Vec<PhysicalOperator>,
    sink: Box<dyn Sink>,
}

trait Source: Send + Sync {
    fn produce(&mut self, producer: Arc<dyn Fn(DataBlock)>) -> Result<(), ExecError>;
}

trait Sink {
    fn consume(&mut self, block: DataBlock) -> Result<(), ExecError>;
}

impl Executor {
    fn new(graph: Graph2, pipeline: Pipeline) -> Self {
        Self { graph, pipeline }
    }

    fn execute_pipeline(&mut self, num_threads: NonZeroUsize) -> Result<(), ExecError> {
        let thread_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads.get())
            .build()?;

        let source = &mut self.pipeline.source;

        thread_pool.scope(|scope| {
            let context = Context::new(&self.graph, scope);
            source.produce(Arc::new(|block| {}))?;
            Ok(())
        })
    }
}
