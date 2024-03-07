use std::{num::NonZeroUsize, sync::Arc};

use arrow2::array::Array;
use raphtory::arrow::graph_impl::Graph2;
use rayon::Scope;

pub mod expr;
mod operators;
use operators::{Operator, PhysicalOperator};

struct DataBlock {
    cols: Vec<Box<dyn Array>>,
}

#[derive(thiserror::Error, Debug)]
pub enum ExecError {
    #[error("Failed to create thread pool: {0}")]
    TPBuildErr(#[from] rayon::ThreadPoolBuildError),
}

#[derive(Clone, Copy)]
struct Context<'graph, 'scope, 'b> {
    scope: &'b Scope<'scope>,
    graph: &'graph Graph2,
}

impl<'graph, 'scope, 'b> Context<'graph, 'scope, 'b> {
    fn new(scope: &'b Scope<'scope>, graph: &'graph Graph2) -> Self {
        Self { scope, graph }
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
    fn produce<'a>(&'a self, producer: Arc<dyn Fn(DataBlock) + 'a>) -> Result<(), ExecError>;
}

trait Sink: Send + Sync {
    fn consume(&self, block: DataBlock) -> Result<(), ExecError>;
}

impl Executor {
    fn new(graph: Graph2, pipeline: Pipeline) -> Self {
        Self { graph, pipeline }
    }

    pub fn execute_pipeline(self, num_threads: NonZeroUsize) -> Result<(), ExecError> {
        let thread_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads.get())
            .build()?;
        let graph = &self.graph;
        let operators = &self.pipeline.operators;

        thread_pool.scope(move |scope| {
            let source = &self.pipeline.source;

            source.produce(Arc::new(|input| {
                let stage = PipelineStage::new(operators);

                run_pipeline(stage, scope, graph, input);
            }))?;
            Ok(())
        })
    }
}

fn run_pipeline<'graph: 'scope, 'scope>(
    stage: PipelineStage<'graph>,
    scope: &Scope<'scope>,
    graph: &'graph Graph2,
    input: DataBlock,
) {
    if let Some((operator, next_stage)) = stage.next_operator() {
        match operator {
            PhysicalOperator::Expand(expand) => {
                for next_input in expand.execute(input, Context::new(scope, graph)) {
                    scope.spawn(move |scope2| {
                        run_pipeline(next_stage, scope2, graph, next_input);
                    });
                }
            }
            PhysicalOperator::Filter(filter) => {
                for next_input in filter.execute(input, Context::new(scope, graph)) {
                    scope.spawn(move |scope2| {
                        run_pipeline(next_stage, scope2, graph, next_input);
                    });
                }
            }
        }
    }
}

#[derive(Debug, Copy, Clone)]
struct PipelineStage<'a> {
    operators: &'a [PhysicalOperator],
    stage: usize,
}

impl<'a> PipelineStage<'a> {
    fn new(operators: &'a [PhysicalOperator]) -> Self {
        Self {
            operators,
            stage: 0,
        }
    }

    fn next_operator(self) -> Option<(PhysicalOperator, Self)> {
        let next = self.operators.get(self.stage)?;
        Some((
            next.clone(),
            Self {
                stage: self.stage + 1,
                ..self
            },
        ))
    }
}
