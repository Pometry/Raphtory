use std::sync::Arc;

use polars_core::frame::DataFrame;
use raphtory::arrow::graph_impl::ArrowGraph;
use rayon::Scope;

mod operators;
use operators::{Operator, PhysicalOperator};

#[derive(Debug, Clone)]
pub struct DataBlock {
    data: DataFrame,
}

#[derive(thiserror::Error, Debug)]
pub enum ExecError {
    #[error("Failed to create thread pool: {0}")]
    TPBuildErr(#[from] rayon::ThreadPoolBuildError),

    #[error("Layer not found: {0}")]
    LayerNotFound(String),
}

#[derive(Clone, Copy)]
struct Context<'graph, 'scope, 'b> {
    scope: &'b Scope<'scope>,
    graph: &'graph ArrowGraph,
}

impl<'graph, 'scope, 'b> Context<'graph, 'scope, 'b> {
    fn new(scope: &'b Scope<'scope>, graph: &'graph ArrowGraph) -> Self {
        Self { scope, graph }
    }
}

pub struct Executor {
    graph: ArrowGraph,
    pipeline: Pipeline,
}

pub struct Pipeline {
    source: Box<dyn Source>,
    operators: Vec<PhysicalOperator>,
    sink: Box<dyn Sink>,
}

impl Pipeline {
    pub fn new(source: impl Source + 'static, sink: impl Sink + 'static) -> Self {
        Self {
            source: Box::new(source),
            operators: Vec::new(),
            sink: Box::new(sink),
        }
    }

    pub fn add_operator(&mut self, operator: PhysicalOperator) {
        self.operators.push(operator);
    }
}

pub trait Source: Send + Sync {
    fn produce<'a, 'b>(
        &'a self,
        g: &'b ArrowGraph,
        producer: Arc<dyn Fn(DataBlock) + Send + Sync + 'a>,
    ) -> Result<(), ExecError>;
}

pub trait Sink: Send + Sync {
    fn consume(&self, block: DataBlock) -> Result<(), ExecError>;
}

pub struct ChannelSink {
    sender: std::sync::mpsc::Sender<DataBlock>,
}

impl Sink for ChannelSink {
    fn consume(&self, block: DataBlock) -> Result<(), ExecError> {
        Ok(())
    }
}

impl ChannelSink {
    pub fn new(sender: std::sync::mpsc::Sender<DataBlock>) -> Self {
        Self { sender }
    }
}

impl Executor {
    pub fn new(graph: ArrowGraph, pipeline: Pipeline) -> Self {
        Self { graph, pipeline }
    }

    pub fn execute_pipeline(self, num_threads: usize) -> Result<(), ExecError> {
        let thread_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build()?;
        let graph = &self.graph;
        let operators = &self.pipeline.operators;

        thread_pool.scope(move |scope| {
            let source = &self.pipeline.source;

            source.produce(graph, Arc::new(|input| {
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
    graph: &'graph ArrowGraph,
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

#[cfg(test)]
mod test {

    use raphtory::prelude::*;

    use super::operators::*;
    use super::*;
    use rand::*;

    #[test]
    fn edge_scan() {
        let graph = Graph::new();

        // generate 50 random edges format is (time, src, dst, weight, name)
        let mut rng = rand::thread_rng();

        let edges = (0..50)
            .map(|_| {
                (
                    rng.gen_range(0i64..100i64),
                    format!("node_{}", rng.gen_range(0..10)),
                    format!("node_{}", rng.gen_range(0..10)),
                    rng.gen_range(0.0..100.0),
                    format!("edge_{}", rng.gen_range(0..10)),
                )
            })
            .collect::<Vec<_>>();

        for (t, src, dst, weight, name) in edges {
            graph
                .add_edge(
                    t,
                    src,
                    dst,
                    [("weight", Prop::F64(weight)), ("name", Prop::str(name))],
                    None,
                )
                .unwrap();
        }

        let tempdir = tempfile::tempdir().unwrap();

        let graph = ArrowGraph::from_graph(&graph, tempdir.path()).unwrap();

        let edge_scan = EdgeScan::new("test", [("weight", 1), ("name", 2)]);
        let (send, recv) = std::sync::mpsc::channel();
        let pipeline = Pipeline::new(edge_scan, ChannelSink::new(send));
        let executor = Executor::new(graph, pipeline);

        executor.execute_pipeline(1).unwrap();

        for block in recv.iter() {
            println!("{:?}", block);
        }
    }
}
