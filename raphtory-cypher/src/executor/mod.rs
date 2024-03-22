use arrow::datatypes::ArrowPrimitiveType;
use arrow2::{array::Arrow2Arrow, types::NativeType};
use datafusion::execution::context::{SQLOptions, SessionContext};

use sqlparser::ast::{self as sql_ast};

mod table_provider;

#[derive(thiserror::Error, Debug)]
pub enum ExecError {
    #[error("Failed to create thread pool: {0}")]
    TPBuildErr(#[from] rayon::ThreadPoolBuildError),

    #[error("Layer not found: {0}")]
    LayerNotFound(String),

    #[error("Failed to execute plan: {0}")]
    DataFusionError(#[from] datafusion::error::DataFusionError),

    #[error("Arrow schema error: {0}")]
    ArrowError(#[from] arrow_schema::ArrowError),
}

pub async fn run_with_datafusion(sql: sql_ast::Statement) -> Result<(), ExecError> {
    let ctx = SessionContext::new();
    let plan = ctx
        .state()
        .statement_to_plan(datafusion::sql::parser::Statement::Statement(Box::new(sql)))
        .await?;
    let opts = SQLOptions::new();
    opts.verify_plan(&plan)?;
    ctx.execute_logical_plan(plan).await?;
    Ok(())
}

fn arrow2_to_arrow<T: NativeType, U: ArrowPrimitiveType>(buffer: &arrow2::buffer::Buffer<T>) -> arrow::array::PrimitiveArray<U> {
    let dt = arrow2::datatypes::DataType::from(<T as arrow2::types::NativeType>::PRIMITIVE);
    let prim_array = arrow2::array::PrimitiveArray::new(dt, buffer.clone(), None);
    prim_array.to_data().into()
}

#[cfg(test)]
mod test{
    

    #[tokio::test]
    async fn test_run_with_datafusion() {
        let _sql = "SELECT * FROM test";
    }

}

// #[derive(Clone, Copy)]
// pub struct Context<'graph, 'scope, 'b> {
//     scope: &'b Scope<'scope>,
//     graph: &'graph ArrowGraph,
// }

// impl<'graph, 'scope, 'b> Context<'graph, 'scope, 'b> {
//     fn new(scope: &'b Scope<'scope>, graph: &'graph ArrowGraph) -> Self {
//         Self { scope, graph }
//     }
// }

// pub struct Executor {
//     graph: ArrowGraph,
//     pipeline: Pipeline,
// }

// #[derive(Debug)]
// pub struct Pipeline {
//     source: Box<dyn Source>,
//     operators: Vec<PhysicalOperator>,
//     sink: Box<dyn Sink>,
// }

// impl Pipeline {
//     pub fn new(source: Box<dyn Source>, sink: Box<dyn Sink>) -> Self {
//         Self {
//             source,
//             operators: Vec::new(),
//             sink,
//         }
//     }

//     pub fn add_operator(&mut self, operator: PhysicalOperator) {
//         self.operators.push(operator);
//     }
// }

// pub trait Source: std::fmt::Debug + Send + Sync {
//     fn produce<'a, 'b>(
//         &'a self,
//         g: &'b ArrowGraph,
//         producer: Arc<dyn Fn(DataBlock) + Send + Sync + 'a>,
//     ) -> Result<(), ExecError>;
// }

// pub trait Sink: std::fmt::Debug + Send + Sync {
//     fn consume(&self, block: DataBlock) -> Result<(), ExecError>;
// }

// #[derive(Debug)]
// pub struct ChannelSink {
//     sender: std::sync::mpsc::Sender<DataBlock>,
// }

// impl Sink for ChannelSink {
//     fn consume(&self, block: DataBlock) -> Result<(), ExecError> {
//         self.sender.send(block)?;
//         Ok(())
//     }
// }

// impl ChannelSink {
//     pub fn new(sender: std::sync::mpsc::Sender<DataBlock>) -> Self {
//         Self { sender }
//     }
// }

// impl Executor {
//     pub fn new(graph: ArrowGraph, pipeline: Pipeline) -> Self {
//         Self { graph, pipeline }
//     }

//     pub fn execute_pipeline(self, num_threads: usize) -> Result<(), ExecError> {
//         let thread_pool = rayon::ThreadPoolBuilder::new()
//             .num_threads(num_threads)
//             .build()?;
//         let graph = &self.graph;
//         let sink = &self.pipeline.sink;
//         let operators = &self.pipeline.operators;

//         thread_pool.scope(move |scope| {
//             let source = &self.pipeline.source;

//             source.produce(
//                 graph,
//                 Arc::new(|input| {
//                     let stage = PipelineStage::new(operators);

//                     run_pipeline(stage, scope, graph, input, sink.as_ref());
//                 }),
//             )?;
//             Ok(())
//         })
//     }
// }

// fn run_pipeline<'graph: 'scope, 'scope>(
//     stage: PipelineStage<'graph>,
//     scope: &Scope<'scope>,
//     graph: &'graph ArrowGraph,
//     input: DataBlock,
//     sink: &'graph dyn Sink,
// ) {
//     if let Some((done, operator, next_stage)) = stage.next_operator() {
//         let op = operator.boxed();
//         for next_input in op.execute(input, Context::new(scope, graph)) {
//             if !done {
//                 scope.spawn(move |scope2| {
//                     run_pipeline(next_stage, scope2, graph, next_input, sink);
//                 });
//             } else {
//                 sink.consume(next_input).expect("Failed to send data block")
//             }
//         }
//     }
// }

// #[derive(Debug, Copy, Clone)]
// struct PipelineStage<'a> {
//     operators: &'a [PhysicalOperator],
//     stage: usize,
// }

// impl<'a> PipelineStage<'a> {
//     fn new(operators: &'a [PhysicalOperator]) -> Self {
//         Self {
//             operators,
//             stage: 0,
//         }
//     }

//     fn next_operator(self) -> Option<(bool, PhysicalOperator, Self)> {
//         let next = self.operators.get(self.stage)?;
//         let done = self.stage == self.operators.len() - 1;
//         Some((
//             done,
//             next.clone(),
//             Self {
//                 stage: self.stage + 1,
//                 ..self
//             },
//         ))
//     }
// }

// #[cfg(test)]
// mod test {

//     use raphtory::prelude::*;

//     use super::{operators::*, *};
//     use rand::*;

//     #[test]
//     fn edge_scan() {
//         let graph = Graph::new();

//         // generate 50 random edges format is (time, src, dst, weight, name)
//         let mut rng = rand::thread_rng();

//         let edges = (0..50)
//             .map(|_| {
//                 (
//                     rng.gen_range(0i64..100i64),
//                     format!("node_{}", rng.gen_range(0..10)),
//                     format!("node_{}", rng.gen_range(0..10)),
//                     rng.gen_range(0.0..100.0),
//                     format!("edge_{}", rng.gen_range(0..10)),
//                 )
//             })
//             .collect::<Vec<_>>();

//         for (t, src, dst, weight, name) in edges {
//             graph
//                 .add_edge(
//                     t,
//                     src,
//                     dst,
//                     [("weight", Prop::F64(weight)), ("name", Prop::str(name))],
//                     None,
//                 )
//                 .unwrap();
//         }

//         let tempdir = tempfile::tempdir().unwrap();

//         let graph = ArrowGraph::from_graph(&graph, tempdir.path()).unwrap();

//         let edge_scan = EdgeScan::new("a", "test", [("weight", 1), ("name", 2)]);
//         let (send, recv) = std::sync::mpsc::channel();
//         let pipeline = Pipeline::new(Box::new(edge_scan), Box::new(ChannelSink::new(send)));
//         let executor = Executor::new(graph, pipeline);

//         executor.execute_pipeline(1).unwrap();

//         for block in recv.iter() {
//             println!("{:?}", block);
//         }
//     }
// }
