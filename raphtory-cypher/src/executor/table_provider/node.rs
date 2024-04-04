use std::{any::Any, fmt::Formatter, sync::Arc};


use arrow_array::{Array};


use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::RecordBatch,
        datatypes::{SchemaRef},
    },
    common::{Statistics},
    config::ConfigOptions,
    datasource::{TableProvider, TableType},
    error::DataFusionError,
    execution::{
        context::{SessionState},
        SendableRecordBatchStream, TaskContext,
    },
    logical_expr::{Expr},
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        metrics::MetricsSet, stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType,
        ExecutionPlan, Partitioning,
    },
};
use futures::Stream;
use raphtory::arrow::{
    chunked_array::array_ops::{ArrayOps, BaseArrayOps},
    graph_impl::ArrowGraph,
};



pub struct NodeTableProvider {
    graph: ArrowGraph,
    schema: SchemaRef,
    num_partitions: usize,
}

#[async_trait]
impl TableProvider for NodeTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Get the type of this table for metadata/catalog purposes.
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let schema = projection
            .as_ref()
            .map(|proj| Arc::new(self.schema().project(proj).expect("failed projection")))
            .unwrap_or_else(|| self.schema().clone());

        Ok(Arc::new(NodeScanExecPlan {
            graph: self.graph.clone(),
            schema,
            num_partitions: self.num_partitions,
        }))
    }
}

async fn produce_record_batch(
    _graph: ArrowGraph,
    _schema: SchemaRef,
    _chunk_id: usize,
) -> Result<RecordBatch, DataFusionError> {
    todo!()
}

struct NodeScanExecPlan {
    graph: ArrowGraph,
    schema: SchemaRef,
    num_partitions: usize,
}

impl NodeScanExecPlan {
    fn stream_record_batches(
        &self,
        chunk_id: usize,
    ) -> impl Stream<Item = Result<RecordBatch, DataFusionError>> {
        futures::stream::once(produce_record_batch(
            self.graph.clone(),
            self.schema.clone(),
            chunk_id,
        ))
    }
}

impl std::fmt::Debug for NodeScanExecPlan {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "NodeScanExecPlan[projection={:?}]",
            self.schema.fields().iter().map(|f| f.name())
        )
    }
}

impl DisplayAs for NodeScanExecPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "NodeScanExecPlan[projection={:?}]",
            self.schema.fields().iter().map(|f| f.name())
        )
    }
}

#[async_trait]
impl ExecutionPlan for NodeScanExecPlan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.num_partitions)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true; self.children().len()]
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(self)
    }

    fn repartitioned(
        &self,
        _target_partitions: usize,
        _config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
        Ok(None)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let stream = self.stream_record_batches(partition);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn statistics(&self) -> Result<Statistics, DataFusionError> {
        Ok(Statistics::new_unknown(&self.schema()))
    }
}
