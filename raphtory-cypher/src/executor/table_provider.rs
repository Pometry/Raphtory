use std::{any::Any, fmt::Formatter, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::{array::RecordBatch, datatypes::SchemaRef}, common::Statistics, config::ConfigOptions, datasource::{TableProvider, TableType}, error::DataFusionError, execution::{context::SessionState, SendableRecordBatchStream, TaskContext}, logical_expr::Expr, physical_expr::PhysicalSortExpr, physical_plan::{metrics::MetricsSet, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning}
};
use futures::Stream;
use raphtory::arrow::graph_impl::ArrowGraph;

struct EdgeListTableProvider {
    layer_id: usize,
    graph: ArrowGraph,
}

#[async_trait]
impl TableProvider for EdgeListTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        todo!()
    }

    /// Get the type of this table for metadata/catalog purposes.
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        todo!()
    }
}

struct EdgeListExecPlan{
    layer_id: usize,
    graph: ArrowGraph,
}

impl EdgeListExecPlan {


    fn stream_record_batches(&self) -> impl Stream<Item = Result<RecordBatch, DataFusionError>> {
        futures::stream::empty()
    }

}

impl std::fmt::Debug for EdgeListExecPlan {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "EdgeListExecPlan")
    }
}

impl DisplayAs for EdgeListExecPlan {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        todo!()
    }
}

#[async_trait]
impl ExecutionPlan for EdgeListExecPlan {
    /// Returns the execution plan as [`Any`] so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any{
        self
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef{
        todo!()
    }

    /// Specifies how the output of this `ExecutionPlan` is split into
    /// partitions.
    fn output_partitioning(&self) -> Partitioning{
        todo!()
    }

    /// If the output of this `ExecutionPlan` within each partition is sorted,
    /// returns `Some(keys)` with the description of how it was sorted.
    ///
    /// For example, Sort, (obviously) produces sorted output as does
    /// SortPreservingMergeStream. Less obviously `Projection`
    /// produces sorted output if its input was sorted as it does not
    /// reorder the input rows,
    ///
    /// It is safe to return `None` here if your `ExecutionPlan` does not
    /// have any particular output order here
    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]>{
        todo!()
    }

    /// Returns `false` if this `ExecutionPlan`'s implementation may reorder
    /// rows within or between partitions.
    ///
    /// For example, Projection, Filter, and Limit maintain the order
    /// of inputs -- they may transform values (Projection) or not
    /// produce the same number of rows that went in (Filter and
    /// Limit), but the rows that are produced go in the same way.
    ///
    /// DataFusion uses this metadata to apply certain optimizations
    /// such as automatically repartitioning correctly.
    ///
    /// The default implementation returns `false`
    ///
    /// WARNING: if you override this default, you *MUST* ensure that
    /// the `ExecutionPlan`'s maintains the ordering invariant or else
    /// DataFusion may produce incorrect results.
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true; self.children().len()]
    }

    /// Get a list of children `ExecutionPlan`s that act as inputs to this plan.
    /// The returned list will be empty for leaf nodes such as scans, will contain
    /// a single value for unary nodes, or two values for binary nodes (such as
    /// joins).
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>>{
        vec![]
    }

    /// Returns a new `ExecutionPlan` where all existing children were replaced
    /// by the `children`, in order
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError>{
        Ok(self)
    }

    /// If supported, attempt to increase the partitioning of this `ExecutionPlan` to
    /// produce `target_partitions` partitions.
    ///
    /// If the `ExecutionPlan` does not support changing its partitioning,
    /// returns `Ok(None)` (the default).
    ///
    /// It is the `ExecutionPlan` can increase its partitioning, but not to the
    /// `target_partitions`, it may return an ExecutionPlan with fewer
    /// partitions. This might happen, for example, if each new partition would
    /// be too small to be efficiently processed individually.
    ///
    /// The DataFusion optimizer attempts to use as many threads as possible by
    /// repartitioning its inputs to match the target number of threads
    /// available (`target_partitions`). Some data sources, such as the built in
    /// CSV and Parquet readers, implement this method as they are able to read
    /// from their input files in parallel, regardless of how the source data is
    /// split amongst files.
    fn repartitioned(
        &self,
        _target_partitions: usize,
        _config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
        Ok(None)
    }

    /// Begin execution of `partition`, returning a [`Stream`] of
    /// [`RecordBatch`]es.
    ///
    /// # Notes
    ///
    /// The `execute` method itself is not `async` but it returns an `async`
    /// [`futures::stream::Stream`]. This `Stream` should incrementally compute
    /// the output, `RecordBatch` by `RecordBatch` (in a streaming fashion).
    /// Most `ExecutionPlan`s should not do any work before the first
    /// `RecordBatch` is requested from the stream.
    ///
    /// [`RecordBatchStreamAdapter`] can be used to convert an `async`
    /// [`Stream`] into a [`SendableRecordBatchStream`].
    ///
    /// Using `async` `Streams` allows for network I/O during execution and
    /// takes advantage of Rust's built in support for `async` continuations and
    /// crate ecosystem.
    ///
    /// [`Stream`]: futures::stream::Stream
    /// [`StreamExt`]: futures::stream::StreamExt
    /// [`TryStreamExt`]: futures::stream::TryStreamExt
    /// [`RecordBatchStreamAdapter`]: crate::stream::RecordBatchStreamAdapter
    ///
    /// # Cancellation / Aborting Execution
    ///
    /// The [`Stream`] that is returned must ensure that any allocated resources
    /// are freed when the stream itself is dropped. This is particularly
    /// important for [`spawn`]ed tasks or threads. Unless care is taken to
    /// "abort" such tasks, they may continue to consume resources even after
    /// the plan is dropped, generating intermediate results that are never
    /// used.
    ///
    /// See [`AbortOnDropSingle`], [`AbortOnDropMany`] and
    /// [`RecordBatchReceiverStreamBuilder`] for structures to help ensure all
    /// background tasks are cancelled.
    ///
    /// [`spawn`]: tokio::task::spawn
    /// [`AbortOnDropSingle`]: crate::common::AbortOnDropSingle
    /// [`AbortOnDropMany`]: crate::common::AbortOnDropMany
    /// [`RecordBatchReceiverStreamBuilder`]: crate::stream::RecordBatchReceiverStreamBuilder
    ///
    /// # Implementation Examples
    ///
    /// While `async` `Stream`s have a non trivial learning curve, the
    /// [`futures`] crate provides [`StreamExt`] and [`TryStreamExt`]
    /// which help simplify many common operations.
    ///
    /// Here are some common patterns:
    ///
    /// ## Return Precomputed `RecordBatch`
    ///
    /// We can return a precomputed `RecordBatch` as a `Stream`:
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow_array::RecordBatch;
    /// # use arrow_schema::SchemaRef;
    /// # use datafusion_common::Result;
    /// # use datafusion_execution::{SendableRecordBatchStream, TaskContext};
    /// # use datafusion_physical_plan::memory::MemoryStream;
    /// # use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
    /// struct MyPlan {
    ///     batch: RecordBatch,
    /// }
    ///
    /// impl MyPlan {
    ///     fn execute(
    ///         &self,
    ///         partition: usize,
    ///         context: Arc<TaskContext>
    ///     ) -> Result<SendableRecordBatchStream> {
    ///         // use functions from futures crate convert the batch into a stream
    ///         let fut = futures::future::ready(Ok(self.batch.clone()));
    ///         let stream = futures::stream::once(fut);
    ///         Ok(Box::pin(RecordBatchStreamAdapter::new(self.batch.schema(), stream)))
    ///     }
    /// }
    /// ```
    ///
    /// ## Lazily (async) Compute `RecordBatch`
    ///
    /// We can also lazily compute a `RecordBatch` when the returned `Stream` is polled
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow_array::RecordBatch;
    /// # use arrow_schema::SchemaRef;
    /// # use datafusion_common::Result;
    /// # use datafusion_execution::{SendableRecordBatchStream, TaskContext};
    /// # use datafusion_physical_plan::memory::MemoryStream;
    /// # use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
    /// struct MyPlan {
    ///     schema: SchemaRef,
    /// }
    ///
    /// /// Returns a single batch when the returned stream is polled
    /// async fn get_batch() -> Result<RecordBatch> {
    ///     todo!()
    /// }
    ///
    /// impl MyPlan {
    ///     fn execute(
    ///         &self,
    ///         partition: usize,
    ///         context: Arc<TaskContext>
    ///     ) -> Result<SendableRecordBatchStream> {
    ///         let fut = get_batch();
    ///         let stream = futures::stream::once(fut);
    ///         Ok(Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream)))
    ///     }
    /// }
    /// ```
    ///
    /// ## Lazily (async) create a Stream
    ///
    /// If you need to to create the return `Stream` using an `async` function,
    /// you can do so by flattening the result:
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow_array::RecordBatch;
    /// # use arrow_schema::SchemaRef;
    /// # use futures::TryStreamExt;
    /// # use datafusion_common::Result;
    /// # use datafusion_execution::{SendableRecordBatchStream, TaskContext};
    /// # use datafusion_physical_plan::memory::MemoryStream;
    /// # use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
    /// struct MyPlan {
    ///     schema: SchemaRef,
    /// }
    ///
    /// /// async function that returns a stream
    /// async fn get_batch_stream() -> Result<SendableRecordBatchStream> {
    ///     todo!()
    /// }
    ///
    /// impl MyPlan {
    ///     fn execute(
    ///         &self,
    ///         partition: usize,
    ///         context: Arc<TaskContext>
    ///     ) -> Result<SendableRecordBatchStream> {
    ///         // A future that yields a stream
    ///         let fut = get_batch_stream();
    ///         // Use TryStreamExt::try_flatten to flatten the stream of streams
    ///         let stream = futures::stream::once(fut).try_flatten();
    ///         Ok(Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream)))
    ///     }
    /// }
    /// ```
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError>{
        todo!()
    }

    /// Return a snapshot of the set of [`Metric`]s for this
    /// [`ExecutionPlan`]. If no `Metric`s are available, return None.
    ///
    /// While the values of the metrics in the returned
    /// [`MetricsSet`]s may change as execution progresses, the
    /// specific metrics will not.
    ///
    /// Once `self.execute()` has returned (technically the future is
    /// resolved) for all available partitions, the set of metrics
    /// should be complete. If this function is called prior to
    /// `execute()` new metrics may appear in subsequent calls.
    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    /// Returns statistics for this `ExecutionPlan` node. If statistics are not
    /// available, should return [`Statistics::new_unknown`] (the default), not
    /// an error.
    fn statistics(&self) -> Result<Statistics, DataFusionError> {
        Ok(Statistics::new_unknown(&self.schema()))
    }
}
