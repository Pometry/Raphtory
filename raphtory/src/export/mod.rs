//! Export of graph data as Arrow tables with one row per update.
//!
//! Each table starts with a few fixed columns, followed by one column per property. A row leaves
//! a property's cell empty when its update did not set that property.

mod nodes;

pub use nodes::node_updates;

use crate::{
    errors::GraphError,
    parquet_encoder::{RecordBatchSink, TIME_COL},
};
use arrow::{
    array::RecordBatch,
    compute::cast,
    datatypes::{DataType, Schema, SchemaRef, TimeUnit},
};
use parking_lot::Mutex;
use std::sync::Arc;

/// An Arrow table: a schema and the record batches that follow it.
#[derive(Debug, Clone)]
pub struct ExportTable {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

/// The type of the time column when times are converted to datetimes.
fn datetime_type() -> DataType {
    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
}

/// Keeps the columns at `indices` of `batches`, which follow the encoder schema `encoded`, and
/// converts the time column to UTC datetimes when `convert_datetime` is set.
fn select_columns(
    encoded: &Schema,
    batches: Vec<RecordBatch>,
    indices: &[usize],
    convert_datetime: bool,
) -> Result<ExportTable, GraphError> {
    let mut schema = encoded.project(indices)?;
    let time = schema.index_of(TIME_COL)?;
    if convert_datetime {
        let mut fields: Vec<_> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        fields[time] = fields[time].clone().with_data_type(datetime_type());
        schema = Schema::new(fields);
    }
    let schema = Arc::new(schema);
    let batches = batches
        .iter()
        .map(|batch| {
            let mut columns = batch.project(indices)?.columns().to_vec();
            if convert_datetime {
                columns[time] = cast(&columns[time], &datetime_type())?;
            }
            Ok(RecordBatch::try_new(schema.clone(), columns)?)
        })
        .collect::<Result<_, GraphError>>()?;
    Ok(ExportTable { schema, batches })
}

/// Gathers the batches an encoder produces, keeping each chunk's batches together so they can be
/// returned in chunk order.
#[derive(Default)]
struct ChunkCollector {
    chunks: Mutex<Vec<(usize, Vec<RecordBatch>)>>,
}

impl ChunkCollector {
    fn sink(&self, chunk: usize) -> ChunkSink<'_> {
        ChunkSink {
            chunk,
            batches: vec![],
            collector: self,
        }
    }

    fn into_batches(self) -> Vec<RecordBatch> {
        let mut chunks = self.chunks.into_inner();
        chunks.sort_unstable_by_key(|(chunk, _)| *chunk);
        chunks
            .into_iter()
            .flat_map(|(_, batches)| batches)
            .collect()
    }
}

struct ChunkSink<'a> {
    chunk: usize,
    batches: Vec<RecordBatch>,
    collector: &'a ChunkCollector,
}

impl RecordBatchSink for ChunkSink<'_> {
    fn send_batch(&mut self, batch: RecordBatch) -> Result<(), GraphError> {
        self.batches.push(batch);
        Ok(())
    }

    fn finish(self) -> Result<(), GraphError> {
        self.collector
            .chunks
            .lock()
            .push((self.chunk, self.batches));
        Ok(())
    }
}
