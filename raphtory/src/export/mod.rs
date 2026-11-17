//! Export of graph data as Arrow tables: update tables with one row per update, and metadata
//! tables with one row per entity.
//!
//! Each table starts with a few fixed columns, followed by one column per property. A row leaves
//! a property's cell empty when it has no value for that property.

mod nodes;

pub use nodes::{node_metadata, node_updates};

use crate::{
    errors::GraphError,
    parquet_encoder::{RecordBatchSink, TIME_COL},
};
use arrow::{
    array::RecordBatch,
    compute::cast,
    datatypes::{DataType, Schema, SchemaBuilder, SchemaRef, TimeUnit},
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

/// `data_type` with every string view in it, nested ones included, replaced by a string.
///
/// pyarrow has no take, filter or sort kernels for string views at the moment, so pandas and
/// pyarrow can't filter, sort or even print a table that has them.
fn without_string_views(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Utf8View => DataType::Utf8,
        DataType::LargeList(field) => DataType::LargeList(Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(without_string_views(field.data_type())),
        )),
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|field| {
                    field
                        .as_ref()
                        .clone()
                        .with_data_type(without_string_views(field.data_type()))
                })
                .collect(),
        ),
        data_type => data_type.clone(),
    }
}

/// Keeps the columns at `indices` of `batches`, which follow the encoder schema `encoded`, and
/// converts string views to strings and, when `convert_datetime` is set, converts the time column
/// to UTC datetimes.
fn select_columns(
    encoded: &Schema,
    batches: Vec<RecordBatch>,
    indices: &[usize],
    convert_datetime: bool,
) -> Result<ExportTable, GraphError> {
    let projected = encoded.project(indices)?;
    let time = convert_datetime
        .then(|| projected.index_of(TIME_COL))
        .transpose()?;
    let mut builder = SchemaBuilder::from(&projected);
    for (i, field) in projected.fields().iter().enumerate() {
        let data_type = if Some(i) == time {
            datetime_type()
        } else {
            without_string_views(field.data_type())
        };
        if &data_type != field.data_type() {
            Arc::make_mut(builder.field_mut(i)).set_data_type(data_type);
        }
    }
    let schema = Arc::new(builder.finish());
    let batches = batches
        .iter()
        .map(|batch| {
            let columns = batch
                .project(indices)?
                .columns()
                .iter()
                .zip(schema.fields())
                .map(|(column, field)| {
                    if column.data_type() == field.data_type() {
                        Ok(column.clone())
                    } else {
                        cast(column, field.data_type())
                    }
                })
                .collect::<Result<_, _>>()?;
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
