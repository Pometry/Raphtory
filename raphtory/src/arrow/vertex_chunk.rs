use arrow2::{
    array::{Array, ListArray, PrimitiveArray, StructArray},
    buffer::Buffer,
    chunk::Chunk,
    datatypes::Schema,
    types::NativeType,
};
use itertools::Itertools;
use std::sync::Arc;

use rayon::prelude::*;

use super::list_buffer::ListColumn;

use arrow_array::ArrayRef;

// this is a list_array<struct_array<(u64, u64)>>, where the struct_array is (vertex, edge)
#[derive(Debug, Clone)]
pub(crate) struct VertexChunk {
    columns: Arc<[Box<dyn Array>]>,
    schema: Schema,
}

impl VertexChunk {
    pub(crate) fn new(chunk: Chunk<Box<dyn Array>>, schema: Schema) -> Self {
        let columns = chunk.into_arrays().into();
        VertexChunk { columns, schema }
    }

    pub(crate) fn neighbours_col(&self) -> Option<ListColumn<u64>> {
        self.columns[0]
            .as_any()
            .downcast_ref::<ListArray<i64>>()
            .and_then(|list| ListColumn::new(list, 0))
    }

    pub(crate) fn edge_col(&self) -> Option<ListColumn<u64>> {
        self.columns[0]
            .as_any()
            .downcast_ref::<ListArray<i64>>()
            .and_then(|list| ListColumn::new(list, 1))
    }

    pub(crate) fn neighbours_own(&self, row: usize) -> Option<RowOwned<u64>> {
        let col = self.neighbours_col()?;
        Some(RowOwned(col.value(row)))
    }

    pub(crate) fn edges_own(&self, row: usize) -> Option<RowOwned<u64>> {
        let col = self.edge_col()?;
        Some(RowOwned(col.value(row)))
    }

    pub(crate) fn len(&self) -> usize {
        self.columns[0].len()
    }

    // convert to arrow-rs format
    pub(crate) fn to_arrow(&self) -> Arc<[ArrayRef]> {
        self.columns
            .iter()
            .map(|x| ArrayRef::from(x.clone()))
            .collect()
    }

    pub(crate) fn to_arrow_fields(&self) -> Vec<arrow_schema::Field> {
        self.schema
            .fields
            .iter()
            .map(|field| super::to_arrow_field(field))
            .collect()
    }
}

#[derive(Debug)]
pub struct RowOwned<T>(Buffer<T>);

impl<T: NativeType> RowOwned<T> {
    pub fn new(row: Buffer<T>) -> Self {
        RowOwned(row)
    }

    pub(crate) fn into_iter(self) -> impl Iterator<Item = T> {
        self.0.into_iter()
    }

    pub(crate) fn par_iter(&self) -> impl rayon::iter::ParallelIterator<Item = &T> + '_ {
        self.0.par_iter()
    }
}
