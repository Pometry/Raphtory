use arrow2::{
    array::{Array, ListArray},
    buffer::Buffer,
    chunk::Chunk,
    types::NativeType,
};
use std::sync::Arc;

use super::list_buffer::ListColumn;

// this is a list_array<struct_array<(u64, u64)>>, where the struct_array is (node, edge)
#[derive(Debug, Clone)]
pub struct NodeChunk {
    columns: Arc<[Box<dyn Array>]>,
}

impl NodeChunk {

    pub(crate) fn empty() -> Self {
        let columns = vec![];
        NodeChunk {
            columns: columns.into(),
        }
    }
    pub(crate) fn new(chunk: Chunk<Box<dyn Array>>) -> Self {
        let columns = chunk.into_arrays().into();
        NodeChunk { columns }
    }

    pub fn adj(&self) -> &ListArray<i64> {
        self.columns[0]
            .as_any()
            .downcast_ref::<ListArray<i64>>()
            .unwrap()
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
}

#[derive(Debug)]
pub struct RowOwned<T>(Buffer<T>);

impl<T: NativeType> RowOwned<T> {
    pub(crate) fn into_iter(self) -> impl Iterator<Item = T> {
        self.0.into_iter()
    }
}
