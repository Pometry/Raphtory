use arrow2::{
    array::{Array, ListArray},
    chunk::Chunk, types::NativeType, buffer::Buffer,
};
use raphtory::core::entities::VID;

use super::list_buffer::ListColumn;

// this is a list_array<struct_array<(u64, u64)>>, where the struct_array is (vertex, edge)
#[derive(Debug)]
pub(crate) struct VertexChunk(Chunk<Box<dyn Array>>);

impl VertexChunk {
    pub(crate) fn new(chunk: Chunk<Box<dyn Array>>) -> Self {
        VertexChunk(chunk)
    }

    pub(crate) fn neighbours_col(&self) -> Option<ListColumn<u64>> {
        self.0[0]
            .as_any()
            .downcast_ref::<ListArray<i64>>()
            .and_then(|list| ListColumn::new(list, 0))
    }

    pub(crate) fn edge_col(&self) -> Option<ListColumn<u64>> {
        self.0[0]
            .as_any()
            .downcast_ref::<ListArray<i64>>()
            .and_then(|list| ListColumn::new(list, 1))
    }

    pub(crate) fn neighbours_own(&self, vid: VID) -> Option<RowOwned<u64>> {
        let row:usize = vid.into();
        let col = self.neighbours_col()?;
        Some(RowOwned(col.value(row)))
    }

    pub(crate) fn edges_own(&self, vid: VID) -> Option<RowOwned<u64>> {
        let row:usize = vid.into();
        let col = self.edge_col()?;
        Some(RowOwned(col.value(row)))
    }

    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

}

pub(crate) struct RowOwned<T>(Buffer<T>);

impl <T: NativeType> RowOwned<T> {

    pub(crate) fn into_iter(self) -> impl Iterator<Item = T> {
        self.0.into_iter()
    }

}