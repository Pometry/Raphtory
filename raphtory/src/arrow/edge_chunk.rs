use crate::arrow::E_DELETIONS_COLUMN;
use arrow2::{
    array::{Array, ListArray, PrimitiveArray},
    chunk::Chunk,
};

use super::{list_buffer::ListColumn, Time};

#[derive(Debug)]
pub(crate) struct EdgeChunk(Chunk<Box<dyn Array>>);

const SRC_COL: usize = 0;
const DST_COL: usize = 1;
const TIME_COL: usize = 2;

impl EdgeChunk {
    pub(crate) fn new(chunk: Chunk<Box<dyn Array>>) -> Self {
        assert!(
            chunk[SRC_COL]
                .as_any()
                .downcast_ref::<PrimitiveArray<u64>>()
                .is_some(),
            "Expected col 0 (source) to be u64"
        );
        assert!(
            chunk[DST_COL]
                .as_any()
                .downcast_ref::<PrimitiveArray<u64>>()
                .is_some(),
            "Expected col 1 (source) to be u64"
        );
        EdgeChunk(chunk)
    }

    pub(crate) fn source(&self) -> PrimitiveArray<u64> {
        let src = self.0[SRC_COL]
            .as_any()
            .downcast_ref::<PrimitiveArray<u64>>()
            .unwrap()
            .clone();
        src
    }

    pub(crate) fn destination(&self) -> PrimitiveArray<u64> {
        let dst = self.0[DST_COL]
            .as_any()
            .downcast_ref::<PrimitiveArray<u64>>()
            .unwrap()
            .clone();
        dst
    }

    pub(crate) fn time(&self) -> ListArray<i64> {
        let time = self.0[TIME_COL]
            .as_any()
            .downcast_ref::<ListArray<i64>>()
            .unwrap()
            .clone();
        time
    }

    pub(crate) fn additions(&self) -> ListColumn<Time> {
        let time = self.0[TIME_COL]
            .as_any()
            .downcast_ref::<ListArray<i64>>()
            .unwrap();
        ListColumn::new(time, 0).unwrap()
    }

    pub(crate) fn deletions(&self) -> ListColumn<Time> {
        todo!("deletions not yet implemented")
    }
}