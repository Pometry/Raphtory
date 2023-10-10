use arrow2::{
    array::{Array, ListArray, PrimitiveArray},
    chunk::Chunk,
};

use super::{list_buffer::ListColumn, vertex_chunk::RowOwned, Time};

#[derive(Debug)]
pub(crate) struct EdgeChunk(Chunk<Box<dyn Array>>);

const SRC_COL: usize = 0;
const DST_COL: usize = 1;
const TIME_COL: usize = 3;

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

    pub(crate) fn len(&self) -> usize {
        self.0.len()
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

    pub(crate) fn temporal_properties(&self) -> &ListArray<i64> {
        let time = self.0[TIME_COL]
            .as_any()
            .downcast_ref::<ListArray<i64>>()
            .unwrap();
        time
    }

    pub(crate) fn times(&self) -> impl Iterator<Item = RowOwned<Time>> + '_ {
        let time = self.0[TIME_COL]
            .as_any()
            .downcast_ref::<ListArray<i64>>()
            .expect(
                format!(
                    "Expected time column to be list array, instead got {:?}",
                    self.0[TIME_COL].data_type()
                )
                .as_str(),
            );
        let list_col = ListColumn::new(time, 0).unwrap();

        (0..time.len())
            .into_iter()
            .map(move |i| RowOwned::new(list_col.value(i)))
    }

    pub(crate) fn additions(&self) -> ListColumn<Time> {
        let time = self.0[TIME_COL]
            .as_any()
            .downcast_ref::<ListArray<i64>>()
            .unwrap();
        ListColumn::new(time, 0).unwrap()
    }
}
