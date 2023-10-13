use crate::arrow::edge_frame_builder::EdgeOverflowChunk;
use arrow2::{
    array::{Array, ListArray, PrimitiveArray},
    chunk::Chunk,
    datatypes::DataType,
    types::{NativeType, Offset},
};
use std::iter;

use super::{list_buffer::ListColumn, vertex_chunk::RowOwned, Time};

#[derive(Debug)]
pub(crate) struct EdgeChunk {
    chunk: Chunk<Box<dyn Array>>,
    overflow: Vec<EdgeOverflowChunk>,
}

const SRC_COL: usize = 0;
const DST_COL: usize = 1;

const OV_COL: usize = 2;
const TIME_COL: usize = 3;

impl EdgeChunk {
    pub(crate) fn new(chunk: Chunk<Box<dyn Array>>, overflow: Vec<EdgeOverflowChunk>) -> Self {
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
        EdgeChunk { chunk, overflow }
    }

    pub(crate) fn len(&self) -> usize {
        self.chunk.len()
    }

    pub(crate) fn source(&self) -> &PrimitiveArray<u64> {
        let src = self.chunk[SRC_COL]
            .as_any()
            .downcast_ref::<PrimitiveArray<u64>>()
            .unwrap();
        src
    }

    pub(crate) fn destination(&self) -> &PrimitiveArray<u64> {
        let dst = self.chunk[DST_COL]
            .as_any()
            .downcast_ref::<PrimitiveArray<u64>>()
            .unwrap();
        dst
    }

    fn overflow(&self) -> &PrimitiveArray<u64> {
        let ov = self.chunk[OV_COL]
            .as_any()
            .downcast_ref::<PrimitiveArray<u64>>()
            .unwrap();
        ov
    }

    pub(crate) fn temporal_primitive_prop<T: NativeType>(
        &self,
        offset: usize,
        prop_id: usize,
    ) -> Option<impl Iterator<Item = Option<&T>> + '_> {
        let t_prop = self.temporal_properties();
        let overflow = self.overflow();
        let maybe_overflow = overflow.get(offset);
        let col = ListColumn::new(&t_prop, prop_id)?;
        let iter = col.into_iter_row(offset).chain(
            maybe_overflow
                .into_iter()
                .flat_map(move |ov| self.overflow[ov as usize].temporal_primitive_prop(prop_id)),
        );
        Some(iter)
    }

    pub(crate) fn temporal_primitive_prop_items<T: NativeType>(
        &self,
        offset: usize,
        prop_id: usize,
    ) -> Option<impl Iterator<Item = (Option<&T>, &Time)> + '_> {
        let t_prop = self.temporal_properties();
        let overflow = self.overflow();
        let maybe_overflow = overflow.get(offset);
        let col = ListColumn::new(&t_prop, prop_id)?;
        let time_col = ListColumn::new(&t_prop, 0)?;

        let time_iter = time_col.into_iter_row(offset).chain(
            maybe_overflow
                .into_iter()
                .flat_map(move |ov| self.overflow[ov as usize].temporal_primitive_prop(0)),
        );

        let iter = col.into_iter_row(offset).chain(
            maybe_overflow
                .into_iter()
                .flat_map(move |ov| self.overflow[ov as usize].temporal_primitive_prop(prop_id)),
        );

        let iter = iter.zip(time_iter).map(|(v, t)| (v, t.unwrap()));
        Some(iter)
    }

    pub(crate) fn temporal_utf8_prop_items<I: Offset>(
        &self,
        offset: usize,
        prop_id: usize,
    ) -> Option<impl Iterator<Item = Option<(&str, &Time)>> + '_> {
        Some(std::iter::empty()) // TODO
    }

    pub(crate) fn timestamps(&self, offset: usize) -> impl Iterator<Item = &[Time]> + '_ {
        let time = self.additions().into_value(offset);
        let ov = self.overflow().get(offset);
        iter::once(time).chain(
            ov.into_iter()
                .flat_map(|ov| self.overflow[ov as usize].timestamps()),
        )
    }

    pub(crate) fn temporal_edge_property_id(&self, name: &str) -> Option<usize> {
        let t_prop = self.temporal_properties();
        match t_prop.data_type() {
            DataType::LargeList(field) => match field.data_type() {
                DataType::Struct(fields) => {
                    let idx = fields.iter().position(|f| f.name == name)?;
                    Some(idx)
                }
                _ => None,
            },
            _ => None,
        }
    }
    fn temporal_properties(&self) -> &ListArray<i64> {
        let time = self.chunk[TIME_COL]
            .as_any()
            .downcast_ref::<ListArray<i64>>()
            .unwrap();
        time
    }

    fn additions(&self) -> ListColumn<Time> {
        let time = self.chunk[TIME_COL]
            .as_any()
            .downcast_ref::<ListArray<i64>>()
            .unwrap();
        ListColumn::new(time, 0).unwrap()
    }
}
