use std::ops::RangeBounds;

use polars_arrow::{
    array::{BooleanArray, StructArray},
    datatypes::ArrowDataType as DataType,
};

use crate::prelude::{normalized_range, ArrayOps, BaseArrayOps, Chunked};

use super::{chunked_array::ChunkedArray, ChunkedArraySlice};

#[derive(Copy, Clone, Debug)]
pub struct ChunkedBoolCol<'a> {
    array: &'a ChunkedArray<StructArray>,
    col: usize,
}

impl<'a> ChunkedBoolCol<'a> {
    pub fn try_new(array: &'a ChunkedArray<StructArray>, col: usize) -> Option<Self> {
        let outer_dtype = match array.data_type() {
            None => {
                return if array.is_empty() {
                    Some(Self { array, col })
                } else {
                    None
                }
            }
            Some(dtype) => dtype,
        };
        match outer_dtype {
            DataType::Struct(fields) => {
                let is_bool_type =
                    fields.get(col).map(|f| f.data_type()) == Some(&DataType::Boolean);
                is_bool_type.then_some(Self { array, col })
            }
            _ => None,
        }
    }

    fn lift_chunk(&self, chunk_idx: usize) -> &BooleanArray {
        self.array
            .chunk(chunk_idx)
            .values()
            .get(self.col)
            .and_then(|arr| arr.as_any().downcast_ref::<BooleanArray>())
            .unwrap()
    }
}

impl<'a> BaseArrayOps<'a> for ChunkedBoolCol<'a> {
    type BaseSlice = &'a Self;
    type OwnedBaseSlice = Self;

    fn len(&self) -> usize {
        self.array.len()
    }

    fn data_type(&self) -> Option<&DataType> {
        match self.array.data_type()? {
            DataType::Struct(fields) => Some(fields[self.col].data_type()),
            _ => None,
        }
    }

    fn slice<R: RangeBounds<usize>>(&'a self, range: R) -> ChunkedArraySlice<'a, &'a Self> {
        ChunkedArraySlice::new(self, normalized_range(range, self.len()))
    }

    fn sliced<R: RangeBounds<usize>>(self, range: R) -> ChunkedArraySlice<'a, Self> {
        let len = self.len();
        ChunkedArraySlice::new(self, normalized_range(range, len))
    }
}

impl<'a> ArrayOps<'a> for ChunkedBoolCol<'a> {
    type Value = Option<bool>;
    type IntoValue = Option<bool>;
    type Chunk = BooleanArray;
    type Slice = Self::BaseSlice;
    type OwnedSlice = Self::OwnedBaseSlice;

    fn get(&'a self, i: usize) -> Self::Value {
        let (chunk_idx, offset) = self.array.resolve_idx(i);
        self.lift_chunk(chunk_idx).get(offset)
    }

    fn into_get(self, i: usize) -> Self::IntoValue {
        self.get(i)
    }

    fn iter_chunks(
        &'a self,
    ) -> impl DoubleEndedIterator<Item = &'a Self::Chunk> + ExactSizeIterator {
        self.array.iter_chunks().map(move |array| {
            {
                array
                    .values()
                    .get(self.col)
                    .and_then(|arr| arr.as_any().downcast_ref::<BooleanArray>())
                    .unwrap()
            }
        })
    }
}

impl<'a> Chunked for ChunkedBoolCol<'a> {
    type Chunk = BooleanArray;

    fn chunk_size(&self) -> usize {
        self.array.chunk_size()
    }

    fn num_chunks(&self) -> usize {
        self.array.num_chunks()
    }

    fn chunk(&self, i: usize) -> &Self::Chunk {
        self.lift_chunk(i)
    }

    fn iter_inner_chunks_range(
        &self,
        range: crate::prelude::ChunkRange,
    ) -> impl DoubleEndedIterator<Item = &Self::Chunk> + ExactSizeIterator {
        let col = self.col;
        self.array
            .iter_inner_chunks_range(range)
            .map(move |array| lift_bool_column(array, col).unwrap())
    }
}

fn lift_bool_column(struct_array: &StructArray, col: usize) -> Option<&BooleanArray> {
    struct_array.values()[col]
        .as_any()
        .downcast_ref::<BooleanArray>()
}
