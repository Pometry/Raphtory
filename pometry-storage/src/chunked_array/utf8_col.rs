use crate::{
    arrow2::{
        array::{StructArray, Utf8Array},
        datatypes::ArrowDataType as DataType,
        types::Offset,
    },
    chunked_array::{can_slice_and_len::AsIter, chunked_array::ChunkedArray, ChunkedArraySlice},
    prelude::{normalized_range, ArrayOps, BaseArrayOps, ChunkRange, Chunked},
};
use rayon::prelude::*;
use std::{
    marker::PhantomData,
    ops::{Range, RangeBounds},
};

fn lift_utf8_column<I: Offset>(struct_array: &StructArray, col: usize) -> Option<&Utf8Array<I>> {
    struct_array.values()[col].as_any().downcast_ref()
}

#[derive(Debug, Copy)]
pub struct StringCol<'a, I> {
    array: &'a ChunkedArray<StructArray>,
    col: usize,
    marker: PhantomData<I>,
}

impl<'a, I> Clone for StringCol<'a, I> {
    fn clone(&self) -> Self {
        Self {
            array: self.array,
            col: self.col,
            marker: PhantomData,
        }
    }
}

impl<'a, I: Offset> StringCol<'a, I> {
    fn lift_utf8_chunk(self, chunk_id: usize) -> Option<&'a Utf8Array<I>> {
        let arr = self.array.chunk(chunk_id).values().get(self.col)?;
        let arr = arr.as_any().downcast_ref::<Utf8Array<I>>()?;
        Some(arr)
    }

    fn get_str(self, chunk_id: usize, offset: usize) -> Option<&'a str> {
        let arr = self.lift_utf8_chunk(chunk_id)?;
        arr.get(offset)
    }
}

impl<'a, I: Offset> BaseArrayOps<'a> for StringCol<'a, I> {
    type BaseSlice = &'a Self;
    type OwnedBaseSlice = Self;

    fn len(&self) -> usize {
        self.array.len()
    }

    fn data_type(&self) -> Option<&DataType> {
        self.array.data_type()
    }

    fn slice<R: RangeBounds<usize>>(&'a self, range: R) -> ChunkedArraySlice<'a, &'a Self> {
        ChunkedArraySlice::new(self, normalized_range(range, self.len()))
    }

    fn sliced<R: RangeBounds<usize>>(self, range: R) -> ChunkedArraySlice<'a, Self> {
        let len = self.len();
        ChunkedArraySlice::new(self, normalized_range(range, len))
    }
}

impl<'a, I: Offset> ArrayOps<'a> for StringCol<'a, I> {
    type Value = Option<&'a str>;
    type IntoValue = Option<&'a str>;
    type Chunk = Utf8Array<I>;
    type Slice = Self::BaseSlice;
    type OwnedSlice = Self::OwnedBaseSlice;

    fn get(&'a self, i: usize) -> Self::Value {
        let (chunk_idx, offset) = self.array.resolve_idx(i);
        self.lift_chunk(chunk_idx).get(offset)
    }

    fn into_get(self, i: usize) -> Self::IntoValue {
        let (chunk_idx, offset) = self.array.resolve_idx(i);
        self.get_str(chunk_idx, offset)
    }

    fn iter_chunks(
        &'a self,
    ) -> impl DoubleEndedIterator<Item = &'a Self::Chunk> + ExactSizeIterator {
        self.array
            .iter_chunks()
            .map(|array| lift_utf8_column(array, self.col).unwrap())
    }
}

impl<'a, I: Offset> Chunked for StringCol<'a, I> {
    type Chunk = Utf8Array<I>;

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
        range: ChunkRange,
    ) -> impl DoubleEndedIterator<Item = &Self::Chunk> + ExactSizeIterator {
        let col = self.col;
        self.array
            .iter_inner_chunks_range(range)
            .map(move |array| lift_utf8_column(array, col).unwrap())
    }
}

impl<'a, I: Offset> StringCol<'a, I> {
    pub(crate) fn try_new(array: &'a ChunkedArray<StructArray>, col: usize) -> Option<Self> {
        let outer_dtype = array.data_type()?;
        let col_dtype = if I::IS_LARGE {
            DataType::LargeUtf8
        } else {
            DataType::Utf8
        };
        match outer_dtype {
            DataType::Struct(fields) => {
                if fields.get(col).map(|f| f.data_type()) == Some(&col_dtype) {
                    Some(Self {
                        array,
                        col,
                        marker: PhantomData,
                    })
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn lift_chunk(&self, chunk_idx: usize) -> &Utf8Array<I> {
        lift_utf8_column(self.array.chunk(chunk_idx), self.col).unwrap()
    }

    pub fn get_ref(&self, i: usize) -> Option<&str> {
        let (chunk_idx, offset) = self.array.resolve_idx(i);
        let chunk = self.lift_chunk(chunk_idx);
        chunk.get(offset)
    }

    pub fn get_ref_unchecked(&self, i: usize) -> &str {
        let (chunk_idx, offset) = self.array.resolve_idx(i);
        self.lift_chunk(chunk_idx).value(offset)
    }

    pub fn par_iter_chunks_window(
        self,
        window: Range<usize>,
    ) -> impl IndexedParallelIterator<Item = Utf8Array<I>> + 'a {
        let (start_chunk, first_offset) = self.array.resolve_idx(window.start);
        let (mut end_chunk, mut last_offset) = self.array.resolve_idx(window.end);

        if last_offset == 0 && !window.is_empty() {
            end_chunk -= 1;
            last_offset = self.array.chunk_size();
        };

        let start_chunk_u16: u16 = start_chunk.try_into().expect("More than u16::MAX chunks, rayon does not support into_par_iter for more than u16 ranges");
        let end_chunk_u16: u16 = end_chunk.try_into().expect("More than u16::MAX chunks, rayon does not support into_par_iter for more than u16 ranges");

        let range = (start_chunk_u16..=end_chunk_u16).into_par_iter();

        let arr = self.array;
        let col = self.col;
        range.into_par_iter().map(move |c| {
            let c = c as usize;
            let mut arr = lift_utf8_column::<I>(arr.chunk(c), col).unwrap().clone();

            if start_chunk == end_chunk {
                let length = last_offset - first_offset;
                arr.slice(first_offset, length);
            } else if c == start_chunk {
                let length = arr.len() - first_offset;
                arr.slice(first_offset, length);
            } else if c == end_chunk {
                let length = last_offset;
                arr.slice(0, length);
            }

            arr
        })
    }
}

impl<'a, I: Offset> IntoIterator for StringCol<'a, I> {
    type Item = Option<&'a str>;
    type IntoIter = Box<dyn Iterator<Item = Option<&'a str>> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        let col = self.col;
        Box::new(
            self.array
                .iter_chunks()
                .flat_map(move |array| lift_utf8_column::<I>(array, col).unwrap().as_iter()),
        )
    }
}
