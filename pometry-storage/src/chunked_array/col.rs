use crate::{
    arrow2::{
        array::{PrimitiveArray, StructArray},
        buffer::Buffer,
        datatypes::ArrowDataType as DataType,
        types::NativeType,
    },
    chunked_array::{can_slice_and_len::AsIter, chunked_array::ChunkedArray, ChunkedArraySlice},
    prelude::{normalized_range, ArrayOps, BaseArrayOps, ChunkRange, Chunked},
};
use polars_utils::index::Indexable;
use rayon::prelude::*;
use std::{
    marker::PhantomData,
    ops::{Range, RangeBounds},
};

fn lift_column<T: NativeType>(
    struct_array: &StructArray,
    col: usize,
) -> Option<&PrimitiveArray<T>> {
    struct_array.values()[col].as_any().downcast_ref()
}

fn lift_non_null_column<T: NativeType>(
    struct_array: &StructArray,
    col: usize,
) -> Option<&Buffer<T>> {
    struct_array.values()[col]
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .filter(|&col| col.validity().is_none())
        .map(|v| v.values())
}

#[derive(Copy, Clone, Debug)]
pub struct ChunkedPrimitiveCol<'a, T> {
    array: &'a ChunkedArray<StructArray>,
    col: usize,
    marker: PhantomData<T>,
}

impl<'a, T: NativeType + 'a> BaseArrayOps<'a> for ChunkedPrimitiveCol<'a, T> {
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

impl<'a, T: NativeType> ArrayOps<'a> for ChunkedPrimitiveCol<'a, T> {
    type Value = Option<T>;
    type IntoValue = Option<T>;
    type Chunk = PrimitiveArray<T>;
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
        self.array
            .iter_chunks()
            .map(move |array| lift_column(array, self.col).unwrap())
    }
}

impl<'a, T: NativeType> Chunked for ChunkedPrimitiveCol<'a, T> {
    type Chunk = PrimitiveArray<T>;

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
            .map(move |array| lift_column(array, col).unwrap())
    }
}

impl<'a, T: NativeType> ChunkedPrimitiveCol<'a, T> {
    pub(crate) fn try_new(array: &'a ChunkedArray<StructArray>, col: usize) -> Option<Self> {
        let outer_dtype = match array.data_type() {
            None => {
                return if array.is_empty() {
                    Some(Self {
                        array,
                        col,
                        marker: PhantomData,
                    })
                } else {
                    None
                }
            }
            Some(dtype) => dtype,
        };
        let col_dtype = DataType::from(T::PRIMITIVE);
        match outer_dtype {
            DataType::Struct(fields) => {
                if fields.get(col).map(|f| f.data_type().to_physical_type())
                    == Some(col_dtype.to_physical_type())
                {
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

    fn lift_chunk(&self, chunk_idx: usize) -> &PrimitiveArray<T> {
        lift_column(self.array.chunk(chunk_idx), self.col).unwrap()
    }

    pub fn par_iter_chunks_window_unchecked(
        self,
        window: Range<usize>,
    ) -> impl IndexedParallelIterator<Item = &'a [T]> + 'a {
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
            let mut arr = lift_column::<T>(arr.chunk(c), col)
                .unwrap()
                .values()
                .as_slice();

            if start_chunk == end_chunk {
                arr = &arr[first_offset..last_offset];
            } else if c == start_chunk {
                arr = &arr[first_offset..];
            } else if c == end_chunk {
                arr = &arr[..last_offset];
            }

            arr
        })
    }

    pub fn iter_chunks_window_unchecked(
        self,
        window: Range<usize>,
    ) -> impl Iterator<Item = &'a [T]> + 'a {
        let (start_chunk, first_offset) = self.array.resolve_idx(window.start);
        let (mut end_chunk, mut last_offset) = self.array.resolve_idx(window.end);

        if last_offset == 0 && !window.is_empty() {
            end_chunk -= 1;
            last_offset = self.array.chunk_size();
        };

        let arr = self.array;
        let col = self.col;
        (start_chunk..=end_chunk).map(move |c| {
            let mut arr = lift_column::<T>(arr.chunk(c), col)
                .unwrap()
                .values()
                .as_slice();

            if start_chunk == end_chunk {
                arr = &arr[first_offset..last_offset];
            } else if c == start_chunk {
                arr = &arr[first_offset..];
            } else if c == end_chunk {
                arr = &arr[..last_offset];
            }

            arr
        })
    }

    pub fn get_ref(&self, i: usize) -> Option<&T> {
        let (chunk_idx, offset) = self.array.resolve_idx(i);
        let arr = self.lift_chunk(chunk_idx);
        if let Some(val) = arr.validity() {
            val.get(offset)
                .filter(|vis| *vis)
                .and_then(|_| arr.values().get(offset))
        } else {
            arr.values().get(offset)
        }
    }

    pub fn get_ref_unchecked(&self, i: usize) -> &T {
        let (chunk_idx, offset) = self.array.resolve_idx(i);
        &self.lift_chunk(chunk_idx).values()[offset]
    }

    pub fn par_iter_chunks_window(
        self,
        window: Range<usize>,
    ) -> impl IndexedParallelIterator<Item = PrimitiveArray<T>> + 'a {
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
            let mut arr = lift_column::<T>(arr.chunk(c), col).unwrap().clone();

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

impl<'a, T: NativeType> IntoIterator for ChunkedPrimitiveCol<'a, T> {
    type Item = Option<T>;
    type IntoIter = Box<dyn Iterator<Item = Option<T>> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        let col = self.col;
        Box::new(
            self.array
                .iter_chunks()
                .flat_map(move |array| lift_column(array, col).unwrap())
                .map(|v| v.copied()),
        )
    }
}

#[derive(Debug, Clone, Copy)]
pub struct NonNullChunkedPrimitiveCol<'a, T> {
    array: &'a ChunkedArray<StructArray>,
    col: usize,
    marker: PhantomData<T>,
}

impl<'a, T: NativeType> Chunked for NonNullChunkedPrimitiveCol<'a, T> {
    type Chunk = Buffer<T>;

    fn chunk_size(&self) -> usize {
        self.array.chunk_size()
    }

    fn num_chunks(&self) -> usize {
        self.array.num_chunks()
    }

    fn chunk(&self, i: usize) -> &Self::Chunk {
        lift_non_null_column(self.array.chunk(i), self.col).unwrap()
    }

    fn iter_inner_chunks_range(
        &self,
        range: ChunkRange,
    ) -> impl DoubleEndedIterator<Item = &Self::Chunk> + ExactSizeIterator {
        let col = self.col;
        self.array
            .iter_inner_chunks_range(range)
            .map(move |chunk| lift_non_null_column(chunk, col).unwrap())
    }
}

impl<'a, T: NativeType> BaseArrayOps<'a> for NonNullChunkedPrimitiveCol<'a, T> {
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

impl<'a, T: NativeType> ArrayOps<'a> for NonNullChunkedPrimitiveCol<'a, T> {
    type Value = T;
    type IntoValue = T;
    type Chunk = Buffer<T>;
    type Slice = Self::BaseSlice;
    type OwnedSlice = Self::OwnedBaseSlice;

    fn get(&'a self, i: usize) -> Self::Value {
        let (chunk_idx, offset) = self.array.resolve_idx(i);
        lift_non_null_column(self.array.chunk(chunk_idx), self.col).unwrap()[offset]
    }

    fn into_get(self, i: usize) -> Self::IntoValue {
        self.get(i)
    }

    fn iter_chunks(
        &'a self,
    ) -> impl DoubleEndedIterator<Item = &'a Self::Chunk> + ExactSizeIterator {
        let col = self.col;
        self.array
            .iter_chunks()
            .map(move |array| lift_non_null_column(array, col).unwrap())
    }
}

impl<'a, T: NativeType> NonNullChunkedPrimitiveCol<'a, T> {
    pub(crate) fn try_new(array: &'a ChunkedArray<StructArray>, col: usize) -> Option<Self> {
        let outer_dtype = array.data_type()?;
        let col_dtype = DataType::from(T::PRIMITIVE);
        match outer_dtype {
            DataType::Struct(fields) => {
                if fields.get(col).map(|f| f.data_type().to_physical_type())
                    == Some(col_dtype.to_physical_type())
                {
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
}

impl<'a, T: NativeType> IntoIterator for NonNullChunkedPrimitiveCol<'a, T> {
    type Item = T;
    type IntoIter = Box<dyn Iterator<Item = Self::Item> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        let col = self.col;
        Box::new(
            self.array
                .iter_chunks()
                .flat_map(move |array| lift_non_null_column(array, col).unwrap().as_iter()),
        )
    }
}
