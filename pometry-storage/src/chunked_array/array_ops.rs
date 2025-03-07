use crate::{
    arrow2::{
        datatypes::ArrowDataType as DataType,
        types::{NativeType, Offset},
    },
    chunked_array::{can_slice_and_len::AsIter, ChunkedArraySlice},
};
use rayon::prelude::*;
use std::{
    cmp::Ordering,
    ops::{Bound, Deref, Range, RangeBounds},
};

use super::{can_slice_and_len::CanSliceAndLen, chunked_iter::RangeChunkedIter};

pub struct ArrayRef<'a, A: ?Sized>(&'a A);
impl<'a, A> Deref for ArrayRef<'a, A> {
    type Target = A;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

pub(crate) fn normalized_range<R: RangeBounds<usize>>(range: R, len: usize) -> Range<usize> {
    let start = match range.start_bound() {
        Bound::Included(start) => *start,
        Bound::Excluded(start) => start.saturating_add(1),
        Bound::Unbounded => 0,
    };
    let end = match range.end_bound() {
        Bound::Included(end) => end.saturating_add(1),
        Bound::Excluded(end) => *end,
        Bound::Unbounded => len,
    };
    start..end
}

#[derive(Debug, Copy, Clone)]
pub struct ChunkRange {
    pub start_chunk: usize,
    pub end_chunk: usize,
    pub start_offset: usize,
    pub end_offset: usize,
}

impl ChunkRange {
    pub fn inner_range(&self) -> Range<usize> {
        if self.start_chunk == self.end_chunk {
            self.end_chunk..self.end_chunk
        } else {
            let start = self.start_chunk.saturating_add(1);
            start..self.end_chunk
        }
    }

    pub fn is_empty(&self) -> bool {
        self.start_chunk == self.end_chunk && self.start_offset == self.end_offset
    }
}

pub trait Chunked {
    type Chunk: CanSliceAndLen;

    fn chunk_size(&self) -> usize;

    fn num_chunks(&self) -> usize;
    fn chunk(&self, i: usize) -> &Self::Chunk;

    fn resolve_index(&self, i: usize) -> (usize, usize) {
        let chunk_index = i / self.chunk_size();
        let offset = i % self.chunk_size();
        (chunk_index, offset)
    }

    fn resolve_range(&self, range: Range<usize>) -> ChunkRange {
        let (start_chunk, start_offset) = self.resolve_index(range.start);
        let (mut end_chunk, mut end_offset) = self.resolve_index(range.end);
        if end_offset == 0 && end_chunk > start_chunk {
            end_chunk = end_chunk.saturating_sub(1);
            end_offset = self.chunk_size();
        }
        ChunkRange {
            start_chunk,
            end_chunk,
            start_offset,
            end_offset,
        }
    }

    fn iter_inner_chunks_range(
        &self,
        range: ChunkRange,
    ) -> impl DoubleEndedIterator<Item = &Self::Chunk> + ExactSizeIterator;

    fn iter_chunks_range(
        &self,
        range: Range<usize>,
    ) -> impl DoubleEndedIterator<Item = &Self::Chunk> + ExactSizeIterator {
        let range = self.resolve_range(range);
        let first_chunk = self.first_chunk_in_range(range);
        let last_chunk = self.last_chunk_in_range(range);

        RangeChunkedIter::new(first_chunk, last_chunk, self.iter_inner_chunks_range(range))
    }

    fn first_chunk_in_range(&self, range: ChunkRange) -> Option<Self::Chunk> {
        if range.is_empty() {
            None
        } else {
            let mut chunk = self.chunk(range.start_chunk).clone();
            if range.start_chunk == range.end_chunk {
                chunk.slice(range.start_offset, range.end_offset - range.start_offset);
            } else {
                chunk.slice(range.start_offset, chunk.len() - range.start_offset);
            }
            Some(chunk)
        }
    }

    fn last_chunk_in_range(&self, range: ChunkRange) -> Option<Self::Chunk> {
        if range.start_chunk == range.end_chunk {
            None
        } else {
            let mut last_chunk = self.chunk(range.end_chunk).clone();
            last_chunk.slice(0, range.end_offset);
            Some(last_chunk)
        }
    }
}

impl<'a, C: Chunked> Chunked for &'a C {
    type Chunk = C::Chunk;

    fn chunk_size(&self) -> usize {
        C::chunk_size(self)
    }

    fn num_chunks(&self) -> usize {
        C::num_chunks(self)
    }

    fn chunk(&self, i: usize) -> &Self::Chunk {
        C::chunk(self, i)
    }

    fn iter_inner_chunks_range(
        &self,
        range: ChunkRange,
    ) -> impl DoubleEndedIterator<Item = &Self::Chunk> + ExactSizeIterator {
        C::iter_inner_chunks_range(self, range)
    }
}

pub trait BaseArrayOps<'a>: Clone + Send + Sync + 'a {
    type BaseSlice: BaseArrayOps<'a>;
    type OwnedBaseSlice: BaseArrayOps<'a>;
    fn len(&self) -> usize;

    fn data_type(&self) -> Option<&DataType>;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn slice<R: RangeBounds<usize>>(&'a self, range: R) -> ChunkedArraySlice<'a, Self::BaseSlice>;

    fn sliced<R: RangeBounds<usize>>(self, range: R)
        -> ChunkedArraySlice<'a, Self::OwnedBaseSlice>;
}

pub trait ArrayOps<'a>:
    BaseArrayOps<'a, BaseSlice = Self::Slice, OwnedBaseSlice = Self::OwnedSlice>
{
    type Value: Send + 'a;
    type IntoValue;
    type Chunk: CanSliceAndLen + AsIter<'a, Item = Self::Value> + 'a;
    type Slice: ArrayOps<'a, Value = Self::Value, IntoValue = Self::Value, Chunk = Self::Chunk>
        + Chunked<Chunk = Self::Chunk>;
    type OwnedSlice: ArrayOps<'a, IntoValue = Self::IntoValue, Value = Self::Value, Chunk = Self::Chunk>
        + Chunked<Chunk = Self::Chunk>;

    fn get(&'a self, i: usize) -> Self::Value;

    fn into_get(self, i: usize) -> Self::IntoValue;

    fn iter_chunks(
        &'a self,
    ) -> impl DoubleEndedIterator<Item = &'a Self::Chunk> + ExactSizeIterator;

    fn iter(&'a self) -> impl Iterator<Item = Self::Value> {
        self.iter_chunks().flat_map(|chunk| chunk.as_iter())
    }

    fn par_iter(&'a self) -> impl IndexedParallelIterator<Item = Self::Value>
    where
        ChunkedArraySlice<'a, Self::Slice>: IntoParallelIterator<Item = Self::Value>,
        <ChunkedArraySlice<'a, Self::Slice> as IntoParallelIterator>::Iter: IndexedParallelIterator,
    {
        self.slice(..).into_par_iter()
    }

    fn par_iter_range(
        &'a self,
        range: Range<usize>,
    ) -> impl IndexedParallelIterator<Item = Self::Value>
    where
        ChunkedArraySlice<'a, Self::Slice>: IntoParallelIterator<Item = Self::Value>,
        <ChunkedArraySlice<'a, Self::Slice> as IntoParallelIterator>::Iter: IndexedParallelIterator,
    {
        self.slice(range).into_par_iter()
    }

    fn binary_search_by<F>(&'a self, mut f: F) -> Result<usize, usize>
    where
        F: FnMut(Self::Value) -> Ordering,
    {
        let mut size = self.len();
        let mut left = 0;
        let mut right = size;
        while left < right {
            let mid = left + size / 2;

            let cmp = f(self.get(mid));

            if cmp == Ordering::Less {
                left = mid + 1;
            } else if cmp == Ordering::Greater {
                right = mid;
            } else {
                return Ok(mid);
            }

            size = right - left;
        }

        Err(left)
    }

    fn partition_point<P>(&'a self, mut pred: P) -> usize
    where
        P: FnMut(Self::Value) -> bool,
    {
        self.binary_search_by(|x| {
            if pred(x) {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        })
        .unwrap_or_else(|i| i)
    }

    /// Find the insertion point for value to maintain sorted order (assumes array is sorted!)
    /// Gives the smallest index that maintains sorted order in case of ties.
    fn insertion_point(&'a self, value: Self::Value) -> usize
    where
        Self::Value: Ord,
    {
        self.partition_point(|v| v < value)
    }
}

pub trait PrimitiveCol<'a> {
    type Column<T: NativeType>: ArrayOps<'a, Value = Option<T>>
    where
        Self: 'a;

    fn primitive_col<T: NativeType>(&'a self, col_idx: usize) -> Option<Self::Column<T>>;
}
pub trait IntoPrimitiveCol<'a> {
    type Column<T: NativeType>: ArrayOps<'a, Value = Option<T>>
    where
        Self: 'a;

    fn into_primitive_col<T: NativeType>(self, col_idx: usize) -> Option<Self::Column<T>>;
}

pub trait IntoBoolCol<'a> {
    type Column: ArrayOps<'a, Value = Option<bool>>
    where
        Self: 'a;

    fn into_bool_col(self, col_idx: usize) -> Option<Self::Column>;
}

pub trait IntoUtf8Col<'a> {
    type Column<I>: ArrayOps<'a, Value = Option<&'a str>>
    where
        Self: 'a,
        I: Offset + 'a;

    fn into_utf8_col<I: Offset>(self, col_idx: usize) -> Option<Self::Column<I>>;
}

pub trait NonNullPrimitiveCol<'a> {
    type Column<T: NativeType>: ArrayOps<'a, Value = T, IntoValue = T>
    where
        Self: 'a;

    fn non_null_primitive_col<T: NativeType>(&'a self, col_idx: usize) -> Option<Self::Column<T>>;
}

pub trait IntoNonNullPrimitiveCol<'a> {
    type Column<T: NativeType>: ArrayOps<'a, Value = T, IntoValue = T>
    where
        Self: 'a;

    fn into_non_null_primitive_col<T: NativeType>(self, col_idx: usize) -> Option<Self::Column<T>>;
}

impl<'a, A: PrimitiveCol<'a> + ?Sized> PrimitiveCol<'a> for &'a A {
    type Column<T: NativeType>
        = A::Column<T>
    where
        Self: 'a;

    fn primitive_col<T: NativeType>(&self, col_idx: usize) -> Option<Self::Column<T>> {
        A::primitive_col(self, col_idx)
    }
}

impl<'a, A: NonNullPrimitiveCol<'a> + ?Sized> NonNullPrimitiveCol<'a> for &'a A {
    type Column<T: NativeType>
        = A::Column<T>
    where
        Self: 'a;

    fn non_null_primitive_col<T: NativeType>(&'a self, col_idx: usize) -> Option<Self::Column<T>> {
        A::non_null_primitive_col(self, col_idx)
    }
}

impl<'a, A: NonNullPrimitiveCol<'a> + ?Sized> IntoNonNullPrimitiveCol<'a> for &'a A {
    type Column<T: NativeType>
        = A::Column<T>
    where
        Self: 'a;

    fn into_non_null_primitive_col<T: NativeType>(self, col_idx: usize) -> Option<Self::Column<T>> {
        A::non_null_primitive_col(self, col_idx)
    }
}

impl<'b, A: BaseArrayOps<'b> + ?Sized> BaseArrayOps<'b> for &'b A {
    type BaseSlice = A::BaseSlice;
    type OwnedBaseSlice = A::BaseSlice;

    fn len(&self) -> usize {
        A::len(self)
    }

    fn data_type(&self) -> Option<&DataType> {
        A::data_type(self)
    }

    fn slice<R: RangeBounds<usize>>(&self, range: R) -> ChunkedArraySlice<'b, Self::BaseSlice> {
        A::slice(self, range)
    }

    fn sliced<R: RangeBounds<usize>>(
        self,
        range: R,
    ) -> ChunkedArraySlice<'b, Self::OwnedBaseSlice> {
        A::slice(self, range)
    }
}

impl<'b, A: ArrayOps<'b> + ?Sized> ArrayOps<'b> for &'b A
where
    A: 'b,
{
    type Value = A::Value;
    type IntoValue = A::Value;
    type Chunk = A::Chunk;
    type Slice = A::Slice;

    type OwnedSlice = A::Slice;

    fn get(&self, i: usize) -> Self::Value {
        A::get(self, i)
    }

    fn into_get(self, i: usize) -> Self::Value {
        A::get(self, i)
    }

    fn iter_chunks(
        &'b self,
    ) -> impl DoubleEndedIterator<Item = &'b Self::Chunk> + ExactSizeIterator {
        A::iter_chunks(self)
    }
}
