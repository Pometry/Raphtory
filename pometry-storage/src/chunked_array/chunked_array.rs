use super::{
    array_ops::{IntoPrimitiveCol, IntoUtf8Col},
    bool_col::ChunkedBoolCol,
    can_slice_and_len::CanSliceAndLen,
    utf8_col::StringCol,
};
use crate::{
    arrow2::{
        array::{Array, PrimitiveArray, StructArray, Utf8Array},
        buffer::Buffer,
        datatypes::ArrowDataType as DataType,
        types::{NativeType, Offset},
    },
    chunked_array::{
        array_ops::{normalized_range, ArrayOps, BaseArrayOps, PrimitiveCol},
        col::{ChunkedPrimitiveCol, NonNullChunkedPrimitiveCol},
        row::{Row, RowOwned},
        ChunkedArraySlice,
    },
    file_prefix::{sorted_file_list, GraphPaths},
    load::{ipc::read_batch, mmap::mmap_batch},
    prelude::{ChunkRange, Chunked, IntoBoolCol, NonNullPrimitiveCol},
    RAError,
};
use polars_arrow::array::BooleanArray;
use polars_utils::index::Indexable;
use std::{
    fmt::Debug,
    ops::RangeBounds,
    path::{Path, PathBuf},
    sync::Arc,
};

mod null_mark {
    use std::fmt::Debug;

    pub trait NullMark: Copy + Send + Sync + Debug {
        fn mark() -> Self;
    }
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct Nullable {}
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct NonNull {}
    impl NullMark for Nullable {
        fn mark() -> Self {
            Self {}
        }
    }
    impl NullMark for NonNull {
        fn mark() -> Self {
            Self {}
        }
    }
}

use null_mark::NullMark;
pub use null_mark::{NonNull, Nullable};

#[derive(Debug)]
pub struct ChunkedArray<A, N = Nullable> {
    pub(crate) chunks: Arc<[A]>,
    chunk_size: usize,
    null_mark: N,
}

impl<A, N: Copy> Clone for ChunkedArray<A, N> {
    fn clone(&self) -> Self {
        Self {
            chunks: self.chunks.clone(),
            chunk_size: self.chunk_size,
            null_mark: self.null_mark,
        }
    }
}

impl<A, N> PartialEq for ChunkedArray<A, N>
where
    for<'a> Self: ArrayOps<'a>,
    for<'a> <Self as ArrayOps<'a>>::Value: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}

impl<A: CanSliceAndLen> Chunked for ChunkedArray<A, Nullable> {
    type Chunk = A;

    fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    fn num_chunks(&self) -> usize {
        self.chunks.len()
    }

    fn chunk(&self, i: usize) -> &Self::Chunk {
        &self.chunks[i]
    }

    /// Iterate over subrange of chunks (note that range is over array indices, not chunks)
    fn iter_inner_chunks_range(
        &self,
        range: ChunkRange,
    ) -> impl DoubleEndedIterator<Item = &Self::Chunk> + ExactSizeIterator {
        self.chunks[range.inner_range()].iter()
    }
}

impl<T: NativeType> Chunked for ChunkedArray<PrimitiveArray<T>, NonNull> {
    type Chunk = Buffer<T>;

    fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    fn num_chunks(&self) -> usize {
        self.chunks.len()
    }

    fn chunk(&self, i: usize) -> &Self::Chunk {
        self.chunks[i].values()
    }

    fn iter_inner_chunks_range(
        &self,
        range: ChunkRange,
    ) -> impl DoubleEndedIterator<Item = &Self::Chunk> + ExactSizeIterator {
        self.chunks[range.inner_range()]
            .iter()
            .map(|chunk| chunk.values())
    }
}

impl<A: Array, N: NullMark> Default for ChunkedArray<A, N> {
    fn default() -> Self {
        Self::empty()
    }
}

impl<A: Array, N: NullMark> ChunkedArray<A, N> {
    pub(crate) fn new(chunks: Vec<A>, chunk_size: usize) -> Self {
        let chunk_size = chunk_size.max(1);
        if !chunks.is_empty() {
            for array in &chunks[..chunks.len() - 1] {
                assert_eq!(
                    array.len(),
                    chunk_size,
                    "all arrays must have the same length"
                );
            }
        }

        Self {
            chunks: chunks.into(),
            chunk_size,
            null_mark: N::mark(),
        }
    }

    pub(crate) fn empty() -> Self {
        Self::new(vec![], 1)
    }

    #[inline]
    pub(super) fn resolve_idx(&self, idx: usize) -> (usize, usize) {
        (idx / self.chunk_size, idx % self.chunk_size)
    }
}

impl<A: Array> From<Vec<A>> for ChunkedArray<A, Nullable> {
    fn from(vec: Vec<A>) -> Self {
        if vec.is_empty() {
            return Self::empty();
        }
        let chunk_size = vec[0].len();
        for array in &vec[..vec.len() - 1] {
            assert_eq!(
                array.len(),
                chunk_size,
                "all arrays must have the same length"
            );
        }

        Self::new(vec, chunk_size)
    }
}

impl<A: Array> ChunkedArray<A, NonNull> {
    pub fn from_non_nulls(vec: Vec<A>) -> Self {
        let chunk_size = if !vec.is_empty() {
            let chunk_size = vec[0].len();
            if vec.len() > 1 {
                for array in &vec[1..vec.len() - 1] {
                    assert_eq!(
                        array.len(),
                        chunk_size,
                        "all arrays must have the same length"
                    );
                }
                assert!(vec[vec.len() - 1].len() <= chunk_size);
            }
            chunk_size
        } else {
            1
        };
        Self::new(vec, chunk_size)
    }
}

impl<T: NativeType> From<Vec<Vec<T>>> for ChunkedArray<PrimitiveArray<T>, NonNull> {
    fn from(value: Vec<Vec<T>>) -> Self {
        let chunk_size = value.first().map(|chunk| chunk.len()).unwrap_or(0);
        let arrays: Vec<PrimitiveArray<T>> =
            value.into_iter().map(PrimitiveArray::from_vec).collect();
        Self::new(arrays, chunk_size)
    }
}

impl<'a, A: Array, N: NullMark + 'a> BaseArrayOps<'a> for ChunkedArray<A, N> {
    type BaseSlice = &'a Self;
    type OwnedBaseSlice = Self;

    fn len(&self) -> usize {
        self.chunk_size * self.chunks.len().saturating_sub(1)
            + self.chunks.last().map(|c| c.len()).unwrap_or(0)
    }

    fn data_type(&self) -> Option<&DataType> {
        self.chunks.first().map(|array| array.data_type())
    }

    fn is_empty(&self) -> bool {
        self.chunks.is_empty() || self.len() == 0
    }

    fn slice<R: RangeBounds<usize>>(&'a self, range: R) -> ChunkedArraySlice<'a, &'a Self> {
        let range = normalized_range(range, self.len());
        ChunkedArraySlice::new(self, range)
    }

    fn sliced<R: RangeBounds<usize>>(self, range: R) -> ChunkedArraySlice<'a, Self> {
        let range = normalized_range(range, self.len());
        ChunkedArraySlice::new(self, range)
    }
}

impl<'a> ArrayOps<'a> for ChunkedArray<BooleanArray, Nullable> {
    type Value = Option<bool>;

    type IntoValue = Option<bool>;

    type Chunk = BooleanArray;

    type Slice = Self::BaseSlice;

    type OwnedSlice = Self::OwnedBaseSlice;

    fn get(&'a self, i: usize) -> Self::Value {
        let (chunk_idx, offset) = self.resolve_idx(i);
        self.chunks[chunk_idx].get(offset)
    }

    fn into_get(self, i: usize) -> Self::IntoValue {
        self.get(i)
    }

    fn iter_chunks(
        &'a self,
    ) -> impl DoubleEndedIterator<Item = &'a Self::Chunk> + ExactSizeIterator {
        self.chunks.iter()
    }
}

impl<'a, T: NativeType> ArrayOps<'a> for ChunkedArray<PrimitiveArray<T>, Nullable> {
    type Value = Option<T>;
    type IntoValue = Option<T>;
    type Chunk = PrimitiveArray<T>;
    type Slice = Self::BaseSlice;
    type OwnedSlice = Self::OwnedBaseSlice;

    fn get(&self, i: usize) -> Self::Value {
        let (chunk_idx, offset) = self.resolve_idx(i);
        self.chunks[chunk_idx].get(offset)
    }

    fn into_get(self, i: usize) -> Self::IntoValue {
        self.get(i)
    }

    fn iter_chunks(
        &'a self,
    ) -> impl DoubleEndedIterator<Item = &'a Self::Chunk> + ExactSizeIterator {
        self.chunks.iter()
    }
}

impl<'a, T: NativeType> ArrayOps<'a> for ChunkedArray<PrimitiveArray<T>, NonNull> {
    type Value = T;
    type IntoValue = T;
    type Chunk = Buffer<T>;
    type Slice = Self::BaseSlice;
    type OwnedSlice = Self::OwnedBaseSlice;

    fn get(&'a self, i: usize) -> Self::Value {
        let (chunk_idx, offset) = self.resolve_idx(i);
        self.chunks[chunk_idx].values()[offset]
    }

    fn into_get(self, i: usize) -> Self::IntoValue {
        self.get(i)
    }

    fn iter_chunks(
        &'a self,
    ) -> impl DoubleEndedIterator<Item = &'a Self::Chunk> + ExactSizeIterator {
        self.chunks.iter().map(|a| a.values())
    }
}

impl<'a, I: Offset> ArrayOps<'a> for ChunkedArray<Utf8Array<I>> {
    type Value = Option<&'a str>;
    type IntoValue = Option<String>;
    type Chunk = Utf8Array<I>;
    type Slice = Self::BaseSlice;
    type OwnedSlice = Self::OwnedBaseSlice;

    fn get(&'a self, i: usize) -> Self::Value {
        let (chunk_idx, offset) = self.resolve_idx(i);
        let chunk = &self.chunks[chunk_idx];
        chunk.get(offset)
    }

    fn into_get(self, i: usize) -> Self::IntoValue {
        self.get(i).map(|v| v.to_string())
    }

    fn iter_chunks(
        &'a self,
    ) -> impl DoubleEndedIterator<Item = &'a Self::Chunk> + ExactSizeIterator {
        self.chunks.iter()
    }
}

impl ChunkedArray<StructArray> {
    pub fn num_cols(&self) -> usize {
        self.chunks
            .first()
            .map(|chunk| chunk.values().len())
            .unwrap_or(0)
    }
}

impl<'b> IntoPrimitiveCol<'b> for &'b ChunkedArray<StructArray> {
    type Column<T: NativeType> = ChunkedPrimitiveCol<'b, T>;

    fn into_primitive_col<T: NativeType>(
        self,
        col_idx: usize,
    ) -> Option<ChunkedPrimitiveCol<'b, T>> {
        ChunkedPrimitiveCol::try_new(self, col_idx)
    }
}

impl<'b> IntoBoolCol<'b> for &'b ChunkedArray<StructArray> {
    type Column = ChunkedBoolCol<'b>;

    fn into_bool_col(self, col_idx: usize) -> Option<Self::Column> {
        ChunkedBoolCol::try_new(self, col_idx)
    }
}

impl<'b> IntoUtf8Col<'b> for &'b ChunkedArray<StructArray> {
    type Column<I>
        = StringCol<'b, I>
    where
        I: Offset + 'b;

    fn into_utf8_col<I: Offset>(self, col_idx: usize) -> Option<Self::Column<I>> {
        StringCol::try_new(self, col_idx)
    }
}

impl<'b> PrimitiveCol<'b> for ChunkedArray<StructArray> {
    type Column<T: NativeType> = ChunkedPrimitiveCol<'b, T>;

    fn primitive_col<T: NativeType>(&self, col_idx: usize) -> Option<ChunkedPrimitiveCol<T>> {
        ChunkedPrimitiveCol::try_new(self, col_idx)
    }
}

impl<'a> NonNullPrimitiveCol<'a> for ChunkedArray<StructArray> {
    type Column<T: NativeType> = NonNullChunkedPrimitiveCol<'a, T>;

    fn non_null_primitive_col<T: NativeType>(&'a self, col_idx: usize) -> Option<Self::Column<T>> {
        NonNullChunkedPrimitiveCol::try_new(self, col_idx)
    }
}

impl<'a> ArrayOps<'a> for ChunkedArray<StructArray> {
    type Value = Row<'a>;
    type IntoValue = RowOwned;
    type Chunk = StructArray;
    type Slice = Self::BaseSlice;
    type OwnedSlice = Self::OwnedBaseSlice;

    fn get(&'a self, i: usize) -> Self::Value {
        let (chunk_idx, row) = self.resolve_idx(i);
        let chunk = &self.chunks[chunk_idx];
        Row::new(chunk, row)
    }

    fn into_get(self, i: usize) -> Self::IntoValue {
        let (chunk_idx, row) = self.resolve_idx(i);
        RowOwned::new(self, chunk_idx, row)
    }

    fn iter_chunks(
        &'a self,
    ) -> impl DoubleEndedIterator<Item = &'a Self::Chunk> + ExactSizeIterator {
        self.chunks.iter()
    }
}

impl<A: Array + Clone, N: NullMark> ChunkedArray<A, N> {
    pub(crate) unsafe fn load_from_dir(
        dir: impl AsRef<Path>,
        prefix: GraphPaths,
        mmap: bool,
    ) -> Result<Self, RAError> {
        let files = sorted_file_list(dir, prefix)?;

        let load_fn = if mmap {
            |path: PathBuf| mmap_batch(path, 0)
        } else {
            |path: PathBuf| read_batch(path).map_err(RAError::from)
        };

        let chunks = files
            .map(|path| {
                load_fn(path)?[0]
                    .as_any()
                    .downcast_ref::<A>()
                    .cloned()
                    .ok_or(RAError::TypeCastError)
            })
            .collect::<Result<Vec<_>, RAError>>()?;
        let chunk_size = chunks.first().map(|c| c.len()).unwrap_or(0);
        Ok(Self::new(chunks, chunk_size))
    }
}

#[cfg(test)]
mod test {
    use crate::arrow2::{
        array::{PrimitiveArray, StructArray},
        datatypes::{ArrowDataType as DataType, Field},
    };
    use itertools::Itertools;

    use crate::chunked_array::{
        array_ops::{ArrayOps, BaseArrayOps, PrimitiveCol},
        chunked_array::ChunkedArray,
    };

    fn make_chunks(data: Vec<i32>, chunk_size: usize) -> ChunkedArray<StructArray> {
        let data = data
            .into_iter()
            .chunks(chunk_size)
            .into_iter()
            .map(|c| c.collect_vec())
            .collect_vec();
        let dtype = DataType::Struct(vec![Field::new("test", DataType::Int32, false)]);
        let arrays = data
            .into_iter()
            .map(|v| {
                StructArray::new(
                    dtype.clone(),
                    vec![PrimitiveArray::from_vec(v).boxed()],
                    None,
                )
            })
            .collect_vec();
        let chunks = ChunkedArray::new(arrays.into(), chunk_size);
        chunks
    }

    #[test]
    fn test() {
        let data = (0..20).collect_vec();
        let chunks = make_chunks(data.clone(), 10);

        let col = chunks.primitive_col::<i32>(0).unwrap();
        let res: Vec<i32> = col.iter().flatten().collect();
        assert_eq!(data, res);
    }

    #[test]
    fn test_slice() {
        let data = (0..20).collect_vec();
        let chunks = make_chunks(data.clone(), 10);
        let slice = chunks.primitive_col::<i32>(0).unwrap();
        let col = slice.slice(1..5);
        let res: Vec<i32> = col.iter().flatten().collect();
        assert_eq!(data[1..5], res);
    }

    #[test]
    fn test_slice_multiple_chunks() {
        let data = (0..20).collect_vec();
        let chunks = make_chunks(data.clone(), 5);
        let slice = chunks.primitive_col::<i32>(0).unwrap();
        let col = slice.slice(1..14);
        let res: Vec<i32> = col.iter().flatten().collect();
        assert_eq!(data[1..14], res);
    }

    #[test]
    fn test_subslice() {
        let data = (0..20).collect_vec();
        let chunks = make_chunks(data.clone(), 5);
        let slice = chunks.primitive_col::<i32>(0).unwrap();
        let col = slice.slice(1..14);
        let col = col.slice(3..10);
        let res: Vec<i32> = col.iter().flatten().collect();
        assert_eq!(data[4..11], res);
    }
}
