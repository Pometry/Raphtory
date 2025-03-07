use crate::{
    arrow2::offset::{Offsets, OffsetsBuffer},
    chunked_array::ChunkedArraySlice,
    file_prefix::{sorted_file_list, GraphPaths},
    load::mmap::mmap_buffer,
    prelude::{normalized_range, ArrayOps, BaseArrayOps, ChunkRange, Chunked},
    RAError,
};
use polars_arrow::datatypes::ArrowDataType;
use rayon::{
    iter::plumbing::{bridge, Consumer, Producer, ProducerCallback, UnindexedConsumer},
    prelude::*,
};
use std::{
    ops::{Range, RangeBounds},
    path::Path,
    sync::Arc,
};

#[derive(Debug, Clone)]
pub struct ChunkedOffsets {
    offsets: Arc<[OffsetsBuffer<i64>]>,
    chunk_size: usize,
}

impl PartialEq for ChunkedOffsets {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}

impl Chunked for ChunkedOffsets {
    type Chunk = OffsetsBuffer<i64>;

    fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    fn num_chunks(&self) -> usize {
        self.offsets.len()
    }

    fn chunk(&self, i: usize) -> &Self::Chunk {
        &self.offsets[i]
    }

    fn iter_inner_chunks_range(
        &self,
        range: ChunkRange,
    ) -> impl DoubleEndedIterator<Item = &Self::Chunk> + ExactSizeIterator {
        self.offsets[range.inner_range()].iter()
    }
}

impl<'a> BaseArrayOps<'a> for ChunkedOffsets {
    type BaseSlice = &'a Self;
    type OwnedBaseSlice = Self;

    fn len(&self) -> usize {
        self.chunk_size * self.offsets.len().saturating_sub(1)
            + self.offsets.last().map(|c| c.len_proxy()).unwrap_or(0)
    }

    fn data_type(&self) -> Option<&ArrowDataType> {
        None
    }

    fn slice<R: RangeBounds<usize>>(&'a self, range: R) -> ChunkedArraySlice<'a, Self::BaseSlice> {
        let range = normalized_range(range, self.len());
        ChunkedArraySlice::new(self, range)
    }

    fn sliced<R: RangeBounds<usize>>(
        self,
        range: R,
    ) -> ChunkedArraySlice<'a, Self::OwnedBaseSlice> {
        let range = normalized_range(range, self.len());
        ChunkedArraySlice::new(self, range)
    }
}

impl<'a> ArrayOps<'a> for ChunkedOffsets {
    type Value = (usize, usize);
    type IntoValue = (usize, usize);
    type Chunk = OffsetsBuffer<i64>;
    type Slice = Self::BaseSlice;
    type OwnedSlice = Self::OwnedBaseSlice;

    fn get(&'a self, i: usize) -> Self::Value {
        let (c, i) = self.resolve_idx(i);
        self.offsets[c].start_end(i)
    }

    fn into_get(self, i: usize) -> Self::IntoValue {
        self.get(i)
    }

    fn iter_chunks(
        &'a self,
    ) -> impl DoubleEndedIterator<Item = &'a Self::Chunk> + ExactSizeIterator {
        self.offsets.iter()
    }
}

impl ChunkedOffsets {
    pub fn new(mut offsets: Vec<OffsetsBuffer<i64>>, chunk_size: usize) -> Self {
        if let Some(last) = offsets.last() {
            if last.len_proxy() == chunk_size {
                offsets.push(Offsets::try_from(vec![*last.last()]).unwrap().into());
            }
        }
        Self {
            offsets: offsets.into(),
            chunk_size,
        }
    }

    pub unsafe fn mmap_from_dir(
        dir: impl AsRef<Path>,
        prefix: GraphPaths,
    ) -> Result<Self, RAError> {
        let files = sorted_file_list(dir, prefix)?;
        let offsets = files
            .map(|path| {
                let buffer = mmap_buffer::<i64>(path, 0)?;
                let offsets = OffsetsBuffer::try_from(buffer)?;
                Ok(offsets)
            })
            .collect::<Result<Vec<_>, RAError>>()?;
        Ok(offsets.into())
    }
}

impl From<Vec<OffsetsBuffer<i64>>> for ChunkedOffsets {
    fn from(value: Vec<OffsetsBuffer<i64>>) -> Self {
        assert!(!value.is_empty());
        let mut chunk_size = value[0].len_proxy();

        if value.len() == 1 {
            chunk_size += 1;
        }

        for window in value.windows(2) {
            assert_eq!(window[0].last(), window[1].first());
        }
        for o in &value[..value.len() - 1] {
            assert_eq!(o.len_proxy(), chunk_size)
        }
        assert!(value.last().unwrap().len_proxy() <= chunk_size);
        Self::new(value, chunk_size)
    }
}

impl From<OffsetsBuffer<i64>> for ChunkedOffsets {
    fn from(value: OffsetsBuffer<i64>) -> Self {
        let chunk_size = value.len_proxy() + 1;
        Self::new(vec![value], chunk_size)
    }
}

impl Default for ChunkedOffsets {
    fn default() -> Self {
        OffsetsBuffer::default().into()
    }
}

impl ChunkedOffsets {
    #[inline]
    pub(super) fn resolve_idx(&self, idx: usize) -> (usize, usize) {
        if self.chunk_size == 0 {
            return (0, 0);
        }
        (idx / self.chunk_size, idx % self.chunk_size)
    }

    /// Returns the number of offset slices in this container.
    #[inline]
    pub fn len(&self) -> usize {
        self.offsets.len().saturating_sub(1) * self.chunk_size
            + self.offsets.last().map(|o| o.len_proxy()).unwrap_or(0)
    }

    /// Returns the range of the offsets.
    #[inline]
    pub fn range(&self) -> usize {
        self.last() - self.first()
    }

    /// Returns the first offset.
    #[inline]
    pub fn first(&self) -> usize {
        *self.offsets[0].first() as usize
    }

    /// Returns the last offset.
    #[inline]
    pub fn last(&self) -> usize {
        *self.offsets.last().unwrap().last() as usize
    }

    /// Returns a range (start, end) corresponding to the position `index`
    /// # Panic
    /// This function panics iff `index >= self.len()`
    #[inline]
    pub fn start_end(&self, index: usize) -> (usize, usize) {
        let (chunk_index, index) = self.resolve_idx(index);
        self.offsets[chunk_index].start_end(index)
    }

    #[inline]
    pub fn start(&self, index: usize) -> usize {
        let (chunk_index, index) = self.resolve_idx(index);
        self.offsets[chunk_index][index] as usize
    }

    #[inline]
    pub fn end(&self, index: usize) -> usize {
        let (chunk_index, index) = self.resolve_idx(index);
        self.offsets[chunk_index][index + 1] as usize
    }

    /// Returns a range (start, end) corresponding to the position `index`
    /// # Safety
    /// `index` must be `< self.len()`
    #[inline]
    pub unsafe fn start_end_unchecked(&self, index: usize) -> (usize, usize) {
        let (chunk_index, index) = self.resolve_idx(index);
        self.offsets[chunk_index].start_end_unchecked(index)
    }

    /// Returns an iterator with the lengths of the offsets
    #[inline]
    pub fn lengths(&self) -> impl Iterator<Item = usize> + '_ {
        self.offsets.iter().flat_map(|o| o.lengths())
    }

    pub fn iter(&self) -> impl Iterator<Item = Range<usize>> + '_ {
        self.offsets
            .iter()
            .flat_map(|offsets| offsets.windows(2).map(|w| w[0] as usize..w[1] as usize))
    }

    pub fn par_iter(&self) -> impl IndexedParallelIterator<Item = Range<usize>> + '_ {
        (0..self.len()).into_par_iter().map(|i| {
            let (start, end) = self.start_end(i);
            start..end
        })
    }

    pub fn slice(&self, range: Range<usize>) -> ChunkedOffsets {
        let (start_chunk_index, start_index) = self.resolve_idx(range.start);
        let (mut end_chunk_index, mut end_index) = self.resolve_idx(range.end);
        if end_index == 0 {
            if end_chunk_index == 0 {
                return Self::default();
            }
            end_chunk_index -= 1;
            end_index = self.chunk_size;
        }
        if start_chunk_index == end_chunk_index {
            let mut offsets = self.offsets[start_chunk_index].clone();
            offsets.slice(start_index, end_index - start_index + 1);
            offsets.into()
        } else {
            let mut first_offset = self.offsets[start_chunk_index].clone();
            first_offset.slice(start_index, first_offset.len() - start_index);
            let mut offsets = vec![first_offset];
            for o in &self.offsets[start_chunk_index + 1..end_chunk_index] {
                offsets.push(o.clone())
            }
            let mut last_offset = self.offsets[end_chunk_index].clone();
            last_offset.slice(0, end_index + 1);
            offsets.push(last_offset);
            offsets.into()
        }
    }

    pub fn make_local_offsets(
        &self,
        start_offset: usize,
        end_offset: usize,
    ) -> (usize, usize, Vec<i64>) {
        if end_offset <= start_offset {
            return (0, 0, vec![0]);
        }

        let end_offset = end_offset.min(self.last());
        let (start_chunk_id, start_index) = self.find_chunk_and_index(start_offset);
        let (end_chunk_id, end_index) = self.find_chunk_and_index(end_offset - 1);

        let end = end_chunk_id * self.chunk_size + end_index + 1;
        let start = start_chunk_id * self.chunk_size + start_index;

        let mut local_offsets = Vec::with_capacity(end - start + 1);
        local_offsets.push(0);

        if start_chunk_id == end_chunk_id {
            let values = &self.offsets[start_chunk_id][start_index + 1..=end_index];
            local_offsets.extend_from_slice(values);
        } else {
            let start_values = &self.offsets[start_chunk_id][start_index + 1..];
            local_offsets.extend_from_slice(start_values);
            for chunk_id in start_chunk_id + 1..end_chunk_id {
                let values = &self.offsets[chunk_id][1..];
                local_offsets.extend_from_slice(values);
            }
            let end_values = &self.offsets[end_chunk_id][1..=end_index];
            local_offsets.extend_from_slice(end_values);
        }

        // sustract the first value from the local offsets
        let len = local_offsets.len();
        local_offsets[1..len]
            .iter_mut()
            .for_each(|v| *v -= start_offset as i64);

        local_offsets.push(end_offset as i64 - start_offset as i64);

        (start, end, local_offsets)
    }

    pub fn find_index(&self, offset: usize) -> usize {
        let (chunk_id, index) = self.find_chunk_and_index(offset);
        if chunk_id >= self.offsets.len() {
            return self.len();
        }
        chunk_id * self.chunk_size + index
    }

    pub fn find_index_from(&self, offset: usize, start_index: usize) -> usize {
        let (chunk_id, index) = self.find_chunk_and_index_from(offset, start_index);
        if chunk_id >= self.offsets.len() {
            self.len()
        } else {
            chunk_id * self.chunk_size + index
        }
    }

    pub fn find_chunk_and_index(&self, offset: usize) -> (usize, usize) {
        let chunk_id = self
            .offsets
            .partition_point(|chunk| chunk.last() <= &(offset as i64));
        if chunk_id >= self.offsets.len() {
            return (chunk_id, 0);
        }
        let offsets_buffer = &self.offsets[chunk_id];
        let index = offsets_buffer.partition_point(|v| v <= &(offset as i64)) - 1;

        (chunk_id, index)
    }

    pub fn find_chunk_and_index_from(&self, offset: usize, start_index: usize) -> (usize, usize) {
        let (start_chunk, start_index) = self.resolve_idx(start_index);
        let chunk_id = self.offsets[start_chunk..]
            .partition_point(|chunk| chunk.last() <= &(offset as i64))
            + start_chunk;
        if chunk_id >= self.offsets.len() {
            return (chunk_id, 0);
        }
        let offsets_buffer = &self.offsets[chunk_id];
        let index = if chunk_id == start_chunk {
            offsets_buffer[start_index..].partition_point(|v| v <= &(offset as i64)) + start_index
                - 1
        } else {
            offsets_buffer.partition_point(|v| v <= &(offset as i64)) - 1
        };

        (chunk_id, index)
    }

    pub fn par_iter_exploded(&self) -> impl IndexedParallelIterator<Item = usize> + '_ {
        ExplodedIndexParIter(ExplodedIndexIter::new(self))
    }
}

pub struct ExplodedValuesIter<'a, A> {
    values: A,
    offsets: &'a ChunkedOffsets,
    start_index: usize,
    start_offset: usize,
    end_offset: usize,
    end_index: usize,
    next_index_offset: usize,
    previous_index_offset: usize,
}

impl<'a, A: ArrayOps<'a>> ExplodedValuesIter<'a, A> {
    pub fn new(values: A, offsets: &'a ChunkedOffsets) -> Self {
        let end_offset = offsets.last();
        let next_index_offset = if end_offset == 0 { 0 } else { offsets.end(0) };
        let end_index = offsets.len().saturating_sub(1);
        let previous_index_offset = offsets.start(end_index);
        Self {
            values,
            offsets,
            start_index: 0,
            start_offset: 0,
            end_offset: offsets.last(),
            end_index,
            next_index_offset,
            previous_index_offset,
        }
    }

    pub fn range(values: A, offsets: &'a ChunkedOffsets, offsets_range: Range<usize>) -> Self {
        let start_index = offsets.find_index(offsets_range.start);
        let end_offset = offsets.last().min(offsets_range.end);
        let next_index_offset = if start_index < offsets.len() {
            offsets.end(start_index)
        } else {
            end_offset
        };
        let end_index = offsets.find_index(offsets_range.end);
        let previous_index_offset = offsets.start(end_index);
        Self {
            values,
            offsets,
            start_index,
            start_offset: offsets_range.start,
            end_offset,
            end_index,
            next_index_offset,
            previous_index_offset,
        }
    }

    pub fn new_from_index_range(
        values: A,
        offsets: &'a ChunkedOffsets,
        start_index: usize,
        start_offset: usize,
        end_index: usize,
        end_offset: usize,
    ) -> Self {
        let next_index_offset = if start_index < offsets.len() {
            offsets.end(start_index)
        } else {
            end_offset
        };
        let previous_index_offset = offsets.start(end_index);
        Self {
            values,
            offsets,
            start_index,
            start_offset,
            end_offset,
            end_index,
            next_index_offset,
            previous_index_offset,
        }
    }
}

impl<'a, A: ArrayOps<'a>> Iterator for ExplodedValuesIter<'a, A> {
    type Item = A::IntoValue;

    fn next(&mut self) -> Option<Self::Item> {
        if self.start_offset >= self.end_offset {
            return None;
        }
        while self.start_offset == self.next_index_offset && self.start_offset != self.end_offset {
            self.start_index += 1;
            self.previous_index_offset = self.next_index_offset;
            self.next_index_offset = self.offsets.end(self.start_index);
        }
        self.start_offset += 1;
        Some(self.values.clone().into_get(self.start_index))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.end_offset - self.start_offset;
        (len, Some(len))
    }
}

impl<'a, A: ArrayOps<'a>> ExactSizeIterator for ExplodedValuesIter<'a, A> {}

impl<'a, A: ArrayOps<'a>> DoubleEndedIterator for ExplodedValuesIter<'a, A> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.end_offset <= self.start_offset {
            return None;
        }
        while self.end_offset == self.previous_index_offset && self.end_offset > self.start_offset {
            self.end_index -= 1;
            self.next_index_offset = self.previous_index_offset;
            self.previous_index_offset = self.offsets.start(self.end_index);
        }
        self.end_offset -= 1;
        Some(self.values.clone().into_get(self.end_index))
    }
}

impl<'a, A: ArrayOps<'a>> IntoParallelIterator for ExplodedValuesIter<'a, A>
where
    A::IntoValue: Send,
{
    type Iter = ExplodedValuesParIter<'a, A>;
    type Item = A::IntoValue;

    fn into_par_iter(self) -> Self::Iter {
        ExplodedValuesParIter(self)
    }
}

pub struct ExplodedValuesParIter<'a, A>(ExplodedValuesIter<'a, A>);

impl<'a, A: ArrayOps<'a>> ParallelIterator for ExplodedValuesParIter<'a, A>
where
    A::IntoValue: Send,
{
    type Item = A::IntoValue;

    fn drive_unindexed<C>(self, consumer: C) -> C::Result
    where
        C: UnindexedConsumer<Self::Item>,
    {
        bridge(self, consumer)
    }

    fn opt_len(&self) -> Option<usize> {
        Some(self.len())
    }
}

impl<'a, A: ArrayOps<'a>> IndexedParallelIterator for ExplodedValuesParIter<'a, A>
where
    A::IntoValue: Send,
{
    fn len(&self) -> usize {
        self.0.len()
    }

    fn drive<C: Consumer<Self::Item>>(self, consumer: C) -> C::Result {
        bridge(self, consumer)
    }

    fn with_producer<CB: ProducerCallback<Self::Item>>(self, callback: CB) -> CB::Output {
        callback.callback(self)
    }
}

impl<'a, A: ArrayOps<'a>> Producer for ExplodedValuesParIter<'a, A>
where
    A::IntoValue: Send,
{
    type Item = A::IntoValue;
    type IntoIter = ExplodedValuesIter<'a, A>;

    fn into_iter(self) -> Self::IntoIter {
        self.0
    }

    fn split_at(self, index: usize) -> (Self, Self) {
        let inner = self.0;
        let offset = inner.start_offset + index;
        let offsets = inner.offsets;
        let center_index = offsets.find_index(offset);
        let left_center_index = if offsets.start(center_index) == offset {
            center_index - 1
        } else {
            center_index
        };
        let left = ExplodedValuesIter {
            values: inner.values.clone(),
            offsets,
            start_index: inner.start_index,
            start_offset: inner.start_offset,
            end_offset: offset,
            end_index: left_center_index,
            next_index_offset: inner.next_index_offset,
            previous_index_offset: offsets.start(left_center_index),
        };
        let right = ExplodedValuesIter {
            values: inner.values,
            offsets,
            start_index: center_index,
            start_offset: offset,
            end_offset: inner.end_offset,
            end_index: inner.end_index,
            next_index_offset: offsets.end(center_index),
            previous_index_offset: inner.previous_index_offset,
        };
        (Self(left), Self(right))
    }
}

pub struct ExplodedIndexIter<'a> {
    offsets: &'a ChunkedOffsets,
    start_index: usize,
    start_offset: usize,
    end_offset: usize,
    end_index: usize,
    next_index_offset: usize,
    previous_index_offset: usize,
}

impl<'a> IntoParallelIterator for ExplodedIndexIter<'a> {
    type Iter = ExplodedIndexParIter<'a>;
    type Item = usize;

    fn into_par_iter(self) -> Self::Iter {
        ExplodedIndexParIter(self)
    }
}

impl<'a> ExplodedIndexIter<'a> {
    pub fn new(offsets: &'a ChunkedOffsets) -> Self {
        let end_offset = offsets.last();
        let next_index_offset = if end_offset == 0 { 0 } else { offsets.end(0) };
        let end_index = offsets.len().saturating_sub(1);
        let previous_index_offset = offsets.start(end_index);
        Self {
            offsets,
            start_index: 0,
            start_offset: 0,
            end_offset: offsets.last(),
            end_index,
            next_index_offset,
            previous_index_offset,
        }
    }

    pub fn range(offsets: &'a ChunkedOffsets, offsets_range: Range<usize>) -> Self {
        let start_index = offsets.find_index(offsets_range.start);
        let end_offset = offsets.last().min(offsets_range.end);
        let next_index_offset = if start_index < offsets.len() {
            offsets.end(start_index)
        } else {
            end_offset
        };
        let end_index = offsets.find_index(offsets_range.end);
        let previous_index_offset = offsets.start(end_index);
        Self {
            offsets,
            start_index,
            start_offset: offsets_range.start,
            end_offset,
            end_index,
            next_index_offset,
            previous_index_offset,
        }
    }

    pub fn new_from_index_range(
        offsets: &'a ChunkedOffsets,
        start_index: usize,
        start_offset: usize,
        end_index: usize,
        end_offset: usize,
    ) -> Self {
        let next_index_offset = if start_index < offsets.len() {
            offsets.end(start_index)
        } else {
            end_offset
        };
        let previous_index_offset = offsets.start(end_index);
        Self {
            offsets,
            start_index,
            start_offset,
            end_offset,
            end_index,
            next_index_offset,
            previous_index_offset,
        }
    }
}

impl<'a> Iterator for ExplodedIndexIter<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if self.start_offset >= self.end_offset {
            return None;
        }
        while self.start_offset == self.next_index_offset && self.start_offset != self.end_offset {
            self.start_index += 1;
            self.previous_index_offset = self.next_index_offset;
            self.next_index_offset = self.offsets.end(self.start_index);
        }
        self.start_offset += 1;
        Some(self.start_index)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.end_offset - self.start_offset;
        (len, Some(len))
    }
}

impl<'a> ExactSizeIterator for ExplodedIndexIter<'a> {}

impl<'a> DoubleEndedIterator for ExplodedIndexIter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.end_offset <= self.start_offset {
            return None;
        }
        while self.end_offset == self.previous_index_offset && self.end_offset > self.start_offset {
            self.end_index -= 1;
            self.next_index_offset = self.previous_index_offset;
            self.previous_index_offset = self.offsets.start(self.end_index);
        }
        self.end_offset -= 1;
        Some(self.end_index)
    }
}

pub struct ExplodedIndexParIter<'a>(ExplodedIndexIter<'a>);

impl<'a> ParallelIterator for ExplodedIndexParIter<'a> {
    type Item = usize;

    fn drive_unindexed<C>(self, consumer: C) -> C::Result
    where
        C: UnindexedConsumer<Self::Item>,
    {
        bridge(self, consumer)
    }

    fn opt_len(&self) -> Option<usize> {
        Some(self.len())
    }
}

impl<'a> IndexedParallelIterator for ExplodedIndexParIter<'a> {
    fn len(&self) -> usize {
        self.0.len()
    }

    fn drive<C: Consumer<Self::Item>>(self, consumer: C) -> C::Result {
        bridge(self, consumer)
    }

    fn with_producer<CB: ProducerCallback<Self::Item>>(self, callback: CB) -> CB::Output {
        callback.callback(self)
    }
}

impl<'a> Producer for ExplodedIndexParIter<'a> {
    type Item = usize;
    type IntoIter = ExplodedIndexIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.0
    }

    fn split_at(self, index: usize) -> (Self, Self) {
        let inner = self.0;
        let offset = inner.start_offset + index;
        let offsets = inner.offsets;
        let center_index = offsets.find_index(offset);
        let left_center_index = if offsets.start(center_index) == offset {
            center_index - 1
        } else {
            center_index
        };
        let left = ExplodedIndexIter {
            offsets,
            start_index: inner.start_index,
            start_offset: inner.start_offset,
            end_offset: offset,
            end_index: left_center_index,
            next_index_offset: inner.next_index_offset,
            previous_index_offset: offsets.start(left_center_index),
        };
        let right = ExplodedIndexIter {
            offsets,
            start_index: center_index,
            start_offset: offset,
            end_offset: inner.end_offset,
            end_index: inner.end_index,
            next_index_offset: offsets.end(center_index),
            previous_index_offset: inner.previous_index_offset,
        };
        (Self(left), Self(right))
    }
}

#[cfg(test)]
mod test {
    use crate::{arrow2::offset::OffsetsBuffer, chunked_array::chunked_array::ChunkedArray};
    use itertools::Itertools;
    use rayon::prelude::*;
    use std::ops::Range;

    use crate::chunked_array::chunked_offsets::{
        ChunkedOffsets, ExplodedIndexIter, ExplodedValuesIter,
    };

    #[test]
    fn test_slice() {
        let offsets = unsafe {
            vec![
                OffsetsBuffer::new_unchecked(vec![0, 1, 2].into()),
                OffsetsBuffer::new_unchecked(vec![2, 3, 4].into()),
                OffsetsBuffer::new_unchecked(vec![4, 5].into()),
            ]
        };
        let chunked_offsets: ChunkedOffsets = offsets.into();
        assert_eq!(chunked_offsets.chunk_size, 2);
        assert_eq!(chunked_offsets.len(), 5);
        assert_eq!(chunked_offsets.lengths().collect_vec(), [1; 5]);
        let slice_one_chunk = chunked_offsets.slice(0..2);
        assert_eq!(slice_one_chunk.first(), 0);
        assert_eq!(slice_one_chunk.last(), 2);
        let slice_two_chunks = chunked_offsets.slice(1..3);
        assert_eq!(slice_two_chunks.first(), 1);
        assert_eq!(slice_two_chunks.last(), 3);
    }

    #[test]
    fn test_make_local_offsets() {
        let edges = vec![0, 1, 2, 3, 4];
        let offsets = unsafe {
            vec![
                OffsetsBuffer::new_unchecked(vec![0, 1, 2].into()),
                OffsetsBuffer::new_unchecked(vec![2, 3, 4].into()),
                OffsetsBuffer::new_unchecked(vec![4, 5].into()),
            ]
        };
        let chunked_offsets: ChunkedOffsets = offsets.into();
        assert_eq!(chunked_offsets.chunk_size, 2);
        assert_eq!(chunked_offsets.len(), 5);
        assert_eq!(chunked_offsets.lengths().collect_vec(), [1; 5]);

        let chunk_size = chunked_offsets.chunk_size;
        let actual = (0..(chunked_offsets.last().div_ceil(chunk_size)))
            .into_iter()
            .map(|chunk_id| {
                let start_offset = chunk_id * chunk_size;
                let end_offset = (chunk_id + 1) * chunk_size;
                let (start, end, local_offsets) =
                    chunked_offsets.make_local_offsets(start_offset, end_offset);
                (edges[start..end].into(), local_offsets)
            })
            .collect_vec();

        assert_eq!(
            actual,
            vec![
                (vec![0, 1], vec![0i64, 1, 2]),
                (vec![2, 3], vec![0i64, 1, 2]),
                (vec![4], vec![0i64, 1])
            ]
        );
    }

    #[test]
    fn test_make_local_offsets_1_chunk() {
        let nodes = vec![0, 1, 2, 3];
        let offsets = unsafe { vec![OffsetsBuffer::new_unchecked(vec![0, 3, 4, 5].into())] };
        let chunked_offsets: ChunkedOffsets = ChunkedOffsets::new(offsets, 5);
        assert_eq!(chunked_offsets.chunk_size, 5);
        assert_eq!(chunked_offsets.len(), 3);
        assert_eq!(chunked_offsets.lengths().collect_vec(), vec![3, 1, 1]);

        let chunk_size = chunked_offsets.chunk_size;
        let actual = (0..(chunked_offsets.last().div_ceil(chunk_size)))
            .into_iter()
            .map(|chunk_id| {
                let start_offset = chunk_id * chunk_size;
                let end_offset = (chunk_id + 1) * chunk_size;
                let (start, end, local_offsets) =
                    chunked_offsets.make_local_offsets(start_offset, end_offset);
                (nodes[start..end].into(), local_offsets)
            })
            .collect_vec();

        assert_eq!(actual, vec![(vec![0, 1, 2], vec![0, 3, 4, 5]),]);
    }

    #[test]
    fn test_make_local_offsets_2_chunk() {
        let offsets = unsafe {
            vec![
                OffsetsBuffer::new_unchecked(vec![0, 3, 4].into()),
                OffsetsBuffer::new_unchecked(vec![4, 5].into()),
            ]
        };
        let chunked_offsets: ChunkedOffsets = ChunkedOffsets::new(offsets, 2);
        assert_eq!(chunked_offsets.chunk_size, 2);
        assert_eq!(chunked_offsets.len(), 3);
        assert_eq!(chunked_offsets.lengths().collect_vec(), vec![3, 1, 1]);

        let chunk_size = chunked_offsets.chunk_size;
        let actual = (0..(chunked_offsets.last().div_ceil(chunk_size)))
            .into_iter()
            .map(|chunk_id| {
                let start_offset = chunk_id * chunk_size;
                let end_offset = (chunk_id + 1) * chunk_size;
                let (start, end, local_offsets) =
                    chunked_offsets.make_local_offsets(start_offset, end_offset);
                (start, end, local_offsets)
            })
            .collect_vec();

        assert_eq!(
            actual,
            vec![
                (0, 1, vec![0, 2]),
                (0, 2, vec![0, 1, 2]),
                (2, 3, vec![0, 1]),
            ]
        );
    }

    #[test]
    fn test_exploded() {
        offsets_test(vec![0, 3, 5, 10], vec![0, 0, 0, 1, 1, 2, 2, 2, 2, 2]);
    }

    #[test]
    fn test_exploded_range() {
        offsets_test_range(vec![0, 3, 5, 10], 1..5, vec![0, 0, 1, 1]);
        offsets_test_range(vec![0, 3, 5, 10], 0..9, vec![0, 0, 0, 1, 1, 2, 2, 2, 2]);
    }

    #[test]
    fn test_exploded_skips() {
        offsets_test(vec![0, 0, 1, 1, 2, 2, 4], vec![1, 3, 5, 5]);
    }

    fn offsets_test(offsets: Vec<i64>, expected: Vec<u64>) {
        let values: Vec<u64> = (0..offsets.len() - 1).map(|v| v as u64).collect();
        let offsets = ChunkedOffsets::from(OffsetsBuffer::try_from(offsets).unwrap());
        let values = ChunkedArray::from(vec![values]);
        let rev_expected: Vec<_> = expected.iter().copied().rev().collect();
        let exploded_index: Vec<_> = ExplodedIndexIter::new(&offsets).map(|v| v as u64).collect();
        let exploded_index_par: Vec<_> = ExplodedIndexIter::new(&offsets)
            .into_par_iter()
            .map(|v| v as u64)
            .collect();
        let rev_exploded_index: Vec<_> = ExplodedIndexIter::new(&offsets)
            .map(|v| v as u64)
            .rev()
            .collect();
        let rev_exploded_index_par: Vec<_> = ExplodedIndexIter::new(&offsets)
            .into_par_iter()
            .map(|v| v as u64)
            .rev()
            .collect();
        let exploded_values: Vec<_> = ExplodedValuesIter::new(&values, &offsets).collect();
        let exploded_values_par: Vec<_> = ExplodedValuesIter::new(&values, &offsets)
            .into_par_iter()
            .collect();
        let rev_exploded_values: Vec<_> =
            ExplodedValuesIter::new(&values, &offsets).rev().collect();
        let rev_exploded_values_par: Vec<_> = ExplodedValuesIter::new(&values, &offsets)
            .into_par_iter()
            .rev()
            .collect();
        assert_eq!(exploded_index, expected);
        assert_eq!(exploded_index_par, expected);
        assert_eq!(rev_exploded_index, rev_expected);
        assert_eq!(rev_exploded_index_par, rev_expected);
        assert_eq!(exploded_values, expected);
        assert_eq!(exploded_values_par, expected);
        assert_eq!(rev_exploded_values, rev_expected);
        assert_eq!(rev_exploded_values_par, rev_expected);
    }

    fn offsets_test_range(offsets: Vec<i64>, range: Range<usize>, expected: Vec<u64>) {
        let values: Vec<u64> = (0..offsets.len() - 1).map(|v| v as u64).collect();
        let offsets = ChunkedOffsets::from(OffsetsBuffer::try_from(offsets).unwrap());
        let values = ChunkedArray::from(vec![values]);
        let rev_expected: Vec<_> = expected.iter().copied().rev().collect();
        let exploded_index: Vec<_> = ExplodedIndexIter::range(&offsets, range.clone())
            .map(|v| v as u64)
            .collect();
        let exploded_index_par: Vec<_> = ExplodedIndexIter::range(&offsets, range.clone())
            .into_par_iter()
            .map(|v| v as u64)
            .collect();
        let rev_exploded_index: Vec<_> = ExplodedIndexIter::range(&offsets, range.clone())
            .map(|v| v as u64)
            .rev()
            .collect();
        let rev_exploded_index_par: Vec<_> = ExplodedIndexIter::range(&offsets, range.clone())
            .into_par_iter()
            .map(|v| v as u64)
            .rev()
            .collect();
        let exploded_values: Vec<_> =
            ExplodedValuesIter::range(&values, &offsets, range.clone()).collect();
        let exploded_values_par: Vec<_> =
            ExplodedValuesIter::range(&values, &offsets, range.clone())
                .into_par_iter()
                .collect();
        let rev_exploded_values: Vec<_> =
            ExplodedValuesIter::range(&values, &offsets, range.clone())
                .rev()
                .collect();
        let rev_exploded_values_par: Vec<_> =
            ExplodedValuesIter::range(&values, &offsets, range.clone())
                .into_par_iter()
                .rev()
                .collect();
        assert_eq!(exploded_index, expected);
        assert_eq!(exploded_index_par, expected);
        assert_eq!(rev_exploded_index, rev_expected);
        assert_eq!(rev_exploded_index_par, rev_expected);
        assert_eq!(exploded_values, expected);
        assert_eq!(exploded_values_par, expected);
        assert_eq!(rev_exploded_values, rev_expected);
        assert_eq!(rev_exploded_values_par, rev_expected);
    }
}
