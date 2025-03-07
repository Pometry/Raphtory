//! This implements merging sorted, indexable data on a per-chunk basis. The algorithm is based on the
//! MergePath algorithm: <https://doi.org/10.1109/IPDPSW.2012.202> which uses binary search to find
//! the chunk boundaries.

use itertools::{EitherOrBoth, Itertools};
use rayon::prelude::*;
use std::{cmp::Ordering, fmt::Debug, marker::PhantomData, mem, ops::Range};

#[derive(Copy, Clone)]
pub struct MappedIndexedView<I, M> {
    inner: I,
    map: M,
}

impl<I: IndexedView, M: Fn(I::Value) -> T + Send + Sync + Copy, T: Send + Sync + Debug> IndexedView
    for MappedIndexedView<I, M>
{
    type Value = T;

    #[inline]
    fn get(self, index: usize) -> Option<Self::Value> {
        self.inner.get(index).map(self.map)
    }

    #[inline]
    fn range(self, range: Range<usize>) -> impl Iterator<Item = Self::Value> {
        self.inner.range(range).map(self.map)
    }

    #[inline]
    fn advance(&mut self, by: usize) {
        self.inner.advance(by)
    }

    #[inline]
    fn len(&self) -> usize {
        self.inner.len()
    }
}

/// Interface for indexable data views that can be merged
pub trait IndexedView: Send + Sync + Copy {
    type Value: Send + Sync + Debug;

    /// Get the value at `index`, or `None` if `index >= self.len()`
    fn get(self, index: usize) -> Option<Self::Value>;

    /// Returns an iterator over the values in `range`
    fn range(self, range: Range<usize>) -> impl Iterator<Item = Self::Value>;

    /// Advance the view, discarding the first `by` values
    fn advance(&mut self, by: usize);

    fn len(&self) -> usize;

    /// Transform the values according to `map_fn` and return a new view
    fn map<T: Send + Sync + Debug, M: Fn(Self::Value) -> T + Send + Sync + Copy>(
        self,
        map_fn: M,
    ) -> MappedIndexedView<Self, M>
    where
        Self: Sized,
    {
        MappedIndexedView {
            inner: self,
            map: map_fn,
        }
    }

    /// Return an iterator over the result of merging `self` and `other` in chunks of size `chunk_size`.
    ///
    /// Both `self` and `other` are assumed to be sorted according to `cmp` and the resulting values
    /// are transformed using `map`.
    fn merge_chunks_by<C: OutputChunk>(
        self,
        other: impl IndexedView<Value = Self::Value>,
        chunk_size: usize,
        cmp: impl Fn(&Self::Value, &Self::Value) -> Ordering + Send + Sync,
        map: impl Fn(Self::Value) -> C::Value + Send + Sync,
    ) -> impl Iterator<Item = C>
    where
        Self: Sized,
    {
        let exhausted = (self.len() == 0 && other.len() == 0) || chunk_size == 0;
        let merger = Merger::new(self, other, cmp);
        ChunkedMergeIter {
            merger,
            chunk_size,
            map,
            exhausted,
            output_chunk: PhantomData,
        }
    }

    /// Return an iterator over the result of merging `self` and `other` in chunks of size `chunk_size`,
    /// discarding duplicate values that exist in both `self` and `other`.
    ///
    /// Both `self` and `other` are assumed to be sorted and deduplicated according to `cmp`
    /// and the resulting values are transformed using `map`.
    fn merge_dedup_chunks_by<C: OutputChunk>(
        self,
        other: impl IndexedView<Value = Self::Value>,
        chunk_size: usize,
        cmp: impl Fn(&Self::Value, &Self::Value) -> Ordering + Send + Sync,
        map: impl Fn(Self::Value) -> C::Value + Send + Sync + Copy,
    ) -> impl Iterator<Item = C>
    where
        Self: Sized,
        C::Value: Copy,
    {
        ChunkedMergeJoinIter::new(
            self,
            other,
            cmp,
            move |v| map(v.reduce(|v, _| v)),
            chunk_size,
        )
    }

    /// Return an iterator over the result of merging `self` and `other` in chunks of size `chunk_size`,
    /// joining any duplicate values that exist in both `self` and `other`.
    ///
    /// Both `self` and `other` are assumed to be sorted and deduplicated according to `cmp`
    /// and the resulting values are transformed using `map`.
    fn merge_join_chunks_by<C: OutputChunk>(
        self,
        other: impl IndexedView<Value = Self::Value>,
        chunk_size: usize,
        cmp: impl Fn(&Self::Value, &Self::Value) -> Ordering + Send + Sync,
        map: impl Fn(EitherOrBoth<Self::Value>) -> C::Value + Send + Sync,
    ) -> impl Iterator<Item = C>
    where
        Self: Sized,
        C::Value: Copy,
    {
        ChunkedMergeJoinIter::new(self, other, cmp, map, chunk_size)
    }
}

/// Interface trait for a chunk of output
pub trait OutputChunk {
    type Value: Send + Sync + Debug;
    type Chunk<'a>: MutChunk<Value = Self::Value>
    where
        Self: 'a;

    /// Truncate the chunk to a new length: `len`
    fn truncate(&mut self, len: usize);

    /// Create a new default-initialized chunk with `len` values
    fn with_capacity(len: usize) -> Self
    where
        Self: Sized;

    /// Get a `MutChunk` reference for the chunk as a whole
    fn as_chunk_mut(&mut self) -> Self::Chunk<'_>;

    /// Remove all items starting from index `start` and return them as a parallel iterator
    fn par_drain_tail(&mut self, start: usize) -> impl IndexedParallelIterator<Item = Self::Value>;
}

/// Interface for a mutable reference to a sub-chunk of output
pub trait MutChunk: Send + Sync {
    type Value;
    type Chunk<'a>: MutChunk<Value = Self::Value>
    where
        Self: 'a;

    fn fill(&mut self, values: impl Iterator<Item = Self::Value>);

    fn par_fill(&mut self, values: impl IndexedParallelIterator<Item = Self::Value>);

    /// Return a parallel iterator over sub-chunks of size `chunk_size`
    fn par_chunks_mut(
        &mut self,
        chunk_size: usize,
    ) -> impl IndexedParallelIterator<Item = Self::Chunk<'_>>;

    /// Return a sub-chunk view starting from `start`
    fn tail(&mut self, start: usize) -> Self::Chunk<'_>;

    /// Copy values for indices `range` to instead start at `dst`
    fn copy_within(&mut self, range: Range<usize>, dst: usize)
    where
        Self::Value: Copy;

    fn len(&self) -> usize;
}

impl<'b, T: Send + Sync + Debug> MutChunk for &'b mut [T] {
    type Value = T;
    type Chunk<'a>
        = &'a mut [T]
    where
        Self: 'a;

    fn fill(&mut self, values: impl Iterator<Item = Self::Value>) {
        for (entry, value) in self.iter_mut().zip(values) {
            *entry = value;
        }
    }

    fn par_fill(&mut self, values: impl IndexedParallelIterator<Item = Self::Value>) {
        self.par_iter_mut().zip(values).for_each(|(entry, value)| {
            *entry = value;
        })
    }

    fn par_chunks_mut(
        &mut self,
        chunk_size: usize,
    ) -> impl IndexedParallelIterator<Item = &mut [T]> {
        ParallelSliceMut::par_chunks_mut(*self, chunk_size)
    }

    fn tail(&mut self, start: usize) -> Self::Chunk<'_> {
        &mut self[start..]
    }

    fn copy_within(&mut self, range: Range<usize>, dst: usize)
    where
        Self::Value: Copy,
    {
        <[T]>::copy_within(self, range, dst)
    }

    fn len(&self) -> usize {
        <[T]>::len(self)
    }
}

impl<T: Send + Sync + Debug + Default> OutputChunk for Vec<T> {
    type Value = T;
    type Chunk<'a>
        = &'a mut [T]
    where
        Self: 'a;

    fn truncate(&mut self, len: usize) {
        self.truncate(len)
    }

    fn with_capacity(len: usize) -> Self
    where
        Self: Sized,
    {
        let mut v = Vec::with_capacity(len);
        v.resize_with(len, T::default);
        v
    }

    fn as_chunk_mut(&mut self) -> Self::Chunk<'_> {
        self
    }

    fn par_drain_tail(&mut self, start: usize) -> impl IndexedParallelIterator<Item = Self::Value> {
        self.par_drain(start..)
    }
}

impl<'a, T: Send + Sync + Debug + 'a> IndexedView for &'a [T] {
    type Value = &'a T;

    #[inline]
    fn get(self, index: usize) -> Option<Self::Value> {
        <[T]>::get(self, index)
    }

    #[inline]
    fn range(self, range: Range<usize>) -> impl Iterator<Item = Self::Value> {
        self[range].iter()
    }

    #[inline]
    fn advance(&mut self, by: usize) {
        let value = mem::take(self);
        *self = &value[by..];
    }

    #[inline]
    fn len(&self) -> usize {
        <[T]>::len(self)
    }
}

pub struct ChunkedMergeIter<F, M, IL, IR, C> {
    merger: Merger<IL, IR, F>,
    map: M,
    chunk_size: usize,
    exhausted: bool,
    output_chunk: PhantomData<C>,
}

impl<
        V,
        F: Fn(&V, &V) -> Ordering + Send + Sync,
        M: Fn(V) -> C::Value + Send + Sync,
        C: OutputChunk,
        IL: IndexedView<Value = V>,
        IR: IndexedView<Value = V>,
    > Iterator for ChunkedMergeIter<F, M, IL, IR, C>
{
    type Item = C;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }
        let mut next_chunk = C::with_capacity(self.chunk_size);
        let actual_len = self
            .merger
            .merge_into_chunk(next_chunk.as_chunk_mut(), &self.map);
        next_chunk.truncate(actual_len);
        self.exhausted = actual_len < self.chunk_size;
        (actual_len > 0).then_some(next_chunk)
    }
}

pub struct ChunkedMergeJoinIter<F, M, IL, IR, T, C> {
    chunk_size: usize,
    remainder: Vec<T>,
    merger: Merger<IL, IR, F>,
    map: M,
    exhausted: bool,
    output_chunk: PhantomData<C>,
}

impl<
        V,
        T: Copy + Send + Sync + Debug,
        C: OutputChunk<Value = T>,
        F: Fn(&V, &V) -> Ordering + Send + Sync,
        M: Fn(EitherOrBoth<V>) -> T + Send + Sync,
        IL: IndexedView<Value = V>,
        IR: IndexedView<Value = V>,
    > ChunkedMergeJoinIter<F, M, IL, IR, T, C>
{
    fn new(left: IL, right: IR, cmp: F, map: M, chunk_size: usize) -> Self {
        let exhausted = (left.len() == 0 && right.len() == 0) || chunk_size == 0;
        Self {
            chunk_size,
            remainder: Vec::with_capacity(chunk_size),
            merger: Merger::new(left, right, cmp),
            map,
            exhausted,
            output_chunk: PhantomData,
        }
    }
}

impl<
        V,
        T: Copy + Send + Sync + Debug,
        C: OutputChunk<Value = T>,
        F: Fn(&V, &V) -> Ordering + Send + Sync,
        M: Fn(EitherOrBoth<V>) -> T + Send + Sync,
        IL: IndexedView<Value = V>,
        IR: IndexedView<Value = V>,
    > Iterator for ChunkedMergeJoinIter<F, M, IL, IR, T, C>
{
    type Item = C;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }
        let rem_len = self.remainder.len();
        // We need to reserve double the output values for the newly merged part to ensure the
        // final output chunk will be full, i.e, we need `2*(self.chunk_size-rem_len) + rem_len` values
        let len = 2 * self.chunk_size - rem_len;
        let mut next_chunk = C::with_capacity(len);

        // First put any remainder from the last merge
        next_chunk
            .as_chunk_mut()
            .par_fill(self.remainder.par_drain(..));

        // Merge the new part and correctly truncate the chunk
        let actual_len = self
            .merger
            .merge_join_into_chunk(next_chunk.as_chunk_mut().tail(rem_len), &self.map);
        let total_len = rem_len + actual_len;
        next_chunk.truncate(total_len);

        if total_len < self.chunk_size {
            self.exhausted = true;
            (total_len > 0).then_some(next_chunk)
        } else {
            // put any excess values into the remainder to be returned in the next chunk
            next_chunk
                .par_drain_tail(self.chunk_size)
                .collect_into_vec(&mut self.remainder);
            Some(next_chunk)
        }
    }
}

pub struct Merger<IL, IR, F> {
    left: IL,
    right: IR,
    cmp: F,
}

impl<
        V,
        IL: IndexedView<Value = V>,
        IR: IndexedView<Value = V>,
        F: Fn(&V, &V) -> Ordering + Send + Sync,
    > Merger<IL, IR, F>
{
    fn new(left: IL, right: IR, cmp: F) -> Self {
        Merger { left, right, cmp }
    }

    /// Extends `self.cmp` with the appropriate boundary conditions for the diagonal binary search
    /// where one of the values may not exist
    #[inline]
    fn cmp_opt(&self, left: Option<&V>, right: Option<&V>) -> bool {
        match (left, right) {
            (Some(l), Some(r)) => self.cmp(l, r),
            (None, Some(_)) => false,
            (Some(_), None) => true,
            (None, None) => false,
        }
    }

    #[inline]
    fn cmp(&self, left: &V, right: &V) -> bool {
        (self.cmp)(left, right).is_le()
    }

    #[inline]
    fn eq(&self, left: &V, right: &V) -> bool {
        (self.cmp)(left, right).is_eq()
    }

    /// Use binary search to find the input indices for position `diag` in the merged output
    #[inline]
    fn bin_search_diag(&self, diag: usize) -> (usize, usize) {
        if self.left.len() + self.right.len() <= diag {
            return (self.left.len(), self.right.len());
        }

        let range_min = diag.saturating_sub(self.right.len());
        let range_max = diag.min(self.left.len());

        let mut size = range_max - range_min;
        let mut left = range_min;
        let mut right = range_max;

        while left < right {
            let mid = left + size / 2;
            let cmp = self.cmp_opt(
                self.left.get(mid).as_ref(),
                self.right.get(diag - mid - 1).as_ref(),
            );

            if cmp {
                left = mid + 1;
            } else {
                right = mid;
            }

            size = right - left;
        }
        let left_idx = left;
        let right_idx = diag - left;

        (left_idx, right_idx)
    }

    /// Merge `self.left` and `self.right` into `chunk` and return the number of values written.
    ///
    /// Note that the chunk is always filled unless both `self.left` and `self.right` is exhausted.
    #[inline]
    fn merge_into_chunk<C: MutChunk>(
        &mut self,
        mut chunk: C,
        map: impl Fn(V) -> C::Value + Send + Sync,
    ) -> usize {
        let len = chunk.len().min(self.left.len() + self.right.len());
        if len > 0 {
            let num_threads = rayon::current_num_threads();
            let sub_chunk_size = len.div_ceil(num_threads);
            chunk
                .par_chunks_mut(sub_chunk_size)
                .enumerate()
                .for_each(|(chunk_id, mut chunk)| {
                    let (left_idx, right_idx) = self.bin_search_diag(chunk_id * sub_chunk_size);
                    chunk.fill(
                        self.left
                            .range(left_idx..self.left.len())
                            .merge_by(self.right.range(right_idx..self.right.len()), |l, r| {
                                self.cmp(l, r)
                            })
                            .map(&map),
                    );
                });
            let (left_idx, right_idx) = self.bin_search_diag(len);
            self.left.advance(left_idx);
            self.right.advance(right_idx);
        }
        len
    }

    /// This assumes both left and right do not have any duplicates.
    /// This first uses the normal binary search on the diagonal to find the chunk boundary.
    /// It then checks if this chunk boundary would split a pair of values that would be joined. If
    /// that is the case, it includes the value in this chunk instead to ensure that no joins happen
    /// across the chunk boundary.
    fn bin_search_diag_merge(&self, diag: usize) -> (usize, usize) {
        let (left_idx, right_idx) = self.bin_search_diag(diag);
        if let Some(left_v) = self.left.get(left_idx) {
            let right_idx_prev = right_idx.saturating_sub(1);
            if let Some(right_v) = self.right.get(right_idx_prev) {
                if self.eq(&left_v, &right_v) {
                    return (left_idx, right_idx_prev);
                }
            }
        }
        if let Some(right_v) = self.right.get(right_idx) {
            let left_idx_prev = left_idx.saturating_sub(1);
            if let Some(left_v) = self.left.get(left_idx_prev) {
                if self.eq(&left_v, &right_v) {
                    return (left_idx_prev, right_idx);
                }
            }
        }
        (left_idx, right_idx)
    }

    /// Merge and join the left and right view into chunk and return the actual size of the chunk
    ///
    /// The resulting chunk has no duplicates if both left and right views have no duplicates.
    ///
    /// Values that compare equal are merged into an `EitherOrBoth::Both` which then need to be
    /// combined into a single value by the mapping function `map_fn`. Calling this method will
    /// try to consume exactly `chunk.len()` values from `self.left` and `self.right` in total. If there
    /// are duplicates, the chunk will only be partially filled with all valid values at the start.
    ///
    /// Note that the chunk will always be at least half-full, unless both `self.left` and
    /// `self.right` are exhausted.
    #[inline]
    fn merge_join_into_chunk<C: MutChunk>(
        &mut self,
        mut chunk: C,
        map_fn: impl Fn(EitherOrBoth<V>) -> C::Value + Send + Sync,
    ) -> usize
    where
        C::Value: Copy,
    {
        let len = chunk.len().min(self.left.len() + self.right.len());
        if len == 0 {
            return 0;
        }

        let num_threads = rayon::current_num_threads();
        let sub_chunk_size = len.div_ceil(num_threads);
        let actual_lens: Vec<_> = chunk
            .par_chunks_mut(sub_chunk_size)
            .enumerate()
            .map(|(chunk_id, mut chunk)| {
                let mut count = 0usize;
                let chunk_start_idx = chunk_id * sub_chunk_size;
                let chunk_end_idx = chunk_start_idx + chunk.len();
                let (left_start_idx, right_start_idx) = self.bin_search_diag_merge(chunk_start_idx);
                let (left_end_idx, right_end_idx) = self.bin_search_diag_merge(chunk_end_idx);
                chunk.fill(
                    self.left
                        .range(left_start_idx..left_end_idx)
                        .merge_join_by(self.right.range(right_start_idx..right_end_idx), &self.cmp)
                        .map(|sorted_value| {
                            count += 1;
                            map_fn(sorted_value)
                        }),
                );
                count
            })
            .collect();

        // make sure all chunks are consecutive in the output
        let mut dst = 0usize;
        for chunk_id in 0..actual_lens.len() - 1 {
            dst += actual_lens[chunk_id];
            let chunk_start = (chunk_id + 1) * sub_chunk_size;
            let chunk_end = chunk_start + actual_lens[chunk_id + 1];
            chunk.copy_within(chunk_start..chunk_end, dst)
        }

        let (left_idx, right_idx) = self.bin_search_diag_merge(len);
        self.left.advance(left_idx);
        self.right.advance(right_idx);
        let len = dst + actual_lens.last().unwrap();
        len
    }
}

#[cfg(test)]
mod test {
    use crate::merge::merge_chunks::IndexedView;
    use itertools::Itertools;
    use proptest::prelude::*;

    prop_compose! {
        fn two_sorted_vecs(max_size: usize)(mut first_vec in prop::collection::vec(any::<u32>(), 0..max_size), chunk_size in 1..max_size, mut second_vec in prop::collection::vec(any::<u32>(), 0..max_size)) -> (Vec<u32>, Vec<u32>, usize) {
            first_vec.sort();
            second_vec.sort();
            (first_vec, second_vec, chunk_size)
        }
    }

    prop_compose! {
        fn two_sorted_vecs_with_duplicates(max_size: usize)(l_size in 0..max_size, r_size in 0..max_size, d_size in 0..max_size)(mut first_vec in prop::collection::vec(any::<u32>(), l_size), mut second_vec in prop::collection::vec(any::<u32>(), r_size), duplicates in prop::collection::vec(any::<u32>(), d_size)) -> (Vec<u32>, Vec<u32>) {
            first_vec.extend_from_slice(&duplicates);
            second_vec.extend_from_slice(&duplicates);
            first_vec.sort();
            first_vec.dedup();
            second_vec.sort();
            second_vec.dedup();
            (first_vec, second_vec)
        }
    }

    fn test_inner(left: Vec<u32>, right: Vec<u32>, chunk_size: usize) {
        let all: Vec<_> = left.iter().merge(right.iter()).cloned().collect();
        let chunked_merge: Vec<_> = left
            .merge_chunks_by::<Vec<_>>(right.as_slice(), chunk_size, |l, r| l.cmp(r), |v| *v)
            .flatten()
            .collect();
        assert_eq!(all, chunked_merge);
    }

    fn test_inner_with_duplicates(left: Vec<u32>, right: Vec<u32>, chunk_size: usize) {
        let all: Vec<_> = left.iter().merge(right.iter()).copied().dedup().collect();
        let chunked_merge: Vec<_> = left
            .merge_dedup_chunks_by::<Vec<_>>(right.as_slice(), chunk_size, |l, r| l.cmp(r), |v| *v)
            .flatten()
            .collect();
        assert_eq!(all, chunked_merge);
    }

    #[test]
    fn two_identical() {
        test_inner(vec![0, 165739864], vec![0, 165739864], 2)
    }

    #[test]
    fn one_empty() {
        test_inner(vec![], vec![0], 1)
    }

    #[test]
    fn strange_chunk_boundary_dedup() {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(3)
            .build()
            .unwrap();
        pool.install(|| test_inner_with_duplicates(vec![0, 1, 2, 4], vec![0, 1, 2, 3, 4], 4))
    }

    #[test]
    fn larger() {
        test_inner(vec![1, 2, 3, 5], vec![4, 5, 7, 11], 4)
    }

    #[test]
    fn size_1() {
        test_inner(vec![0], vec![1], 1)
    }

    #[test]
    fn test_merge() {
        proptest!(|((left, right, chunk_size) in two_sorted_vecs(100))| {
            test_inner(left, right, chunk_size)
        })
    }

    #[test]
    fn test_dedup() {
        proptest!(|((left, right) in two_sorted_vecs_with_duplicates(100), chunk_size in 1usize..100, num_threads in 1usize..32usize)| {
                    let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build()
            .unwrap();
            pool.install(|| {test_inner_with_duplicates(left, right, chunk_size)})
        })
    }

    #[test]
    fn test_example() {
        let left = [1usize, 2, 3, 4];
        let right = [1usize, 3, 4, 5];
        let res: Vec<_> = left
            .merge_dedup_chunks_by::<Vec<_>>(&right[..], 2, |l, r| l.cmp(r), |v| *v)
            .collect();
        assert_eq!(res, [vec![1, 2], vec![3, 4], vec![5]])
    }
}
