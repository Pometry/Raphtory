use crate::pages::SegmentCounts;
use rayon::prelude::*;
use std::{
    ops::{Deref, Index, IndexMut},
    sync::Arc,
};

/// Index resolver for sharded storage with fixed-size chunks
///
/// Given a sharding scheme where items are distributed across chunks:
/// - chunk_id = index / max_page_len
/// - local_pos = index % max_page_len
///
/// This struct provides O(1) lookup to map any global index to a flat array position,
/// accounting for partially filled chunks.
///
/// # Example
/// With max_page_len = 1000:
/// - Chunk 0: 1000 items (offsets[0] = 0, offsets[1] = 1000)
/// - Chunk 1: 500 items  (offsets[1] = 1000, offsets[2] = 1500)
/// - Chunk 2: 1000 items (offsets[2] = 1500, offsets[3] = 2500)
///
/// To resolve index 1200:
/// - chunk = 1200 / 1000 = 1
/// - local_pos = 1200 % 1000 = 200
/// - flat_index = offsets[1] + 200 = 1000 + 200 = 1200
#[derive(Debug, Clone)]
pub struct StateIndex<I = usize> {
    /// Cumulative offsets: offsets[chunk_id] = starting position in flat array for that chunk
    /// Length is equal to number of chunks + 1 (includes final cumulative value)
    offsets: Box<[usize]>,
    /// Maximum items per chunk
    max_page_len: u32,
    /// Phantom data for index type
    _marker: std::marker::PhantomData<I>,
}

impl<I> From<SegmentCounts<I>> for StateIndex<I>
where
    I: From<usize> + Into<usize>,
{
    fn from(counts: SegmentCounts<I>) -> Self {
        Self::new(
            counts.counts().iter().map(|c| *c as usize),
            counts.max_seg_len(),
        )
    }
}

impl<I: From<usize> + Into<usize>> StateIndex<I> {
    /// Create a new StateIndex with the given chunk configuration
    ///
    /// # Arguments
    /// * `chunk_sizes` - The actual size of each chunk (can be <= max_page_len)
    /// * `max_page_len` - Maximum capacity of each chunk
    pub fn new(chunk_sizes: impl IntoIterator<Item = usize>, max_page_len: u32) -> Self {
        // Build cumulative offsets (includes final cumulative value)
        let mut offsets = Vec::new();
        let mut cumulative = 0;
        for size in chunk_sizes {
            offsets.push(cumulative);
            cumulative += size;
        }
        offsets.push(cumulative); // Add final cumulative value

        Self {
            offsets: offsets.into_boxed_slice(),
            max_page_len,
            _marker: std::marker::PhantomData,
        }
    }

    /// Resolve a global index to a flat array index
    ///
    /// # Arguments
    /// * `index` - Global index across all chunks
    ///
    /// # Returns
    /// Some(flat_index) if the index is valid, None otherwise
    #[inline(always)]
    pub fn resolve(&self, index: I) -> Option<usize> {
        let index: usize = index.into();
        let chunk = index / self.max_page_len as usize;
        let local_pos = index % self.max_page_len as usize;

        let offset = *self.offsets.get(chunk)?;
        let flat_index = offset + local_pos;

        // Verify the flat_index is within bounds of this chunk
        let next_offset = *self.offsets.get(chunk + 1)?;
        if flat_index < next_offset {
            Some(flat_index)
        } else {
            None
        }
    }

    /// Resolve a global index to a flat array index without bounds checking
    ///
    /// # Arguments
    /// * `index` - Global index across all chunks
    ///
    /// # Returns
    /// The flat array index
    ///
    /// # Safety
    /// Panics if the index is out of bounds
    #[inline(always)]
    pub fn resolve_unchecked(&self, index: I) -> usize {
        let index: usize = index.into();
        let chunk = index / self.max_page_len as usize;
        let local_pos = index % self.max_page_len as usize;

        let offset = self.offsets[chunk];
        offset + local_pos
    }

    /// Get the number of chunks
    #[inline]
    pub fn num_chunks(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    /// Get the total number of items across all chunks
    #[inline]
    pub fn len(&self) -> usize {
        self.offsets[self.num_chunks()]
    }

    /// Check if there are no items
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the maximum page length
    #[inline]
    pub fn max_page_len(&self) -> u32 {
        self.max_page_len
    }

    /// Create an iterator over all valid global indices
    ///
    /// This iterates through all chunks and yields the global indices for each item.
    /// For example, with chunk_sizes [10, 1, 5] and max_page_len 10:
    /// - Chunk 0: yields 0..10
    /// - Chunk 1: yields 10..11
    /// - Chunk 2: yields 20..25
    pub fn iter(&self) -> StateIndexIter<&Self> {
        StateIndexIter {
            index: self,
            current_chunk: 0,
            current_local: 0,
        }
    }

    /// Create a parallel iterator over all valid global indices with their flat indices
    ///
    /// This iterates through all chunks in parallel and yields tuples of (flat_index, global_index).
    /// The flat_index starts at 0 and increments for each item in iteration order.
    ///
    /// For example, with chunk_sizes [10, 1, 5] and max_page_len 10:
    /// - Chunk 0: yields (0, 0)..(9, 9)
    /// - Chunk 1: yields (10, 10)
    /// - Chunk 2: yields (11, 20)..(15, 24)
    pub fn par_iter(&self) -> impl ParallelIterator<Item = (usize, I)> + '_
    where
        I: Send + Sync,
    {
        let max_page_len = self.max_page_len as usize;
        let num_chunks = self.num_chunks();
        (0..num_chunks).into_par_iter().flat_map(move |chunk_idx| {
            let chunk_start = self.offsets[chunk_idx];
            let chunk_end = self.offsets[chunk_idx + 1];
            let chunk_size = chunk_end - chunk_start;
            let global_base = chunk_idx * max_page_len;
            (0..chunk_size).into_par_iter().map(move |local_offset| {
                let flat_idx = chunk_start + local_offset;
                let global_idx = I::from(global_base + local_offset);
                (flat_idx, global_idx)
            })
        })
    }

    pub fn arc_into_iter(self: Arc<Self>) -> StateIndexIter<Arc<Self>> {
        StateIndexIter {
            index: self,
            current_chunk: 0,
            current_local: 0,
        }
    }
}

impl<I: From<usize> + Into<usize>> StateIndex<I> {
    /// Create a parallel iterator over all valid global indices with their flat indices
    ///
    /// This iterates through all chunks in parallel and yields tuples of (flat_index, global_index).
    /// The flat_index starts at 0 and increments for each item in iteration order.
    ///
    /// For example, with chunk_sizes [10, 1, 5] and max_page_len 10:
    /// - Chunk 0: yields (0, 0)..(9, 9)
    /// - Chunk 1: yields (10, 10)
    /// - Chunk 2: yields (11, 20)..(15, 24)
    pub fn into_par_iter(self: Arc<Self>) -> impl ParallelIterator<Item = (usize, I)>
    where
        I: Send + Sync,
    {
        let max_page_len = self.max_page_len as usize;
        let num_chunks = self.num_chunks();
        (0..num_chunks).into_par_iter().flat_map(move |chunk_idx| {
            let chunk_start = self.offsets[chunk_idx];
            let chunk_end = self.offsets[chunk_idx + 1];
            let chunk_size = chunk_end - chunk_start;
            let global_base = chunk_idx * max_page_len;
            (0..chunk_size).into_par_iter().map(move |local_offset| {
                let flat_idx = chunk_start + local_offset;
                let global_idx = I::from(global_base + local_offset);
                (flat_idx, global_idx)
            })
        })
    }
}

/// Iterator over global indices in a StateIndex
#[derive(Debug, Clone)]
pub struct StateIndexIter<I> {
    index: I,
    current_chunk: usize,
    current_local: usize,
}

impl<V: From<usize> + Into<usize>, I: Deref<Target = StateIndex<V>>> Iterator
    for StateIndexIter<I>
{
    type Item = V;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current_chunk >= self.index.num_chunks() {
                return None;
            }

            let chunk_start = self.index.offsets[self.current_chunk];
            let chunk_end = self.index.offsets[self.current_chunk + 1];
            let chunk_size = chunk_end - chunk_start;

            if self.current_local < chunk_size {
                let global_idx =
                    self.current_chunk * self.index.max_page_len as usize + self.current_local;
                self.current_local += 1;
                return Some(V::from(global_idx));
            }

            // Move to next chunk
            self.current_chunk += 1;
            self.current_local = 0;
        }
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        // fast skip
        if self.current_chunk >= self.index.num_chunks() {
            return None;
        }
        let current = self.index.offsets[self.current_chunk] + self.current_local;
        let target = current.saturating_add(n);
        if &target >= self.index.offsets.last()? {
            return None;
        }
        // find the first offset > target, then substract 1 to get the last chunk starting at <= target
        let skip_chunks = self.index.offsets[self.current_chunk..]
            .partition_point(|&offset| offset <= target)
            .saturating_sub(1);
        self.current_chunk += skip_chunks;
        self.current_local = target - self.index.offsets[self.current_chunk];
        let global_idx = self.current_chunk * self.index.max_page_len as usize + self.current_local;
        self.current_local += 1;
        Some(V::from(global_idx))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let total = self.index.len();
        let consumed = if self.current_chunk < self.index.num_chunks() {
            self.index.offsets[self.current_chunk] + self.current_local
        } else {
            total
        };
        let remaining = total.saturating_sub(consumed);
        (remaining, Some(remaining))
    }
}

impl<V: From<usize> + Into<usize>, I: Deref<Target = StateIndex<V>>> ExactSizeIterator
    for StateIndexIter<I>
{
}

/// Address resolver for sharded storage with fixed-size chunks
///
/// This struct combines a StateIndex with a flat array to provide O(1) access
/// to elements in a sharded storage scheme with partially filled chunks.
#[derive(Debug)]
pub struct State<A, I = usize> {
    /// Index resolver
    index: StateIndex<I>,
    /// Flat array of state cells
    state: Box<[A]>,
}

impl<A: Default, I: From<usize> + Into<usize>> State<A, I> {
    /// Create a new State with the given chunk configuration
    ///
    /// # Arguments
    /// * `chunk_sizes` - The actual size of each chunk (can be <= max_page_len)
    /// * `max_page_len` - Maximum capacity of each chunk
    ///
    /// # Example
    /// ```
    /// use db4_storage::state::State;
    /// use std::sync::atomic::AtomicUsize;
    ///
    /// // 3 chunks with sizes 1000, 500, 1000 and max capacity 1000
    /// let state: State<AtomicUsize> = State::new(vec![1000, 500, 1000], 1000);
    /// ```
    pub fn new(chunk_sizes: Vec<usize>, max_page_len: u32) -> Self {
        let index = StateIndex::<I>::new(chunk_sizes, max_page_len);
        let total_size = index.len();

        // Initialize state array with default values
        let state: Box<[A]> = (0..total_size)
            .map(|_| A::default())
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self { index, state }
    }

    /// Get a reference to the StateIndex
    #[inline]
    pub fn index(&self) -> &StateIndex<I> {
        &self.index
    }

    /// Get a reference to the cell for the given global index
    ///
    /// # Arguments
    /// * `index` - Global index across all chunks
    ///
    /// # Returns
    /// Some(&A) if the index is valid, None otherwise
    #[inline(always)]
    pub fn get(&self, index: I) -> Option<&A> {
        let flat_index = self.index.resolve(index)?;
        self.state.get(flat_index)
    }

    /// Get a mutable reference to the cell for the given global index
    ///
    /// # Arguments
    /// * `index` - Global index across all chunks
    ///
    /// # Returns
    /// Some(&mut A) if the index is valid, None otherwise
    #[inline(always)]
    pub fn get_mut(&mut self, index: I) -> Option<&mut A> {
        let flat_index = self.index.resolve(index)?;
        self.state.get_mut(flat_index)
    }

    /// Get a reference to the cell for the given global index without bounds checking
    ///
    /// # Arguments
    /// * `index` - Global index across all chunks
    ///
    /// # Returns
    /// Reference to the corresponding cell
    ///
    /// # Safety
    /// Panics if the index is out of bounds
    #[inline(always)]
    pub fn get_unchecked(&self, index: I) -> &A {
        let flat_index = self.index.resolve_unchecked(index);
        &self.state[flat_index]
    }

    /// Get a mutable reference to the cell for the given global index without bounds checking
    ///
    /// # Arguments
    /// * `index` - Global index across all chunks
    ///
    /// # Returns
    /// Mutable reference to the corresponding cell
    ///
    /// # Safety
    /// Panics if the index is out of bounds
    #[inline(always)]
    pub fn get_mut_unchecked(&mut self, index: I) -> &mut A {
        let flat_index = self.index.resolve_unchecked(index);
        &mut self.state[flat_index]
    }

    /// Get the number of chunks
    #[inline]
    pub fn num_chunks(&self) -> usize {
        self.index.num_chunks()
    }

    /// Get the total number of state cells
    #[inline]
    pub fn len(&self) -> usize {
        self.state.len()
    }

    /// Check if the state is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.state.is_empty()
    }

    /// Get the maximum page length
    #[inline]
    pub fn max_page_len(&self) -> u32 {
        self.index.max_page_len()
    }

    /// Create an iterator over all elements in the state
    ///
    /// Yields references to each element in order of their global indices.
    pub fn iter(&self) -> StateIter<'_, A, I> {
        StateIter {
            state: self,
            inner: self.index.iter(),
        }
    }
}

/// Iterator over elements in a State
#[derive(Debug)]
pub struct StateIter<'a, A, I> {
    state: &'a State<A, I>,
    inner: StateIndexIter<&'a StateIndex<I>>,
}

impl<'a, A: Default, I: From<usize> + Into<usize>> Iterator for StateIter<'a, A, I> {
    type Item = &'a A;

    fn next(&mut self) -> Option<Self::Item> {
        let global_idx = self.inner.next()?;
        Some(self.state.get_unchecked(global_idx))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<'a, A: Default, I: From<usize> + Into<usize>> ExactSizeIterator for StateIter<'a, A, I> {
    fn len(&self) -> usize {
        self.inner.len()
    }
}

impl<A: Default, I: From<usize> + Into<usize> + std::fmt::Debug + Copy> Index<I> for State<A, I> {
    type Output = A;

    #[inline(always)]
    fn index(&self, index: I) -> &Self::Output {
        self.get(index)
            .unwrap_or_else(|| panic!("index out of bounds: {:?}", index))
    }
}

impl<A: Default, I: From<usize> + Into<usize> + std::fmt::Debug + Copy> IndexMut<I>
    for State<A, I>
{
    #[inline(always)]
    fn index_mut(&mut self, index: I) -> &mut Self::Output {
        self.get_mut(index)
            .unwrap_or_else(|| panic!("index out of bounds: {:?}", index))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn test_state_index_resolve() {
        let index: StateIndex<usize> = StateIndex::new(vec![1000, 500, 1000], 1000);

        assert_eq!(index.num_chunks(), 3);
        assert_eq!(index.len(), 2500);
        assert_eq!(index.max_page_len(), 1000);

        // Test chunk 0
        assert_eq!(index.resolve(0), Some(0));
        assert_eq!(index.resolve(999), Some(999));

        // Test chunk 1
        assert_eq!(index.resolve(1000), Some(1000));
        assert_eq!(index.resolve(1499), Some(1499));

        // Test chunk 2
        assert_eq!(index.resolve(2000), Some(1500));
        assert_eq!(index.resolve(2999), Some(2499));

        // Test out of bounds
        assert_eq!(index.resolve(3000), None);
        assert_eq!(index.resolve(1500), None); // In chunk 1 but beyond its actual size
    }

    #[test]
    fn test_basic_get() {
        let state: State<AtomicUsize> = State::new(vec![1000, 500, 1000], 1000);

        // Test chunk 0
        state.get_unchecked(0).store(42, Ordering::Relaxed);
        assert_eq!(state.get_unchecked(0).load(Ordering::Relaxed), 42);

        state.get_unchecked(999).store(123, Ordering::Relaxed);
        assert_eq!(state.get_unchecked(999).load(Ordering::Relaxed), 123);

        // Test chunk 1 (offset should be 1000)
        state.get_unchecked(1000).store(77, Ordering::Relaxed);
        assert_eq!(state.get_unchecked(1000).load(Ordering::Relaxed), 77);

        state.get_unchecked(1499).store(88, Ordering::Relaxed);
        assert_eq!(state.get_unchecked(1499).load(Ordering::Relaxed), 88);

        // Test chunk 2 (offset should be 1500)
        state.get_unchecked(2000).store(99, Ordering::Relaxed);
        assert_eq!(state.get_unchecked(2000).load(Ordering::Relaxed), 99);

        state.get_unchecked(2999).store(111, Ordering::Relaxed);
        assert_eq!(state.get_unchecked(2999).load(Ordering::Relaxed), 111);
    }

    #[test]
    fn test_get_option() {
        let state: State<AtomicUsize> = State::new(vec![100, 50], 100);

        assert!(state.get(0).is_some());
        assert!(state.get(99).is_some());
        assert!(state.get(100).is_some());
        assert!(state.get(149).is_some());

        // Out of bounds chunk
        assert!(state.get(200).is_none());
        assert!(state.get(1000).is_none());

        // In bounds chunk but beyond chunk's actual size
        assert!(state.get(150).is_none());
    }

    #[test]
    #[should_panic]
    fn test_out_of_bounds_chunk() {
        let state: State<AtomicUsize> = State::new(vec![100], 100);
        state.get_unchecked(200); // Should panic
    }

    #[test]
    fn test_partially_filled_chunks() {
        // Simulate real scenario: chunks with varying fill levels
        let state: State<AtomicUsize> = State::new(vec![1000, 300, 1000, 50], 1000);

        // First chunk - fully filled
        state.get_unchecked(0).store(1, Ordering::Relaxed);
        state.get_unchecked(999).store(2, Ordering::Relaxed);
        assert_eq!(state.get_unchecked(0).load(Ordering::Relaxed), 1);
        assert_eq!(state.get_unchecked(999).load(Ordering::Relaxed), 2);

        // Second chunk - partially filled (300 items)
        // Global indices: 1000-1299
        state.get_unchecked(1000).store(3, Ordering::Relaxed);
        state.get_unchecked(1299).store(4, Ordering::Relaxed);
        assert_eq!(state.get_unchecked(1000).load(Ordering::Relaxed), 3);
        assert_eq!(state.get_unchecked(1299).load(Ordering::Relaxed), 4);

        // Third chunk - fully filled
        // Global indices: 2000-2999
        state.get_unchecked(2000).store(5, Ordering::Relaxed);
        state.get_unchecked(2999).store(6, Ordering::Relaxed);
        assert_eq!(state.get_unchecked(2000).load(Ordering::Relaxed), 5);
        assert_eq!(state.get_unchecked(2999).load(Ordering::Relaxed), 6);

        // Fourth chunk - minimally filled (50 items)
        // Global indices: 3000-3049
        state.get_unchecked(3000).store(7, Ordering::Relaxed);
        state.get_unchecked(3049).store(8, Ordering::Relaxed);
        assert_eq!(state.get_unchecked(3000).load(Ordering::Relaxed), 7);
        assert_eq!(state.get_unchecked(3049).load(Ordering::Relaxed), 8);

        assert_eq!(state.len(), 2350); // 1000 + 300 + 1000 + 50
        assert_eq!(state.num_chunks(), 4);
    }

    #[test]
    fn test_resolve_pos_consistency() {
        // Test that our addressing matches the resolve_pos function
        let max_page_len = 1000u32;
        let state: State<AtomicUsize> = State::new(vec![1000, 500, 1000], max_page_len);

        // Helper to simulate resolve_pos
        let resolve_pos = |i: usize| -> (usize, u32) {
            let chunk = i / max_page_len as usize;
            let pos = (i % max_page_len as usize) as u32;
            (chunk, pos)
        };

        for index in [0, 500, 999, 1000, 1250, 1499, 2000, 2500, 2999] {
            let (chunk, local_pos) = resolve_pos(index);

            // Verify our addressing scheme matches
            let computed_chunk = index / max_page_len as usize;
            let computed_local = index % max_page_len as usize;

            assert_eq!(chunk, computed_chunk);
            assert_eq!(local_pos, computed_local as u32);

            // Verify we can access the cell
            state.get_unchecked(index).store(index, Ordering::Relaxed);
            assert_eq!(state.get_unchecked(index).load(Ordering::Relaxed), index);
        }
    }

    #[test]
    fn test_generic_over_different_types() {
        // Test with usize
        let state_usize: State<usize> = State::new(vec![10, 5], 10);
        assert_eq!(*state_usize.get_unchecked(0), 0);
        assert_eq!(*state_usize.get_unchecked(10), 0);

        // Test with Option
        let state_option: State<Option<i32>> = State::new(vec![10, 5], 10);
        assert_eq!(*state_option.get_unchecked(0), None);
        assert_eq!(*state_option.get_unchecked(10), None);

        // Test with AtomicUsize
        let state_atomic: State<AtomicUsize> = State::new(vec![10, 5], 10);
        state_atomic.get_unchecked(0).store(42, Ordering::Relaxed);
        assert_eq!(state_atomic.get_unchecked(0).load(Ordering::Relaxed), 42);
    }

    #[test]
    fn test_mutable_access() {
        let mut state: State<usize> = State::new(vec![100, 50], 100);

        // Test get_mut
        *state.get_mut(0).unwrap() = 42;
        assert_eq!(*state.get(0).unwrap(), 42);

        *state.get_mut(50).unwrap() = 99;
        assert_eq!(*state.get(50).unwrap(), 99);

        // Test get_mut in second chunk
        *state.get_mut(100).unwrap() = 123;
        assert_eq!(*state.get(100).unwrap(), 123);

        // Test get_mut_unchecked
        *state.get_mut_unchecked(10) = 77;
        assert_eq!(*state.get_unchecked(10), 77);

        // Test out of bounds returns None
        assert!(state.get_mut(200).is_none());
    }

    #[test]
    fn test_index_trait() {
        let mut state: State<usize> = State::new(vec![100, 50], 100);

        // Test Index trait
        state[0] = 42;
        assert_eq!(state[0], 42);

        state[99] = 100;
        assert_eq!(state[99], 100);

        // Test in second chunk
        state[100] = 200;
        assert_eq!(state[100], 200);

        state[149] = 300;
        assert_eq!(state[149], 300);
    }

    #[test]
    #[should_panic(expected = "index out of bounds")]
    fn test_index_out_of_bounds() {
        let state: State<usize> = State::new(vec![100], 100);
        let _ = state[200];
    }

    #[test]
    fn test_offsets_include_final_cumulative() {
        let state: State<usize> = State::new(vec![1000, 500, 1000], 1000);

        // offsets should be [0, 1000, 1500, 2500]
        assert_eq!(state.num_chunks(), 3);
        assert_eq!(state.len(), 2500);

        // Verify via StateIndex API
        assert_eq!(state.index().len(), state.len());
    }

    #[test]
    fn test_state_index_can_be_used_independently() {
        // StateIndex can be used independently of State
        let index: StateIndex<usize> = StateIndex::new(vec![1000, 500, 1000], 1000);

        // Create your own array
        let mut data = vec![0usize; index.len()];

        // Use the index to access elements
        if let Some(flat_idx) = index.resolve(1200) {
            data[flat_idx] = 42;
        }

        if let Some(flat_idx) = index.resolve(1200) {
            assert_eq!(data[flat_idx], 42);
        }
    }

    #[test]
    fn test_state_index_iter() {
        let index: StateIndex<usize> = StateIndex::new(vec![10, 1, 5], 10);

        let global_indices: Vec<usize> = index.iter().collect();

        // Chunk 0: global indices 0-9 (10 items)
        // Chunk 1: global index 10 (1 item)
        // Chunk 2: global indices 20-24 (5 items)
        let expected = vec![
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9,  // Chunk 0
            10, // Chunk 1
            20, 21, 22, 23, 24, // Chunk 2
        ];

        assert_eq!(global_indices, expected);
        assert_eq!(index.iter().len(), 16);
    }

    #[test]
    fn test_state_index_par_iter() {
        let index: StateIndex<usize> = StateIndex::new(vec![10, 1, 5], 10);

        let mut results: Vec<(usize, usize)> = index.par_iter().collect();
        results.sort_by_key(|(flat_idx, _)| *flat_idx); // Sort by flat index

        // Expected: (flat_idx, global_idx) tuples
        // Chunk 0: flat indices 0-9, global indices 0-9
        // Chunk 1: flat index 10, global index 10
        // Chunk 2: flat indices 11-15, global indices 20-24
        let expected = vec![
            (0, 0),
            (1, 1),
            (2, 2),
            (3, 3),
            (4, 4),
            (5, 5),
            (6, 6),
            (7, 7),
            (8, 8),
            (9, 9),   // Chunk 0
            (10, 10), // Chunk 1
            (11, 20),
            (12, 21),
            (13, 22),
            (14, 23),
            (15, 24), // Chunk 2
        ];

        assert_eq!(results, expected);

        // Verify count matches
        assert_eq!(index.par_iter().count(), 16);

        // Verify flat indices are sequential
        let flat_indices: Vec<usize> = results.iter().map(|(flat_idx, _)| *flat_idx).collect();
        assert_eq!(flat_indices, (0..16).collect::<Vec<_>>());
    }

    #[test]
    fn test_state_iter() {
        let mut state: State<usize> = State::new(vec![10, 1, 5], 10);

        // Collect global indices first to avoid borrow checker issues
        let global_indices: Vec<usize> = state.index().iter().collect();

        // Initialize state with global indices
        for global_idx in global_indices {
            state[global_idx] = global_idx * 10;
        }

        // Collect values via iter
        let values: Vec<usize> = state.iter().copied().collect();

        let expected = vec![
            0, 10, 20, 30, 40, 50, 60, 70, 80, 90,  // Chunk 0
            100, // Chunk 1
            200, 210, 220, 230, 240, // Chunk 2
        ];

        assert_eq!(values, expected);
        assert_eq!(state.iter().len(), 16);
    }

    #[test]
    fn test_state_iter_with_atomics() {
        let state: State<AtomicUsize> = State::new(vec![10, 5], 10);

        // Collect global indices first to avoid borrow checker issues
        let global_indices: Vec<usize> = state.index().iter().collect();

        // Set values via global indices
        for global_idx in global_indices {
            state
                .get_unchecked(global_idx)
                .store(global_idx, Ordering::Relaxed);
        }

        // Read via iterator
        let values: Vec<usize> = state.iter().map(|a| a.load(Ordering::Relaxed)).collect();

        let expected = vec![
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, // Chunk 0
            10, 11, 12, 13, 14, // Chunk 1
        ];

        assert_eq!(values, expected);
    }

    #[test]
    fn test_iter_skip() {
        let index: StateIndex<usize> = StateIndex::new(vec![10, 1, 5], 10);

        let expected = (0..10).chain(10..11).chain(20..25);
        // check all skips
        for (i, v) in expected.enumerate() {
            assert_eq!(index.iter().nth(i), Some(v));
        }

        assert_eq!(index.iter().nth(100), None);
        assert_eq!(index.iter().nth(16), None);
    }
}
