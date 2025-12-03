use std::ops::{Index, IndexMut};

/// Address resolver for sharded storage with fixed-size chunks
///
/// Given a sharding scheme where items are distributed across chunks:
/// - chunk_id = index / max_page_len
/// - local_pos = index % max_page_len
///
/// This struct provides O(1) lookup to map any index to a cell in a flat array,
/// accounting for partially filled chunks.
///
/// # Example
/// With max_page_len = 1000:
/// - Chunk 0: 1000 items (offsets[0] = 0, offsets[1] = 1000)
/// - Chunk 1: 500 items  (offsets[1] = 1000, offsets[2] = 1500)
/// - Chunk 2: 1000 items (offsets[2] = 1500, offsets[3] = 2500)
/// - state: Array of length 2500
///
/// To access index 1200:
/// - chunk = 1200 / 1000 = 1
/// - local_pos = 1200 % 1000 = 200
/// - cell_index = offsets[1] + 200 = 1000 + 200 = 1200
#[derive(Debug)]
pub struct State<A> {
    /// Cumulative offsets: offsets[chunk_id] = starting position in `state` for that chunk
    /// Length is equal to number of chunks + 1 (includes final cumulative value)
    offsets: Box<[usize]>,
    /// Flat array of state cells
    state: Box<[A]>,
    /// Maximum items per chunk
    max_page_len: u32,
}

impl<A: Default> State<A> {
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
        let num_chunks = chunk_sizes.len();
        let total_size: usize = chunk_sizes.iter().sum();

        // Build cumulative offsets (includes final cumulative value)
        let mut offsets = Vec::with_capacity(num_chunks + 1);
        let mut cumulative = 0;
        for size in chunk_sizes {
            offsets.push(cumulative);
            cumulative += size;
        }
        offsets.push(cumulative); // Add final cumulative value

        // Initialize state array with default values
        let state: Box<[A]> = (0..total_size)
            .map(|_| A::default())
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            offsets: offsets.into_boxed_slice(),
            state,
            max_page_len,
        }
    }

    /// Get a reference to the cell for the given global index
    ///
    /// # Arguments
    /// * `index` - Global index across all chunks
    ///
    /// # Returns
    /// Some(&A) if the index is valid, None otherwise
    #[inline(always)]
    pub fn get(&self, index: usize) -> Option<&A> {
        let chunk = index / self.max_page_len as usize;
        let local_pos = index % self.max_page_len as usize;

        let offset = *self.offsets.get(chunk)?;
        let cell_index = offset + local_pos;

        self.state.get(cell_index)
    }

    /// Get a mutable reference to the cell for the given global index
    ///
    /// # Arguments
    /// * `index` - Global index across all chunks
    ///
    /// # Returns
    /// Some(&mut A) if the index is valid, None otherwise
    #[inline(always)]
    pub fn get_mut(&mut self, index: usize) -> Option<&mut A> {
        let chunk = index / self.max_page_len as usize;
        let local_pos = index % self.max_page_len as usize;

        let offset = *self.offsets.get(chunk)?;
        let cell_index = offset + local_pos;

        self.state.get_mut(cell_index)
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
    pub fn get_unchecked(&self, index: usize) -> &A {
        let chunk = index / self.max_page_len as usize;
        let local_pos = index % self.max_page_len as usize;

        let offset = self.offsets[chunk];
        let cell_index = offset + local_pos;

        &self.state[cell_index]
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
    pub fn get_mut_unchecked(&mut self, index: usize) -> &mut A {
        let chunk = index / self.max_page_len as usize;
        let local_pos = index % self.max_page_len as usize;

        let offset = self.offsets[chunk];
        let cell_index = offset + local_pos;

        &mut self.state[cell_index]
    }

    /// Get the number of chunks
    #[inline]
    pub fn num_chunks(&self) -> usize {
        self.offsets.len().saturating_sub(1)
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
        self.max_page_len
    }
}

impl<A: Default> Index<usize> for State<A> {
    type Output = A;

    #[inline(always)]
    fn index(&self, index: usize) -> &Self::Output {
        self.get(index)
            .unwrap_or_else(|| panic!("index out of bounds: {}", index))
    }
}

impl<A: Default> IndexMut<usize> for State<A> {
    #[inline(always)]
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        self.get_mut(index)
            .unwrap_or_else(|| panic!("index out of bounds: {}", index))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

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

        // Verify the final offset equals total length
        assert_eq!(state.offsets[state.num_chunks()], state.len());
    }
}
