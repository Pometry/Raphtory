use raphtory_api::core::storage::FxDashMap;
use raphtory_core::entities::VID;
use std::{
    collections::BTreeSet,
    sync::atomic::{AtomicUsize, Ordering},
};

/// In-memory index that maps a node type id to `VID`s.
#[derive(Debug)]
pub struct MemNodeTypeIndex {
    map: FxDashMap<usize, BTreeSet<VID>>,

    /// Number of `(type_id, VID)` pairs in the index.
    /// Approximate, since this is not updated atomically with `map`.
    entry_count: AtomicUsize,

    /// Estimated memory size of the index in bytes.
    est_size: AtomicUsize,
}

impl Default for MemNodeTypeIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl MemNodeTypeIndex {
    pub fn new() -> Self {
        Self {
            map: FxDashMap::default(),
            entry_count: AtomicUsize::new(0),
            est_size: AtomicUsize::new(0),
        }
    }

    /// Associates `vid` with `type_id`. Idempotent for duplicate pairs.
    pub fn insert(&self, type_id: usize, vid: VID) {
        let mut entry = self.map.entry(type_id).or_default();

        if entry.insert(vid) {
            self.entry_count.fetch_add(1, Ordering::Relaxed);

            // TODO: Refine est_size calculation.
            let entry_size = std::mem::size_of::<usize>() + std::mem::size_of::<VID>();
            self.est_size.fetch_add(entry_size, Ordering::Relaxed);
        }
    }

    /// Returns the VIDs for `type_id` in ascending order.
    pub fn get(&self, type_id: usize) -> Vec<VID> {
        self.map
            .get(&type_id)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default()
    }

    pub fn max_type_id(&self) -> Option<usize> {
        self.map.iter().map(|entry| *entry.key()).max()
    }

    pub fn num_entries(&self) -> usize {
        self.entry_count.load(Ordering::Relaxed)
    }

    pub fn est_size(&self) -> usize {
        self.est_size.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.num_entries() == 0
    }

    /// Drains `self` into a new instance, leaving this one empty.
    pub fn take(&mut self) -> Self {
        let taken = Self::new();

        for mut entry in self.map.iter_mut() {
            let type_id = *entry.key();
            let vids: BTreeSet<VID> = std::mem::take(entry.value_mut());

            if !vids.is_empty() {
                taken.map.insert(type_id, vids);
            }
        }

        let taken_count = self.entry_count.load(Ordering::Relaxed);
        let taken_est_size = self.est_size.load(Ordering::Relaxed);

        self.entry_count.store(0, Ordering::Relaxed);
        self.est_size.store(0, Ordering::Relaxed);

        taken.entry_count.store(taken_count, Ordering::Relaxed);
        taken.est_size.store(taken_est_size, Ordering::Relaxed);

        taken
    }

    /// Yields `(type_id, vids)` in ascending `type_id` order with ascending `VID`s per type.
    pub fn iter_sorted(&self) -> Vec<(usize, Vec<VID>)> {
        let mut type_ids: Vec<usize> = self.map.iter().map(|entry| *entry.key()).collect();
        type_ids.sort_unstable();

        type_ids
            .into_iter()
            .filter_map(|type_id| {
                self.map
                    .get(&type_id)
                    .map(|set| (type_id, set.iter().copied().collect()))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::Arc, thread};

    #[test]
    fn get_returns_sorted_unique_vids() {
        let index = MemNodeTypeIndex::new();

        index.insert(1, VID(4));
        index.insert(1, VID(1));
        index.insert(1, VID(2));
        index.insert(1, VID(1));

        assert_eq!(index.get(1), vec![VID(1), VID(2), VID(4)]);
    }

    #[test]
    fn get_missing_type_is_empty() {
        let index = MemNodeTypeIndex::new();
        assert!(index.get(3).is_empty());
    }

    #[test]
    fn max_type_id_returns_largest_type_id() {
        let index = MemNodeTypeIndex::new();
        assert_eq!(index.max_type_id(), None);

        index.insert(0, VID(1));
        assert_eq!(index.max_type_id(), Some(0));

        index.insert(5, VID(2));
        index.insert(2, VID(3));
        assert_eq!(index.max_type_id(), Some(5));
    }

    #[test]
    fn num_entries_counts_unique_pairs() {
        let index = MemNodeTypeIndex::new();
        assert_eq!(index.num_entries(), 0);

        index.insert(1, VID(2));
        index.insert(1, VID(1));
        index.insert(1, VID(1));
        index.insert(3, VID(0));

        assert_eq!(index.num_entries(), 3);
    }

    #[test]
    fn iter_sorted_returns_types_in_order_with_sorted_vids() {
        let index = MemNodeTypeIndex::new();
        index.insert(2, VID(5));
        index.insert(0, VID(3));
        index.insert(2, VID(1));
        index.insert(0, VID(1));

        let collected: Vec<(usize, Vec<VID>)> = index.iter_sorted();

        assert_eq!(
            collected,
            vec![(0, vec![VID(1), VID(3)]), (2, vec![VID(1), VID(5)]),]
        );
    }

    #[test]
    fn take_drains_index() {
        let mut index = MemNodeTypeIndex::new();
        index.insert(1, VID(1));
        index.insert(1, VID(3));
        index.insert(2, VID(2));

        let taken = index.take();
        assert!(index.is_empty());
        assert_eq!(taken.get(1), vec![VID(1), VID(3)]);
        assert_eq!(taken.get(2), vec![VID(2)]);
    }

    #[test]
    fn concurrent_same_type_inserts() {
        let index = Arc::new(MemNodeTypeIndex::new());

        thread::scope(|scope| {
            for i in 0..100usize {
                let index = Arc::clone(&index);
                scope.spawn(move || {
                    index.insert(1, VID(i));
                });
            }
        });

        assert_eq!(index.num_entries(), 100);
        assert_eq!(index.get(1).len(), 100);
    }
}
