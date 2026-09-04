use dashmap::mapref::multiple::RefMulti;
use itertools::Itertools;
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

    /// Returns the sorted VIDs for `type_ids`.
    pub fn nodes_of_type(&self, type_ids: &[usize]) -> Vec<VID> {
        if type_ids.is_empty() {
            return vec![];
        }

        // Fast path for single type that avoids kmerge.
        if let [type_id] = type_ids {
            return self
                .map
                .get(type_id)
                .map(|set| set.iter().copied().collect())
                .unwrap_or_default();
        }

        let sets: Vec<_> = type_ids
            .iter()
            .filter_map(|type_id| self.map.get(type_id))
            .collect();

        // No need to dedup after kmerge since a node can only have one type.
        sets.iter()
            .map(|set| set.iter().copied())
            .kmerge()
            .collect()
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
        std::mem::replace(self, Self::new())
    }

    /// Returns `(type_id, vids)` in ascending `type_id` order with ascending `VID`s per type.
    // TODO: We are exposing dashmap here, return a better type for this.
    pub fn sorted_entries(&self) -> Vec<RefMulti<'_, usize, BTreeSet<VID>>> {
        let mut entries: Vec<_> = self.map.iter().collect();
        entries.sort_unstable_by_key(|entry| entry.key().clone());

        entries
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

        assert_eq!(index.nodes_of_type(&[1]), vec![VID(1), VID(2), VID(4)]);
    }

    #[test]
    fn get_missing_type_is_empty() {
        let index = MemNodeTypeIndex::new();
        assert!(index.nodes_of_type(&[3]).is_empty());
    }

    #[test]
    fn get_merges_sorted_vids() {
        let index = MemNodeTypeIndex::new();

        index.insert(1, VID(4));
        index.insert(1, VID(1));
        index.insert(2, VID(2));
        index.insert(2, VID(1));
        index.insert(3, VID(5));

        assert_eq!(
            index.nodes_of_type(&[1, 2]),
            vec![VID(1), VID(1), VID(2), VID(4)]
        );
        assert_eq!(index.nodes_of_type(&[3]), vec![VID(5)]);
        assert!(index.nodes_of_type(&[9]).is_empty());
        assert!(index.nodes_of_type(&[]).is_empty());
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
    fn sorted_entries_returns_types_in_order_with_sorted_vids() {
        let index = MemNodeTypeIndex::new();
        index.insert(2, VID(5));
        index.insert(0, VID(3));
        index.insert(2, VID(1));
        index.insert(0, VID(1));

        let entries = index.sorted_entries();
        let collected = entries
            .iter()
            .map(|entry| {
                (
                    *entry.key(),
                    entry.value().iter().copied().collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();

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
        assert_eq!(taken.nodes_of_type(&[1]), vec![VID(1), VID(3)]);
        assert_eq!(taken.nodes_of_type(&[2]), vec![VID(2)]);
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
        assert_eq!(index.nodes_of_type(&[1]).len(), 100);
    }
}
