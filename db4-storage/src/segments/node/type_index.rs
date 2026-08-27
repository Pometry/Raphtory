use crate::LocalPOS;
use std::collections::{BTreeMap, BTreeSet};

/// Enables fast node lookups by node type.
#[derive(Debug, Default)]
pub struct MemNodeTypeIndex {
    /// Maps a node type id to the positions of nodes with that type.
    map: BTreeMap<usize, BTreeSet<LocalPOS>>,
}

impl MemNodeTypeIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Associates the node at `pos` with `type_id`.
    pub fn insert(&mut self, type_id: usize, pos: LocalPOS) {
        self.map.entry(type_id).or_default().insert(pos);
    }

    /// Returns the positions of nodes with the given type id, in ascending order.
    pub fn get(&self, type_id: usize) -> impl Iterator<Item = LocalPOS> + '_ {
        self.map.get(&type_id).into_iter().flatten().copied()
    }

    /// Largest type id present, or `None` if empty.
    pub fn max_type_id(&self) -> Option<usize> {
        self.map.last_key_value().map(|(k, _)| *k)
    }

    /// Total number of `(type_id, position)` pairs.
    pub fn num_entries(&self) -> usize {
        self.map.values().map(|s| s.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Iterates `(type_id, positions)` in ascending type-id order.
    pub fn iter(&self) -> impl Iterator<Item = (usize, impl Iterator<Item = LocalPOS> + '_)> + '_ {
        self.map
            .iter()
            .map(|(type_id, positions)| (*type_id, positions.iter().copied()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_returns_sorted_unique_positions() {
        let mut index = MemNodeTypeIndex::new();

        index.insert(1, LocalPOS(4));
        index.insert(1, LocalPOS(1));
        index.insert(1, LocalPOS(2));
        index.insert(1, LocalPOS(1));

        assert!(index.get(1).eq([LocalPOS(1), LocalPOS(2), LocalPOS(4)]));
    }

    #[test]
    fn get_missing_type_is_empty() {
        let index = MemNodeTypeIndex::new();

        assert_eq!(index.get(3).count(), 0);
    }

    #[test]
    fn max_type_id_tracks_largest_key() {
        let mut index = MemNodeTypeIndex::new();
        assert_eq!(index.max_type_id(), None);

        index.insert(0, LocalPOS(1));
        assert_eq!(index.max_type_id(), Some(0));

        index.insert(5, LocalPOS(2));
        index.insert(2, LocalPOS(3));
        assert_eq!(index.max_type_id(), Some(5));
    }

    #[test]
    fn num_entries_counts_unique_pairs() {
        let mut index = MemNodeTypeIndex::new();
        assert_eq!(index.num_entries(), 0);

        index.insert(1, LocalPOS(2));
        index.insert(1, LocalPOS(1));
        index.insert(1, LocalPOS(1)); // duplicate
        index.insert(3, LocalPOS(0));

        assert_eq!(index.num_entries(), 3);
    }

    #[test]
    fn iter_returns_types_in_order_with_sorted_positions() {
        let mut index = MemNodeTypeIndex::new();
        index.insert(2, LocalPOS(5));
        index.insert(0, LocalPOS(3));
        index.insert(2, LocalPOS(1));
        index.insert(0, LocalPOS(1));

        let collected: Vec<(usize, Vec<LocalPOS>)> = index
            .iter()
            .map(|(type_id, positions)| (type_id, positions.collect()))
            .collect();

        assert_eq!(
            collected,
            vec![
                (0, vec![LocalPOS(1), LocalPOS(3)]),
                (2, vec![LocalPOS(1), LocalPOS(5)]),
            ]
        );
    }
}
