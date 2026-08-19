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
}
