use crate::LocalPOS;
use std::collections::BTreeMap;

/// Enables fast node lookups by node type.
#[derive(Debug, Default)]
pub struct MemNodeTypeIndex {
    /// Maps a node type id to the positions of nodes with that type.
    map: BTreeMap<usize, Vec<LocalPOS>>,
}

impl MemNodeTypeIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Associates the node at `pos` with `type_id`.
    pub fn insert(&mut self, type_id: usize, pos: LocalPOS) {
        self.map.entry(type_id).or_default().push(pos);
    }

    /// Returns the positions of nodes with the given type id, in insertion order.
    pub fn get(&self, type_id: usize) -> &[LocalPOS] {
        self.map.get(&type_id).map(Vec::as_slice).unwrap_or(&[])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_returns_insertion_order() {
        let mut index = MemNodeTypeIndex::new();

        index.insert(1, LocalPOS(4));
        index.insert(1, LocalPOS(1));
        index.insert(1, LocalPOS(2));

        assert_eq!(index.get(1), &[LocalPOS(4), LocalPOS(1), LocalPOS(2)]);
    }

    #[test]
    fn get_missing_type_is_empty() {
        let index = MemNodeTypeIndex::new();

        assert!(index.get(3).is_empty());
    }
}
