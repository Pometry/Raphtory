use crate::LocalPOS;
use std::collections::BTreeMap;

/// Enables fast node lookups by node type.
#[derive(Debug, Default)]
pub struct NodeTypeIndex {
    /// Maps a node type id to the positions of nodes with that type.
    /// The positions list is maintained in ascending order.
    map: BTreeMap<usize, Vec<LocalPOS>>,
}

impl NodeTypeIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Records that `type_id` includes the node at `pos`.
    pub fn insert(&mut self, type_id: usize, pos: LocalPOS) {
        let list = self.map.entry(type_id).or_default();

        match list.last() {
            Some(last) if pos > *last => {
                // Directly append pos since it is greater than all elements.
                list.push(pos);
            }
            Some(_) => {
                // Insert pos at the right index so that the list remains sorted.
                let insert_idx = list.partition_point(|p| *p < pos);

                if list.get(insert_idx) != Some(&pos) {
                    list.insert(insert_idx, pos);
                }
            }
            None => list.push(pos),
        }
    }

    /// Returns the positions of nodes with the given type id.
    /// The returned slice is sorted in ascending order.
    pub fn get(&self, type_id: usize) -> &[LocalPOS] {
        self.map.get(&type_id).map(Vec::as_slice).unwrap_or(&[])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_keeps_list_sorted() {
        let mut index = NodeTypeIndex::new();

        index.insert(1, LocalPOS(4));
        index.insert(1, LocalPOS(1));
        index.insert(1, LocalPOS(2));

        assert_eq!(index.get(1), &[LocalPOS(1), LocalPOS(2), LocalPOS(4)]);
    }

    #[test]
    fn insert_ignores_duplicates() {
        let mut index = NodeTypeIndex::new();

        index.insert(1, LocalPOS(2));
        index.insert(1, LocalPOS(2));
        index.insert(1, LocalPOS(5));
        index.insert(1, LocalPOS(2));

        assert_eq!(index.get(1), &[LocalPOS(2), LocalPOS(5)]);
    }

    #[test]
    fn get_missing_type_is_empty() {
        let index = NodeTypeIndex::new();

        assert!(index.get(3).is_empty());
    }
}
