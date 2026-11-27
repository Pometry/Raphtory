use either::Either;
use raphtory_api::core::storage::sorted_vec_map::SVM;
use raphtory_core::storage::timeindex::EventTime;
use std::{collections::BTreeMap, ops::RangeInclusive};

/// Online writes flip a [`ReverseTIndex`] past this many entries to a `BTreeMap`, so an
/// out-of-order insert stops costing a shift of the whole vector.
const REVERSE_TINDEX_BTREE_CUTOFF: usize = 128;

pub type Entry = (EventTime, u32);

/// Reverse time index: the rows updated at each time, ordered by `(time, row)`, each with a
/// value `V` (what the update was, where the row alone does not say). One time can map to
/// several rows; a repeated `(time, row)` keeps the last value written, as the row's own
/// cell does.
///
/// Bulk loads append to the vector without ordering it ([`Self::push_unsorted`]) and order it
/// once ([`Self::sort_dedup`]), so it stays a vector however large it grows. Online writes keep
/// it ordered ([`Self::insert`]) and flip it to a `BTreeMap` once it is past the cutoff.
#[derive(Debug)]
pub enum ReverseTIndex<V = ()> {
    Svm(SVM<Entry, V>),
    BTree(BTreeMap<Entry, V>),
}

impl<V> Default for ReverseTIndex<V> {
    fn default() -> Self {
        Self::Svm(SVM::new())
    }
}

impl<V> ReverseTIndex<V> {
    /// Online insert: keeps the index ordered.
    pub fn insert(&mut self, t: EventTime, row: u32, value: V) {
        match self {
            Self::Svm(svm) if svm.len() < REVERSE_TINDEX_BTREE_CUTOFF => {
                svm.insert((t, row), value);
            }
            Self::Svm(svm) => {
                let mut map: BTreeMap<Entry, V> = std::mem::take(svm).into_iter().collect();
                map.insert((t, row), value);
                *self = Self::BTree(map);
            }
            Self::BTree(map) => {
                map.insert((t, row), value);
            }
        }
    }

    /// Bulk-load insert: appends without ordering; [`Self::sort_dedup`] must run before the
    /// index is read.
    #[inline]
    pub fn push_unsorted(&mut self, t: EventTime, row: u32, value: V) {
        match self {
            Self::Svm(svm) => svm.push_unsorted((t, row), value),
            Self::BTree(map) => {
                map.insert((t, row), value);
            }
        }
    }

    /// Orders an index filled with [`Self::push_unsorted`] and drops repeated entries,
    /// keeping the last value pushed for each.
    pub fn sort_dedup(&mut self) {
        if let Self::Svm(svm) = self {
            svm.sort_dedup();
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Svm(svm) => svm.len(),
            Self::BTree(map) => map.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The `(time, row)` entries within `range` with their values, in order.
    pub fn range(&self, range: RangeInclusive<Entry>) -> impl Iterator<Item = (&Entry, &V)> {
        if range.start() > range.end() {
            return Either::Left(std::iter::empty());
        }
        Either::Right(match self {
            Self::Svm(svm) => {
                let entries = svm.as_slice();
                let start = entries.partition_point(|(e, _)| e < range.start());
                let end = *range.end();
                Either::Left(
                    entries[start..]
                        .iter()
                        .map(|(e, v)| (e, v))
                        .take_while(move |(e, _)| **e <= end),
                )
            }
            Self::BTree(map) => Either::Right(map.range(range)),
        })
    }

    /// Every entry with its value, in order.
    pub fn iter(&self) -> impl Iterator<Item = (&Entry, &V)> {
        self.range((EventTime::MIN, 0)..=(EventTime::MAX, u32::MAX))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(t: i64) -> EventTime {
        EventTime(t, 0)
    }

    fn all<V: Copy>(index: &ReverseTIndex<V>) -> Vec<(Entry, V)> {
        index.iter().map(|(e, v)| (*e, *v)).collect()
    }

    #[test]
    fn keeps_several_rows_at_one_time() {
        let mut index = ReverseTIndex::default();
        index.insert(t(5), 2, 'a');
        index.insert(t(5), 1, 'b');
        index.insert(t(5), 2, 'c');
        assert_eq!(all(&index), vec![((t(5), 1), 'b'), ((t(5), 2), 'c')]);
    }

    #[test]
    fn bulk_appends_match_online_inserts_and_stay_a_vector() {
        // repeats of an entry carry different values: the last one written stays
        let entries: Vec<(Entry, u32)> = (0..1000u32)
            .rev()
            .map(|i| ((t((i % 300) as i64), i % 7), i))
            .collect();
        let mut online = ReverseTIndex::default();
        let mut bulk = ReverseTIndex::default();
        for &((time, row), value) in &entries {
            online.insert(time, row, value);
            bulk.push_unsorted(time, row, value);
        }
        bulk.sort_dedup();
        assert!(matches!(online, ReverseTIndex::BTree(_)));
        assert!(matches!(bulk, ReverseTIndex::Svm(_)));
        assert_eq!(all(&bulk), all(&online));
        assert_eq!(bulk.len(), online.len());
    }

    #[test]
    fn online_insert_past_cutoff_flips_a_sorted_bulk_vector() {
        let mut index = ReverseTIndex::default();
        for i in (0..200u32).rev() {
            index.push_unsorted(t(i as i64), i, ());
        }
        index.sort_dedup();
        index.insert(t(50), 999, ());
        assert!(matches!(index, ReverseTIndex::BTree(_)));
        assert_eq!(index.len(), 201);
        let at_50: Vec<_> = index
            .range((t(50), 0)..=(t(50), u32::MAX))
            .map(|(e, _)| *e)
            .collect();
        assert_eq!(at_50, vec![(t(50), 50), (t(50), 999)]);
    }

    #[test]
    fn range_is_inclusive_in_both_variants() {
        let mut svm = ReverseTIndex::default();
        let mut btree = ReverseTIndex::BTree(BTreeMap::new());
        for i in 0..10u32 {
            svm.insert(t(i as i64), 0, ());
            btree.insert(t(i as i64), 0, ());
        }
        for index in [&svm, &btree] {
            let got: Vec<_> = index
                .range((t(2), 0)..=(t(4), u32::MAX))
                .map(|(e, _)| e.0.0)
                .collect();
            assert_eq!(got, vec![2, 3, 4]);
            assert_eq!(index.range((t(4), 0)..=(t(2), u32::MAX)).count(), 0);
        }
    }
}
