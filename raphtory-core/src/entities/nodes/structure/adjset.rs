//! A data structure for efficiently storing and querying the temporal adjacency set of a node in a temporal graph.
use raphtory_api::{core::storage::sorted_vec_map::SVM, iter::BoxedLIter};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, hash::Hash};

const SMALL_SET: usize = 1024;

/**
 * Temporal adjacency set can track when adding edge v -> u
 * does u exist already
 * and if it does what is the edge metadata
 *
 *  */
#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
pub enum AdjSet<K: Ord + Copy + Hash + Send + Sync, V: Into<usize> + Copy + Send + Sync> {
    #[default]
    Empty,
    One(K, V),
    Small(SVM<K, V>), // the neighbours and their edge metadata
    Large {
        vs: BTreeMap<K, V>, // this is equiv to vs and edges
    },
    // TODO: if we use BTreeSet<(K, Option<V>)> we could implement intersections and support edge label queries such as a && b
}

impl<K: Ord + Copy + Hash + Send + Sync, V: Into<usize> + Copy + Send + Sync> AdjSet<K, V> {
    pub fn len(&self) -> usize {
        match self {
            AdjSet::Empty => 0,
            AdjSet::One(_, _) => 1,
            AdjSet::Small(vs) => vs.len(),
            AdjSet::Large { vs } => vs.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            AdjSet::Empty => true,
            AdjSet::One(_, _) => false,
            AdjSet::Small(vs) => vs.is_empty(),
            AdjSet::Large { vs } => vs.is_empty(),
        }
    }
    pub fn new(v: K, e: V) -> Self {
        Self::One(v, e)
    }

    /// Push a new node and edge into the adjacency set.
    ///
    /// If the node already exists, it will not be added again.
    /// Returns `true` if the node was added, `false` if it already existed
    pub fn push(&mut self, v: K, e: V) -> bool {
        match self {
            AdjSet::Empty => {
                *self = Self::new(v, e);
                true
            }
            AdjSet::One(vv, ee) => {
                if *vv == v {
                    // already exists
                    return false;
                }
                let mut vs = SVM::new();
                vs.insert(*vv, *ee);
                vs.insert(v, e);
                *self = Self::Small(vs);
                true
            }
            AdjSet::Small(vs) => {
                if vs.len() < SMALL_SET {
                    vs.insert_if_absent(v, e)
                } else if vs.get(&v).is_some() {
                    false
                } else {
                    let mut map: BTreeMap<K, V> = std::mem::take(vs).into_iter().collect();
                    map.insert(v, e);
                    *self = Self::Large { vs: map };
                    true
                }
            }
            AdjSet::Large { vs } => vs.insert(v, e).is_none(),
        }
    }

    /// Bulk-load insert: appends a node and edge without keeping a small set sorted or checking
    /// for duplicates. The set must be restored with [`Self::sort_dedup`] before it is read.
    /// Returns `true` if the set may now be out of order.
    #[inline]
    pub fn push_unsorted(&mut self, v: K, e: V) -> bool {
        match self {
            AdjSet::Empty => {
                *self = Self::new(v, e);
                false
            }
            AdjSet::One(vv, ee) => {
                let out_of_order = v <= *vv;
                let mut vs = SVM::new();
                vs.push_unsorted(*vv, *ee);
                vs.push_unsorted(v, e);
                *self = Self::Small(vs);
                out_of_order
            }
            AdjSet::Small(vs) => {
                let out_of_order = vs.last_key_value().is_some_and(|(last, _)| v <= *last);
                vs.push_unsorted(v, e);
                out_of_order
            }
            AdjSet::Large { vs } => {
                vs.insert(v, e);
                false
            }
        }
    }

    /// Sorts a set filled with [`Self::push_unsorted`] in place, keeping the last edge for a
    /// repeated node, and returns the number of duplicates removed. Small sets that grew past
    /// the cutoff stay vectors; [`Self::push`] turns them into a `BTreeMap` if they are written
    /// to again.
    pub fn sort_dedup(&mut self) -> usize {
        match self {
            AdjSet::Small(vs) => {
                let len = vs.len();
                vs.sort_dedup();
                len - vs.len()
            }
            _ => 0,
        }
    }

    pub fn iter(&self) -> BoxedLIter<'_, (K, V)> {
        match self {
            AdjSet::Empty => Box::new(std::iter::empty()),
            AdjSet::One(v, e) => Box::new(std::iter::once((*v, *e))),
            AdjSet::Small(vs) => Box::new(vs.iter().map(|(k, v)| (*k, *v))),
            AdjSet::Large { vs } => Box::new(vs.iter().map(|(k, v)| (*k, *v))),
        }
    }

    pub fn nodes(&self) -> Box<dyn Iterator<Item = K> + Send + '_> {
        match self {
            AdjSet::Empty => Box::new(std::iter::empty()),
            AdjSet::One(v, ..) => Box::new(std::iter::once(*v)),
            AdjSet::Small(vs) => Box::new(vs.iter().map(|(k, _)| *k)),
            AdjSet::Large { vs } => Box::new(vs.keys().copied()),
        }
    }

    pub fn find(&self, v: K) -> Option<V> {
        match self {
            AdjSet::Empty => None,
            AdjSet::One(vv, e) => (*vv == v).then_some(*e),
            AdjSet::Small(vs) => vs.get(&v).copied(),
            AdjSet::Large { vs } => vs.get(&v).copied(),
        }
    }

    /// The entries of a small set after `last`, or all of them if `last` is `None`.
    fn small_after(vs: &SVM<K, V>, last: Option<K>) -> &[(K, V)] {
        let entries = vs.as_slice();
        match last {
            Some(l) => &entries[entries.partition_point(|(k, _)| *k <= l)..],
            None => entries,
        }
    }

    /// puts elements into page and returns number of returned elements
    pub fn fill_page<const P: usize>(&self, last: Option<K>, page: &mut [(K, V); P]) -> usize {
        match self {
            AdjSet::Empty => 0,
            AdjSet::One(v, i) => {
                if let Some(l) = last {
                    if l < *v {
                        page[0] = (*v, *i);
                        1
                    } else {
                        0
                    }
                } else {
                    page[0] = (*v, *i);
                    1
                }
            }
            AdjSet::Small(vs) => {
                let entries = Self::small_after(vs, last);
                let n = entries.len().min(P);
                page[..n].copy_from_slice(&entries[..n]);
                n
            }
            AdjSet::Large { vs } => {
                if let Some(l) = last {
                    let mut index = 0;
                    vs.range(l..).skip(1).take(P).for_each(|(a, b)| {
                        page[index] = (*a, *b);
                        index += 1;
                    });
                    index
                } else {
                    let mut index = 0;
                    vs.iter().take(P).for_each(|(a, b)| {
                        page[index] = (*a, *b);
                        index += 1
                    });
                    index
                }
            }
        }
    }
    pub fn get_page_vec(&self, last: Option<K>, page_size: usize) -> Vec<(K, V)> {
        match self {
            AdjSet::Empty => vec![],
            AdjSet::One(v, i) => {
                if let Some(l) = last {
                    if l < *v {
                        vec![(*v, *i)]
                    } else {
                        vec![]
                    }
                } else {
                    vec![(*v, *i)]
                }
            }
            AdjSet::Small(vs) => {
                let entries = Self::small_after(vs, last);
                entries[..entries.len().min(page_size)].to_vec()
            }
            AdjSet::Large { vs } => {
                if let Some(l) = last {
                    vs.range(l..)
                        .skip(1)
                        .take(page_size)
                        .map(|(a, b)| (*a, *b))
                        .collect()
                } else {
                    vs.iter().take(page_size).map(|(a, b)| (*a, *b)).collect()
                }
            }
        }
    }
}

#[cfg(test)]
mod tadjset_tests {
    use super::*;
    use proptest::{arbitrary::any, prop_assert, proptest};

    #[test]
    fn insert_fuzz_proptest() {
        proptest!(|(input in any::<Vec<usize>>())| {
            let mut ts: AdjSet<usize, usize> = AdjSet::default();
            for (e, i) in input.iter().enumerate() {
                ts.push(*i, e);
            }
            prop_assert!(input.iter().all(|i| ts.find(*i).is_some()));
        })
    }

    #[test]
    fn insert() {
        let mut ts: AdjSet<usize, usize> = AdjSet::default();

        ts.push(7, 5);
        let actual = ts.iter().collect::<Vec<_>>();
        let expected: Vec<(usize, usize)> = vec![(7, 5)];
        assert_eq!(actual, expected)
    }

    #[test]
    fn insert_large() {
        let mut ts: AdjSet<usize, usize> = AdjSet::default();

        for i in 0..SMALL_SET + 2 {
            ts.push(i, i);
        }

        for i in 0..SMALL_SET + 2 {
            assert_eq!(ts.find(i), Some(i));
        }
    }

    #[test]
    fn insert_twice() {
        let mut ts: AdjSet<usize, usize> = AdjSet::default();

        ts.push(7, 9);
        ts.push(7, 9);

        let actual = ts.iter().collect::<Vec<_>>();
        let expected: Vec<(usize, usize)> = vec![(7, 9)];
        assert_eq!(actual, expected);
    }

    #[test]
    fn insert_two_different() {
        let mut ts: AdjSet<usize, usize> = AdjSet::default();

        ts.push(1, 0);
        ts.push(7, 1);

        let actual = ts.iter().collect::<Vec<_>>();
        let expected: Vec<(usize, usize)> = vec![(1, 0), (7, 1)];
        assert_eq!(actual, expected);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sort_dedup_after_push_unsorted_matches_push() {
        let input: Vec<(u64, usize)> = (0..3000u64).rev().map(|v| (v % 2000, v as usize)).collect();
        let mut sorted = AdjSet::default();
        let mut appended = AdjSet::default();
        for &(v, e) in &input {
            sorted.push(v, e);
            appended.push_unsorted(v, e);
        }
        assert_eq!(appended.sort_dedup(), 1000);
        assert_eq!(appended.nodes().collect::<Vec<_>>(), sorted.nodes().collect::<Vec<_>>());
        assert!(matches!(appended, AdjSet::Small(_)));
        assert!(appended.push(5000, 0));
        assert!(matches!(appended, AdjSet::Large { .. }));
    }

    #[test]
    fn push_unsorted_reports_out_of_order() {
        let mut set = AdjSet::default();
        assert!(!set.push_unsorted(1u64, 0usize));
        assert!(!set.push_unsorted(2, 1));
        assert!(set.push_unsorted(0, 2));
        assert_eq!(set.sort_dedup(), 0);
        assert_eq!(set.iter().collect::<Vec<_>>(), vec![(0, 2), (1, 0), (2, 1)]);
    }
}
