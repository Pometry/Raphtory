use std::{
    collections::{btree_map::Entry, BTreeMap, BTreeSet, HashSet},
    hash::Hash,
    ops::Range,
};

use itertools::Itertools;

use crate::bitset::BitSet;

#[derive(Debug, PartialEq, Default)]
pub struct TSet<V: Eq + Hash> {
    vs: BTreeMap<u64, BTreeSet<V>>,
}

impl<V: Ord + Hash> TSet<V> {
    pub fn new(t: u64, k: V) -> Self {
        TSet {
            vs: BTreeMap::from([(t, BTreeSet::from([k; 1])); 1]),
        }
    }

    pub fn push(&mut self, t: u64, k: V) {
        let entry = self.vs.entry(t);
        match entry {
            Entry::Vacant(ve) => {
                ve.insert(BTreeSet::from([k; 1]));
            }
            Entry::Occupied(mut oc) => {
                oc.get_mut().insert(k);
            }
        }
        // let insert_at = match self.vs.binary_search_by_key(&t, |(t0, _)| *t0) {
        //     Ok(found) => {
        //         // we found a time slot, need to iterate over all the values associated with time t
        //         // if we find K exists already we exit, otherwise we insert it in
        //         let vs_t = &self.vs[found..];

        //         let mut i:usize= found;
        //         for (t0, k0) in vs_t {
        //             if k0 == &k {
        //                 return self; // done, nothing to do here
        //             }
        //             if *t0 != t {
        //                 break; // we done searching
        //             }
        //             i += 1;
        //         }
        //         i // this is where we insert the tuple
        //     }
        //     Err(not_found) => not_found,
        // };

        // self.vs.insert(insert_at, (t, k));
        // self
    }

    pub fn iter_window(&self, r: Range<u64>) -> Box<dyn Iterator<Item = &V> + '_> {
        Box::new(self.vs.range(r).map(|(_, set)| set.iter()).kmerge().dedup())
    }

    pub fn iter_window_t(&self, r: Range<u64>) -> Box<dyn Iterator<Item = (&u64, &V)> + '_> {
        Box::new(
            self.vs
                .range(r)
                .flat_map(|(t, set)| set.iter().map(move |v| (t, v))),
        )
    }

    pub fn iter(&self) -> Box<dyn Iterator<Item = &V> + '_> {
        Box::new(self.vs.iter().map(|(_, set)| set.iter()).kmerge().dedup())
    }
}

#[cfg(test)]
mod tset_tests {
    use super::*;

    #[test]
    fn insert() {
        let mut ts: TSet<usize> = TSet::default();

        ts.push(3, 7);

        let actual = ts.iter_window(0..3).collect::<Vec<_>>();
        let expected: Vec<&usize> = vec![];
        assert_eq!(actual, expected);

        let actual = ts.iter_window(0..4).collect::<Vec<_>>();
        let expected: Vec<&usize> = vec![&7];
        assert_eq!(actual, expected)
    }

    #[test]
    fn insert_twice() {
        let mut ts: TSet<usize> = TSet::default();

        ts.push(3, 7);
        ts.push(3, 7);

        let actual = ts.iter_window(0..3).collect::<Vec<_>>();
        let expected: Vec<&usize> = vec![];
        assert_eq!(actual, expected);

        let actual = ts.iter_window(0..4).collect::<Vec<_>>();
        let expected: Vec<&usize> = vec![&7];
        assert_eq!(actual, expected)
    }

    #[test]
    fn insert_twice_different_time() {
        let mut ts: TSet<usize> = TSet::default();

        ts.push(3, 7);
        ts.push(9, 7);

        let actual = ts.iter_window(0..3).collect::<Vec<_>>();
        let expected: Vec<&usize> = vec![];
        assert_eq!(actual, expected);

        let actual = ts.iter_window(0..4).collect::<Vec<_>>();
        let expected: Vec<&usize> = vec![&7];
        assert_eq!(actual, expected);

        let actual = ts.iter_window(0..12).collect::<Vec<_>>();
        let expected: Vec<&usize> = vec![&7];
        assert_eq!(actual, expected)
    }

    #[test]
    fn insert_different_time() {
        let mut ts: TSet<usize> = TSet::default();

        ts.push(9, 1);
        ts.push(3, 7);

        let actual = ts.iter_window(0..3).collect::<Vec<_>>();
        let expected: Vec<&usize> = vec![];
        assert_eq!(actual, expected);

        let actual = ts.iter_window(0..4).collect::<Vec<_>>();
        let expected: Vec<&usize> = vec![&7];
        assert_eq!(actual, expected);

        let actual = ts.iter_window(0..12).collect::<Vec<_>>();
        let expected: Vec<&usize> = vec![&1, &7];
        assert_eq!(actual, expected)
    }

    #[test]
    fn k_merge() {
        let a: Vec<usize> = vec![7];
        let b: Vec<usize> = vec![1];

        let actual = vec![a.iter(), b.iter()]
            .into_iter()
            .kmerge()
            .dedup()
            .collect::<Vec<_>>();

        let expected: Vec<&usize> = vec![&1, &7];
        assert_eq!(actual, expected)
    }
}
