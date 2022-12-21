use std::{
    borrow::Borrow,
    collections::{btree_map::Entry, BTreeMap, BTreeSet},
    ops::Range,
};

use itertools::Itertools;

#[derive(Debug, PartialEq)]
pub enum TSet<V: Ord> {
    Empty,
    One(u64, V),
    Tree(BTreeMap<u64, BTreeSet<V>>),
}

impl<V: Ord> Default for TSet<V> {
    fn default() -> Self {
        TSet::Empty
    }
}

impl<V: Ord + Clone> TSet<V> {
    pub fn new(t: u64, k: V) -> Self {
        TSet::One(t, k)
    }

    pub fn push(&mut self, t: u64, k: V) {
        match self.borrow() {
            TSet::Empty => {
                *self = TSet::One(t, k);
            }
            TSet::One(t0, v0) => {
                if !(t == *t0 && &k == v0) {
                    *self = TSet::Tree(BTreeMap::from([
                        (t, BTreeSet::from([k])),
                        (*t0, BTreeSet::from([v0.clone()])),
                    ]));
                }
            }
            TSet::Tree(_) => {
                if let TSet::Tree(vs) = self {
                    let entry = vs.entry(t);
                    match entry {
                        Entry::Vacant(ve) => {
                            ve.insert(BTreeSet::from([k; 1]));
                        }
                        Entry::Occupied(mut oc) => {
                            oc.get_mut().insert(k);
                        }
                    }
                }
            }
        }
    }


    pub fn iter_window(&self, r: Range<u64>) -> Box<dyn Iterator<Item = &V> + '_> {
        match self {
            TSet::Empty => Box::new(std::iter::empty()),
            TSet::One(t, v) => {
                if r.contains(t) {
                    Box::new(std::iter::once(v))
                } else {
                    Box::new(std::iter::empty())
                }
            }
            TSet::Tree(vs) => Box::new(vs.range(r).map(|(_, set)| set.iter()).kmerge().dedup()),
        }
    }

    pub fn iter_window_t(&self, r: Range<u64>) -> Box<dyn Iterator<Item = (&u64, &V)> + '_> {
        match self {
            TSet::Empty => Box::new(std::iter::empty()),
            TSet::One(t, v) => Box::new(std::iter::once((t, v))),
            TSet::Tree(vs) => Box::new(
                vs.range(r)
                    .flat_map(|(t, set)| set.iter().map(move |v| (t, v))),
            ),
        }
    }

    pub fn iter(&self) -> Box<dyn Iterator<Item = &V> + '_> {
        match self {
            TSet::Empty => Box::new(std::iter::empty()),
            TSet::One(_, v) => Box::new(std::iter::once(v)),
            TSet::Tree(vs) => Box::new(vs.iter().map(|(_, set)| set.iter()).kmerge().dedup()),
        }
    }

    pub fn len(&self) -> usize {
        self.iter().count()
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
