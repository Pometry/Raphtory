use std::{
    borrow::Borrow,
    collections::{btree_map::Entry, BTreeMap, BTreeSet},
    ops::Range,
};

use itertools::Itertools;

/**
 * This is a time aware set there are two major components
 * the time index aka (you should be able to locate in log(n) time the vertices participating in ti -> tj window where i <= j)
 * the uniqueness of the V values, since these are used in adjacency sets and keep track
 * of edge pointers we need to reliably get to any V in log(n) time
 * idempotency, adding the same pair (t, V) does nothing
 *  */
#[derive(Debug, PartialEq)]
pub enum TSet<V: Ord> {
    Empty,
    One(u64, V),
    Tree {
        t_index: BTreeMap<u64, BTreeSet<V>>,
        vs: BTreeSet<V>,
    }, // HashSet of V
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

    pub fn find(&self, v: V) -> Option<&V> {
        match self {
            TSet::Empty => None,
            TSet::One(_, v0) => {
                if v0 >= &v {
                    Some(v0)
                } else {
                    None
                }
            }
            TSet::Tree { vs, .. } => vs.range(v..).next(),
        }
    }

    pub fn push(&mut self, t: u64, v: V) {
        match self.borrow() {
            TSet::Empty => {
                *self = TSet::One(t, v);
            }
            TSet::One(t0, v0) => {
                if !(t == *t0 && &v == v0) {
                    *self = TSet::Tree {
                        t_index: BTreeMap::from([
                            (t, BTreeSet::from([v.clone()])),
                            (*t0, BTreeSet::from([v0.clone()])),
                        ]),
                        vs: BTreeSet::from([v, v0.clone()]),
                    };
                }
            }
            TSet::Tree { .. } => {
                if let TSet::Tree { t_index, vs } = self {
                    vs.insert(v.clone());
                    let entry = t_index.entry(t);
                    match entry {
                        Entry::Vacant(ve) => {
                            ve.insert(BTreeSet::from([v; 1]));
                        }
                        Entry::Occupied(mut oc) => {
                            oc.get_mut().insert(v);
                        }
                    }
                }
            }
        }
    }

    pub fn iter_window(&self, r: &Range<u64>) -> Box<dyn Iterator<Item = &V> + '_> {
        match self {
            TSet::Empty => Box::new(std::iter::empty()),
            TSet::One(t, v) => {
                if r.contains(t) {
                    Box::new(std::iter::once(v))
                } else {
                    Box::new(std::iter::empty())
                }
            }
            TSet::Tree { t_index, .. } => {
                Box::new(t_index.range(r.clone()).map(|(_, set)| set.iter()).kmerge().dedup())
            }
        }
    }


    pub fn iter_window_t(&self, r: &Range<u64>) -> Box<dyn Iterator<Item = (&u64, &V)> + '_> {
        match self {
            TSet::Empty => Box::new(std::iter::empty()),
            TSet::One(t, v) => Box::new(std::iter::once((t, v))),
            TSet::Tree { t_index, .. } => Box::new(
                t_index
                    .range(r.clone())
                    .flat_map(|(t, set)| set.iter().map(move |v| (t, v))),
            ),
        }
    }

    pub fn iter(&self) -> Box<dyn Iterator<Item = &V> + '_> {
        match self {
            TSet::Empty => Box::new(std::iter::empty()),
            TSet::One(_, v) => Box::new(std::iter::once(v)),
            TSet::Tree { vs, .. } => Box::new(vs.iter()),
        }
    }

    pub fn len(&self) -> usize {
        self.iter().count()
    }
}

#[cfg(test)]
mod tset_tests {
    use crate::edge::Edge;

    use super::*;

    #[test]
    fn insert() {
        let mut ts: TSet<usize> = TSet::default();

        ts.push(3, 7);

        let actual = ts.iter_window(&(0..3)).collect::<Vec<_>>();
        let expected: Vec<&usize> = vec![];
        assert_eq!(actual, expected);

        let actual = ts.iter_window(&(0..4)).collect::<Vec<_>>();
        let expected: Vec<&usize> = vec![&7];
        assert_eq!(actual, expected)
    }

    #[test]
    fn insert_twice() {
        let mut ts: TSet<usize> = TSet::default();

        ts.push(3, 7);
        ts.push(3, 7);

        let actual = ts.iter_window(&(0..3)).collect::<Vec<_>>();
        let expected: Vec<&usize> = vec![];
        assert_eq!(actual, expected);

        let actual = ts.iter_window(&(0..4)).collect::<Vec<_>>();
        let expected: Vec<&usize> = vec![&7];
        assert_eq!(actual, expected)
    }

    #[test]
    fn insert_twice_different_time() {
        let mut ts: TSet<usize> = TSet::default();

        ts.push(3, 7);
        ts.push(9, 7);

        let actual = ts.iter_window(&(0..3)).collect::<Vec<_>>();
        let expected: Vec<&usize> = vec![];
        assert_eq!(actual, expected);

        let actual = ts.iter_window(&(0..4)).collect::<Vec<_>>();
        let expected: Vec<&usize> = vec![&7];
        assert_eq!(actual, expected);

        let actual = ts.iter_window(&(0..12)).collect::<Vec<_>>();
        let expected: Vec<&usize> = vec![&7];
        assert_eq!(actual, expected)
    }

    #[test]
    fn insert_different_time() {
        let mut ts: TSet<usize> = TSet::default();

        ts.push(9, 1);
        ts.push(3, 7);

        let actual = ts.iter_window(&(0..3)).collect::<Vec<_>>();
        let expected: Vec<&usize> = vec![];
        assert_eq!(actual, expected);

        let actual = ts.iter_window(&(0..4)).collect::<Vec<_>>();
        let expected: Vec<&usize> = vec![&7];
        assert_eq!(actual, expected);

        let actual = ts.iter_window(&(0..12)).collect::<Vec<_>>();
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

    #[test]
    fn find_added_edge_just_by_destination_id() {
        let mut ts: TSet<Edge> = TSet::default();

        ts.push(1, Edge::new(1, 3)); // t:1, v: 1 edge_id: 3
        //
        let actual = ts.find(Edge { v: 1, e_meta: None });
        assert_eq!(actual, Some(&Edge::new(1, 3)));

        let actual = ts.find(Edge { v: 13, e_meta: None });
        assert_eq!(actual, None);

        ts.push(1, Edge::new(4, 12)); // t:1, v: 4 edge_id: 12
        ts.push(1, Edge::new(17, 119)); // t:1, v: 17 edge_id: 119

        // find the edge by destination only (independent of time?)
        let actual = ts.find(Edge { v: 1, e_meta: None });
        assert_eq!(actual, Some(&Edge::new(1, 3)));

        let actual = ts.find(Edge { v: 4, e_meta: None });
        assert_eq!(actual, Some(&Edge::new(4, 12)));

        let actual = ts.find(Edge {
            v: 17,
            e_meta: None,
        });
        assert_eq!(actual, Some(&Edge::new(17, 119)));

        let actual = ts.find(Edge { v: 5, e_meta: None }); // we need to activelly filter this out
        assert_eq!(actual, Some(&Edge::new(17, 119)));
    }
}
