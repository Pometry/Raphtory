//! A data structure for efficiently storing and querying the temporal adjacency set of a node in a temporal graph.

use std::collections::btree_map::Entry;
use std::collections::BTreeSet;
use std::{
    borrow::{Borrow, BorrowMut},
    collections::BTreeMap,
    hash::Hash,
    ops::{Neg, Range},
};

use itertools::Itertools;
use replace_with::replace_with_or_abort;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};

use crate::core::bitset::BitSet;
use crate::core::sorted_vec_map::SVM;
use crate::core::tgraph::TimeIndex;

const SMALL_SET: usize = 1024;
type Time = i64;
/**
 * Temporal adjacency set can track when adding edge v -> u
 * does u exist already
 * and if it does what is the edge metadata
 * and if the edge is remote or local
 *
 *  */
#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
pub enum TAdjSet<V: Ord + TryInto<usize> + Hash> {
    #[default]
    Empty,
    // One(Time, V, AdjEdge),
    Many {
        n_map: BTreeMap<V, (AdjEdge, TimeIndex)>,
    },
    // Small {
    //     vs: Vec<V>,                 // the neighbours
    //     edges: Vec<AdjEdge>,        // edge metadata
    //     t_index: SVM<Time, BitSet>, // index from t -> [v] where v is the value of vs and edges
    // },
    // Large {
    //     vs: FxHashMap<V, AdjEdge>, // this is equiv to vs and edges
    //     t_index: BTreeMap<Time, BitSet>,
    // },
}

impl<V: Ord + Into<usize> + From<usize> + Copy + Hash + Send + Sync> TAdjSet<V> {
    pub fn new(t: Time, v: V, e: AdjEdge) -> Self {
        let mut n_map = BTreeMap::new();
        n_map.insert(v, (e, TimeIndex::one(t)));
        Self::Many { n_map }
    }

    pub fn len(&self) -> usize {
        match self {
            TAdjSet::Empty => 0,
            TAdjSet::Many { n_map } => n_map.len(),
        }
    }

    pub fn len_window(&self, w: &Range<Time>) -> usize {
        match self {
            TAdjSet::Empty => 0,
            TAdjSet::Many { n_map } => n_map
                .values()
                .filter(|(_, timestamps)| timestamps.active(w.clone()))
                .count(),
        }
    }

    pub fn push(&mut self, t: Time, v: V, e: AdjEdge) {
        match self {
            TAdjSet::Empty => {
                *self = Self::new(t, v, e);
            }
            TAdjSet::Many { n_map } => {
                n_map
                    .entry(v)
                    .and_modify(|(_, timestamps)| {
                        timestamps.insert(t);
                    })
                    .or_insert((e, TimeIndex::one(t)));
            }
        }
    }

    pub fn iter(&self) -> Box<dyn Iterator<Item = (&V, AdjEdge)> + Send + '_> {
        match self {
            TAdjSet::Empty => Box::new(std::iter::empty()),
            TAdjSet::Many { n_map } => Box::new(n_map.iter().map(|(v, (e, _))| (v, *e))),
        }
    }

    pub fn iter_window(
        &self,
        r: &Range<Time>,
    ) -> Box<dyn Iterator<Item = (V, AdjEdge)> + Send + '_> {
        let r = r.clone();
        match self {
            TAdjSet::Empty => Box::new(std::iter::empty()),
            TAdjSet::Many { n_map } => Box::new(
                n_map
                    .iter()
                    .filter_map(move |(v, (e, ts))| ts.active(r.clone()).then_some((*v, *e))),
            ),
        }
    }

    pub fn iter_window_t(
        &self,
        r: &Range<Time>,
    ) -> Box<dyn Iterator<Item = (V, Time, AdjEdge)> + Send + '_> {
        let r = r.clone();
        match self {
            TAdjSet::Empty => Box::new(std::iter::empty()),
            TAdjSet::Many { n_map } => Box::new(
                n_map
                    .iter()
                    .flat_map(move |(v, (e, ts))| ts.range(r.clone()).map(|t| (*v, *t, *e))),
            ),
        }
    }

    pub fn find(&self, v: V) -> Option<AdjEdge> {
        match self {
            TAdjSet::Empty => None,
            TAdjSet::Many { n_map } => n_map.get(&v).map(|(e, _)| *e),
        }
    }

    pub fn find_t(&self, v: V) -> Option<Box<dyn Iterator<Item = (Time, AdjEdge)> + Send + '_>> {
        match self {
            TAdjSet::Empty => None,
            TAdjSet::Many { n_map } => n_map.get(&v).map(|(e, ts)| {
                let iter: Box<dyn Iterator<Item = (Time, AdjEdge)> + Send + '_> =
                    Box::new(ts.iter().map(|t| (*t, *e)));
                iter
            }),
        }
    }

    pub fn find_window(&self, v: V, w: &Range<Time>) -> Option<AdjEdge> {
        match self {
            TAdjSet::Empty => None,
            TAdjSet::Many { n_map } => n_map
                .get(&v)
                .and_then(|(e, ts)| ts.active(w.clone()).then_some(*e)),
        }
    }

    pub fn find_window_t(
        &self,
        v: V,
        w: &Range<Time>,
    ) -> Option<Box<dyn Iterator<Item = (Time, AdjEdge)> + Send + '_>> {
        match self {
            TAdjSet::Empty => None,
            TAdjSet::Many { n_map } => n_map.get(&v).map(|(e, ts)| {
                let iter: Box<dyn Iterator<Item = (Time, AdjEdge)> + Send + '_> =
                    Box::new(ts.range(w.clone()).map(|t| (*t, *e)));
                iter
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Edge<V: Clone + PartialEq + Eq + PartialOrd + Ord> {
    v: V,
    edge_meta: AdjEdge,
    t: Option<u64>,
}

#[repr(transparent)]
#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct AdjEdge(pub(crate) i64);

impl AdjEdge {
    pub fn new(i: usize, local: bool) -> Self {
        if local {
            Self::local(i)
        } else {
            Self::remote(i)
        }
    }

    pub(crate) fn local(i: usize) -> Self {
        AdjEdge(i.try_into().unwrap())
    }

    pub(crate) fn remote(i: usize) -> Self {
        let rev: i64 = i.try_into().unwrap();
        AdjEdge(rev.neg())
    }

    fn is_remote(&self) -> bool {
        self.0 < 0
    }

    pub fn is_local(&self) -> bool {
        !self.is_remote()
    }

    pub fn edge_id(&self) -> usize {
        self.0.abs().try_into().unwrap()
    }
}

#[cfg(test)]
mod tadjset_tests {
    use super::*;

    #[test]
    fn insert() {
        let mut ts: TAdjSet<usize> = TAdjSet::default();

        ts.push(3, 7, AdjEdge::remote(5));

        let actual = ts.iter_window(&(0..3)).collect::<Vec<_>>();
        let expected: Vec<(usize, AdjEdge)> = vec![];
        assert_eq!(actual, expected);

        let actual = ts.iter_window(&(0..4)).collect::<Vec<_>>();
        let expected: Vec<(usize, AdjEdge)> = vec![(7, AdjEdge::remote(5))];
        assert_eq!(actual, expected)
    }

    #[test]
    fn insert_large() {
        let mut ts: TAdjSet<usize> = TAdjSet::default();

        for i in 0..SMALL_SET + 2 {
            ts.push(i.try_into().unwrap(), i, AdjEdge::remote(i));
        }

        for i in 0..SMALL_SET + 2 {
            assert_eq!(ts.find(i), Some(AdjEdge::remote(i)));
        }

        for i in 0..SMALL_SET + 2 {
            let start: i64 = i.try_into().unwrap();
            let mut iter = ts.iter_window(&(start..start + 1));
            assert_eq!(iter.next(), Some((i, AdjEdge::remote(i))));
            assert_eq!(iter.next(), None);
        }

        for i in 0..SMALL_SET + 2 {
            let start: i64 = i.try_into().unwrap();
            let mut iter = ts.iter_window_t(&(start..start + 1));
            let t: i64 = i.try_into().unwrap();
            assert_eq!(iter.next(), Some((i, t, AdjEdge::remote(i))));
            assert_eq!(iter.next(), None);
        }
    }

    #[test]
    fn insert_twice() {
        let mut ts: TAdjSet<usize> = TAdjSet::default();

        ts.push(3, 7, AdjEdge::local(9));
        ts.push(3, 7, AdjEdge::local(9));

        let actual = ts.iter_window(&(0..3)).collect::<Vec<_>>();
        let expected: Vec<(usize, AdjEdge)> = vec![];
        assert_eq!(actual, expected);

        let actual = ts.iter_window(&(0..4)).collect::<Vec<_>>();
        let expected: Vec<(usize, AdjEdge)> = vec![(7, AdjEdge::local(9))];
        assert_eq!(actual, expected);
    }

    #[test]
    fn insert_twice_different_time() {
        let mut ts: TAdjSet<usize> = TAdjSet::default();

        ts.push(3, 7, AdjEdge::remote(19));
        ts.push(9, 7, AdjEdge::remote(19));

        let actual = ts.iter_window(&(0..3)).collect::<Vec<_>>();
        let expected: Vec<(usize, AdjEdge)> = vec![];
        assert_eq!(actual, expected);

        let actual = ts.iter_window(&(0..4)).collect::<Vec<_>>();
        let expected: Vec<(usize, AdjEdge)> = vec![(7, AdjEdge::remote(19))];
        assert_eq!(actual, expected);

        let actual = ts.iter_window(&(0..12)).collect::<Vec<_>>();
        let expected: Vec<(usize, AdjEdge)> = vec![(7, AdjEdge::remote(19))];
        assert_eq!(actual, expected)
    }

    #[test]
    fn insert_different_time() {
        let mut ts: TAdjSet<usize> = TAdjSet::default();

        ts.push(9, 1, AdjEdge::local(0));
        ts.push(3, 7, AdjEdge::remote(1));

        let actual = ts.iter_window(&(0..3)).collect::<Vec<_>>();
        let expected: Vec<(usize, AdjEdge)> = vec![];
        assert_eq!(actual, expected);

        let actual = ts.iter_window(&(0..4)).collect::<Vec<_>>();
        let expected: Vec<(usize, AdjEdge)> = vec![(7, AdjEdge::remote(1))];
        assert_eq!(actual, expected);

        let actual = ts.iter_window(&(4..10)).collect::<Vec<_>>();
        let expected: Vec<(usize, AdjEdge)> = vec![(1, AdjEdge::local(0))];
        assert_eq!(actual, expected);

        let actual = ts.iter_window(&(0..12)).collect::<Vec<_>>();
        let expected: Vec<(usize, AdjEdge)> = vec![(1, AdjEdge::local(0)), (7, AdjEdge::remote(1))];
        assert_eq!(actual, expected);
    }

    #[test]
    fn insert_different_time_with_times() {
        let mut ts: TAdjSet<usize> = TAdjSet::default();

        ts.push(9, 1, AdjEdge::local(0));
        ts.push(3, 7, AdjEdge::remote(1));

        let actual = ts.iter_window(&(0..3)).collect::<Vec<_>>();
        let expected: Vec<(usize, AdjEdge)> = vec![];
        assert_eq!(actual, expected);

        let actual = ts.iter_window_t(&(0..4)).collect::<Vec<_>>();
        let expected: Vec<(usize, i64, AdjEdge)> = vec![(7, 3, AdjEdge::remote(1))];
        assert_eq!(actual, expected);

        let actual = ts.iter_window_t(&(4..10)).collect::<Vec<_>>();
        let expected: Vec<(usize, i64, AdjEdge)> = vec![(1, 9, AdjEdge::local(0))];
        assert_eq!(actual, expected);

        let actual = ts.iter_window_t(&(0..12)).collect::<Vec<_>>();
        let expected: Vec<(usize, i64, AdjEdge)> =
            vec![(1, 9, AdjEdge::local(0)), (7, 3, AdjEdge::remote(1))];
        assert_eq!(actual, expected);
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
