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
    One(V, AdjEdge),
    Small {
        vs: Vec<V>,          // the neighbours
        edges: Vec<AdjEdge>, // edge metadata
    },
    Large {
        vs: BTreeMap<V, AdjEdge>, // this is equiv to vs and edges
    },
}

impl<V: Ord + Into<usize> + From<usize> + Copy + Hash + Send + Sync> TAdjSet<V> {
    pub fn new(v: V, e: AdjEdge) -> Self {
        Self::One(v, e)
    }

    pub fn len(&self) -> usize {
        match self {
            TAdjSet::Empty => 0,
            TAdjSet::One(_, _) => 1,
            TAdjSet::Small { vs, .. } => vs.len(),
            TAdjSet::Large { vs } => vs.len(),
        }
    }

    pub fn len_window(&self, timestamps: &[TimeIndex], window: &Range<i64>) -> usize {
        match self {
            TAdjSet::Empty => 0,
            TAdjSet::One(_, e) => {
                if timestamps[e.edge_id()].active(window.clone()) {
                    1
                } else {
                    0
                }
            }

            TAdjSet::Small { edges, .. } => edges
                .iter()
                .filter(|&&e| timestamps[e.edge_id()].active(window.clone()))
                .count(),
            TAdjSet::Large { vs } => vs
                .values()
                .filter(|&&e| timestamps[e.edge_id()].active(window.clone()))
                .count(),
        }
    }

    pub fn push(&mut self, v: V, e: AdjEdge) {
        match self {
            TAdjSet::Empty => {
                *self = Self::new(v, e);
            }
            TAdjSet::One(vv, ee) => {
                if *vv < v {
                    *self = Self::Small {
                        vs: vec![*vv, v],
                        edges: vec![*ee, e],
                    }
                } else if *vv > v {
                    *self = Self::Small {
                        vs: vec![v, *vv],
                        edges: vec![e, *ee],
                    }
                }
            }
            TAdjSet::Small { vs, edges } => match vs.binary_search(&v) {
                Ok(_) => {}
                Err(i) => {
                    if vs.len() < SMALL_SET {
                        vs.insert(i, v);
                        edges.insert(i, e);
                    } else {
                        let mut map =
                            BTreeMap::from_iter(vs.iter().copied().zip(edges.iter().copied()));
                        map.insert(v, e);
                        *self = Self::Large { vs: map }
                    }
                }
            },
            TAdjSet::Large { vs } => {
                vs.insert(v, e);
            }
        }
    }

    pub fn iter(&self) -> Box<dyn Iterator<Item = (V, AdjEdge)> + Send + '_> {
        match self {
            TAdjSet::Empty => Box::new(std::iter::empty()),
            TAdjSet::One(v, e) => Box::new(std::iter::once((*v, *e))),
            TAdjSet::Small { vs, edges } => Box::new(vs.iter().copied().zip(edges.iter().copied())),
            TAdjSet::Large { vs } => Box::new(vs.iter().map(|(k, v)| (*k, *v))),
        }
    }

    pub fn vertices(&self) -> Box<dyn Iterator<Item = V> + Send + '_> {
        match self {
            TAdjSet::Empty => Box::new(std::iter::empty()),
            TAdjSet::One(v, ..) => Box::new(std::iter::once(*v)),
            TAdjSet::Small { vs, .. } => Box::new(vs.iter().copied()),
            TAdjSet::Large { vs } => Box::new(vs.keys().copied()),
        }
    }

    pub fn iter_window<'a>(
        &'a self,
        timestamps: &'a [TimeIndex],
        window: &Range<i64>,
    ) -> Box<dyn Iterator<Item = (V, AdjEdge)> + Send + 'a> {
        let w = window.clone();
        Box::new(
            self.iter()
                .filter(move |(_, e)| timestamps[e.edge_id()].active(w.clone())),
        )
    }

    pub fn vertices_window<'a>(
        &'a self,
        timestamps: &'a [TimeIndex],
        window: &Range<i64>,
    ) -> Box<dyn Iterator<Item = V> + Send + 'a> {
        let w = window.clone();
        Box::new(
            self.iter()
                .filter(move |(_, e)| timestamps[e.edge_id()].active(w.clone()))
                .map(|(v, e)| v),
        )
    }

    pub fn find(&self, v: V) -> Option<AdjEdge> {
        match self {
            TAdjSet::Empty => None,
            TAdjSet::One(vv, e) => (*vv == v).then_some(*e),
            TAdjSet::Small { vs, edges } => vs.binary_search(&v).ok().map(|i| edges[i]),
            TAdjSet::Large { vs } => vs.get(&v).copied(),
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
    // Internal id uses sign to store remote/local flag, hence it is offset by 1 as 0 cannot be used
    pub fn new(i: usize, local: bool) -> Self {
        if local {
            Self::local(i)
        } else {
            Self::remote(i)
        }
    }

    pub(crate) fn local(i: usize) -> Self {
        AdjEdge((i + 1).try_into().unwrap())
    }

    pub(crate) fn remote(i: usize) -> Self {
        let rev: i64 = i.try_into().unwrap();
        AdjEdge(rev.neg() - 1)
    }

    pub(crate) fn is_remote(&self) -> bool {
        self.0 < 0
    }

    pub fn is_local(&self) -> bool {
        !self.is_remote()
    }

    pub fn edge_id(&self) -> usize {
        (self.0.abs() - 1).try_into().unwrap()
    }
}

#[cfg(test)]
mod tadjset_tests {
    use super::*;
    use crate::core::adj::Adj;
    use quickcheck::TestResult;

    #[quickcheck]
    fn insert_fuzz(input: Vec<(usize, bool)>) -> bool {
        let mut ts: TAdjSet<usize> = TAdjSet::default();

        for (e, (i, is_remote)) in input.iter().enumerate() {
            if *is_remote {
                ts.push(*i, AdjEdge::remote(e));
            } else {
                ts.push(*i, AdjEdge::local(e));
            }
        }

        let res = input.iter().all(|(i, _)| ts.find(*i).is_some());
        if !res {
            let ts_vec: Vec<(usize, AdjEdge)> = ts.iter().collect();
            println!("Input: {:?}", input);
            println!("TAdjSet: {:?}", ts_vec);
        }
        res
    }

    #[quickcheck]
    fn adjedge_fuzz(e_id: i64, is_remote: bool) -> TestResult {
        if e_id < 0 || e_id == i64::MAX {
            return TestResult::discard();
        }
        let e_id = e_id as usize;
        let e = AdjEdge::new(e_id, !is_remote);
        TestResult::from_bool(e.is_remote() == is_remote && e_id == e.edge_id())
    }

    #[test]
    fn insert() {
        let mut ts: TAdjSet<usize> = TAdjSet::default();

        ts.push(7, AdjEdge::remote(5));
        let actual = ts.iter().collect::<Vec<_>>();
        let expected: Vec<(usize, AdjEdge)> = vec![(7, AdjEdge::remote(5))];
        assert_eq!(actual, expected)
    }

    #[test]
    fn insert_large() {
        let mut ts: TAdjSet<usize> = TAdjSet::default();

        for i in 0..SMALL_SET + 2 {
            ts.push(i, AdjEdge::remote(i));
        }

        for i in 0..SMALL_SET + 2 {
            assert_eq!(ts.find(i), Some(AdjEdge::remote(i)));
        }
    }

    #[test]
    fn insert_twice() {
        let mut ts: TAdjSet<usize> = TAdjSet::default();

        ts.push(7, AdjEdge::local(9));
        ts.push(7, AdjEdge::local(9));

        let actual = ts.iter().collect::<Vec<_>>();
        let expected: Vec<(usize, AdjEdge)> = vec![(7, AdjEdge::local(9))];
        assert_eq!(actual, expected);
    }

    #[test]
    fn insert_two_different() {
        let mut ts: TAdjSet<usize> = TAdjSet::default();

        ts.push(1, AdjEdge::local(0));
        ts.push(7, AdjEdge::remote(1));

        let actual = ts.iter().collect::<Vec<_>>();
        let expected: Vec<(usize, AdjEdge)> = vec![(1, AdjEdge::local(0)), (7, AdjEdge::remote(1))];
        assert_eq!(actual, expected);
    }
}
