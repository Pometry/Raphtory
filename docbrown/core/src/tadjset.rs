use std::{
    borrow::{Borrow, BorrowMut},
    collections::{BTreeMap, HashMap},
    hash::Hash,
    ops::{Neg, Range},
};

use itertools::Itertools;
use replace_with::replace_with_or_abort;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};

use crate::{bitset::BitSet, sorted_vec_map::SVM};

const SMALL_SET: usize = 1024;

/**
 * Temporal adjacency set can track when adding edge v -> u
 * does u exist already
 * and if it does what is the edge metadata
 * and if the edge is remote or local
 *
 *  */
#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
pub enum TAdjSet<V: Ord + TryInto<usize> + std::hash::Hash, Time: Copy + Ord> {
    #[default]
    Empty,
    One(Time, V, AdjEdge),
    Small {
        vs: Vec<V>,                 // the neighbours
        edges: Vec<AdjEdge>,        // edge metadata
        t_index: SVM<Time, BitSet>, // index from t -> [v] where v is the value of vs and edges
    },
    Large {
        vs: FxHashMap<V, AdjEdge>, // this is equiv to vs and edges
        t_index: BTreeMap<Time, BitSet>,
    },
}

impl<
        V: Ord + Into<usize> + From<usize> + Copy + Hash + Send + Sync,
        Time: Copy + Ord + Send + Sync,
    > TAdjSet<V, Time>
{
    pub fn new(t: Time, v: V, e: AdjEdge) -> Self {
        Self::One(t, v, e)
    }

    pub fn len(&self) -> usize {
        match self {
            TAdjSet::Empty => 0,
            TAdjSet::One(_, _, _) => 1,
            TAdjSet::Small { vs, .. } => vs.len(),
            TAdjSet::Large { vs, .. } => vs.len(),
        }
    }

    pub fn len_window(&self, window: &Range<Time>) -> usize {
        match self {
            TAdjSet::Empty => 0,
            TAdjSet::One(t, _, _) => {
                if window.contains(t) {
                    1
                } else {
                    0
                }
            }
            TAdjSet::Small { t_index, .. } => t_index
                .range(window.clone())
                .map(|(_, bs)| bs.iter())
                .kmerge()
                .dedup()
                .count(),
            TAdjSet::Large { t_index, .. } => t_index
                .range(window.clone())
                .map(|(_, bs)| bs.iter())
                .kmerge()
                .dedup()
                .count(),
        }
    }

    pub fn push(&mut self, t: Time, v: V, e: AdjEdge) {
        match self.borrow() {
            TAdjSet::Empty => {
                *self = Self::new(t, v, e);
            }
            TAdjSet::One(t0, v0, e0) => {
                let vs = vec![v];
                let edges = vec![e];
                let t_index = SVM::from_iter(vec![(t, BitSet::one(v.into()))]);

                let mut new_set = TAdjSet::Small { vs, edges, t_index };
                new_set.push(*t0, *v0, *e0);

                *self = new_set;
            }
            TAdjSet::Small { vs, .. } => {
                if vs.len() < SMALL_SET {
                    if let TAdjSet::Small { vs, edges, t_index } = self {
                        if let Err(i) = vs.binary_search(&v) {
                            vs.insert(i, v);
                            edges.insert(i, e);
                        }
                        t_index
                            .entry(t)
                            .and_modify(|set| set.push(v.into()))
                            .or_insert(BitSet::one(v.into()));
                    }
                } else {
                    replace_with_or_abort(self, |_self| {
                        if let TAdjSet::Small { vs, edges, t_index } = _self {
                            let pairs: Vec<(V, AdjEdge)> =
                                vs.into_iter().zip(edges.into_iter()).collect();
                            let mut bt = BTreeMap::default();
                            for (t, bs) in t_index {
                                bt.insert(t, bs);
                            }
                            let mut entry = TAdjSet::Large {
                                vs: FxHashMap::from_iter(pairs),
                                t_index: bt,
                            };
                            entry.push(t, v, e);
                            entry
                        } else {
                            _self
                        }
                    });
                }
            }
            TAdjSet::Large { .. } => {
                if let TAdjSet::Large { vs, t_index } = self.borrow_mut() {
                    vs.entry(v).and_modify(|e0| *e0 = e).or_insert(e);
                    t_index
                        .entry(t)
                        .and_modify(|set| set.push(v.into()))
                        .or_insert(BitSet::one(v.into()));
                }
            }
        }
    }

    pub fn iter(&self) -> Box<dyn Iterator<Item = (&V, AdjEdge)> + Send + '_> {
        match self {
            TAdjSet::Empty => Box::new(std::iter::empty()),
            TAdjSet::One(_, v, e) => Box::new(std::iter::once((v, *e))),
            TAdjSet::Small { vs, edges, .. } => {
                Box::new(vs.iter().zip(Box::new(edges.iter().map(|e| *e))))
            }
            TAdjSet::Large { vs, .. } => Box::new(vs.iter().map(|(v, e)| (v, *e))),
        }
    }

    pub fn iter_window(
        &self,
        r: &Range<Time>,
    ) -> Box<dyn Iterator<Item = (V, AdjEdge)> + Send + '_> {
        match self {
            TAdjSet::Empty => Box::new(std::iter::empty()),
            TAdjSet::One(t, v, e) => {
                if r.contains(t) {
                    Box::new(std::iter::once((*v, *e)))
                } else {
                    Box::new(std::iter::empty())
                }
            }
            TAdjSet::Small { vs, edges, t_index } => {
                let iter = t_index
                    .range(r.clone())
                    .map(|(_, v_ids)| v_ids.iter())
                    .kmerge()
                    .dedup()
                    .flat_map(|v| {
                        let key = V::from(v);
                        vs.binary_search(&key).ok().map(|i| (V::from(v), edges[i]))
                    });

                Box::new(iter)
            }
            TAdjSet::Large { vs, t_index } => {
                let iter = t_index
                    .range(r.clone())
                    .map(|(_, v_ids)| v_ids.iter())
                    .kmerge()
                    .dedup()
                    .flat_map(|v| {
                        let key = V::from(v);
                        vs.get(&key).map(|e| (V::from(v), *e))
                    });

                Box::new(iter)
            }
        }
    }

    pub fn iter_window_t(
        &self,
        r: &Range<Time>,
    ) -> Box<dyn Iterator<Item = (V, Time, AdjEdge)> + Send + '_> {
        match self {
            TAdjSet::Empty => Box::new(std::iter::empty()),
            TAdjSet::One(t, v, e) => {
                if r.contains(t) {
                    Box::new(std::iter::once((*v, *t, *e)))
                } else {
                    Box::new(std::iter::empty())
                }
            }
            TAdjSet::Small { vs, edges, t_index } => {
                let iter = t_index.range(r.clone()).flat_map(|(t, v_ids)| {
                    // for all the vertices at time t
                    v_ids.iter().flat_map(|v| {
                        let key = V::from(v);
                        vs.binary_search(&key)
                            .ok()
                            .map(|i| (V::from(v), *t, edges[i])) // return the (vertex, time, edge_id)
                    })
                });

                Box::new(iter)
            }
            TAdjSet::Large { vs, t_index } => {
                let iter = t_index.range(r.clone()).flat_map(|(t, v_ids)| {
                    // for all the vertices at time t
                    v_ids.iter().flat_map(|v| {
                        let key = V::from(v);
                        vs.get(&key).map(|e| (V::from(v), *t, *e)) // return the (vertex, time, edge_id)
                    })
                });

                Box::new(iter)
            }
        }
    }

    pub fn find(&self, v: V) -> Option<AdjEdge> {
        match self {
            TAdjSet::Empty => None,
            TAdjSet::One(_, v0, e) => {
                if v0 == &v {
                    Some(*e)
                } else {
                    None
                }
            }
            TAdjSet::Small { vs, edges, .. } => match vs.binary_search(&v) {
                Ok(i) => edges.get(i).copied(),
                _ => None,
            },
            TAdjSet::Large { vs, .. } => vs.get(&v).map(|e| *e),
        }
    }

    pub fn find_window(&self, v: V, w: &Range<Time>) -> Option<AdjEdge> {
        self.iter_window(&w).find(|t| (*t).0 == v).map(|f| f.1)
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
        let mut ts: TAdjSet<usize, i64> = TAdjSet::default();

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
        let mut ts: TAdjSet<usize, i64> = TAdjSet::default();

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
        let mut ts: TAdjSet<usize, u64> = TAdjSet::default();

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
        let mut ts: TAdjSet<usize, i64> = TAdjSet::default();

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
        let mut ts: TAdjSet<usize, i64> = TAdjSet::default();

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
        let mut ts: TAdjSet<usize, i64> = TAdjSet::default();

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
            vec![(7, 3, AdjEdge::remote(1)), (1, 9, AdjEdge::local(0))];
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
