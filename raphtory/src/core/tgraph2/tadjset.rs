//! A data structure for efficiently storing and querying the temporal adjacency set of a node in a temporal graph.

use crate::core::timeindex::TimeIndex;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, hash::Hash, ops::Range};

const SMALL_SET: usize = 1024;

/**
 * Temporal adjacency set can track when adding edge v -> u
 * does u exist already
 * and if it does what is the edge metadata
 * and if the edge is remote or local
 *
 *  */
#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
pub enum TAdjSet<K: Ord + Copy + Hash + Send + Sync, V: Into<usize>+ Copy + Send + Sync> {
    #[default]
    Empty,
    One(K, V),
    Small {
        vs: Vec<K>,        // the neighbours
        edges: Vec<V>, // edge metadata
    },
    Large {
        vs: BTreeMap<K, V>, // this is equiv to vs and edges
    },
}

impl<K: Ord + Copy + Hash + Send + Sync, V: Into<usize> + Copy + Send + Sync> TAdjSet<K, V> {
    pub fn new(v: K, e: V) -> Self {
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
                let i:usize = (*e).into();
                if timestamps[i].active(window.clone()) {
                    1
                } else {
                    0
                }
            }

            TAdjSet::Small { edges, .. } => edges
                .iter()
                .filter(|&&e| timestamps[e.into()].active(window.clone()))
                .count(),
            TAdjSet::Large { vs } => vs
                .values()
                .filter(|&&e| timestamps[e.into()].active(window.clone()))
                .count(),
        }
    }

    pub fn push(&mut self, v: K, e: V) {
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

    pub fn iter(&self) -> Box<dyn Iterator<Item = (K, V)> + Send + '_> {
        match self {
            TAdjSet::Empty => Box::new(std::iter::empty()),
            TAdjSet::One(v, e) => Box::new(std::iter::once((*v, *e))),
            TAdjSet::Small { vs, edges } => Box::new(vs.iter().copied().zip(edges.iter().copied())),
            TAdjSet::Large { vs } => Box::new(vs.iter().map(|(k, v)| (*k, *v))),
        }
    }

    pub fn vertices(&self) -> Box<dyn Iterator<Item = K> + Send + '_> {
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
    ) -> Box<dyn Iterator<Item = (K, V)> + Send + 'a> {
        let w = window.clone();
        Box::new(
            self.iter()
                .filter(move |(_, e)| {
                    let i:usize = (*e).into();
                     timestamps[i].active(w.clone()) 
                    }),
        )
    }

    pub fn vertices_window<'a>(
        &'a self,
        timestamps: &'a [TimeIndex],
        window: &Range<i64>,
    ) -> Box<dyn Iterator<Item = K> + Send + 'a> {
        let w = window.clone();
        Box::new(
            self.iter()
                .filter(move |(_, e)| timestamps[(*e).into() as usize].active(w.clone()))
                .map(|(v, _)| v),
        )
    }

    pub fn find(&self, v: K) -> Option<V> {
        match self {
            TAdjSet::Empty => None,
            TAdjSet::One(vv, e) => (*vv == v).then_some(*e),
            TAdjSet::Small { vs, edges } => vs.binary_search(&v).ok().map(|i| edges[i]),
            TAdjSet::Large { vs } => vs.get(&v).copied(),
        }
    }

    pub fn get_page_vec(&self, last: Option<K>, page_size: usize) -> Vec<(K, V)> {
        match self {
            TAdjSet::Empty => vec![],
            TAdjSet::One(v, i) => {
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
            TAdjSet::Small { vs, edges } => {
                if let Some(l) = last {

                    let i = match vs.binary_search(&l) {
                        Ok(i) => i+1,
                        Err(i) => i,
                    };

                    if i >= vs.len() {
                        return vec![];
                    }

                    vs[i..]
                        .iter()
                        .zip(edges[i..].iter())
                        .take(page_size)
                        .map(|(a, b)| (*a, *b))
                        .collect()
                } else {
                    vs.iter()
                        .zip(edges.iter())
                        .take(page_size)
                        .map(|(a, b)| (*a, *b))
                        .collect()
                }
            },
            TAdjSet::Large { vs } => {
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

    #[quickcheck]
    fn insert_fuzz(input: Vec<usize>) -> bool {
        let mut ts: TAdjSet<usize, usize> = TAdjSet::default();

        for (e, i) in input.iter().enumerate() {
            ts.push(*i, e);
        }

        let res = input.iter().all(|i| ts.find(*i).is_some());
        if !res {
            let ts_vec: Vec<(usize, usize)> = ts.iter().collect();
            println!("Input: {:?}", input);
            println!("TAdjSet: {:?}", ts_vec);
        }
        res
    }

    #[test]
    fn insert() {
        let mut ts: TAdjSet<usize, usize> = TAdjSet::default();

        ts.push(7, 5);
        let actual = ts.iter().collect::<Vec<_>>();
        let expected: Vec<(usize, usize)> = vec![(7, 5)];
        assert_eq!(actual, expected)
    }

    #[test]
    fn insert_large() {
        let mut ts: TAdjSet<usize, usize> = TAdjSet::default();

        for i in 0..SMALL_SET + 2 {
            ts.push(i, i);
        }

        for i in 0..SMALL_SET + 2 {
            assert_eq!(ts.find(i), Some(i));
        }
    }

    #[test]
    fn insert_twice() {
        let mut ts: TAdjSet<usize, usize> = TAdjSet::default();

        ts.push(7, 9);
        ts.push(7, 9);

        let actual = ts.iter().collect::<Vec<_>>();
        let expected: Vec<(usize, usize)> = vec![(7, 9)];
        assert_eq!(actual, expected);
    }

    #[test]
    fn insert_two_different() {
        let mut ts: TAdjSet<usize, usize> = TAdjSet::default();

        ts.push(1, 0);
        ts.push(7, 1);

        let actual = ts.iter().collect::<Vec<_>>();
        let expected: Vec<(usize, usize)> = vec![(1, 0), (7, 1)];
        assert_eq!(actual, expected);
    }
}
