//! A data structure for efficiently storing and querying the temporal adjacency set of a node in a temporal graph.

use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, hash::Hash};

const SMALL_SET: usize = 1024;

/**
 * Temporal adjacency set can track when adding edge v -> u
 * does u exist already
 * and if it does what is the edge metadata
 * and if the edge is remote or local
 *
 *  */
#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
pub enum AdjSet<K: Ord + Copy + Hash + Send + Sync, V: Into<usize> + Copy + Send + Sync> {
    #[default]
    Empty,
    One(K, V),
    Small {
        vs: Vec<K>,    // the neighbours
        edges: Vec<V>, // edge metadata
    },
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
            AdjSet::Small { vs, .. } => vs.len(),
            AdjSet::Large { vs } => vs.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            AdjSet::Empty => true,
            AdjSet::One(_, _) => false,
            AdjSet::Small { vs, .. } => vs.is_empty(),
            AdjSet::Large { vs } => vs.is_empty(),
        }
    }
    pub fn new(v: K, e: V) -> Self {
        Self::One(v, e)
    }

    pub fn push(&mut self, v: K, e: V) {
        match self {
            AdjSet::Empty => {
                *self = Self::new(v, e);
            }
            AdjSet::One(vv, ee) => {
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
            AdjSet::Small { vs, edges } => match vs.binary_search(&v) {
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
            AdjSet::Large { vs } => {
                vs.insert(v, e);
            }
        }
    }

    pub fn iter(&self) -> Box<dyn Iterator<Item = (K, V)> + Send + '_> {
        match self {
            AdjSet::Empty => Box::new(std::iter::empty()),
            AdjSet::One(v, e) => Box::new(std::iter::once((*v, *e))),
            AdjSet::Small { vs, edges } => Box::new(vs.iter().copied().zip(edges.iter().copied())),
            AdjSet::Large { vs } => Box::new(vs.iter().map(|(k, v)| (*k, *v))),
        }
    }

    pub fn vertices(&self) -> Box<dyn Iterator<Item = K> + Send + '_> {
        match self {
            AdjSet::Empty => Box::new(std::iter::empty()),
            AdjSet::One(v, ..) => Box::new(std::iter::once(*v)),
            AdjSet::Small { vs, .. } => Box::new(vs.iter().copied()),
            AdjSet::Large { vs } => Box::new(vs.keys().copied()),
        }
    }

    pub fn find(&self, v: K) -> Option<V> {
        match self {
            AdjSet::Empty => None,
            AdjSet::One(vv, e) => (*vv == v).then_some(*e),
            AdjSet::Small { vs, edges } => vs.binary_search(&v).ok().map(|i| edges[i]),
            AdjSet::Large { vs } => vs.get(&v).copied(),
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
            AdjSet::Small { vs, edges } => {
                if let Some(l) = last {
                    let i = match vs.binary_search(&l) {
                        Ok(i) => i + 1,
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

    #[quickcheck]
    fn insert_fuzz(input: Vec<usize>) -> bool {
        let mut ts: AdjSet<usize, usize> = AdjSet::default();

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
