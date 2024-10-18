use std::{borrow::Borrow, collections::HashMap, hash::Hash, sync::Arc};
use either::Either;
use raphtory_api::core::entities::{EID, VID};
use rayon::prelude::*;

#[derive(Clone, Debug)]
pub struct Index<K> {
    pub keys: Arc<[K]>,
    pub map: Arc<HashMap<K, usize>>,
}

impl<K: Copy + Hash + Eq> From<Vec<K>> for Index<K> {
    fn from(keys: Vec<K>) -> Self {
        let map = keys
            .iter()
            .copied()
            .enumerate()
            .map(|(i, k)| (k, i))
            .collect();
        Self {
            keys: keys.into(),
            map: Arc::new(map),
        }
    }
}

impl<K: Copy + Hash + Eq + Send + Sync> Index<K> {
    pub fn iter(&self) -> impl Iterator<Item = &K> + '_ {
        self.keys.iter()
    }

    pub fn into_par_iter(self) -> impl IndexedParallelIterator<Item = K> {
        let keys = self.keys;
        (0..keys.len()).into_par_iter().map(move |i| keys[i])
    }

    pub fn into_iter(self) -> impl Iterator<Item = K> {
        let keys = self.keys;
        (0..keys.len()).map(move |i| keys[i])
    }

    pub fn index<Q: ?Sized>(&self, key: &Q) -> Option<usize>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.map.get(key).copied()
    }

    pub fn key(&self, index: usize) -> Option<K> {
        self.keys.get(index).copied()
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn contains<Q: ?Sized>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.map.contains_key(key)
    }
}

impl<K: Copy + Hash + Eq + Send + Sync> Index<K> {
    pub fn par_iter(&self) -> impl IndexedParallelIterator<Item = &K> + '_ {
        self.keys.par_iter()
    }
}


#[derive(Debug, Clone)]
pub enum NodeList {
    All { num_nodes: usize },
    List { nodes: Index<VID> },
}

impl NodeList {
    pub fn par_iter(&self) -> impl IndexedParallelIterator<Item = VID> + '_ {
        match self {
            NodeList::All { num_nodes } => Either::Left((0..*num_nodes).into_par_iter().map(VID)),
            NodeList::List { nodes } => Either::Right(nodes.par_iter().copied()),
        }
    }

    pub fn into_par_iter(self) -> impl IndexedParallelIterator<Item = VID> {
        match self {
            NodeList::All { num_nodes } => Either::Left((0..num_nodes).into_par_iter().map(VID)),
            NodeList::List { nodes } => Either::Right(
                (0..nodes.len())
                    .into_par_iter()
                    .map(move |i| nodes.key(i).unwrap()),
            ),
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = VID> + '_ {
        match self {
            NodeList::All { num_nodes } => Either::Left((0..*num_nodes).map(VID)),
            NodeList::List { nodes } => Either::Right(nodes.iter().copied()),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            NodeList::All { num_nodes } => *num_nodes,
            NodeList::List { nodes } => nodes.len(),
        }
    }
}

impl IntoIterator for NodeList {
    type Item = VID;
    type IntoIter = Box<dyn Iterator<Item = Self::Item> + Send + Sync>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            NodeList::All { num_nodes } => Box::new((0..num_nodes).map(VID)),
            NodeList::List { nodes } => Box::new(nodes.into_iter()),
        }
    }
}

#[derive(Clone)]
pub enum EdgeList {
    All { num_edges: usize },
    List { edges: Arc<[EID]> },
}

impl EdgeList {
    pub fn par_iter(&self) -> impl IndexedParallelIterator<Item = EID> + '_ {
        match self {
            EdgeList::All { num_edges } => Either::Left((0..*num_edges).into_par_iter().map(EID)),
            EdgeList::List { edges } => Either::Right(edges.par_iter().copied()),
        }
    }

    pub fn into_par_iter(self) -> impl IndexedParallelIterator<Item = EID> {
        match self {
            EdgeList::All { num_edges } => Either::Left((0..num_edges).into_par_iter().map(EID)),
            EdgeList::List { edges } => {
                Either::Right((0..edges.len()).into_par_iter().map(move |i| edges[i]))
            }
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = EID> + '_ {
        match self {
            EdgeList::All { num_edges } => Either::Left((0..*num_edges).map(EID)),
            EdgeList::List { edges } => Either::Right(edges.iter().copied()),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            EdgeList::All { num_edges } => *num_edges,
            EdgeList::List { edges } => edges.len(),
        }
    }
}

impl IntoIterator for EdgeList {
    type Item = EID;
    type IntoIter = Box<dyn Iterator<Item = Self::Item> + Send + Sync>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            EdgeList::All { num_edges } => Box::new((0..num_edges).map(EID)),
            EdgeList::List { edges } => Box::new((0..edges.len()).map(move |i| edges[i])),
        }
    }
}
