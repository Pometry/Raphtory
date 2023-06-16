use std::{ops::Deref, rc::Rc};

use serde::{Deserialize, Serialize};

use crate:: storage::{ self, iter::RefT, Entry, EntryMut, PairEntryMut, } ;

use super::{edge_store::EdgeStore, node_store::NodeStore};

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct GraphStorage<const N: usize, L: lock_api::RawRwLock> {
    // node storage with having (id, time_index, properties, adj list for each layer)
    nodes: storage::RawStorage<NodeStore<N>, L, N>,

    // edge storage with having (src, dst, time_index, properties) for each layer
    edges: storage::RawStorage<EdgeStore<N>, L, N>,
}

impl<const N: usize, L: lock_api::RawRwLock> GraphStorage<N, L> {
    pub(crate) fn new() -> Self {
        Self {
            nodes: storage::RawStorage::new(),
            edges: storage::RawStorage::new(),
        }
    }

    pub(crate) fn push_node(&self, node: NodeStore<N>) -> usize {
        self.nodes.push(node)
    }

    pub(crate) fn push_edge(&self, edge: EdgeStore<N>) -> usize {
        self.edges.push(edge)
    }

    pub(crate) fn get_node_mut(&self, id: usize) -> EntryMut<'_, NodeStore<N>, L> {
        self.nodes.entry_mut(id)
    }

    pub(crate) fn get_edge_mut(&self, id: usize) -> EntryMut<'_, EdgeStore<N>, L> {
        self.edges.entry_mut(id)
    }

    pub(crate) fn get_node(&self, id: usize) -> Entry<'_, NodeStore<N>, L, N> {
        self.nodes.entry(id)
    }

    pub(crate) fn get_edge(&self, id: usize) -> Entry<'_, EdgeStore<N>, L, N> {
        self.edges.entry(id)
    }

    pub(crate) fn pair_node_mut(&self, i: usize, j: usize) -> PairEntryMut<'_, NodeStore<N>, L> {
        self.nodes.pair_entry_mut(i, j)
    }

    pub(crate) fn nodes_len(&self) -> usize {
        self.nodes.len()
    }

    pub(crate) fn edges_len(&self) -> usize {
        self.edges.len()
    }

    fn lock(&self) -> LockedGraphStorage<'_, N, L> {
        LockedGraphStorage::new(self) 
    }

    pub(crate) fn locked_nodes(&self) -> impl Iterator<Item = GraphEntry<'_, NodeStore<N>, L, N>> {
        LockedVIter {
            from: 0,
            to: self.nodes.len(),
            locked_gs: Rc::new(self.lock()),
        }
    }

    pub(crate) fn nodes(&self) -> impl Iterator<Item = RefT<'_, NodeStore<N>, L, N>> {
        self.nodes.iter()
    }
}

struct LockedVIter<'a, const N: usize, L: lock_api::RawRwLock> {
    from: usize,
    to: usize,
    locked_gs: Rc<LockedGraphStorage<'a, N, L>>,
}

impl<'a, const N: usize, L: lock_api::RawRwLock> Iterator for LockedVIter<'a, N, L> {
    type Item = GraphEntry<'a, NodeStore<N>, L, N>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.from < self.to {
            let node = Some(GraphEntry {
                locked_gs: self.locked_gs.clone(),
                i: self.from,
                _marker: std::marker::PhantomData,
            });
            self.from += 1;
            node
        } else {
            None
        }
    }
}

pub struct GraphEntry<'a, T, L: lock_api::RawRwLock, const N: usize> {
    locked_gs: Rc<LockedGraphStorage<'a, N, L>>,
    i: usize,
    _marker: std::marker::PhantomData<T>,
}


// impl new
impl<'a, const N: usize, L: lock_api::RawRwLock, T> GraphEntry<'a, T, L, N> {
    pub(crate) fn new(gs: Rc<LockedGraphStorage<'a, N, L>>,i: usize) -> Self {
        Self {
            locked_gs: gs,
            i,
            _marker: std::marker::PhantomData,
        }
    }

    pub(crate) fn index(&self) -> usize {
        self.i
    }

    pub(crate) fn locked_gs(&self) -> &Rc<LockedGraphStorage<'a, N, L>> {
        &self.locked_gs
    }
}

// impl Deref for RefY of NodeStore<N>
impl<'a, const N: usize, L: lock_api::RawRwLock> Deref for GraphEntry<'a, NodeStore<N>, L, N> {
    type Target = NodeStore<N>;

    fn deref(&self) -> &Self::Target {
        self.locked_gs.get_node(self.i)
    }
}

#[derive(Debug)]
pub(crate) struct LockedGraphStorage<'a, const N: usize, L: lock_api::RawRwLock> {
    nodes: storage::ReadLockedStorage<'a, NodeStore<N>, L, N>,
    _edges: storage::ReadLockedStorage<'a, EdgeStore<N>, L, N>,
}

impl<'a, const N: usize, L: lock_api::RawRwLock> LockedGraphStorage<'a, N, L> {
    pub(crate) fn new(storage: &'a GraphStorage<N, L>) -> Self {
        Self {
            nodes: storage.nodes.read_lock(),
            _edges: storage.edges.read_lock(),
        }
    }

    pub(crate) fn get_node(&'a self, id: usize) -> &'a NodeStore<N> {
        self.nodes.get(id)
    }

}
