use std::{ops::Deref, rc::Rc, sync::Arc};

use serde::{Deserialize, Serialize};

use crate::{
    core::Direction,
    storage::{self, iter::RefT, ArcEntry, Entry, EntryMut, PairEntryMut},
};

use super::{edge_store::EdgeStore, node_store::NodeStore};

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub(crate) struct GraphStorage<const N: usize> {
    // node storage with having (id, time_index, properties, adj list for each layer)
    nodes: storage::RawStorage<NodeStore<N>, N>,

    // edge storage with having (src, dst, time_index, properties) for each layer
    edges: storage::RawStorage<EdgeStore<N>, N>,
}

impl<const N: usize> GraphStorage<N> {
    pub(crate) fn new() -> Self {
        Self {
            nodes: storage::RawStorage::new(),
            edges: storage::RawStorage::new(),
        }
    }

    pub(crate) fn push_node(&self, node: NodeStore<N>) -> usize {
        self.nodes.push(node, |vid, node| node.vid = vid.into())
    }

    pub(crate) fn push_edge(&self, edge: EdgeStore<N>) -> usize {
        self.edges.push(edge, |eid, edge| edge.eid = eid.into())
    }

    pub(crate) fn get_node_mut(&self, id: usize) -> EntryMut<'_, NodeStore<N>> {
        self.nodes.entry_mut(id)
    }

    pub(crate) fn get_edge_mut(&self, id: usize) -> EntryMut<'_, EdgeStore<N>> {
        self.edges.entry_mut(id)
    }

    pub(crate) fn get_node(&self, id: usize) -> Entry<'_, NodeStore<N>, N> {
        self.nodes.entry(id)
    }

    pub(crate) fn get_node_arc(&self, id: usize) -> ArcEntry<NodeStore<N>, N> {
        self.nodes.entry_arc(id)
    }

    pub(crate) fn get_edge(&self, id: usize) -> Entry<'_, EdgeStore<N>, N> {
        self.edges.entry(id)
    }

    pub(crate) fn pair_node_mut(&self, i: usize, j: usize) -> PairEntryMut<'_, NodeStore<N>> {
        self.nodes.pair_entry_mut(i, j)
    }

    pub(crate) fn nodes_len(&self) -> usize {
        self.nodes.len()
    }

    pub(crate) fn edges_len(&self, layer: Option<usize>) -> usize {
        match layer {
            None => self.edges.len(),
            Some(layer_id) => self.nodes.iter().fold(0, |len, node| {
                len + node.edge_tuples(Some(layer_id), Direction::OUT).count()
            }),
        }
    }

    fn lock(&self) -> LockedGraphStorage<'_, N> {
        LockedGraphStorage::new(self)
    }

    pub(crate) fn locked_nodes(&self) -> impl Iterator<Item = GraphEntry<'_, NodeStore<N>, N>> {
        LockedVIter {
            from: 0,
            to: self.nodes.len(),
            locked_gs: Arc::new(self.lock()),
        }
    }

    pub(crate) fn nodes(&self) -> impl Iterator<Item = RefT<'_, NodeStore<N>, N>> {
        self.nodes.iter()
    }
}

struct LockedVIter<'a, const N: usize> {
    from: usize,
    to: usize,
    locked_gs: Arc<LockedGraphStorage<'a, N>>,
}

impl<'a, const N: usize> Iterator for LockedVIter<'a, N> {
    type Item = GraphEntry<'a, NodeStore<N>, N>;

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

pub struct GraphEntry<'a, T, const N: usize> {
    locked_gs: Arc<LockedGraphStorage<'a, N>>,
    i: usize,
    _marker: std::marker::PhantomData<T>,
}

// impl new
impl<'a, const N: usize, T> GraphEntry<'a, T, N> {
    pub(crate) fn new(gs: Arc<LockedGraphStorage<'a, N>>, i: usize) -> Self {
        Self {
            locked_gs: gs,
            i,
            _marker: std::marker::PhantomData,
        }
    }

    pub(crate) fn index(&self) -> usize {
        self.i
    }

    pub(crate) fn locked_gs(&self) -> &Arc<LockedGraphStorage<'a, N>> {
        &self.locked_gs
    }
}

// impl Deref for RefY of NodeStore<N>
impl<'a, const N: usize> Deref for GraphEntry<'a, NodeStore<N>, N> {
    type Target = NodeStore<N>;

    fn deref(&self) -> &Self::Target {
        self.locked_gs.get_node(self.i)
    }
}

#[derive(Debug)]
pub(crate) struct LockedGraphStorage<'a, const N: usize> {
    nodes: storage::ReadLockedStorage<'a, NodeStore<N>, N>,
    edges: storage::ReadLockedStorage<'a, EdgeStore<N>, N>,
}

impl<'a, const N: usize> LockedGraphStorage<'a, N> {
    pub(crate) fn new(storage: &'a GraphStorage<N>) -> Self {
        Self {
            nodes: storage.nodes.read_lock(),
            edges: storage.edges.read_lock(),
        }
    }

    pub(crate) fn get_node(&'a self, id: usize) -> &'a NodeStore<N> {
        self.nodes.get(id)
    }

    pub(crate) fn get_edge(&'a self, id: usize) -> &'a EdgeStore<N> {
        self.edges.get(id)
    }
}
