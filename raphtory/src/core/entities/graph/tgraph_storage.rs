use crate::core::{
    entities::{
        edges::{edge_ref::EdgeRef, edge_store::EdgeStore},
        nodes::node_store::NodeStore,
        LayerIds, EID, VID,
    },
    storage::{self, ArcEntry, Entry, EntryMut, PairEntryMut},
    Direction,
};
use rayon::prelude::{ParallelBridge, ParallelIterator};
use serde::{Deserialize, Serialize};
use std::{
    ops::{Deref, Range},
    sync::Arc,
};

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub(crate) struct GraphStorage {
    // node storage with having (id, time_index, properties, adj list for each layer)
    pub(crate) nodes: storage::RawStorage<NodeStore, VID>,

    // edge storage with having (src, dst, time_index, properties) for each layer
    pub(crate) edges: storage::RawStorage<EdgeStore, EID>,
}

impl GraphStorage {
    pub(crate) fn new(num_locks: usize) -> Self {
        Self {
            nodes: storage::RawStorage::new(num_locks),
            edges: storage::RawStorage::new(num_locks),
        }
    }

    pub(crate) fn push_node(&self, node: NodeStore) -> VID {
        self.nodes
            .push(node, |vid, node| node.vid = vid.into())
            .into()
    }

    pub(crate) fn push_edge(&self, edge: EdgeStore) -> EID {
        self.edges
            .push(edge, |eid, edge| edge.eid = eid.into())
            .into()
    }

    #[inline]
    pub(crate) fn get_node_mut(&self, id: VID) -> EntryMut<'_, NodeStore> {
        self.nodes.entry_mut(id)
    }

    #[inline]
    pub(crate) fn get_edge_mut(&self, id: EID) -> EntryMut<'_, EdgeStore> {
        self.edges.entry_mut(id)
    }

    #[inline]
    pub(crate) fn get_node(&self, id: VID) -> Entry<'_, NodeStore> {
        self.nodes.entry(id)
    }

    pub(crate) fn get_node_arc(&self, id: VID) -> ArcEntry<NodeStore> {
        self.nodes.entry_arc(id)
    }

    pub(crate) fn get_edge_arc(&self, id: EID) -> ArcEntry<EdgeStore> {
        self.edges.entry_arc(id)
    }

    #[inline]
    pub(crate) fn get_edge(&self, id: EID) -> Entry<'_, EdgeStore> {
        self.edges.entry(id)
    }

    pub(crate) fn pair_node_mut(&self, i: VID, j: VID) -> PairEntryMut<'_, NodeStore> {
        self.nodes.pair_entry_mut(i, j)
    }

    pub(crate) fn edge_refs(&self) -> impl Iterator<Item = EdgeRef> + Send {
        self.edges.read_lock().into_iter().map(EdgeRef::from)
    }
}

pub(crate) struct LockedIter<T> {
    from: usize,
    to: usize,
    locked_gs: Arc<LockedGraphStorage>,
    phantom: std::marker::PhantomData<T>,
}

impl Iterator for LockedIter<NodeStore> {
    type Item = GraphEntry<NodeStore>;

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

impl<'a> Iterator for LockedIter<EdgeStore> {
    type Item = GraphEntry<EdgeStore>;

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

pub struct GraphEntry<T> {
    locked_gs: Arc<LockedGraphStorage>,
    i: usize,
    _marker: std::marker::PhantomData<T>,
}

// impl new
impl<'a, T> GraphEntry<T> {
    pub(crate) fn new(gs: Arc<LockedGraphStorage>, i: usize) -> Self {
        Self {
            locked_gs: gs,
            i,
            _marker: std::marker::PhantomData,
        }
    }

    pub(crate) fn index(&self) -> usize {
        self.i
    }

    pub(crate) fn locked_gs(&self) -> &Arc<LockedGraphStorage> {
        &self.locked_gs
    }
}

impl Deref for GraphEntry<NodeStore> {
    type Target = NodeStore;

    fn deref(&self) -> &Self::Target {
        self.locked_gs.get_node(VID(self.i))
    }
}

impl Deref for GraphEntry<EdgeStore> {
    type Target = EdgeStore;

    fn deref(&self) -> &Self::Target {
        self.locked_gs.get_edge(EID(self.i))
    }
}

#[derive(Debug)]
pub(crate) struct LockedGraphStorage {
    nodes: storage::ReadLockedStorage<NodeStore, VID>,
    edges: storage::ReadLockedStorage<EdgeStore, EID>,
}

impl LockedGraphStorage {
    pub(crate) fn new(storage: &GraphStorage) -> Self {
        Self {
            nodes: storage.nodes.read_lock(),
            edges: storage.edges.read_lock(),
        }
    }

    pub(crate) fn get_node(&self, id: VID) -> &NodeStore {
        self.nodes.get(id)
    }

    pub(crate) fn get_edge(&self, id: EID) -> &EdgeStore {
        self.edges.get(id)
    }
}
