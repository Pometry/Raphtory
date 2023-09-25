use crate::core::{
    entities::{
        edges::{edge_ref::EdgeRef, edge_store::EdgeStore},
        vertices::vertex_store::VertexStore,
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
pub(crate) struct GraphStorage<const N: usize> {
    // node storage with having (id, time_index, properties, adj list for each layer)
    pub(crate) nodes: storage::RawStorage<VertexStore, N>,

    // edge storage with having (src, dst, time_index, properties) for each layer
    pub(crate) edges: storage::RawStorage<EdgeStore, N>,
}

impl<const N: usize> GraphStorage<N> {
    pub(crate) fn new() -> Self {
        Self {
            nodes: storage::RawStorage::new(),
            edges: storage::RawStorage::new(),
        }
    }

    pub(crate) fn push_node(&self, node: VertexStore) -> VID {
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
    pub(crate) fn get_node_mut(&self, id: VID) -> EntryMut<'_, VertexStore> {
        self.nodes.entry_mut(id.into())
    }

    #[inline]
    pub(crate) fn get_edge_mut(&self, id: EID) -> EntryMut<'_, EdgeStore> {
        self.edges.entry_mut(id.into())
    }

    #[inline]
    pub(crate) fn get_node(&self, id: VID) -> Entry<'_, VertexStore, N> {
        self.nodes.entry(id.into())
    }

    pub(crate) fn get_node_arc(&self, id: VID) -> ArcEntry<VertexStore> {
        self.nodes.entry_arc(id.into())
    }

    pub(crate) fn get_edge_arc(&self, id: EID) -> ArcEntry<EdgeStore> {
        self.edges.entry_arc(id.into())
    }

    #[inline]
    pub(crate) fn get_edge(&self, id: EID) -> Entry<'_, EdgeStore, N> {
        self.edges.entry(id.into())
    }

    pub(crate) fn pair_node_mut(&self, i: VID, j: VID) -> PairEntryMut<'_, VertexStore> {
        self.nodes.pair_entry_mut(i.into(), j.into())
    }

    fn lock(&self) -> LockedGraphStorage<N> {
        LockedGraphStorage::new(self)
    }

    pub(crate) fn locked_nodes(&self) -> LockedIter<N, VertexStore> {
        LockedIter {
            from: 0,
            to: self.nodes.len(),
            locked_gs: Arc::new(self.lock()),
            phantom: std::marker::PhantomData,
        }
    }

    pub(crate) fn locked_edges(&self) -> impl Iterator<Item = ArcEntry<EdgeStore>> {
        self.edges.read_lock().into_iter()
    }

    pub(crate) fn edge_refs(&self) -> impl Iterator<Item = EdgeRef> + Send {
        self.edges
            .read_lock()
            .into_iter()
            .map(|entry| EdgeRef::from(entry))
    }
}

pub(crate) struct LockedIter<const N: usize, T> {
    from: usize,
    to: usize,
    locked_gs: Arc<LockedGraphStorage<N>>,
    phantom: std::marker::PhantomData<T>,
}

impl<const N: usize> Iterator for LockedIter<N, VertexStore> {
    type Item = GraphEntry<VertexStore, N>;

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

impl<'a, const N: usize> Iterator for LockedIter<N, EdgeStore> {
    type Item = GraphEntry<EdgeStore, N>;

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

pub struct GraphEntry<T, const N: usize> {
    locked_gs: Arc<LockedGraphStorage<N>>,
    i: usize,
    _marker: std::marker::PhantomData<T>,
}

// impl new
impl<'a, const N: usize, T> GraphEntry<T, N> {
    pub(crate) fn new(gs: Arc<LockedGraphStorage<N>>, i: usize) -> Self {
        Self {
            locked_gs: gs,
            i,
            _marker: std::marker::PhantomData,
        }
    }

    pub(crate) fn index(&self) -> usize {
        self.i
    }

    pub(crate) fn locked_gs(&self) -> &Arc<LockedGraphStorage<N>> {
        &self.locked_gs
    }
}

impl<'a, const N: usize> Deref for GraphEntry<VertexStore, N> {
    type Target = VertexStore;

    fn deref(&self) -> &Self::Target {
        self.locked_gs.get_node(self.i)
    }
}

impl<'a, const N: usize> Deref for GraphEntry<EdgeStore, N> {
    type Target = EdgeStore;

    fn deref(&self) -> &Self::Target {
        self.locked_gs.get_edge(self.i)
    }
}

#[derive(Debug)]
pub(crate) struct LockedGraphStorage<const N: usize> {
    nodes: storage::ReadLockedStorage<VertexStore, N>,
    edges: storage::ReadLockedStorage<EdgeStore, N>,
}

impl<const N: usize> LockedGraphStorage<N> {
    pub(crate) fn new(storage: &GraphStorage<N>) -> Self {
        Self {
            nodes: storage.nodes.read_lock(),
            edges: storage.edges.read_lock(),
        }
    }

    pub(crate) fn get_node(&self, id: usize) -> &VertexStore {
        self.nodes.get(id)
    }

    pub(crate) fn get_edge(&self, id: usize) -> &EdgeStore {
        self.edges.get(id)
    }
}
