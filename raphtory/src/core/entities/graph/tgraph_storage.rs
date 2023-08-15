use crate::core::{
    entities::{
        edges::{edge_ref::EdgeRef, edge_store::EdgeStore},
        vertices::vertex_store::VertexStore,
        LayerIds, EID,
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
    nodes: storage::RawStorage<VertexStore<N>, N>,

    // edge storage with having (src, dst, time_index, properties) for each layer
    pub(crate) edges: storage::RawStorage<EdgeStore<N>, N>,
}

impl<const N: usize> GraphStorage<N> {
    pub(crate) fn new() -> Self {
        Self {
            nodes: storage::RawStorage::new(),
            edges: storage::RawStorage::new(),
        }
    }

    pub(crate) fn push_node(&self, node: VertexStore<N>) -> usize {
        self.nodes.push(node, |vid, node| node.vid = vid.into())
    }

    pub(crate) fn push_edge(&self, edge: EdgeStore<N>) -> EID {
        self.edges
            .push(edge, |eid, edge| edge.eid = eid.into())
            .into()
    }

    pub(crate) fn get_node_mut(&self, id: usize) -> EntryMut<'_, VertexStore<N>> {
        self.nodes.entry_mut(id)
    }

    pub(crate) fn get_edge_mut(&self, id: EID) -> EntryMut<'_, EdgeStore<N>> {
        self.edges.entry_mut(id.into())
    }

    pub(crate) fn get_node(&self, id: usize) -> Entry<'_, VertexStore<N>, N> {
        self.nodes.entry(id)
    }

    pub(crate) fn get_node_arc(&self, id: usize) -> ArcEntry<VertexStore<N>, N> {
        self.nodes.entry_arc(id)
    }

    pub(crate) fn get_edge_arc(&self, id: usize) -> ArcEntry<EdgeStore<N>, N> {
        self.edges.entry_arc(id)
    }

    #[inline]
    pub(crate) fn get_edge(&self, id: usize) -> Entry<'_, EdgeStore<N>, N> {
        self.edges.entry(id)
    }

    pub(crate) fn pair_node_mut(&self, i: usize, j: usize) -> PairEntryMut<'_, VertexStore<N>> {
        self.nodes.pair_entry_mut(i, j)
    }

    pub(crate) fn nodes_len(&self) -> usize {
        self.nodes.len()
    }

    pub(crate) fn edges_len(&self, layers: LayerIds) -> usize {
        match layers {
            LayerIds::All => self.edges.len(),
            _ => self.edges.iter().filter(|e| e.has_layer(&layers)).count(),
        }
    }

    pub(crate) fn edges_window_len(&self, layers: LayerIds, w: Range<i64>) -> usize {
        self.edges.count_with_filter(|e|{
            e.active(&layers, w.clone())
        })
        // self.edges
        //     .iter()
        //     .par_bridge()
        //     .filter(|e| e.active(&layers, w.clone()))
        //     .count()
    }

    fn lock(&self) -> LockedGraphStorage<N> {
        LockedGraphStorage::new(self)
    }

    pub(crate) fn locked_nodes(&self) -> LockedIter<N, VertexStore<N>> {
        LockedIter {
            from: 0,
            to: self.nodes.len(),
            locked_gs: Arc::new(self.lock()),
            phantom: std::marker::PhantomData,
        }
    }

    pub(crate) fn locked_edges(&self) -> LockedIter<N, EdgeStore<N>> {
        LockedIter {
            from: 0,
            to: self.edges.len(),
            locked_gs: Arc::new(self.lock()),
            phantom: std::marker::PhantomData,
        }
    }

    pub(crate) fn edge_refs(&self) -> impl Iterator<Item = EdgeRef> + Send {
        let locked = self.edges.read_lock();
        (0..self.edges.len()).map(move |id| EdgeRef::from(locked.get(id)))
    }
}

pub(crate) struct LockedIter<const N: usize, T> {
    from: usize,
    to: usize,
    locked_gs: Arc<LockedGraphStorage<N>>,
    phantom: std::marker::PhantomData<T>,
}

impl<const N: usize> Iterator for LockedIter<N, VertexStore<N>> {
    type Item = GraphEntry<VertexStore<N>, N>;

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

impl<'a, const N: usize> Iterator for LockedIter<N, EdgeStore<N>> {
    type Item = GraphEntry<EdgeStore<N>, N>;

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

impl<'a, const N: usize> Deref for GraphEntry<VertexStore<N>, N> {
    type Target = VertexStore<N>;

    fn deref(&self) -> &Self::Target {
        self.locked_gs.get_node(self.i)
    }
}

impl<'a, const N: usize> Deref for GraphEntry<EdgeStore<N>, N> {
    type Target = EdgeStore<N>;

    fn deref(&self) -> &Self::Target {
        self.locked_gs.get_edge(self.i)
    }
}

#[derive(Debug)]
pub(crate) struct LockedGraphStorage<const N: usize> {
    nodes: storage::ReadLockedStorage<VertexStore<N>, N>,
    edges: storage::ReadLockedStorage<EdgeStore<N>, N>,
}

impl<const N: usize> LockedGraphStorage<N> {
    pub(crate) fn new(storage: &GraphStorage<N>) -> Self {
        Self {
            nodes: storage.nodes.read_lock(),
            edges: storage.edges.read_lock(),
        }
    }

    pub(crate) fn get_node(&self, id: usize) -> &VertexStore<N> {
        self.nodes.get(id)
    }

    pub(crate) fn get_edge(&self, id: usize) -> &EdgeStore<N> {
        self.edges.get(id)
    }
}
