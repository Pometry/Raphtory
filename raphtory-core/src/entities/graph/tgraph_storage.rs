use crate::{
    entities::{edges::edge_store::EdgeStore, nodes::node_store::NodeStore, EID, VID},
    storage::{
        self,
        raw_edges::{EdgeRGuard, EdgeWGuard, EdgesStorage, LockedEdges, UninitialisedEdge},
        EntryMut, NodeEntry, NodeSlot, NodeStorage, PairEntryMut, UninitialisedEntry,
    },
};
use parking_lot::RwLockWriteGuard;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct GraphStorage {
    // node storage with having (id, time_index, properties, adj list for each layer)
    pub nodes: NodeStorage,
    pub edges: EdgesStorage,
}

impl GraphStorage {
    pub fn new(num_locks: usize) -> Self {
        Self {
            nodes: storage::NodeStorage::new(num_locks),
            edges: EdgesStorage::new(num_locks),
        }
    }

    pub fn num_shards(&self) -> usize {
        self.nodes.data.len()
    }

    #[inline]
    pub fn nodes_read_lock(&self) -> storage::ReadLockedStorage {
        self.nodes.read_lock()
    }

    #[inline]
    pub fn edges_read_lock(&self) -> LockedEdges {
        self.edges.read_lock()
    }

    #[inline]
    pub fn nodes_len(&self) -> usize {
        self.nodes.len()
    }

    #[inline]
    pub fn edges_len(&self) -> usize {
        self.edges.len()
    }

    #[inline]
    pub fn push_node(&self, node: NodeStore) -> UninitialisedEntry<'_, NodeStore, NodeSlot> {
        self.nodes.push(node)
    }
    #[inline]
    pub fn push_edge(&self, edge: EdgeStore) -> UninitialisedEdge<'_> {
        self.edges.push(edge)
    }

    #[inline]
    pub fn get_node_mut(&self, id: VID) -> EntryMut<'_, RwLockWriteGuard<'_, NodeSlot>> {
        self.nodes.entry_mut(id)
    }

    #[inline]
    pub fn get_edge_mut(&self, eid: EID) -> EdgeWGuard<'_> {
        self.edges.get_edge_mut(eid)
    }

    #[inline]
    pub fn get_node(&self, id: VID) -> NodeEntry<'_> {
        self.nodes.entry(id)
    }

    #[inline]
    pub fn edge_entry(&self, eid: EID) -> EdgeRGuard<'_> {
        self.edges.get_edge(eid)
    }

    pub fn try_edge_entry(&self, eid: EID) -> Option<EdgeRGuard<'_>> {
        self.edges.try_get_edge(eid)
    }

    #[inline]
    pub fn pair_node_mut(&self, i: VID, j: VID) -> PairEntryMut<'_> {
        self.nodes.loop_pair_entry_mut(i, j)
    }
}
