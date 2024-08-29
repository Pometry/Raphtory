use crate::core::{
    entities::{edges::edge_store::EdgeStore, nodes::node_store::NodeStore, EID, VID},
    storage::{
        self,
        raw_edges::{
            EdgeArcGuard, EdgeRGuard, EdgeWGuard, EdgesStorage, LockedEdges, UninitialisedEdge,
        },
        Entry, EntryMut, PairEntryMut, UninitialisedEntry,
    },
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub(crate) struct GraphStorage {
    // node storage with having (id, time_index, properties, adj list for each layer)
    pub(crate) nodes: storage::RawStorage<NodeStore, VID>,

    pub(crate) edges: EdgesStorage,
}

impl GraphStorage {
    pub(crate) fn new(num_locks: usize) -> Self {
        Self {
            nodes: storage::RawStorage::new(num_locks),
            edges: EdgesStorage::new(),
        }
    }

    #[inline]
    pub fn nodes_read_lock(&self) -> storage::ReadLockedStorage<NodeStore, VID> {
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
    pub(crate) fn push_node(&self, node: NodeStore) -> UninitialisedEntry<NodeStore> {
        self.nodes.push(node, |vid, node| node.vid = vid.into())
    }
    #[inline]
    pub(crate) fn push_edge(&self, edge: EdgeStore) -> UninitialisedEdge {
        self.edges.push(edge)
    }

    #[inline]
    pub(crate) fn get_node_mut(&self, id: VID) -> EntryMut<'_, NodeStore> {
        self.nodes.entry_mut(id)
    }

    #[inline]
    pub(crate) fn get_edge_mut(&self, eid: EID) -> EdgeWGuard {
        self.edges.get_edge_mut(eid)
    }

    #[inline]
    pub(crate) fn get_node(&self, id: VID) -> Entry<'_, NodeStore> {
        self.nodes.entry(id)
    }

    #[inline]
    pub(crate) fn edge_entry(&self, eid: EID) -> EdgeRGuard {
        self.edges.get_edge(eid)
    }

    #[inline]
    pub(crate) fn get_edge_arc(&self, eid: EID) -> EdgeArcGuard {
        self.edges.get_edge_arc(eid)
    }

    #[inline]
    pub(crate) fn pair_node_mut(&self, i: VID, j: VID) -> PairEntryMut<'_, NodeStore> {
        self.nodes.pair_entry_mut(i, j)
    }
}
