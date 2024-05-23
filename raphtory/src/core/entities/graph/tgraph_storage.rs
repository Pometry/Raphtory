use crate::core::{
    entities::{edges::edge_store::EdgeStore, nodes::node_store::NodeStore, EID, VID},
    storage::{self, Entry, EntryMut, PairEntryMut},
};
use serde::{Deserialize, Serialize};

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

    #[inline]
    pub(crate) fn get_edge(&self, id: EID) -> Entry<'_, EdgeStore> {
        self.edges.entry(id)
    }

    pub(crate) fn pair_node_mut(&self, i: VID, j: VID) -> PairEntryMut<'_, NodeStore> {
        self.nodes.pair_entry_mut(i, j)
    }
}
