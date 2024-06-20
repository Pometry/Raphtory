use crate::{
    core::{
        entities::edges::edge_store::EdgeStore,
        storage::raw_edges::{EdgeArcGuard, EdgeRGuard, EdgeWGuard, EdgesShard, LockedEdgesShard},
    },
    db::api::storage::edges::edge_storage_ops::MemEdge,
};
use raphtory_api::core::entities::EID;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub(crate) struct EdgesStorage {
    edges: EdgesShard,
}

#[derive(Debug)]
pub struct LockedEdges {
    edges: LockedEdgesShard,
}

impl LockedEdges {
    pub fn iter(&self) -> impl Iterator<Item = MemEdge> + '_ {
        self.edges.iter()
    }

    pub fn par_iter(&self) -> impl rayon::iter::ParallelIterator<Item = MemEdge> + '_ {
        self.edges.par_iter()
    }

    #[inline]
    pub fn get(&self, id: EID) -> MemEdge {
        self.edges.get_mem(id)
    }

    pub fn len(&self) -> usize {
        self.edges.len()
    }
}

impl EdgesStorage {
    pub(crate) fn new() -> Self {
        Self {
            edges: EdgesShard::new(),
        }
    }

    pub fn read_lock(&self) -> LockedEdges {
        LockedEdges {
            edges: self.edges.lock(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.edges.len()
    }

    pub(crate) fn push(&self, edge: EdgeStore) -> EdgeWGuard {
        let (eid, mut edge) = self.edges.push(edge);
        edge.edge_store_mut().eid = eid;
        edge
    }

    #[inline]
    pub(crate) fn entry(&self, id: EID) -> EdgeRGuard {
        self.edges.get_edge(id)
    }

    #[inline]
    pub(crate) fn entry_mut(&self, id: EID) -> EdgeWGuard {
        self.edges.get_edge_mut(id)
    }

    #[inline]
    pub(crate) fn entry_arc(&self, id: EID) -> EdgeArcGuard {
        self.edges.get_edge_arc(id)
    }
}
