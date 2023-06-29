#![allow(unused)]

use std::ops::Deref;

use serde::{Deserialize, Serialize};

use self::{edge::ERef, node_store::NodeStore, tgraph::TGraph, tgraph_storage::GraphEntry};

use super::{storage::Entry, vertex_ref::VertexRef, Direction};

mod adj;
pub mod adjset;
pub(crate) mod edge;
mod edge_store;
mod graph_props;
mod iter;
mod node_store;
pub mod props;
pub mod tgraph;
mod tgraph_storage;
pub(crate) mod timer;
mod vertex;

// the only reason this is public is because the phisical ids of the vertices don't move
#[repr(transparent)]
#[derive(
    Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize, Serialize, Default,
)]
pub struct VID(usize);

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct LocalID {
    pub(crate) bucket: usize,
    pub(crate) offset: usize,
}

impl From<usize> for VID {
    fn from(id: usize) -> Self {
        VID(id)
    }
}

impl From<VID> for usize {
    fn from(id: VID) -> Self {
        id.0
    }
}

impl From<VertexRef> for VID {
    fn from(id: VertexRef) -> Self {
        match id {
            VertexRef::Local(vid) => vid,
            _ => panic!("Cannot convert remote vertex reference to VID"),
        }
    }
}

#[repr(transparent)]
#[derive(
    Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize, Serialize, Default,
)]
pub struct EID(usize);

impl From<EID> for usize {
    fn from(id: EID) -> Self {
        id.0
    }
}

impl From<usize> for EID {
    fn from(id: usize) -> Self {
        EID(id)
    }
}

pub(crate) enum VRef<'a, const N: usize> {
    Entry(Entry<'a, NodeStore<N>, N>), // returned from graph.vertex
    LockedEntry(GraphEntry<NodeStore<N>, N>), // returned from locked_vertices
}

// return index -> usize for VRef
impl<'a, const N: usize> VRef<'a, N> {
    fn index(&'a self) -> usize {
        match self {
            VRef::Entry(e) => e.index(),
            VRef::LockedEntry(ge) => ge.index(),
        }
    }

    fn edge_ref(&self, edge_id: EID, graph: &'a TGraph<N>) -> ERef<'a, N> {
        match self {
            VRef::Entry(_) => ERef::ERef(graph.edge_entry(edge_id)),
            VRef::LockedEntry(ge) => ERef::ELock {
                lock: ge.locked_gs().clone(),
                eid: edge_id,
            },
        }
    }
}

impl<'a, const N: usize> Deref for VRef<'a, N> {
    type Target = NodeStore<N>;

    fn deref(&self) -> &Self::Target {
        match self {
            VRef::Entry(e) => e,
            VRef::LockedEntry(e) => e,
        }
    }
}

pub(crate) trait GraphItem<'a, const N: usize> {
    fn from_edge_ids(
        src: VID,
        dst: VID,
        e_id: ERef<'a, N>,
        dir: Direction,
        graph: &'a TGraph<N>,
    ) -> Self;
}
