use std::ops::Deref;

use serde::{Deserialize, Serialize};

use crate::storage::{Entry, iter::RefT};

use self::{node_store::NodeStore, tgraph::TGraph};

use super::Direction;

mod adj;
mod edge_layer;
mod edge_store;
mod node_store;
mod props;
mod timer;
mod tadjset;
mod vertex;
pub(crate) mod edge;
mod iter;
mod tgraph_storage;
pub mod tgraph;

// the only reason this is public is because the phisical ids of the vertices don't move
#[repr(transparent)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize, Serialize, Default)]
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

impl From<VID> for usize{
    fn from(id: VID) -> Self {
        id.0
    }
}

// impl VID {
//     #[inline(always)]
//     pub(crate) fn as_local<const N: usize>(&self) -> LocalID {
//         let bucket = self.0 % N;
//         let offset = self.0 / N;
//         LocalID { bucket, offset }
//     }
// }


#[repr(transparent)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize, Serialize)]
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

// impl EID {
//     #[inline(always)]
//     pub(crate) fn as_local<const N: usize>(&self) -> LocalID {
//         let bucket = self.0 % N;
//         let offset = self.0 / N;
//         LocalID { bucket, offset }
//     }
// }

pub(crate) enum VRef<'a, const N: usize, L: lock_api::RawRwLock> {
    Entry(Entry<'a, NodeStore<N>, L, N>), // fastest thing, returned from graph.vertex
    RefT(RefT<'a, NodeStore<N>, L, N>),   // returned from graph.vertices
}

// return index -> usize for VRef
impl<'a, const N: usize, L: lock_api::RawRwLock> VRef<'a, N, L> {
    fn index(&'a self) -> usize {
        match self {
            VRef::RefT(r) => r.index(),
            VRef::Entry(e) => e.index(),
        }
    }
}

impl<'a, const N: usize, L: lock_api::RawRwLock> Deref for VRef<'a, N, L> {
    type Target = NodeStore<N>;

    fn deref(&self) -> &Self::Target {
        match self {
            VRef::RefT(r) => r,
            VRef::Entry(e) => e,
        }
    }
}

pub trait GraphItem<'a, const N: usize, L: lock_api::RawRwLock> {

    fn from_edge_ids(
        src: VID,
        dst: VID,
        e_id: EID,
        dir: Direction,
        graph: &'a TGraph<N, L>,
    ) -> Self;

}
