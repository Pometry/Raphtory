#![allow(unused)]

use std::{ops::Deref, sync::Arc};

use crate::core::entities::edges::edge_ref::EdgeRef;
use edges::edge::ERef;
use graph::{tgraph::TGraph, tgraph_storage::GraphEntry};
use serde::{Deserialize, Serialize};
use vertices::{vertex_ref::VertexRef, vertex_store::VertexStore};

use super::{storage::Entry, Direction};

pub mod edges;
pub mod graph;
pub mod properties;
pub mod vertices;

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
    Entry(Entry<'a, VertexStore, N>), // returned from graph.vertex
    LockedEntry(GraphEntry<VertexStore, N>), // returned from locked_vertices
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
    type Target = VertexStore;

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

#[derive(Clone, Debug)]
pub enum LayerIds {
    None,
    All,
    One(usize),
    Multiple(Arc<[usize]>),
}

impl LayerIds {
    pub fn find(&self, layer_id: usize) -> Option<usize> {
        match self {
            LayerIds::All => Some(layer_id),
            LayerIds::One(id) => {
                if *id == layer_id {
                    Some(layer_id)
                } else {
                    None
                }
            }
            LayerIds::Multiple(ids) => ids.binary_search(&layer_id).ok().map(|_| layer_id),
            LayerIds::None => None,
        }
    }

    pub fn constrain_from_edge(self, e: EdgeRef) -> LayerIds {
        match e.layer() {
            None => self,
            Some(l) => self
                .find(*l)
                .map(|l| LayerIds::One(l))
                .unwrap_or(LayerIds::None),
        }
    }

    pub fn contains(&self, layer_id: &usize) -> bool {
        self.find(*layer_id).is_some()
    }
}

impl From<Vec<usize>> for LayerIds {
    fn from(mut v: Vec<usize>) -> Self {
        match v.len() {
            0 => LayerIds::All,
            1 => LayerIds::One(v[0]),
            _ => {
                v.sort_unstable();
                v.dedup();
                LayerIds::Multiple(v.into())
            }
        }
    }
}

impl<const N: usize> From<[usize; N]> for LayerIds {
    fn from(v: [usize; N]) -> Self {
        match v.len() {
            0 => LayerIds::All,
            1 => LayerIds::One(v[0]),
            _ => {
                let mut v = v.to_vec();
                v.sort_unstable();
                v.dedup();
                LayerIds::Multiple(v.into())
            }
        }
    }
}

impl From<usize> for LayerIds {
    fn from(id: usize) -> Self {
        LayerIds::One(id)
    }
}

impl From<Arc<[usize]>> for LayerIds {
    fn from(id: Arc<[usize]>) -> Self {
        LayerIds::Multiple(id)
    }
}
