#![allow(unused)]

use std::{ops::Deref, sync::Arc};

use crate::core::entities::edges::edge_ref::EdgeRef;
use edges::edge::ERef;
use graph::{tgraph::TGraph, tgraph_storage::GraphEntry};
use nodes::{node_ref::NodeRef, node_store::NodeStore};
use serde::{Deserialize, Serialize};

use super::{storage::Entry, Direction};

pub mod edges;
pub mod graph;
pub mod nodes;
pub mod properties;

// the only reason this is public is because the physical ids of the nodes don't move
#[repr(transparent)]
#[derive(
    Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize, Serialize, Default,
)]
pub struct VID(pub usize);

impl VID {
    pub fn index(&self) -> usize {
        self.0
    }
}

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
pub struct EID(pub usize);

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
    Entry(Entry<'a, NodeStore, N>),        // returned from graph.node
    LockedEntry(GraphEntry<NodeStore, N>), // returned from locked_nodes
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
    type Target = NodeStore;

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

    pub fn intersect(&self, other: &LayerIds) -> LayerIds {
        match (self, other) {
            (LayerIds::None, _) => LayerIds::None,
            (_, LayerIds::None) => LayerIds::None,
            (LayerIds::All, other) => other.clone(),
            (this, LayerIds::All) => this.clone(),
            (LayerIds::One(id), other) => {
                if other.contains(id) {
                    LayerIds::One(*id)
                } else {
                    LayerIds::None
                }
            }
            (LayerIds::Multiple(ids), other) => {
                let ids: Vec<usize> = ids
                    .iter()
                    .filter(|id| other.contains(id))
                    .copied()
                    .collect();
                match ids.len() {
                    0 => LayerIds::None,
                    1 => LayerIds::One(ids[0]),
                    _ => LayerIds::Multiple(ids.into()),
                }
            }
        }
    }

    pub fn constrain_from_edge(self, e: EdgeRef) -> LayerIds {
        match e.layer() {
            None => self,
            Some(l) => self
                .find(*l)
                .map(LayerIds::One)
                .unwrap_or(LayerIds::None),
        }
    }

    pub fn contains(&self, layer_id: &usize) -> bool {
        self.find(*layer_id).is_some()
    }

    pub fn is_none(&self) -> bool {
        matches!(self, LayerIds::None)
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
