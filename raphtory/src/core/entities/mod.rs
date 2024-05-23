use std::sync::Arc;

#[cfg(feature = "arrow")]
use raphtory_arrow::interop::{AsEID, AsVID};
use serde::{Deserialize, Serialize};

use crate::core::entities::edges::edge_ref::EdgeRef;

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

    pub fn as_u64(&self) -> u64 {
        self.0 as u64
    }
}

impl From<usize> for VID {
    fn from(id: usize) -> Self {
        VID(id)
    }
}

#[cfg(feature = "arrow")]
impl From<raphtory_arrow::interop::VID> for VID {
    fn from(vid: raphtory_arrow::interop::VID) -> Self {
        VID(vid.0)
    }
}

impl From<VID> for usize {
    fn from(id: VID) -> Self {
        id.0
    }
}

#[cfg(feature = "arrow")]
impl AsVID for VID {
    fn as_vid(&self) -> raphtory_arrow::interop::VID {
        raphtory_arrow::interop::VID::from(self.0)
    }
}

#[cfg(feature = "arrow")]
impl PartialEq<VID> for raphtory_arrow::interop::VID {
    fn eq(&self, other: &VID) -> bool {
        self.0 == other.0
    }
}

#[cfg(feature = "arrow")]
impl Into<raphtory_arrow::interop::VID> for VID {
    #[inline]
    fn into(self) -> raphtory_arrow::interop::VID {
        raphtory_arrow::interop::VID(self.0)
    }
}

#[repr(transparent)]
#[derive(
    Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize, Serialize, Default,
)]
pub struct EID(pub usize);

#[cfg(feature = "arrow")]
impl From<raphtory_arrow::interop::EID> for EID {
    fn from(eid: raphtory_arrow::interop::EID) -> Self {
        EID(eid.0)
    }
}

#[cfg(feature = "arrow")]
impl Into<raphtory_arrow::interop::EID> for EID {
    #[inline]
    fn into(self) -> raphtory_arrow::interop::EID {
        raphtory_arrow::interop::EID(self.0)
    }
}

#[cfg(feature = "arrow")]
impl AsEID for EID {
    fn as_eid(&self) -> raphtory_arrow::interop::EID {
        raphtory_arrow::interop::EID(self.0)
    }
}

#[derive(
    Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize, Serialize, Default,
)]
pub struct ELID {
    edge: EID,
    layer: Option<usize>,
}

impl ELID {
    pub fn new(edge: EID, layer: Option<usize>) -> Self {
        Self { edge, layer }
    }
    pub fn pid(&self) -> EID {
        self.edge
    }

    pub fn layer(&self) -> Option<usize> {
        self.layer
    }
}

impl From<EdgeRef> for ELID {
    fn from(value: EdgeRef) -> Self {
        ELID {
            edge: value.pid(),
            layer: value.layer().copied(),
        }
    }
}
impl EID {
    pub fn from_u64(id: u64) -> Self {
        EID(id as usize)
    }
}

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

    pub fn diff<'a>(
        &self,
        graph: impl crate::prelude::GraphViewOps<'a>,
        other: &LayerIds,
    ) -> LayerIds {
        match (self, other) {
            (LayerIds::None, _) => LayerIds::None,
            (this, LayerIds::None) => this.clone(),
            (_, LayerIds::All) => LayerIds::None,
            (LayerIds::One(id), other) => {
                if other.contains(id) {
                    LayerIds::None
                } else {
                    LayerIds::One(*id)
                }
            }
            (LayerIds::Multiple(ids), other) => {
                let ids: Vec<usize> = ids
                    .iter()
                    .filter(|id| !other.contains(id))
                    .copied()
                    .collect();
                match ids.len() {
                    0 => LayerIds::None,
                    1 => LayerIds::One(ids[0]),
                    _ => LayerIds::Multiple(ids.into()),
                }
            }
            (LayerIds::All, other) => {
                let all_layer_ids: Vec<usize> = graph
                    .unique_layers()
                    .map(|name| graph.get_layer_id(name.as_ref()).unwrap())
                    .into_iter()
                    .filter(|id| !other.contains(id))
                    .collect();
                match all_layer_ids.len() {
                    0 => LayerIds::None,
                    1 => LayerIds::One(all_layer_ids[0]),
                    _ => LayerIds::Multiple(all_layer_ids.into()),
                }
            }
        }
    }

    pub fn constrain_from_edge(&self, e: EdgeRef) -> LayerIds {
        match e.layer() {
            None => self.clone(),
            Some(l) => self.find(*l).map(LayerIds::One).unwrap_or(LayerIds::None),
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
