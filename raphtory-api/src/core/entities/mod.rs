use serde::{Deserialize, Serialize};

use self::edges::edge_ref::EdgeRef;

pub mod edges;

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
