use serde::{Deserialize, Serialize};

mod adj;
mod edge_layer;
mod edge_store;
mod node_store;
mod props;
mod timer;
pub mod tgraph;

// the only reason this is public is because the phisical ids of the vertices don't move
#[repr(transparent)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize, Serialize)]
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

impl VID {
    #[inline(always)]
    pub(crate) fn as_local<const N: usize>(&self) -> LocalID {
        let bucket = self.0 % N;
        let offset = self.0 / N;
        LocalID { bucket, offset }
    }
}


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

impl EID {
    #[inline(always)]
    pub(crate) fn as_local<const N: usize>(&self) -> LocalID {
        let bucket = self.0 % N;
        let offset = self.0 / N;
        LocalID { bucket, offset }
    }
}