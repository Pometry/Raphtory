use crate::core::tgraph::{vertices::input_vertex::InputVertex, VID};

#[derive(Copy, Clone, PartialOrd, PartialEq, Debug)]
pub enum VertexRef {
    Local(VID),
    Remote(u64),
}

impl VertexRef {
    /// Makes a new local vertex reference from a `pid` and `shard` id.
    /// Values are unchecked and the vertex is assumed to exist so use with caution!
    pub fn new_local(vid: VID) -> Self {
        VertexRef::Local(vid)
    }

    /// Returns the local id of the vertex if it is the local variant
    pub fn pid(&self) -> Option<VID> {
        match self {
            Self::Local(vid) => Some(*vid),
            _ => None,
        }
    }

    /// Returns the global id of the vertex if it is the remote variant
    pub fn gid(&self) -> Option<u64> {
        match self {
            Self::Remote(gid) => Some(*gid),
            _ => None,
        }
    }
}

impl<V: InputVertex> From<V> for VertexRef {
    fn from(value: V) -> Self {
        VertexRef::Remote(value.id())
    }
}

impl From<VID> for VertexRef {
    fn from(value: VID) -> Self {
        VertexRef::Local(value)
    }
}
