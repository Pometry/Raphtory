use crate::core::entities::{vertices::input_vertex::InputVertex, VID};

#[derive(Copy, Clone, PartialOrd, PartialEq, Debug)]
pub enum VertexRef {
    Internal(VID),
    External(u64),
}

impl VertexRef {
    /// Makes a new vertex reference from an internal `VID`.
    /// Values are unchecked and the vertex is assumed to exist so use with caution!
    pub fn new(vid: VID) -> Self {
        VertexRef::Internal(vid)
    }
}

impl<V: InputVertex> From<V> for VertexRef {
    fn from(value: V) -> Self {
        VertexRef::External(value.id())
    }
}

impl From<VID> for VertexRef {
    fn from(value: VID) -> Self {
        VertexRef::Internal(value)
    }
}
