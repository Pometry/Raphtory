use crate::core::vertex::InputVertex;

#[derive(Copy, Clone, PartialOrd, PartialEq, Debug, Eq, Hash)]
pub struct LocalVertexRef {
    pub shard_id: usize,
    pub pid: usize,
}

impl Into<usize> for LocalVertexRef {
    fn into(self) -> usize {
        self.pid
    }
}

impl LocalVertexRef {
    pub(crate) fn new(pid: usize, shard_id: usize) -> Self {
        Self { shard_id, pid }
    }
}

#[derive(Copy, Clone, PartialOrd, PartialEq, Debug)]
pub enum VertexRef {
    Local(LocalVertexRef),
    Remote(u64),
}

impl VertexRef {
    /// Makes a new local vertex reference from a `pid` and `shard` id.
    /// Values are unchecked and the vertex is assumed to exist so use with caution!
    pub fn new_local(pid: usize, shard: usize) -> Self {
        VertexRef::Local(LocalVertexRef {
            shard_id: shard,
            pid,
        })
    }

    /// Returns the local id of the vertex if it is the local variant
    pub fn pid(&self) -> Option<usize> {
        match self {
            Self::Local(LocalVertexRef { pid, .. }) => Some(*pid),
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

    /// return local reference if the vertex is the local variant
    pub fn local(self) -> Option<LocalVertexRef> {
        match self {
            Self::Local(local) => Some(local),
            _ => None,
        }
    }
}

impl<V: InputVertex> From<V> for VertexRef {
    fn from(value: V) -> Self {
        VertexRef::Remote(value.id())
    }
}

impl From<LocalVertexRef> for VertexRef {
    fn from(value: LocalVertexRef) -> Self {
        VertexRef::Local(value)
    }
}
