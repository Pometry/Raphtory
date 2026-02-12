use crate::entities::VID;
use either::Either;
use raphtory_api::core::entities::{GidRef, GID};

#[derive(Copy, Clone, PartialOrd, PartialEq, Debug, Eq, Hash, Ord)]
pub enum NodeRef<'a> {
    Internal(VID),
    External(GidRef<'a>),
}

pub trait AsNodeRef: Send + Sync {
    fn as_node_ref(&self) -> NodeRef<'_>;

    fn into_gid(self) -> Either<GID, VID>
    where
        Self: Sized,
    {
        match self.as_node_ref() {
            NodeRef::Internal(vid) => Either::Right(vid),
            NodeRef::External(gid) => Either::Left(gid.into()),
        }
    }

    fn as_gid_ref(&self) -> Option<GidRef<'_>> {
        match self.as_node_ref() {
            NodeRef::Internal(_) => None,
            NodeRef::External(u) => Some(u),
        }
    }
}

impl<'a> AsNodeRef for NodeRef<'a> {
    fn as_node_ref(&self) -> NodeRef<'_> {
        *self
    }
}

impl AsNodeRef for VID {
    fn as_node_ref(&self) -> NodeRef<'_> {
        NodeRef::Internal(*self)
    }
}

impl AsNodeRef for u64 {
    fn as_node_ref(&self) -> NodeRef<'_> {
        NodeRef::External(GidRef::U64(*self))
    }
}

impl AsNodeRef for String {
    fn as_node_ref(&self) -> NodeRef<'_> {
        NodeRef::External(GidRef::Str(self))
    }
}

impl AsNodeRef for &str {
    fn as_node_ref(&self) -> NodeRef<'_> {
        NodeRef::External(GidRef::Str(self))
    }
}

impl<V: AsNodeRef> AsNodeRef for &V {
    fn as_node_ref(&self) -> NodeRef<'_> {
        V::as_node_ref(self)
    }
}

impl AsNodeRef for GID {
    fn as_node_ref(&self) -> NodeRef<'_> {
        let gid_ref: GidRef = self.into();
        NodeRef::External(gid_ref)
    }
}

impl<'a> AsNodeRef for GidRef<'a> {
    fn as_node_ref(&self) -> NodeRef<'_> {
        NodeRef::External(*self)
    }
}

impl<'a> NodeRef<'a> {
    /// Makes a new node reference from an internal `VID`.
    /// Values are unchecked and the node is assumed to exist so use with caution!
    pub fn new(vid: VID) -> Self {
        NodeRef::Internal(vid)
    }
}
