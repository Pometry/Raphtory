use crate::core::entities::VID;
use either::Either;
use raphtory_api::core::entities::{GidRef, GID};

#[derive(Copy, Clone, PartialOrd, PartialEq, Debug, Eq, Hash, Ord)]
pub enum NodeRef<'a> {
    Internal(VID),
    External(GidRef<'a>),
}

pub trait AsNodeRef {
    fn as_node_ref(&self) -> NodeRef;

    fn into_gid(self) -> Either<GID, VID>
    where
        Self: Sized,
    {
        match self.as_node_ref() {
            NodeRef::Internal(vid) => Either::Right(vid),
            NodeRef::External(gid) => Either::Left(gid.into()),
        }
    }

    fn as_gid_ref(&self) -> Either<GidRef, VID> {
        match self.as_node_ref() {
            NodeRef::Internal(vid) => Either::Right(vid),
            NodeRef::External(u) => Either::Left(u),
        }
    }
}

impl<'a> AsNodeRef for NodeRef<'a> {
    fn as_node_ref(&self) -> NodeRef {
        *self
    }
}

impl AsNodeRef for VID {
    fn as_node_ref(&self) -> NodeRef {
        NodeRef::Internal(*self)
    }
}

impl AsNodeRef for u64 {
    fn as_node_ref(&self) -> NodeRef {
        NodeRef::External(GidRef::U64(*self))
    }
}

impl AsNodeRef for String {
    fn as_node_ref(&self) -> NodeRef {
        NodeRef::External(GidRef::Str(&self))
    }
}

impl<'a> AsNodeRef for &'a str {
    fn as_node_ref(&self) -> NodeRef {
        NodeRef::External(GidRef::Str(self))
    }
}

impl<'a, V: AsNodeRef> AsNodeRef for &'a V {
    fn as_node_ref(&self) -> NodeRef {
        V::as_node_ref(self)
    }
}

impl AsNodeRef for GID {
    fn as_node_ref(&self) -> NodeRef {
        let gid_ref: GidRef = self.into();
        NodeRef::External(gid_ref)
    }
}

impl<'a> AsNodeRef for GidRef<'a> {
    fn as_node_ref(&self) -> NodeRef {
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
