use crate::core::entities::VID;

#[derive(Copy, Clone, PartialOrd, PartialEq, Debug)]
pub enum NodeRef<'a> {
    Internal(VID),
    External(u64),
    ExternalStr(&'a str),
}

pub trait AsNodeRef {
    fn as_node_ref(&self) -> NodeRef;
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
        NodeRef::External(*self)
    }
}

impl AsNodeRef for String {
    fn as_node_ref(&self) -> NodeRef {
        NodeRef::ExternalStr(self.as_ref())
    }
}

impl<'a> AsNodeRef for &'a str {
    fn as_node_ref(&self) -> NodeRef {
        NodeRef::ExternalStr(self)
    }
}

impl<'a, V: AsNodeRef> AsNodeRef for &'a V {
    fn as_node_ref(&self) -> NodeRef {
        V::as_node_ref(self)
    }
}

impl<'a> NodeRef<'a> {
    /// Makes a new node reference from an internal `VID`.
    /// Values are unchecked and the node is assumed to exist so use with caution!
    pub fn new(vid: VID) -> Self {
        NodeRef::Internal(vid)
    }
}
