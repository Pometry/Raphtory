use crate::core::entities::{nodes::input_node::InputNode, VID};

#[derive(Copy, Clone, PartialOrd, PartialEq, Debug)]
pub enum NodeRef {
    Internal(VID),
    External(u64),
}

impl NodeRef {
    /// Makes a new node reference from an internal `VID`.
    /// Values are unchecked and the node is assumed to exist so use with caution!
    pub fn new(vid: VID) -> Self {
        NodeRef::Internal(vid)
    }
}

impl<V: InputNode> From<V> for NodeRef {
    fn from(value: V) -> Self {
        NodeRef::External(value.id())
    }
}

impl From<VID> for NodeRef {
    fn from(value: VID) -> Self {
        NodeRef::Internal(value)
    }
}
