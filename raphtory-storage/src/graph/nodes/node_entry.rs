use crate::graph::nodes::node_ref::NodeStorageRef;
use storage::{
    api::nodes::NodeEntryOps, pages::node_store::SegmentLockedNodeEntry, Extension, NodeEntry,
    NodeEntryRef, NS,
};

pub enum NodeStorageEntry<'a> {
    Mem(NodeEntryRef<'a>),
    Unlocked(NodeEntry<'a>),
    Segment(SegmentLockedNodeEntry<NS<Extension>, Extension>),
}

impl<'a> From<NodeEntryRef<'a>> for NodeStorageEntry<'a> {
    fn from(value: NodeEntryRef<'a>) -> Self {
        NodeStorageEntry::Mem(value)
    }
}

impl<'a> From<NodeEntry<'a>> for NodeStorageEntry<'a> {
    fn from(value: NodeEntry<'a>) -> Self {
        NodeStorageEntry::Unlocked(value)
    }
}

impl<'a> NodeEntryOps for NodeStorageEntry<'a> {
    type Ref<'b>
        = NodeStorageRef<'b>
    where
        Self: 'b;

    #[inline]
    fn as_ref<'b>(&'b self) -> Self::Ref<'b> {
        match self {
            NodeStorageEntry::Mem(entry) => *entry,
            NodeStorageEntry::Unlocked(entry) => entry.as_ref(),
            NodeStorageEntry::Segment(entry) => entry.as_ref(),
        }
    }
}

impl<'a, 'b: 'a> From<&'a NodeStorageEntry<'b>> for NodeStorageRef<'a> {
    fn from(value: &'a NodeStorageEntry<'b>) -> Self {
        value.as_ref()
    }
}
