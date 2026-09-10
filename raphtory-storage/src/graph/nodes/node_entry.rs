use std::{ops::Range, sync::Arc};

use crate::graph::nodes::node_ref::NodeStorageRef;
use raphtory_api::core::{
    entities::{edges::edge_ref::EdgeRef, properties::prop::Prop, GidRef, LayerId, LayerIds, VID},
    Direction,
};
use raphtory_core::storage::timeindex::EventTime;
use storage::{
    api::nodes::{self, IntoEdges, NodeEntryOps},
    generic_time_ops::LayerIter,
    utils::Iter2,
    NodeDeletions, NodeEntry, NodeEntryRef,
};

pub enum NodeStorageEntry<'a> {
    Mem(NodeEntryRef<'a>),
    Unlocked(NodeEntry<'a>),
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
        }
    }
}

impl<'a, 'b: 'a> From<&'a NodeStorageEntry<'b>> for NodeStorageRef<'a> {
    fn from(value: &'a NodeStorageEntry<'b>) -> Self {
        value.as_ref()
    }
}
