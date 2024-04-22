use crate::{
    core::{
        entities::nodes::{node_ref::NodeRef, node_store::NodeStore},
        storage::Entry,
    },
    db::api::storage::{arrow::nodes::ArrowNode, nodes::node_ref::NodeStorageRef},
};
use std::ops::Deref;

pub enum NodeStorageEntry<'a> {
    Mem(Entry<'a, NodeStore>),
    #[cfg(feature = "arrow")]
    Arrow(ArrowNode<'a>),
}

impl<'a> NodeStorageEntry<'a> {
    pub fn as_ref(&self) -> NodeStorageRef {
        match self {
            NodeStorageEntry::Mem(entry) => NodeStorageRef::Mem(entry),
            NodeStorageEntry::Arrow(node) => NodeStorageRef::Arrow(*node),
        }
    }
}

impl<'a, 'b: 'a> From<&'a NodeStorageEntry<'b>> for NodeStorageRef<'a> {
    fn from(value: &'a NodeStorageEntry<'b>) -> Self {
        value.as_ref()
    }
}
