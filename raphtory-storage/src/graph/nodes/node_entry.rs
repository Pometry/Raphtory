use crate::graph::nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps};
use raphtory_api::core::{
    entities::{edges::edge_ref::EdgeRef, properties::prop::Prop, GidRef, LayerIds, VID},
    Direction,
};
use storage::{
    api::nodes::{self, NodeEntryOps},
    utils::Iter2,
    NodeEntry, NodeEntryRef,
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

impl<'a> NodeStorageEntry<'a> {
    #[inline]
    pub fn as_ref(&self) -> NodeStorageRef {
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

impl<'b> NodeStorageEntry<'b> {
    pub fn into_edges_iter<'a: 'b>(
        self,
        layers: &'a LayerIds,
        dir: Direction,
    ) -> impl Iterator<Item = EdgeRef> + Send + Sync + 'b {
        match self {
            NodeStorageEntry::Mem(entry) => {
                Iter2::I1(nodes::NodeRefOps::edges_iter(entry, layers, dir))
            }
            NodeStorageEntry::Unlocked(entry) => Iter2::I2(entry.into_edges(layers, dir)),
        }
    }

    // pub fn prop_ids(self) -> BoxedLIter<'b, usize> {
    //     match self {
    //         NodeStorageEntry::Mem(entry) => Box::new(entry.node().const_prop_ids()),
    //         NodeStorageEntry::Unlocked(entry) => Box::new(GenLockedIter::from(entry, |e| {
    //             Box::new(e.as_ref().node().const_prop_ids())
    //         })),
    //         #[cfg(feature = "storage")]
    //         NodeStorageEntry::Disk(node) => Box::new(node.constant_node_prop_ids()),
    //     }
    // }

    // pub fn temporal_prop_ids(self) -> Box<dyn Iterator<Item = usize> + 'b> {
    //     match self {
    //         NodeStorageEntry::Mem(entry) => Box::new(entry.temporal_prop_ids()),
    //         NodeStorageEntry::Unlocked(entry) => Box::new(GenLockedIter::from(entry, |e| {
    //             Box::new(e.as_ref().temporal_prop_ids())
    //         })),
    //         #[cfg(feature = "storage")]
    //         NodeStorageEntry::Disk(node) => Box::new(node.temporal_node_prop_ids()),
    //     }
    // }
}

impl<'a, 'b: 'a> NodeStorageOps<'a> for &'a NodeStorageEntry<'b> {
    fn degree(self, layers: &LayerIds, dir: Direction) -> usize {
        self.as_ref().degree(layers, dir)
    }

    fn additions(self, layer_ids: &'a LayerIds) -> storage::NodeAdditions<'a> {
        self.as_ref().additions(layer_ids)
    }

    fn tprop(self, layer_ids: &'a LayerIds, prop_id: usize) -> storage::NodeTProps<'a> {
        self.as_ref().tprop(layer_ids, prop_id)
    }

    fn tprops(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl Iterator<Item = (usize, storage::NodeTProps<'a>)> {
        self.as_ref().tprops(layer_ids)
    }

    fn prop(self, layer_id: usize, prop_id: usize) -> Option<Prop> {
        self.as_ref().prop(layer_id, prop_id)
    }

    fn edges_iter(
        self,
        layers: &LayerIds,
        dir: Direction,
    ) -> impl Iterator<Item = EdgeRef> + Send + Sync + 'a {
        self.as_ref().edges_iter(layers, dir)
    }

    fn node_type_id(self) -> usize {
        self.as_ref().node_type_id()
    }

    fn vid(self) -> VID {
        self.as_ref().vid()
    }

    fn id(self) -> GidRef<'a> {
        self.as_ref().id()
    }

    fn find_edge(self, dst: VID, layer_ids: &LayerIds) -> Option<EdgeRef> {
        self.as_ref().find_edge(dst, layer_ids)
    }
}
