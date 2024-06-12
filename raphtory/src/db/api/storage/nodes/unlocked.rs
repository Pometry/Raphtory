use core::panic;

use raphtory_api::core::{
    entities::{edges::edge_ref::EdgeRef, VID},
    Direction,
};

use crate::{
    core::{
        entities::{graph::tgraph_storage::GraphStorage, nodes::node_store::NodeStore, properties::tprop::TProp, LayerIds},
        storage::{ArcEntry, Entry},
    },
    db::api::{storage::tprop_storage_ops::TPropOps, view::internal::NodeAdditions},
};

use super::node_storage_ops::NodeStorageOps;

#[derive(Clone, Copy, Debug)]
pub struct UnlockedNodeRef<'a> {
    gs: &'a GraphStorage,
    vid: VID,
}

impl<'a> UnlockedNodeRef<'a> {
    pub fn new(gs: &'a GraphStorage, vid: VID) -> Self {
        Self { gs, vid }
    }

    pub fn vid(&self) -> VID {
        self.vid
    }

    fn entry(&self) -> Entry<NodeStore> {
        self.gs.get_node(self.vid)
    }

    fn arc_entry(&self) -> ArcEntry<NodeStore> {
        self.gs.get_arc_node(self.vid)
    }
}

impl<'a> NodeStorageOps<'a> for UnlockedNodeRef<'a> {
    fn degree(self, layers: &LayerIds, dir: Direction) -> usize {
        self.entry().degree(layers, dir)
    }

    fn additions(self) -> NodeAdditions<'a> {
        // self.entry().additions()
        todo!()
    }

    fn tprop(self, prop_id: usize) -> impl TPropOps<'a> {
        // self.entry().tprop(prop_id)
        // panic!("Not implemented")
        &TProp::Empty
    }

    fn edges_iter(
        self,
        layers: &'a LayerIds,
        dir: Direction,
    ) -> impl Iterator<Item = EdgeRef> + 'a {
        self.arc_entry().into_edges(layers, dir)
    }

    fn node_type_id(self) -> usize {
        self.entry().node_type_id()
    }

    fn vid(self) -> VID {
        self.vid
    }

    fn id(self) -> u64 {
        self.entry().id()
    }

    fn name(self) -> Option<&'a str> {
        self.entry().name()
    }

    fn find_edge(self, dst: VID, layer_ids: &LayerIds) -> Option<EdgeRef> {
        self.entry().find_edge(dst, layer_ids)
    }
}
