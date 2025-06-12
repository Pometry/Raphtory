use crate::graph::nodes::node_additions::NodeAdditions;
use raphtory_api::core::{
    entities::{
        edges::edge_ref::EdgeRef,
        properties::{prop::Prop, tprop::TPropOps},
        GidRef, LayerIds, VID,
    },
    Direction,
};
use raphtory_core::{entities::nodes::node_store::NodeStore, storage::node_entry::NodePtr};
use std::borrow::Cow;

pub trait NodeStorageOps<'a>: Sized {
    fn degree(self, layers: &LayerIds, dir: Direction) -> usize;

    fn additions(self) -> NodeAdditions<'a>;

    fn tprop(self, prop_id: usize) -> impl TPropOps<'a>;

    fn tprops(self) -> impl Iterator<Item = (usize, impl TPropOps<'a>)>;

    fn prop(self, prop_id: usize) -> Option<Prop>;

    fn edges_iter(
        self,
        layers: &LayerIds,
        dir: Direction,
    ) -> impl Iterator<Item = EdgeRef> + Send + Sync + 'a;

    fn node_type_id(self) -> usize;

    fn vid(self) -> VID;

    fn id(self) -> GidRef<'a>;

    fn name(self) -> Option<Cow<'a, str>>;

    fn find_edge(self, dst: VID, layer_ids: &LayerIds) -> Option<EdgeRef>;
}

impl<'a> NodeStorageOps<'a> for NodePtr<'a> {
    fn degree(self, layers: &LayerIds, dir: Direction) -> usize {
        self.node.degree(layers, dir)
    }

    fn additions(self) -> NodeAdditions<'a> {
        NodeAdditions::Mem(self.node.timestamps())
    }

    fn tprop(self, prop_id: usize) -> impl TPropOps<'a> {
        self.t_prop(prop_id)
    }

    fn tprops(self) -> impl Iterator<Item = (usize, impl TPropOps<'a>)> {
        self.temporal_prop_ids()
            .map(move |tid| (tid, self.tprop(tid)))
    }

    fn prop(self, prop_id: usize) -> Option<Prop> {
        self.node.constant_property(prop_id).cloned()
    }

    fn edges_iter(self, layers: &LayerIds, dir: Direction) -> impl Iterator<Item = EdgeRef> + 'a {
        self.node.edge_tuples(layers, dir)
    }

    fn node_type_id(self) -> usize {
        self.node.node_type
    }

    fn vid(self) -> VID {
        self.node.vid
    }

    fn id(self) -> GidRef<'a> {
        (&self.node.global_id).into()
    }

    fn name(self) -> Option<Cow<'a, str>> {
        self.node.global_id.as_str().map(Cow::from)
    }

    fn find_edge(self, dst: VID, layer_ids: &LayerIds) -> Option<EdgeRef> {
        let eid = NodeStore::find_edge_eid(self.node, dst, layer_ids)?;
        Some(EdgeRef::new_outgoing(eid, self.node.vid, dst))
    }
}
