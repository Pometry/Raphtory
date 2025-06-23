use raphtory_api::core::{
    entities::{edges::edge_ref::EdgeRef, properties::prop::Prop, GidRef, LayerIds, VID},
    Direction,
};
use std::borrow::Cow;
use storage::{api::nodes::NodeRefOps, NodeEntryRef};

pub trait NodeStorageOps<'a>: Sized {
    fn degree(self, layers: &LayerIds, dir: Direction) -> usize;

    fn additions(self, layer_ids: &'a LayerIds) -> storage::NodeAdditions<'a>;

    fn tprop(self, layer_ids: &'a LayerIds, prop_id: usize) -> storage::NodeTProps<'a>;

    fn tprops(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl Iterator<Item = (usize, storage::NodeTProps<'a>)>;

    fn prop(self, layer_id: usize, prop_id: usize) -> Option<Prop>;

    fn edges_iter(
        self,
        layers: &LayerIds,
        dir: Direction,
    ) -> impl Iterator<Item = EdgeRef> + Send + Sync + 'a;

    fn node_type_id(self) -> usize;

    fn vid(self) -> VID;

    fn id(self) -> GidRef<'a>;

    fn name(self) -> Cow<'a, str> {
        self.id().to_str()
    }

    fn find_edge(self, dst: VID, layer_ids: &LayerIds) -> Option<EdgeRef>;
}

impl<'a> NodeStorageOps<'a> for NodeEntryRef<'a> {
    fn degree(self, layers: &LayerIds, dir: Direction) -> usize {
        NodeRefOps::degree(self, layers, dir)
    }

    fn additions(self, layer_ids: &'a LayerIds) -> storage::NodeAdditions<'a> {
        NodeRefOps::additions(self, layer_ids)
    }

    fn tprop(self, layer_ids: &'a LayerIds, prop_id: usize) -> storage::NodeTProps<'a> {
        NodeRefOps::t_prop(self, layer_ids, prop_id)
    }

    fn tprops(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl Iterator<Item = (usize, storage::NodeTProps<'a>)> {
        NodeRefOps::t_props(self, layer_ids)
    }

    fn prop(self, layer_id: usize, prop_id: usize) -> Option<Prop> {
        NodeRefOps::c_prop(self, layer_id, prop_id)
    }

    fn edges_iter(
        self,
        layers: &LayerIds,
        dir: Direction,
    ) -> impl Iterator<Item = EdgeRef> + Send + Sync + 'a {
        NodeRefOps::edges_iter(self, layers, dir)
    }

    fn node_type_id(self) -> usize {
        NodeRefOps::node_type_id(&self)
    }

    fn vid(self) -> VID {
        NodeRefOps::vid(&self)
    }

    fn id(self) -> GidRef<'a> {
        NodeRefOps::gid(&self)
    }

    fn find_edge(self, dst: VID, layer_ids: &LayerIds) -> Option<EdgeRef> {
        NodeRefOps::find_edge(&self, dst, layer_ids)
    }
}
