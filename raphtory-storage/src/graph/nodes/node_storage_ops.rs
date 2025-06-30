use raphtory_api::core::{
    entities::{edges::edge_ref::EdgeRef, properties::prop::Prop, GidRef, LayerIds, VID},
    Direction,
};
use raphtory_core::{entities::LayerVariants, storage::timeindex::TimeIndexEntry};
use std::{borrow::Cow, ops::Range};
use storage::{api::nodes::NodeRefOps, NodeEntryRef};

static ALL_LAYERS: &LayerIds = &LayerIds::All;

pub trait NodeStorageOps<'a>: Copy + Sized + Send + Sync + 'a {
    fn degree(self, layers: &LayerIds, dir: Direction) -> usize;

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

    fn layer_ids_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl Iterator<Item = usize> + Send + Sync + 'a;

    fn layer_additions(self, layer_id: usize) -> storage::NodeAdditions<'a>;

    fn additions(self) -> storage::NodeAdditions<'a>;

    fn deletions(self, layer_id: usize) -> storage::NodeAdditions<'a>;

    fn temporal_prop_layer(self, layer_id: usize, prop_id: usize) -> storage::NodeTProps<'a>;

    fn temporal_prop_iter(
        self,
        layer_ids: &'a LayerIds,
        prop_id: usize,
    ) -> impl Iterator<Item = (usize, storage::NodeTProps<'a>)> + 'a {
        self.layer_ids_iter(layer_ids)
            .map(move |id| (id, self.temporal_prop_layer(id, prop_id)))
    }

    fn tprop(self, prop_id: usize) -> storage::NodeTProps<'a>;

    fn constant_prop_layer(self, layer_id: usize, prop_id: usize) -> Option<Prop>;

    fn constant_prop_iter(
        self,
        layer_ids: &'a LayerIds,
        prop_id: usize,
    ) -> impl Iterator<Item = (usize, Prop)> + 'a {
        self.layer_ids_iter(layer_ids)
            .filter_map(move |id| Some((id, self.constant_prop_layer(id, prop_id)?)))
    }

    fn temp_prop_rows_range(
        self,
        w: Option<Range<TimeIndexEntry>>,
    ) -> impl Iterator<Item = (TimeIndexEntry, usize, Vec<(usize, Prop)>)>;

    fn temp_prop_rows(self) -> impl Iterator<Item = (TimeIndexEntry, usize, Vec<(usize, Prop)>)> {
        self.temp_prop_rows_range(None)
    }
}

impl<'a> NodeStorageOps<'a> for NodeEntryRef<'a> {
    fn degree(self, layers: &LayerIds, dir: Direction) -> usize {
        NodeRefOps::degree(self, layers, dir)
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

    fn layer_ids_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl Iterator<Item = usize> + Send + Sync + 'a {
        match layer_ids {
            LayerIds::None => LayerVariants::None(std::iter::empty()),
            LayerIds::All => LayerVariants::All(
                (0..self.internal_num_layers()).filter(move |&l| self.has_layer_inner(l)),
            ),
            LayerIds::One(id) => {
                LayerVariants::One(self.has_layer_inner(*id).then_some(*id).into_iter())
            }
            LayerIds::Multiple(ids) => {
                LayerVariants::Multiple(ids.iter().filter(move |&id| self.has_layer_inner(id)))
            }
        }
    }

    fn deletions(self, layer_id: usize) -> storage::NodeAdditions<'a> {
        NodeRefOps::layer_additions(self, layer_id)
    }

    fn layer_additions(self, layer_ids: usize) -> storage::NodeAdditions<'a> {
        NodeRefOps::layer_additions(self, layer_ids)
    }

    fn additions(self) -> storage::NodeAdditions<'a> {
        NodeRefOps::additions(self, ALL_LAYERS)
    }

    fn tprop(self, prop_id: usize) -> storage::NodeTProps<'a> {
        NodeRefOps::t_prop(self, &ALL_LAYERS, prop_id)
    }

    fn temporal_prop_layer(self, layer_id: usize, prop_id: usize) -> storage::NodeTProps<'a> {
        NodeRefOps::temporal_prop_layer(self, layer_id, prop_id)
    }

    fn constant_prop_layer(self, layer_id: usize, prop_id: usize) -> Option<Prop> {
        NodeRefOps::c_prop(self, layer_id, prop_id)
    }

    fn temp_prop_rows_range(
        self,
        w: Option<Range<TimeIndexEntry>>,
    ) -> impl Iterator<Item = (TimeIndexEntry, usize, Vec<(usize, Prop)>)> {
        NodeRefOps::temp_prop_rows(self, w)
    }
}
