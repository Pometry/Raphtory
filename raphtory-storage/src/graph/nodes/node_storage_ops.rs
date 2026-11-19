use crate::graph::layer_ids_with_static;
use either::Either;
use raphtory_api::core::{
    entities::{
        edges::edge_ref::EdgeRef,
        properties::{
            meta::{STATIC_GRAPH_LAYER, STATIC_GRAPH_LAYER_ID},
            prop::Prop,
        },
        GidRef, LayerId, LayerIds, VID,
    },
    storage::timeindex::TimeIndexOps,
    Direction,
};
use raphtory_core::{entities::LayerVariants, storage::timeindex::EventTime};
use std::{borrow::Cow, iter, ops::Range, sync::Arc};
use storage::{api::nodes::NodeRefOps, generic_time_ops::LayerIter, NodeEntryRef};

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
    ) -> impl Iterator<Item = LayerId> + Send + Sync + 'a;

    fn has_layers(self, layer_ids: &'a LayerIds) -> bool {
        !self.node_prop_additions(&STATIC_GRAPH_LAYER).is_empty()
            || self.layer_ids_iter(layer_ids).next().is_some()
    }

    fn node_prop_additions<L: Into<LayerIter<'a>>>(
        self,
        layer_id: L,
    ) -> storage::NodePropAdditions<'a>;

    fn node_edge_additions<L: Into<LayerIter<'a>>>(
        self,
        layer_id: L,
    ) -> storage::NodeEdgeAdditions<'a>;

    fn temporal_prop_layer(self, layer_id: LayerId, prop_id: usize) -> storage::NodeTProps<'a>;

    fn tprop(self, prop_id: usize) -> storage::NodeTProps<'a>;

    /// Number of layers in the underlying storage for this node.
    fn num_layers(self) -> usize;

    /// Iterate over `NodeTProps` for each layer specified by `layer_ids`, always
    /// including `STATIC_GRAPH_LAYER_ID` (the layer for nodes added without an
    /// explicit layer name). Unlayered nodes must be visible in every view.
    fn tprop_iter_layers(
        self,
        layer_ids: &LayerIds,
        prop_id: usize,
    ) -> impl Iterator<Item = (LayerId, storage::NodeTProps<'a>)> + Send + Sync + use<'a, Self>
    {
        layer_ids_with_static(self.num_layers(), layer_ids)
            .map(move |id| (id, self.temporal_prop_layer(id, prop_id)))
    }

    fn constant_prop_layer(self, layer_id: LayerId, prop_id: usize) -> Option<Prop>;

    fn constant_prop_iter(
        self,
        layer_ids: &'a LayerIds,
        prop_id: usize,
    ) -> impl Iterator<Item = (LayerId, Prop)> + 'a {
        self.layer_ids_iter(layer_ids)
            .filter_map(move |id| Some((id, self.constant_prop_layer(id, prop_id)?)))
    }

    fn temp_prop_rows_range(
        self,
        w: Option<Range<EventTime>>,
        prop_ids: &'a [usize],
        layer_id: LayerId,
    ) -> impl Iterator<Item = (EventTime, Vec<(usize, Prop)>)> + 'a;

    fn temp_prop_rows(
        self,
        prop_ids: &'a [usize],
        layer_id: LayerId,
    ) -> impl Iterator<Item = (EventTime, Vec<(usize, Prop)>)> + 'a {
        self.temp_prop_rows_range(None, prop_ids, layer_id)
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
    ) -> impl Iterator<Item = LayerId> + Send + Sync + 'a {
        let iter = match layer_ids {
            LayerIds::None => LayerVariants::None(std::iter::once(STATIC_GRAPH_LAYER_ID)),
            LayerIds::All => LayerVariants::All((0..self.internal_num_layers()).map(LayerId)),
            LayerIds::One(id) => LayerVariants::One(if *id != STATIC_GRAPH_LAYER_ID {
                Either::Left([STATIC_GRAPH_LAYER_ID, *id].into_iter())
            } else {
                Either::Right(iter::once(*id))
            }),
            LayerIds::Multiple(ids) => match ids.iter().next() {
                Some(STATIC_GRAPH_LAYER_ID) => LayerVariants::Multiple(Either::Left(ids.iter())),
                Some(_) => LayerVariants::Multiple(Either::Right(
                    iter::once(STATIC_GRAPH_LAYER_ID).chain(ids.iter()),
                )),
                None => LayerVariants::None(iter::once(STATIC_GRAPH_LAYER_ID)),
            },
        };
        iter.filter(move |&id| self.has_layer_inner(id))
    }

    fn node_prop_additions<L: Into<LayerIter<'a>>>(
        self,
        layer_ids: L,
    ) -> storage::NodePropAdditions<'a> {
        NodeRefOps::node_additions(self, layer_ids)
    }

    fn node_edge_additions<L: Into<LayerIter<'a>>>(
        self,
        layer_id: L,
    ) -> storage::NodeEdgeAdditions<'a> {
        NodeRefOps::edge_additions(self, layer_id)
    }

    fn tprop(self, prop_id: usize) -> storage::NodeTProps<'a> {
        NodeRefOps::temporal_prop_layer(self, LayerId(0), prop_id)
    }

    fn num_layers(self) -> usize {
        NodeRefOps::internal_num_layers(&self)
    }

    fn temporal_prop_layer(self, layer_id: LayerId, prop_id: usize) -> storage::NodeTProps<'a> {
        NodeRefOps::temporal_prop_layer(self, layer_id, prop_id)
    }

    fn constant_prop_layer(self, layer_id: LayerId, prop_id: usize) -> Option<Prop> {
        NodeRefOps::c_prop(self, layer_id, prop_id)
    }

    fn temp_prop_rows_range(
        self,
        w: Option<Range<EventTime>>,
        prop_ids: &'a [usize],
        layer_id: LayerId,
    ) -> impl Iterator<Item = (EventTime, Vec<(usize, Prop)>)> {
        NodeRefOps::temp_prop_rows(self, w, prop_ids, layer_id)
    }
}
