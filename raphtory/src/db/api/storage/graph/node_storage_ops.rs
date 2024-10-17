use std::borrow::Cow;

use crate::{
    core::{
        entities::{
            edges::edge_ref::EdgeRef, nodes::node_store::NodeStore, properties::tprop::TProp,
            GidRef, LayerIds, VID,
        },
        storage::ArcEntry,
        Direction,
    },
    db::api::{storage::graph::tprop_storage_ops::TPropOps, view::internal::NodeAdditions},
    prelude::Prop,
};
use itertools::Itertools;

pub trait NodeStorageOps<'a>: Sized {
    fn degree(self, layers: &LayerIds, dir: Direction) -> usize;

    fn additions(self) -> NodeAdditions<'a>;

    fn tprop(self, prop_id: usize) -> impl TPropOps<'a>;

    fn prop(self, prop_id: usize) -> Option<Prop>;

    fn edges_iter(self, layers: &'a LayerIds, dir: Direction)
        -> impl Iterator<Item = EdgeRef> + 'a;

    fn node_type_id(self) -> usize;

    fn vid(self) -> VID;

    fn id(self) -> GidRef<'a>;

    fn name(self) -> Option<Cow<'a, str>>;

    fn find_edge(self, dst: VID, layer_ids: &LayerIds) -> Option<EdgeRef>;
}


impl<'a> NodeStorageOps<'a> for NodeStorageRef<'a> {
    fn degree(self, layers: &LayerIds, dir: Direction) -> usize {
        for_all!(self, node => node.degree(layers, dir))
    }

    fn additions(self) -> NodeAdditions<'a> {
        for_all!(self, node => node.additions())
    }

    fn tprop(self, prop_id: usize) -> impl TPropOps<'a> {
        for_all_iter!(self, node => node.tprop(prop_id))
    }

    fn edges_iter(
        self,
        layers: &'a LayerIds,
        dir: Direction,
    ) -> impl Iterator<Item = EdgeRef> + 'a {
        for_all_iter!(self, node => node.edges_iter(layers, dir))
    }

    fn node_type_id(self) -> usize {
        for_all!(self, node => node.node_type_id())
    }

    fn vid(self) -> VID {
        for_all!(self, node => node.vid())
    }

    fn id(self) -> GidRef<'a> {
        for_all!(self, node => node.id())
    }

    fn name(self) -> Option<Cow<'a, str>> {
        for_all!(self, node => node.name())
    }

    fn find_edge(self, dst: VID, layer_ids: &LayerIds) -> Option<EdgeRef> {
        for_all!(self, node => NodeStorageOps::find_edge(node, dst, layer_ids))
    }

    fn prop(self, prop_id: usize) -> Option<Prop> {
        for_all!(self, node => node.prop(prop_id))
    }
}

impl<'a> NodeStorageOps<'a> for &'a NodeStore {
    fn degree(self, layers: &LayerIds, dir: Direction) -> usize {
        self.degree(layers, dir)
    }

    fn additions(self) -> NodeAdditions<'a> {
        NodeAdditions::Mem(self.timestamps())
    }

    fn tprop(self, prop_id: usize) -> impl TPropOps<'a> {
        self.temporal_property(prop_id).unwrap_or(&TProp::Empty)
    }

    fn prop(self, prop_id: usize) -> Option<Prop> {
        self.constant_property(prop_id).cloned()
    }

    fn edges_iter(
        self,
        layers: &'a LayerIds,
        dir: Direction,
    ) -> impl Iterator<Item = EdgeRef> + 'a {
        self.edge_tuples(layers, dir)
    }

    fn node_type_id(self) -> usize {
        self.node_type
    }

    fn vid(self) -> VID {
        self.vid
    }

    fn id(self) -> GidRef<'a> {
        (&self.global_id).into()
    }

    fn name(self) -> Option<Cow<'a, str>> {
        self.global_id.as_str().map(Cow::from)
    }

    fn find_edge(self, dst: VID, layer_ids: &LayerIds) -> Option<EdgeRef> {
        let eid = NodeStore::find_edge_eid(self, dst, layer_ids)?;
        Some(EdgeRef::new_outgoing(eid, self.vid, dst))
    }
}

pub trait NodeStorageIntoOps: Sized {
    fn into_edges_iter(self, layers: LayerIds, dir: Direction) -> impl Iterator<Item = EdgeRef>;

    fn into_neighbours_iter(self, layers: LayerIds, dir: Direction) -> impl Iterator<Item = VID> {
        self.into_edges_iter(layers, dir)
            .map(|e| e.remote())
            .dedup()
    }
}

impl NodeStorageIntoOps for ArcEntry<NodeStore> {
    fn into_edges_iter(self, layers: LayerIds, dir: Direction) -> impl Iterator<Item = EdgeRef> {
        self.into_edges(&layers, dir)
    }

    fn into_neighbours_iter(self, layers: LayerIds, dir: Direction) -> impl Iterator<Item = VID> {
        self.into_neighbours(&layers, dir)
    }
}
