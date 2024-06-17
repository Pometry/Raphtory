use std::borrow::Cow;

use crate::{
    core::{
        entities::{
            edges::edge_ref::EdgeRef, nodes::node_store::NodeStore, properties::tprop::TProp,
            LayerIds, VID,
        },
        storage::ArcEntry,
        Direction, OptionAsStr,
    },
    db::api::{storage::tprop_storage_ops::TPropOps, view::internal::NodeAdditions},
};
use itertools::Itertools;

pub trait NodeStorageOps<'a>: Sized {
    fn degree(self, layers: &LayerIds, dir: Direction) -> usize;

    fn additions(self) -> NodeAdditions<'a>;

    fn tprop(self, prop_id: usize) -> impl TPropOps<'a>;

    fn edges_iter(self, layers: &'a LayerIds, dir: Direction)
        -> impl Iterator<Item = EdgeRef> + 'a;

    fn node_type_id(self) -> usize;

    fn vid(self) -> VID;

    fn id(self) -> u64;

    fn name(self) -> Option<Cow<'a, str>>;

    fn find_edge(self, dst: VID, layer_ids: &LayerIds) -> Option<EdgeRef>;
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

    fn id(self) -> u64 {
        self.global_id
    }

    fn name(self) -> Option<Cow<'a, str>> {
        self.name.as_str().map(Cow::from)
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
