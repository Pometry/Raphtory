use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, nodes::node_store::NodeStore, LayerIds, VID},
        storage::ArcEntry,
        Direction, OptionAsStr,
    },
    db::api::view::internal::NodeAdditions,
};
use itertools::Itertools;
use std::ops::Deref;

pub trait NodeStorageOps<'a>: Sized {
    fn degree(self, layers: &LayerIds, dir: Direction) -> usize;

    fn additions(self) -> NodeAdditions<'a>;

    fn edges_iter(self, layers: &'a LayerIds, dir: Direction)
        -> impl Iterator<Item = EdgeRef> + 'a;

    fn node_type_id(self) -> usize;

    fn vid(self) -> VID;

    fn name(self) -> Option<&'a str>;

    fn find_edge(self, dst: VID, layer_ids: &LayerIds) -> Option<EdgeRef>;
}

impl<'a> NodeStorageOps<'a> for &'a NodeStore {
    fn degree(self, layers: &LayerIds, dir: Direction) -> usize {
        self.degree(layers, dir)
    }

    fn additions(self) -> NodeAdditions<'a> {
        NodeAdditions::Mem(self.timestamps())
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

    fn name(self) -> Option<&'a str> {
        self.name.as_str()
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
