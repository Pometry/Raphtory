use crate::graph::edges::{
    edge_ref::EdgeStorageRef,
    edge_storage_ops::{EdgeStorageOps, TimeIndexRef},
};
use raphtory_api::core::entities::{
    properties::{prop::Prop, tprop::TPropOps},
    LayerIds, EID, VID,
};
use raphtory_core::{entities::edges::edge_store::MemEdge, storage::raw_edges::EdgeRGuard};
use rayon::prelude::*;
use std::ops::Range;

#[cfg(feature = "storage")]
use crate::disk::graph_impl::DiskEdge;

#[derive(Debug)]
pub enum EdgeStorageEntry<'a> {
    Mem(MemEdge<'a>),
    Unlocked(EdgeRGuard<'a>),
    #[cfg(feature = "storage")]
    Disk(DiskEdge<'a>),
}

impl<'a> EdgeStorageEntry<'a> {
    #[inline]
    pub fn as_ref(&self) -> EdgeStorageRef {
        match self {
            EdgeStorageEntry::Mem(edge) => EdgeStorageRef::Mem(*edge),
            EdgeStorageEntry::Unlocked(edge) => EdgeStorageRef::Mem(edge.as_mem_edge()),
            #[cfg(feature = "storage")]
            EdgeStorageEntry::Disk(edge) => EdgeStorageRef::Disk(*edge),
        }
    }
}

impl<'a, 'b: 'a> EdgeStorageOps<'a> for &'a EdgeStorageEntry<'b> {
    fn added(self, layer_ids: &LayerIds, w: Range<i64>) -> bool {
        self.as_ref().added(layer_ids, w)
    }

    fn has_layer(self, layer_ids: &LayerIds) -> bool {
        self.as_ref().has_layer(layer_ids)
    }

    fn src(self) -> VID {
        self.as_ref().src()
    }

    fn dst(self) -> VID {
        self.as_ref().dst()
    }

    fn eid(self) -> EID {
        self.as_ref().eid()
    }

    fn layer_ids_iter(self, layer_ids: &'a LayerIds) -> impl Iterator<Item = usize> + 'a {
        self.as_ref().layer_ids_iter(layer_ids)
    }

    fn layer_ids_par_iter(self, layer_ids: &LayerIds) -> impl ParallelIterator<Item = usize> + 'a {
        self.as_ref().layer_ids_par_iter(layer_ids)
    }

    fn additions_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl Iterator<Item = (usize, TimeIndexRef<'a>)> + 'a {
        self.as_ref().additions_iter(layer_ids)
    }

    fn additions_par_iter(
        self,
        layer_ids: &LayerIds,
    ) -> impl ParallelIterator<Item = (usize, TimeIndexRef<'a>)> + 'a {
        self.as_ref().additions_par_iter(layer_ids)
    }

    fn deletions_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl Iterator<Item = (usize, TimeIndexRef<'a>)> + 'a {
        self.as_ref().deletions_iter(layer_ids)
    }

    fn deletions_par_iter(
        self,
        layer_ids: &LayerIds,
    ) -> impl ParallelIterator<Item = (usize, TimeIndexRef<'a>)> + 'a {
        self.as_ref().deletions_par_iter(layer_ids)
    }

    fn updates_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl Iterator<Item = (usize, TimeIndexRef<'a>, TimeIndexRef<'a>)> + 'a {
        self.as_ref().updates_iter(layer_ids)
    }

    fn updates_par_iter(
        self,
        layer_ids: &LayerIds,
    ) -> impl ParallelIterator<Item = (usize, TimeIndexRef<'a>, TimeIndexRef<'a>)> + 'a {
        self.as_ref().updates_par_iter(layer_ids)
    }

    fn additions(self, layer_id: usize) -> TimeIndexRef<'a> {
        self.as_ref().additions(layer_id)
    }

    fn deletions(self, layer_id: usize) -> TimeIndexRef<'a> {
        self.as_ref().deletions(layer_id)
    }

    fn temporal_prop_layer(self, layer_id: usize, prop_id: usize) -> impl TPropOps<'a> + 'a {
        self.as_ref().temporal_prop_layer(layer_id, prop_id)
    }

    fn temporal_prop_iter(
        self,
        layer_ids: &'a LayerIds,
        prop_id: usize,
    ) -> impl Iterator<Item = (usize, impl TPropOps<'a>)> + 'a {
        self.as_ref().temporal_prop_iter(layer_ids, prop_id)
    }

    fn temporal_prop_par_iter(
        self,
        layer_ids: &LayerIds,
        prop_id: usize,
    ) -> impl ParallelIterator<Item = (usize, impl TPropOps<'a>)> + 'a {
        self.as_ref().temporal_prop_par_iter(layer_ids, prop_id)
    }

    fn constant_prop_layer(self, layer_id: usize, prop_id: usize) -> Option<Prop> {
        self.as_ref().constant_prop_layer(layer_id, prop_id)
    }
}
