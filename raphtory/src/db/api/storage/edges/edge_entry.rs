use crate::{
    core::{
        entities::{
            edges::{edge_ref::EdgeRef, edge_store::EdgeStore},
            LayerIds, VID,
        },
        storage::Entry,
    },
    db::api::storage::edges::{
        edge_ref::EdgeStorageRef,
        edge_storage_ops::{EdgeStorageOps, TimeIndexRef},
    },
};

#[cfg(feature = "storage")]
use crate::disk_graph::storage_interface::edge::DiskEdge;

use crate::db::api::storage::tprop_storage_ops::TPropOps;
use rayon::prelude::*;
use std::ops::Range;

#[derive(Debug)]
pub enum EdgeStorageEntry<'a> {
    Mem(Entry<'a, EdgeStore>),
    #[cfg(feature = "storage")]
    Disk(DiskEdge<'a>),
}

impl<'a> EdgeStorageEntry<'a> {
    pub fn as_ref(&self) -> EdgeStorageRef {
        match self {
            EdgeStorageEntry::Mem(edge) => EdgeStorageRef::Mem(edge),
            #[cfg(feature = "storage")]
            EdgeStorageEntry::Disk(edge) => EdgeStorageRef::Disk(*edge),
        }
    }
}

impl<'a, 'b: 'a> EdgeStorageOps<'a> for &'a EdgeStorageEntry<'b> {
    fn in_ref(self) -> EdgeRef {
        self.as_ref().in_ref()
    }

    fn out_ref(self) -> EdgeRef {
        self.as_ref().out_ref()
    }

    fn active(self, layer_ids: &LayerIds, w: Range<i64>) -> bool {
        self.as_ref().active(layer_ids, w)
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

    fn layer_ids_iter(self, layer_ids: &'a LayerIds) -> impl Iterator<Item = usize> + 'a {
        self.as_ref().layer_ids_iter(layer_ids)
    }

    fn layer_ids_par_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl ParallelIterator<Item = usize> + 'a {
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
        layer_ids: &'a LayerIds,
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
        layer_ids: &'a LayerIds,
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
        layer_ids: &'a LayerIds,
    ) -> impl ParallelIterator<Item = (usize, TimeIndexRef<'a>, TimeIndexRef<'a>)> + 'a {
        self.as_ref().updates_par_iter(layer_ids)
    }

    fn additions(self, layer_id: usize) -> TimeIndexRef<'a> {
        self.as_ref().additions(layer_id)
    }

    fn deletions(self, layer_id: usize) -> TimeIndexRef<'a> {
        self.as_ref().deletions(layer_id)
    }

    fn has_temporal_prop(self, layer_ids: &LayerIds, prop_id: usize) -> bool {
        self.as_ref().has_temporal_prop(layer_ids, prop_id)
    }

    fn temporal_prop_layer(self, layer_id: usize, prop_id: usize) -> impl TPropOps<'a> + Sync + 'a {
        self.as_ref().temporal_prop_layer(layer_id, prop_id)
    }

    fn temporal_prop_iter(
        self,
        layer_ids: &'a LayerIds,
        prop_id: usize,
    ) -> impl Iterator<Item = (usize, impl TPropOps)> + 'a {
        self.as_ref().temporal_prop_iter(layer_ids, prop_id)
    }

    fn temporal_prop_par_iter(
        self,
        layer_ids: &'a LayerIds,
        prop_id: usize,
    ) -> impl ParallelIterator<Item = (usize, impl TPropOps)> + 'a {
        self.as_ref().temporal_prop_par_iter(layer_ids, prop_id)
    }
}
