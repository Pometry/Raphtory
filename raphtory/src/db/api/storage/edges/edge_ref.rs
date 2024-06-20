use std::ops::Range;

use rayon::prelude::*;

#[cfg(feature = "storage")]
use crate::db::api::storage::variants::storage_variants::StorageVariants;
#[cfg(feature = "storage")]
use crate::disk_graph::storage_interface::edge::DiskEdge;
use crate::{
    core::entities::{edges::edge_ref::EdgeRef, LayerIds, EID, VID},
    db::api::storage::{
        edges::edge_storage_ops::{EdgeStorageOps, TimeIndexRef},
        tprop_storage_ops::TPropOps,
    },
};

use super::edge_storage_ops::MemEdge;

macro_rules! for_all {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            EdgeStorageRef::Mem($pattern) => $result,
            #[cfg(feature = "storage")]
            EdgeStorageRef::Disk($pattern) => $result,
        }
    };
}

#[cfg(feature = "storage")]
macro_rules! for_all_iter {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            EdgeStorageRef::Mem($pattern) => StorageVariants::Mem($result),
            EdgeStorageRef::Disk($pattern) => StorageVariants::Disk($result),
        }
    };
}

#[cfg(not(feature = "storage"))]
macro_rules! for_all_iter {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            EdgeStorageRef::Mem($pattern) => $result,
        }
    };
}

#[derive(Copy, Clone, Debug)]
pub enum EdgeStorageRef<'a> {
    Mem(MemEdge<'a>),
    #[cfg(feature = "storage")]
    Disk(DiskEdge<'a>),
}

impl<'a> EdgeStorageRef<'a> {
    #[inline]
    pub fn eid(&self) -> EID {
        match self {
            EdgeStorageRef::Mem(e) => e.eid(),
            #[cfg(feature = "storage")]
            EdgeStorageRef::Disk(e) => e.eid(),
        }
    }
}

impl<'a> EdgeStorageOps<'a> for EdgeStorageRef<'a> {
    fn in_ref(self) -> EdgeRef {
        for_all!(self, edge => EdgeStorageOps::in_ref(edge))
    }

    fn out_ref(self) -> EdgeRef {
        for_all!(self, edge => EdgeStorageOps::out_ref(edge))
    }

    fn active(self, layer_ids: &LayerIds, w: Range<i64>) -> bool {
        for_all!(self, edge => EdgeStorageOps::active(edge, layer_ids, w))
    }

    fn has_layer(self, layer_ids: &LayerIds) -> bool {
        for_all!(self, edge => EdgeStorageOps::has_layer(edge, layer_ids))
    }

    fn src(self) -> VID {
        for_all!(self, edge => edge.src())
    }

    fn dst(self) -> VID {
        for_all!(self, edge => edge.dst())
    }

    fn layer_ids_iter(self, layer_ids: &'a LayerIds) -> impl Iterator<Item = usize> + 'a {
        for_all_iter!(self, edge => EdgeStorageOps::layer_ids_iter(edge, layer_ids))
    }

    fn layer_ids_par_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl ParallelIterator<Item = usize> + 'a {
        for_all_iter!(self, edge => EdgeStorageOps::layer_ids_par_iter(edge, layer_ids))
    }

    fn additions_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl Iterator<Item = (usize, TimeIndexRef<'a>)> + 'a {
        for_all_iter!(self, edge => EdgeStorageOps::additions_iter(edge, layer_ids))
    }

    fn additions_par_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl ParallelIterator<Item = (usize, TimeIndexRef<'a>)> + 'a {
        for_all_iter!(self, edge => EdgeStorageOps::additions_par_iter(edge, layer_ids))
    }

    fn deletions_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl Iterator<Item = (usize, TimeIndexRef<'a>)> + 'a {
        for_all_iter!(self, edge => EdgeStorageOps::deletions_iter(edge, layer_ids))
    }

    fn deletions_par_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl ParallelIterator<Item = (usize, TimeIndexRef<'a>)> + 'a {
        for_all_iter!(self, edge => EdgeStorageOps::deletions_par_iter(edge, layer_ids))
    }

    fn updates_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl Iterator<Item = (usize, TimeIndexRef<'a>, TimeIndexRef<'a>)> + 'a {
        for_all_iter!(self, edge => EdgeStorageOps::updates_iter(edge, layer_ids))
    }

    fn updates_par_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl ParallelIterator<Item = (usize, TimeIndexRef<'a>, TimeIndexRef<'a>)> + 'a {
        for_all_iter!(self, edge => EdgeStorageOps::updates_par_iter(edge, layer_ids))
    }

    fn additions(self, layer_id: usize) -> TimeIndexRef<'a> {
        for_all!(self, edge => EdgeStorageOps::additions(edge, layer_id))
    }

    fn deletions(self, layer_id: usize) -> TimeIndexRef<'a> {
        for_all!(self, edge => EdgeStorageOps::deletions(edge, layer_id))
    }

    fn has_temporal_prop(self, layer_ids: &LayerIds, prop_id: usize) -> bool {
        for_all!(self, edge => EdgeStorageOps::has_temporal_prop(edge, layer_ids, prop_id))
    }

    fn temporal_prop_layer(self, layer_id: usize, prop_id: usize) -> impl TPropOps<'a> + Sync + 'a {
        for_all_iter!(self, edge => edge.temporal_prop_layer(layer_id, prop_id))
    }
}
