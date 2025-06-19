use crate::graph::edges::edge_storage_ops::{EdgeStorageOps, TimeIndexRef};
use raphtory_api::core::entities::{
    properties::{prop::Prop, tprop::TPropOps},
    LayerIds, EID, VID,
};
use raphtory_core::entities::edges::edge_store::MemEdge;
use std::ops::Range;
use storage::{EdgeEntry, EdgeEntryRef};

#[cfg(feature = "storage")]
use crate::{disk::graph_impl::DiskEdge, graph::variants::storage_variants2::StorageVariants2};

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
            EdgeStorageRef::Mem($pattern) => StorageVariants2::Mem($result),
            EdgeStorageRef::Disk($pattern) => StorageVariants2::Disk($result),
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
    Mem(EdgeEntryRef<'a>),
    #[cfg(feature = "storage")]
    Disk(DiskEdge<'a>),
}

impl<'a> EdgeStorageOps<'a> for EdgeStorageRef<'a> {
    fn added(self, layer_ids: &LayerIds, w: Range<i64>) -> bool {
        for_all!(self, edge => EdgeStorageOps::added(edge, layer_ids, w))
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

    fn eid(self) -> EID {
        for_all!(self, edge => edge.eid())
    }

    fn layer_ids_iter(self, layer_ids: &'a LayerIds) -> impl Iterator<Item = usize> + 'a {
        for_all_iter!(self, edge => EdgeStorageOps::layer_ids_iter(edge, layer_ids))
    }

    fn additions_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl Iterator<Item = (usize, TimeIndexRef<'a>)> + 'a {
        for_all_iter!(self, edge => EdgeStorageOps::additions_iter(edge, layer_ids))
    }

    fn deletions_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl Iterator<Item = (usize, TimeIndexRef<'a>)> + 'a {
        for_all_iter!(self, edge => EdgeStorageOps::deletions_iter(edge, layer_ids))
    }

    fn updates_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl Iterator<Item = (usize, TimeIndexRef<'a>, TimeIndexRef<'a>)> + 'a {
        for_all_iter!(self, edge => EdgeStorageOps::updates_iter(edge, layer_ids))
    }

    fn additions(self, layer_id: usize) -> TimeIndexRef<'a> {
        for_all!(self, edge => EdgeStorageOps::additions(edge, layer_id))
    }

    fn deletions(self, layer_id: usize) -> TimeIndexRef<'a> {
        for_all!(self, edge => EdgeStorageOps::deletions(edge, layer_id))
    }

    fn temporal_prop_layer(self, layer_id: usize, prop_id: usize) -> impl TPropOps<'a> + 'a {
        for_all_iter!(self, edge => edge.temporal_prop_layer(layer_id, prop_id))
    }

    fn constant_prop_layer(self, layer_id: usize, prop_id: usize) -> Option<Prop> {
        for_all!(self, edge => edge.constant_prop_layer(layer_id, prop_id))
    }
}
