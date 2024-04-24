use crate::{
    core::entities::{
        edges::{edge_ref::EdgeRef, edge_store::EdgeStore},
        properties::tprop::TPropOps,
        LayerIds, EID, VID,
    },
    db::api::storage::{
        arrow::edges::ArrowEdge,
        edge_storage_ops::{EdgeStorageOps, TimeIndexLike},
    },
};
use rayon::{iter::Either, prelude::*};
use std::ops::Range;

macro_rules! for_all {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            EdgeStorageRef::Mem($pattern) => $result,
            #[cfg(feature = "arrow")]
            EdgeStorageRef::Arrow($pattern) => $result,
        }
    };
}

#[cfg(feature = "arrow")]
macro_rules! for_all_iter {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            EdgeStorageRef::Mem($pattern) => Either::Left($result),
            EdgeStorageRef::Arrow($pattern) => Either::Right($result),
        }
    };
}

#[cfg(not(feature = "arrow"))]
macro_rules! for_all_iter {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            EdgeStorageRef::Mem($pattern) => $result,
        }
    };
}

#[derive(Copy, Clone, Debug)]
pub enum EdgeStorageRef<'a> {
    Mem(&'a EdgeStore),
    #[cfg(feature = "arrow")]
    Arrow(ArrowEdge<'a>),
}

impl<'a> EdgeStorageRef<'a> {
    #[inline]
    pub fn eid(&self) -> EID {
        match self {
            EdgeStorageRef::Mem(e) => e.eid,
            #[cfg(feature = "arrow")]
            EdgeStorageRef::Arrow(e) => e.eid(),
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

    fn additions_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> Box<dyn Iterator<Item = TimeIndexLike<'a>> + 'a> {
        for_all!(self, edge => EdgeStorageOps::additions_iter(edge, layer_ids))
    }

    fn additions_par_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl ParallelIterator<Item = TimeIndexLike<'a>> + 'a {
        for_all_iter!(self, edge => EdgeStorageOps::additions_par_iter(edge, layer_ids))
    }

    fn deletions_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> Box<dyn Iterator<Item = TimeIndexLike<'a>> + 'a> {
        for_all!(self, edge => EdgeStorageOps::deletions_iter(edge, layer_ids))
    }

    fn deletions_par_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl ParallelIterator<Item = TimeIndexLike<'a>> + 'a {
        for_all_iter!(self, edge => EdgeStorageOps::deletions_par_iter(edge, layer_ids))
    }

    fn updates_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl Iterator<Item = (usize, TimeIndexLike<'a>, TimeIndexLike<'a>)> + 'a {
        for_all_iter!(self, edge => EdgeStorageOps::updates_iter(edge, layer_ids))
    }

    fn updates_par_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl ParallelIterator<Item = (usize, TimeIndexLike<'a>, TimeIndexLike<'a>)> + 'a {
        for_all_iter!(self, edge => EdgeStorageOps::updates_par_iter(edge, layer_ids))
    }

    fn additions(self, layer_id: usize) -> TimeIndexLike<'a> {
        for_all!(self, edge => EdgeStorageOps::additions(edge, layer_id))
    }

    fn deletions(self, layer_id: usize) -> TimeIndexLike<'a> {
        for_all!(self, edge => EdgeStorageOps::deletions(edge, layer_id))
    }

    fn has_temporal_prop(self, layer_ids: &LayerIds, prop_id: usize) -> bool {
        for_all!(self, edge => EdgeStorageOps::has_temporal_prop(edge, layer_ids, prop_id))
    }

    fn temporal_prop_layer(
        self,
        layer_id: usize,
        prop_id: usize,
    ) -> Option<Box<dyn TPropOps + 'a>> {
        for_all!(self, edge => EdgeStorageOps::temporal_prop_layer(edge, layer_id, prop_id))
    }
}
