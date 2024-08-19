use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, LayerIds, VID},
        storage::raw_edges::EdgeArcGuard,
        Prop,
    },
    db::api::storage::graph::{
        edges::{
            edge_ref::EdgeStorageRef,
            edge_storage_ops::{EdgeStorageIntoOps, EdgeStorageOps, TimeIndexRef},
        },
        tprop_storage_ops::TPropOps,
    },
};
use raphtory_api::core::storage::timeindex::TimeIndexEntry;
use rayon::iter::ParallelIterator;
use std::ops::Range;

#[cfg(feature = "storage")]
use crate::db::api::storage::graph::variants::storage_variants::StorageVariants;
#[cfg(feature = "storage")]
use crate::disk_graph::storage_interface::edge::DiskOwnedEdge;

#[derive(Debug, Clone)]
pub enum EdgeOwnedEntry {
    Mem(EdgeArcGuard),
    #[cfg(feature = "storage")]
    Disk(DiskOwnedEdge),
}

#[cfg(feature = "storage")]
macro_rules! for_all_variants {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            EdgeOwnedEntry::Mem($pattern) => StorageVariants::Mem($result),
            EdgeOwnedEntry::Disk($pattern) => StorageVariants::Disk($result),
        }
    };
}

#[cfg(not(feature = "storage"))]
macro_rules! for_all_variants {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            EdgeOwnedEntry::Mem($pattern) => $result,
        }
    };
}

impl EdgeOwnedEntry {
    pub fn as_ref(&self) -> EdgeStorageRef {
        match self {
            EdgeOwnedEntry::Mem(entry) => EdgeStorageRef::Mem(entry.as_mem_edge()),
            #[cfg(feature = "storage")]
            EdgeOwnedEntry::Disk(entry) => EdgeStorageRef::Disk(entry.as_ref()),
        }
    }
}

impl<'a> EdgeStorageOps<'a> for &'a EdgeOwnedEntry {
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
    ) -> impl Iterator<Item = (usize, impl TPropOps<'a>)> + 'a {
        self.as_ref().temporal_prop_iter(layer_ids, prop_id)
    }

    fn temporal_prop_par_iter(
        self,
        layer_ids: &'a LayerIds,
        prop_id: usize,
    ) -> impl ParallelIterator<Item = (usize, impl TPropOps<'a>)> + 'a {
        self.as_ref().temporal_prop_par_iter(layer_ids, prop_id)
    }

    fn constant_prop_layer(self, layer_id: usize, prop_id: usize) -> Option<Prop> {
        self.as_ref().constant_prop_layer(layer_id, prop_id)
    }
}

impl EdgeStorageIntoOps for EdgeOwnedEntry {
    fn into_layers(
        self,
        layer_ids: LayerIds,
        eref: EdgeRef,
    ) -> impl Iterator<Item = EdgeRef> + Send {
        for_all_variants!(self, edge => edge.into_layers(layer_ids, eref))
    }

    fn into_exploded(
        self,
        layer_ids: LayerIds,
        eref: EdgeRef,
    ) -> impl Iterator<Item = EdgeRef> + Send {
        for_all_variants!(self, edge => edge.into_exploded(layer_ids, eref))
    }

    fn into_exploded_window(
        self,
        layer_ids: LayerIds,
        w: Range<TimeIndexEntry>,
        eref: EdgeRef,
    ) -> impl Iterator<Item = EdgeRef> + Send {
        for_all_variants!(self, edge => edge.into_exploded_window(layer_ids, w, eref))
    }
}
