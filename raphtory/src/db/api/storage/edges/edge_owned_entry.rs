use crate::{
    core::{
        entities::{
            edges::{edge_ref::EdgeRef, edge_store::EdgeStore},
            properties::tprop::TPropOps,
            LayerIds, VID,
        },
        storage::ArcEntry,
    },
    db::api::storage::{
        edge_storage_ops::{EdgeStorageOps, TimeIndexLike},
        edges::edge_ref::EdgeStorageRef,
    },
};

#[cfg(feature = "arrow")]
use crate::db::api::storage::arrow::edges::ArrowOwnedEdge;

use rayon::iter::ParallelIterator;
use std::ops::Range;

pub enum EdgeOwnedEntry {
    Mem(ArcEntry<EdgeStore>),
    #[cfg(feature = "arrow")]
    Arrow(ArrowOwnedEdge),
}

impl EdgeOwnedEntry {
    pub fn as_ref(&self) -> EdgeStorageRef {
        match self {
            EdgeOwnedEntry::Mem(entry) => EdgeStorageRef::Mem(entry),
            #[cfg(feature = "arrow")]
            EdgeOwnedEntry::Arrow(entry) => EdgeStorageRef::Arrow(entry.as_ref()),
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

    fn additions_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> Box<dyn Iterator<Item = TimeIndexLike<'a>> + 'a> {
        self.as_ref().additions_iter(layer_ids)
    }

    fn additions_par_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl ParallelIterator<Item = TimeIndexLike<'a>> + 'a {
        self.as_ref().additions_par_iter(layer_ids)
    }

    fn deletions_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> Box<dyn Iterator<Item = TimeIndexLike<'a>> + 'a> {
        self.as_ref().deletions_iter(layer_ids)
    }

    fn deletions_par_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl ParallelIterator<Item = TimeIndexLike<'a>> + 'a {
        self.as_ref().deletions_par_iter(layer_ids)
    }

    fn updates_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl Iterator<Item = (usize, TimeIndexLike<'a>, TimeIndexLike<'a>)> + 'a {
        self.as_ref().updates_iter(layer_ids)
    }

    fn updates_par_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl ParallelIterator<Item = (usize, TimeIndexLike<'a>, TimeIndexLike<'a>)> + 'a {
        self.as_ref().updates_par_iter(layer_ids)
    }

    fn additions(self, layer_id: usize) -> TimeIndexLike<'a> {
        self.as_ref().additions(layer_id)
    }

    fn deletions(self, layer_id: usize) -> TimeIndexLike<'a> {
        self.as_ref().deletions(layer_id)
    }

    fn has_temporal_prop(self, layer_ids: &LayerIds, prop_id: usize) -> bool {
        self.as_ref().has_temporal_prop(layer_ids, prop_id)
    }

    fn temporal_prop_layer(
        self,
        layer_id: usize,
        prop_id: usize,
    ) -> Option<Box<dyn TPropOps + 'a>> {
        self.as_ref().temporal_prop_layer(layer_id, prop_id)
    }
}
