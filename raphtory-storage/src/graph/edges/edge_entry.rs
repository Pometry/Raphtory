use crate::graph::edges::edge_storage_ops::EdgeStorageOps;
use raphtory_api::core::entities::properties::{prop::Prop, tprop::TPropOps};
use raphtory_core::entities::{LayerIds, EID, VID};
use std::ops::Range;
use raphtory_api::core::entities::LayerId;
use storage::{api::edges::EdgeEntryOps, EdgeEntry, EdgeEntryRef};

#[derive(Debug)]
pub enum EdgeStorageEntry<'a> {
    Mem(EdgeEntryRef<'a>),
    Unlocked(EdgeEntry<'a>),
}

impl<'a> EdgeStorageEntry<'a> {
    #[inline]
    pub fn as_ref(&self) -> EdgeEntryRef<'_> {
        match self {
            EdgeStorageEntry::Mem(edge) => *edge,
            EdgeStorageEntry::Unlocked(edge) => edge.as_ref(),
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

    fn layer_ids_iter(self, layer_ids: &'a LayerIds) -> impl Iterator<Item = LayerId> + 'a {
        self.as_ref().layer_ids_iter(layer_ids)
    }

    fn additions_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl Iterator<Item = (LayerId, storage::EdgeAdditions<'a>)> + 'a {
        self.as_ref().additions_iter(layer_ids)
    }

    fn deletions_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl Iterator<Item = (LayerId, storage::EdgeDeletions<'a>)> + 'a {
        self.as_ref().deletions_iter(layer_ids)
    }

    fn updates_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl Iterator<
        Item = (
            LayerId,
            storage::EdgeAdditions<'a>,
            storage::EdgeDeletions<'a>,
        ),
    > + 'a {
        self.as_ref().updates_iter(layer_ids)
    }

    fn additions(self, layer_id: LayerId) -> storage::EdgeAdditions<'a> {
        self.as_ref().additions(layer_id)
    }

    fn deletions(self, layer_id: LayerId) -> storage::EdgeDeletions<'a> {
        self.as_ref().deletions(layer_id)
    }

    fn temporal_prop_layer(self, layer_id: LayerId, prop_id: usize) -> impl TPropOps<'a> + 'a {
        self.as_ref().temporal_prop_layer(layer_id, prop_id)
    }

    fn temporal_prop_iter(
        self,
        layer_ids: &'a LayerIds,
        prop_id: usize,
    ) -> impl Iterator<Item = (LayerId, impl TPropOps<'a>)> + 'a {
        self.as_ref().temporal_prop_iter(layer_ids, prop_id)
    }

    fn metadata_layer(self, layer_id: LayerId, prop_id: usize) -> Option<Prop> {
        self.as_ref().metadata_layer(layer_id, prop_id)
    }
}
