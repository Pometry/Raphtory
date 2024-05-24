use crate::{
    arrow::graph_impl::tprops::read_tprop_column,
    core::{
        entities::{edges::edge_ref::EdgeRef, LayerIds, VID},
        storage::timeindex::{TimeIndex, TimeIndexOps},
    },
    db::api::storage::{
        edges::edge_storage_ops::{EdgeStorageOps, TimeIndexRef},
        tprop_storage_ops::TPropOps,
    },
    prelude::TimeIndexEntry,
};
use raphtory_arrow::{edge::Edge, tprops::ArrowTProp};
use rayon::prelude::*;
use std::{iter, ops::Range};

impl<'a> EdgeStorageOps<'a> for Edge<'a> {
    fn in_ref(self) -> EdgeRef {
        EdgeRef::new_incoming(
            self.eid(),
            self.src_id(),
            self.dst_id(),
        )
        .at_layer(self.layer_id())
    }

    fn out_ref(self) -> EdgeRef {
        EdgeRef::new_outgoing(
            self.eid(),
            self.src_id(),
            self.dst_id(),
        )
        .at_layer(self.layer_id())
    }

    fn active(self, layer_ids: &LayerIds, w: Range<i64>) -> bool {
        // this is probably heavy
        self.has_layer(layer_ids) && self.timestamps::<i64>().active_t(w)
    }

    fn has_layer(self, layer_ids: &LayerIds) -> bool {
        layer_ids.contains(&self.layer_id())
    }

    fn src(self) -> VID {
        self.src_id()
    }

    fn dst(self) -> VID {
        self.dst_id()
    }

    fn layer_ids_iter(self, layer_ids: &'a LayerIds) -> impl Iterator<Item = usize> + 'a {
        layer_ids
            .contains(&self.layer_id())
            .then_some(self.layer_id())
            .into_iter()
    }

    fn layer_ids_par_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl ParallelIterator<Item = usize> + 'a {
        layer_ids
            .contains(&self.layer_id())
            .then_some(self.layer_id())
            .into_par_iter()
    }

    fn deletions_iter(
        self,
        _layer_ids: &'a LayerIds,
    ) -> impl Iterator<Item = (usize, TimeIndexRef<'a>)> + 'a {
        Box::new(iter::empty())
    }

    fn deletions_par_iter(
        self,
        _layer_ids: &'a LayerIds,
    ) -> impl ParallelIterator<Item = (usize, TimeIndexRef<'a>)> + 'a {
        rayon::iter::empty()
    }

    fn additions(self, layer_id: usize) -> TimeIndexRef<'a> {
        if self.layer_id() != layer_id {
            TimeIndexRef::Ref(&TimeIndex::Empty)
        } else {
            TimeIndexRef::External(self.timestamps::<TimeIndexEntry>())
        }
    }

    fn deletions(self, _layer_id: usize) -> TimeIndexRef<'a> {
        TimeIndexRef::Ref(&TimeIndex::Empty)
    }

    fn has_temporal_prop(self, layer_ids: &LayerIds, prop_id: usize) -> bool {
        layer_ids.contains(&self.layer_id()) && self.has_temporal_prop_inner(prop_id)
    }

    fn temporal_prop_layer(
        self,
        layer_id: usize,
        prop_id: usize,
    ) -> impl TPropOps<'a> + Sync + 'a {
        if layer_id == self.layer_id() {
            self.temporal_property_field(prop_id)
                .and_then(|field| read_tprop_column(prop_id, field, self))
        } else {
            None
        }
        .unwrap_or(ArrowTProp::empty())
    }
}
