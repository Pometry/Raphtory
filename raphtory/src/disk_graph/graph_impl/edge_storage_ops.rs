use crate::{
    core::{
        entities::{LayerIds, VID},
        storage::timeindex::{TimeIndex, TimeIndexOps},
        Prop,
    },
    db::api::storage::graph::{
        edges::edge_storage_ops::{EdgeStorageOps, TimeIndexRef},
        tprop_storage_ops::TPropOps,
        variants::layer_variants::LayerVariants,
    },
};
use pometry_storage::{edge::Edge, tprops::DiskTProp};
use raphtory_api::core::{entities::EID, storage::timeindex::TimeIndexEntry};
use rayon::prelude::*;
use std::{iter, ops::Range};

impl<'a> EdgeStorageOps<'a> for Edge<'a> {
    fn added(self, layer_ids: &LayerIds, w: Range<i64>) -> bool {
        self.has_layer(layer_ids) && {
            match layer_ids {
                LayerIds::None => false,
                LayerIds::All => self
                    .additions_iter(layer_ids.clone())
                    .any(|(_, t_index)| t_index.active_t(w.clone())),
                LayerIds::One(l_id) => self.get_additions::<i64>(*l_id).active_t(w),
                LayerIds::Multiple(layers) => layers
                    .iter()
                    .any(|l_id| self.added(&LayerIds::One(l_id), w.clone())),
            }
        }
    }

    fn has_layer(self, layer_ids: &LayerIds) -> bool {
        match layer_ids {
            LayerIds::None => false,
            LayerIds::All => true,
            LayerIds::One(id) => self.has_layer_inner(*id),
            LayerIds::Multiple(ids) => ids.iter().any(|id| self.has_layer_inner(id)),
        }
    }

    fn src(self) -> VID {
        self.src_id()
    }

    fn dst(self) -> VID {
        self.dst_id()
    }

    fn eid(self) -> EID {
        self.pid()
    }

    fn layer_ids_iter(self, layer_ids: LayerIds) -> impl Iterator<Item = usize> + 'a {
        match layer_ids {
            LayerIds::None => LayerVariants::None(std::iter::empty()),
            LayerIds::All => LayerVariants::All(
                (0..self.internal_num_layers()).filter(move |&l| self.has_layer_inner(l)),
            ),
            LayerIds::One(id) => {
                LayerVariants::One(self.has_layer_inner(id).then_some(id).into_iter())
            }
            LayerIds::Multiple(ids) => {
                LayerVariants::Multiple(ids.into_iter().filter(move |&id| self.has_layer_inner(id)))
            }
        }
    }

    fn layer_ids_par_iter(self, layer_ids: &LayerIds) -> impl ParallelIterator<Item = usize> + 'a {
        match layer_ids {
            LayerIds::None => LayerVariants::None(rayon::iter::empty()),
            LayerIds::All => LayerVariants::All(
                (0..self.internal_num_layers())
                    .into_par_iter()
                    .filter(move |&l| self.has_layer_inner(l)),
            ),
            LayerIds::One(id) => {
                LayerVariants::One(self.has_layer_inner(*id).then_some(*id).into_par_iter())
            }
            LayerIds::Multiple(ids) => {
                LayerVariants::Multiple(ids.par_iter().filter(move |&id| self.has_layer_inner(id)))
            }
        }
    }

    fn deletions_iter(
        self,
        _layer_ids: LayerIds,
    ) -> impl Iterator<Item = (usize, TimeIndexRef<'a>)> + 'a {
        Box::new(iter::empty())
    }

    fn deletions_par_iter(
        self,
        _layer_ids: &LayerIds,
    ) -> impl ParallelIterator<Item = (usize, TimeIndexRef<'a>)> + 'a {
        rayon::iter::empty()
    }

    fn additions(self, layer_id: usize) -> TimeIndexRef<'a> {
        TimeIndexRef::External(self.get_additions::<TimeIndexEntry>(layer_id))
    }

    fn deletions(self, _layer_id: usize) -> TimeIndexRef<'a> {
        TimeIndexRef::Ref(&TimeIndex::Empty)
    }

    fn temporal_prop_layer(self, layer_id: usize, prop_id: usize) -> impl TPropOps<'a> + Sync + 'a {
        self.graph()
            .localize_edge_prop_id(layer_id, prop_id)
            .map(|prop_id| {
                self.graph()
                    .layer(layer_id)
                    .edges_storage()
                    .prop(self.eid(), prop_id)
            })
            .unwrap_or(DiskTProp::empty())
    }

    fn constant_prop_layer(self, _layer_id: usize, _prop_id: usize) -> Option<Prop> {
        // TODO: constant edge properties not implemented in diskgraph yet
        None
    }
}
