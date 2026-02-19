use std::{borrow::Borrow, ops::Range};

use either::Either;
use itertools::Itertools;
use raphtory_api::core::entities::LayerId;
use raphtory_api::core::entities::properties::{prop::Prop, tprop::TPropOps};
use raphtory_api_macros::box_on_debug_lifetime;
use raphtory_core::{entities::LayerIds, storage::timeindex::EventTime};

use crate::utils::Iter4;

/// `WithTProps` defines behavior for types that store multiple temporal
/// properties either in memory or on disk.
///
/// Used by `GenericTProps` to implement `TPropOps` for such types.
pub trait WithTProps<'a>: Clone + Copy + Send + Sync
where
    Self: 'a,
{
    type TProp: TPropOps<'a>;

    fn num_layers(&self) -> usize;

    fn into_t_props(
        self,
        layer_id: LayerId,
        prop_id: usize,
    ) -> impl Iterator<Item = Self::TProp> + Send + Sync + 'a;

    #[box_on_debug_lifetime]
    fn into_t_props_layers(
        self,
        layers: impl Borrow<LayerIds>,
        prop_id: usize,
    ) -> impl Iterator<Item = Self::TProp> + Send + Sync + 'a {
        match layers.borrow() {
            LayerIds::None => Iter4::I(std::iter::empty()),
            LayerIds::One(layer_id) => Iter4::J(self.into_t_props(LayerId(*layer_id), prop_id)),
            LayerIds::All => Iter4::K(
                (0..self.num_layers())
                    .flat_map(move |layer_id| self.into_t_props(LayerId(layer_id), prop_id)),
            ),
            LayerIds::Multiple(layers) => Iter4::L(
                layers
                    .clone()
                    .into_iter()
                    .flat_map(move |layer_id| self.into_t_props(LayerId(layer_id), prop_id)),
            ),
        }
    }
}

/// A generic implementation of `TPropOps` that aggregates temporal properties
/// across storage.
///
/// Wraps types implementing `WithTProps` (eg, `MemNodeRef`, `DiskNodeRef`)
/// to provide unified access to temporal properties. Also handles k-merging
/// temporal properties when queried.
#[derive(Clone, Copy)]
pub struct GenericTProps<'a, Ref: WithTProps<'a>> {
    reference: Ref,
    layer_id: Either<&'a LayerIds, LayerId>,
    prop_id: usize,
}

impl<'a, Ref: WithTProps<'a>> GenericTProps<'a, Ref> {
    pub fn new(reference: Ref, layer_id: &'a LayerIds, prop_id: usize) -> Self {
        Self {
            reference,
            layer_id: Either::Left(layer_id),
            prop_id,
        }
    }

    pub fn new_with_layer(reference: Ref, layer_id: LayerId, prop_id: usize) -> Self {
        Self {
            reference,
            layer_id: Either::Right(layer_id),
            prop_id,
        }
    }
}

impl<'a, Ref: WithTProps<'a>> GenericTProps<'a, Ref> {
    #[box_on_debug_lifetime]
    fn tprops(self, prop_id: usize) -> impl Iterator<Item = Ref::TProp> + Send + Sync + 'a {
        match self.layer_id {
            Either::Left(layer_ids) => {
                Either::Left(self.reference.into_t_props_layers(layer_ids, prop_id))
            }
            Either::Right(layer_id) => {
                Either::Right(self.reference.into_t_props(layer_id, prop_id))
            }
        }
    }
}

impl<'a, Ref: WithTProps<'a>> TPropOps<'a> for GenericTProps<'a, Ref> {
    fn last_before(&self, t: EventTime) -> Option<(EventTime, Prop)> {
        self.tprops(self.prop_id)
            .filter_map(|t_props| t_props.last_before(t))
            .max_by_key(|(t, _)| *t)
    }

    fn iter_inner(
        self,
        w: Option<Range<EventTime>>,
    ) -> impl Iterator<Item = (EventTime, Prop)> + Send + Sync + 'a {
        let tprops = self.tprops(self.prop_id);
        tprops
            .map(|t_prop| t_prop.iter_inner(w.clone()))
            .kmerge_by(|(a, _), (b, _)| a < b)
    }

    fn iter_inner_rev(
        self,
        w: Option<Range<EventTime>>,
    ) -> impl Iterator<Item = (EventTime, Prop)> + Send + Sync + 'a {
        let tprops = self
            .tprops(self.prop_id)
            .map(move |t_cell| t_cell.iter_inner_rev(w.clone()));
        tprops.kmerge_by(|(a, _), (b, _)| a > b)
    }

    fn at(&self, ti: &EventTime) -> Option<Prop> {
        self.tprops(self.prop_id)
            .flat_map(|t_props| t_props.at(ti))
            .next() // TODO: need to figure out how to handle this
    }
}
