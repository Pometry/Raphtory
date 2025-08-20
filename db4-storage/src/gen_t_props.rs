use std::{borrow::Borrow, ops::Range};

use either::Either;
use itertools::Itertools;
use raphtory_api::core::entities::properties::{prop::Prop, tprop::TPropOps};
use raphtory_core::{entities::LayerIds, storage::timeindex::TimeIndexEntry};

use crate::utils::Iter4;

pub trait WithTProps<'a>: Clone + Copy + Send + Sync
where
    Self: 'a,
{
    type TProp: TPropOps<'a>;

    fn num_layers(&self) -> usize;
    fn into_t_props(
        self,
        layer_id: usize,
        prop_id: usize,
    ) -> impl Iterator<Item = Self::TProp> + 'a;

    fn into_t_props_layers(
        self,
        layers: impl Borrow<LayerIds>,
        prop_id: usize,
    ) -> impl Iterator<Item = Self::TProp> + 'a {
        match layers.borrow() {
            LayerIds::None => Iter4::I(std::iter::empty()),
            LayerIds::One(layer_id) => Iter4::J(self.into_t_props(*layer_id, prop_id)),
            LayerIds::All => Iter4::K(
                (0..self.num_layers())
                    .flat_map(move |layer_id| self.into_t_props(layer_id, prop_id)),
            ),
            LayerIds::Multiple(layers) => Iter4::L(
                layers
                    .clone()
                    .into_iter()
                    .flat_map(move |layer_id| self.into_t_props(layer_id, prop_id)),
            ),
        }
    }
}

#[derive(Clone, Copy)]
pub struct GenTProps<'a, Ref> {
    node: Ref,
    layer_id: Either<&'a LayerIds, usize>,
    prop_id: usize,
}

impl<'a, Ref> GenTProps<'a, Ref> {
    pub fn new(node: Ref, layer_id: &'a LayerIds, prop_id: usize) -> Self {
        Self {
            node,
            layer_id: Either::Left(layer_id),
            prop_id,
        }
    }

    pub fn new_with_layer(node: Ref, layer_id: usize, prop_id: usize) -> Self {
        Self {
            node,
            layer_id: Either::Right(layer_id),
            prop_id,
        }
    }
}

impl<'a, Ref: WithTProps<'a>> GenTProps<'a, Ref> {
    fn tprops(self, prop_id: usize) -> impl Iterator<Item = Ref::TProp> + 'a {
        match self.layer_id {
            Either::Left(layer_ids) => {
                Either::Left(self.node.into_t_props_layers(layer_ids, prop_id))
            }
            Either::Right(layer_id) => Either::Right(self.node.into_t_props(layer_id, prop_id)),
        }
    }
}

impl<'a, Ref: WithTProps<'a> + 'a> TPropOps<'a> for GenTProps<'a, Ref> {
    fn last_before(&self, t: TimeIndexEntry) -> Option<(TimeIndexEntry, Prop)> {
        self.tprops(self.prop_id)
            .filter_map(|t_props| t_props.last_before(t))
            .max_by_key(|(t, _)| *t)
    }

    fn iter_inner(
        self,
        w: Option<Range<TimeIndexEntry>>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a {
        let tprops = self.tprops(self.prop_id);
        tprops
            .map(|t_prop| t_prop.iter_inner(w.clone()))
            .kmerge_by(|(a, _), (b, _)| a < b)
    }

    fn iter_inner_rev(
        self,
        w: Option<Range<TimeIndexEntry>>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a {
        let tprops = self
            .tprops(self.prop_id)
            .map(move |t_cell| t_cell.iter_inner_rev(w.clone()));
        tprops.kmerge_by(|(a, _), (b, _)| a > b)
    }

    fn at(&self, ti: &TimeIndexEntry) -> Option<Prop> {
        self.tprops(self.prop_id)
            .flat_map(|t_props| t_props.at(ti))
            .next() //TODO: need to figure out how to handle this
    }
}
