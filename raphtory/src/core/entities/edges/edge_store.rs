use crate::{
    core::{
        entities::{
            edges::edge_ref::EdgeRef,
            properties::{props::Props, tprop::TProp},
            LayerIds, EID, VID,
        },
        storage::{
            lazy_vec::IllegalSet,
            locked_view::LockedView,
            timeindex::{TimeIndex, TimeIndexEntry, TimeIndexOps},
        },
        utils::errors::{GraphError, MutateGraphError},
        Prop,
    },
    db::api::view::{BoxedLIter, IntoDynBoxed},
    prelude::TimeOps,
};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::{
    iter,
    ops::{Deref, DerefMut, Range},
};

#[derive(Serialize, Deserialize, Debug, Default, PartialEq)]
pub struct EdgeStore {
    pub(crate) eid: EID,
    src: VID,
    dst: VID,
    layers: Vec<EdgeLayer>, // each layer has its own set of properties
    additions: Vec<TimeIndex<TimeIndexEntry>>,
    deletions: Vec<TimeIndex<TimeIndexEntry>>,
}

#[derive(Serialize, Deserialize, Debug, Default, PartialEq)]
pub struct EdgeLayer {
    props: Option<Props>, // memory optimisation: only allocate props if needed
}

impl EdgeLayer {
    pub fn props(&self) -> Option<&Props> {
        self.props.as_ref()
    }

    pub fn add_prop(
        &mut self,
        t: TimeIndexEntry,
        prop_id: usize,
        prop: Prop,
    ) -> Result<(), GraphError> {
        let props = self.props.get_or_insert_with(|| Props::new());
        props.add_prop(t, prop_id, prop)
    }

    pub fn add_constant_prop(
        &mut self,
        prop_id: usize,
        prop: Prop,
    ) -> Result<(), IllegalSet<Option<Prop>>> {
        let props = self.props.get_or_insert_with(|| Props::new());
        props.add_constant_prop(prop_id, prop)
    }

    pub fn update_constant_prop(&mut self, prop_id: usize, prop: Prop) -> Result<(), GraphError> {
        let props = self.props.get_or_insert_with(Props::new);
        props.update_constant_prop(prop_id, prop)
    }

    pub(crate) fn const_prop_ids(&self) -> impl Iterator<Item = usize> + '_ {
        self.props
            .as_ref()
            .into_iter()
            .flat_map(|props| props.const_prop_ids())
    }

    pub(crate) fn const_prop(&self, prop_id: usize) -> Option<&Prop> {
        self.props.as_ref().and_then(|ps| ps.const_prop(prop_id))
    }

    pub(crate) fn temporal_property(&self, prop_id: usize) -> Option<&TProp> {
        self.props.as_ref().and_then(|ps| ps.temporal_prop(prop_id))
    }

    pub(crate) fn temporal_properties<'a>(
        &'a self,
        prop_id: usize,
        window: Option<Range<i64>>,
    ) -> Box<dyn Iterator<Item = (i64, Prop)> + 'a> {
        if let Some(window) = window {
            self.props
                .as_ref()
                .map(|props| props.temporal_props_window(prop_id, window.start, window.end))
                .unwrap_or_else(|| Box::new(std::iter::empty()))
        } else {
            self.props
                .as_ref()
                .map(|props| props.temporal_props(prop_id))
                .unwrap_or_else(|| Box::new(std::iter::empty()))
        }
    }
}

impl<E: Deref<Target = EdgeStore>> From<E> for EdgeRef {
    fn from(val: E) -> Self {
        EdgeRef::new_outgoing(val.e_id(), val.src(), val.dst())
    }
}

impl EdgeStore {
    fn get_or_allocate_layer(&mut self, layer_id: usize) -> &mut EdgeLayer {
        if self.layers.len() <= layer_id {
            self.layers.resize_with(layer_id + 1, Default::default);
        }
        &mut self.layers[layer_id]
    }

    pub fn has_layer(&self, layers: &LayerIds) -> bool {
        match layers {
            LayerIds::All => true,
            LayerIds::One(layer_ids) => {
                self.additions
                    .get(*layer_ids)
                    .filter(|t_index| !t_index.is_empty())
                    .is_some()
                    || self
                        .deletions
                        .get(*layer_ids)
                        .filter(|t_index| !t_index.is_empty())
                        .is_some()
            }
            LayerIds::Multiple(layer_ids) => layer_ids
                .iter()
                .any(|layer_id| self.has_layer(&LayerIds::One(*layer_id))),
            LayerIds::None => false,
        }
    }

    // an edge is in a layer if it has either deletions or additions in that layer
    pub fn layer_ids(&self) -> LayerIds {
        let layer_ids = self.layer_ids_iter().collect::<Vec<_>>();
        if layer_ids.len() == 1 {
            LayerIds::One(layer_ids[0])
        } else {
            LayerIds::Multiple(layer_ids.into())
        }
    }

    pub fn layer_iter(&self) -> impl Iterator<Item = &EdgeLayer> + '_ {
        self.layers.iter()
    }

    pub fn additions_iter<'a>(
        &'a self,
        layers: &'a LayerIds,
    ) -> BoxedLIter<'a, &TimeIndex<TimeIndexEntry>> {
        match layers {
            LayerIds::None => iter::empty().into_dyn_boxed(),
            LayerIds::All => self.additions.iter().into_dyn_boxed(),
            LayerIds::One(id) => self.additions.get(*id).into_iter().into_dyn_boxed(),
            LayerIds::Multiple(ids) => ids
                .iter()
                .flat_map(|id| self.additions.get(*id))
                .into_dyn_boxed(),
        }
    }
    pub fn layer_ids_iter(&self) -> impl Iterator<Item = usize> + '_ {
        let layer_ids = self
            .additions
            .iter()
            .enumerate()
            .zip_longest(self.deletions.iter().enumerate())
            .flat_map(|e| match e {
                itertools::EitherOrBoth::Both((i, t1), (_, t2)) => {
                    if !t1.is_empty() || !t2.is_empty() {
                        Some(i)
                    } else {
                        None
                    }
                }
                itertools::EitherOrBoth::Left((i, t)) => {
                    if !t.is_empty() {
                        Some(i)
                    } else {
                        None
                    }
                }
                itertools::EitherOrBoth::Right((i, t)) => {
                    if !t.is_empty() {
                        Some(i)
                    } else {
                        None
                    }
                }
            });
        layer_ids
    }

    pub fn layer_ids_window_iter(&self, w: Range<i64>) -> impl Iterator<Item = usize> + '_ {
        let layer_ids = self
            .additions
            .iter()
            .enumerate()
            .zip_longest(self.deletions.iter().enumerate())
            .flat_map(move |e| match e {
                itertools::EitherOrBoth::Both((i, t1), (_, t2)) => {
                    if t1.contains(w.clone()) || t2.contains(w.clone()) {
                        Some(i)
                    } else {
                        None
                    }
                }
                itertools::EitherOrBoth::Left((i, t)) => {
                    if t.contains(w.clone()) {
                        Some(i)
                    } else {
                        None
                    }
                }
                itertools::EitherOrBoth::Right((i, t)) => {
                    if t.contains(w.clone()) {
                        Some(i)
                    } else {
                        None
                    }
                }
            });

        layer_ids
    }

    pub fn new(src: VID, dst: VID) -> Self {
        Self {
            eid: 0.into(),
            src,
            dst,
            layers: Vec::with_capacity(1),
            additions: Vec::with_capacity(1),
            deletions: Vec::with_capacity(1),
        }
    }

    pub fn layer(&self, layer_id: usize) -> Option<&EdgeLayer> {
        self.layers.get(layer_id)
    }

    pub fn additions(&self) -> &Vec<TimeIndex<TimeIndexEntry>> {
        &self.additions
    }

    pub fn deletions(&self) -> &Vec<TimeIndex<TimeIndexEntry>> {
        &self.deletions
    }

    /// an edge is active in a window if it has an addition event in any of the layers
    pub fn active(&self, layer_ids: &LayerIds, w: Range<i64>) -> bool {
        match layer_ids {
            LayerIds::None => false,
            LayerIds::All => self
                .additions()
                .iter()
                .any(|t_index| t_index.contains(w.clone())),
            LayerIds::One(l_id) => self
                .additions()
                .get(*l_id)
                .map(|t_index| t_index.contains(w))
                .unwrap_or(false),
            LayerIds::Multiple(layers) => layers
                .iter()
                .any(|l_id| self.active(&LayerIds::One(*l_id), w.clone())),
        }
    }

    pub fn last_deletion(&self, layer_ids: &LayerIds) -> Option<TimeIndexEntry> {
        match layer_ids {
            LayerIds::None => None,
            LayerIds::All => self.deletions().iter().flat_map(|d| d.last()).max(),
            LayerIds::One(id) => self.deletions.get(*id).and_then(|t| t.last()),
            LayerIds::Multiple(ids) => ids
                .iter()
                .flat_map(|id| self.deletions.get(*id).and_then(|t| t.last()))
                .max(),
        }
    }

    pub fn last_addition(&self, layer_ids: &LayerIds) -> Option<TimeIndexEntry> {
        match layer_ids {
            LayerIds::None => None,
            LayerIds::All => self.additions().iter().flat_map(|d| d.last()).max(),
            LayerIds::One(id) => self.additions.get(*id).and_then(|t| t.last()),
            LayerIds::Multiple(ids) => ids
                .iter()
                .flat_map(|id| self.additions.get(*id).and_then(|t| t.last()))
                .max(),
        }
    }

    pub fn last_deletion_before(&self, layer_ids: &LayerIds, t: i64) -> Option<TimeIndexEntry> {
        match layer_ids {
            LayerIds::None => None,
            LayerIds::All => self
                .deletions()
                .iter()
                .flat_map(|dels| dels.range(i64::MIN..t).last())
                .max(),
            LayerIds::One(id) => {
                let layer = self.deletions.get(*id)?;
                layer.range(i64::MIN..t).last()
            }
            LayerIds::Multiple(ids) => ids
                .iter()
                .flat_map(|id| {
                    self.deletions
                        .get(*id)
                        .and_then(|t_index| t_index.range(i64::MIN..t).last())
                })
                .max(),
        }
    }

    pub fn has_temporal_prop(&self, layer_ids: &LayerIds, prop_id: usize) -> bool {
        match layer_ids {
            LayerIds::None => false,
            LayerIds::All => self.layer_ids_iter().any(|id| {
                self.layer(id)
                    .and_then(|layer| layer.temporal_property(prop_id))
                    .is_some()
            }),
            LayerIds::One(id) => self
                .layer(*id)
                .and_then(|layer| layer.temporal_property(prop_id))
                .is_some(),
            LayerIds::Multiple(ids) => ids.iter().any(|id| {
                self.layer(*id)
                    .and_then(|layer| layer.temporal_property(prop_id))
                    .is_some()
            }),
        }
    }

    pub fn has_temporal_prop_window(
        &self,
        layer_ids: LayerIds,
        prop_id: usize,
        w: Range<i64>,
    ) -> bool {
        match layer_ids {
            LayerIds::None => false,
            LayerIds::All => self.layer_ids_iter().any(|id| {
                self.layer(id)
                    .and_then(|layer| {
                        layer
                            .temporal_property(prop_id)
                            .filter(|p| p.iter_window_t(w.clone()).next().is_some())
                    })
                    .is_some()
            }),
            LayerIds::One(id) => self
                .layer(id)
                .and_then(|layer| {
                    layer
                        .temporal_property(prop_id)
                        .filter(|p| p.iter_window_t(w.clone()).next().is_some())
                })
                .is_some(),
            LayerIds::Multiple(ids) => ids.iter().any(|id| {
                self.layer(*id)
                    .and_then(|layer| {
                        layer
                            .temporal_property(prop_id)
                            .filter(|p| p.iter_window_t(w.clone()).next().is_some())
                    })
                    .is_some()
            }),
        }
    }

    pub fn temporal_prop_layer(&self, layer_id: usize, prop_id: usize) -> Option<&TProp> {
        self.layers
            .get(layer_id)
            .and_then(|layer| layer.temporal_property(prop_id))
    }

    pub fn layer_mut(&mut self, layer_id: usize) -> impl DerefMut<Target = EdgeLayer> + '_ {
        self.get_or_allocate_layer(layer_id)
    }

    pub fn deletions_mut(&mut self, layer_id: usize) -> &mut TimeIndex<TimeIndexEntry> {
        if self.deletions.len() <= layer_id {
            self.deletions.resize_with(layer_id + 1, Default::default);
        }
        &mut self.deletions[layer_id]
    }

    pub fn additions_mut(&mut self, layer_id: usize) -> &mut TimeIndex<TimeIndexEntry> {
        if self.additions.len() <= layer_id {
            self.additions.resize_with(layer_id + 1, Default::default);
        }
        &mut self.additions[layer_id]
    }

    pub fn src(&self) -> VID {
        self.src
    }

    pub fn dst(&self) -> VID {
        self.dst
    }

    pub fn e_id(&self) -> EID {
        self.eid
    }

    pub(crate) fn props(&self, layer_id: Option<usize>) -> Box<dyn Iterator<Item = &Props> + '_> {
        if let Some(layer_id) = layer_id {
            let iter = self
                .layers
                .get(layer_id)
                .into_iter()
                .flat_map(|layer| layer.props());
            Box::new(iter)
        } else {
            Box::new(self.layers.iter().flat_map(|layer| layer.props()))
        }
    }

    pub(crate) fn temp_prop_ids(
        &self,
        layer_id: Option<usize>,
    ) -> Box<dyn Iterator<Item = usize> + '_> {
        if let Some(layer_id) = layer_id {
            Box::new(self.layers.get(layer_id).into_iter().flat_map(|layer| {
                layer
                    .props()
                    .into_iter()
                    .flat_map(|props| props.temporal_prop_ids())
            }))
        } else {
            Box::new(
                self.layers
                    .iter()
                    .flat_map(|layer| layer.props().map(|prop| prop.temporal_prop_ids()))
                    .kmerge()
                    .dedup(),
            )
        }
    }
}
