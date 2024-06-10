use crate::{
    core::{
        entities::{
            properties::{props::Props, tprop::TProp},
            LayerIds, EID, VID,
        },
        storage::{
            lazy_vec::IllegalSet,
            timeindex::{TimeIndex, TimeIndexEntry, TimeIndexIntoOps, TimeIndexOps},
            ArcEntry,
        },
        utils::errors::GraphError,
        Prop,
    },
    db::api::{
        storage::edges::edge_storage_ops::{EdgeStorageIntoOps, EdgeStorageOps},
        view::{BoxedLIter, IntoDynBoxed},
    },
};

use raphtory_api::core::entities::edges::edge_ref::EdgeRef;
pub use raphtory_api::core::entities::edges::*;

use itertools::{EitherOrBoth, Itertools};
use ouroboros::self_referencing;
use serde::{Deserialize, Serialize};
use std::{
    iter,
    ops::{DerefMut, Range},
};

#[derive(Serialize, Deserialize, Debug, Default, PartialEq)]
pub struct EdgeStore {
    pub(crate) eid: EID,
    pub(crate) src: VID,
    pub(crate) dst: VID,
    pub(crate) layers: Vec<EdgeLayer>, // each layer has its own set of properties
    pub(crate) additions: Vec<TimeIndex<TimeIndexEntry>>,
    pub(crate) deletions: Vec<TimeIndex<TimeIndexEntry>>,
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
        let props = self.props.get_or_insert_with(Props::new);
        props.add_prop(t, prop_id, prop)
    }

    pub fn add_constant_prop(
        &mut self,
        prop_id: usize,
        prop: Prop,
    ) -> Result<(), IllegalSet<Option<Prop>>> {
        let props = self.props.get_or_insert_with(Props::new);
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
}

impl EdgeStore {
    pub fn as_edge_ref(&self) -> EdgeRef {
        EdgeRef::new_outgoing(self.eid, self.src, self.dst)
    }

    pub fn internal_num_layers(&self) -> usize {
        self.layers
            .len()
            .max(self.additions.len())
            .max(self.deletions.len())
    }
    fn get_or_allocate_layer(&mut self, layer_id: usize) -> &mut EdgeLayer {
        if self.layers.len() <= layer_id {
            self.layers.resize_with(layer_id + 1, Default::default);
        }
        &mut self.layers[layer_id]
    }

    pub fn has_layer_inner(&self, layer_id: usize) -> bool {
        self.additions
            .get(layer_id)
            .filter(|t_index| !t_index.is_empty())
            .is_some()
            || self
                .deletions
                .get(layer_id)
                .filter(|t_index| !t_index.is_empty())
                .is_some()
    }

    pub fn layer_iter(&self) -> impl Iterator<Item = &EdgeLayer> + '_ {
        self.layers.iter()
    }

    /// Iterate over (layer_id, additions, deletions) triplets for edge
    pub fn updates_iter_inner<'a>(
        &'a self,
        layers: &'a LayerIds,
    ) -> impl Iterator<
        Item = (
            usize,
            &'a TimeIndex<TimeIndexEntry>,
            &'a TimeIndex<TimeIndexEntry>,
        ),
    > + 'a {
        match layers {
            LayerIds::None => Box::new(iter::empty()),
            LayerIds::All => self
                .additions_iter_inner(layers)
                .zip_longest(self.deletions_iter_inner(layers))
                .enumerate()
                .map(|(l, zipped)| match zipped {
                    EitherOrBoth::Both(additions, deletions) => (l, additions, deletions),
                    EitherOrBoth::Left(additions) => (l, additions, &TimeIndex::Empty),
                    EitherOrBoth::Right(deletions) => (l, &TimeIndex::Empty, deletions),
                })
                .into_dyn_boxed(),
            LayerIds::One(id) => Box::new(iter::once((
                *id,
                self.additions.get(*id).unwrap_or(&TimeIndex::Empty),
                self.deletions.get(*id).unwrap_or(&TimeIndex::Empty),
            ))),
            LayerIds::Multiple(ids) => Box::new(ids.iter().map(|id| {
                (
                    *id,
                    self.additions.get(*id).unwrap_or(&TimeIndex::Empty),
                    self.deletions.get(*id).unwrap_or(&TimeIndex::Empty),
                )
            })),
        }
    }

    pub fn additions_iter_inner<'a>(
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

    pub fn deletions_iter_inner<'a>(
        &'a self,
        layers: &'a LayerIds,
    ) -> BoxedLIter<'a, &TimeIndex<TimeIndexEntry>> {
        match layers {
            LayerIds::None => iter::empty().into_dyn_boxed(),
            LayerIds::All => self.deletions.iter().into_dyn_boxed(),
            LayerIds::One(id) => self.deletions.get(*id).into_iter().into_dyn_boxed(),
            LayerIds::Multiple(ids) => ids
                .iter()
                .flat_map(|id| self.deletions.get(*id))
                .into_dyn_boxed(),
        }
    }

    pub fn layer_ids_window_iter(&self, w: Range<i64>) -> impl Iterator<Item = usize> + '_ {
        let layer_ids = self
            .additions
            .iter()
            .enumerate()
            .zip_longest(self.deletions.iter().enumerate())
            .flat_map(move |e| match e {
                EitherOrBoth::Both((i, t1), (_, t2)) => {
                    if t1.contains(w.clone()) || t2.contains(w.clone()) {
                        Some(i)
                    } else {
                        None
                    }
                }
                EitherOrBoth::Left((i, t)) => {
                    if t.contains(w.clone()) {
                        Some(i)
                    } else {
                        None
                    }
                }
                EitherOrBoth::Right((i, t)) => {
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

    /// an edge is active in a window if it has an addition event in any of the layers
    pub fn active(&self, layer_ids: &LayerIds, w: Range<i64>) -> bool {
        match layer_ids {
            LayerIds::None => false,
            LayerIds::All => self
                .additions
                .iter()
                .any(|t_index| t_index.contains(w.clone())),
            LayerIds::One(l_id) => self
                .additions
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
            LayerIds::All => self.deletions.iter().flat_map(|d| d.last()).max(),
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
            LayerIds::All => self.additions.iter().flat_map(|d| d.last()).max(),
            LayerIds::One(id) => self.additions.get(*id).and_then(|t| t.last()),
            LayerIds::Multiple(ids) => ids
                .iter()
                .flat_map(|id| self.additions.get(*id).and_then(|t| t.last()))
                .max(),
        }
    }

    pub fn temporal_prop_layer_inner(&self, layer_id: usize, prop_id: usize) -> Option<&TProp> {
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

impl EdgeStorageIntoOps for ArcEntry<EdgeStore> {
    fn into_layers(
        self,
        layer_ids: LayerIds,
        eref: EdgeRef,
    ) -> impl Iterator<Item = EdgeRef> + Send {
        let layer_ids = layer_ids.constrain_from_edge(eref);
        ExplodedIterBuilder {
            entry: self,
            layer_ids,
            iter_builder: move |edge, layer_ids| {
                edge.layer_ids_iter(layer_ids)
                    .map(move |l| eref.at_layer(l))
                    .into_dyn_boxed()
            },
        }
        .build()
    }

    fn into_exploded(
        self,
        layer_ids: LayerIds,
        eref: EdgeRef,
    ) -> impl Iterator<Item = EdgeRef> + Send {
        let layer_ids = layer_ids.constrain_from_edge(eref);
        ExplodedIterBuilder {
            entry: self,
            layer_ids,
            iter_builder: move |edge, layers| {
                edge.additions_iter(layers)
                    .map(move |(l, a)| a.into_iter().map(move |t| eref.at(t).at_layer(l)))
                    .kmerge_by(|e1, e2| e1.time() <= e2.time())
                    .into_dyn_boxed()
            },
        }
        .build()
    }

    fn into_exploded_window(
        self,
        layer_ids: LayerIds,
        w: Range<TimeIndexEntry>,
        eref: EdgeRef,
    ) -> impl Iterator<Item = EdgeRef> + Send {
        let layer_ids = layer_ids.constrain_from_edge(eref);
        ExplodedIterBuilder {
            entry: self,
            layer_ids,
            iter_builder: move |edge, layers| {
                edge.additions_iter(layers)
                    .flat_map(move |(l, a)| {
                        a.into_range(w.clone())
                            .into_iter()
                            .map(move |t| eref.at(t).at_layer(l))
                    })
                    .into_dyn_boxed()
            },
        }
        .build()
    }
}

#[self_referencing]
pub struct ExplodedIter {
    entry: ArcEntry<EdgeStore>,
    layer_ids: LayerIds,
    #[borrows(entry, layer_ids)]
    #[covariant]
    iter: Box<dyn Iterator<Item = EdgeRef> + Send + 'this>,
}

impl Iterator for ExplodedIter {
    type Item = EdgeRef;

    fn next(&mut self) -> Option<Self::Item> {
        self.with_iter_mut(|iter| iter.next())
    }
}
