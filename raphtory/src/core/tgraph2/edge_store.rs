use std::ops::{Deref, DerefMut};

use itertools::Itertools;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};

use crate::core::{
    edge_ref::EdgeRef, tgraph::errors::MutateGraphError, timeindex::TimeIndex, tprop::TProp, Prop,
};

use super::{props::Props, EID, VID};

#[derive(Serialize, Deserialize, Debug, Default, PartialEq)]
pub(crate) struct EdgeStore<const N: usize> {
    pub(crate) eid: EID,
    src: VID,
    dst: VID,
    layers: FxHashMap<usize, EdgeLayer>, // each layer has its own set of properties
}

#[derive(Serialize, Deserialize, Debug, Default, PartialEq)]
pub(crate) struct EdgeLayer {
    timestamps: TimeIndex,
    deletions: TimeIndex,
    props: Props,
}

impl EdgeLayer {
    pub fn props(&self) -> &Props {
        &self.props
    }

    pub fn update_time(&mut self, t: i64) {
        self.timestamps.insert(t);
    }

    pub fn delete(&mut self, t: i64) {
        self.deletions.insert(t);
    }

    pub fn add_prop(&mut self, t: i64, prop_id: usize, prop: Prop) {
        self.props.add_prop(t, prop_id, prop);
    }

    pub fn add_static_prop(
        &mut self,
        prop_id: usize,
        prop_name: &str,
        prop: Prop,
    ) -> Result<(), MutateGraphError> {
        self.props.add_static_prop(prop_id, prop_name, prop)
    }

    pub(crate) fn static_prop_ids(&self) -> Vec<usize> {
        self.props.static_prop_ids()
    }

    pub fn timestamps(&self) -> &TimeIndex {
        &self.timestamps
    }

    pub fn deletions(&self) -> &TimeIndex {
        &self.deletions
    }

    pub(crate) fn static_property(&self, prop_id: usize) -> Option<&Prop> {
        self.props.static_prop(prop_id)
    }

    pub(crate) fn temporal_property(&self, prop_id: usize) -> Option<&TProp> {
        self.props.temporal_prop(prop_id)
    }
}

impl<const N: usize> Into<EdgeRef> for &EdgeStore<N> {
    fn into(self) -> EdgeRef {
        EdgeRef::LocalOut {
            e_pid: self.e_id(),
            layer_id: 0,
            src_pid: self.src(),
            dst_pid: self.dst(),
            time: None,
        }
    }
}

impl<const N: usize> EdgeStore<N> {
    pub fn has_layer(&self, layer_id: usize) -> bool {
        self.layers.contains_key(&layer_id)
    }

    pub fn new(src: VID, dst: VID) -> Self {
        Self {
            eid: 0.into(),
            src,
            dst,
            layers: FxHashMap::default(),
        }
    }

    pub fn layer(&self, layer_id: usize) -> Option<impl Deref<Target = EdgeLayer> + '_> {
        self.layers.get(&layer_id)
    }

    pub fn unsafe_layer(&self, layer_id: usize) -> impl Deref<Target = EdgeLayer> + '_ {
        self.layers.get(&layer_id).unwrap()
    }

    pub fn layer_timestamps(&self, layer_id: usize) -> &TimeIndex {
        &self.layers.get(&layer_id).unwrap().timestamps
    }
    pub fn layer_deletions(&self, layer_id: usize) -> &TimeIndex {
        &self.layers.get(&layer_id).unwrap().deletions
    }
    pub fn temporal_prop(&self, layer_id: usize, prop_id: usize) -> Option<&TProp> {
        self.layers
            .get(&layer_id).and_then(|layer| layer.temporal_property(prop_id))
    }

    pub fn layer_mut(&mut self, layer_id: usize) -> impl DerefMut<Target = EdgeLayer> + '_ {
        self.layers.entry(layer_id).or_default();
        self.layers.get_mut(&layer_id).unwrap()
    }

    pub fn get_or_create_layer(&mut self, layer_id: usize) -> &mut EdgeLayer {
        self.layers.entry(layer_id).or_default()
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
                .get(&layer_id)
                .into_iter()
                .map(|layer| layer.props());
            Box::new(iter)
        } else {
            Box::new(self.layers.values().into_iter().map(|layer| layer.props()))
        }
    }

    pub(crate) fn temp_prop_ids(&self, layer_id: Option<usize>) -> Vec<usize> {
        if let Some(layer_id) = layer_id {
            self.layers
                .get(&layer_id)
                .map(|layer| layer.props().temporal_prop_ids())
                .unwrap_or_default()
        } else {
            self.layers
                .values()
                .map(|layer| layer.props().temporal_prop_ids())
                .kmerge()
                .dedup()
                .collect()
        }
    }
}
