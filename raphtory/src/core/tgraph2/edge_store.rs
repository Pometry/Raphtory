use itertools::Itertools;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};

use crate::core::{
    edge_ref::EdgeRef, tgraph::errors::MutateGraphError, timeindex::TimeIndex, Prop,
};

use super::{props::Props, EID, VID};

#[derive(Serialize, Deserialize, Debug, Default, PartialEq)]
pub(crate) struct EdgeStore<const N: usize> {
    pub(crate) eid: EID,
    src: VID,
    dst: VID,
    timestamps: TimeIndex,
    layer_props: FxHashMap<usize, Props>, // each layer has its own set of properties
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
        self.layer_props.contains_key(&layer_id)
    }

    pub fn new(src: VID, dst: VID, t: i64) -> Self {
        Self {
            eid: 0.into(),
            src,
            dst,
            timestamps: TimeIndex::one(t),
            layer_props: FxHashMap::default(),
        }
    }

    pub fn update_time(&mut self, t: i64) {
        self.timestamps.insert(t);
    }

    pub fn add_prop(&mut self, t: i64, prop_id: usize, prop: Prop, layer_id: usize) {
        let layer_props = self
            .layer_props
            .entry(layer_id)
            .or_insert_with(|| Props::new());
        layer_props.add_prop(t, prop_id, prop);
    }

    pub fn add_static_prop(
        &mut self,
        prop_id: usize,
        prop_name: &str,
        prop: Prop,
        layer_id: usize,
    ) -> Result<(), MutateGraphError> {
        self.layer_props
            .entry(layer_id)
            .or_insert_with(|| Props::new())
            .add_static_prop(prop_id, prop_name, prop)
    }

    pub fn timestamps(&self) -> &TimeIndex {
        &self.timestamps
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

    pub(crate) fn static_prop_ids(&self, layer: usize) -> Vec<usize> {
        self.layer_props
            .get(&layer)
            .map(|props| props.static_prop_ids())
            .unwrap_or_default()
    }

    pub(crate) fn static_property(&self, prop_id: usize, layer_id: usize) -> Option<&Prop> {
        let layer = self.layer_props.get(&layer_id)?;
        layer.static_prop(prop_id)
    }

    pub(crate) fn props(&self, layer_id: Option<usize>) -> Box<dyn Iterator<Item = &Props> + '_> {
        if let Some(layer_id) = layer_id {
            Box::new(self.layer_props.get(&layer_id).into_iter())
        } else {
            Box::new(self.layer_props.values().into_iter())
        }
    }

    pub(crate) fn temp_prop_ids(&self, layer_id: Option<usize>) -> Vec<usize> {
        if let Some(layer_id) = layer_id {
            self.layer_props
                .get(&layer_id)
                .map(|props| props.temporal_prop_ids())
                .unwrap_or_default()
        } else {
            self.layer_props
                .values()
                .map(|props| props.temporal_prop_ids())
                .kmerge()
                .dedup()
                .collect()
        }
    }
}
