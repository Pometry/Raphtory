use crate::core::{
    entities::{
        edges::edge_ref::EdgeRef,
        properties::{props::Props, tprop::TProp},
        LayerIds, EID, VID,
    },
    storage::{locked_view::LockedView, timeindex::TimeIndex},
    utils::errors::MutateGraphError,
    Prop,
};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::ops::{Deref, DerefMut, Range};

#[derive(Serialize, Deserialize, Debug, Default, PartialEq)]
pub(crate) struct EdgeStore<const N: usize> {
    pub(crate) eid: EID,
    src: VID,
    dst: VID,
    layers: Vec<EdgeLayer>, // each layer has its own set of properties
    additions: Vec<TimeIndex>,
    deletions: Vec<TimeIndex>,
}

#[derive(Serialize, Deserialize, Debug, Default, PartialEq)]
pub(crate) struct EdgeLayer {
    props: Option<Props>, // memory optimisation: only allocate props if needed
}

impl EdgeLayer {
    pub fn props(&self) -> Option<&Props> {
        self.props.as_ref()
    }

    pub fn add_prop(&mut self, t: i64, prop_id: usize, prop: Prop) {
        let props = self.props.get_or_insert_with(|| Props::new());
        props.add_prop(t, prop_id, prop);
    }

    pub fn add_static_prop(
        &mut self,
        prop_id: usize,
        prop_name: &str,
        prop: Prop,
    ) -> Result<(), MutateGraphError> {
        let props = self.props.get_or_insert_with(|| Props::new());
        props.add_static_prop(prop_id, prop_name, prop)
    }

    pub(crate) fn static_prop_ids(&self) -> Vec<usize> {
        self.props
            .as_ref()
            .map(|props| props.static_prop_ids())
            .unwrap_or_default()
    }

    pub(crate) fn static_property(&self, prop_id: usize) -> Option<&Prop> {
        self.props.as_ref().and_then(|ps| ps.static_prop(prop_id))
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

impl<const N: usize> From<&EdgeStore<N>> for EdgeRef {
    fn from(val: &EdgeStore<N>) -> Self {
        EdgeRef::new_outgoing(val.e_id(), val.src(), val.dst())
    }
}

impl<const N: usize> EdgeStore<N> {
    fn get_or_allocate_layer(&mut self, layer_id: usize) -> &mut EdgeLayer {
        if self.layers.len() <= layer_id {
            self.layers.resize_with(layer_id + 1, Default::default);
        }
        &mut self.layers[layer_id]
    }

    pub fn has_layer(&self, layers: &LayerIds) -> bool {
        match layers {
            LayerIds::All => true,
            LayerIds::One(layer_ids) => self
                .additions
                .get(*layer_ids)
                .filter(|t_index| !t_index.is_empty())
                .is_some(),
            LayerIds::Multiple(layer_ids) => todo!(),
        }
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

    pub fn layer(&self, layer_id: usize) -> Option<impl Deref<Target = EdgeLayer> + '_> {
        self.layers.get(layer_id)
    }

    pub fn unsafe_layer(&self, layer_id: usize) -> impl Deref<Target = EdgeLayer> + '_ {
        self.layers.get(layer_id).unwrap()
    }

    pub fn additions(&self) -> &Vec<TimeIndex> {
        &self.additions
    }

    pub fn deletions(&self) -> &Vec<TimeIndex> {
        &self.deletions
    }

    pub fn temporal_prop(&self, layer_id: usize, prop_id: usize) -> Option<&TProp> {
        self.layers
            .get(layer_id)
            .and_then(|layer| layer.temporal_property(prop_id))
    }

    pub fn layer_mut(&mut self, layer_id: usize) -> impl DerefMut<Target = EdgeLayer> + '_ {
        self.get_or_allocate_layer(layer_id)
    }

    pub fn deletions_mut(&mut self, layer_id: usize) -> &mut TimeIndex {
        if self.deletions.len() <= layer_id {
            self.deletions.resize_with(layer_id + 1, Default::default);
        }
        &mut self.deletions[layer_id]
    }

    pub fn additions_mut(&mut self, layer_id: usize) -> &mut TimeIndex {
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

    pub(crate) fn temp_prop_ids(&self, layer_id: Option<usize>) -> Vec<usize> {
        if let Some(layer_id) = layer_id {
            self.layers
                .get(layer_id)
                .map(|layer| {
                    layer
                        .props()
                        .map(|props| props.temporal_prop_ids())
                        .unwrap_or_default()
                })
                .unwrap_or_default()
        } else {
            self.layers
                .iter()
                .map(|layer| {
                    layer
                        .props()
                        .map(|prop| prop.temporal_prop_ids())
                        .unwrap_or_default()
                })
                .kmerge()
                .dedup()
                .collect()
        }
    }
}
