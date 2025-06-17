use crate::{
    entities::{
        properties::props::{ConstPropError, Props, TPropError},
        EID, VID,
    },
    storage::{
        raw_edges::EdgeShard,
        timeindex::{TimeIndex, TimeIndexEntry},
    },
    utils::iter::GenLockedIter,
};
use raphtory_api::core::entities::{edges::edge_ref::EdgeRef, properties::prop::Prop};
use serde::{Deserialize, Serialize};
use std::{
    fmt::{Debug, Formatter},
    ops::Deref,
};

#[derive(Clone, Serialize, Deserialize, Debug, Default, PartialEq)]
pub struct EdgeStore {
    pub eid: EID,
    pub src: VID,
    pub dst: VID,
}

pub trait EdgeDataLike<'a> {
    fn temporal_prop_ids(self) -> impl Iterator<Item = usize> + 'a;
    fn const_prop_ids(self) -> impl Iterator<Item = usize> + 'a;
}

impl<'a, T: Deref<Target = EdgeLayer> + 'a> EdgeDataLike<'a> for T {
    fn temporal_prop_ids(self) -> impl Iterator<Item = usize> + 'a {
        GenLockedIter::from(self, |layer| {
            Box::new(
                layer
                    .props()
                    .into_iter()
                    .flat_map(|props| props.temporal_prop_ids()),
            )
        })
    }

    fn const_prop_ids(self) -> impl Iterator<Item = usize> + 'a {
        GenLockedIter::from(self, |layer| {
            Box::new(
                layer
                    .props()
                    .into_iter()
                    .flat_map(|props| props.const_prop_ids()),
            )
        })
    }
}

#[derive(Serialize, Deserialize, Debug, Default, PartialEq)]
pub struct EdgeLayer {
    props: Option<Props>, // memory optimisation: only allocate props if needed
}

impl EdgeLayer {
    pub fn props(&self) -> Option<&Props> {
        self.props.as_ref()
    }

    pub fn into_props(self) -> Option<Props> {
        self.props
    }

    pub fn add_prop(
        &mut self,
        t: TimeIndexEntry,
        prop_id: usize,
        prop: Prop,
    ) -> Result<(), TPropError> {
        let props = self.props.get_or_insert_with(Props::new);
        props.add_prop(t, prop_id, prop)
    }

    pub fn add_constant_prop(&mut self, prop_id: usize, prop: Prop) -> Result<(), ConstPropError> {
        let props = self.props.get_or_insert_with(Props::new);
        props.add_constant_prop(prop_id, prop)
    }

    pub fn update_constant_prop(
        &mut self,
        prop_id: usize,
        prop: Prop,
    ) -> Result<(), ConstPropError> {
        let props = self.props.get_or_insert_with(Props::new);
        props.update_constant_prop(prop_id, prop)
    }
}

impl EdgeStore {
    pub fn new(src: VID, dst: VID) -> Self {
        Self {
            eid: 0.into(),
            src,
            dst,
        }
    }

    pub fn initialised(&self) -> bool {
        self.eid != EID::default()
    }

    pub fn as_edge_ref(&self) -> EdgeRef {
        EdgeRef::new_outgoing(self.eid, self.src, self.dst)
    }
}

#[derive(Clone, Copy)]
pub struct MemEdge<'a> {
    edges: &'a EdgeShard,
    offset: usize,
}

impl<'a> Debug for MemEdge<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Edge")
            .field("src", &self.src())
            .field("dst", &self.dst())
            .field("eid", &self.eid())
            .finish()
    }
}

impl<'a> MemEdge<'a> {
    pub fn new(edges: &'a EdgeShard, offset: usize) -> Self {
        MemEdge { edges, offset }
    }

    pub fn src(&self) -> VID {
        self.edge_store().src
    }

    pub fn dst(&self) -> VID {
        self.edge_store().dst
    }
    pub fn edge_store(&self) -> &'a EdgeStore {
        self.edges.edge_store(self.offset)
    }

    #[inline]
    pub fn props(self, layer_id: usize) -> Option<&'a Props> {
        self.edges
            .props(self.offset, layer_id)
            .and_then(|el| el.props())
    }

    pub fn eid(self) -> EID {
        self.edge_store().eid
    }

    pub fn as_edge_ref(&self) -> EdgeRef {
        EdgeRef::new_outgoing(self.eid(), self.src(), self.dst())
    }

    pub fn internal_num_layers(self) -> usize {
        self.edges.internal_num_layers()
    }

    pub fn get_additions(self, layer_id: usize) -> Option<&'a TimeIndex<TimeIndexEntry>> {
        self.edges.additions(self.offset, layer_id)
    }

    pub fn get_deletions(self, layer_id: usize) -> Option<&'a TimeIndex<TimeIndexEntry>> {
        self.edges.deletions(self.offset, layer_id)
    }

    pub fn has_layer_inner(self, layer_id: usize) -> bool {
        self.get_additions(layer_id)
            .filter(|t_index| !t_index.is_empty())
            .is_some()
            || self
                .get_deletions(layer_id)
                .filter(|t_index| !t_index.is_empty())
                .is_some()
    }
}
