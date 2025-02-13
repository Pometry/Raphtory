use crate::core::{
    entities::{properties::props::Props, EID, VID},
    storage::{lazy_vec::IllegalSet, timeindex::TimeIndexEntry},
    utils::{errors::GraphError, iter::GenLockedIter},
    Prop,
};
use raphtory_api::core::entities::edges::edge_ref::EdgeRef;
pub use raphtory_api::core::entities::edges::*;
use serde::{Deserialize, Serialize};
use std::ops::Deref;

#[derive(Clone, Serialize, Deserialize, Debug, Default, PartialEq)]
pub struct EdgeStore {
    pub(crate) eid: EID,
    pub(crate) src: VID,
    pub(crate) dst: VID,
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
