use crate::{
    core::{
        entities::{
            properties::{props::Props, tprop::TProp},
            LayerIds, EID, VID,
        },
        storage::{
            lazy_vec::IllegalSet,
            raw_edges::EdgeArcGuard,
            timeindex::{TimeIndexEntry, TimeIndexIntoOps},
        },
        utils::{errors::GraphError, iter::GenLockedIter},
        Prop,
    },
    db::api::{
        storage::graph::edges::edge_storage_ops::{EdgeStorageIntoOps, EdgeStorageOps},
        view::IntoDynBoxed,
    },
};
use itertools::Itertools;
use raphtory_api::core::entities::edges::edge_ref::EdgeRef;
pub use raphtory_api::core::entities::edges::*;
use serde::{Deserialize, Serialize};
use std::ops::{Deref, Range};

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

    pub(crate) fn const_prop(&self, prop_id: usize) -> Option<&Prop> {
        self.props.as_ref().and_then(|ps| ps.const_prop(prop_id))
    }

    pub(crate) fn temporal_property(&self, prop_id: usize) -> Option<&TProp> {
        self.props.as_ref().and_then(|ps| ps.temporal_prop(prop_id))
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

    pub fn as_edge_ref(&self) -> EdgeRef {
        EdgeRef::new_outgoing(self.eid, self.src, self.dst)
    }
}

impl EdgeStorageIntoOps for EdgeArcGuard {
    fn into_layers(
        self,
        layer_ids: LayerIds,
        eref: EdgeRef,
    ) -> impl Iterator<Item = EdgeRef> + Send {
        let layer_ids = layer_ids.constrain_from_edge(eref);
        GenLockedIter::from((self, layer_ids), |(edge, layers)| {
            Box::new(
                edge.as_mem_edge()
                    .layer_ids_iter(layers)
                    .map(move |l| eref.at_layer(l)),
            )
        })
    }

    fn into_exploded(
        self,
        layer_ids: LayerIds,
        eref: EdgeRef,
    ) -> impl Iterator<Item = EdgeRef> + Send {
        let layer_ids = layer_ids.constrain_from_edge(eref);
        GenLockedIter::from((self, layer_ids, eref), |(edge, layers, eref)| {
            edge.as_mem_edge()
                .additions_iter(layers)
                .map(move |(l, a)| a.into_iter().map(move |t| eref.at(t).at_layer(l)))
                .kmerge_by(|e1, e2| e1.time() <= e2.time())
                .into_dyn_boxed()
        })
    }

    fn into_exploded_window(
        self,
        layer_ids: LayerIds,
        w: Range<TimeIndexEntry>,
        eref: EdgeRef,
    ) -> impl Iterator<Item = EdgeRef> + Send {
        let layer_ids = layer_ids.constrain_from_edge(eref);
        GenLockedIter::from((self, layer_ids, w), |(edge, layers, w)| {
            Box::new(
                edge.as_mem_edge()
                    .additions_iter(layers)
                    .flat_map(move |(l, a)| {
                        a.into_range(w.clone())
                            .into_iter()
                            .map(move |t| eref.at(t).at_layer(l))
                    }),
            )
        })
    }
}
