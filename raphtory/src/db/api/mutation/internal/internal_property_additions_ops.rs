use crate::{
    core::{
        entities::{EID, VID},
        storage::timeindex::TimeIndexEntry,
        utils::errors::{GraphError, IllegalMutate},
        Prop,
    },
    db::api::view::internal::Base,
};
use enum_dispatch::enum_dispatch;

/// internal (dyn friendly) methods for adding properties
#[enum_dispatch]
pub trait InternalPropertyAdditionOps {
    fn internal_add_properties(
        &self,
        t: TimeIndexEntry,
        props: Vec<(usize, Prop)>,
    ) -> Result<(), GraphError>;

    fn internal_add_static_properties(&self, props: Vec<(usize, Prop)>) -> Result<(), GraphError>;

    fn internal_add_constant_vertex_properties(
        &self,
        vid: VID,
        props: Vec<(usize, Prop)>,
    ) -> Result<(), GraphError>;

    fn internal_add_constant_edge_properties(
        &self,
        eid: EID,
        layer: usize,
        props: Vec<(usize, Prop)>,
    ) -> Result<(), GraphError>;
}

pub trait InheritPropertyAdditionOps: Base {}

impl<G: InheritPropertyAdditionOps + ?Sized> DelegatePropertyAdditionOps for G
where
    <G as Base>::Base: InternalPropertyAdditionOps,
{
    type Internal = <G as Base>::Base;

    fn graph(&self) -> &Self::Internal {
        self.base()
    }
}

pub trait DelegatePropertyAdditionOps {
    type Internal: InternalPropertyAdditionOps + ?Sized;

    fn graph(&self) -> &Self::Internal;
}

impl<G: DelegatePropertyAdditionOps> InternalPropertyAdditionOps for G {
    #[inline(always)]
    fn internal_add_properties(
        &self,
        t: TimeIndexEntry,
        props: Vec<(usize, Prop)>,
    ) -> Result<(), GraphError> {
        self.graph().internal_add_properties(t, props)
    }

    #[inline(always)]
    fn internal_add_static_properties(&self, props: Vec<(usize, Prop)>) -> Result<(), GraphError> {
        self.graph().internal_add_static_properties(props)
    }

    #[inline]
    fn internal_add_constant_vertex_properties(
        &self,
        vid: VID,
        props: Vec<(usize, Prop)>,
    ) -> Result<(), GraphError> {
        self.graph()
            .internal_add_constant_vertex_properties(vid, props)
    }

    #[inline]
    fn internal_add_constant_edge_properties(
        &self,
        eid: EID,
        layer: usize,
        props: Vec<(usize, Prop)>,
    ) -> Result<(), GraphError> {
        self.graph()
            .internal_add_constant_edge_properties(eid, layer, props)
    }
}
