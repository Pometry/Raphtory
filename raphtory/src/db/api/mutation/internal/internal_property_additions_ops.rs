use crate::{
    core::{
        entities::{vertices::vertex_ref::VertexRef, EID},
        storage::{lazy_vec::IllegalSet, timeindex::TimeIndexEntry},
        utils::errors::{GraphError, IllegalMutate},
        Prop,
    },
    db::api::view::internal::Base,
};

/// internal (dyn friendly) methods for adding properties
pub trait InternalPropertyAdditionOps {
    /// internal (dyn friendly)
    fn internal_add_vertex_properties(
        &self,
        v: u64,
        data: Vec<(String, Prop)>,
    ) -> Result<(), GraphError>;

    fn internal_add_properties(
        &self,
        t: TimeIndexEntry,
        props: Vec<(String, Prop)>,
    ) -> Result<(), GraphError>;

    fn internal_add_static_properties(&self, props: Vec<(String, Prop)>) -> Result<(), GraphError>;

    fn internal_add_edge_properties(
        &self,
        eid: EID,
        props: Vec<(String, Prop)>,
        layer: usize,
    ) -> Result<(), IllegalMutate>;
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
    fn internal_add_vertex_properties(
        &self,
        v: u64,
        data: Vec<(String, Prop)>,
    ) -> Result<(), GraphError> {
        self.graph().internal_add_vertex_properties(v, data)
    }

    #[inline(always)]
    fn internal_add_properties(
        &self,
        t: TimeIndexEntry,
        props: Vec<(String, Prop)>,
    ) -> Result<(), GraphError> {
        self.graph().internal_add_properties(t, props)
    }

    #[inline(always)]
    fn internal_add_static_properties(&self, props: Vec<(String, Prop)>) -> Result<(), GraphError> {
        self.graph().internal_add_static_properties(props)
    }

    #[inline(always)]
    fn internal_add_edge_properties(
        &self,
        eid: EID,
        props: Vec<(String, Prop)>,
        layer: usize,
    ) -> Result<(), IllegalMutate> {
        self.graph().internal_add_edge_properties(eid, props, layer)
    }
}
