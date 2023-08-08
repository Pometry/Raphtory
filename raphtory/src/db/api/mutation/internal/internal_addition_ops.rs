use crate::{
    core::{
        entities::{EID, VID},
        storage::timeindex::TimeIndexEntry,
        utils::errors::GraphError,
        Prop,
    },
    db::api::view::internal::Base,
};

pub trait InternalAdditionOps {
    fn next_event_id(&self) -> usize;

    fn internal_add_vertex(
        &self,
        t: TimeIndexEntry,
        v: u64,
        name: Option<&str>,
        props: Vec<(String, Prop)>,
    ) -> Result<VID, GraphError>;

    fn internal_add_edge(
        &self,
        t: TimeIndexEntry,
        src: u64,
        dst: u64,
        props: Vec<(String, Prop)>,
        layer: Option<&str>,
    ) -> Result<EID, GraphError>;
}

pub trait InheritAdditionOps: Base {}

impl<G: InheritAdditionOps> DelegateAdditionOps for G
where
    G::Base: InternalAdditionOps,
{
    type Internal = G::Base;

    fn graph(&self) -> &Self::Internal {
        self.base()
    }
}

pub trait DelegateAdditionOps {
    type Internal: InternalAdditionOps + ?Sized;
    fn graph(&self) -> &Self::Internal;
}

impl<G: DelegateAdditionOps> InternalAdditionOps for G {
    #[inline(always)]
    fn next_event_id(&self) -> usize {
        self.graph().next_event_id()
    }

    #[inline(always)]
    fn internal_add_vertex(
        &self,
        t: TimeIndexEntry,
        v: u64,
        name: Option<&str>,
        props: Vec<(String, Prop)>,
    ) -> Result<VID, GraphError> {
        self.graph().internal_add_vertex(t, v, name, props)
    }

    #[inline(always)]
    fn internal_add_edge(
        &self,
        t: TimeIndexEntry,
        src: u64,
        dst: u64,
        props: Vec<(String, Prop)>,
        layer: Option<&str>,
    ) -> Result<EID, GraphError> {
        self.graph().internal_add_edge(t, src, dst, props, layer)
    }
}
