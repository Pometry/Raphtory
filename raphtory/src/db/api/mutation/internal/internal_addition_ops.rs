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
    /// get the sequence id for the next event
    fn next_event_id(&self) -> usize;

    /// map layer name to id and allocate a new layer if needed
    fn resolve_layer(&self, layer: Option<&str>) -> usize;

    /// map external vertex id to internal id, allocating a new empty vertex if needed
    fn resolve_vertex(&self, id: u64) -> VID;

    /// add vertex update
    fn internal_add_vertex(
        &self,
        t: TimeIndexEntry,
        v: VID,
        name: Option<&str>,
        props: Vec<(String, Prop)>,
    ) -> Result<VID, GraphError>;

    /// add edge update
    fn internal_add_edge(
        &self,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        props: Vec<(String, Prop)>,
        layer: usize,
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

    #[inline]
    fn resolve_layer(&self, layer: Option<&str>) -> usize {
        self.graph().resolve_layer(layer)
    }

    #[inline]
    fn resolve_vertex(&self, id: u64) -> VID {
        self.graph().resolve_vertex(id)
    }

    #[inline(always)]
    fn internal_add_vertex(
        &self,
        t: TimeIndexEntry,
        v: VID,
        name: Option<&str>,
        props: Vec<(String, Prop)>,
    ) -> Result<VID, GraphError> {
        self.graph().internal_add_vertex(t, v, name, props)
    }

    #[inline(always)]
    fn internal_add_edge(
        &self,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        props: Vec<(String, Prop)>,
        layer: usize,
    ) -> Result<EID, GraphError> {
        self.graph().internal_add_edge(t, src, dst, props, layer)
    }
}
