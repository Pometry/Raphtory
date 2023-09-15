use crate::{
    core::{
        entities::{EID, VID},
        storage::timeindex::TimeIndexEntry,
        utils::errors::GraphError,
        Prop, PropType,
    },
    db::api::view::internal::Base,
};
use enum_dispatch::enum_dispatch;

#[enum_dispatch]
pub trait InternalAdditionOps {
    /// get the sequence id for the next event
    fn next_event_id(&self) -> usize;

    /// map layer name to id and allocate a new layer if needed
    fn resolve_layer(&self, layer: Option<&str>) -> usize;

    /// map external vertex id to internal id, allocating a new empty vertex if needed
    fn resolve_vertex(&self, id: u64, name: Option<&str>) -> VID;

    /// map property key to internal id, allocating new property if needed
    fn resolve_graph_property(&self, prop: &str, is_static: bool) -> usize;

    /// map property key to internal id, allocating new property if needed and checking property type.
    /// returns `None` if the type does not match
    fn resolve_vertex_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<usize, GraphError>;

    fn resolve_edge_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<usize, GraphError>;

    /// add vertex update
    fn internal_add_vertex(
        &self,
        t: TimeIndexEntry,
        v: VID,
        props: Vec<(usize, Prop)>,
    ) -> Result<(), GraphError>;

    /// add edge update
    fn internal_add_edge(
        &self,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        props: Vec<(usize, Prop)>,
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
    fn resolve_vertex(&self, id: u64, name: Option<&str>) -> VID {
        self.graph().resolve_vertex(id, name)
    }

    #[inline]
    fn resolve_graph_property(&self, prop: &str, is_static: bool) -> usize {
        self.graph().resolve_graph_property(prop, is_static)
    }

    #[inline]
    fn resolve_vertex_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<usize, GraphError> {
        self.graph().resolve_vertex_property(prop, dtype, is_static)
    }

    #[inline]
    fn resolve_edge_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<usize, GraphError> {
        self.graph().resolve_edge_property(prop, dtype, is_static)
    }

    #[inline(always)]
    fn internal_add_vertex(
        &self,
        t: TimeIndexEntry,
        v: VID,
        props: Vec<(usize, Prop)>,
    ) -> Result<(), GraphError> {
        self.graph().internal_add_vertex(t, v, props)
    }

    #[inline(always)]
    fn internal_add_edge(
        &self,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        props: Vec<(usize, Prop)>,
        layer: usize,
    ) -> Result<EID, GraphError> {
        self.graph().internal_add_edge(t, src, dst, props, layer)
    }
}
