use crate::{
    core::{
        entities::{nodes::node_ref::AsNodeRef, EID, VID},
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
    fn next_event_id(&self) -> Result<usize, GraphError>;

    /// map layer name to id and allocate a new layer if needed
    fn resolve_layer(&self, layer: Option<&str>) -> Result<usize, GraphError>;

    fn set_node_type(&self, v_id: VID, node_type: &str) -> Result<(), GraphError>;

    /// map external node id to internal id, allocating a new empty node if needed
    fn resolve_node<V: AsNodeRef>(&self, id: V) -> Result<VID, GraphError>;

    /// map property key to internal id, allocating new property if needed
    fn resolve_graph_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<usize, GraphError>;

    /// map property key to internal id, allocating new property if needed and checking property type.
    /// returns `None` if the type does not match
    fn resolve_node_property(
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

    /// add node update
    fn internal_add_node(
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

    /// add update for an existing edge
    fn internal_add_edge_update(
        &self,
        t: TimeIndexEntry,
        edge: EID,
        props: Vec<(usize, Prop)>,
        layer: usize,
    ) -> Result<(), GraphError>;
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
    fn next_event_id(&self) -> Result<usize, GraphError> {
        self.graph().next_event_id()
    }

    #[inline]
    fn resolve_layer(&self, layer: Option<&str>) -> Result<usize, GraphError> {
        self.graph().resolve_layer(layer)
    }

    #[inline]
    fn set_node_type(&self, v_id: VID, node_type: &str) -> Result<(), GraphError> {
        self.graph().set_node_type(v_id, node_type)
    }

    #[inline]
    fn resolve_node<V: AsNodeRef>(&self, n: V) -> Result<VID, GraphError> {
        self.graph().resolve_node(n)
    }

    #[inline]
    fn resolve_graph_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<usize, GraphError> {
        self.graph().resolve_graph_property(prop, dtype, is_static)
    }

    #[inline]
    fn resolve_node_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<usize, GraphError> {
        self.graph().resolve_node_property(prop, dtype, is_static)
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
    fn internal_add_node(
        &self,
        t: TimeIndexEntry,
        v: VID,
        props: Vec<(usize, Prop)>,
    ) -> Result<(), GraphError> {
        self.graph().internal_add_node(t, v, props)
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

    #[inline(always)]
    fn internal_add_edge_update(
        &self,
        t: TimeIndexEntry,
        edge: EID,
        props: Vec<(usize, Prop)>,
        layer: usize,
    ) -> Result<(), GraphError> {
        self.graph().internal_add_edge_update(t, edge, props, layer)
    }
}
