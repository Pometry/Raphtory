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

    fn resolve_node_type(&self, v_id: VID, node_type: Option<&str>) -> Result<usize, GraphError>;

    /// map external node id to internal id, allocating a new empty node if needed
    fn resolve_node(&self, id: u64, name: Option<&str>) -> VID;

    /// map property key to internal id, allocating new property if needed
    fn resolve_graph_property(&self, prop: &str, is_static: bool) -> usize;

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

    fn process_prop_value(&self, prop: Prop) -> Prop;

    /// add node update
    fn internal_add_node(
        &self,
        t: TimeIndexEntry,
        v: VID,
        props: Vec<(usize, Prop)>,
        node_type_id: usize,
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
    fn resolve_node_type(&self, v_id: VID, node_type: Option<&str>) -> Result<usize, GraphError> {
        self.graph().resolve_node_type(v_id, node_type)
    }

    #[inline]
    fn resolve_node(&self, id: u64, name: Option<&str>) -> VID {
        self.graph().resolve_node(id, name)
    }

    #[inline]
    fn resolve_graph_property(&self, prop: &str, is_static: bool) -> usize {
        self.graph().resolve_graph_property(prop, is_static)
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

    #[inline]
    fn process_prop_value(&self, prop: Prop) -> Prop {
        self.graph().process_prop_value(prop)
    }

    #[inline(always)]
    fn internal_add_node(
        &self,
        t: TimeIndexEntry,
        v: VID,
        props: Vec<(usize, Prop)>,
        node_type_id: usize,
    ) -> Result<(), GraphError> {
        self.graph().internal_add_node(t, v, props, node_type_id)
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
