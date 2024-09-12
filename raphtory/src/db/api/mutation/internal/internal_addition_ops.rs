use crate::{
    core::{
        entities::{nodes::node_ref::AsNodeRef, EID, VID},
        storage::{raw_edges::WriteLockedEdges, timeindex::TimeIndexEntry, WriteLockedNodes},
        utils::errors::GraphError,
        Prop, PropType,
    },
    db::api::{storage::graph::locked::WriteLockedGraph, view::internal::Base},
};
use enum_dispatch::enum_dispatch;
use raphtory_api::core::{entities::GidType, storage::dict_mapper::MaybeNew};

#[enum_dispatch]
pub trait InternalAdditionOps {
    fn id_type(&self) -> Option<GidType>;
    fn write_lock(&self) -> Result<WriteLockedGraph, GraphError>;

    fn write_lock_nodes(&self) -> Result<WriteLockedNodes, GraphError>;

    fn write_lock_edges(&self) -> Result<WriteLockedEdges, GraphError>;
    fn num_shards(&self) -> Result<usize, GraphError>;
    /// get the sequence id for the next event
    fn next_event_id(&self) -> Result<usize, GraphError>;

    fn reserve_event_ids(&self, num_ids: usize) -> Result<usize, GraphError>;

    /// map layer name to id and allocate a new layer if needed
    fn resolve_layer(&self, layer: Option<&str>) -> Result<MaybeNew<usize>, GraphError>;

    /// map external node id to internal id, allocating a new empty node if needed
    fn resolve_node<V: AsNodeRef>(&self, id: V) -> Result<MaybeNew<VID>, GraphError>;

    /// resolve a node and corresponding type, outer MaybeNew tracks whether the type assignment is new for the node even if both node and type already existed.
    fn resolve_node_and_type<V: AsNodeRef>(
        &self,
        id: V,
        node_type: &str,
    ) -> Result<MaybeNew<(MaybeNew<VID>, MaybeNew<usize>)>, GraphError>;

    /// map property key to internal id, allocating new property if needed
    fn resolve_graph_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, GraphError>;

    /// map property key to internal id, allocating new property if needed and checking property type.
    /// returns `None` if the type does not match
    fn resolve_node_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, GraphError>;

    fn resolve_edge_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, GraphError>;

    /// add node update
    fn internal_add_node(
        &self,
        t: TimeIndexEntry,
        v: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError>;

    /// add edge update
    fn internal_add_edge(
        &self,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        props: &[(usize, Prop)],
        layer: usize,
    ) -> Result<MaybeNew<EID>, GraphError>;

    /// add update for an existing edge
    fn internal_add_edge_update(
        &self,
        t: TimeIndexEntry,
        edge: EID,
        props: &[(usize, Prop)],
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
    #[inline]
    fn id_type(&self) -> Option<GidType> {
        self.graph().id_type()
    }

    #[inline]
    fn write_lock(&self) -> Result<WriteLockedGraph, GraphError> {
        self.graph().write_lock()
    }

    #[inline]
    fn write_lock_nodes(&self) -> Result<WriteLockedNodes, GraphError> {
        self.graph().write_lock_nodes()
    }

    #[inline]
    fn write_lock_edges(&self) -> Result<WriteLockedEdges, GraphError> {
        self.graph().write_lock_edges()
    }

    #[inline]
    fn num_shards(&self) -> Result<usize, GraphError> {
        self.graph().num_shards()
    }

    #[inline(always)]
    fn next_event_id(&self) -> Result<usize, GraphError> {
        self.graph().next_event_id()
    }

    #[inline]
    fn reserve_event_ids(&self, num_ids: usize) -> Result<usize, GraphError> {
        self.graph().reserve_event_ids(num_ids)
    }

    #[inline]
    fn resolve_layer(&self, layer: Option<&str>) -> Result<MaybeNew<usize>, GraphError> {
        self.graph().resolve_layer(layer)
    }

    #[inline]
    fn resolve_node<V: AsNodeRef>(&self, n: V) -> Result<MaybeNew<VID>, GraphError> {
        self.graph().resolve_node(n)
    }

    #[inline]
    fn resolve_node_and_type<V: AsNodeRef>(
        &self,
        id: V,
        node_type: &str,
    ) -> Result<MaybeNew<(MaybeNew<VID>, MaybeNew<usize>)>, GraphError> {
        self.graph().resolve_node_and_type(id, node_type)
    }

    #[inline]
    fn resolve_graph_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, GraphError> {
        self.graph().resolve_graph_property(prop, dtype, is_static)
    }

    #[inline]
    fn resolve_node_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, GraphError> {
        self.graph().resolve_node_property(prop, dtype, is_static)
    }

    #[inline]
    fn resolve_edge_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, GraphError> {
        self.graph().resolve_edge_property(prop, dtype, is_static)
    }

    #[inline(always)]
    fn internal_add_node(
        &self,
        t: TimeIndexEntry,
        v: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        self.graph().internal_add_node(t, v, props)
    }

    #[inline(always)]
    fn internal_add_edge(
        &self,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        props: &[(usize, Prop)],
        layer: usize,
    ) -> Result<MaybeNew<EID>, GraphError> {
        self.graph().internal_add_edge(t, src, dst, props, layer)
    }

    #[inline(always)]
    fn internal_add_edge_update(
        &self,
        t: TimeIndexEntry,
        edge: EID,
        props: &[(usize, Prop)],
        layer: usize,
    ) -> Result<(), GraphError> {
        self.graph().internal_add_edge_update(t, edge, props, layer)
    }
}
