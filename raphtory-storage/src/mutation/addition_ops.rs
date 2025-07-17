use crate::{
    graph::graph::GraphStorage,
    mutation::{
        addition_ops_ext::{UnlockedSession, WriteS},
        MutationError,
    },
};
use db4_graph::WriteLockedGraph;
use raphtory_api::{
    core::{
        entities::{
            properties::{
                meta::Meta,
                prop::{Prop, PropType},
            },
            GidRef, EID, VID,
        },
        storage::{dict_mapper::MaybeNew, timeindex::TimeIndexEntry},
    },
    inherit::Base,
};
use raphtory_core::{
    entities::{nodes::node_ref::NodeRef, ELID},
    storage::{raw_edges::WriteLockedEdges, WriteLockedNodes},
};
use storage::Extension;

pub trait InternalAdditionOps {
    type Error: From<MutationError>;
    type WS<'a>: SessionAdditionOps<Error = Self::Error>
    where
        Self: 'a;

    type AtomicAddEdge<'a>: EdgeWriteLock
    where
        Self: 'a;

    fn write_lock(&self) -> Result<WriteLockedGraph<Extension>, Self::Error>;
    fn write_lock_nodes(&self) -> Result<WriteLockedNodes, Self::Error>;
    fn write_lock_edges(&self) -> Result<WriteLockedEdges, Self::Error>;
    /// map layer name to id and allocate a new layer if needed
    fn resolve_layer(&self, layer: Option<&str>) -> Result<MaybeNew<usize>, Self::Error>;
    /// map external node id to internal id, allocating a new empty node if needed
    fn resolve_node(&self, id: NodeRef) -> Result<MaybeNew<VID>, Self::Error>;
    /// resolve a node and corresponding type, outer MaybeNew tracks whether the type assignment is new for the node even if both node and type already existed.
    fn resolve_node_and_type(
        &self,
        id: NodeRef,
        node_type: &str,
    ) -> Result<MaybeNew<(MaybeNew<VID>, MaybeNew<usize>)>, Self::Error>;

    fn resolve_node_and_type_fast(
        &self,
        id: NodeRef,
        node_type: Option<&str>,
    ) -> Result<(VID, usize), Self::Error> {
        match node_type {
            Some(node_type) => {
                let (vid, node_type_id) = self.resolve_node_and_type(id, node_type)?.inner();
                Ok((vid.inner(), node_type_id.inner()))
            }
            None => {
                let vid = self.resolve_node(id)?.inner();
                Ok((vid, 0))
            }
        }
    }

    /// validate the GidRef is the correct type
    fn validate_gids<'a>(
        &self,
        gids: impl IntoIterator<Item = GidRef<'a>>,
    ) -> Result<(), Self::Error>;

    fn write_session(&self) -> Result<Self::WS<'_>, Self::Error>;

    fn atomic_add_edge(
        &self,
        src: VID,
        dst: VID,
        e_id: Option<EID>,
        layer_id: usize,
    ) -> Result<Self::AtomicAddEdge<'_>, Self::Error>;

    fn internal_add_node(
        &self,
        t: TimeIndexEntry,
        v: impl Into<VID>,
        gid: Option<GidRef>,
        node_type: Option<usize>,
        props: impl IntoIterator<Item = (usize, Prop)>,
    ) -> Result<(), Self::Error>;

    fn validate_props<PN: AsRef<str>>(
        &self,
        is_static: bool,
        meta: &Meta,
        prop: impl Iterator<Item = (PN, Prop)>,
    ) -> Result<Vec<(usize, Prop)>, Self::Error>;
}

pub trait EdgeWriteLock: Send + Sync {
    fn internal_add_static_edge(
        &mut self,
        src: impl Into<VID>,
        dst: impl Into<VID>,
        lsn: u64,
    ) -> MaybeNew<EID>;

    /// add edge update
    fn internal_add_edge(
        &mut self,
        t: TimeIndexEntry,
        src: impl Into<VID>,
        dst: impl Into<VID>,
        eid: MaybeNew<ELID>,
        lsn: u64,
        props: impl IntoIterator<Item = (usize, Prop)>,
    ) -> MaybeNew<ELID>;

    fn internal_delete_edge(
        &mut self,
        t: TimeIndexEntry,
        src: impl Into<VID>,
        dst: impl Into<VID>,
        lsn: u64,
        layer: usize,
    ) -> MaybeNew<ELID>;

    fn store_src_node_info(&mut self, id: impl Into<VID>, node_id: Option<GidRef>);
    fn store_dst_node_info(&mut self, id: impl Into<VID>, node_id: Option<GidRef>);
}

pub trait AtomicNodeAddition: Send + Sync {
    /// add node update
    fn internal_add_node(
        &mut self,
        t: TimeIndexEntry,
        v: impl Into<VID>,
        props: impl IntoIterator<Item = (usize, Prop)>,
    ) -> Result<(), MutationError>;
}

pub trait SessionAdditionOps: Send + Sync {
    type Error: From<MutationError>;
    /// get the sequence id for the next event
    fn next_event_id(&self) -> Result<usize, Self::Error>;
    fn reserve_event_ids(&self, num_ids: usize) -> Result<usize, Self::Error>;
    fn set_node(&self, gid: GidRef, vid: VID) -> Result<(), Self::Error>;
    /// map property key to internal id, allocating new property if needed
    fn resolve_graph_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, Self::Error>;
    /// map property key to internal id, allocating new property if needed and checking property type.
    /// returns `None` if the type does not match
    fn resolve_node_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, Self::Error>;
    fn resolve_edge_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, Self::Error>;
    /// add node update
    fn internal_add_node(
        &self,
        t: TimeIndexEntry,
        v: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error>;
    /// add edge update
    fn internal_add_edge(
        &self,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        props: &[(usize, Prop)],
        layer: usize,
    ) -> Result<MaybeNew<EID>, Self::Error>;
    /// add update for an existing edge
    fn internal_add_edge_update(
        &self,
        t: TimeIndexEntry,
        edge: EID,
        props: &[(usize, Prop)],
        layer: usize,
    ) -> Result<(), Self::Error>;
}

impl InternalAdditionOps for GraphStorage {
    type Error = MutationError;
    type WS<'b> = UnlockedSession<'b>;

    type AtomicAddEdge<'a> = WriteS<'a, Extension>;

    fn write_lock(&self) -> Result<WriteLockedGraph<Extension>, Self::Error> {
        self.mutable()?.write_lock()
    }

    fn write_lock_nodes(&self) -> Result<WriteLockedNodes, Self::Error> {
        self.mutable()?.write_lock_nodes()
    }

    fn write_lock_edges(&self) -> Result<WriteLockedEdges, Self::Error> {
        self.mutable()?.write_lock_edges()
    }

    fn resolve_layer(&self, layer: Option<&str>) -> Result<MaybeNew<usize>, Self::Error> {
        self.mutable()?.resolve_layer(layer)
    }

    fn resolve_node(&self, id: NodeRef) -> Result<MaybeNew<VID>, Self::Error> {
        Ok(self.mutable()?.resolve_node(id)?)
    }

    fn resolve_node_and_type(
        &self,
        id: NodeRef,
        node_type: &str,
    ) -> Result<MaybeNew<(MaybeNew<VID>, MaybeNew<usize>)>, Self::Error> {
        Ok(self.mutable()?.resolve_node_and_type(id, node_type)?)
    }

    fn write_session(&self) -> Result<Self::WS<'_>, Self::Error> {
        self.mutable()?.write_session()
    }

    fn atomic_add_edge(
        &self,
        src: VID,
        dst: VID,
        e_id: Option<EID>,
        layer_id: usize,
    ) -> Result<Self::AtomicAddEdge<'_>, Self::Error> {
        self.mutable()?.atomic_add_edge(src, dst, e_id, layer_id)
    }

    fn internal_add_node(
        &self,
        t: TimeIndexEntry,
        v: impl Into<VID>,
        gid: Option<GidRef>,
        node_type: Option<usize>,
        props: impl IntoIterator<Item = (usize, Prop)>,
    ) -> Result<(), Self::Error> {
        self.mutable()?
            .internal_add_node(t, v, gid, node_type, props)
    }

    fn validate_props<PN: AsRef<str>>(
        &self,
        is_static: bool,
        meta: &Meta,
        prop: impl Iterator<Item = (PN, Prop)>,
    ) -> Result<Vec<(usize, Prop)>, Self::Error> {
        self.mutable()?
            .validate_props(is_static, meta, prop)
            .map_err(MutationError::from)
    }

    fn validate_gids<'a>(
        &self,
        gids: impl IntoIterator<Item = GidRef<'a>>,
    ) -> Result<(), Self::Error> {
        Ok(self.mutable()?.validate_gids(gids)?)
    }
}

pub trait InheritAdditionOps: Base {}

impl<G: InheritAdditionOps> InternalAdditionOps for G
where
    G::Base: InternalAdditionOps,
{
    type Error = <G::Base as InternalAdditionOps>::Error;
    type WS<'a>
        = <G::Base as InternalAdditionOps>::WS<'a>
    where
        <G as Base>::Base: 'a,
        G: 'a;

    type AtomicAddEdge<'a>
        = <G::Base as InternalAdditionOps>::AtomicAddEdge<'a>
    where
        <G as Base>::Base: 'a,
        G: 'a;

    #[inline]
    fn write_lock(&self) -> Result<WriteLockedGraph<Extension>, Self::Error> {
        self.base().write_lock()
    }

    #[inline]
    fn write_lock_nodes(&self) -> Result<WriteLockedNodes, Self::Error> {
        self.base().write_lock_nodes()
    }

    #[inline]
    fn write_lock_edges(&self) -> Result<WriteLockedEdges, Self::Error> {
        self.base().write_lock_edges()
    }

    #[inline]
    fn resolve_layer(&self, layer: Option<&str>) -> Result<MaybeNew<usize>, Self::Error> {
        self.base().resolve_layer(layer)
    }

    #[inline]
    fn resolve_node(&self, id: NodeRef) -> Result<MaybeNew<VID>, Self::Error> {
        self.base().resolve_node(id)
    }

    #[inline]
    fn resolve_node_and_type(
        &self,
        id: NodeRef,
        node_type: &str,
    ) -> Result<MaybeNew<(MaybeNew<VID>, MaybeNew<usize>)>, Self::Error> {
        self.base().resolve_node_and_type(id, node_type)
    }

    #[inline]
    fn write_session(&self) -> Result<Self::WS<'_>, Self::Error> {
        self.base().write_session()
    }

    #[inline]
    fn atomic_add_edge(
        &self,
        src: VID,
        dst: VID,
        e_id: Option<EID>,
        layer_id: usize,
    ) -> Result<Self::AtomicAddEdge<'_>, Self::Error> {
        self.base().atomic_add_edge(src, dst, e_id, layer_id)
    }

    #[inline]
    fn internal_add_node(
        &self,
        t: TimeIndexEntry,
        v: impl Into<VID>,
        gid: Option<GidRef>,
        node_type: Option<usize>,
        props: impl IntoIterator<Item = (usize, Prop)>,
    ) -> Result<(), Self::Error> {
        self.base().internal_add_node(t, v, gid, node_type, props)
    }

    #[inline]
    fn validate_props<PN: AsRef<str>>(
        &self,
        is_static: bool,
        meta: &Meta,
        prop: impl Iterator<Item = (PN, Prop)>,
    ) -> Result<Vec<(usize, Prop)>, Self::Error> {
        self.base().validate_props(is_static, meta, prop)
    }

    #[inline]
    fn validate_gids<'a>(
        &self,
        gids: impl IntoIterator<Item = GidRef<'a>>,
    ) -> Result<(), Self::Error> {
        self.base().validate_gids(gids)
    }
}
