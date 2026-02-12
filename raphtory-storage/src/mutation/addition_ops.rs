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
        storage::{dict_mapper::MaybeNew, timeindex::EventTime},
    },
    inherit::Base,
};
use raphtory_core::entities::{nodes::node_ref::NodeRef, ELID};
use storage::{wal::LSN, Extension};

pub trait InternalAdditionOps {
    type Error: From<MutationError>;
    type WS<'a>: SessionAdditionOps<Error = Self::Error>
    where
        Self: 'a;

    type AtomicAddEdge<'a>: EdgeWriteLock
    where
        Self: 'a;

    fn write_lock(&self) -> Result<WriteLockedGraph<'_, Extension>, Self::Error>;

    /// map layer name to id and allocate a new layer if needed
    fn resolve_layer(&self, layer: Option<&str>) -> Result<MaybeNew<usize>, Self::Error>;

    /// map external node id to internal id, allocating a new empty node if needed
    fn resolve_node(&self, id: NodeRef) -> Result<MaybeNew<VID>, Self::Error>;

    /// Resolve a node and corresponding type, outer MaybeNew tracks whether the type
    /// assignment is new for the node even if both node and type already existed.
    /// updates the storage atomically to set the node type
    fn resolve_and_update_node_and_type(
        &self,
        id: NodeRef,
        node_type: Option<&str>,
    ) -> Result<MaybeNew<(MaybeNew<VID>, MaybeNew<usize>)>, Self::Error>;

    /// resolve node and type without modifying the storage (use in bulk loaders only)
    fn resolve_node_and_type(
        &self,
        id: NodeRef,
        node_type: Option<&str>,
    ) -> Result<(VID, usize), Self::Error>;

    /// validate the GidRef is the correct type
    fn validate_gids<'a>(
        &self,
        gids: impl IntoIterator<Item = GidRef<'a>>,
    ) -> Result<(), Self::Error>;

    fn write_session(&self) -> Result<Self::WS<'_>, Self::Error>;

    fn atomic_add_edge(
        &self,
        src: NodeRef,
        dst: NodeRef,
        e_id: Option<EID>,
        layer_id: usize,
    ) -> Result<Self::AtomicAddEdge<'_>, Self::Error>;

    fn internal_add_node(
        &self,
        t: EventTime,
        v: VID,
        props: Vec<(usize, Prop)>,
    ) -> Result<(), Self::Error>;

    fn validate_props<PN: AsRef<str>>(
        &self,
        is_static: bool,
        meta: &Meta,
        prop: impl Iterator<Item = (PN, Prop)>,
    ) -> Result<Vec<(usize, Prop)>, Self::Error>;

    /// Validates props and returns them with their creation status (new vs existing)
    fn validate_props_with_status<PN: AsRef<str>>(
        &self,
        is_static: bool,
        meta: &Meta,
        props: impl Iterator<Item = (PN, Prop)>,
    ) -> Result<Vec<MaybeNew<(PN, usize, Prop)>>, Self::Error>;
}

pub trait EdgeWriteLock: Send + Sync {
    /// add edge update
    fn internal_add_update(
        &mut self,
        t: EventTime,
        layer: usize,
        props: impl IntoIterator<Item = (usize, Prop)>,
    );

    fn internal_delete_edge(&mut self, t: EventTime, layer: usize);

    fn set_lsn(&mut self, lsn: LSN);

    fn src(&self) -> MaybeNew<VID>;

    fn dst(&self) -> MaybeNew<VID>;

    fn eid(&self) -> MaybeNew<EID>;
}

pub trait SessionAdditionOps: Send + Sync {
    type Error: From<MutationError>;

    /// Reads the current event id.
    fn read_event_id(&self) -> Result<usize, Self::Error>;

    /// Sets the event_id to the provided event_id.
    fn set_event_id(&self, event_id: usize) -> Result<(), Self::Error>;

    /// get the sequence id for the next event
    fn next_event_id(&self) -> Result<usize, Self::Error>;

    /// Reserve a consecutive block of event_ids with length num_ids.
    /// Returns the starting event_id of the reserved block.
    fn reserve_event_ids(&self, num_ids: usize) -> Result<usize, Self::Error>;

    /// Sets the event_id to the maximum of the current event_id and the provided event_id.
    /// Returns the old value before the update.
    fn set_max_event_id(&self, event_id: usize) -> Result<usize, Self::Error>;

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
}

impl InternalAdditionOps for GraphStorage {
    type Error = MutationError;
    type WS<'b> = UnlockedSession<'b>;

    type AtomicAddEdge<'a> = WriteS<'a, Extension>;

    fn write_lock(&self) -> Result<WriteLockedGraph<'_, Extension>, Self::Error> {
        self.mutable()?.write_lock()
    }

    fn resolve_layer(&self, layer: Option<&str>) -> Result<MaybeNew<usize>, Self::Error> {
        self.mutable()?.resolve_layer(layer)
    }

    fn resolve_node(&self, id: NodeRef) -> Result<MaybeNew<VID>, Self::Error> {
        self.mutable()?.resolve_node(id)
    }

    fn resolve_and_update_node_and_type(
        &self,
        id: NodeRef,
        node_type: Option<&str>,
    ) -> Result<MaybeNew<(MaybeNew<VID>, MaybeNew<usize>)>, Self::Error> {
        Ok(self
            .mutable()?
            .resolve_and_update_node_and_type(id, node_type)?)
    }

    fn write_session(&self) -> Result<Self::WS<'_>, Self::Error> {
        self.mutable()?.write_session()
    }

    fn atomic_add_edge(
        &self,
        src: NodeRef,
        dst: NodeRef,
        e_id: Option<EID>,
        layer_id: usize,
    ) -> Result<Self::AtomicAddEdge<'_>, Self::Error> {
        self.mutable()?.atomic_add_edge(src, dst, e_id, layer_id)
    }

    fn internal_add_node(
        &self,
        t: EventTime,
        v: VID,
        props: Vec<(usize, Prop)>,
    ) -> Result<(), Self::Error> {
        self.mutable()?.internal_add_node(t, v, props)
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

    fn validate_props_with_status<PN: AsRef<str>>(
        &self,
        is_static: bool,
        meta: &Meta,
        props: impl Iterator<Item = (PN, Prop)>,
    ) -> Result<Vec<MaybeNew<(PN, usize, Prop)>>, Self::Error> {
        self.mutable()?
            .validate_props_with_status(is_static, meta, props)
            .map_err(MutationError::from)
    }

    fn validate_gids<'a>(
        &self,
        gids: impl IntoIterator<Item = GidRef<'a>>,
    ) -> Result<(), Self::Error> {
        Ok(self.mutable()?.validate_gids(gids)?)
    }

    fn resolve_node_and_type(
        &self,
        id: NodeRef,
        node_type: Option<&str>,
    ) -> Result<(VID, usize), Self::Error> {
        self.mutable()?
            .resolve_node_and_type(id, node_type)
            .map_err(MutationError::from)
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
    fn write_lock(&self) -> Result<WriteLockedGraph<'_, Extension>, Self::Error> {
        self.base().write_lock()
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
    fn resolve_and_update_node_and_type(
        &self,
        id: NodeRef,
        node_type: Option<&str>,
    ) -> Result<MaybeNew<(MaybeNew<VID>, MaybeNew<usize>)>, Self::Error> {
        self.base().resolve_and_update_node_and_type(id, node_type)
    }

    #[inline]
    fn write_session(&self) -> Result<Self::WS<'_>, Self::Error> {
        self.base().write_session()
    }

    #[inline]
    fn atomic_add_edge(
        &self,
        src: NodeRef,
        dst: NodeRef,
        e_id: Option<EID>,
        layer_id: usize,
    ) -> Result<Self::AtomicAddEdge<'_>, Self::Error> {
        self.base().atomic_add_edge(src, dst, e_id, layer_id)
    }

    #[inline]
    fn internal_add_node(
        &self,
        t: EventTime,
        v: VID,
        props: Vec<(usize, Prop)>,
    ) -> Result<(), Self::Error> {
        self.base().internal_add_node(t, v, props)
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
    fn validate_props_with_status<PN: AsRef<str>>(
        &self,
        is_static: bool,
        meta: &Meta,
        props: impl Iterator<Item = (PN, Prop)>,
    ) -> Result<Vec<MaybeNew<(PN, usize, Prop)>>, Self::Error> {
        self.base()
            .validate_props_with_status(is_static, meta, props)
    }

    #[inline]
    fn validate_gids<'a>(
        &self,
        gids: impl IntoIterator<Item = GidRef<'a>>,
    ) -> Result<(), Self::Error> {
        self.base().validate_gids(gids)
    }

    fn resolve_node_and_type(
        &self,
        id: NodeRef,
        node_type: Option<&str>,
    ) -> Result<(VID, usize), Self::Error> {
        self.base().resolve_node_and_type(id, node_type)
    }
}
