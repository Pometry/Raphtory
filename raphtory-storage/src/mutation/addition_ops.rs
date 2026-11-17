use crate::{
    graph::graph::{GraphStorage, Immutable},
    mutation::{
        addition_ops_ext::{AtomicAddEdge, AtomicAddNode, AtomicUpdateEdge},
        GraphPropWriterT, MutationError,
    },
};
use db4_graph::{TemporalGraph, WriteLockedGraph};
use raphtory_api::{
    core::{
        entities::{
            edges::edge_ref::EdgeRef,
            properties::{
                meta::Meta,
                prop::{AsPropEntry, Prop, PropType},
            },
            GidRef, GidType, LayerId, EID, VID,
        },
        storage::{dict_mapper::MaybeNew, timeindex::EventTime},
    },
    inherit::Base,
    ArcRwLockWriteGuard,
};
use raphtory_core::entities::nodes::node_ref::NodeRef;
use storage::{
    properties::props_meta_writer::PropsMetaWriter, segments::node::segment::MemNodeSegment,
    wal::LSN, Extension,
};

/// A resolved node and node type id; the outer [`MaybeNew`] tracks whether the type assignment is new.
pub type NodeTypeResolution = MaybeNew<(MaybeNew<VID>, MaybeNew<usize>)>;

pub trait InternalAdditionOps {
    fn write_lock(&self) -> Result<WriteLockedGraph<'_, Extension>, MutationError>;

    fn mutable(&self) -> Result<&TemporalGraph, Immutable>;
}

pub trait GraphMutationOps: Send + Sync {
    fn atomic_add_edge(
        &self,
        src: NodeRef,
        dst: NodeRef,
        e_id: Option<EID>,
    ) -> Result<AtomicAddEdge<'_, Extension>, MutationError>;

    fn atomic_update_edge(&self, edge: EdgeRef) -> AtomicUpdateEdge<'_>;

    /// Get or create writer for a node
    fn atomic_add_node(
        &self,
        node: NodeRef,
        node_type: Option<&str>,
    ) -> Result<AtomicAddNode<'_>, MutationError>;

    fn atomic_update_graph_props(&self) -> GraphPropWriterT<'_>;

    fn validate_props<'a, PN: AsRef<str> + 'a, P: Into<Prop>>(
        &self,
        is_static: bool,
        meta: &'a Meta,
        props: impl IntoIterator<Item = (PN, P)>,
    ) -> Result<PropsMetaWriter<'a, PN>, MutationError>;

    unsafe fn bulk_load_resolve_node(
        &self,
        id: GidRef<'_>,
    ) -> Result<(MaybeNew<VID>, Option<ArcRwLockWriteGuard<MemNodeSegment>>), MutationError>;
    fn resolve_layer(&self, layer: Option<&str>) -> MaybeNew<LayerId>;

    fn read_event_id(&self) -> usize;

    /// Sets the event_id to the provided event_id.
    fn set_event_id(&self, event_id: usize) -> ();

    /// get the sequence id for the next event
    fn next_event_id(&self) -> usize;

    /// Reserve a consecutive block of event_ids with length num_ids.
    /// Returns the starting event_id of the reserved block.
    fn reserve_event_ids(&self, num_ids: usize) -> usize;

    /// Sets the event_id to the maximum of the current event_id and the provided event_id.
    /// Returns the old value before the update.
    fn set_max_event_id(&self, event_id: usize) -> usize;

    fn set_node(&self, gid: GidRef, vid: VID) -> Result<(), MutationError>;

    fn id_type(&self) -> Option<GidType>;

    /// map property key to internal id, allocating new property if needed
    fn resolve_graph_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, MutationError>;

    /// map property key to internal id, allocating new property if needed and checking property type.
    /// returns `None` if the type does not match
    fn resolve_node_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, MutationError>;

    fn resolve_edge_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, MutationError>;
}

pub trait PropertyMutationOps {
    /// Add temporal property updates, not allowed to fail (all types should already be validated)
    fn update_properties(
        &mut self,
        t: EventTime,
        layer: LayerId,
        props: impl IntoIterator<Item = (usize, Prop)>,
    );
}
pub trait MetadataMutationOps {
    /// Check metadata updates against existing values and error if any value changes
    fn check_metadata<P: AsPropEntry>(
        &self,
        layer: LayerId,
        props: impl Iterator<Item = P>,
    ) -> Result<(), MutationError>;

    /// Update the metadata, overwriting any existing values
    fn update_metadata(&mut self, layer: LayerId, props: impl IntoIterator<Item = (usize, Prop)>);
}

pub trait SetLsn {
    fn set_lsn(&mut self, lsn: LSN);
}

pub trait EdgeWriteLock: SetLsn + MetadataMutationOps + PropertyMutationOps + Send + Sync {
    fn internal_delete_edge(&mut self, t: EventTime, layer: LayerId);

    fn src(&self) -> MaybeNew<VID>;

    fn dst(&self) -> MaybeNew<VID>;

    fn eid(&self) -> MaybeNew<EID>;
}

pub trait NodeWriteLock: SetLsn + MetadataMutationOps + PropertyMutationOps + Send + Sync {
    fn can_set_type(&self) -> bool;

    fn get_type(&self) -> usize;

    fn set_type(&mut self, node_type: usize);

    fn node(&self) -> MaybeNew<VID>;
}

impl InternalAdditionOps for GraphStorage {
    fn write_lock(&self) -> Result<WriteLockedGraph<'_, Extension>, MutationError> {
        self.mutable()?.write_lock()
    }

    fn mutable(&self) -> Result<&TemporalGraph, Immutable> {
        match self {
            GraphStorage::Locked(_) => Err(Immutable::ReadLockedImmutable),
            GraphStorage::Unlocked(graph) => Ok(graph),
        }
    }
}

pub trait InheritAdditionOps: Base {}

impl<G: InheritAdditionOps> InternalAdditionOps for G
where
    G::Base: InternalAdditionOps,
{
    #[inline]
    fn write_lock(&self) -> Result<WriteLockedGraph<'_, Extension>, MutationError> {
        self.base().write_lock()
    }

    #[inline]
    fn mutable(&self) -> Result<&TemporalGraph, Immutable> {
        self.base().mutable()
    }
}
