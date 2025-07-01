use std::ops::DerefMut;

use db4_graph::{TemporalGraph, WriteLockedGraph};
use parking_lot::RwLockWriteGuard;
use raphtory_api::core::{
    entities::properties::{
        meta::Meta,
        prop::{Prop, PropType},
    },
    storage::dict_mapper::MaybeNew,
};
use raphtory_core::{
    entities::{
        graph::tgraph::TooManyLayers, nodes::node_ref::NodeRef, GidRef, EID, ELID, MAX_LAYER, VID,
    },
    storage::{raw_edges::WriteLockedEdges, timeindex::TimeIndexEntry, WriteLockedNodes},
};
use storage::{
    pages::{session::WriteSession, NODE_ID_PROP_KEY},
    persist::strategy::PersistentStrategy,
    properties::props_meta_writer::PropsMetaWriter,
    resolver::GIDResolverOps,
    segments::{edge::MemEdgeSegment, node::MemNodeSegment},
    Extension, ES, NS,
};

use crate::mutation::{
    addition_ops::{AtomicAdditionOps, InternalAdditionOps, SessionAdditionOps},
    MutationError,
};

pub struct WriteS<
    'a,
    MNS: DerefMut<Target = MemNodeSegment>,
    MES: DerefMut<Target = MemEdgeSegment>,
    EXT: PersistentStrategy<NS = NS<EXT>, ES = ES<EXT>>,
> {
    static_session: WriteSession<'a, MNS, MES, NS<EXT>, ES<EXT>, EXT>,
    layer: Option<WriteSession<'a, MNS, MES, NS<EXT>, ES<EXT>, EXT>>,
}

pub struct UnlockedSession<'a, EXT> {
    graph: &'a TemporalGraph<EXT>,
}

impl<
        'a,
        MNS: DerefMut<Target = MemNodeSegment> + Send + Sync,
        MES: DerefMut<Target = MemEdgeSegment> + Send + Sync,
        EXT: PersistentStrategy<NS = NS<EXT>, ES = ES<EXT>>,
    > AtomicAdditionOps for WriteS<'a, MNS, MES, EXT>
{
    fn internal_add_edge(
        &mut self,
        t: TimeIndexEntry,
        src: impl Into<VID>,
        dst: impl Into<VID>,
        lsn: u64,
        layer: usize,
        props: impl IntoIterator<Item = (usize, Prop)>,
    ) -> MaybeNew<ELID> {
        let src = src.into();
        let dst = dst.into();
        let eid = self
            .static_session
            .add_static_edge(src, dst, lsn)
            .map(|eid| eid.with_layer(0));

        self.static_session
            .add_edge_into_layer(t, src, dst, eid, lsn, props);

        // TODO: consider storing node id as const prop here?

        eid
    }

    fn store_node_id_as_prop(&mut self, id: NodeRef, vid: impl Into<VID>) {
        match id {
            NodeRef::External(id) => {
                let vid = vid.into();
                let _ = self.static_session.store_node_id_as_prop(id, vid);
            }
            NodeRef::Internal(_) => (),
        }
    }
}

impl<'a, EXT: Send + Sync> SessionAdditionOps for UnlockedSession<'a, EXT> {
    type Error = MutationError;

    fn next_event_id(&self) -> Result<usize, Self::Error> {
        todo!()
    }

    fn reserve_event_ids(&self, num_ids: usize) -> Result<usize, Self::Error> {
        todo!()
    }

    fn set_node(&self, gid: GidRef, vid: VID) -> Result<(), Self::Error> {
        todo!()
    }

    fn resolve_graph_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, Self::Error> {
        todo!()
    }

    fn resolve_node_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, Self::Error> {
        todo!()
    }

    fn resolve_edge_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, Self::Error> {
        todo!()
    }

    fn internal_add_node(
        &self,
        t: TimeIndexEntry,
        v: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error> {
        todo!()
    }

    fn internal_add_edge(
        &self,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        props: &[(usize, Prop)],
        layer: usize,
    ) -> Result<MaybeNew<EID>, Self::Error> {
        todo!()
    }

    fn internal_add_edge_update(
        &self,
        t: TimeIndexEntry,
        edge: EID,
        props: &[(usize, Prop)],
        layer: usize,
    ) -> Result<(), Self::Error> {
        todo!()
    }
}

impl InternalAdditionOps for TemporalGraph {
    type Error = MutationError;

    type WS<'a> = UnlockedSession<'a, Extension>;

    type AtomicAddEdge<'a> = WriteS<
        'a,
        RwLockWriteGuard<'a, MemNodeSegment>,
        RwLockWriteGuard<'a, MemEdgeSegment>,
        Extension,
    >;

    fn write_lock(&self) -> Result<WriteLockedGraph<Extension>, Self::Error> {
        let locked_g = self.write_locked_graph();
        Ok(locked_g)
    }

    fn write_lock_nodes(&self) -> Result<WriteLockedNodes, Self::Error> {
        todo!()
    }

    fn write_lock_edges(&self) -> Result<WriteLockedEdges, Self::Error> {
        todo!()
    }

    fn resolve_layer(&self, layer: Option<&str>) -> Result<MaybeNew<usize>, Self::Error> {
        let id = self.edge_meta().get_or_create_layer_id(layer);
        if let MaybeNew::New(id) = id {
            if id > MAX_LAYER {
                Err(TooManyLayers)?;
            }
        }
        Ok(id)
    }

    fn resolve_node(&self, id: NodeRef) -> Result<MaybeNew<VID>, Self::Error> {
        match id {
            NodeRef::External(id) => {
                let id = self.logical_to_physical.get_or_init(id, || {
                    // When initializing a new node, reserve node_id as a const prop.
                    // Done here since the id type is not known until node creation.
                    reserve_node_id_as_prop(self.node_meta(), id);

                    self.node_count
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                        .into()
                })?;

                Ok(id)
            }
            NodeRef::Internal(id) => Ok(MaybeNew::Existing(id)),
        }
    }

    fn resolve_node_and_type(
        &self,
        id: NodeRef,
        node_type: &str,
    ) -> Result<MaybeNew<(MaybeNew<VID>, MaybeNew<usize>)>, Self::Error> {
        todo!()
    }

    fn validate_gids<'a>(
        &self,
        gids: impl IntoIterator<Item = GidRef<'a>>,
    ) -> Result<(), Self::Error> {
        self.logical_to_physical.validate_gids(gids)?;
        Ok(())
    }

    fn write_session(&self) -> Result<Self::WS<'_>, Self::Error> {
        todo!()
    }

    fn atomic_add_edge(
        &self,
        src: VID,
        dst: VID,
        e_id: Option<EID>,
        layer_id: usize,
    ) -> Self::AtomicAddEdge<'_> {
        let static_session = self.storage().write_session(src, dst, e_id);

        WriteS {
            static_session,
            layer: None,
        }
    }

    fn validate_edge_props<PN: AsRef<str>>(
        &self,
        is_static: bool,
        props: impl ExactSizeIterator<Item = (PN, Prop)>,
    ) -> Result<Vec<(usize, Prop)>, Self::Error> {
        if is_static {
            let prop_ids = PropsMetaWriter::constant(self.edge_meta(), props)
                .and_then(|pmw| pmw.into_props_const())
                .map_err(MutationError::DBV4Error)?;
            Ok(prop_ids)
        } else {
            let prop_ids = PropsMetaWriter::temporal(self.edge_meta(), props)
                .and_then(|pmw| pmw.into_props_temporal())
                .map_err(MutationError::DBV4Error)?;
            Ok(prop_ids)
        }
    }
}

fn reserve_node_id_as_prop(node_meta: &Meta, id: GidRef) -> usize {
    match id {
        GidRef::U64(_) => node_meta
            .const_prop_meta()
            .get_or_create_and_validate(NODE_ID_PROP_KEY, PropType::U64)
            .unwrap()
            .inner(),
        GidRef::Str(_) => node_meta
            .const_prop_meta()
            .get_or_create_and_validate(NODE_ID_PROP_KEY, PropType::Str)
            .unwrap()
            .inner(),
    }
}
