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
        graph::tgraph::TooManyLayers,
        nodes::node_ref::{AsNodeRef, NodeRef},
        GidRef, EID, ELID, MAX_LAYER, VID,
    },
    storage::{raw_edges::WriteLockedEdges, timeindex::TimeIndexEntry, WriteLockedNodes},
};
use storage::{
    error::DBV4Error,
    pages::{
        node_page::writer::{node_info_as_props, NodeWriter},
        session::WriteSession,
        NODE_ID_PROP_KEY,
    },
    persist::strategy::PersistentStrategy,
    properties::props_meta_writer::PropsMetaWriter,
    resolver::GIDResolverOps,
    segments::{edge::MemEdgeSegment, node::MemNodeSegment},
    Extension, ES, NS,
};

use crate::mutation::{
    addition_ops::{EdgeWriteLock, InternalAdditionOps, SessionAdditionOps},
    MutationError,
};

pub struct WriteS<'a, EXT: PersistentStrategy<NS = NS<EXT>, ES = ES<EXT>>> {
    static_session: WriteSession<'a, NS<EXT>, ES<EXT>, EXT>,
}

#[derive(Clone, Copy, Debug)]
pub struct UnlockedSession<'a> {
    graph: &'a TemporalGraph<Extension>,
}

impl<
        'a,
        EXT: PersistentStrategy<NS = NS<EXT>, ES = ES<EXT>>,
    > EdgeWriteLock for WriteS<'a, EXT>
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
            .map(|eid| eid.with_layer(layer));

        self.static_session
            .add_edge_into_layer(t, src, dst, eid, lsn, props);

        eid
    }

    fn internal_delete_edge(
        &mut self,
        t: TimeIndexEntry,
        src: impl Into<VID>,
        dst: impl Into<VID>,
        lsn: u64,
        layer: usize,
    ) -> MaybeNew<ELID> {
        let src = src.into();
        let dst = dst.into();
        let eid = self
            .static_session
            .add_static_edge(src, dst, lsn)
            .map(|eid| eid.with_layer(layer));

        self.static_session
            .delete_edge_from_layer(t, src, dst, eid, lsn);

        eid
    }

    fn store_src_node_info(&mut self, vid: impl Into<VID>, node_id: Option<GidRef>) {
        if let Some(id) = node_id {
            let pos = self.static_session.resolve_node_pos(vid);
            let prop_id = self.static_session.node_id_prop_id();

            self.static_session
                .node_writers()
                .get_mut_src()
                .update_c_props(pos, 0, [(prop_id, id.into())], 0);
        };
    }

    fn store_dst_node_info(&mut self, vid: impl Into<VID>, node_id: Option<GidRef>) {
        if let Some(id) = node_id {
            let pos = self.static_session.resolve_node_pos(vid);
            let prop_id = self.static_session.node_id_prop_id();

            self.static_session
                .node_writers()
                .get_mut_dst()
                .update_c_props(pos, 0, [(prop_id, id.into())], 0);
        };
    }
}

impl<'a> SessionAdditionOps for UnlockedSession<'a> {
    type Error = MutationError;

    fn next_event_id(&self) -> Result<usize, Self::Error> {
        Ok(self.graph.next_event_id())
    }

    fn reserve_event_ids(&self, num_ids: usize) -> Result<usize, Self::Error> {
        todo!()
    }

    fn set_node(&self, gid: GidRef, vid: VID) -> Result<(), Self::Error> {
        Ok(self.graph.logical_to_physical.set(gid, vid)?)
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

    type WS<'a> = UnlockedSession<'a>;

    type AtomicAddEdge<'a> = WriteS<'a, Extension>;

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
        let vid = self.resolve_node(id)?;
        let node_type_id = self.node_meta().get_or_create_node_type_id(node_type);
        Ok(vid.map(|_| (vid, node_type_id)))

        // let mut entry = self.storage.get_node_mut(vid.inner());
        // let mut entry_ref = entry.to_mut();
        // let node_store = entry_ref.node_store_mut();
        // if node_store.node_type == 0 {
        //     node_store.update_node_type(node_type_id.inner());
        //     Ok(MaybeNew::New((vid, node_type_id)))
        // } else {
        //     let node_type_id = self
        //         .node_meta
        //         .get_node_type_id(node_type)
        //         .filter(|&node_type| node_type == node_store.node_type)
        //         .ok_or(MutationError::NodeTypeError)?;
        //     Ok(MaybeNew::Existing((vid, MaybeNew::Existing(node_type_id))))
        // }
    }

    fn validate_gids<'a>(
        &self,
        gids: impl IntoIterator<Item = GidRef<'a>>,
    ) -> Result<(), Self::Error> {
        self.logical_to_physical.validate_gids(gids)?;
        Ok(())
    }

    fn write_session(&self) -> Result<Self::WS<'_>, Self::Error> {
        Ok(UnlockedSession { graph: self })
    }

    fn atomic_add_edge(
        &self,
        src: VID,
        dst: VID,
        e_id: Option<EID>,
        _layer_id: usize,
    ) -> Result<Self::AtomicAddEdge<'_>, Self::Error> {
        Ok(WriteS {
            static_session: self.storage().write_session(src, dst, e_id),
        })
    }

    fn internal_add_node(
        &self,
        t: TimeIndexEntry,
        v: impl Into<VID>,
        gid: Option<GidRef>,
        node_type: Option<usize>,
        props: impl IntoIterator<Item = (usize, Prop)>,
    ) -> Result<(), Self::Error> {
        let v = v.into();
        let (segment, node_pos) = self.storage().nodes().resolve_pos(v);
        let mut node_writer = self.storage().node_writer(segment);
        let node_info = node_info_as_props(gid, node_type);
        node_writer.add_props(t, node_pos, 0, props, 0);
        node_writer.update_c_props(node_pos, 0, node_info, 0);

        Ok(())
    }

    fn validate_props<PN: AsRef<str>>(
        &self,
        is_static: bool,
        meta: &Meta,
        props: impl Iterator<Item = (PN, Prop)>,
    ) -> Result<Vec<(usize, Prop)>, Self::Error> {
        if is_static {
            let prop_ids = PropsMetaWriter::constant(meta, props)
                .and_then(|pmw| pmw.into_props_const())
                .map_err(MutationError::DBV4Error)?;
            Ok(prop_ids)
        } else {
            let prop_ids = PropsMetaWriter::temporal(meta, props)
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
