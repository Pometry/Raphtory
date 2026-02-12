use crate::mutation::{
    addition_ops::{EdgeWriteLock, InternalAdditionOps, SessionAdditionOps},
    durability_ops::DurabilityOps,
    MutationError,
};
use db4_graph::{TemporalGraph, WriteLockedGraph};
use parking_lot::RwLockWriteGuard;
use raphtory_api::core::{
    entities::properties::{
        meta::{Meta, NODE_ID_IDX, NODE_TYPE_IDX, STATIC_GRAPH_LAYER_ID},
        prop::{Prop, PropType, PropUnwrap},
    },
    storage::dict_mapper::MaybeNew,
};
use raphtory_core::{
    entities::{
        graph::tgraph::TooManyLayers,
        nodes::node_ref::{AsNodeRef, NodeRef},
        GidRef, EID, ELID, MAX_LAYER, VID,
    },
    storage::timeindex::EventTime,
};
use std::sync::atomic::Ordering;
use storage::{
    api::{edges::EdgeSegmentOps, graph_props::GraphPropSegmentOps, nodes::NodeSegmentOps},
    pages::{
        node_page::writer::{node_info_as_props, NodeWriter, NodeWriters},
        node_store::increment_and_clamp,
        session::WriteSession,
    },
    persist::{config::ConfigOps, strategy::PersistenceStrategy},
    properties::props_meta_writer::PropsMetaWriter,
    resolver::GIDResolverOps,
    transaction::TransactionManager,
    wal::LSN,
    Extension, LocalPOS, Wal, ES, GS, NS,
};

pub struct WriteS<'a, EXT>
where
    EXT: PersistenceStrategy<NS = NS<EXT>, ES = ES<EXT>, GS = GS<EXT>>,
    NS<EXT>: NodeSegmentOps<Extension = EXT>,
    ES<EXT>: EdgeSegmentOps<Extension = EXT>,
    GS<EXT>: GraphPropSegmentOps<Extension = EXT>,
{
    static_session: WriteSession<'a, NS<EXT>, ES<EXT>, GS<EXT>, EXT>,
    src: MaybeNew<VID>,
    dst: MaybeNew<VID>,
    eid: MaybeNew<EID>,
}

#[derive(Clone, Copy, Debug)]
pub struct UnlockedSession<'a> {
    graph: &'a TemporalGraph<Extension>,
}

impl<'a, EXT> EdgeWriteLock for WriteS<'a, EXT>
where
    EXT: PersistenceStrategy<NS = NS<EXT>, ES = ES<EXT>, GS = GS<EXT>>,
    NS<EXT>: NodeSegmentOps<Extension = EXT>,
    ES<EXT>: EdgeSegmentOps<Extension = EXT>,
    GS<EXT>: GraphPropSegmentOps<Extension = EXT>,
{
    fn internal_add_update(
        &mut self,
        t: EventTime,
        layer: usize,
        props: impl IntoIterator<Item = (usize, Prop)>,
    ) {
        self.static_session.add_edge_into_layer(
            t,
            self.src.inner(),
            self.dst.inner(),
            self.eid.map(|eid| eid.with_layer(layer)),
            props,
        );
    }

    fn internal_delete_edge(&mut self, t: EventTime, layer: usize) {
        self.static_session.delete_edge_from_layer(
            t,
            self.src.inner(),
            self.dst.inner(),
            self.eid.map(|eid| eid.with_layer_deletion(layer)),
        );
    }

    fn set_lsn(&mut self, lsn: LSN) {
        self.static_session.set_lsn(lsn);
    }

    fn src(&self) -> MaybeNew<VID> {
        self.src
    }

    fn dst(&self) -> MaybeNew<VID> {
        self.dst
    }

    fn eid(&self) -> MaybeNew<EID> {
        self.eid
    }
}

impl<'a> SessionAdditionOps for UnlockedSession<'a> {
    type Error = MutationError;

    fn read_event_id(&self) -> Result<usize, Self::Error> {
        Ok(self.graph.storage().read_event_id())
    }

    fn set_event_id(&self, event_id: usize) -> Result<(), Self::Error> {
        Ok(self.graph.storage().set_event_id(event_id))
    }

    fn next_event_id(&self) -> Result<usize, Self::Error> {
        Ok(self.graph.storage().next_event_id())
    }

    fn reserve_event_ids(&self, num_ids: usize) -> Result<usize, Self::Error> {
        let event_id = self.graph.storage().reserve_event_ids(num_ids);
        Ok(event_id)
    }

    fn set_max_event_id(&self, value: usize) -> Result<usize, Self::Error> {
        Ok(self.graph.storage().set_max_event_id(value))
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
        Ok(self
            .graph
            .graph_props_meta()
            .resolve_prop_id(prop, dtype, is_static)?)
    }

    fn resolve_node_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, Self::Error> {
        Ok(self
            .graph
            .node_meta()
            .resolve_prop_id(prop, dtype, is_static)?)
    }

    fn resolve_edge_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, Self::Error> {
        Ok(self
            .graph
            .edge_meta()
            .resolve_prop_id(prop, dtype, is_static)?)
    }
}

impl InternalAdditionOps for TemporalGraph {
    type Error = MutationError;
    type WS<'a> = UnlockedSession<'a>;
    type AtomicAddEdge<'a> = WriteS<'a, Extension>;

    fn write_lock(&self) -> Result<WriteLockedGraph<'_, Extension>, Self::Error> {
        let locked_g = self.write_locked_graph();
        Ok(locked_g)
    }

    fn resolve_layer(&self, layer: Option<&str>) -> Result<MaybeNew<usize>, Self::Error> {
        let id = self.edge_meta().get_or_create_layer_id(layer);
        // TODO: we replicate the layer id in the node meta as well, perhaps layer meta should be common
        if id.is_new() {
            self.node_meta().layer_meta().set_id(
                self.edge_meta().layer_meta().get_name(id.inner()),
                id.inner(),
            );
        }
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
                    let (seg, pos) = self.storage().nodes().reserve_free_pos(
                        self.event_counter
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                    );
                    pos.as_vid(seg, self.extension().config().max_node_page_len())
                })?;

                Ok(id)
            }
            NodeRef::Internal(id) => Ok(MaybeNew::Existing(id)),
        }
    }

    fn resolve_and_update_node_and_type(
        &self,
        id: NodeRef,
        node_type: Option<&str>,
    ) -> Result<MaybeNew<(MaybeNew<VID>, MaybeNew<usize>)>, Self::Error> {
        let vid = self.resolve_node(id)?;
        let (segment_id, local_pos) = self.storage().nodes().resolve_pos(vid.inner());
        let mut writer = self.storage().nodes().writer(segment_id);
        let node_type_id = match node_type {
            None => {
                writer.update_c_props(
                    local_pos,
                    STATIC_GRAPH_LAYER_ID,
                    node_info_as_props(id.as_gid_ref(), None),
                );
                MaybeNew::Existing(0)
            }
            Some(node_type) => {
                let old_type = writer
                    .get_metadata(local_pos, STATIC_GRAPH_LAYER_ID, NODE_TYPE_IDX)
                    .into_u64();
                match old_type {
                    None => {
                        let node_type_id = self.node_meta().get_or_create_node_type_id(node_type);
                        writer.update_c_props(
                            local_pos,
                            STATIC_GRAPH_LAYER_ID,
                            node_info_as_props(
                                id.as_gid_ref(),
                                Some(node_type_id.inner()).filter(|&id| id != 0),
                            ),
                        );
                        node_type_id
                    }
                    Some(old_type) => MaybeNew::Existing(
                        self.node_meta()
                            .get_node_type_id(node_type)
                            .filter(|&new_id| new_id == old_type as usize)
                            .ok_or(MutationError::NodeTypeError)?,
                    ),
                }
            }
        };
        Ok(vid.map(|_| (vid, node_type_id)))
    }

    fn resolve_node_and_type(
        &self,
        id: NodeRef,
        node_type: Option<&str>,
    ) -> Result<(VID, usize), Self::Error> {
        let vid = self.resolve_node(id)?.inner();
        let node_type_id = match node_type {
            Some(node_type) => self
                .node_meta()
                .get_or_create_node_type_id(node_type)
                .inner(),
            None => 0,
        };
        Ok((vid, node_type_id))
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
        src: NodeRef,
        dst: NodeRef,
        e_id: Option<EID>,
        _layer_id: usize,
    ) -> Result<Self::AtomicAddEdge<'_>, Self::Error> {
        let mut new_writer = None;
        let nodes = self.storage().nodes();

        let (src_id, dst_id) = match (src, dst) {
            (NodeRef::External(src), NodeRef::External(dst)) => {
                let src_id = self.logical_to_physical.get_or_init(src, || {
                    let (pos, mut writer) = nodes.reserve_and_lock_segment(
                        self.event_counter.fetch_add(1, Ordering::Relaxed),
                        2,
                    );
                    let vid = pos.as_vid(
                        writer.page.segment_id(),
                        self.extension().config().max_node_page_len(),
                    );
                    new_writer = Some((pos, writer));
                    vid
                })?;
                let dst_id = if let Some((src_pos, writer)) = new_writer.as_mut() {
                    writer.update_c_props(
                        *src_pos,
                        STATIC_GRAPH_LAYER_ID,
                        [(NODE_ID_IDX, src.into())],
                    );
                    let dst_id = self.logical_to_physical.get_or_init(dst, || {
                        let dst_pos = LocalPOS(src_pos.0 + 1); // we reserved space for both nodes above
                        let vid = dst_pos
                            .as_vid(writer.page.segment_id(), writer.mut_segment.max_page_len());
                        vid
                    })?;

                    if dst_id.is_new() {
                        writer.update_c_props(
                            LocalPOS(src_pos.0 + 1),
                            STATIC_GRAPH_LAYER_ID,
                            [(NODE_ID_IDX, dst.into())],
                        );
                    } else {
                        // need to un-reserve the space for the destination node
                        writer.page.nodes_counter().fetch_sub(1, Ordering::Relaxed);
                    }
                    dst_id
                } else {
                    let dst_id = self.logical_to_physical.get_or_init(dst, || {
                        let (pos, mut writer) = nodes.reserve_and_lock_segment(
                            self.event_counter.fetch_add(1, Ordering::Relaxed),
                            1,
                        );
                        let vid =
                            pos.as_vid(writer.page.segment_id(), writer.mut_segment.max_page_len());
                        new_writer = Some((pos, writer));
                        vid
                    })?;
                    if let Some((pos, writer)) = new_writer.as_mut() {
                        writer.update_c_props(
                            *pos,
                            STATIC_GRAPH_LAYER_ID,
                            [(NODE_ID_IDX, dst.into())],
                        );
                    }
                    dst_id
                };
                (src_id, dst_id)
            }
            (NodeRef::Internal(src_id), NodeRef::External(dst)) => {
                let dst_id = self.logical_to_physical.get_or_init(dst, || {
                    let (pos, mut writer) = nodes.reserve_and_lock_segment(
                        self.event_counter.fetch_add(1, Ordering::Relaxed),
                        1,
                    );
                    let vid =
                        pos.as_vid(writer.page.segment_id(), writer.mut_segment.max_page_len());
                    new_writer = Some((pos, writer));
                    vid
                })?;
                (MaybeNew::Existing(src_id), dst_id)
            }
            (NodeRef::External(src), NodeRef::Internal(dst_id)) => {
                let src_id = self.logical_to_physical.get_or_init(src, || {
                    let (pos, mut writer) = nodes.reserve_and_lock_segment(
                        self.event_counter.fetch_add(1, Ordering::Relaxed),
                        1,
                    );
                    let vid =
                        pos.as_vid(writer.page.segment_id(), writer.mut_segment.max_page_len());
                    new_writer = Some((pos, writer));
                    vid
                })?;
                (src_id, MaybeNew::Existing(dst_id))
            }
            (NodeRef::Internal(src_id), NodeRef::Internal(dst_id)) => {
                (MaybeNew::Existing(src_id), MaybeNew::Existing(dst_id))
            }
        };

        let (src_chunk, src_pos) = nodes.resolve_pos(src_id.inner());
        let (dst_chunk, dst_pos) = nodes.resolve_pos(dst_id.inner());

        let mut node_writers = match new_writer {
            None => {
                if src_chunk == dst_chunk {
                    let writer = nodes.writer(src_chunk);
                    NodeWriters {
                        src: writer,
                        dst: None,
                    }
                } else {
                    loop {
                        if let Some(src_writer) = nodes.try_writer(src_chunk) {
                            if let Some(dst_writer) = nodes.try_writer(dst_chunk) {
                                break NodeWriters {
                                    src: src_writer,
                                    dst: Some(dst_writer),
                                };
                            }
                        }
                    }
                }
            }
            Some((_, writer)) => {
                if src_chunk == dst_chunk {
                    NodeWriters {
                        src: writer,
                        dst: None,
                    }
                } else {
                    if src_id.is_new() {
                        let dst_writer = nodes.writer(dst_chunk);
                        NodeWriters {
                            src: writer,
                            dst: Some(dst_writer),
                        }
                    } else {
                        let src_writer = nodes.writer(src_chunk);
                        NodeWriters {
                            src: src_writer,
                            dst: Some(writer),
                        }
                    }
                }
            }
        };

        let existing_eid =
            node_writers
                .src
                .get_out_edge(src_pos, dst_id.inner(), STATIC_GRAPH_LAYER_ID);

        let (edge_id, edge_writer) = match e_id.or(existing_eid) {
            Some(edge_id) => (
                MaybeNew::Existing(edge_id),
                self.storage().edge_writer(edge_id),
            ),
            None => {
                let mut edge_writer = self.storage().get_free_writer();
                let edge_pos = None;
                let already_counted = false;
                let edge_pos = edge_writer.add_static_edge(
                    edge_pos,
                    src_id.inner(),
                    dst_id.inner(),
                    already_counted,
                );
                let edge_id =
                    edge_pos.as_eid(edge_writer.segment_id(), edge_writer.writer.max_page_len());

                node_writers.get_mut_src().add_static_outbound_edge(
                    src_pos,
                    dst_id.inner(),
                    edge_id,
                );
                node_writers.get_mut_dst().add_static_inbound_edge(
                    dst_pos,
                    src_id.inner(),
                    edge_id,
                );
                (MaybeNew::New(edge_id), edge_writer)
            }
        };

        Ok(WriteS {
            static_session: WriteSession::new(node_writers, edge_writer, self.storage()),
            src: src_id,
            dst: dst_id,
            eid: edge_id,
        })
    }

    fn internal_add_node(
        &self,
        t: EventTime,
        v: VID,
        props: Vec<(usize, Prop)>,
    ) -> Result<(), Self::Error> {
        let (segment, node_pos) = self.storage().nodes().resolve_pos(v);
        let mut node_writer = self.storage().node_writer(segment);
        node_writer.add_props(t, node_pos, STATIC_GRAPH_LAYER_ID, props);
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
                .map_err(MutationError::StorageError)?;
            Ok(prop_ids)
        } else {
            let prop_ids = PropsMetaWriter::temporal(meta, props)
                .and_then(|pmw| pmw.into_props_temporal())
                .map_err(MutationError::StorageError)?;
            Ok(prop_ids)
        }
    }

    fn validate_props_with_status<PN: AsRef<str>>(
        &self,
        is_static: bool,
        meta: &Meta,
        props: impl Iterator<Item = (PN, Prop)>,
    ) -> Result<Vec<MaybeNew<(PN, usize, Prop)>>, Self::Error> {
        if is_static {
            let prop_ids = PropsMetaWriter::constant(meta, props)
                .and_then(|pmw| pmw.into_props_const_with_status())
                .map_err(MutationError::StorageError)?;
            Ok(prop_ids)
        } else {
            let prop_ids = PropsMetaWriter::temporal(meta, props)
                .and_then(|pmw| pmw.into_props_temporal_with_status())
                .map_err(MutationError::StorageError)?;
            Ok(prop_ids)
        }
    }
}

impl DurabilityOps for TemporalGraph {
    fn transaction_manager(&self) -> Result<&TransactionManager, MutationError> {
        Ok(&self.transaction_manager)
    }

    fn wal(&self) -> Result<&Wal, MutationError> {
        Ok(&self.extension().wal())
    }
}
