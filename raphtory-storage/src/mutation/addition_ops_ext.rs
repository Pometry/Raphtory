use crate::mutation::{
    addition_ops::{EdgeWriteLock, InternalAdditionOps, SessionAdditionOps},
    durability_ops::DurabilityOps,
    MutationError, NodeWriterT,
};
use db4_graph::{TemporalGraph, WriteLockedGraph};
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
use storage::{
    api::{edges::EdgeSegmentOps, graph_props::GraphPropSegmentOps, nodes::NodeSegmentOps},
    pages::{node_page::writer::node_info_as_props, session::WriteSession},
    persist::{config::ConfigOps, strategy::PersistenceStrategy},
    properties::props_meta_writer::PropsMetaWriter,
    resolver::GIDResolverOps,
    transaction::TransactionManager,
    wal::LSN,
    Extension, Wal, ES, GS, NS,
};

pub struct WriteS<'a, EXT>
where
    EXT: PersistenceStrategy<NS = NS<EXT>, ES = ES<EXT>, GS = GS<EXT>>,
    NS<EXT>: NodeSegmentOps<Extension = EXT>,
    ES<EXT>: EdgeSegmentOps<Extension = EXT>,
    GS<EXT>: GraphPropSegmentOps<Extension = EXT>,
{
    static_session: WriteSession<'a, NS<EXT>, ES<EXT>, GS<EXT>, EXT>,
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
    fn internal_add_static_edge(
        &mut self,
        src: impl Into<VID>,
        dst: impl Into<VID>,
    ) -> MaybeNew<EID> {
        self.static_session.add_static_edge(src, dst)
    }

    fn internal_add_edge(
        &mut self,
        t: EventTime,
        src: impl Into<VID>,
        dst: impl Into<VID>,
        eid: MaybeNew<ELID>,
        props: impl IntoIterator<Item = (usize, Prop)>,
    ) -> MaybeNew<ELID> {
        self.static_session
            .add_edge_into_layer(t, src, dst, eid, props);

        eid
    }

    fn internal_delete_edge(
        &mut self,
        t: EventTime,
        src: impl Into<VID>,
        dst: impl Into<VID>,
        layer: usize,
    ) -> MaybeNew<ELID> {
        let src = src.into();
        let dst = dst.into();
        let eid = self
            .static_session
            .add_static_edge(src, dst)
            .map(|eid| eid.with_layer_deletion(layer));

        self.static_session.delete_edge_from_layer(t, src, dst, eid);

        eid
    }

    fn store_src_node_info(&mut self, vid: impl Into<VID>, node_id: Option<GidRef>) {
        if let Some(id) = node_id {
            let pos = self.static_session.resolve_node_pos(vid);

            self.static_session
                .node_writers()
                .get_mut_src()
                .update_c_props(pos, STATIC_GRAPH_LAYER_ID, [(NODE_ID_IDX, id.into())]);
        };
    }

    fn store_dst_node_info(&mut self, vid: impl Into<VID>, node_id: Option<GidRef>) {
        if let Some(id) = node_id {
            let pos = self.static_session.resolve_node_pos(vid);

            self.static_session
                .node_writers()
                .get_mut_dst()
                .update_c_props(pos, STATIC_GRAPH_LAYER_ID, [(NODE_ID_IDX, id.into())]);
        };
    }

    fn set_lsn(&mut self, lsn: LSN) {
        self.static_session.set_lsn(lsn);
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
                    node_info_as_props(id.as_gid_ref().left(), None),
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
                                id.as_gid_ref().left(),
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
        t: EventTime,
        v: VID,
        props: Vec<(usize, Prop)>,
    ) -> Result<NodeWriterT<'_>, Self::Error> {
        let (segment, node_pos) = self.storage().nodes().resolve_pos(v);
        let mut node_writer = self.storage().node_writer(segment);
        node_writer.add_props(t, node_pos, STATIC_GRAPH_LAYER_ID, props);
        Ok(node_writer)
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
