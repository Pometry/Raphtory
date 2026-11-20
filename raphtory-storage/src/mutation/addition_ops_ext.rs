use crate::{
    durability_ops::DurabilityOps,
    graph::graph::Immutable,
    mutation::{
        addition_ops::{
            GraphMutationOps, InternalAdditionOps, MetadataMutationOps, PropertyMutationOps,
        },
        EdgeWriterT, GraphPropWriterT, MutationError, NodeTypeIndexWriterT, NodeWriterT,
        NodeWritersT,
    },
    recovery_ops::RecoveryOps,
};
use db4_graph::{TemporalGraph, WriteLockedGraph};
use raphtory_api::{
    core::{
        entities::{
            edges::edge_ref::EdgeRef,
            properties::{
                meta::{Meta, DEFAULT_NODE_TYPE_ID, NODE_TYPE_PROP_ID, STATIC_GRAPH_LAYER_ID},
                prop::{AsPropEntry, Prop, PropType, PropUnwrap},
            },
            GidType, LayerId,
        },
        storage::dict_mapper::MaybeNew,
    },
    ArcRwLockWriteGuard,
};
use raphtory_core::{
    entities::{
        nodes::node_ref::{AsNodeRef, NodeRef},
        GidRef, EID, ELID, GID, VID,
    },
    storage::timeindex::{AsTime, EventTime},
};
use storage::{
    api::nodes::NodeSegmentOps,
    error::StorageError,
    pages::resolve_pos,
    persist::{config::ConfigOps, strategy::PersistenceStrategy},
    properties::props_meta_writer::PropsMetaWriter,
    resolver::{GIDResolverOps, Initialiser, MaybeInit},
    segments::{graph_prop::segment::MemGraphPropSegment, node::segment::MemNodeSegment},
    transaction::TransactionGuard,
    wal::LSN,
    ControlFile, Extension, LocalPOS, Wal,
};

pub struct AtomicAddEdge<'a> {
    src: MaybeNew<VID>,
    dst: MaybeNew<VID>,
    eid: MaybeNew<EID>,
    edge_writer: EdgeWriterT<'a>,
    node_writers: NodeWritersT<'a>,
}

impl<'a> PropertyMutationOps for AtomicAddEdge<'a> {
    fn update_properties(
        &mut self,
        t: EventTime,
        layer: LayerId,
        props: impl IntoIterator<Item = (usize, Prop)>,
    ) {
        self.add_edge_into_layer(
            t,
            self.src.inner(),
            self.dst.inner(),
            self.eid.map(|eid| eid.with_layer(layer)),
            props,
        );
    }
}

impl<'a> MetadataMutationOps for AtomicAddEdge<'a> {
    fn check_metadata<P: AsPropEntry>(
        &self,
        layer: LayerId,
        props: impl Iterator<Item = P>,
    ) -> Result<(), MutationError> {
        let pos = self
            .edge_writer
            .resolve_pos(self.eid.inner())
            .expect("eid should match writer for AtomicAddEdge");
        Ok(self.check_metadata(pos, layer, props)?)
    }

    fn update_metadata(&mut self, layer: LayerId, props: impl IntoIterator<Item = (usize, Prop)>) {
        let pos = self
            .edge_writer
            .resolve_pos(self.eid.inner())
            .expect("eid should match writer for AtomicAddEdge");
        self.update_metadata_at(pos, self.src.inner(), self.dst.inner(), layer, props)
    }
}

impl<'a> AtomicAddEdge<'a> {
    pub fn internal_delete_edge(&mut self, t: EventTime, layer: LayerId) {
        self.delete_edge_from_layer(
            t,
            self.src.inner(),
            self.dst.inner(),
            self.eid.map(|eid| eid.with_layer_deletion(layer)),
        );
    }

    pub fn src(&self) -> MaybeNew<VID> {
        self.src
    }

    pub fn dst(&self) -> MaybeNew<VID> {
        self.dst
    }

    pub fn eid(&self) -> MaybeNew<EID> {
        self.eid
    }

    pub fn set_lsn(&mut self, lsn: LSN) {
        self.node_writers.set_lsn(lsn);
        self.edge_writer.set_lsn(lsn);
    }

    fn add_edge_into_layer<T: AsTime>(
        &mut self,
        t: T,
        src: impl Into<VID>,
        dst: impl Into<VID>,
        edge: MaybeNew<ELID>,
        props: impl IntoIterator<Item = (usize, Prop)>,
    ) {
        let src = src.into();
        let dst = dst.into();
        let e_id = edge.inner();
        let layer = e_id.layer();

        let (_, src_pos) = resolve_pos(src, self.node_writers.get_mut_src().head.max_page_len());
        let (_, dst_pos) = resolve_pos(dst, self.node_writers.get_mut_dst().head.max_page_len());

        let edge_max_page_len = self
            .edge_writer
            .head
            .get_or_create_layer(layer)
            .max_page_len();
        let (_, edge_pos) = resolve_pos(e_id.eid(), edge_max_page_len);

        self.edge_writer
            .add_edge(t, edge_pos, src, dst, props, layer);

        let edge_id = edge.inner();

        if edge.is_new()
            || self
                .node_writers
                .get_mut_src()
                .get_out_edge(src_pos, dst, edge_id.layer())
                .is_none()
        {
            self.node_writers
                .get_mut_src()
                .add_outbound_edge(Some(t), src_pos, dst, edge_id);
            self.node_writers
                .get_mut_dst()
                .add_inbound_edge(Some(t), dst_pos, src, edge_id);
        }

        self.node_writers
            .get_mut_src()
            .update_timestamp(t, src_pos, e_id);
        self.node_writers
            .get_mut_dst()
            .update_timestamp(t, dst_pos, e_id);
    }

    fn delete_edge_from_layer<T: AsTime>(
        &mut self,
        t: T,
        src: impl Into<VID>,
        dst: impl Into<VID>,
        edge: MaybeNew<ELID>,
    ) {
        let src = src.into();
        let dst = dst.into();
        let e_id = edge.inner();
        let layer = e_id.layer();

        let (_, src_pos) = resolve_pos(src, self.node_writers.get_mut_src().head.max_page_len());
        let (_, dst_pos) = resolve_pos(dst, self.node_writers.get_mut_dst().head.max_page_len());

        let edge_max_page_len = self
            .edge_writer
            .head
            .get_or_create_layer(layer)
            .max_page_len();
        let (_, edge_pos) = resolve_pos(e_id.eid(), edge_max_page_len);

        self.edge_writer.delete_edge(t, edge_pos, src, dst, layer);

        let edge_id = edge.inner();

        if edge_id.layer() > STATIC_GRAPH_LAYER_ID {
            if edge.is_new()
                || self
                    .node_writers
                    .get_mut_src()
                    .get_out_edge(src_pos, dst, edge_id.layer())
                    .is_none()
            {
                self.node_writers
                    .get_mut_src()
                    .add_outbound_edge(Some(t), src_pos, dst, edge_id);

                self.node_writers
                    .get_mut_dst()
                    .add_inbound_edge(Some(t), dst_pos, src, edge_id);
            }

            self.node_writers
                .get_mut_src()
                .update_deletion_time(t, src_pos, e_id);

            self.node_writers
                .get_mut_dst()
                .update_deletion_time(t, dst_pos, e_id);
        }
    }

    fn check_metadata<P: AsPropEntry>(
        &self,
        pos: LocalPOS,
        layer: LayerId,
        props: impl Iterator<Item = P>,
    ) -> Result<(), StorageError> {
        self.edge_writer.check_metadata(pos, layer, props)
    }

    fn update_metadata_at(
        &mut self,
        pos: LocalPOS,
        src: VID,
        dst: VID,
        layer: LayerId,
        props: impl IntoIterator<Item = (usize, Prop)>,
    ) {
        self.edge_writer.update_c_props(pos, src, dst, layer, props)
    }
}

pub struct AtomicUpdateEdge<'a> {
    pos: LocalPOS,
    src: VID,
    dst: VID,
    writer: EdgeWriterT<'a>,
}

impl<'a> MetadataMutationOps for AtomicUpdateEdge<'a> {
    fn check_metadata<P: AsPropEntry>(
        &self,
        layer: LayerId,
        props: impl Iterator<Item = P>,
    ) -> Result<(), MutationError> {
        Ok(self.writer.check_metadata(self.pos, layer, props)?)
    }

    fn update_metadata(&mut self, layer: LayerId, props: impl IntoIterator<Item = (usize, Prop)>) {
        self.writer
            .update_c_props(self.pos, self.src, self.dst, layer, props)
    }
}

impl<'a> AtomicUpdateEdge<'a> {
    pub fn set_lsn(&mut self, lsn: LSN) {
        self.writer.set_lsn(lsn)
    }
}

#[derive(Debug)]
pub struct AtomicAddNode<'a> {
    vid: MaybeNew<VID>,
    node_writer: NodeWriterT<'a>,
    type_index_writer: NodeTypeIndexWriterT<'a>,
}

impl<'a> PropertyMutationOps for AtomicAddNode<'a> {
    fn update_properties(
        &mut self,
        t: EventTime,
        layer: LayerId,
        props: impl IntoIterator<Item = (usize, Prop)>,
    ) {
        let pos = self.local_pos();
        self.node_writer.add_props(t, pos, layer, props)
    }
}

impl<'a> MetadataMutationOps for AtomicAddNode<'a> {
    fn check_metadata<P: AsPropEntry>(
        &self,
        layer: LayerId,
        props: impl Iterator<Item = P>,
    ) -> Result<(), MutationError> {
        let pos = self.local_pos();
        Ok(self.node_writer.check_metadata(pos, layer, props)?)
    }

    fn update_metadata(&mut self, layer: LayerId, props: impl IntoIterator<Item = (usize, Prop)>) {
        let pos = self.local_pos();
        self.node_writer.update_c_props(pos, layer, props)
    }
}

impl<'a> AtomicAddNode<'a> {
    pub fn can_set_type(&self) -> bool {
        self.vid.is_new() || self.get_type() == DEFAULT_NODE_TYPE_ID
    }

    pub fn get_type(&self) -> usize {
        self.node_writer
            .get_metadata(self.local_pos(), STATIC_GRAPH_LAYER_ID, NODE_TYPE_PROP_ID)
            .into_u64()
            .map(|u| u as usize)
            .unwrap_or(DEFAULT_NODE_TYPE_ID)
    }

    pub fn store_node_id(&mut self, gid: GID) {
        self.node_writer.store_node_id(self.local_pos(), gid);
    }

    pub fn set_type(&mut self, node_type: usize) {
        let pos = self.local_pos();
        self.node_writer.store_node_type(pos, node_type);

        if node_type != DEFAULT_NODE_TYPE_ID {
            self.type_index_writer.insert(node_type, self.vid.inner());
        }
    }

    pub fn node(&self) -> MaybeNew<VID> {
        self.vid
    }

    pub fn set_lsn(&mut self, lsn: LSN) {
        self.node_writer.head.set_lsn(lsn)
    }

    fn local_pos(&self) -> LocalPOS {
        let (_, pos) = resolve_pos(self.vid.inner(), self.node_writer.head.max_page_len());
        pos
    }
}

impl<'a> PropertyMutationOps for GraphPropWriterT<'a> {
    fn update_properties(
        &mut self,
        t: EventTime,
        layer: LayerId,
        props: impl IntoIterator<Item = (usize, Prop)>,
    ) {
        assert_eq!(layer, MemGraphPropSegment::DEFAULT_LAYER); // TODO: graph properties should support layers
        self.add_properties(t, props)
    }
}

impl<'a> MetadataMutationOps for GraphPropWriterT<'a> {
    fn check_metadata<P: AsPropEntry>(
        &self,
        layer: LayerId,
        props: impl Iterator<Item = P>,
    ) -> Result<(), MutationError> {
        assert_eq!(layer, MemGraphPropSegment::DEFAULT_LAYER); // TODO: graph properties should support layers
        Ok(self.check_metadata_inner(props)?)
    }

    fn update_metadata(&mut self, layer: LayerId, props: impl IntoIterator<Item = (usize, Prop)>) {
        assert_eq!(layer, MemGraphPropSegment::DEFAULT_LAYER); // TODO: graph properties should support layers
        self.update_metadata_inner(props)
    }
}

impl GraphMutationOps for TemporalGraph {
    fn resolve_layer(&self, layer: Option<&str>) -> MaybeNew<LayerId> {
        let id = self.edge_meta().get_or_create_layer_id(layer);
        // TODO: we replicate the layer id in the node meta as well, perhaps layer meta should be common
        if id.is_new() {
            self.node_meta().layer_meta().set_id(
                self.edge_meta().layer_meta().get_name(id.inner().0),
                id.inner().0,
            );
        }
        id
    }

    fn atomic_add_edge(
        &self,
        src: NodeRef,
        dst: NodeRef,
        e_id: Option<EID>,
    ) -> Result<AtomicAddEdge<'_>, MutationError> {
        let nodes = self.storage().nodes();

        let (src_init, dst_init) = match (src, dst) {
            (NodeRef::Internal(src_id), NodeRef::Internal(dst_id)) => {
                (MaybeInit::VID(src_id), Some(MaybeInit::VID(dst_id)))
            }
            (NodeRef::Internal(src_id), NodeRef::External(dst_gid)) => (
                MaybeInit::VID(src_id),
                Some(self.gid_resolver.get_or_init(dst_gid)?),
            ),
            (NodeRef::External(src_gid), NodeRef::Internal(dst_id)) => (
                self.gid_resolver.get_or_init(src_gid)?,
                Some(MaybeInit::VID(dst_id)),
            ),
            (NodeRef::External(src_gid), NodeRef::External(dst_gid)) => {
                // make sure both ids are of the correct type before modifying anything
                self.gid_resolver.validate_gids([src_gid, dst_gid])?;
                // resolve the smaller id first to avoid deadlocks when adding the same edge
                // in both directions
                match src_gid.cmp(&dst_gid) {
                    std::cmp::Ordering::Less => (
                        self.gid_resolver.get_or_init(src_gid)?,
                        Some(self.gid_resolver.get_or_init(dst_gid)?),
                    ),
                    std::cmp::Ordering::Equal => (self.gid_resolver.get_or_init(src_gid)?, None),
                    std::cmp::Ordering::Greater => {
                        let dst_init = self.gid_resolver.get_or_init(dst_gid)?;
                        (self.gid_resolver.get_or_init(src_gid)?, Some(dst_init))
                    }
                }
            }
        };

        let dst_init = dst_init.filter(|dst_init| dst_init != &src_init);

        let (mut node_writers, src_id, dst_id) = match (src_init, dst_init) {
            (src_init, None) => {
                // self-loop
                match src_init {
                    MaybeInit::VID(vid) => {
                        let (src_chunk, _) = nodes.resolve_pos(vid);
                        let writer = nodes.writer(src_chunk);

                        (
                            NodeWritersT {
                                src: writer,
                                dst: None,
                            },
                            MaybeNew::Existing(vid),
                            MaybeNew::Existing(vid),
                        )
                    }
                    MaybeInit::Init(init) => {
                        let (pos, writer) = nodes.reserve_and_lock_segment(1, None);
                        let vid =
                            pos.as_vid(writer.segment.segment_id(), writer.head.max_page_len());

                        init.init(vid)?;

                        (
                            NodeWritersT {
                                src: writer,
                                dst: None,
                            },
                            MaybeNew::New(vid),
                            MaybeNew::New(vid),
                        )
                    }
                }
            }
            (MaybeInit::Init(src_init), Some(MaybeInit::Init(dst_init))) => {
                // both new, put them in the same segment
                let (pos, writer) = nodes.reserve_and_lock_segment(2, None);
                let src_id = pos.as_vid(writer.segment.segment_id(), writer.head.max_page_len());
                let dst_id = LocalPOS(pos.0 + 1)
                    .as_vid(writer.segment.segment_id(), writer.head.max_page_len());

                src_init.init(src_id)?;
                dst_init.init(dst_id)?;

                (
                    NodeWritersT {
                        src: writer,
                        dst: None,
                    },
                    MaybeNew::New(src_id),
                    MaybeNew::New(dst_id),
                )
            }
            (MaybeInit::Init(src_init), Some(MaybeInit::VID(dst_id))) => {
                let (dst_chunk, _) = nodes.resolve_pos(dst_id);
                let dst_writer = nodes.writer(dst_chunk);
                match nodes.reserve_segment_row(dst_writer.segment) {
                    None => {
                        // existing segment is full, need to get a new one, pass in the existing
                        // writer reference so we don't end up trying to block on it when
                        // potentially evicting it from free segments
                        let (src_pos, src_writer) =
                            nodes.reserve_and_lock_segment(1, Some(&dst_writer));
                        let src_id = src_pos.as_vid(
                            src_writer.segment.segment_id(),
                            src_writer.head.max_page_len(),
                        );

                        src_init.init(src_id)?;

                        (
                            NodeWritersT {
                                src: src_writer,
                                dst: Some(dst_writer),
                            },
                            MaybeNew::New(src_id),
                            MaybeNew::Existing(dst_id),
                        )
                    }
                    Some(src_pos) => {
                        let src_id = LocalPOS(src_pos).as_vid(
                            dst_writer.segment.segment_id(),
                            dst_writer.head.max_page_len(),
                        );

                        src_init.init(src_id)?;

                        (
                            NodeWritersT {
                                src: dst_writer,
                                dst: None,
                            },
                            MaybeNew::New(src_id),
                            MaybeNew::Existing(dst_id),
                        )
                    }
                }
            }
            (MaybeInit::VID(src_id), Some(MaybeInit::Init(dst_init))) => {
                let (src_chunk, _) = nodes.resolve_pos(src_id);
                let src_writer = nodes.writer(src_chunk);
                match nodes.reserve_segment_row(src_writer.segment) {
                    None => {
                        let (dst_pos, dst_writer) =
                            nodes.reserve_and_lock_segment(1, Some(&src_writer));
                        let dst_id = dst_pos.as_vid(
                            dst_writer.segment.segment_id(),
                            dst_writer.head.max_page_len(),
                        );

                        dst_init.init(dst_id)?;

                        (
                            NodeWritersT {
                                src: src_writer,
                                dst: Some(dst_writer),
                            },
                            MaybeNew::Existing(src_id),
                            MaybeNew::New(dst_id),
                        )
                    }
                    Some(dst_pos) => {
                        let dst_id = LocalPOS(dst_pos).as_vid(
                            src_writer.segment.segment_id(),
                            src_writer.head.max_page_len(),
                        );
                        dst_init.init(dst_id)?;
                        (
                            NodeWritersT {
                                src: src_writer,
                                dst: None,
                            },
                            MaybeNew::Existing(src_id),
                            MaybeNew::New(dst_id),
                        )
                    }
                }
            }
            (MaybeInit::VID(src_id), Some(MaybeInit::VID(dst_id))) => {
                let (src_chunk, _) = nodes.resolve_pos(src_id);
                let (dst_chunk, _) = nodes.resolve_pos(dst_id);
                if src_chunk == dst_chunk {
                    let writer = nodes.writer(src_chunk);
                    (
                        NodeWritersT {
                            src: writer,
                            dst: None,
                        },
                        MaybeNew::Existing(src_id),
                        MaybeNew::Existing(dst_id),
                    )
                } else {
                    let node_writers = if src_chunk < dst_chunk {
                        loop {
                            if let Some(src_writer) = nodes.try_writer(src_chunk) {
                                if let Some(dst_writer) = nodes.try_writer(dst_chunk) {
                                    break NodeWritersT {
                                        src: src_writer,
                                        dst: Some(dst_writer),
                                    };
                                }
                            }
                        }
                    } else {
                        loop {
                            if let Some(dst_writer) = nodes.try_writer(dst_chunk) {
                                if let Some(src_writer) = nodes.try_writer(src_chunk) {
                                    break NodeWritersT {
                                        src: src_writer,
                                        dst: Some(dst_writer),
                                    };
                                }
                            }
                        }
                    };
                    (
                        node_writers,
                        MaybeNew::Existing(src_id),
                        MaybeNew::Existing(dst_id),
                    )
                }
            }
        };

        let (_, src_pos) = nodes.resolve_pos(src_id.inner());
        let (_, dst_pos) = nodes.resolve_pos(dst_id.inner());

        if src_id.is_new() {
            if let Some(gid) = src.as_gid_ref() {
                node_writers
                    .get_mut_src()
                    .store_node_id(src_pos, gid.to_owned());
            }
        }

        if dst_id.is_new() {
            if let Some(gid) = dst.as_gid_ref() {
                node_writers
                    .get_mut_dst()
                    .store_node_id(dst_pos, gid.to_owned());
            }
        }

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
                    edge_pos.as_eid(edge_writer.segment_id(), edge_writer.head.max_page_len());

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

        Ok(AtomicAddEdge {
            src: src_id,
            dst: dst_id,
            eid: edge_id,
            edge_writer,
            node_writers,
        })
    }

    fn atomic_update_edge(&self, edge: EdgeRef) -> AtomicUpdateEdge<'_> {
        let writer = self.storage().edge_writer(edge.pid());
        let pos = writer
            .resolve_pos(edge.pid())
            .expect("resolve should not fail for writer matching eid");

        AtomicUpdateEdge {
            pos,
            src: edge.src(),
            dst: edge.dst(),
            writer,
        }
    }

    fn atomic_add_node(
        &self,
        node: NodeRef,
        node_type: Option<&str>,
    ) -> Result<AtomicAddNode<'_>, MutationError> {
        let type_index = self.storage().node_type_index();
        // This may flush the index which waits for the head exclusive lock.
        let type_index_writer = NodeTypeIndexWriterT::new_shared(type_index.as_ref());

        let node_vid = match node {
            NodeRef::Internal(vid) => vid,
            NodeRef::External(gid) => match self.gid_resolver.get_or_init(gid)? {
                MaybeInit::VID(vid) => vid,
                MaybeInit::Init(init) => {
                    // This is a new node, reserve a pos and fill in node data.
                    let (pos, node_writer) =
                        self.storage().nodes().reserve_and_lock_segment(1, None);
                    let vid = pos.as_vid(
                        node_writer.segment.segment_id(),
                        node_writer.head.max_page_len(),
                    );

                    init.init(vid)?;

                    let mut atomic = AtomicAddNode {
                        vid: MaybeNew::New(vid),
                        node_writer,
                        type_index_writer,
                    };

                    atomic.store_node_id(gid.to_owned());

                    if let Some(node_type) = node_type {
                        let node_type_id = self
                            .node_meta()
                            .get_or_create_node_type_id(node_type)
                            .inner();

                        atomic.set_type(node_type_id);
                    }

                    return Ok(atomic);
                }
            },
        };

        let (segment_id, _) = self.storage().nodes().resolve_pos(node_vid);
        let node_writer = self.storage().node_writer(segment_id);

        let mut atomic = AtomicAddNode {
            vid: MaybeNew::Existing(node_vid),
            node_writer,
            type_index_writer,
        };

        // The node already exists, set or validate the given node type.
        if let Some(node_type) = node_type {
            if atomic.can_set_type() {
                let node_type_id = self
                    .node_meta()
                    .get_or_create_node_type_id(node_type)
                    .inner();

                atomic.set_type(node_type_id);
            } else {
                self.node_meta()
                    .get_node_type_id(node_type)
                    .filter(|&node_type_id| atomic.get_type() == node_type_id)
                    .ok_or(MutationError::NodeTypeError)?;
            }
        }

        Ok(atomic)
    }

    unsafe fn bulk_load_resolve_node(
        &self,
        id: GidRef<'_>,
    ) -> Result<(MaybeNew<VID>, Option<ArcRwLockWriteGuard<MemNodeSegment>>), MutationError> {
        let vid = match self.gid_resolver.get(id) {
            Some(vid) => (MaybeNew::Existing(vid), None),
            None => {
                let (seg, pos, new_locks) = self.storage().nodes().reserve_free_pos();
                let new_vid = pos.as_vid(seg, self.extension().config().max_node_page_len());
                self.gid_resolver.set(id, new_vid)?;
                (MaybeNew::New(new_vid), new_locks)
            }
        };
        Ok(vid)
    }

    fn validate_props<'a, PN: AsRef<str> + 'a, P: Into<Prop>>(
        &self,
        is_static: bool,
        meta: &'a Meta,
        props: impl IntoIterator<Item = (PN, P)>,
    ) -> Result<PropsMetaWriter<'a, PN>, MutationError> {
        let props = props.into_iter().map(|(name, prop)| (name, prop.into()));
        if is_static {
            Ok(PropsMetaWriter::constant(meta, props)?)
        } else {
            Ok(PropsMetaWriter::temporal(meta, props)?)
        }
    }

    fn read_event_id(&self) -> usize {
        self.storage().read_event_id()
    }

    fn set_event_id(&self, event_id: usize) {
        self.storage().set_event_id(event_id);
    }

    fn next_event_id(&self) -> usize {
        self.storage().next_event_id()
    }

    fn reserve_event_ids(&self, num_ids: usize) -> usize {
        self.storage().reserve_event_ids(num_ids)
    }

    fn set_max_event_id(&self, value: usize) -> usize {
        self.storage().set_max_event_id(value)
    }

    fn set_node(&self, gid: GidRef, vid: VID) -> Result<(), MutationError> {
        Ok(self.gid_resolver.set(gid, vid)?)
    }

    fn resolve_graph_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, MutationError> {
        Ok(self
            .graph_props_meta()
            .resolve_prop_id(prop, dtype, is_static)?)
    }

    fn resolve_node_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, MutationError> {
        Ok(self.node_meta().resolve_prop_id(prop, dtype, is_static)?)
    }

    fn resolve_edge_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, MutationError> {
        Ok(self.edge_meta().resolve_prop_id(prop, dtype, is_static)?)
    }

    fn atomic_update_graph_props(&self) -> GraphPropWriterT<'_> {
        self.storage().graph_props().writer()
    }

    fn id_type(&self) -> Option<GidType> {
        self.gid_resolver.dtype()
    }
}

impl InternalAdditionOps for TemporalGraph {
    fn write_lock(&self) -> Result<WriteLockedGraph<'_, Extension>, MutationError> {
        let locked_g = self.write_locked_graph();
        Ok(locked_g)
    }

    fn mutable(&self) -> Result<&TemporalGraph, Immutable> {
        Ok(self)
    }
}

impl DurabilityOps for TemporalGraph {
    fn transaction_manager(&self) -> Result<TransactionGuard<'_>, MutationError> {
        Ok(TransactionGuard::new(
            &self.transaction_manager,
            self.storage().bulk_load_shared(),
        ))
    }

    fn wal(&self) -> Result<&Wal, MutationError> {
        Ok(self.extension().wal())
    }

    fn control_file(&self) -> Result<&ControlFile, MutationError> {
        Ok(self.extension().control_file())
    }
}

impl RecoveryOps for TemporalGraph {}
