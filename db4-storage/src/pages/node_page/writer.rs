use crate::{
    LocalPOS,
    api::nodes::NodeSegmentOps,
    error::StorageError,
    pages::{layer_counter::GraphStats, resolve_pos},
    segments::node::segment::MemNodeSegment,
    wal::LSN,
};
use parking_lot::RwLockWriteGuard;
use raphtory_api::core::{
    entities::{
        EID, GID, LayerId, VID,
        properties::{
            meta::{NODE_ID_IDX, NODE_TYPE_IDX, STATIC_GRAPH_LAYER_ID},
            prop::{AsPropRef, Prop},
        },
    },
    storage::timeindex::EventTime,
};
use raphtory_core::{
    entities::{ELID, GidRef},
    storage::timeindex::AsTime,
};
use std::ops::DerefMut;

#[derive(Debug)]
pub struct NodeWriter<'a, MP: DerefMut<Target = MemNodeSegment> + 'a, NS: NodeSegmentOps> {
    pub segment: &'a NS,
    pub writer: MP,
    pub graph_stats: &'a GraphStats,
    old_est_size: usize,
}

impl<'a, MP: DerefMut<Target = MemNodeSegment> + 'a, NS: NodeSegmentOps> NodeWriter<'a, MP, NS> {
    pub fn new(segment: &'a NS, graph_stats: &'a GraphStats, writer: MP) -> Self {
        let old_est_size = writer.est_size();

        Self {
            segment,
            writer,
            graph_stats,
            old_est_size,
        }
    }

    pub fn add_outbound_edge<T: AsTime>(
        &mut self,
        t: Option<T>,
        src_pos: impl Into<LocalPOS>,
        dst: impl Into<VID>,
        e_id: impl Into<ELID>,
    ) {
        self.add_outbound_edge_inner(t, src_pos, dst, e_id, |layer_id| {
            self.graph_stats.increment(layer_id);
        });
    }

    pub fn add_static_outbound_edge(
        &mut self,
        src_pos: LocalPOS,
        dst: impl Into<VID>,
        e_id: impl Into<EID>,
    ) {
        let e_id = e_id.into();
        self.add_outbound_edge_inner::<i64>(
            None,
            src_pos,
            dst,
            e_id.with_layer(STATIC_GRAPH_LAYER_ID),
            |layer_id| {
                self.graph_stats.increment(layer_id);
            },
        );
    }

    pub(crate) fn add_outbound_edge_inner<T: AsTime>(
        &mut self,
        t: Option<T>,
        src_pos: impl Into<LocalPOS>,
        dst: impl Into<VID>,
        e_id: impl Into<ELID>,
        mut layer_counter: impl FnMut(LayerId),
    ) {
        let src_pos = src_pos.into();
        let dst = dst.into();
        if let Some(t) = t {
            self.graph_stats.update_time(t.t());
        }

        let e_id = e_id.into();
        let layer_id = e_id.layer();
        let (is_new_node, add) = self.writer.add_outbound_edge(t, src_pos, dst, e_id);
        self.writer.increment_est_size(add);

        if is_new_node && !self.segment.has_node(src_pos, layer_id) {
            layer_counter(layer_id);
        }
    }

    pub fn add_inbound_edge<T: AsTime>(
        &mut self,
        t: Option<T>,
        dst_pos: impl Into<LocalPOS>,
        src: impl Into<VID>,
        e_id: impl Into<ELID>,
    ) {
        self.add_inbound_edge_inner(t, dst_pos, src, e_id, |layer| {
            self.graph_stats.increment(layer);
        });
    }

    pub fn add_static_inbound_edge(
        &mut self,
        dst_pos: LocalPOS,
        src: impl Into<VID>,
        e_id: impl Into<EID>,
    ) {
        let e_id = e_id.into();
        self.add_inbound_edge_inner::<i64>(
            None,
            dst_pos,
            src,
            e_id.with_layer(STATIC_GRAPH_LAYER_ID),
            |layer| {
                self.graph_stats.increment(layer);
            },
        );
    }

    pub(crate) fn add_inbound_edge_inner<T: AsTime>(
        &mut self,
        t: Option<T>,
        dst_pos: impl Into<LocalPOS>,
        src: impl Into<VID>,
        e_id: impl Into<ELID>,
        mut layer_counter: impl FnMut(LayerId),
    ) {
        let e_id = e_id.into();
        let src = src.into();

        if let Some(t) = t {
            self.graph_stats.update_time(t.t());
        }

        let layer = e_id.layer();
        let dst_pos = dst_pos.into();
        let (is_new_node, add) = self.writer.add_inbound_edge(t, dst_pos, src, e_id);

        self.writer.increment_est_size(add);

        if is_new_node && !self.segment.has_node(dst_pos, layer) {
            layer_counter(layer);
        }
    }

    pub fn add_props<T: AsTime, P: AsPropRef>(
        &mut self,
        t: T,
        pos: LocalPOS,
        layer_id: LayerId,
        props: impl IntoIterator<Item = (usize, P)>,
    ) {
        self.graph_stats.update_time(t.t());

        let (is_new_node, add) = self.writer.add_props(t, pos, layer_id, props);
        self.writer.increment_est_size(add);

        if is_new_node && !self.segment.has_node(pos, layer_id) {
            self.graph_stats.increment(layer_id);
        }
    }

    pub fn delete(&mut self, t: EventTime, pos: LocalPOS, layer_id: LayerId) {
        self.graph_stats.update_time(t.t());
        let (is_new_node, add) = self.writer.delete(t, pos, layer_id);
        self.writer.increment_est_size(add);

        if is_new_node && !self.segment.has_node(pos, layer_id) {
            self.graph_stats.increment(layer_id);
        }
    }

    pub fn get_metadata(&self, pos: LocalPOS, layer_id: LayerId, prop_id: usize) -> Option<Prop> {
        self.writer
            .get_metadata(pos, layer_id, prop_id)
            .or_else(|| self.segment.get_metadata_immut(pos, layer_id, prop_id))
    }

    pub fn check_metadata<P: AsPropRef>(
        &self,
        pos: LocalPOS,
        layer_id: LayerId,
        props: &[(usize, P)],
    ) -> Result<(), StorageError> {
        self.writer.check_metadata(pos, layer_id, props)?;
        self.segment.check_metadata_immut(pos, layer_id, props)
    }

    pub fn update_c_props<P: AsPropRef>(
        &mut self,
        pos: LocalPOS,
        layer_id: LayerId,
        props: impl IntoIterator<Item = (usize, P)>,
    ) {
        self.update_c_props_inner(pos, layer_id, props, |layer_id| {
            self.graph_stats.increment(layer_id);
        });
    }

    pub(crate) fn update_c_props_inner<P: AsPropRef>(
        &mut self,
        pos: LocalPOS,
        layer_id: LayerId,
        props: impl IntoIterator<Item = (usize, P)>,
        mut layer_counter: impl FnMut(LayerId),
    ) {
        let (is_new_node, add) = self.writer.update_metadata(pos, layer_id, props);
        self.writer.increment_est_size(add);
        if is_new_node && !self.segment.has_node(pos, layer_id) {
            layer_counter(layer_id);
        }
    }

    pub fn update_timestamp<T: AsTime>(&mut self, t: T, pos: LocalPOS, e_id: ELID) {
        self.graph_stats.update_time(t.t());
        let add = self.writer.update_timestamp(t, pos, e_id);
        self.writer.increment_est_size(add);
    }

    #[inline]
    pub fn get_out_edge(&self, pos: LocalPOS, dst: VID, layer_id: LayerId) -> Option<EID> {
        self.segment
            .get_out_edge(pos, dst, layer_id, self.writer.deref())
    }

    pub fn get_inb_edge(&self, pos: LocalPOS, src: VID, layer_id: LayerId) -> Option<EID> {
        self.segment
            .get_inb_edge(pos, src, layer_id, self.writer.deref())
    }

    pub fn store_node_id_and_node_type(
        &mut self,
        pos: LocalPOS,
        layer_id: LayerId,
        gid: GidRef<'_>,
        node_type: usize,
    ) {
        let node_type = (node_type != 0).then_some(node_type);
        self.update_c_props(pos, layer_id, node_info_as_props(Some(gid), node_type));
    }

    pub fn store_node_id(&mut self, pos: LocalPOS, layer_id: LayerId, gid: GID) {
        let gid = match gid {
            GID::U64(id) => Prop::U64(id),
            GID::Str(s) => Prop::str(s),
        };
        let props = [(NODE_ID_IDX, gid)];
        self.update_c_props(pos, layer_id, props);
    }

    pub fn store_node_type(&mut self, pos: LocalPOS, layer_id: LayerId, node_type: usize) {
        let props = [(NODE_TYPE_IDX, Prop::U64(node_type as u64))];
        self.update_c_props(pos, layer_id, props);
    }

    pub fn update_deletion_time<T: AsTime>(&mut self, t: T, node: LocalPOS, e_id: ELID) {
        self.update_timestamp(t, node, e_id);
    }

    pub fn increment_seg_num_nodes(&mut self) {
        self.segment.increment_num_nodes(self.writer.max_page_len());
    }

    pub fn has_node(&self, node: LocalPOS, layer_id: LayerId) -> bool {
        self.writer.has_node(node, layer_id) || self.segment.has_node(node, layer_id)
    }

    pub fn set_lsn(&mut self, lsn: LSN) {
        self.writer.set_lsn(lsn);
    }

    #[inline(always)]
    pub fn resolve_pos(&self, node_id: VID) -> Option<LocalPOS> {
        let (page, pos) = resolve_pos(node_id, self.writer.max_page_len());

        if page == self.writer.segment_id() {
            Some(pos)
        } else {
            None
        }
    }
}

impl<'a, NS: NodeSegmentOps> NodeWriter<'a, RwLockWriteGuard<'a, MemNodeSegment>, NS> {
    pub fn unlocked<R>(&mut self, op: impl FnOnce() -> R) -> R {
        RwLockWriteGuard::unlocked(&mut self.writer, op)
    }
}

pub fn node_info_as_props(
    gid: Option<GidRef>,
    node_type: Option<usize>,
) -> impl Iterator<Item = (usize, Prop)> {
    gid.into_iter().map(|g| (NODE_ID_IDX, g.into())).chain(
        node_type
            .into_iter()
            .map(|nt| (NODE_TYPE_IDX, Prop::U64(nt as u64))),
    )
}

impl<'a, MP: DerefMut<Target = MemNodeSegment> + 'a, NS: NodeSegmentOps> Drop
    for NodeWriter<'a, MP, NS>
{
    fn drop(&mut self) {
        self.writer
            .increment_global_est_size(self.writer.est_size() - self.old_est_size);

        self.segment
            .notify_write(self.writer.deref_mut())
            .expect("Failed to persist node page");
    }
}

/// Holds writers for src and dst node segments when adding an edge.
/// If both nodes are in the same segment, `dst` is `None` and `src` is used for both.
pub struct NodeWriters<'a, MP: DerefMut<Target = MemNodeSegment>, NS: NodeSegmentOps> {
    pub src: NodeWriter<'a, MP, NS>,
    pub dst: Option<NodeWriter<'a, MP, NS>>,
}

impl<'a, MP: DerefMut<Target = MemNodeSegment>, NS: NodeSegmentOps> NodeWriters<'a, MP, NS> {
    pub fn get_mut_src(&mut self) -> &mut NodeWriter<'a, MP, NS> {
        &mut self.src
    }

    pub fn get_mut_dst(&mut self) -> &mut NodeWriter<'a, MP, NS> {
        self.dst.as_mut().unwrap_or(&mut self.src)
    }

    pub fn set_lsn(&mut self, lsn: LSN) {
        self.src.set_lsn(lsn);

        if let Some(dst) = &mut self.dst {
            dst.set_lsn(lsn);
        }
    }
}
