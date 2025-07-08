use crate::{
    LocalPOS, api::nodes::NodeSegmentOps, pages::layer_counter::GraphStats,
    segments::node::MemNodeSegment,
};
use raphtory_api::core::entities::{EID, VID, properties::prop::Prop};
use raphtory_core::{
    entities::{ELID, GidRef},
    storage::timeindex::AsTime,
};
use std::ops::DerefMut;

#[derive(Debug)]
pub struct NodeWriter<'a, MP: DerefMut<Target = MemNodeSegment> + 'a, NS: NodeSegmentOps> {
    pub page: &'a NS,
    pub writer: MP, // TODO: rename to m_segment
    pub l_counter: &'a GraphStats,
}

impl<'a, MP: DerefMut<Target = MemNodeSegment> + 'a, NS: NodeSegmentOps> NodeWriter<'a, MP, NS> {
    pub fn new(page: &'a NS, global_num_nodes: &'a GraphStats, writer: MP) -> Self {
        Self {
            page,
            writer,
            l_counter: global_num_nodes,
        }
    }

    pub fn add_outbound_edge<T: AsTime>(
        &mut self,
        t: Option<T>,
        src_pos: impl Into<LocalPOS>,
        dst: impl Into<VID>,
        e_id: impl Into<ELID>,
        lsn: u64,
    ) {
        self.add_outbound_edge_inner(t, src_pos, dst, e_id, lsn);
    }

    pub fn add_static_outbound_edge(
        &mut self,
        src_pos: LocalPOS,
        dst: impl Into<VID>,
        e_id: impl Into<EID>,
        lsn: u64,
    ) {
        let e_id = e_id.into();
        self.add_outbound_edge_inner::<i64>(None, src_pos, dst, e_id.with_layer(0), lsn);
    }

    fn add_outbound_edge_inner<T: AsTime>(
        &mut self,
        t: Option<T>,
        src_pos: impl Into<LocalPOS>,
        dst: impl Into<VID>,
        e_id: impl Into<ELID>,
        lsn: u64,
    ) {
        let src_pos = src_pos.into();
        if let Some(t) = t {
            self.l_counter.update_time(t.t());
        }

        let e_id = e_id.into();
        let layer_id = e_id.layer();
        let is_new_node = self.writer.add_outbound_edge(t, src_pos, dst, e_id, lsn);

        if is_new_node && !self.page.check_node(src_pos, layer_id) {
            self.l_counter.increment(layer_id);
        }
    }

    pub fn add_inbound_edge<T: AsTime>(
        &mut self,
        t: Option<T>,
        dst_pos: impl Into<LocalPOS>,
        src: impl Into<VID>,
        e_id: impl Into<ELID>,
        lsn: u64,
    ) {
        self.add_inbound_edge_inner(t, dst_pos, src, e_id, lsn);
    }

    pub fn add_static_inbound_edge(
        &mut self,
        dst_pos: LocalPOS,
        src: impl Into<VID>,
        e_id: impl Into<EID>,
        lsn: u64,
    ) {
        let e_id = e_id.into();
        self.add_inbound_edge_inner::<i64>(None, dst_pos, src, e_id.with_layer(0), lsn);
    }

    fn add_inbound_edge_inner<T: AsTime>(
        &mut self,
        t: Option<T>,
        dst_pos: impl Into<LocalPOS>,
        src: impl Into<VID>,
        e_id: impl Into<ELID>,
        lsn: u64,
    ) {
        let e_id = e_id.into();
        if let Some(t) = t {
            self.l_counter.update_time(t.t());
        }
        let layer = e_id.layer();
        let dst_pos = dst_pos.into();
        let is_new_node = self.writer.add_inbound_edge(t, dst_pos, src, e_id, lsn);

        if is_new_node && !self.page.check_node(dst_pos, layer) {
            self.l_counter.increment(layer);
        }
    }

    pub fn add_props<T: AsTime>(
        &mut self,
        t: T,
        pos: LocalPOS,
        layer_id: usize,
        props: impl IntoIterator<Item = (usize, Prop)>,
        lsn: u64,
    ) {
        self.writer.as_mut()[layer_id].set_lsn(lsn);
        self.l_counter.update_time(t.t());
        let is_new_node = self.writer.add_props(t, pos, layer_id, props);
        if is_new_node && !self.page.check_node(pos, layer_id) {
            self.l_counter.increment(layer_id);
        }
    }

    pub fn update_c_props(
        &mut self,
        pos: LocalPOS,
        layer_id: usize,
        props: impl IntoIterator<Item = (usize, Prop)>,
        lsn: u64,
    ) {
        self.writer.as_mut()[layer_id].set_lsn(lsn);
        let is_new_node = self.writer.update_c_props(pos, layer_id, props);
        if is_new_node && !self.page.check_node(pos, layer_id) {
            self.l_counter.increment(layer_id);
        }
    }

    pub fn update_timestamp<T: AsTime>(&mut self, t: T, pos: LocalPOS, e_id: ELID, lsn: u64) {
        let layer_id = e_id.layer();
        self.l_counter.update_time(t.t());
        self.writer.as_mut()[layer_id].set_lsn(lsn);
        self.writer.update_timestamp(t, pos, e_id);
    }

    pub fn get_out_edge(&self, pos: LocalPOS, dst: VID, layer_id: usize) -> Option<EID> {
        self.page
            .get_out_edge(pos, dst, layer_id, self.writer.deref())
    }

    pub fn get_inb_edge(&self, pos: LocalPOS, src: VID, layer_id: usize) -> Option<EID> {
        self.page
            .get_inb_edge(pos, src, layer_id, self.writer.deref())
    }

    pub fn store_node_id_and_node_type(
        &mut self,
        pos: LocalPOS,
        layer_id: usize,
        gid: GidRef<'_>,
        node_type: usize,
        lsn: u64,
    ) {
        self.update_c_props(
            pos,
            layer_id,
            node_info_as_props(Some(gid), Some(node_type)),
            lsn,
        );
    }

    pub fn store_node_id(&mut self, pos: LocalPOS, layer_id: usize, gid: GidRef<'_>, lsn: u64) {
        self.update_c_props(pos, layer_id, node_info_as_props(Some(gid), None), lsn);
    }

    pub fn update_deletion_time<T: AsTime>(&mut self, t: T, node: LocalPOS, e_id: ELID, lsn: u64) {
        self.update_timestamp(t, node, e_id, lsn);
    }
}

pub fn node_info_as_props(
    gid: Option<GidRef>,
    node_type: Option<usize>,
) -> impl Iterator<Item = (usize, Prop)> {
    gid.into_iter()
        .map(|g| (1, g.into()))
        .chain(node_type.into_iter().map(|nt| (0, Prop::U64(nt as u64))))
}

impl<'a, MP: DerefMut<Target = MemNodeSegment> + 'a, NS: NodeSegmentOps> Drop
    for NodeWriter<'a, MP, NS>
{
    fn drop(&mut self) {
        // S::persist_node_page(self.est_size, self.page, self.writer.deref_mut());
        self.page
            .notify_write(self.writer.deref_mut())
            .expect("Failed to persist node page");
    }
}

pub enum WriterPair<'a, MP: DerefMut<Target = MemNodeSegment>, NS: NodeSegmentOps> {
    Same {
        writer: NodeWriter<'a, MP, NS>,
    },
    Different {
        src_writer: NodeWriter<'a, MP, NS>,
        dst_writer: NodeWriter<'a, MP, NS>,
    },
}

impl<'a, MP: DerefMut<Target = MemNodeSegment>, NS: NodeSegmentOps> WriterPair<'a, MP, NS> {
    pub fn get_mut_src(&mut self) -> &mut NodeWriter<'a, MP, NS> {
        match self {
            WriterPair::Same { writer, .. } => writer,
            WriterPair::Different {
                src_writer: writer_i,
                ..
            } => writer_i,
        }
    }

    pub fn get_mut_dst(&mut self) -> &mut NodeWriter<'a, MP, NS> {
        match self {
            WriterPair::Same { writer, .. } => writer,
            WriterPair::Different {
                dst_writer: writer_j,
                ..
            } => writer_j,
        }
    }
}
