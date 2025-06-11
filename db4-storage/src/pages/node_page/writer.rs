use crate::{LocalPOS, NodeSegmentOps, segments::node::MemNodeSegment};
use raphtory_api::core::entities::{EID, VID, properties::prop::Prop};
use raphtory_core::{entities::ELID, storage::timeindex::AsTime};
use std::{ops::DerefMut, sync::atomic::AtomicUsize};

#[derive(Debug)]
pub struct NodeWriter<'a, MP: DerefMut<Target = MemNodeSegment> + 'a, NS: NodeSegmentOps> {
    pub page: &'a NS,
    pub writer: MP, // TODO: rename to m_segment
    pub global_num_nodes: &'a AtomicUsize,
}

impl<'a, MP: DerefMut<Target = MemNodeSegment> + 'a, NS: NodeSegmentOps> NodeWriter<'a, MP, NS> {
    pub fn new(page: &'a NS, global_num_nodes: &'a AtomicUsize, writer: MP) -> Self {
        Self {
            page,
            writer,
            global_num_nodes,
        }
    }

    pub fn add_outbound_edge<T: AsTime>(
        &mut self,
        t: T,
        src_pos: impl Into<LocalPOS>,
        dst: impl Into<VID>,
        e_id: impl Into<ELID>,
        lsn: u64,
    ) {
        self.add_outbound_edge_inner(Some(t), src_pos, dst, e_id, lsn);
    }

    pub fn add_static_outbound_edge(
        &mut self,
        src_pos: LocalPOS,
        dst: impl Into<VID>,
        e_id: impl Into<ELID>,
        lsn: u64,
    ) {
        self.add_outbound_edge_inner::<i64>(None, src_pos, dst, e_id, lsn);
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
        self.writer.as_mut().set_lsn(lsn);

        let e_id = e_id.into();
        let is_new_node = self.writer.add_outbound_edge(t, src_pos, dst, e_id);

        if is_new_node && !self.page.check_node(src_pos) {
            self.page.increment_num_nodes();
            self.global_num_nodes
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    pub fn add_inbound_edge<T: AsTime>(
        &mut self,
        t: T,
        dst_pos: impl Into<LocalPOS>,
        src: impl Into<VID>,
        e_id: impl Into<ELID>,
        lsn: u64,
    ) {
        self.add_inbound_edge_inner(Some(t), dst_pos, src, e_id, lsn);
    }

    pub fn add_static_inbound_edge(
        &mut self,
        dst_pos: LocalPOS,
        src: impl Into<VID>,
        e_id: impl Into<ELID>,
        lsn: u64,
    ) {
        self.add_inbound_edge_inner::<i64>(None, dst_pos, src, e_id, lsn);
    }

    fn add_inbound_edge_inner<T: AsTime>(
        &mut self,
        t: Option<T>,
        dst_pos: impl Into<LocalPOS>,
        src: impl Into<VID>,
        e_id: impl Into<ELID>,
        lsn: u64,
    ) {
        self.writer.as_mut().set_lsn(lsn);
        let e_id = e_id.into();
        let dst_pos = dst_pos.into();
        let is_new_node = self.writer.add_inbound_edge(t, dst_pos, src, e_id);

        if is_new_node && !self.page.check_node(dst_pos) {
            self.page.increment_num_nodes();
            self.global_num_nodes
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    pub fn add_props<T: AsTime>(
        &mut self,
        t: T,
        pos: LocalPOS,
        props: impl IntoIterator<Item = (usize, Prop)>,
        lsn: u64,
    ) {
        self.writer.as_mut().set_lsn(lsn);
        self.writer.add_props(t, pos, props);
        // self.est_size = self.page.increment_size(size_of::<(i64, i64)>());
    }

    pub fn update_c_props(
        &mut self,
        pos: LocalPOS,
        props: impl IntoIterator<Item = (usize, Prop)>,
        lsn: u64,
    ) {
        self.writer.as_mut().set_lsn(lsn);
        self.writer.update_c_props(pos, props);
        // self.est_size = self.page.increment_size(size_of::<(i64, i64)>());
    }

    pub fn update_timestamp<T: AsTime>(&mut self, t: T, pos: LocalPOS, e_id: ELID, lsn: u64) {
        self.writer.as_mut().set_lsn(lsn);
        self.writer.update_timestamp(t, pos, e_id);
        // self.est_size = self.page.increment_size(size_of::<(i64, i64)>());
    }

    pub fn get_out_edge(&self, pos: LocalPOS, dst: VID) -> Option<EID> {
        self.page.get_out_edge(pos, dst, self.writer.deref())
    }

    pub fn get_inb_edge(&self, pos: LocalPOS, src: VID) -> Option<EID> {
        self.page.get_inb_edge(pos, src, self.writer.deref())
    }
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
