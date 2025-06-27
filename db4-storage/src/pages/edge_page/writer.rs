use std::ops::DerefMut;

use crate::{
    LocalPOS, api::edges::EdgeSegmentOps, pages::layer_counter::LayerCounter,
    segments::edge::MemEdgeSegment,
};
use raphtory_api::core::entities::{VID, properties::prop::Prop};
use raphtory_core::storage::timeindex::AsTime;

pub struct EdgeWriter<'a, MP: DerefMut<Target = MemEdgeSegment>, ES: EdgeSegmentOps> {
    pub page: &'a ES,
    pub writer: MP,
    pub global_num_edges: &'a LayerCounter,
}

impl<'a, MP: DerefMut<Target = MemEdgeSegment>, ES: EdgeSegmentOps> EdgeWriter<'a, MP, ES> {
    pub fn new(global_num_edges: &'a LayerCounter, page: &'a ES, writer: MP) -> Self {
        Self {
            page,
            writer,
            global_num_edges,
        }
    }

    fn new_local_pos(&self, layer_id: usize) -> LocalPOS {
        let new_pos = LocalPOS(self.page.increment_num_edges());
        self.increment_layer_num_edges(layer_id);
        new_pos
    }

    pub fn add_edge<T: AsTime>(
        &mut self,
        t: T,
        edge_pos: Option<LocalPOS>,
        src: impl Into<VID>,
        dst: impl Into<VID>,
        props: impl IntoIterator<Item = (usize, Prop)>,
        layer_id: usize,
        lsn: u64,
        exists_hint: Option<bool>, // used when edge_pos is Some but the is not counted, this is used in the bulk loader
    ) -> LocalPOS {
        self.writer.as_mut()[layer_id].set_lsn(lsn);

        if exists_hint == Some(false) && edge_pos.is_some() {
            self.new_local_pos(layer_id); // increment the counts, this is triggered from the bulk loader
        }

        let edge_pos = edge_pos.unwrap_or_else(|| self.new_local_pos(layer_id));
        self.writer
            .insert_edge_internal(t, edge_pos, src, dst, layer_id, props);
        edge_pos
    }

    pub fn delete_edge<T: AsTime>(
        &mut self,
        t: T,
        edge_pos: LocalPOS,
        src: impl Into<VID>,
        dst: impl Into<VID>,
        layer_id: usize,
        lsn: u64,
    ) {
        self.writer.as_mut()[layer_id].set_lsn(lsn);
        self.writer
            .delete_edge_internal(t, edge_pos, src, dst, layer_id);
    }

    pub fn add_static_edge(
        &mut self,
        edge_pos: Option<LocalPOS>,
        src: impl Into<VID>,
        dst: impl Into<VID>,
        layer_id: usize,
        lsn: u64,
        exists_hint: Option<bool>, // used when edge_pos is Some but the is not counted, this is used in the bulk loader
    ) -> LocalPOS {
        self.writer.as_mut()[layer_id].set_lsn(lsn);

        if exists_hint == Some(false) && edge_pos.is_some() {
            self.new_local_pos(layer_id); // increment the counts, this is triggered from the bulk loader
        }

        let edge_pos = edge_pos.unwrap_or_else(|| self.new_local_pos(layer_id));
        self.writer
            .insert_static_edge_internal(edge_pos, src, dst, layer_id);
        edge_pos
    }

    pub fn segment_id(&self) -> usize {
        self.page.segment_id()
    }

    fn increment_layer_num_edges(&self, layer_id: usize) {
        self.global_num_edges.increment(layer_id);
    }

    pub fn contains_edge(&self, pos: LocalPOS, layer_id: usize) -> bool {
        self.page.contains_edge(pos, layer_id, self.writer.deref())
    }

    pub fn get_edge(&self, layer_id: usize, edge_pos: LocalPOS) -> Option<(VID, VID)> {
        self.page.get_edge(edge_pos, layer_id, self.writer.deref())
    }

    pub fn update_c_props(
        &mut self,
        edge_pos: LocalPOS,
        src: impl Into<VID>,
        dst: impl Into<VID>,
        layer_id: usize,
        props: impl IntoIterator<Item = (usize, Prop)>,
    ) {
        self.writer
            .update_const_properties(edge_pos, src, dst, layer_id, props);
    }
}

impl<'a, MP: DerefMut<Target = MemEdgeSegment>, ES: EdgeSegmentOps> Drop
    for EdgeWriter<'a, MP, ES>
{
    fn drop(&mut self) {
        if let Err(err) = self.page.notify_write(self.writer.deref_mut()) {
            println!("Failed to persist {}, err: {}", self.segment_id(), err)
        }
    }
}
