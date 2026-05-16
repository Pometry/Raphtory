use std::ops::DerefMut;

use raphtory_api::core::entities::properties::{
    meta::{NODE_ID_IDX, STATIC_GRAPH_LAYER_ID},
    prop::Prop,
};
use raphtory_core::{
    entities::{EID, ELID, GID, LayerId, VID},
    storage::timeindex::AsTime,
};

use crate::{
    LocalPOS, api::nodes::NodeSegmentOps, pages::node_page::writer::NodeWriter,
    segments::node::segment::MemNodeSegment,
};

#[derive(Debug)]
pub struct BulkNodeWriter<'a, MP: DerefMut<Target = MemNodeSegment> + 'a, NS: NodeSegmentOps> {
    nw: NodeWriter<'a, MP, NS>,
    layers: Vec<usize>,
}

impl<'a, MP: DerefMut<Target = MemNodeSegment> + 'a, NS: NodeSegmentOps>
    From<NodeWriter<'a, MP, NS>> for BulkNodeWriter<'a, MP, NS>
{
    fn from(value: NodeWriter<'a, MP, NS>) -> Self {
        Self {
            nw: value,
            layers: Vec::new(),
        }
    }
}

impl<'a, MP: DerefMut<Target = MemNodeSegment> + 'a, NS: NodeSegmentOps>
    BulkNodeWriter<'a, MP, NS>
{
    #[inline]
    pub fn get_out_edge(&self, pos: LocalPOS, dst: VID, layer_id: LayerId) -> Option<EID> {
        self.nw.get_out_edge(pos, dst, layer_id)
    }

    #[inline(always)]
    pub fn resolve_pos(&self, node_id: VID) -> Option<LocalPOS> {
        self.nw.resolve_pos(node_id)
    }

    #[inline(always)]
    pub fn add_static_outbound_edge(
        &mut self,
        src_pos: LocalPOS,
        dst: impl Into<VID>,
        e_id: impl Into<EID>,
    ) {
        let e_id = e_id.into();
        self.nw.add_outbound_edge_inner::<i64>(
            None,
            src_pos,
            dst,
            e_id.with_layer(STATIC_GRAPH_LAYER_ID),
            |layer_id| {
                Self::update_layer_count(layer_id, &mut self.layers);
            },
        );
    }

    pub fn add_static_inbound_edge(
        &mut self,
        dst_pos: LocalPOS,
        src: impl Into<VID>,
        e_id: impl Into<EID>,
    ) {
        let e_id = e_id.into();
        self.nw.add_inbound_edge_inner::<i64>(
            None,
            dst_pos,
            src,
            e_id.with_layer(STATIC_GRAPH_LAYER_ID),
            |layer_id| {
                Self::update_layer_count(layer_id, &mut self.layers);
            },
        );
    }

    #[inline(always)]
    pub fn add_outbound_edge<T: AsTime>(
        &mut self,
        t: Option<T>,
        src_pos: impl Into<LocalPOS>,
        dst: impl Into<VID>,
        e_id: impl Into<ELID>,
    ) {
        self.nw
            .add_outbound_edge_inner(t, src_pos, dst, e_id, |layer_id| {
                Self::update_layer_count(layer_id, &mut self.layers);
            });
    }

    pub fn add_inbound_edge<T: AsTime>(
        &mut self,
        t: Option<T>,
        dst_pos: impl Into<LocalPOS>,
        src: impl Into<VID>,
        e_id: impl Into<ELID>,
    ) {
        self.nw
            .add_inbound_edge_inner(t, dst_pos, src, e_id, |layer_id| {
                Self::update_layer_count(layer_id, &mut self.layers);
            });
    }

    fn update_layer_count(layer_id: LayerId, layers: &mut Vec<usize>) {
        if layers.len() <= layer_id.0 {
            layers.resize_with(layer_id.0 + 1, Default::default);
        }
        layers[layer_id.0] += 1;
    }

    #[inline(always)]
    pub fn update_timestamp<T: AsTime>(&mut self, t: T, pos: LocalPOS, e_id: ELID) {
        self.nw.update_timestamp(t, pos, e_id);
    }

    #[inline(always)]
    pub fn store_node_id(&mut self, pos: LocalPOS, layer_id: LayerId, gid: GID) {
        let gid = match gid {
            GID::U64(id) => Prop::U64(id),
            GID::Str(s) => Prop::str(s),
        };
        let props = [(NODE_ID_IDX, gid)];
        self.nw
            .update_c_props_inner(pos, layer_id, props, |layer_id| {
                Self::update_layer_count(layer_id, &mut self.layers);
            });
    }
}

impl<'a, MP: DerefMut<Target = MemNodeSegment>, ES: NodeSegmentOps> Drop
    for BulkNodeWriter<'a, MP, ES>
{
    fn drop(&mut self) {
        for (layer_id, count) in self.layers.iter().enumerate() {
            self.nw.l_counter.increment_by(LayerId(layer_id), *count);
        }
    }
}
