use crate::{
    LocalPOS, api::edges::EdgeSegmentOps, pages::edge_page::writer::EdgeWriter,
    segments::edge::segment::MemEdgeSegment,
};
use raphtory_api::core::entities::{
    EID, LayerId, VID,
    properties::{meta::STATIC_GRAPH_LAYER_ID, prop::AsPropRef},
};
use raphtory_core::storage::timeindex::{AsTime, EventTime};
use std::ops::DerefMut;

pub struct BulkEdgeWriter<
    'a,
    MP: DerefMut<Target = MemEdgeSegment> + std::fmt::Debug,
    ES: EdgeSegmentOps,
> {
    ew: EdgeWriter<'a, MP, ES>,
    layers: Vec<usize>,
    earliest: EventTime,
    latest: EventTime,
}

impl<'a, MP: DerefMut<Target = MemEdgeSegment> + std::fmt::Debug, ES: EdgeSegmentOps>
    From<EdgeWriter<'a, MP, ES>> for BulkEdgeWriter<'a, MP, ES>
{
    fn from(value: EdgeWriter<'a, MP, ES>) -> Self {
        Self {
            ew: value,
            layers: vec![0],
            earliest: EventTime::MAX,
            latest: EventTime::MIN,
        }
    }
}

impl<'a, MP: DerefMut<Target = MemEdgeSegment> + std::fmt::Debug, ES: EdgeSegmentOps>
    BulkEdgeWriter<'a, MP, ES>
{
    pub fn bulk_add_edge<P: AsPropRef>(
        &mut self,
        t: EventTime,
        edge_pos: LocalPOS,
        src: VID,
        dst: VID,
        edge_exists: bool,
        layer_id: LayerId,
        c_props: impl IntoIterator<Item = (usize, P)>,
        t_props: impl IntoIterator<Item = (usize, P)>,
    ) {
        if !edge_exists
            && self
                .ew
                .writer
                .insert_static_edge_internal(edge_pos, src, dst, STATIC_GRAPH_LAYER_ID)
        {
            self.increment_layer_num_edges(STATIC_GRAPH_LAYER_ID);
        }

        // `*_bulk`: no per-prop layer-presence marking
        if self
            .ew
            .writer
            .insert_edge_internal_bulk(t, edge_pos, src, dst, layer_id, t_props)
            && !self.ew.segment.immut_has_edge(edge_pos, layer_id)
        {
            self.increment_layer_num_edges(layer_id);
        }

        self.update_time(t);

        self.ew
            .writer
            .update_const_properties_bulk(edge_pos, src, dst, layer_id, c_props);
    }

    pub fn bulk_delete_edge(
        &mut self,
        t: EventTime,
        edge_pos: LocalPOS,
        src: VID,
        dst: VID,
        exists: bool,
        layer_id: LayerId,
    ) {
        if !exists
            && self
                .ew
                .writer
                .insert_static_edge_internal(edge_pos, src, dst, STATIC_GRAPH_LAYER_ID)
        {
            self.increment_layer_num_edges(STATIC_GRAPH_LAYER_ID);
        }

        self.update_time(t);
        if self
            .ew
            .writer
            .delete_edge_internal(t, edge_pos, src, dst, layer_id)
            && !self.ew.segment.immut_has_edge(edge_pos, layer_id)
        {
            self.increment_layer_num_edges(layer_id);
        }
    }

    #[inline]
    fn increment_layer_num_edges(&mut self, layer_id: LayerId) {
        if self.layers.len() <= layer_id.0 {
            self.layers.resize_with(layer_id.0 + 1, Default::default);
        }
        self.layers[layer_id.0] += 1;
    }

    #[inline]
    fn update_time(&mut self, t: EventTime) {
        self.earliest = self.earliest.min(t);
        self.latest = self.latest.max(t);
    }

    #[inline(always)]
    pub fn resolve_pos(&self, edge_id: EID) -> Option<LocalPOS> {
        self.ew.resolve_pos(edge_id)
    }
}

impl<'a, MP: DerefMut<Target = MemEdgeSegment> + std::fmt::Debug, ES: EdgeSegmentOps> Drop
    for BulkEdgeWriter<'a, MP, ES>
{
    fn drop(&mut self) {
        for (layer_id, count) in self.layers.iter().enumerate() {
            self.ew.graph_stats.increment_by(LayerId(layer_id), *count);
        }
        self.ew.graph_stats.update_time(self.earliest.t());
        self.ew.graph_stats.update_time(self.latest.t());
    }
}
