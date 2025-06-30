use raphtory_api::core::entities::properties::prop::Prop;
use raphtory_core::{
    entities::{EID, LayerIds, Multiple, VID, properties::tprop::TPropCell},
    storage::timeindex::{TimeIndexEntry, TimeIndexOps},
};

use crate::{
    EdgeAdditions, EdgeTProps, LocalPOS,
    api::edges::{EdgeEntryOps, EdgeRefOps},
    gen_t_props::WithTProps,
    gen_ts::WithTimeCells,
};

use super::{additions::MemAdditions, edge::MemEdgeSegment};

#[derive(Debug)]
pub struct MemEdgeEntry<'a, MES> {
    pos: LocalPOS,
    es: MES,
    __marker: std::marker::PhantomData<&'a ()>,
}

impl<'a, MES: std::ops::Deref<Target = MemEdgeSegment>> MemEdgeEntry<'a, MES> {
    pub fn new(pos: LocalPOS, es: MES) -> Self {
        Self {
            pos,
            es,
            __marker: std::marker::PhantomData,
        }
    }
}

impl<'a, MES: std::ops::Deref<Target = MemEdgeSegment> + Send + Sync> EdgeEntryOps<'a>
    for MemEdgeEntry<'a, MES>
{
    type Ref<'b>
        = MemEdgeRef<'b>
    where
        'a: 'b,
        MES: 'b;

    fn as_ref<'b>(&'b self) -> Self::Ref<'b>
    where
        'a: 'b,
    {
        MemEdgeRef {
            pos: self.pos,
            es: &self.es,
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct MemEdgeRef<'a> {
    pos: LocalPOS,
    es: &'a MemEdgeSegment,
}

impl<'a> MemEdgeRef<'a> {
    pub fn new(pos: LocalPOS, es: &'a MemEdgeSegment) -> Self {
        Self { pos, es }
    }

    pub fn has_layers(&self, layer_ids: &Multiple) -> bool {
        layer_ids.iter().any(|layer_id| {
            self.es
                .as_ref()
                .get(layer_id)
                .and_then(|entry| entry.items().get(self.pos.0))
                .is_some_and(|item| *item)
        })
    }
}

impl<'a> WithTimeCells<'a> for MemEdgeRef<'a> {
    type TimeCell = MemAdditions<'a>;

    fn t_props_time_cells(
        self,
        layer_id: usize,
        range: Option<(TimeIndexEntry, TimeIndexEntry)>,
    ) -> impl Iterator<Item = Self::TimeCell> + 'a {
        let t_cell = MemAdditions::Props(self.es.as_ref()[layer_id].additions(self.pos).props_ts());
        std::iter::once(
            range
                .map(|(start, end)| t_cell.range(start..end))
                .unwrap_or_else(|| t_cell),
        )
    }

    fn additions_time_cells(
        self,
        _layer_id: usize,
        _range: Option<(TimeIndexEntry, TimeIndexEntry)>,
    ) -> impl Iterator<Item = Self::TimeCell> + 'a {
        std::iter::empty()
    }

    fn num_layers(&self) -> usize {
        self.es.as_ref().len()
    }
}

impl<'a> WithTProps<'a> for MemEdgeRef<'a> {
    type TProp = TPropCell<'a>;

    fn num_layers(&self) -> usize {
        self.es.as_ref().len()
    }

    fn into_t_props(
        self,
        layer_id: usize,
        prop_id: usize,
    ) -> impl Iterator<Item = Self::TProp> + 'a {
        let edge_pos = self.pos;
        self.es.as_ref()[layer_id]
            .t_prop(edge_pos, prop_id)
            .into_iter()
            .map(|t_prop| t_prop.into())
    }
}

impl<'a> EdgeRefOps<'a> for MemEdgeRef<'a> {
    type Additions = EdgeAdditions<'a>;

    type TProps = EdgeTProps<'a>;

    fn edge(self, layer_id: usize) -> Option<(VID, VID)> {
        self.es.as_ref()[layer_id]
            .get(&self.pos)
            .map(|entry| (entry.src, entry.dst))
    }

    fn additions(self, layer_id: &'a LayerIds) -> Self::Additions {
        EdgeAdditions::new(self, layer_id)
    }

    fn layer_additions(self, layer_id: usize) -> Self::Additions {
        EdgeAdditions::new_with_layer(self, layer_id)
    }

    fn c_prop(self, layer_id: usize, prop_id: usize) -> Option<Prop> {
        self.es.as_ref()[layer_id].c_prop(self.pos, prop_id)
    }

    fn t_prop(self, layer_id: &'a LayerIds, prop_id: usize) -> Self::TProps {
        EdgeTProps::new(self, layer_id, prop_id)
    }

    fn layer_t_prop(self, layer_id: usize, prop_id: usize) -> Self::TProps {
        EdgeTProps::new_with_layer(self, layer_id, prop_id)
    }

    fn src(&self) -> VID {
        self.es.as_ref()[0]
            .get(&self.pos)
            .map(|entry| entry.src)
            .expect("Edge must have a source vertex")
    }

    fn dst(&self) -> VID {
        self.es.as_ref()[0]
            .get(&self.pos)
            .map(|entry| entry.dst)
            .expect("Edge must have a destination vertex")
    }

    fn edge_id(&self) -> EID {
        let segment_id = self.es.as_ref()[0].segment_id();
        let max_page_len = self.es.as_ref()[0].max_page_len();
        self.pos.as_eid(segment_id, max_page_len)
    }

    fn internal_num_layers(self) -> usize {
        self.es.as_ref().len()
    }
}
