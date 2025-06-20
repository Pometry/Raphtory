use raphtory_api::core::entities::properties::prop::Prop;
use raphtory_core::{
    entities::{LayerIds, VID, properties::tprop::TPropCell},
    storage::timeindex::{TimeIndexEntry, TimeIndexOps},
};

use crate::{
    EdgeAdditions, LocalPOS,
    api::edges::{EdgeEntryOps, EdgeRefOps},
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

impl<'a, MES: std::ops::Deref<Target = MemEdgeSegment>> EdgeEntryOps<'a> for MemEdgeEntry<'a, MES> {
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
}

impl<'a> WithTimeCells<'a> for MemEdgeRef<'a> {
    type TimeCell = MemAdditions<'a>;

    fn layer_time_cells(
        self,
        layer_id: usize,
        range: Option<(TimeIndexEntry, TimeIndexEntry)>,
    ) -> impl Iterator<Item = Self::TimeCell> + 'a {
        std::iter::once(
            range
                .map(|(start, end)| {
                    MemAdditions::Window(
                        self.es.as_ref()[layer_id]
                            .additions(self.pos)
                            .range(start..end),
                    )
                })
                .unwrap_or_else(|| {
                    MemAdditions::Props(self.es.as_ref()[layer_id].additions(self.pos))
                }),
        )
    }

    fn num_layers(&self) -> usize {
        self.es.as_ref().len()
    }
}

impl<'a> EdgeRefOps<'a> for MemEdgeRef<'a> {
    type Additions = EdgeAdditions<'a>;

    type TProps = TPropCell<'a>;

    fn edge(self, layer_id: usize) -> Option<(VID, VID)> {
        self.es.as_ref()[layer_id]
            .get(&self.pos)
            .map(|entry| (entry.src, entry.dst))
    }

    fn additions(self, layer_id: &'a LayerIds) -> Self::Additions {
        EdgeAdditions::new(self, layer_id)
    }

    fn c_prop(self, layer_id: usize, prop_id: usize) -> Option<Prop> {
        self.es.as_ref()[layer_id].c_prop(self.pos, prop_id)
    }

    fn t_prop(self, layer_id: usize, prop_id: usize) -> Self::TProps {
        self.es.as_ref()[layer_id]
            .t_prop(self.pos, prop_id)
            .unwrap_or_default()
    }
}
