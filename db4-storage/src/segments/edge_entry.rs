use raphtory_api::core::entities::properties::prop::Prop;
use raphtory_core::{
    entities::{
        EID, Multiple, VID,
        properties::{tcell::TCell, tprop::TPropCell},
    },
    storage::timeindex::{TimeIndexEntry, TimeIndexOps},
};

use crate::{
    EdgeAdditions, EdgeDeletions, EdgeTProps, LocalPOS,
    api::edges::{EdgeEntryOps, EdgeRefOps},
    gen_t_props::WithTProps,
    gen_ts::{AdditionCellsRef, DeletionCellsRef, WithTimeCells},
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
                .is_some_and(|layer| layer.has_item(self.pos))
        })
    }
}

impl<'a> WithTimeCells<'a> for MemEdgeRef<'a> {
    type TimeCell = MemAdditions<'a>;

    fn t_props_tc(
        self,
        layer_id: usize,
        range: Option<(TimeIndexEntry, TimeIndexEntry)>,
    ) -> impl Iterator<Item = Self::TimeCell> + 'a {
        self.es
            .as_ref()
            .get(layer_id)
            .map(|layer| MemAdditions::Props(layer.times_from_props(self.pos)))
            .into_iter()
            .map(move |t_props| {
                range
                    .map(|(start, end)| t_props.range(start..end))
                    .unwrap_or_else(|| t_props)
            })
    }

    fn additions_tc(
        self,
        _layer_id: usize,
        _range: Option<(TimeIndexEntry, TimeIndexEntry)>,
    ) -> impl Iterator<Item = Self::TimeCell> + 'a {
        std::iter::empty()
    }

    fn deletions_tc(
        self,
        layer_id: usize,
        range: Option<(TimeIndexEntry, TimeIndexEntry)>,
    ) -> impl Iterator<Item = Self::TimeCell> + 'a {
        let deletions = self
            .es
            .as_ref()
            .get(layer_id)
            .map(|layer| layer.deletions(self.pos))
            .unwrap_or(&TCell::Empty);
        let t_cell = MemAdditions::Edges(deletions);
        std::iter::once(
            range
                .map(|(start, end)| t_cell.range(start..end))
                .unwrap_or_else(|| t_cell),
        )
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
        self.es
            .as_ref()
            .get(layer_id)
            .into_iter()
            .flat_map(move |layer| layer.t_prop(edge_pos, prop_id).into_iter())
    }
}

impl<'a> EdgeRefOps<'a> for MemEdgeRef<'a> {
    type Additions = EdgeAdditions<'a>;

    type Deletions = EdgeDeletions<'a>;

    type TProps = EdgeTProps<'a>;

    fn edge(self, layer_id: usize) -> Option<(VID, VID)> {
        self.es
            .as_ref()
            .get(layer_id)? //.get(layer_id)?
            .get(self.pos)
            .map(|entry| (entry.src, entry.dst))
    }

    fn layer_additions(self, layer_id: usize) -> Self::Additions {
        EdgeAdditions::new_with_layer(AdditionCellsRef::new(self), layer_id)
    }

    fn layer_deletions(self, layer_id: usize) -> Self::Deletions {
        EdgeDeletions::new_with_layer(DeletionCellsRef::new(self), layer_id)
    }

    fn c_prop(self, layer_id: usize, prop_id: usize) -> Option<Prop> {
        self.es.as_ref().get(layer_id)?.c_prop(self.pos, prop_id)
    }

    fn layer_t_prop(self, layer_id: usize, prop_id: usize) -> Self::TProps {
        EdgeTProps::new_with_layer(self, layer_id, prop_id)
    }

    fn src(&self) -> Option<VID> {
        self.es.as_ref()[0].get(self.pos).map(|entry| entry.src)
    }

    fn dst(&self) -> Option<VID> {
        self.es.as_ref()[0].get(self.pos).map(|entry| entry.dst)
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
