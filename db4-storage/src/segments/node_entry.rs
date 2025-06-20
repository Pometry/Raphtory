use crate::{
    LocalPOS, NodeAdditions,
    api::nodes::{NodeEntryOps, NodeRefOps},
    gen_ts::{GenericTimeOps, WithTimeCells},
    segments::node::MemNodeSegment,
};
use raphtory_api::core::{
    Direction,
    entities::{EID, VID, properties::prop::Prop},
};
use raphtory_core::{
    entities::{LayerIds, nodes::node_store::PropTimestamps, properties::tprop::TPropCell},
    storage::timeindex::{TimeIndexEntry, TimeIndexOps},
};
use std::{iter::Empty, ops::Deref};

use super::additions::MemAdditions;

pub struct MemNodeEntry<'a, MNS> {
    pos: LocalPOS,
    ns: MNS,
    __marker: std::marker::PhantomData<&'a ()>,
}

impl<'a, MNS: Deref<Target = MemNodeSegment>> MemNodeEntry<'a, MNS> {
    pub fn new(pos: LocalPOS, ns: MNS) -> Self {
        Self {
            pos,
            ns,
            __marker: std::marker::PhantomData,
        }
    }
}

impl<'a, MNS: Deref<Target = MemNodeSegment> + Send + Sync + 'a> NodeEntryOps<'a>
    for MemNodeEntry<'a, MNS>
{
    type Ref<'b>
        = MemNodeRef<'b>
    where
        'a: 'b,
        MNS: 'b;

    fn as_ref<'b>(&'b self) -> Self::Ref<'b>
    where
        'a: 'b,
    {
        MemNodeRef {
            pos: self.pos,
            ns: self.ns.deref(),
        }
    }
}
#[derive(Copy, Clone, Debug)]
pub struct MemNodeRef<'a> {
    pos: LocalPOS,
    ns: &'a MemNodeSegment,
}

impl<'a> MemNodeRef<'a> {
    pub fn new(pos: LocalPOS, ns: &'a MemNodeSegment) -> Self {
        Self { pos, ns }
    }
}

impl<'a> WithTimeCells<'a> for MemNodeRef<'a> {
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
                        self.ns.as_ref()[layer_id]
                            .additions(self.pos)
                            .range(start..end),
                    )
                })
                .unwrap_or_else(|| {
                    MemAdditions::Props(self.ns.as_ref()[layer_id].additions(self.pos))
                }),
        )
    }

    fn num_layers(&self) -> usize {
        self.ns.as_ref().len()
    }
}

impl<'a> NodeRefOps<'a> for MemNodeRef<'a> {
    type Additions = NodeAdditions<'a>;
    type TProps = TPropCell<'a>;

    fn vid(&self) -> VID {
        self.ns.to_vid(self.pos)
    }

    fn out_edges(self, layer_id: usize) -> impl Iterator<Item = (VID, EID)> + 'a {
        self.ns.out_edges(self.pos, layer_id)
    }

    fn inb_edges(self, layer_id: usize) -> impl Iterator<Item = (VID, EID)> + 'a {
        self.ns.inb_edges(self.pos, layer_id)
    }

    fn out_edges_sorted(self, layer_id: usize) -> impl Iterator<Item = (VID, EID)> + 'a {
        self.ns.out_edges(self.pos, layer_id)
    }

    fn inb_edges_sorted(self, layer_id: usize) -> impl Iterator<Item = (VID, EID)> + 'a {
        self.ns.inb_edges(self.pos, layer_id)
    }

    fn additions(self, layer_ids: &'a LayerIds) -> Self::Additions {
        NodeAdditions::new(self, layer_ids)
    }

    fn c_prop(self, layer_id: usize, prop_id: usize) -> Option<Prop> {
        self.ns.as_ref()[layer_id].c_prop(self.pos, prop_id)
    }

    fn t_prop(self, layer_id: usize, prop_id: usize) -> Self::TProps {
        self.ns.as_ref()[layer_id]
            .t_prop(self.pos, prop_id)
            .unwrap_or_default()
    }

    fn temp_prop_rows(
        self,
    ) -> impl Iterator<
        Item = (
            TimeIndexEntry,
            usize,
            impl Iterator<Item = (usize, Option<Prop>)>,
        ),
    > + 'a {
        // self.ns.as_ref().iter().enumerate().flat_map(|(layer_id, layer)| {
        //     let rows = layer.t_prop_rows(self.pos);
        // }).flat_map(|t_prop| {
        //     t_prop.
        // })

        //TODO
        std::iter::empty::<(_, _, Empty<_>)>()
    }

    fn degree(self, layers: &LayerIds, dir: Direction) -> usize {
        match layers {
            LayerIds::One(layer_id) => self.ns.degree(self.pos, *layer_id, dir),
            LayerIds::All => self.ns.degree(self.pos, 0, dir),
            LayerIds::None => 0,
            layers => self.edges_iter(layers, dir).count(),
        }
    }
}
