use crate::{LocalPOS, NodeEntryOps, NodeRefOps, segments::node::MemNodeSegment};
use raphtory_api::core::entities::{EID, VID, properties::prop::Prop};
use raphtory_core::entities::properties::tprop::TPropCell;
use std::ops::Deref;

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

impl<'a, MNS: Deref<Target = MemNodeSegment>> NodeEntryOps<'a> for MemNodeEntry<'a, MNS> {
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

impl<'a> NodeRefOps<'a> for MemNodeRef<'a> {
    type Additions = MemAdditions<'a>;
    type TProps = TPropCell<'a>;

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

    fn additions(self, layer_id: usize) -> Self::Additions {
        MemAdditions::Props(self.ns.as_ref()[layer_id].additions(self.pos))
    }

    fn c_prop(self, layer_id: usize, prop_id: usize) -> Option<Prop> {
        self.ns.as_ref()[layer_id].c_prop(self.pos, prop_id)
    }

    fn t_prop(self, layer_id: usize, prop_id: usize) -> Self::TProps {
        self.ns.as_ref()[layer_id]
            .t_prop(self.pos, prop_id)
            .unwrap_or_default()
    }
}
