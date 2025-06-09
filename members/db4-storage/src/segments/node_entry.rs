use crate::{NodeEntryOps, NodeRefOps, segments::node::MemNodeSegment};
use db4_common::LocalPOS;
use raphtory::{core::entities::properties::tprop::TPropCell, prelude::Prop};
use raphtory_api::core::entities::{EID, VID};
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
#[derive(Copy, Clone)]
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

    fn out_edges(self) -> impl Iterator<Item = (VID, EID)> + 'a {
        self.ns.out_edges(self.pos)
    }

    fn inb_edges(self) -> impl Iterator<Item = (VID, EID)> + 'a {
        self.ns.inb_edges(self.pos)
    }

    fn out_edges_sorted(self) -> impl Iterator<Item = (VID, EID)> + 'a {
        self.ns.out_edges(self.pos)
    }

    fn inb_edges_sorted(self) -> impl Iterator<Item = (VID, EID)> + 'a {
        self.ns.inb_edges(self.pos)
    }

    fn additions(self) -> Self::Additions {
        MemAdditions::Props(self.ns.as_ref().additions(self.pos))
    }

    fn c_prop(self, prop_id: usize) -> Option<Prop> {
        self.ns.as_ref().c_prop(self.pos, prop_id)
    }

    fn t_prop(self, prop_id: usize) -> Self::TProps {
        self.ns
            .as_ref()
            .t_prop(self.pos, prop_id, 0)
            .unwrap_or_default()
    }
}
