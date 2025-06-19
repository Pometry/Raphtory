use std::{
    ops::{Deref, DerefMut},
    path::Path,
    sync::Arc,
};

use parking_lot::{RwLockReadGuard, RwLockWriteGuard, lock_api::ArcRwLockReadGuard};
use raphtory_api::core::entities::properties::{meta::Meta, prop::Prop, tprop::TPropOps};
use raphtory_core::{
    entities::{EID, VID},
    storage::timeindex::{TimeIndexEntry, TimeIndexOps},
};

use crate::{LocalPOS, error::DBV4Error, segments::node::MemNodeSegment};

pub trait NodeSegmentOps: Send + Sync + std::fmt::Debug {
    type Extension;

    type Entry<'a>: NodeEntryOps<'a>
    where
        Self: 'a;

    type ArcLockedSegment: LockedNSSegment;

    fn latest(&self) -> Option<TimeIndexEntry>;
    fn earliest(&self) -> Option<TimeIndexEntry>;

    fn t_len(&self) -> usize;

    fn load(
        page_id: usize,
        max_page_len: usize,
        meta: Arc<Meta>,
        path: impl AsRef<Path>,
        ext: Self::Extension,
    ) -> Result<Self, DBV4Error>
    where
        Self: Sized;
    fn new(
        page_id: usize,
        max_page_len: usize,
        meta: Arc<Meta>,
        path: impl AsRef<Path>,
        ext: Self::Extension,
    ) -> Self;

    fn segment_id(&self) -> usize;

    fn head_arc(&self) -> ArcRwLockReadGuard<parking_lot::RawRwLock, MemNodeSegment>;
    fn head(&self) -> RwLockReadGuard<MemNodeSegment>;

    fn head_mut(&self) -> RwLockWriteGuard<MemNodeSegment>;

    fn num_nodes(&self) -> usize {
        self.layer_num_nodes(0)
    }

    fn num_layers(&self) -> usize;

    fn layer_num_nodes(&self, layer_id: usize) -> usize;

    fn notify_write(
        &self,
        head_lock: impl DerefMut<Target = MemNodeSegment>,
    ) -> Result<(), DBV4Error>;

    fn check_node(&self, pos: LocalPOS, layer_id: usize) -> bool;

    fn get_out_edge(
        &self,
        pos: LocalPOS,
        dst: impl Into<VID>,
        layer_id: usize,
        locked_head: impl Deref<Target = MemNodeSegment>,
    ) -> Option<EID>;

    fn get_inb_edge(
        &self,
        pos: LocalPOS,
        src: impl Into<VID>,
        layer_id: usize,
        locked_head: impl Deref<Target = MemNodeSegment>,
    ) -> Option<EID>;

    fn entry<'a>(&'a self, pos: impl Into<LocalPOS>) -> Self::Entry<'a>;

    fn locked(self: &Arc<Self>) -> Self::ArcLockedSegment;
}

pub trait LockedNSSegment: std::fmt::Debug + Send + Sync {
    type EntryRef<'a>: NodeRefOps<'a>
    where
        Self: 'a;

    fn entry_ref<'a>(&'a self, pos: impl Into<LocalPOS>) -> Self::EntryRef<'a>;
}

#[derive(Debug)]
pub struct ReadLockedNS<NS: NodeSegmentOps> {
    ns: Arc<NS>,
    head: NS::ArcLockedSegment,
}

pub trait NodeEntryOps<'a> {
    type Ref<'b>: NodeRefOps<'b>
    where
        'a: 'b,
        Self: 'b;

    fn as_ref<'b>(&'b self) -> Self::Ref<'b>
    where
        'a: 'b;
}

pub trait NodeRefOps<'a>: Copy + Clone + Send + Sync {
    type Additions: TimeIndexOps<'a>;

    type TProps: TPropOps<'a>;

    fn out_edges(self, layer_id: usize) -> impl Iterator<Item = (VID, EID)> + 'a;

    fn inb_edges(self, layer_id: usize) -> impl Iterator<Item = (VID, EID)> + 'a;

    fn out_edges_sorted(self, layer_id: usize) -> impl Iterator<Item = (VID, EID)> + 'a;

    fn inb_edges_sorted(self, layer_id: usize) -> impl Iterator<Item = (VID, EID)> + 'a;

    fn out_nbrs(self, layer_id: usize) -> impl Iterator<Item = VID> + 'a
    where
        Self: Sized,
    {
        self.out_edges(layer_id).map(|(v, _)| v)
    }

    fn inb_nbrs(self, layer_id: usize) -> impl Iterator<Item = VID> + 'a
    where
        Self: Sized,
    {
        self.inb_edges(layer_id).map(|(v, _)| v)
    }

    fn out_nbrs_sorted(self, layer_id: usize) -> impl Iterator<Item = VID> + 'a
    where
        Self: Sized,
    {
        self.out_edges_sorted(layer_id).map(|(v, _)| v)
    }

    fn inb_nbrs_sorted(self, layer_id: usize) -> impl Iterator<Item = VID> + 'a
    where
        Self: Sized,
    {
        self.inb_edges_sorted(layer_id).map(|(v, _)| v)
    }

    fn additions(self, layer_id: usize) -> Self::Additions;

    fn c_prop(self, layer_id: usize, prop_id: usize) -> Option<Prop>;

    fn t_prop(self, layer_id: usize, prop_id: usize) -> Self::TProps;
}
