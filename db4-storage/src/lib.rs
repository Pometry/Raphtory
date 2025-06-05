use std::{
    ops::{Deref, DerefMut},
    path::Path,
    sync::Arc,
};

use crate::{
    pages::GraphStore,
    segments::{edge::EdgeSegmentView, node::NodeSegmentView},
};
use db4_common::{LocalPOS, error::DBV4Error};
use parking_lot::{RwLockReadGuard, RwLockWriteGuard};
use raphtory::{
    core::entities::{EID, VID},
    prelude::Prop,
};
use raphtory_api::core::{
    entities::properties::{meta::Meta, tprop::TPropOps},
    storage::timeindex::{TimeIndexEntry, TimeIndexOps},
};
use segments::{edge::MemEdgeSegment, node::MemNodeSegment};

pub mod loaders;
pub mod pages;
pub mod properties;
pub mod segments;

pub type Layer<EXT> = GraphStore<NodeSegmentView, EdgeSegmentView, EXT>;

pub trait EdgeSegmentOps: Send + Sync {
    type Extension;

    type Entry<'a>: EdgeEntryOps<'a>
    where
        Self: 'a;

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

    fn num_edges(&self) -> usize;

    fn head(&self) -> RwLockReadGuard<MemEdgeSegment>;

    fn head_mut(&self) -> RwLockWriteGuard<MemEdgeSegment>;

    fn try_head_mut(&self) -> Option<RwLockWriteGuard<MemEdgeSegment>>;

    fn notify_write(
        &self,
        head_lock: impl DerefMut<Target = MemEdgeSegment>,
    ) -> Result<(), DBV4Error>;

    fn increment_num_edges(&self) -> usize;

    fn contains_edge(
        &self,
        edge_pos: LocalPOS,
        locked_head: impl Deref<Target = MemEdgeSegment>,
    ) -> bool;

    fn get_edge(
        &self,
        edge_pos: LocalPOS,
        locked_head: impl Deref<Target = MemEdgeSegment>,
    ) -> Option<(VID, VID)>;

    fn entry<'a, LP: Into<LocalPOS>>(&'a self, edge_pos: LP) -> Self::Entry<'a>;
}

pub trait EdgeEntryOps<'a> {
    type Ref<'b>: EdgeRefOps<'b>
    where
        'a: 'b,
        Self: 'b;

    fn as_ref<'b>(&'b self) -> Self::Ref<'b>
    where
        'a: 'b;
}

pub trait EdgeRefOps<'a>: Copy + Clone + Send + Sync {
    type Additions: TimeIndexOps<'a>;
    type TProps: TPropOps<'a>;

    fn edge(self) -> Option<(VID, VID)>;

    fn additions(self) -> Self::Additions;

    fn c_prop(self, prop_id: usize) -> Option<Prop>;

    fn t_prop(self, prop_id: usize) -> Self::TProps;
}

pub trait NodeSegmentOps: Send + Sync {
    type Extension;

    type Entry<'a>: NodeEntryOps<'a>
    where
        Self: 'a;

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

    fn head(&self) -> RwLockReadGuard<MemNodeSegment>;

    fn head_mut(&self) -> RwLockWriteGuard<MemNodeSegment>;

    fn num_nodes(&self) -> usize;

    fn increment_num_nodes(&self) -> usize;

    fn notify_write(
        &self,
        head_lock: impl DerefMut<Target = MemNodeSegment>,
    ) -> Result<(), DBV4Error>;

    fn check_node(&self, pos: LocalPOS) -> bool;

    fn get_out_edge(
        &self,
        pos: LocalPOS,
        dst: impl Into<VID>,
        locked_head: impl Deref<Target = MemNodeSegment>,
    ) -> Option<EID>;

    fn get_inb_edge(
        &self,
        pos: LocalPOS,
        src: impl Into<VID>,
        locked_head: impl Deref<Target = MemNodeSegment>,
    ) -> Option<EID>;

    fn entry<'a>(&'a self, pos: impl Into<LocalPOS>) -> Self::Entry<'a>;
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

    fn out_edges(self) -> impl Iterator<Item = (VID, EID)> + 'a;

    fn inb_edges(self) -> impl Iterator<Item = (VID, EID)> + 'a;

    fn out_edges_sorted(self) -> impl Iterator<Item = (VID, EID)> + 'a;

    fn inb_edges_sorted(self) -> impl Iterator<Item = (VID, EID)> + 'a;

    fn out_nbrs(self) -> impl Iterator<Item = VID> + 'a
    where
        Self: Sized,
    {
        self.out_edges().map(|(v, _)| v)
    }

    fn inb_nbrs(self) -> impl Iterator<Item = VID> + 'a
    where
        Self: Sized,
    {
        self.inb_edges().map(|(v, _)| v)
    }

    fn out_nbrs_sorted(self) -> impl Iterator<Item = VID> + 'a
    where
        Self: Sized,
    {
        self.out_edges_sorted().map(|(v, _)| v)
    }

    fn inb_nbrs_sorted(self) -> impl Iterator<Item = VID> + 'a
    where
        Self: Sized,
    {
        self.inb_edges_sorted().map(|(v, _)| v)
    }

    fn additions(self) -> Self::Additions;

    fn c_prop(self, prop_id: usize) -> Option<Prop>;

    fn t_prop(self, prop_id: usize) -> Self::TProps;
}
