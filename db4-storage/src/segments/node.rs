use either::Either;
use parking_lot::lock_api::ArcRwLockReadGuard;
use raphtory_api::core::{
    Direction,
    entities::{
        EID, VID,
        properties::{meta::Meta, prop::Prop},
    },
};
use raphtory_core::{
    entities::{ELID, nodes::structure::adj::Adj},
    storage::timeindex::{AsTime, TimeIndexEntry},
};
use std::{
    ops::{Deref, DerefMut},
    sync::{Arc, atomic, atomic::AtomicUsize},
};

use super::{HasRow, SegmentContainer};
use crate::{LocalPOS, NodeSegmentOps, error::DBV4Error, segments::node_entry::MemNodeEntry};

#[derive(Debug)]
pub struct MemNodeSegment {
    inner: SegmentContainer<AdjEntry>,
}

#[derive(Debug, Default)]
pub struct AdjEntry {
    row: usize,
    adj: Adj,
}

impl AdjEntry {
    pub fn degree(&self, d: Direction) -> usize {
        self.adj.degree(d)
    }

    pub fn edges(&self, d: Direction) -> impl Iterator<Item = (VID, EID)> + '_ {
        match d {
            Direction::IN => Either::Left(self.adj.inb_iter()),
            Direction::OUT => Either::Right(self.adj.out_iter()),
            Direction::BOTH => panic!("AdjEntry::edges: BOTH direction is not supported"),
        }
    }
}

impl HasRow for AdjEntry {
    fn row(&self) -> usize {
        self.row
    }

    fn row_mut(&mut self) -> &mut usize {
        &mut self.row
    }
}

impl AsRef<SegmentContainer<AdjEntry>> for MemNodeSegment {
    fn as_ref(&self) -> &SegmentContainer<AdjEntry> {
        &self.inner
    }
}

impl AsMut<SegmentContainer<AdjEntry>> for MemNodeSegment {
    fn as_mut(&mut self) -> &mut SegmentContainer<AdjEntry> {
        &mut self.inner
    }
}

impl MemNodeSegment {
    #[inline(always)]
    fn get_adj(&self, n: LocalPOS) -> Option<&Adj> {
        self.inner.get(&n).map(|AdjEntry { adj, .. }| adj)
    }

    pub fn has_node(&self, n: LocalPOS) -> bool {
        self.inner.get(&n).is_some()
    }

    // pub(crate) fn contains_out(&self, n: LocalPOS, dst: VID) -> bool {
    //     self.get_out_edge(n, dst).is_some()
    // }

    // pub(crate) fn contains_in(&self, n: LocalPOS, src: VID) -> bool {
    //     self.get_in_edge(n, src).is_some()
    // }

    pub fn get_out_edge(&self, n: LocalPOS, dst: VID) -> Option<EID> {
        self.get_adj(n)
            .and_then(|adj| adj.get_edge(dst, Direction::OUT))
    }

    pub fn get_inb_edge(&self, n: LocalPOS, src: VID) -> Option<EID> {
        self.get_adj(n)
            .and_then(|adj| adj.get_edge(src, Direction::IN))
    }

    pub fn out_edges(&self, n: LocalPOS) -> impl Iterator<Item = (VID, EID)> + '_ {
        self.get_adj(n).into_iter().flat_map(|adj| adj.out_iter())
    }

    pub fn inb_edges(&self, n: LocalPOS) -> impl Iterator<Item = (VID, EID)> + '_ {
        self.get_adj(n).into_iter().flat_map(|adj| adj.inb_iter())
    }
}

impl MemNodeSegment {
    pub fn new(segment_id: usize, max_page_len: usize, meta: Arc<Meta>) -> Self {
        Self {
            inner: SegmentContainer::new(segment_id, max_page_len, meta),
        }
    }

    pub fn add_outbound_edge<T: AsTime>(
        &mut self,
        t: Option<T>,
        src_pos: LocalPOS,
        dst: impl Into<VID>,
        e_id: impl Into<ELID>,
    ) -> bool {
        let dst = dst.into();
        let e_id = e_id.into();
        let add_out = self.inner.reserve_local_row(src_pos).map_either(
            |row| {
                row.adj.add_edge_out(dst, e_id.edge);
                row.row()
            },
            |row| {
                row.adj.add_edge_out(dst, e_id.edge);
                row.row()
            },
        );

        let new_entry = add_out.is_right();
        if let Some(t) = t {
            self.update_timestamp_inner(t, add_out.either(|a| a, |a| a), e_id);
        }
        new_entry
    }

    pub fn add_inbound_edge(
        &mut self,
        t: Option<impl AsTime>,
        dst_pos: impl Into<LocalPOS>,
        src: impl Into<VID>,
        e_id: impl Into<ELID>,
    ) -> bool {
        let src = src.into();
        let e_id = e_id.into();
        let dst_pos = dst_pos.into();

        let add_in = self.inner.reserve_local_row(dst_pos).map_either(
            |row| {
                row.adj.add_edge_into(src, e_id.edge);
                row.row()
            },
            |row| {
                row.adj.add_edge_into(src, e_id.edge);
                row.row()
            },
        );
        let new_entry = add_in.is_right();
        if let Some(t) = t {
            self.update_timestamp_inner(t, add_in.either(|a| a, |a| a), e_id);
        }
        new_entry
    }

    fn update_timestamp_inner<T: AsTime>(&mut self, t: T, row: usize, e_id: ELID) {
        let mut prop_mut_entry = self.inner.properties_mut().get_mut_entry(row);
        let ts = TimeIndexEntry::new(t.t(), t.i());

        prop_mut_entry.append_edge_ts(ts, e_id);
    }

    pub fn update_timestamp<T: AsTime>(&mut self, t: T, node_pos: LocalPOS, e_id: ELID) {
        let row = self
            .inner
            .reserve_local_row(node_pos)
            .either(|a| a.row, |a| a.row);
        self.update_timestamp_inner(t, row, e_id);
    }

    pub fn add_props<T: AsTime>(
        &mut self,
        t: T,
        node_pos: LocalPOS,
        props: impl IntoIterator<Item = (usize, Prop)>,
    ) {
        let row = self
            .inner
            .reserve_local_row(node_pos)
            .either(|a| a.row, |a| a.row);
        let mut prop_mut_entry = self.inner.properties_mut().get_mut_entry(row);
        let ts = TimeIndexEntry::new(t.t(), t.i());
        prop_mut_entry.append_t_props(ts, props);
    }

    pub fn update_c_props(
        &mut self,
        node_pos: LocalPOS,
        props: impl IntoIterator<Item = (usize, Prop)>,
    ) {
        let row = self
            .inner
            .reserve_local_row(node_pos)
            .either(|a| a.row, |a| a.row);
        let mut prop_mut_entry = self.inner.properties_mut().get_mut_entry(row);
        prop_mut_entry.append_const_props(props);
    }
}

pub struct NodeSegmentView<EXT = ()> {
    inner: Arc<parking_lot::RwLock<MemNodeSegment>>,
    segment_id: usize,
    num_nodes: AtomicUsize,
    _ext: EXT,
}

impl NodeSegmentOps for NodeSegmentView {
    type Extension = ();

    type Entry<'a> = MemNodeEntry<'a, parking_lot::RwLockReadGuard<'a, MemNodeSegment>>;

    fn latest(&self) -> Option<TimeIndexEntry> {
        self.head().as_ref().latest()
    }

    fn earliest(&self) -> Option<TimeIndexEntry> {
        self.head().as_ref().earliest()
    }

    fn t_len(&self) -> usize {
        self.head().as_ref().t_len()
    }

    fn load(
        _page_id: usize,
        _max_page_len: usize,
        _meta: Arc<Meta>,
        _path: impl AsRef<std::path::Path>,
        _ext: Self::Extension,
    ) -> Result<Self, DBV4Error>
    where
        Self: Sized,
    {
        todo!()
    }

    fn new(
        page_id: usize,
        max_page_len: usize,
        meta: Arc<Meta>,
        _path: impl AsRef<std::path::Path>,
        _ext: Self::Extension,
    ) -> Self {
        Self {
            inner: parking_lot::RwLock::new(MemNodeSegment::new(page_id, max_page_len, meta)).into(),
            segment_id: page_id,
            num_nodes: AtomicUsize::new(0),
            _ext: (),
        }
    }

    fn segment_id(&self) -> usize {
        self.segment_id
    }

    fn head(&self) -> parking_lot::RwLockReadGuard<MemNodeSegment> {
        self.inner.read()
    }

    fn head_arc(&self) -> ArcRwLockReadGuard<parking_lot::RawRwLock, MemNodeSegment> {
        self.inner.read_arc_recursive()
    }

    fn head_mut(&self) -> parking_lot::RwLockWriteGuard<MemNodeSegment> {
        self.inner.write()
    }

    fn num_nodes(&self) -> usize {
        self.num_nodes.load(atomic::Ordering::Relaxed)
    }

    fn increment_num_nodes(&self) -> usize {
        self.num_nodes.fetch_add(1, atomic::Ordering::Relaxed)
    }

    fn notify_write(
        &self,
        _head_lock: impl DerefMut<Target = MemNodeSegment>,
    ) -> Result<(), DBV4Error> {
        Ok(())
    }

    fn check_node(&self, _pos: LocalPOS) -> bool {
        false
    }

    fn get_out_edge(
        &self,
        pos: LocalPOS,
        dst: impl Into<VID>,
        locked_head: impl Deref<Target = MemNodeSegment>,
    ) -> Option<EID> {
        locked_head.get_out_edge(pos, dst.into())
    }

    fn get_inb_edge(
        &self,
        pos: LocalPOS,
        src: impl Into<VID>,
        locked_head: impl Deref<Target = MemNodeSegment>,
    ) -> Option<EID> {
        locked_head.get_inb_edge(pos, src.into())
    }

    fn entry<'a>(&'a self, pos: impl Into<LocalPOS>) -> Self::Entry<'a> {
        let pos = pos.into();
        MemNodeEntry::new(pos, self.head())
    }
}
