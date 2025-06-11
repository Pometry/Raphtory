use std::{
    ops::{Deref, DerefMut},
    sync::{
        Arc,
        atomic::{self, AtomicUsize},
    },
};

use raphtory_api::core::entities::{
    VID,
    properties::{meta::Meta, prop::Prop},
};
use raphtory_core::storage::timeindex::{AsTime, TimeIndexEntry};

use crate::{EdgeSegmentOps, LocalPOS, error::DBV4Error, properties::PropMutEntry};

use super::{HasRow, SegmentContainer, edge_entry::MemEdgeEntry};

#[derive(Debug, Default)]
pub struct MemPageEntry {
    pub src: VID,
    pub dst: VID,
    pub row: usize,
}

impl HasRow for MemPageEntry {
    fn row(&self) -> usize {
        self.row
    }

    fn row_mut(&mut self) -> &mut usize {
        &mut self.row
    }
}

#[derive(Debug)]
pub struct MemEdgeSegment {
    inner: SegmentContainer<MemPageEntry>,
}

impl AsRef<SegmentContainer<MemPageEntry>> for MemEdgeSegment {
    fn as_ref(&self) -> &SegmentContainer<MemPageEntry> {
        &self.inner
    }
}

impl AsMut<SegmentContainer<MemPageEntry>> for MemEdgeSegment {
    fn as_mut(&mut self) -> &mut SegmentContainer<MemPageEntry> {
        &mut self.inner
    }
}

impl MemEdgeSegment {
    pub fn new(segment_id: usize, max_page_len: usize, meta: Arc<Meta>) -> Self {
        Self {
            inner: SegmentContainer::new(segment_id, max_page_len, meta),
        }
    }

    pub fn get_edge(&self, edge_pos: impl Into<LocalPOS>) -> Option<(VID, VID)> {
        let edge_pos = edge_pos.into();
        self.inner
            .get(&edge_pos)
            .map(|entry| (entry.src, entry.dst))
    }

    pub fn insert_edge_internal<T: AsTime>(
        &mut self,
        t: T,
        edge_pos: impl Into<LocalPOS>,
        src: impl Into<VID>,
        dst: impl Into<VID>,
        props: impl IntoIterator<Item = (usize, Prop)>,
    ) {
        let edge_pos = edge_pos.into();
        let src = src.into();
        let dst = dst.into();
        let local_row = self.reserve_local_row(edge_pos, src, dst);

        let mut prop_entry: PropMutEntry<'_> = self.inner.properties_mut().get_mut_entry(local_row);
        let ts = TimeIndexEntry::new(t.t(), t.i());
        prop_entry.append_t_props(ts, props)
    }

    pub fn insert_static_edge_internal(
        &mut self,
        edge_pos: LocalPOS,
        src: impl Into<VID>,
        dst: impl Into<VID>,
    ) {
        let src = src.into();
        let dst = dst.into();
        self.reserve_local_row(edge_pos, src, dst);
    }

    fn reserve_local_row(
        &mut self,
        edge_pos: LocalPOS,
        src: impl Into<VID>,
        dst: impl Into<VID>,
    ) -> usize {
        let src = src.into();
        let dst = dst.into();
        let row = self.inner.reserve_local_row(edge_pos).map_either(
            |row| {
                row.src = src;
                row.dst = dst;
                row.row()
            },
            |row| {
                row.src = src;
                row.dst = dst;
                row.row()
            },
        );
        row.either(|a| a, |a| a)
    }

    pub fn update_const_properties(
        &mut self,
        edge_pos: impl Into<LocalPOS>,
        src: impl Into<VID>,
        dst: impl Into<VID>,
        props: impl IntoIterator<Item = (usize, Prop)>,
    ) {
        let edge_pos = edge_pos.into();
        let src = src.into();
        let dst = dst.into();
        let local_row = self.reserve_local_row(edge_pos, src, dst);
        let mut prop_entry: PropMutEntry<'_> = self.inner.properties_mut().get_mut_entry(local_row);
        prop_entry.append_const_props(props)
    }

    pub fn insert_edge(&mut self, edge_pos: LocalPOS, src: impl Into<VID>, dst: impl Into<VID>) {
        self.insert_edge_internal(0, edge_pos, src, dst, []);
    }

    pub fn contains_edge(&self, edge_pos: LocalPOS) -> bool {
        self.inner
            .items()
            .get::<usize>(edge_pos.0)
            .map(|b| *b)
            .unwrap_or_default()
    }
}

pub struct EdgeSegmentView<EXT = ()> {
    segment: parking_lot::RwLock<MemEdgeSegment>,
    segment_id: usize,
    num_edges: AtomicUsize,
    _ext: EXT,
}

impl EdgeSegmentOps for EdgeSegmentView {
    type Extension = ();

    type Entry<'a> = MemEdgeEntry<'a, parking_lot::RwLockReadGuard<'a, MemEdgeSegment>>;

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
            segment: parking_lot::RwLock::new(MemEdgeSegment::new(page_id, max_page_len, meta)),
            segment_id: page_id,
            num_edges: AtomicUsize::new(0),
            _ext: (),
        }
    }

    fn segment_id(&self) -> usize {
        self.segment_id
    }

    fn num_edges(&self) -> usize {
        self.num_edges.load(atomic::Ordering::Relaxed)
    }

    fn head(&self) -> parking_lot::RwLockReadGuard<MemEdgeSegment> {
        self.segment.read_recursive()
    }

    fn head_mut(&self) -> parking_lot::RwLockWriteGuard<MemEdgeSegment> {
        self.segment.write()
    }

    fn try_head_mut(&self) -> Option<parking_lot::RwLockWriteGuard<MemEdgeSegment>> {
        self.segment.try_write()
    }

    fn notify_write(
        &self,
        _head_lock: impl DerefMut<Target = MemEdgeSegment>,
    ) -> Result<(), DBV4Error> {
        Ok(())
    }

    fn increment_num_edges(&self) -> usize {
        self.num_edges.fetch_add(1, atomic::Ordering::Relaxed)
    }

    fn contains_edge(
        &self,
        edge_pos: LocalPOS,
        locked_head: impl Deref<Target = MemEdgeSegment>,
    ) -> bool {
        locked_head.contains_edge(edge_pos)
    }

    fn get_edge(
        &self,
        edge_pos: LocalPOS,
        locked_head: impl Deref<Target = MemEdgeSegment>,
    ) -> Option<(VID, VID)> {
        locked_head.get_edge(edge_pos)
    }

    fn entry<'a, LP: Into<LocalPOS>>(&'a self, edge_pos: LP) -> Self::Entry<'a> {
        let edge_pos = edge_pos.into();
        MemEdgeEntry::new(edge_pos, self.head())
    }
}
