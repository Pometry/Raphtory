use crate::{
    api::nodes::LockedNodeSegment,
    segments::node::{entry::MemNodeRef, segment::MemNodeSegment},
    LocalPOS,
};
use parking_lot::{
    lock_api::ArcRwLockReadGuard,
    RawRwLock,
};

#[derive(Debug)]
pub struct ArcLockedNodeSegmentView {
    inner: ArcRwLockReadGuard<RawRwLock, MemNodeSegment>,
    num_nodes: u32,
}

impl ArcLockedNodeSegmentView {
    pub(crate) fn new(
        inner: ArcRwLockReadGuard<RawRwLock, MemNodeSegment>,
        num_nodes: u32,
    ) -> Self {
        Self { inner, num_nodes }
    }
}

impl LockedNodeSegment for ArcLockedNodeSegmentView {
    type EntryRef<'a> = MemNodeRef<'a>;

    fn num_nodes(&self) -> u32 {
        self.num_nodes
    }

    fn entry_ref<'a>(&'a self, pos: impl Into<LocalPOS>) -> Self::EntryRef<'a> {
        let pos = pos.into();
        MemNodeRef::new(pos, &self.inner)
    }
}
