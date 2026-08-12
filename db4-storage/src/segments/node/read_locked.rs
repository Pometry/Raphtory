use crate::{
    LocalPOS,
    api::nodes::ReadLockedNodeSegmentOps,
    segments::node::{entry::MemNodeRef, segment::MemNodeSegment},
};
use parking_lot::{RawRwLock, lock_api::ArcRwLockReadGuard};

#[derive(Debug)]
pub struct ReadLockedNodeSegmentView {
    inner: ArcRwLockReadGuard<RawRwLock, MemNodeSegment>,
    num_nodes: u32,
}

impl ReadLockedNodeSegmentView {
    pub(crate) fn new(
        inner: ArcRwLockReadGuard<RawRwLock, MemNodeSegment>,
        num_nodes: u32,
    ) -> Self {
        Self { inner, num_nodes }
    }
}

impl ReadLockedNodeSegmentOps for ReadLockedNodeSegmentView {
    type EntryRef<'a> = MemNodeRef<'a>;

    fn num_nodes(&self) -> u32 {
        self.num_nodes
    }

    fn entry_ref<'a>(&'a self, pos: impl Into<LocalPOS>) -> Self::EntryRef<'a> {
        let pos = pos.into();
        MemNodeRef::new(pos, &self.inner)
    }
}
