pub mod entry;
pub mod segment;

use crate::{
    api::graph_props::GraphPropSegmentOps,
    error::StorageError,
    persist::strategy::NoOpStrategy,
    segments::graph_prop::{entry::MemGraphPropEntry, segment::MemGraphPropSegment},
};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use raphtory_api::core::entities::properties::meta::Meta;
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

/// `GraphPropSegmentView` manages graph temporal properties and graph metadata
/// (constant properties). Reads / writes are always served from the in-memory segment.
#[derive(Debug)]
pub struct GraphPropSegmentView {
    /// In-memory segment that contains the latest graph properties
    /// and graph metadata writes.
    head: Arc<RwLock<MemGraphPropSegment>>,

    /// Estimated size of the segment in bytes.
    est_size: AtomicUsize,

    is_dirty: AtomicBool,
}

impl GraphPropSegmentOps for GraphPropSegmentView {
    type Extension = NoOpStrategy;

    type Entry<'a> = MemGraphPropEntry<'a>;

    fn new(meta: Arc<Meta>, _path: Option<&Path>, _ext: Self::Extension) -> Self {
        Self {
            head: Arc::new(RwLock::new(MemGraphPropSegment::new_with_meta(meta))),
            est_size: AtomicUsize::new(0),
            is_dirty: AtomicBool::new(false),
        }
    }

    fn load(
        meta: Arc<Meta>,
        _path: impl AsRef<Path>,
        _ext: Self::Extension,
    ) -> Result<Self, StorageError> {
        todo!()
    }

    fn head(&self) -> RwLockReadGuard<'_, MemGraphPropSegment> {
        self.head.read()
    }

    fn head_mut(&self) -> RwLockWriteGuard<'_, MemGraphPropSegment> {
        self.head.write()
    }

    fn entry(&self) -> Self::Entry<'_> {
        let head = self.head.read();

        MemGraphPropEntry::new(head)
    }

    fn increment_est_size(&self, size: usize) {
        self.est_size.fetch_add(size, Ordering::Relaxed);
    }

    fn est_size(&self) -> usize {
        self.est_size.load(Ordering::Relaxed)
    }

    fn mark_dirty(&self) {
        self.is_dirty.store(true, Ordering::Relaxed);
    }

    fn notify_write(
        &self,
        _mem_segment: &mut RwLockWriteGuard<'_, MemGraphPropSegment>,
    ) -> Result<(), StorageError> {
        Ok(())
    }
}
