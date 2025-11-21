pub mod entry;
pub mod segment;

use crate::api::graph::GraphPropOps;
use crate::error::StorageError;
use crate::persist::strategy::NoOpStrategy;
use crate::segments::graph::entry::MemGraphEntry;
use crate::segments::graph::segment::MemGraphProps;
use parking_lot::RwLock;
use parking_lot::{RwLockReadGuard, RwLockWriteGuard};
use raphtory_api::core::entities::properties::meta::Meta;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

/// `GraphSegmentView` manages graph temporal properties and graph metadata
/// (constant properties). Reads / writes are always served from the in-memory segment.
#[derive(Debug)]
pub struct GraphSegmentView {
    /// In-memory segment that contains the latest graph properties
    /// and graph metadata writes.
    head: Arc<RwLock<MemGraphProps>>,

    /// Estimated size of the segment in bytes.
    est_size: AtomicUsize,
}

impl GraphPropOps for GraphSegmentView {
    type Extension = NoOpStrategy;

    type Entry<'a> = MemGraphEntry<'a>;

    fn new(meta: Arc<Meta>, _path: Option<&Path>, _ext: Self::Extension) -> Self {
        Self {
            head: Arc::new(RwLock::new(MemGraphProps::new_with_metadata(meta))),
            est_size: AtomicUsize::new(0),
        }
    }

    fn load(
        meta: Arc<Meta>,
        _path: impl AsRef<Path>,
        _ext: Self::Extension,
    ) -> Result<Self, StorageError> {
        todo!()
    }

    fn head(&self) -> RwLockReadGuard<'_, MemGraphProps> {
        self.head.read()
    }

    fn head_mut(&self) -> RwLockWriteGuard<'_, MemGraphProps> {
        self.head.write()
    }

    fn entry(&self) -> Self::Entry<'_> {
        let head = self.head.read();

        MemGraphEntry::new(head)
    }

    fn increment_est_size(&self, size: usize) {
        self.est_size
            .fetch_add(size, std::sync::atomic::Ordering::Relaxed);
    }

    fn est_size(&self) -> usize {
        self.est_size.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn notify_write(
        &self,
        _mem_segment: &mut RwLockWriteGuard<'_, MemGraphProps>,
    ) -> Result<(), StorageError> {
        Ok(())
    }
}
