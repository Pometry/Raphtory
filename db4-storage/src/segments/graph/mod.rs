pub mod entry;
pub mod segment;

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use parking_lot::RwLock;
use std::path::Path;
use crate::api::graph::GraphSegmentOps;
use crate::error::StorageError;
use crate::segments::graph::segment::MemGraphSegment;
use crate::segments::graph::entry::MemGraphEntry;
use parking_lot::{RwLockReadGuard, RwLockWriteGuard};

/// `GraphSegmentView` manages graph temporal properties and graph metadata
/// (constant properties). Reads / writes are always served from the in-memory segment.
#[derive(Debug)]
pub struct GraphSegmentView {
    /// In-memory segment that contains the latest graph properties
    /// and graph metadata writes.
    head: Arc<RwLock<MemGraphSegment>>,

    /// Estimated size of the segment in bytes.
    est_size: AtomicUsize,
}

impl GraphSegmentOps for GraphSegmentView {
    type Entry<'a> = MemGraphEntry<'a>;

    fn new(_path: Option<&Path>) -> Self {
        Self {
            head: Arc::new(RwLock::new(MemGraphSegment::new())),
            est_size: AtomicUsize::new(0),
        }
    }

    fn load(_path: impl AsRef<Path>) -> Result<Self, StorageError> {
        todo!()
    }

    fn head(&self) -> RwLockReadGuard<'_, MemGraphSegment> {
        self.head.read()
    }

    fn head_mut(&self) -> RwLockWriteGuard<'_, MemGraphSegment> {
        self.head.write()
    }

    fn entry(&self) -> Self::Entry<'_> {
        let head = self.head.read();

        MemGraphEntry::new(head)
    }
}
