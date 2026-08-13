use crate::{
    api::graph_props::GraphPropSegmentOps, error::StorageError,
    segments::graph_prop::segment::MemGraphPropSegment, wal::LSN,
};
use parking_lot::{RawRwLock, lock_api::ArcRwLockWriteGuard};
use raphtory_api::core::entities::properties::prop::Prop;
use raphtory_core::storage::timeindex::AsTime;
use std::{ops::DerefMut, path::Path, sync::Arc};

pub struct LockedGraphPropPage<GS: GraphPropSegmentOps> {
    page: Arc<GS>,
    lock: ArcRwLockWriteGuard<RawRwLock, MemGraphPropSegment>,
}

impl<GS: GraphPropSegmentOps> LockedGraphPropPage<GS> {
    pub fn new(page: Arc<GS>, lock: ArcRwLockWriteGuard<RawRwLock, MemGraphPropSegment>) -> Self {
        Self { page, lock }
    }

    pub fn segment(&self) -> &GS {
        self.page.as_ref()
    }

    /// Add temporal properties to the graph
    pub fn add_properties<T: AsTime>(
        &mut self,
        t: T,
        props: impl IntoIterator<Item = (usize, Prop)>,
    ) {
        let add = self.lock.add_properties(t, props);

        self.page.increment_est_size(add);
        self.page.set_dirty(true);
    }

    /// Add metadata (constant properties) to the graph
    pub fn add_metadata(&mut self, props: impl IntoIterator<Item = (usize, Prop)>) {
        self.update_metadata(props);
    }

    /// Update metadata (constant properties) on the graph
    pub fn update_metadata(&mut self, props: impl IntoIterator<Item = (usize, Prop)>) {
        let add = self.lock.update_metadata(props);

        self.page.increment_est_size(add);
        self.page.set_dirty(true);
    }

    pub fn set_lsn(&mut self, lsn: LSN) {
        self.lock.set_lsn(lsn);
    }
}

impl<GS: GraphPropSegmentOps> Drop for LockedGraphPropPage<GS> {
    fn drop(&mut self) {
        self.page
            .notify_write(self.lock.deref_mut())
            .expect("Failed to persist graph props page");
    }
}

pub struct WriteLockedGraphPropPages<GS: GraphPropSegmentOps> {
    writer: LockedGraphPropPage<GS>,
}

impl<GS: GraphPropSegmentOps> WriteLockedGraphPropPages<GS> {
    pub fn new(writer: LockedGraphPropPage<GS>) -> Self {
        Self { writer }
    }

    pub fn writer(&mut self) -> &mut LockedGraphPropPage<GS> {
        &mut self.writer
    }

    pub fn flush(&mut self) -> Result<(), StorageError> {
        let LockedGraphPropPage { page, lock } = &mut self.writer;
        page.flush(lock.deref_mut())
    }

    pub fn copy_to(&self, dst: &Path) -> Result<(), StorageError> {
        self.writer.segment().copy_to(dst)
    }
}
