use crate::{
    api::graph_props::GraphPropSegmentOps, segments::graph_prop::segment::MemGraphPropSegment,
};
use parking_lot::RwLockWriteGuard;
use raphtory_api::core::entities::properties::prop::Prop;
use raphtory_core::storage::timeindex::AsTime;

pub struct LockedGraphPropPage<'a, GS: GraphPropSegmentOps> {
    page: &'a GS,
    lock: RwLockWriteGuard<'a, MemGraphPropSegment>,
}

impl<'a, GS: GraphPropSegmentOps> LockedGraphPropPage<'a, GS> {
    pub fn new(page: &'a GS, lock: RwLockWriteGuard<'a, MemGraphPropSegment>) -> Self {
        Self { page, lock }
    }

    pub fn segment(&self) -> &GS {
        self.page
    }

    /// Add temporal properties to the graph
    pub fn add_properties<T: AsTime>(
        &mut self,
        t: T,
        props: impl IntoIterator<Item = (usize, Prop)>,
        lsn: u64,
    ) {
        let add = self.lock.add_properties(t, props);
        self.lock.layers_mut()[MemGraphPropSegment::DEFAULT_LAYER].set_lsn(lsn);

        self.page.increment_est_size(add);
        self.page.mark_dirty();
    }

    /// Add metadata (constant properties) to the graph
    pub fn add_metadata(&mut self, props: impl IntoIterator<Item = (usize, Prop)>, lsn: u64) {
        self.update_metadata(props, lsn);
    }

    /// Update metadata (constant properties) on the graph
    pub fn update_metadata(&mut self, props: impl IntoIterator<Item = (usize, Prop)>, lsn: u64) {
        let add = self.lock.update_metadata(props);
        self.lock.layers_mut()[MemGraphPropSegment::DEFAULT_LAYER].set_lsn(lsn);

        self.page.increment_est_size(add);
        self.page.mark_dirty();
    }
}

impl<GS: GraphPropSegmentOps> Drop for LockedGraphPropPage<'_, GS> {
    fn drop(&mut self) {
        self.page
            .notify_write(&mut self.lock)
            .expect("Failed to persist graph props page");
    }
}

pub struct WriteLockedGraphPropPages<'a, GS: GraphPropSegmentOps> {
    writer: Option<LockedGraphPropPage<'a, GS>>,
}

impl<GS: GraphPropSegmentOps> Default for WriteLockedGraphPropPages<'_, GS> {
    fn default() -> Self {
        Self { writer: None }
    }
}

impl<'a, GS: GraphPropSegmentOps> WriteLockedGraphPropPages<'a, GS> {
    pub fn new(writer: LockedGraphPropPage<'a, GS>) -> Self {
        Self {
            writer: Some(writer),
        }
    }

    pub fn writer(&mut self) -> Option<&mut LockedGraphPropPage<'a, GS>> {
        self.writer.as_mut()
    }
}
