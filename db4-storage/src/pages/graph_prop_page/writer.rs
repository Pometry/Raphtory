use crate::{
    api::graph_props::GraphPropSegmentOps, error::StorageError,
    segments::graph_prop::segment::MemGraphPropSegment, wal::LSN,
};
use parking_lot::RwLockWriteGuard;
use raphtory_api::core::entities::properties::prop::{AsPropEntry, AsPropRef, Prop};
use raphtory_core::storage::timeindex::AsTime;

/// Provides mutable access to a graph segment. Holds an exclusive write lock
/// on the in-memory segment for the duration of its lifetime.
pub struct GraphPropWriter<'a, GS: GraphPropSegmentOps> {
    pub mem_segment: RwLockWriteGuard<'a, MemGraphPropSegment>,
    pub graph_props: &'a GS,
}

impl<'a, GS: GraphPropSegmentOps> GraphPropWriter<'a, GS> {
    pub fn new(
        graph_props: &'a GS,
        mem_segment: RwLockWriteGuard<'a, MemGraphPropSegment>,
    ) -> Self {
        Self {
            mem_segment,
            graph_props,
        }
    }

    pub fn add_properties<T: AsTime, P: AsPropRef>(
        &mut self,
        t: T,
        props: impl IntoIterator<Item = (usize, P)>,
    ) {
        let add = self.mem_segment.add_properties(t, props);

        self.graph_props.increment_est_size(add);
        self.graph_props.set_dirty(true);
    }

    pub fn check_metadata_inner<P: AsPropEntry>(
        &self,
        props: impl Iterator<Item = P>,
    ) -> Result<(), StorageError> {
        for entry in props {
            let (prop_id, prop) = entry.as_prop_entry();
            self.mem_segment.check_metadata(prop_id, prop.clone())?;
            self.graph_props.check_metadata_immut(prop_id, prop)?;
        }
        Ok(())
    }

    pub fn update_metadata_inner(&mut self, props: impl IntoIterator<Item = (usize, Prop)>) {
        let add = self.mem_segment.update_metadata(props);
        self.graph_props.increment_est_size(add);
        self.graph_props.set_dirty(true);
    }

    pub fn set_lsn(&mut self, lsn: LSN) {
        self.mem_segment.set_lsn(lsn);
    }
}

impl<GS: GraphPropSegmentOps> Drop for GraphPropWriter<'_, GS> {
    fn drop(&mut self) {
        self.graph_props
            .notify_write(&mut self.mem_segment)
            .expect("Failed to persist node page");
    }
}
