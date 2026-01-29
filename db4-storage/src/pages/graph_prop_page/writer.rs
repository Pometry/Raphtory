use crate::{
    api::graph_props::GraphPropSegmentOps, error::StorageError,
    segments::graph_prop::segment::MemGraphPropSegment,
};
use parking_lot::RwLockWriteGuard;
use raphtory_api::core::entities::properties::prop::Prop;
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

    pub fn add_properties<T: AsTime>(
        &mut self,
        t: T,
        props: impl IntoIterator<Item = (usize, Prop)>,
    ) {
        let add = self.mem_segment.add_properties(t, props);

        self.graph_props.increment_est_size(add);
        self.graph_props.set_dirty(true);
    }

    pub fn check_metadata(&self, props: &[(usize, Prop)]) -> Result<(), StorageError> {
        self.mem_segment.check_metadata(props)
    }

    pub fn update_metadata(&mut self, props: impl IntoIterator<Item = (usize, Prop)>) {
        let add = self.mem_segment.update_metadata(props);

        self.graph_props.increment_est_size(add);
        self.graph_props.set_dirty(true);
    }
}

impl<GS: GraphPropSegmentOps> Drop for GraphPropWriter<'_, GS> {
    fn drop(&mut self) {
        self.graph_props
            .notify_write(&mut self.mem_segment)
            .expect("Failed to persist node page");
    }
}
