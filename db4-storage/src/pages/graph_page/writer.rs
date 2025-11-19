use crate::{api::graph::GraphSegmentOps, segments::graph::segment::MemGraphSegment};
use parking_lot::RwLockWriteGuard;
use raphtory_api::core::entities::properties::prop::Prop;
use raphtory_core::storage::timeindex::TimeIndexEntry;

/// Provides mutable access to a graph segment. Holds an exclusive write lock
/// on the in-memory segment for the duration of its lifetime.
pub struct GraphWriter<'a, GS: GraphSegmentOps> {
    pub mem_segment: RwLockWriteGuard<'a, MemGraphSegment>,
    pub graph_props: &'a GS,
}

impl<'a, GS: GraphSegmentOps> GraphWriter<'a, GS> {
    pub fn new(graph_props: &'a GS, mem_segment: RwLockWriteGuard<'a, MemGraphSegment>) -> Self {
        Self {
            mem_segment,
            graph_props,
        }
    }

    pub fn add_properties(
        &mut self,
        t: TimeIndexEntry,
        props: impl IntoIterator<Item = (usize, Prop)>,
        lsn: u64,
    ) {
        let add = self.mem_segment.add_properties(t, props);
        self.mem_segment.as_mut()[MemGraphSegment::LAYER].set_lsn(lsn);
        self.graph_props.increment_est_size(add);
    }

    pub fn add_metadata(&mut self, props: impl IntoIterator<Item = (usize, Prop)>, lsn: u64) {
        self.update_metadata(props, lsn);
    }

    pub fn update_metadata(&mut self, props: impl IntoIterator<Item = (usize, Prop)>, lsn: u64) {
        let add = self.mem_segment.update_metadata(props);
        self.mem_segment.as_mut()[MemGraphSegment::LAYER].set_lsn(lsn);
        self.graph_props.increment_est_size(add);
    }
}
