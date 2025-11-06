use crate::{
    segments::graph::segment::MemGraphSegment,
};
use raphtory_core::storage::timeindex::TimeIndexEntry;
use raphtory_api::core::entities::properties::prop::Prop;
use parking_lot::RwLockWriteGuard;

/// Provides mutable access to a graph segment. Holds an exclusive write lock
/// on the in-memory segment for the duration of its lifetime.
pub struct GraphWriter<'a> {
    pub mem_segment: RwLockWriteGuard<'a, MemGraphSegment>,
}

impl<'a> GraphWriter<'a> {
    pub fn new(mem_segment: RwLockWriteGuard<'a, MemGraphSegment>) -> Self {
        Self { mem_segment }
    }

    pub fn add_properties(&mut self, t: TimeIndexEntry, props: impl IntoIterator<Item = (usize, Prop)>) {
        self.mem_segment.add_properties(t, props)
    }

    pub fn add_metadata(&mut self, props: impl IntoIterator<Item = (usize, Prop)>) {
        self.mem_segment.add_metadata(props)
    }

    pub fn update_metadata(&mut self, props: impl IntoIterator<Item = (usize, Prop)>) {
        self.mem_segment.update_metadata(props)
    }
}
