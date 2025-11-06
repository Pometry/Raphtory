use crate::segments::graph::segment::MemGraphSegment;
use crate::api::graph::GraphEntryOps;
use raphtory_core::entities::properties::tprop::TPropCell;
use raphtory_api::core::entities::properties::prop::Prop;
use parking_lot::RwLockReadGuard;

/// A borrowed view enabling read operations on an in-memory graph segment.
pub struct MemGraphEntry<'a> {
    mem_segment: RwLockReadGuard<'a, MemGraphSegment>,
}

impl<'a> MemGraphEntry<'a> {
    pub fn new(mem_segment: RwLockReadGuard<'a, MemGraphSegment>) -> Self {
        Self { mem_segment }
    }
}

impl<'a> GraphEntryOps<'a> for MemGraphEntry<'a> {
    fn get_temporal_prop(&self, prop_id: usize) -> Option<TPropCell<'_>> {
        let layer = &self.mem_segment.layers().get(MemGraphSegment::LAYER).expect("GraphSegment has no layers");

        layer.t_prop(MemGraphSegment::ROW, prop_id)
    }

    fn get_metadata(&self, prop_id: usize) -> Option<Prop> {
        let layer = &self.mem_segment.layers().get(MemGraphSegment::LAYER).expect("GraphSegment has no layers");

        layer.c_prop(MemGraphSegment::ROW, prop_id)
    }
}
