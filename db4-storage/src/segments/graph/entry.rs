use crate::segments::graph::segment::MemGraphSegment;
use crate::api::graph::GraphEntryOps;
use raphtory_core::entities::properties::tprop::TPropCell;
use raphtory_api::core::entities::properties::prop::Prop;
use parking_lot::RwLockReadGuard;

/// A borrowed view enabling read operations on an in-memory graph segment.
pub struct MemGraphEntry<'a> {
    mem: RwLockReadGuard<'a, MemGraphSegment>,
}

impl<'a> MemGraphEntry<'a> {
    pub fn new(mem: RwLockReadGuard<'a, MemGraphSegment>) -> Self {
        Self { mem }
    }
}

impl<'a> GraphEntryOps<'a> for MemGraphEntry<'a> {
    type TProp = TPropCell<'a>;

    fn get_temporal_prop(&'a self, prop_id: usize) -> Option<Self::TProp> {
        self.mem.get_temporal_prop(prop_id)
    }

    fn get_metadata(&'a self, prop_id: usize) -> Option<Prop> {
        self.mem.get_metadata(prop_id)
    }
}
