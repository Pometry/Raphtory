use crate::segments::graph::MemGraphSegment;
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

impl GraphEntryOps for MemGraphEntry<'_> {
    fn get_temporal_prop(&self, prop_id: usize) -> Option<TPropCell<'_>> {
        let row = 0; // GraphSegment has only one row

        // TODO: handle multiple layers
        let layer = &self.mem.layers().get(0).expect("GraphSegment has no layers");

        layer.t_prop(row, prop_id)
    }

    fn get_metadata(&self, prop_id: usize) -> Option<Prop> {
        let row = 0; // GraphSegment has only one row

        // TODO: handle multiple layers
        let layer = &self.mem.layers().get(0).expect("GraphSegment has no layers");

        layer.c_prop(row, prop_id)
    }
}
