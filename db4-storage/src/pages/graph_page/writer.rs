use crate::{api::graph::GraphPropOps, segments::graph::segment::MemGraphProps};
use parking_lot::RwLockWriteGuard;
use raphtory_api::core::entities::properties::prop::Prop;
use raphtory_core::storage::timeindex::AsTime;

/// Provides mutable access to a graph segment. Holds an exclusive write lock
/// on the in-memory segment for the duration of its lifetime.
pub struct GraphWriter<'a, GS: GraphPropOps> {
    pub mem_segment: RwLockWriteGuard<'a, MemGraphProps>,
    pub graph_props: &'a GS,
}

impl<'a, GS: GraphPropOps> GraphWriter<'a, GS> {
    pub fn new(graph_props: &'a GS, mem_segment: RwLockWriteGuard<'a, MemGraphProps>) -> Self {
        Self {
            mem_segment,
            graph_props,
        }
    }

    pub fn add_properties<T: AsTime>(
        &mut self,
        t: T,
        props: impl IntoIterator<Item = (usize, Prop)>,
        lsn: u64,
    ) {
        let add = self.mem_segment.add_properties(t, props);
        self.mem_segment.layers_mut()[MemGraphProps::DEFAULT_LAYER].set_lsn(lsn);

        self.graph_props.increment_est_size(add);
        self.graph_props.mark_dirty();
    }

    pub fn add_metadata(&mut self, props: impl IntoIterator<Item = (usize, Prop)>, lsn: u64) {
        self.update_metadata(props, lsn);
    }

    pub fn update_metadata(&mut self, props: impl IntoIterator<Item = (usize, Prop)>, lsn: u64) {
        let add = self.mem_segment.update_metadata(props);
        self.mem_segment.layers_mut()[MemGraphProps::DEFAULT_LAYER].set_lsn(lsn);

        self.graph_props.increment_est_size(add);
        self.graph_props.mark_dirty();
    }
}

impl<GS: GraphPropOps> Drop for GraphWriter<'_, GS> {
    fn drop(&mut self) {
        self.graph_props
            .notify_write(&mut self.mem_segment)
            .expect("Failed to persist node page");
    }
}
