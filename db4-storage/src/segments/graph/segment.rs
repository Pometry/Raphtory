use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use crate::api::graph::GraphSegmentOps;
use crate::error::StorageError;
use crate::segments::graph::entry::MemGraphEntry;
use crate::segments::{HasRow, SegmentContainer};
use parking_lot::RwLock;
use raphtory_api::core::entities::properties::meta::Meta;
use raphtory_core::storage::timeindex::TimeIndexEntry;
use raphtory_api::core::entities::properties::prop::Prop;

/// In-memory segment that contains graph temporal properties and graph metadata.
#[derive(Debug)]
pub struct MemGraphSegment {
    /// Layers containing graph properties and metadata.
    layers: Vec<SegmentContainer<GraphSegmentEntry>>
}

/// A unit-like struct for use with `SegmentContainer`.
/// Graph properties and metadata are already stored in `SegmentContainer`,
/// hence this struct is empty.
#[derive(Debug, Default)]
pub struct GraphSegmentEntry;

impl HasRow for GraphSegmentEntry {
    fn row(&self) -> usize {
        todo!()
    }

    fn row_mut(&mut self) -> &mut usize {
        todo!()
    }
}

impl MemGraphSegment {
    /// Graph segments only have a single row.
    pub const ROW: usize = 0;

    /// Graph segments are currently only written to a single layer.
    pub const LAYER: usize = 0;

    pub fn new() -> Self {
        // Technically, these aren't used since there is always only one graph segment.
        let segment_id = 0;
        let max_page_len = 1;
        let meta = Arc::new(Meta::new_for_graph());

        Self {
            layers: vec![SegmentContainer::new(segment_id, max_page_len, meta)],
        }
    }

    pub fn layers(&self) -> &[SegmentContainer<GraphSegmentEntry>] {
        &self.layers
    }

    pub fn add_properties(&mut self, t: TimeIndexEntry, props: impl IntoIterator<Item = (usize, Prop)>) {
        let layer = &mut self.layers[Self::LAYER];

        layer.properties_mut().get_mut_entry(Self::ROW).append_t_props(t, props);
    }

    pub fn add_metadata(&mut self, props: impl IntoIterator<Item = (usize, Prop)>) {
        let layer = &mut self.layers[Self::LAYER];

        layer.properties_mut().get_mut_entry(Self::ROW).append_const_props(props);
    }

    pub fn update_metadata(&mut self, props: impl IntoIterator<Item = (usize, Prop)>) {
        let layer = &mut self.layers[Self::LAYER];

        layer.properties_mut().get_mut_entry(Self::ROW).append_const_props(props);
    }
}
