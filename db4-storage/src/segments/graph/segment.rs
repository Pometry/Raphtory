use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use crate::api::graph::GraphSegmentOps;
use crate::error::StorageError;
use crate::segments::graph::entry::MemGraphEntry;
use crate::segments::{HasRow, SegmentContainer};
use parking_lot::RwLock;
use raphtory_api::core::entities::properties::meta::Meta;

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
    pub fn new() -> Self {
        // Technically, these aren't used since there is always only one graph segment.
        let segment_id = 0;
        let max_page_len = 0;
        let meta = Arc::new(Meta::new_for_graph());

        Self {
            layers: vec![SegmentContainer::new(segment_id, max_page_len, meta)],
        }
    }

    pub fn layers(&self) -> &[SegmentContainer<GraphSegmentEntry>] {
        &self.layers
    }
}
