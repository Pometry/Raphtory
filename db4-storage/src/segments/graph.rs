use raphtory_core::entities::properties::graph_meta::GraphMeta;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use crate::api::graph::{GraphSegmentOps, GraphPropOps};
use crate::error::StorageError;
use crate::segments::{HasRow, SegmentContainer};
use crate::LocalPOS;
use parking_lot::RwLock;
use raphtory_core::entities::properties::props::MetadataError;
use raphtory_core::storage::locked_view::LockedView;
use raphtory_core::entities::properties::tprop::{IllegalPropType, TProp, TPropCell};
use raphtory_core::storage::timeindex::TimeIndexEntry;
use raphtory_api::core::entities::properties::prop::Prop;
use crate::properties::Properties;
use raphtory_api::core::entities::properties::meta::Meta;

/// In-memory segment that contains graph temporal properties and graph metadata.
#[derive(Debug)]
pub struct MemGraphSegment {
    /// Layerss containing graph properties and metadata.
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
}

/// `GraphSegmentView` manages graph temporal properties and graph metadata
/// (constant properties). Reads / writes are always served from the in-memory segment.
#[derive(Debug)]
pub struct GraphSegmentView {
    /// In-memory segment that contains the latest graph properties
    /// and graph metadata writes.
    head: Arc<RwLock<MemGraphSegment>>,

    /// Estimated size of the segment in bytes.
    est_size: AtomicUsize,
}

impl GraphSegmentOps for GraphSegmentView {
    fn new() -> Self {
        Self {
            head: Arc::new(RwLock::new(MemGraphSegment::new())),
            est_size: AtomicUsize::new(0),
        }
    }

    fn load(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        todo!()
    }
}

impl<'a> GraphPropOps for &'a GraphSegmentView {
    fn get_temporal_prop(&self, prop_id: usize) -> Option<TPropCell<'_>> {
        todo!()
    }

    fn get_metadata(&self, id: usize) -> Option<Prop> {
        todo!()
    }
}
