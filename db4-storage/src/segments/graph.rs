use raphtory_core::entities::properties::graph_meta::GraphMeta;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use crate::api::graph::{GraphSegmentOps, GraphPropOps};
use crate::error::StorageError;
use parking_lot::RwLock;
use raphtory_core::entities::properties::props::MetadataError;
use raphtory_core::storage::locked_view::LockedView;
use raphtory_core::entities::properties::tprop::{TProp, IllegalPropType};
use raphtory_core::storage::timeindex::TimeIndexEntry;
use raphtory_api::core::entities::properties::prop::Prop;
use crate::properties::Properties;

/// In-memory segment that contains graph temporal properties and graph metadata.
#[derive(Debug)]
pub struct MemGraphSegment {
    properties: Properties,
}

impl MemGraphSegment {
    pub fn new() -> Self {
        Self {
            properties: Properties::default(),
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

impl GraphPropOps for GraphSegmentView {
    fn get_temporal_prop(&self, prop_id: usize) -> Option<LockedView<'_, TProp>> {
        todo!()
    }

    fn add_prop(&self, t: TimeIndexEntry, prop_id: usize, prop: Prop) -> Result<(), IllegalPropType> {
        todo!()
    }

    fn get_metadata(&self, id: usize) -> Option<Prop> {
        todo!()
    }

    fn add_metadata(&self, prop_id: usize, prop: Prop) -> Result<(), MetadataError> {
        todo!()
    }

    fn update_metadata(&self, prop_id: usize, prop: Prop) {
        todo!()
    }
}
