use raphtory_core::entities::properties::graph_meta::GraphMeta;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use crate::api::graph_props::GraphPropSegmentOps;
use crate::error::StorageError;
use parking_lot::RwLock;

#[derive(Debug)]
pub struct GraphPropSegmentView {
    head: Arc<RwLock<GraphMeta>>,
    est_size: AtomicUsize,
}

impl GraphPropSegmentOps for GraphPropSegmentView {
    fn new() -> Self {
        Self {
            head: Arc::new(RwLock::new(GraphMeta::new())),
            est_size: AtomicUsize::new(0),
        }
    }

    fn load(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        todo!()
    }
}
