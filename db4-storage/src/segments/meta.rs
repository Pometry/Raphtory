use raphtory_core::entities::properties::graph_meta::GraphMeta;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use crate::api::meta::GraphMetaSegmentOps;
use crate::error::StorageError;

#[derive(Debug)]
pub struct GraphMetaSegmentView {
    inner: Arc<GraphMeta>,
    est_size: AtomicUsize,
}

impl GraphMetaSegmentOps for GraphMetaSegmentView {
    fn new() -> Self {
        Self {
            inner: Arc::new(GraphMeta::new()),
            est_size: AtomicUsize::new(0),
        }
    }

    fn load(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        todo!()
    }
}
