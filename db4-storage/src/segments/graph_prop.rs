use raphtory_core::entities::properties::graph_meta::GraphMeta;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use crate::api::graph_props::GraphPropSegmentOps;
use crate::error::StorageError;

#[derive(Debug)]
pub struct GraphPropSegmentView {
    inner: Arc<GraphMeta>,
    est_size: AtomicUsize,
}

impl GraphPropSegmentOps for GraphPropSegmentView {
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
