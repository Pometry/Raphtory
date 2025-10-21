use parking_lot::RwLock;
use raphtory_core::entities::properties::graph_meta::GraphMeta;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

#[derive(Debug)]
pub struct MetaSegmentView<P> {
    inner: Arc<RwLock<GraphMeta>>,

    persistent: P,

    est_size: AtomicUsize,
}
