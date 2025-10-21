use raphtory_core::entities::properties::graph_meta::GraphMeta;
use std::path::PathBuf;
use std::sync::Arc;

/// Backing store for graph temporal properties and graph metadata.
/// MS: MetaSegment?
#[derive(Debug)]
pub struct MetaStorageInner<MS, EXT> {
    page: Arc<MS>,
    path: Option<PathBuf>,
    ext: EXT,
}
