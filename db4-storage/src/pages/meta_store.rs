use std::path::{Path, PathBuf};
use std::sync::Arc;
use crate::api::meta::MetaSegmentOps;
use crate::error::StorageError;
use crate::persist::strategy::Config;
use raphtory_core::entities::properties::graph_meta::GraphMeta;

/// Backing store for graph temporal properties and graph metadata.
/// MS: MetaSegment?
#[derive(Debug)]
pub struct MetaStorageInner<MS, EXT> {
    page: Arc<MS>,
    graph_meta: Arc<GraphMeta>,
    path: Option<PathBuf>,
    ext: EXT,
}

impl<MS: MetaSegmentOps, EXT: Config> MetaStorageInner<MS, EXT> {
    pub fn new(path: Option<PathBuf>, ext: EXT) -> Self {
        let graph_meta = Arc::new(GraphMeta::new());

        Self {
            page: Arc::new(MS::new()),
            path,
            graph_meta,
            ext,
        }
    }

    pub fn load(path: impl AsRef<Path>, ext: EXT) -> Result<Self, StorageError> {
        let graph_meta = Arc::new(GraphMeta::new());

        Ok(Self {
            page: Arc::new(MS::load(path.as_ref())?),
            path: Some(path.as_ref().to_path_buf()),
            graph_meta,
            ext,
        })
    }

    pub fn graph_meta(&self) -> &Arc<GraphMeta> {
        &self.graph_meta
    }
}
