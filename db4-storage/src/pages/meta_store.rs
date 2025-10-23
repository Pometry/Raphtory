use std::path::{Path, PathBuf};
use std::sync::Arc;
use crate::api::meta::MetaSegmentOps;
use crate::error::StorageError;
use crate::persist::strategy::Config;

/// Backing store for graph temporal properties and graph metadata.
/// MS: MetaSegment?
#[derive(Debug)]
pub struct MetaStorageInner<MS, EXT> {
    page: Arc<MS>,
    path: Option<PathBuf>,
    ext: EXT,
}

impl<MS: MetaSegmentOps, EXT: Config> MetaStorageInner<MS, EXT> {
    pub fn new(path: Option<PathBuf>, ext: EXT) -> Self {
        Self {
            page: Arc::new(MS::new()),
            path,
            ext,
        }
    }

    pub fn load(path: impl AsRef<Path>, ext: EXT) -> Result<Self, StorageError> {
        Ok(Self {
            page: Arc::new(MS::load(path.as_ref())?),
            path: Some(path.as_ref().to_path_buf()),
            ext,
        })
    }
}
