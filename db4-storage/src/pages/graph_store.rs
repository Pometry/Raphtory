use std::path::{Path, PathBuf};
use std::sync::Arc;
use crate::api::graph::GraphSegmentOps;
use crate::error::StorageError;
use crate::persist::strategy::Config;
use raphtory_core::entities::properties::graph_meta::GraphMeta;

/// Backing store for graph temporal properties and graph metadata.
/// GPS: GraphPropSegment?
#[derive(Debug)]
pub struct GraphStorageInner<GS, EXT> {
    page: Arc<GS>,

    /// Stores graph prop metadata (prop name -> prop id mappings, etc).
    graph_meta: Arc<GraphMeta>,

    path: Option<PathBuf>,

    ext: EXT,
}

impl<GPS: GraphSegmentOps, EXT: Config> GraphStorageInner<GPS, EXT> {
    pub fn new(path: Option<PathBuf>, ext: EXT) -> Self {
        let page = Arc::new(GPS::new());
        let graph_meta = Arc::new(GraphMeta::new());

        Self {
            page,
            path,
            graph_meta,
            ext,
        }
    }

    pub fn new_with_meta(path: Option<PathBuf>, graph_meta: Arc<GraphMeta>, ext: EXT) -> Self {
        let page = Arc::new(GPS::new());

        Self {
            page,
            path,
            graph_meta,
            ext,
        }
    }

    pub fn load(path: impl AsRef<Path>, ext: EXT) -> Result<Self, StorageError> {
        let graph_meta = Arc::new(GraphMeta::new());

        Ok(Self {
            page: Arc::new(GPS::load(path.as_ref())?),
            path: Some(path.as_ref().to_path_buf()),
            graph_meta,
            ext,
        })
    }

    pub fn graph_meta(&self) -> &Arc<GraphMeta> {
        &self.graph_meta
    }
}
