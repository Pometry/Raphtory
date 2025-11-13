use std::path::{Path, PathBuf};
use std::sync::Arc;
use crate::api::graph::GraphSegmentOps;
use crate::error::StorageError;
use crate::pages::graph_page::writer::GraphWriter;
use crate::persist::strategy::Config;
use raphtory_core::entities::properties::graph_meta::GraphMeta;

/// Backing store for graph temporal properties and graph metadata.
#[derive(Debug)]
pub struct GraphStorageInner<GS, EXT> {
    /// The graph segment that contains all graph properties and graph metadata.
    /// Unlike node and edge segments, which are split into multiple segments,
    /// there is always only one graph segment.
    page: Arc<GS>,

    /// Stores graph prop metadata (prop name -> prop id mappings, etc).
    graph_meta: Arc<GraphMeta>,

    path: Option<PathBuf>,

    ext: EXT,
}

impl<GS: GraphSegmentOps, EXT: Config> GraphStorageInner<GS, EXT> {
    pub fn new(path: Option<&Path>, ext: EXT) -> Self {
        let page = Arc::new(GS::new(path));
        let graph_meta = Arc::new(GraphMeta::new());

        Self {
            page,
            path: path.map(|p| p.to_path_buf()),
            graph_meta,
            ext,
        }
    }

    pub fn new_with_meta(path: Option<&Path>, graph_meta: Arc<GraphMeta>, ext: EXT) -> Self {
        let page = Arc::new(GS::new(path));

        Self {
            page,
            path: path.map(|p| p.to_path_buf()),
            graph_meta,
            ext,
        }
    }

    pub fn load(path: impl AsRef<Path>, ext: EXT) -> Result<Self, StorageError> {
        let graph_meta = Arc::new(GraphMeta::new());

        Ok(Self {
            page: Arc::new(GS::load(path.as_ref())?),
            path: Some(path.as_ref().to_path_buf()),
            graph_meta,
            ext,
        })
    }

    pub fn graph_meta(&self) -> &Arc<GraphMeta> {
        &self.graph_meta
    }

    pub fn graph_entry(&self) -> GS::Entry<'_> {
        self.page.entry()
    }

    pub fn writer(&self) -> GraphWriter<'_> {
        let head = self.page.head_mut();

        GraphWriter::new(head)
    }
}
