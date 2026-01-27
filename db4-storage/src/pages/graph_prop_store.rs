use raphtory_api::core::entities::properties::meta::Meta;

use crate::{
    api::graph_props::GraphPropSegmentOps,
    error::StorageError,
    pages::{
        graph_prop_page::writer::GraphPropWriter,
        locked::graph_props::{LockedGraphPropPage, WriteLockedGraphPropPages},
    },
    persist::strategy::Config,
};

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

/// Backing store for graph temporal properties and graph metadata.
#[derive(Debug)]
pub struct GraphPropStorageInner<GS, EXT> {
    /// The graph props segment that contains all graph properties and graph metadata.
    /// Unlike node and edge segments, which are split into multiple segments,
    /// there is always only one graph props segment.
    page: Arc<GS>,

    /// Stores graph prop metadata (prop name -> prop id mappings).
    meta: Arc<Meta>,

    path: Option<PathBuf>,

    ext: EXT,
}

impl<GS: GraphPropSegmentOps<Extension = EXT>, EXT: Config> GraphPropStorageInner<GS, EXT> {
    pub fn new_with_meta(path: Option<&Path>, meta: Arc<Meta>, ext: EXT) -> Self {
        let page = Arc::new(GS::new(meta.clone(), path, ext.clone()));

        Self {
            page,
            path: path.map(|p| p.to_path_buf()),
            meta,
            ext,
        }
    }

    pub fn load(path: impl AsRef<Path>, ext: EXT) -> Result<Self, StorageError> {
        let graph_props_meta = Arc::new(Meta::new_for_graph_props());

        Ok(Self {
            page: Arc::new(GS::load(
                graph_props_meta.clone(),
                path.as_ref(),
                ext.clone(),
            )?),
            path: Some(path.as_ref().to_path_buf()),
            meta: graph_props_meta,
            ext,
        })
    }

    pub fn meta(&self) -> &Arc<Meta> {
        &self.meta
    }

    pub fn graph_entry(&self) -> GS::Entry<'_> {
        self.page.entry()
    }

    pub fn writer(&self) -> GraphPropWriter<'_, GS> {
        let head = self.page.head_mut();
        let graph_props = &self.page;
        GraphPropWriter::new(graph_props, head)
    }

    pub fn write_locked<'a>(&'a self) -> WriteLockedGraphPropPages<'a, GS> {
        WriteLockedGraphPropPages::new(LockedGraphPropPage::new(
            self.page.as_ref(),
            self.page.head_mut(),
        ))
    }

    pub fn flush(&self) -> Result<(), StorageError> {
        self.page.flush()
    }
}
