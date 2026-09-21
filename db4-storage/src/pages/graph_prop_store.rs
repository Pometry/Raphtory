use crate::{
    api::graph_props::GraphPropSegmentOps,
    error::StorageError,
    pages::{
        graph_prop_page::writer::GraphPropWriter, locked::graph_props::WriteLockedGraphPropSegment,
    },
    persist::strategy::PersistenceStrategy,
};
use raphtory_api::core::entities::properties::meta::Meta;
use std::{marker::PhantomData, path::Path, sync::Arc};

/// Backing store for graph temporal properties and graph metadata.
#[derive(Debug)]
pub struct GraphPropStorageInner<GS, EXT> {
    /// The graph props segment that contains all graph properties and graph metadata.
    /// Unlike node and edge segments, which are split into multiple segments,
    /// there is always only one graph props segment.
    segment: Arc<GS>,

    /// Stores graph prop metadata (prop name -> prop id mappings).
    meta: Arc<Meta>,
    _ext: PhantomData<EXT>,
}

impl<GS: GraphPropSegmentOps<Extension = EXT>, EXT: PersistenceStrategy>
    GraphPropStorageInner<GS, EXT>
{
    pub fn new(path: Option<&Path>, meta: Arc<Meta>, ext: EXT) -> Result<Self, StorageError> {
        if let Some(path) = path {
            std::fs::create_dir_all(path)?;
        }

        let segment = Arc::new(GS::new(meta.clone(), path, ext.clone()));

        Ok(Self {
            segment,
            meta,
            _ext: PhantomData,
        })
    }

    pub fn load(path: impl AsRef<Path>, ext: EXT) -> Result<Self, StorageError> {
        let meta = Arc::new(Meta::new_for_graph_props());
        let segment = Arc::new(GS::load(meta.clone(), path.as_ref(), ext.clone())?);

        Ok(Self {
            segment,
            meta,
            _ext: PhantomData,
        })
    }

    pub fn meta(&self) -> &Arc<Meta> {
        &self.meta
    }

    pub fn graph_entry(&self) -> GS::Entry<'_> {
        self.segment.entry()
    }

    pub fn segment(&self) -> &Arc<GS> {
        &self.segment
    }

    pub fn writer(&self) -> GraphPropWriter<'_, GS> {
        let head = self.segment.head_mut();
        let graph_props = &self.segment;
        GraphPropWriter::new(graph_props, head)
    }

    pub fn write_locked<'a>(&'a self) -> WriteLockedGraphPropSegment<'a, GS> {
        WriteLockedGraphPropSegment::new(self.segment.as_ref(), self.segment.head_mut())
    }

    pub fn flush(&self) -> Result<(), StorageError> {
        self.segment.flush()
    }
}
