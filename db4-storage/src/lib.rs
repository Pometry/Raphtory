use std::path::Path;

use crate::{
    gen_t_props::GenTProps,
    gen_ts::GenericTimeOps,
    pages::{
        GraphStore, ReadLockedGraphStore, edge_store::ReadLockedEdgeStorage,
        node_store::ReadLockedNodeStorage,
    },
    segments::{
        edge::EdgeSegmentView,
        edge_entry::{MemEdgeEntry, MemEdgeRef},
        node::NodeSegmentView,
        node_entry::{MemNodeEntry, MemNodeRef},
    },
};
use raphtory_api::core::entities::{EID, VID};
use segments::{edge::MemEdgeSegment, node::MemNodeSegment};

pub mod api;
pub mod gen_t_props;
pub mod gen_ts;
pub mod pages;
pub mod persist;
pub mod properties;
pub mod segments;
pub mod utils;
// pub mod loaders;

pub type Extension = ();
pub type NS<P> = NodeSegmentView<P>;
pub type ES<P> = EdgeSegmentView<P>;
pub type Layer<EXT> = GraphStore<NodeSegmentView<EXT>, EdgeSegmentView<EXT>, EXT>;
pub type ReadLockedLayer<EXT> = ReadLockedGraphStore<NodeSegmentView, EdgeSegmentView, EXT>;

pub type ReadLockedNodes<P> = ReadLockedNodeStorage<NodeSegmentView, P>;
pub type ReadLockedEdges<P> = ReadLockedEdgeStorage<EdgeSegmentView, P>;

pub type NodeEntry<'a> = MemNodeEntry<'a, parking_lot::RwLockReadGuard<'a, MemNodeSegment>>;
pub type EdgeEntry<'a> = MemEdgeEntry<'a, parking_lot::RwLockReadGuard<'a, MemEdgeSegment>>;
pub type NodeEntryRef<'a> = MemNodeRef<'a>;
pub type EdgeEntryRef<'a> = MemEdgeRef<'a>;

pub type NodeAdditions<'a> = GenericTimeOps<'a, MemNodeRef<'a>>;
pub type EdgeAdditions<'a> = GenericTimeOps<'a, MemEdgeRef<'a>>;
pub type NodeTProps<'a> = GenTProps<'a, MemNodeRef<'a>>;
pub type EdgeTProps<'a> = GenTProps<'a, MemEdgeRef<'a>>;

pub mod error {
    use std::{path::PathBuf, sync::Arc};

    use raphtory_api::core::entities::properties::prop::PropError;
    use raphtory_core::utils::time::ParseTimeError;

    #[derive(thiserror::Error, Debug)]
    pub enum DBV4Error {
        #[error("External Storage Error {0}")]
        External(#[from] Arc<dyn std::error::Error + Send + Sync>),
        #[error("IO error: {0}")]
        IO(#[from] std::io::Error),
        #[error("Serde error: {0}")]
        Serde(#[from] serde_json::Error),
        #[error("Arrow-rs error: {0}")]
        ArrowRS(#[from] arrow_schema::ArrowError),
        #[error("Parquet error: {0}")]
        Parquet(#[from] parquet::errors::ParquetError),

        #[error("Property error: {0}")]
        PropError(#[from] PropError),
        #[error("Empty Graph: {0}")]
        EmptyGraphDir(PathBuf),
        #[error("Failed to parse time string")]
        ParseTime {
            #[from]
            source: ParseTimeError,
        },
        // #[error("Failed to mutate: {0}")]
        // MutationError(#[from] MutationError),
        #[error("Unnamed Failure: {0}")]
        GenericFailure(String),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct LocalPOS(pub usize);

impl LocalPOS {
    pub fn as_vid(self, page_id: usize, max_page_len: usize) -> VID {
        VID(page_id * max_page_len + self.0)
    }

    pub fn as_eid(self, page_id: usize, max_page_len: usize) -> EID {
        EID(page_id * max_page_len + self.0)
    }
}

impl From<usize> for LocalPOS {
    fn from(pos: usize) -> Self {
        Self(pos)
    }
}

pub fn calculate_size_recursive(path: &Path) -> Result<usize, std::io::Error> {
    let mut size = 0;
    if path.is_dir() {
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                size += calculate_size_recursive(&path)?;
            } else {
                size += path.metadata()?.len() as usize;
            }
        }
    } else {
        size += path.metadata()?.len() as usize;
    }
    Ok(size)
}
