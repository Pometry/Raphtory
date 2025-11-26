use std::{
    path::{Path, PathBuf},
    thread,
    time::Duration,
};

use crate::{
    generic_t_props::GenericTProps,
    gen_ts::{
        AdditionCellsRef, DeletionCellsRef, EdgeAdditionCellsRef, GenericTimeOps,
        PropAdditionCellsRef,
    },
    pages::{
        GraphStore, ReadLockedGraphStore, edge_store::ReadLockedEdgeStorage,
        node_store::ReadLockedNodeStorage,
    },
    persist::strategy::NoOpStrategy,
    resolver::mapping_resolver::MappingResolver,
    segments::{
        edge::{
            entry::{MemEdgeEntry, MemEdgeRef},
            segment::EdgeSegmentView,
        },
        graph::entry::MemGraphEntry,
        node::{
            entry::{MemNodeEntry, MemNodeRef},
            segment::NodeSegmentView,
        },
    },
    wal::no_wal::NoWal,
};
use parking_lot::RwLock;
use raphtory_api::core::entities::{EID, VID};
use segments::{
    edge::segment::MemEdgeSegment, graph::GraphSegmentView, node::segment::MemNodeSegment,
};

pub mod api;
pub mod generic_t_props;
pub mod gen_ts;
pub mod pages;
pub mod persist;
pub mod properties;
pub mod resolver;
pub mod segments;
pub mod utils;
pub mod wal;

pub type Extension = NoOpStrategy;
pub type NS<P> = NodeSegmentView<P>;
pub type ES<P> = EdgeSegmentView<P>;
pub type GS = GraphSegmentView;
pub type Layer<P> = GraphStore<NS<P>, ES<P>, GS, P>;

pub type WalImpl = NoWal;
pub type GIDResolver = MappingResolver;

pub type ReadLockedLayer<P> = ReadLockedGraphStore<NS<P>, ES<P>, GS, P>;
pub type ReadLockedNodes<P> = ReadLockedNodeStorage<NS<P>, P>;
pub type ReadLockedEdges<P> = ReadLockedEdgeStorage<ES<P>, P>;

pub type NodeEntry<'a> = MemNodeEntry<'a, parking_lot::RwLockReadGuard<'a, MemNodeSegment>>;
pub type EdgeEntry<'a> = MemEdgeEntry<'a, parking_lot::RwLockReadGuard<'a, MemEdgeSegment>>;
pub type NodeEntryRef<'a> = MemNodeRef<'a>;
pub type EdgeEntryRef<'a> = MemEdgeRef<'a>;
pub type GraphEntry<'a> = MemGraphEntry<'a>;

pub type NodePropAdditions<'a> = GenericTimeOps<'a, PropAdditionCellsRef<'a, MemNodeRef<'a>>>;
pub type NodeEdgeAdditions<'a> = GenericTimeOps<'a, EdgeAdditionCellsRef<'a, MemNodeRef<'a>>>;

pub type EdgeAdditions<'a> = GenericTimeOps<'a, AdditionCellsRef<'a, MemEdgeRef<'a>>>;
pub type EdgeDeletions<'a> = GenericTimeOps<'a, DeletionCellsRef<'a, MemEdgeRef<'a>>>;

pub type NodeTProps<'a> = GenericTProps<'a, MemNodeRef<'a>>;
pub type EdgeTProps<'a> = GenericTProps<'a, MemEdgeRef<'a>>;

pub mod error {
    use std::{path::PathBuf, sync::Arc};

    use raphtory_api::core::entities::properties::prop::PropError;
    use raphtory_core::{
        entities::{graph::logical_to_physical::InvalidNodeId, properties::props::MetadataError},
        utils::time::ParseTimeError,
    };

    #[derive(thiserror::Error, Debug)]
    pub enum StorageError {
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
        #[error(transparent)]
        PropError(#[from] PropError),
        #[error(transparent)]
        MetadataError(#[from] MetadataError),
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
        #[error(transparent)]
        InvalidNodeId(#[from] InvalidNodeId),

        #[error("Failed to vacuum storage")]
        VacuumError,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize)]
#[repr(transparent)]
pub struct LocalPOS(pub u32);

impl From<usize> for LocalPOS {
    fn from(value: usize) -> Self {
        assert!(value <= u32::MAX as usize);
        LocalPOS(value as u32)
    }
}

impl LocalPOS {
    pub fn as_vid(self, page_id: usize, max_page_len: u32) -> VID {
        VID(page_id * (max_page_len as usize) + (self.0 as usize))
    }

    pub fn as_eid(self, page_id: usize, max_page_len: u32) -> EID {
        EID(page_id * (max_page_len as usize) + (self.0 as usize))
    }

    pub fn as_index(self) -> usize {
        self.0 as usize
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

pub fn collect_tree_paths(path: &Path) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    if path.is_dir() {
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let entry_path = entry.path();
            if entry_path.is_dir() {
                paths.extend(collect_tree_paths(&entry_path));
            } else {
                paths.push(entry_path);
            }
        }
    } else {
        paths.push(path.to_path_buf());
    }
    paths
}

pub fn loop_lock_write<A>(l: &RwLock<A>) -> parking_lot::RwLockWriteGuard<'_, A> {
    const MAX_BACKOFF_US: u64 = 1000; // 1ms max
    let mut backoff_us = 1;
    loop {
        if let Some(guard) = l.try_write_for(Duration::from_micros(50)) {
            return guard;
        }
        thread::park_timeout(Duration::from_micros(backoff_us));
        backoff_us = (backoff_us * 2).min(MAX_BACKOFF_US);
    }
}
