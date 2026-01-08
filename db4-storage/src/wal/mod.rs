use crate::error::StorageError;
use raphtory_api::core::entities::properties::prop::Prop;
use raphtory_core::{
    entities::{EID, GID, VID},
    storage::timeindex::TimeIndexEntry,
};
use std::path::{Path, PathBuf};

pub mod entry;
pub mod no_wal;

pub type LSN = u64;
pub type TransactionID = u64;

#[derive(Debug)]
pub struct WalRecord {
    pub lsn: LSN,
    pub data: Vec<u8>,
}

/// Core Wal methods.
pub trait Wal {
    fn new(dir: Option<PathBuf>) -> Result<Self, StorageError>
    where
        Self: Sized;

    /// Appends data to the WAL and returns the assigned LSN.
    fn append(&self, data: &[u8]) -> Result<LSN, StorageError>;

    /// Flushes in-memory WAL entries up to the given LSN to disk.
    fn flush(&self, lsn: LSN) -> Result<(), StorageError>;

    /// Rotates the underlying WAL file.
    /// `cutoff_lsn` acts as a hint for which records can be safely discarded during rotation.
    fn rotate(&self, cutoff_lsn: LSN) -> Result<(), StorageError>;

    /// Returns an iterator over the wal entries in the given directory.
    fn replay(dir: impl AsRef<Path>) -> impl Iterator<Item = Result<WalRecord, StorageError>>;
}

// Raphtory-specific logging & replay methods.
pub trait GraphWal {
    /// ReplayEntry represents the type of the wal entry returned during replay.
    type ReplayEntry;

    fn log_add_edge(
        &self,
        transaction_id: TransactionID,
        t: TimeIndexEntry,
        src_name: GID,
        src_id: VID,
        dst_name: GID,
        dst_id: VID,
        eid: EID,
        layer_name: Option<&str>,
        layer_id: usize,
        props: Vec<(&str, usize, Prop)>,
    ) -> Result<LSN, StorageError>;

    /// Logs a checkpoint record, indicating that all Wal operations upto and including
    /// `lsn` has been persisted to disk.
    fn log_checkpoint(&self, lsn: LSN) -> Result<LSN, StorageError>;

    /// Returns an iterator over the wal entries in the given directory.
    fn replay_iter(
        dir: impl AsRef<Path>,
    ) -> impl Iterator<Item = Result<(LSN, Self::ReplayEntry), StorageError>>;

    /// Replays and applies all the wal entries in the given directory to the given graph.
    fn replay_to_graph<G: GraphReplay>(
        dir: impl AsRef<Path>,
        graph: &mut G,
    ) -> Result<(), StorageError>;
}

/// Trait for defining callbacks for replaying from wal.
pub trait GraphReplay {
    fn replay_add_edge(
        &mut self,
        lsn: LSN,
        transaction_id: TransactionID,
        t: TimeIndexEntry,
        src_name: GID,
        src_id: VID,
        dst_name: GID,
        dst_id: VID,
        eid: EID,
        layer_name: Option<String>,
        layer_id: usize,
        props: Vec<(String, usize, Prop)>,
    ) -> Result<(), StorageError>;
}
