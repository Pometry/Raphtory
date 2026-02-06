use crate::error::StorageError;
use raphtory_api::core::entities::{GidRef, properties::prop::Prop};
use raphtory_core::{
    entities::{EID, GID, VID},
    storage::timeindex::EventTime,
};
use std::path::Path;

pub mod entry;
pub mod no_wal;

pub type LSN = u64;
pub type TransactionID = u64;

/// Core Wal methods.
pub trait WalOps {
    type Config;

    fn new(dir: Option<&Path>, config: Self::Config) -> Result<Self, StorageError>
    where
        Self: Sized;

    /// Loads an existing WAL file from the given directory in append mode.
    fn load(dir: Option<&Path>, config: Self::Config) -> Result<Self, StorageError>
    where
        Self: Sized;

    /// Appends data to the WAL and returns the assigned LSN.
    fn append(&self, data: &[u8]) -> Result<LSN, StorageError>;

    /// Flushes in-memory WAL entries up to the given LSN to disk.
    /// Returns immediately if the given LSN is already flushed to disk.
    fn flush(&self, lsn: LSN) -> Result<(), StorageError>;

    /// Rotates the underlying WAL file.
    /// `cutoff_lsn` acts as a hint for which records can be safely discarded during rotation.
    fn rotate(&self, cutoff_lsn: LSN) -> Result<(), StorageError>;

    /// Returns an iterator over the entries in the wal.
    fn replay(&self) -> impl Iterator<Item = Result<ReplayRecord, StorageError>>;

    /// Returns true if there are entries in the WAL file on disk.
    fn has_entries(&self) -> Result<bool, StorageError>;
}

#[derive(Debug)]
pub struct ReplayRecord {
    lsn: LSN,

    data: Vec<u8>,

    /// The raw bytes of the WAL entry stored on disk, including CRC data.
    raw_bytes: Vec<u8>,
}

impl ReplayRecord {
    pub fn new(lsn: LSN, data: Vec<u8>, raw_bytes: Vec<u8>) -> Self {
        Self {
            lsn,
            data,
            raw_bytes,
        }
    }

    pub fn lsn(&self) -> LSN {
        self.lsn
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn raw_bytes(&self) -> &[u8] {
        &self.raw_bytes
    }
}

// Raphtory-specific logging & replay methods.
pub trait GraphWalOps {
    /// ReplayEntry represents the type of the wal entry returned during replay.
    type ReplayEntry;

    fn log_add_edge(
        &self,
        transaction_id: TransactionID,
        t: EventTime,
        src_name: Option<GidRef<'_>>,
        src_id: VID,
        dst_name: Option<GidRef<'_>>,
        dst_id: VID,
        eid: EID,
        layer_name: Option<&str>,
        layer_id: usize,
        props: Vec<(&str, usize, Prop)>,
    ) -> Result<LSN, StorageError>;

    /// Logs a checkpoint record, indicating that all Wal operations upto and including
    /// `lsn` has been persisted to disk.
    fn log_checkpoint(&self, lsn: LSN) -> Result<LSN, StorageError>;

    /// Returns an iterator over the entries in the wal.
    fn replay_iter(&self) -> impl Iterator<Item = Result<(LSN, Self::ReplayEntry), StorageError>>;

    /// Replays and applies all the entries in the wal to the given graph.
    /// Subsequent appends to the WAL will start from the LSN of the last replayed entry.
    fn replay_to_graph<G: GraphReplay>(&self, graph: &mut G) -> Result<(), StorageError>;
}

/// Trait for defining callbacks for replaying from wal.
pub trait GraphReplay {
    fn replay_add_edge(
        &mut self,
        lsn: LSN,
        transaction_id: TransactionID,
        t: EventTime,
        src_name: Option<GID>,
        src_id: VID,
        dst_name: Option<GID>,
        dst_id: VID,
        eid: EID,
        layer_name: Option<String>,
        layer_id: usize,
        props: Vec<(String, usize, Prop)>,
    ) -> Result<(), StorageError>;
}
