use crate::error::StorageError;
use raphtory_api::core::{entities::properties::prop::Prop, storage::dict_mapper::MaybeNew};
use raphtory_core::{
    entities::{EID, GID, VID},
    storage::timeindex::TimeIndexEntry,
};
use std::path::Path;

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
    fn new(dir: impl AsRef<Path>) -> Result<Self, StorageError>
    where
        Self: Sized;

    /// Returns the directory the WAL is stored in.
    fn dir(&self) -> &Path;

    /// Appends data to the WAL and returns the assigned LSN.
    fn append(&self, data: &[u8]) -> Result<LSN, StorageError>;

    /// Immediately flushes in-memory WAL entries to disk.
    fn sync(&self) -> Result<(), StorageError>;

    /// Blocks until the WAL has fsynced the given LSN to disk.
    fn wait_for_sync(&self, lsn: LSN);

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

    fn log_begin_transaction(&self, transaction_id: TransactionID) -> Result<LSN, StorageError>;

    fn log_end_transaction(&self, transaction_id: TransactionID) -> Result<LSN, StorageError>;

    /// Log a static edge addition.
    ///
    /// # Arguments
    ///
    /// * `transaction_id` - The transaction ID
    /// * `t` - The timestamp of the edge addition
    /// * `src` - The source vertex ID
    /// * `dst` - The destination vertex ID
    fn log_add_static_edge(
        &self,
        transaction_id: TransactionID,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
    ) -> Result<LSN, StorageError>;

    /// Log an edge addition to a layer with temporal props.
    ///
    /// # Arguments
    ///
    /// * `transaction_id` - The transaction ID
    /// * `t` - The timestamp of the edge addition
    /// * `src` - The source vertex ID
    /// * `dst` - The destination vertex ID
    /// * `eid` - The edge ID
    /// * `layer_id` - The layer ID
    /// * `props` - The temporal properties of the edge
    fn log_add_edge(
        &self,
        transaction_id: TransactionID,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        eid: EID,
        layer_id: usize,
        props: &[(usize, Prop)],
    ) -> Result<LSN, StorageError>;

    fn log_node_id(
        &self,
        transaction_id: TransactionID,
        gid: GID,
        vid: VID,
    ) -> Result<LSN, StorageError>;

    fn log_edge_id(
        &self,
        transaction_id: TransactionID,
        src: VID,
        dst: VID,
        eid: EID,
        layer_id: usize,
    ) -> Result<LSN, StorageError>;

    /// Log constant prop name -> prop id mappings.
    ///
    /// # Arguments
    ///
    /// * `transaction_id` - The transaction ID
    /// * `props` - A slice containing new or existing tuples of (prop name, id, value)
    fn log_const_prop_ids<PN: AsRef<str>>(
        &self,
        transaction_id: TransactionID,
        props: &[MaybeNew<(PN, usize, Prop)>],
    ) -> Result<LSN, StorageError>;

    /// Log temporal prop name -> prop id mappings.
    ///
    /// # Arguments
    ///
    /// * `transaction_id` - The transaction ID
    /// * `props` - A slice containing new or existing tuples of (prop name, id, value).
    fn log_temporal_prop_ids<PN: AsRef<str>>(
        &self,
        transaction_id: TransactionID,
        props: &[MaybeNew<(PN, usize, Prop)>],
    ) -> Result<LSN, StorageError>;

    fn log_layer_id(
        &self,
        transaction_id: TransactionID,
        name: &str,
        id: usize,
    ) -> Result<LSN, StorageError>;

    /// Logs a checkpoint record, indicating that all Wal operations upto and including
    /// `lsn` has been persisted to disk.
    fn log_checkpoint(&self, lsn: LSN) -> Result<LSN, StorageError>;

    /// Returns an iterator over the wal entries in the given directory.
    fn replay_iter(
        dir: impl AsRef<Path>,
    ) -> impl Iterator<Item = Result<(LSN, Self::ReplayEntry), StorageError>>;

    /// Replays and applies all the wal entries in the given directory to the given graph.
    fn replay_to_graph<G: GraphReplayer>(
        dir: impl AsRef<Path>,
        graph: &mut G,
    ) -> Result<(), StorageError>;
}

/// Trait for defining callbacks for replaying from wal
pub trait GraphReplayer {
    fn replay_begin_transaction(
        &self,
        lsn: LSN,
        transaction_id: TransactionID,
    ) -> Result<(), StorageError>;

    fn replay_end_transaction(
        &self,
        lsn: LSN,
        transaction_id: TransactionID,
    ) -> Result<(), StorageError>;

    fn replay_add_static_edge(
        &self,
        lsn: LSN,
        transaction_id: TransactionID,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
    ) -> Result<(), StorageError>;

    fn replay_add_edge(
        &self,
        lsn: LSN,
        transaction_id: TransactionID,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        eid: EID,
        layer_id: usize,
        props: &[(usize, Prop)],
    ) -> Result<(), StorageError>;

    fn replay_node_id(
        &self,
        lsn: LSN,
        transaction_id: TransactionID,
        gid: GID,
        vid: VID,
    ) -> Result<(), StorageError>;

    fn replay_const_prop_ids<PN: AsRef<str>>(
        &self,
        lsn: LSN,
        transaction_id: TransactionID,
        props: &[MaybeNew<(PN, usize, Prop)>],
    ) -> Result<(), StorageError>;

    fn replay_temporal_prop_ids<PN: AsRef<str>>(
        &self,
        lsn: LSN,
        transaction_id: TransactionID,
        props: &[MaybeNew<(PN, usize, Prop)>],
    ) -> Result<(), StorageError>;

    fn replay_layer_id(
        &self,
        lsn: LSN,
        transaction_id: TransactionID,
        name: &str,
        id: usize,
    ) -> Result<(), StorageError>;
}
