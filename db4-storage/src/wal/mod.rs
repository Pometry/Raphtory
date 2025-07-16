use crate::error::DBV4Error;
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
pub trait WalOps {
    fn new(dir: impl AsRef<Path>) -> Result<Self, DBV4Error>
    where
        Self: Sized;

    fn dir(&self) -> &Path;

    /// Appends data to the WAL and returns the assigned LSN.
    fn append(&self, data: &[u8]) -> Result<LSN, DBV4Error>;

    /// Blocks until the WAL has fsynced the given LSN to disk.
    fn wait_for_sync(&self, lsn: LSN);

    /// Rotates the underlying WAL file.
    /// `cutoff_lsn` acts as a hint for which records can be safely discarded during rotation.
    fn rotate(&self, cutoff_lsn: LSN) -> Result<(), DBV4Error>;

    fn replay(dir: impl AsRef<Path>) -> impl Iterator<Item = Result<WalRecord, DBV4Error>>;
}

// High-level, raphtory-specific logging methods.
pub trait WalEntryOps {
    fn log_begin_txn(&self, txn_id: TransactionID) -> Result<LSN, DBV4Error>;

    fn log_end_txn(&self, txn_id: TransactionID) -> Result<LSN, DBV4Error>;

    fn log_add_edge(
        &self,
        txn_id: TransactionID,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        layer_id: usize,
        t_props: &[(usize, Prop)],
        c_props: &[(usize, Prop)],
    ) -> Result<LSN, DBV4Error>;

    fn log_node_id(&self, txn_id: TransactionID, gid: GID, vid: VID) -> Result<LSN, DBV4Error>;

    fn log_edge_id(&self, txn_id: TransactionID, src: VID, dst: VID, eid: EID) -> Result<LSN, DBV4Error>;

    /// Log new constant prop name -> prop id mappings.
    ///
    /// # Arguments
    ///
    /// * `txn_id` - The transaction ID
    /// * `props` - A slice containing new or existing tuples of (prop name, id, value)
    fn log_const_prop_ids<PN: AsRef<str>>(
        &self,
        txn_id: TransactionID,
        props: &[MaybeNew<(PN, usize, Prop)>],
    ) -> Result<LSN, DBV4Error>;

    /// Log new temporal prop name -> prop id mappings.
    ///
    /// # Arguments
    ///
    /// * `txn_id` - The transaction ID
    /// * `props` - A slice containing new or existing tuples of (prop name, id, value).
    fn log_temporal_prop_ids<PN: AsRef<str>>(
        &self,
        txn_id: TransactionID,
        props: &[MaybeNew<(PN, usize, Prop)>],
    ) -> Result<LSN, DBV4Error>;

    fn log_layer_id(&self, txn_id: TransactionID, name: &str, id: usize) -> Result<LSN, DBV4Error>;

    /// Logs a checkpoint record, indicating that all Wal operations upto and including
    /// `lsn` has been persisted to disk.
    fn log_checkpoint(&self, lsn: LSN) -> Result<LSN, DBV4Error>;

    // // Methods to serialize/deserialize

    // fn to_bytes(&self) -> Result<Vec<u8>, DBV4Error>;

    // fn from_bytes(bytes: &[u8]) -> Result<Self, DBV4Error>
    // where
    //     Self: Sized;
}
