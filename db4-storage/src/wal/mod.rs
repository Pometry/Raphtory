use std::path::Path;
use raphtory_core::{
    entities::{VID, EID, GID},
    storage::timeindex::TimeIndexEntry,
};
use std::borrow::Cow;
use raphtory_api::core::{entities::properties::prop::Prop, storage::dict_mapper::MaybeNew};
use crate::error::DBV4Error;

pub mod no_wal;
pub mod entry;

pub type LSN = u64;
pub type TransactionID = u64;

#[derive(Debug)]
pub struct WalRecord {
    pub lsn: LSN,
    pub data: Vec<u8>,
}

pub trait Wal {
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

pub trait WalEntryBuilder<'a> {
    fn begin_txn(txn_id: TransactionID) -> Self;

    fn commit_txn(txn_id: TransactionID) -> Self;

    fn add_edge(
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        eid: EID,
        layer_id: usize,
        t_props: Cow<'a, [(usize, Prop)]>,
        c_props: Cow<'a, [(usize, Prop)]>,
    ) -> Self;

    fn add_node_id(gid: GID, vid: VID) -> Self;

    /// Log new constant prop name -> prop id mappings.
    ///
    /// # Arguments
    ///
    /// * `props` - A slice containing new or existing tuples of (prop name, id, value)
    fn add_new_const_prop_ids<PN: AsRef<str>>(props: &'a [MaybeNew<(PN, usize, Prop)>]) -> Self;

    /// Log new temporal prop name -> prop id mappings.
    ///
    /// # Arguments
    ///
    /// * `props` - A slice containing new or existing tuples of (prop name, id, value).
    fn add_new_temporal_prop_ids<PN: AsRef<str>>(props: &'a [MaybeNew<(PN, usize, Prop)>]) -> Self;

    fn add_layer_id(name: String, id: usize) -> Self;

    /// Logs a checkpoint record, indicating that all Wal operations upto and including
    /// `lsn` has been persisted to disk.
    fn checkpoint(lsn: LSN) -> Self;

    // Methods to serialize/deserialize

    fn to_bytes(&self) -> Result<Vec<u8>, DBV4Error>;

    fn from_bytes(bytes: &[u8]) -> Result<Self, DBV4Error> where Self: Sized;
}
