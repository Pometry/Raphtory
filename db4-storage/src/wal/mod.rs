use std::path::Path;
use raphtory_core::{
    entities::{VID, EID, GID},
    storage::timeindex::TimeIndexEntry,
};
use std::borrow::Cow;
use raphtory_api::core::entities::properties::prop::Prop;
use crate::error::DBV4Error;

pub mod no_wal;
pub mod entry;

pub type LSN = u64;

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

    /// Reserves and returns the next available LSN without writing any data.
    fn reserve(&self) -> LSN;

    /// Appends data to the WAL with the specified LSN.
    /// The LSN must have been previously reserved.
    fn append_with_lsn(&self, lsn: LSN, data: &[u8]) -> Result<(), DBV4Error>;

    /// Appends data to the WAL and returns the assigned LSN.
    /// This is a convenience method that combines reserve() and append_with_lsn().
    fn append(&self, data: &[u8]) -> Result<LSN, DBV4Error> {
        let lsn = self.reserve();
        self.append_with_lsn(lsn, data)?;
        Ok(lsn)
    }

    /// Blocks until the WAL has fsynced the given LSN to disk.
    fn wait_for_sync(&self, lsn: LSN);

    /// Rotates the underlying WAL file.
    /// `cutoff_lsn` acts as a hint for which records can be safely discarded during rotation.
    fn rotate(&self, cutoff_lsn: LSN) -> Result<(), DBV4Error>;

    fn replay(dir: impl AsRef<Path>) -> impl Iterator<Item = Result<WalRecord, DBV4Error>>;
}

pub trait WalEntry {
    fn to_bytes(&self) -> Result<Vec<u8>, DBV4Error>;

    fn from_bytes(bytes: &[u8]) -> Result<Self, DBV4Error> where Self: Sized;
}

pub trait WalEntryBuilder<'a> {
    type Entry: WalEntry;

    fn add_edge(
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        eid: EID,
        layer_id: usize,
        t_props: Cow<'a, Vec<(usize, Prop)>>,
        c_props: Cow<'a, Vec<(usize, Prop)>>,
    ) -> Self::Entry;

    fn add_node_id(gid: GID, vid: VID) -> Self::Entry;

    fn add_const_prop_ids(props: Vec<(Cow<'a, str>, usize)>) -> Self::Entry;

    fn add_temporal_prop_ids(props: Vec<(Cow<'a, str>, usize)>) -> Self::Entry;

    fn add_layer_id(name: String, id: usize) -> Self::Entry;

    fn checkpoint(lsn: LSN) -> Self::Entry;
}
