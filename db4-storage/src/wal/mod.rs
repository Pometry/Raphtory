use std::path::Path;

use crate::error::DBV4Error;

pub mod no_wal;
pub mod entries;

pub type LSN = u64;

#[derive(Debug)]
pub struct WalRecord {
    pub lsn: LSN,
    pub data: Vec<u8>,
}

pub trait WalOps {
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

    fn replay(dir: impl AsRef<Path>) -> impl Iterator<Item = Result<WalRecord, DBV4Error>>;
}
