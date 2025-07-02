use std::path::Path;

use crate::error::DBV4Error;

pub mod no_wal;

pub type LSN = u64;

pub struct WalRow {
    pub lsn: LSN,
    pub data: Vec<u8>,
}

pub trait WalOps {
    fn new(dir: impl AsRef<Path>) -> Result<Self, DBV4Error>
    where
        Self: Sized;

    fn dir(&self) -> &Path;

    fn append(&self, data: &[u8]) -> Result<LSN, DBV4Error>;

    /// Blocks until the WAL has fsynced the given LSN to disk.
    fn wait_for_sync(&self, lsn: LSN);

    fn recover(dir: impl AsRef<Path>) -> impl Iterator<Item = Result<WalRow, DBV4Error>>;
}
