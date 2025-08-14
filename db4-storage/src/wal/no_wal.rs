use std::path::{Path, PathBuf};

use crate::{
    error::StorageError,
    wal::{LSN, Wal, WalRecord},
};

/// NoWAL is a no-op WAL implementation that discards all writes.
/// Used for in-memory only graphs.
#[derive(Debug)]
pub struct NoWal {
    dir: PathBuf,
}

impl Wal for NoWal {
    fn new(dir: impl AsRef<Path>) -> Result<Self, StorageError> {
        Ok(Self {
            dir: dir.as_ref().to_path_buf(),
        })
    }

    fn dir(&self) -> &Path {
        &self.dir
    }

    fn append(&self, _data: &[u8]) -> Result<LSN, StorageError> {
        Ok(0)
    }

    fn sync(&self) -> Result<(), StorageError> {
        Ok(())
    }

    fn wait_for_sync(&self, _lsn: LSN) {}

    fn rotate(&self, _cutoff_lsn: LSN) -> Result<(), StorageError> {
        Ok(())
    }

    fn replay(_dir: impl AsRef<Path>) -> impl Iterator<Item = Result<WalRecord, StorageError>> {
        let error = "Recovery is not supported for NoWAL";
        std::iter::once(Err(StorageError::GenericFailure(error.to_string())))
    }
}
