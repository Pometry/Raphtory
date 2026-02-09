use std::path::Path;

use crate::{
    error::StorageError,
    wal::{LSN, ReplayRecord, WalOps},
};

/// `NoWAL` is a no-op WAL implementation that discards all writes.
/// Used for in-memory only graphs.
#[derive(Debug, Copy, Clone)]
pub struct NoWal;

impl WalOps for NoWal {
    type Config = ();
    fn new(_dir: Option<&Path>, _config: ()) -> Result<Self, StorageError> {
        Ok(Self)
    }

    fn load(_dir: Option<&Path>, _config: ()) -> Result<Self, StorageError> {
        Ok(Self)
    }

    fn append(&self, _data: &[u8]) -> Result<LSN, StorageError> {
        Ok(0)
    }

    fn flush(&self, _lsn: LSN) -> Result<(), StorageError> {
        Ok(())
    }

    fn rotate(&self, _cutoff_lsn: LSN) -> Result<(), StorageError> {
        Ok(())
    }

    fn replay(&self) -> impl Iterator<Item = Result<ReplayRecord, StorageError>> {
        let error = "Recovery is not supported for NoWAL";
        std::iter::once(Err(StorageError::GenericFailure(error.to_string())))
    }

    fn has_entries(&self) -> Result<bool, StorageError> {
        Ok(false)
    }
}
