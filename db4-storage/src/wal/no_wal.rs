use crate::{
    error::StorageError,
    wal::{LSN, ReplayRecord, WalOps},
};

/// `NoWAL` is a no-op WAL implementation that discards all writes.
/// Used for in-memory only graphs.
#[derive(Debug, Copy, Clone)]
pub struct NoWal;

impl WalOps for NoWal {
    fn append(&self, _data: &[u8]) -> Result<LSN, StorageError> {
        Ok(0)
    }

    fn flush(&self, _lsn: LSN) -> Result<(), StorageError> {
        Ok(())
    }

    fn replay(&self, _start: LSN) -> impl Iterator<Item = Result<ReplayRecord, StorageError>> {
        let error = "Recovery is not supported for NoWAL";
        std::iter::once(Err(StorageError::GenericFailure(error.to_string())))
    }

    fn read(&self, _lsn: LSN) -> Result<Option<ReplayRecord>, StorageError> {
        Err(StorageError::GenericFailure(
            "read is not supported for NoWAL".to_string(),
        ))
    }

    fn next_lsn(&self) -> LSN {
        0
    }

    fn set_next_lsn(&self, _lsn: LSN) {
        panic!("set_next_lsn is not supported for NoWAL");
    }
}
