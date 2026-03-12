use crate::{error::StorageError, wal::LSN};
use std::path::Path;

#[derive(Debug)]
pub enum DBState {
    Running,
    Shutdown,
    CrashRecovery,
    NotSupported,
}

pub trait ControlFileOps: Sized {
    fn load(dir: &Path) -> Result<Self, StorageError>;

    fn save(&self) -> Result<(), StorageError>;

    fn db_state(&self) -> &DBState;

    fn last_checkpoint(&self) -> LSN;

    fn set_db_state(&self, state: DBState);

    fn set_checkpoint(&self, lsn: LSN);
}

#[derive(Debug, Clone)]
pub struct NoControlFile;

impl ControlFileOps for NoControlFile {
    fn load(_dir: &Path) -> Result<Self, StorageError> {
        Ok(NoControlFile)
    }

    fn save(&self) -> Result<(), StorageError> {
        Ok(())
    }

    fn db_state(&self) -> &DBState {
        &DBState::NotSupported
    }

    fn last_checkpoint(&self) -> LSN {
        0
    }

    fn set_db_state(&self, state: DBState) {}

    fn set_checkpoint(&self, lsn: LSN) {}
}
