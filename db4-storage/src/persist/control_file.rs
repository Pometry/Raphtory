use crate::{error::StorageError, wal::LSN};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum DBState {
    Running,
    Shutdown,
    CrashRecovery,
    NotSupported,
}

// Starting value for `last_checkpoint` in the control file.
pub const LAST_CHECKPOINT_INIT: LSN = 0;

pub trait ControlFileOps: Sized {
    fn load(dir: &Path) -> Result<Self, StorageError>;

    fn save(&self) -> Result<(), StorageError>;

    fn db_state(&self) -> DBState;

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

    fn db_state(&self) -> DBState {
        DBState::NotSupported
    }

    fn last_checkpoint(&self) -> LSN {
        0
    }

    fn set_db_state(&self, _state: DBState) {}

    fn set_checkpoint(&self, _lsn: LSN) {}
}
