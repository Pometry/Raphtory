use std::path::Path;
use crate::{error::StorageError, wal::LSN};

#[derive(Debug)]
pub enum DBState {
    Running,
    Shutdown,
    CrashRecovery,
}

pub trait ControlFileOps: Sized {
    fn load_from_dir(dir: &Path) -> Result<Self, StorageError>;

    fn save_to_dir(&self, dir: &Path) -> Result<(), StorageError>;

    fn db_state(&self) -> &DBState;

    fn last_checkpoint(&self) -> LSN;

    fn set_db_state(&self, state: DBState) -> Result<(), StorageError>;

    fn set_last_checkpoint(&self, lsn: LSN) -> Result<(), StorageError>;
}

#[derive(Debug, Clone)]
pub struct NoControlFile;

impl ControlFileOps for NoControlFile {
    fn load_from_dir(_dir: &Path) -> Result<Self, StorageError> {
        Ok(NoControlFile)
    }

    fn save_to_dir(&self, _dir: &Path) -> Result<(), StorageError> {
        Ok(())
    }

    fn db_state(&self) -> &DBState {
        // Without a control file there is no persistence, hence this always reports a clean
        // shutdown state so that no recovery is attempted.
        &DBState::Shutdown
    }

    fn last_checkpoint(&self) -> LSN {
        0
    }

    fn set_db_state(&self, state: DBState) -> Result<(), StorageError> {
        Ok(())
    }

    fn set_last_checkpoint(&self, lsn: LSN) -> Result<(), StorageError> {
        Ok(())
    }
}
