use std::path::Path;
use crate::{error::StorageError, wal::LSN};

pub const CONTROL_FILE_NAME: &str = "control.json";

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

pub struct NoControlFile;

impl ControlFileOps for NoControlFile {
    fn load_from_dir(_dir: &Path) -> Result<Self, StorageError> {
        Ok(NoControlFile)
    }

    fn save_to_dir(&self, _dir: &Path) -> Result<(), StorageError> {
        Ok(())
    }

    fn db_state(&self) -> &DBState {
        &DBState::Running
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
