use std::path::{Path, PathBuf};

use crate::error::DBV4Error;
use crate::wal::{LSN, Wal, WalRecord};

pub struct NoWal {
    dir: PathBuf,
}

impl Wal for NoWal {
    fn new(dir: impl AsRef<Path>) -> Result<Self, DBV4Error> {
        Ok(Self { dir: dir.as_ref().to_path_buf() })
    }

    fn dir(&self) -> &Path {
        &self.dir
    }

    fn reserve(&self) -> LSN {
        0
    }

    fn append_with_lsn(&self, _lsn: LSN, _data: &[u8]) -> Result<(), DBV4Error> {
        Ok(())
    }

    fn wait_for_sync(&self, _lsn: LSN) {}

    fn rotate(&self, _cutoff_lsn: LSN) -> Result<(), DBV4Error> {
        Ok(())
    }

    fn replay(_dir: impl AsRef<Path>) -> impl Iterator<Item = Result<WalRecord, DBV4Error>> {
        let error = "Recovery is not supported for NoWAL";
        std::iter::once(Err(DBV4Error::GenericFailure(error.to_string())))
    }
}
