use std::path::Path;

use raphtory::core::entities::{EID, VID};

pub mod error {
    use std::{path::PathBuf, sync::Arc};

    use raphtory::{
        api::core::entities::properties::prop::PropError, core::utils::time::ParseTimeError,
        errors::LoadError,
    };

    #[derive(thiserror::Error, Debug)]
    pub enum DBV4Error {
        #[error("External Storage Error {0}")]
        External(#[from] Arc<dyn std::error::Error + Send + Sync>),
        #[error("IO error: {0}")]
        IO(#[from] std::io::Error),
        #[error("Serde error: {0}")]
        Serde(#[from] serde_json::Error),
        #[error("Load error: {0}")]
        LoadError(#[from] LoadError),
        #[error("Arrow-rs error: {0}")]
        ArrowRS(#[from] arrow_schema::ArrowError),
        #[error("Parquet error: {0}")]
        Parquet(#[from] parquet::errors::ParquetError),

        #[error("Property error: {0}")]
        PropError(#[from] PropError),
        #[error("Empty Graph: {0}")]
        EmptyGraphDir(PathBuf),
        #[error("Failed to parse time string")]
        ParseTime {
            #[from]
            source: ParseTimeError,
        },
        #[error("Unnamed Failure: {0}")]
        GenericFailure(String),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct LocalPOS(pub usize);

impl LocalPOS {
    pub fn as_vid(self, page_id: usize, max_page_len: usize) -> VID {
        VID(page_id * max_page_len + self.0)
    }

    pub fn as_eid(self, page_id: usize, max_page_len: usize) -> EID {
        EID(page_id * max_page_len + self.0)
    }
}

impl From<usize> for LocalPOS {
    fn from(pos: usize) -> Self {
        Self(pos)
    }
}

pub fn calculate_size_recursive(path: &Path) -> Result<usize, std::io::Error> {
    let mut size = 0;
    if path.is_dir() {
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                size += calculate_size_recursive(&path)?;
            } else {
                size += path.metadata()?.len() as usize;
            }
        }
    } else {
        size += path.metadata()?.len() as usize;
    }
    Ok(size)
}
