use std::fmt::Debug;
use std::path::Path;
use crate::error::StorageError;

pub trait GraphPropSegmentOps: Send + Sync + Debug + 'static
where
    Self: Sized,
{
    fn new() -> Self;

    fn load(path: impl AsRef<Path>) -> Result<Self, StorageError>;
}
