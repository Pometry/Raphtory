use crate::{error::StorageError, segments::node_type_index::index::MemNodeTypeIndex};
use parking_lot::{RwLockReadGuard, RwLockWriteGuard};
use raphtory_core::entities::VID;
use std::{fmt::Debug, path::Path};

pub trait NodeTypeIndexOps: Send + Sync + Debug + 'static
where
    Self: Sized,
{
    type Extension;

    fn new(path: Option<&Path>, ext: Self::Extension) -> Self;

    fn load(path: impl AsRef<Path>, ext: Self::Extension) -> Result<Self, StorageError>;

    fn head(&self) -> RwLockReadGuard<'_, MemNodeTypeIndex>;

    fn head_mut(&self) -> RwLockWriteGuard<'_, MemNodeTypeIndex>;

    /// Returns the `VID`s of nodes with the given type.
    fn nodes_of_type(&self, type_id: usize) -> Vec<VID>;

    fn est_size(&self) -> usize;

    fn is_dirty(&self) -> bool;

    fn set_dirty(&self, dirty: bool);

    fn notify_write(&self);

    fn flush(&self) -> Result<(), StorageError>;
}
