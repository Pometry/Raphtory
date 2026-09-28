use crate::{
    error::StorageError, pages::locked::node_type_index::WriteLockedNodeTypeIndex,
    segments::node_type_index::MemNodeTypeIndex,
};
use parking_lot::{RwLockWriteGuard, RawRwLock};
use std::{fmt::Debug, ops::DerefMut, path::Path, sync::Arc};
use lock_api::ArcRwLockReadGuard;

pub trait NodeTypeIndexOps: Send + Sync + Debug + 'static
where
    Self: Sized,
{
    type Extension;

    type Entry;

    fn new(path: Option<&Path>, ext: Self::Extension) -> Result<Self, StorageError>;

    fn load(path: impl AsRef<Path>, ext: Self::Extension) -> Result<Self, StorageError>;

    fn head_shared(&self) -> ArcRwLockReadGuard<RawRwLock, MemNodeTypeIndex>;

    fn head_exclusive(&self) -> RwLockWriteGuard<'_, MemNodeTypeIndex>;

    fn entry(&self, type_ids: &[usize]) -> Self::Entry;

    /// Returns `true` if the index has no `(type_id, VID)` entries.
    fn is_empty(&self) -> bool;

    fn est_size(&self) -> usize;

    fn is_dirty(&self) -> bool;

    fn set_dirty(&self, dirty: bool);

    fn notify_write(&self);

    fn write_locked(self: &Arc<Self>) -> WriteLockedNodeTypeIndex<Self>;

    fn flush(&self) -> Result<(), StorageError> {
        let head = self.head_exclusive();
        self.flush_with_head(head)
    }

    fn flush_with_head(
        &self,
        head: impl DerefMut<Target = MemNodeTypeIndex>,
    ) -> Result<(), StorageError>;

    fn copy_to(&self, dst: &Path) -> Result<(), StorageError>;
}
