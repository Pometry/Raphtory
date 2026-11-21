use crate::{
    error::StorageError, pages::locked::node_type_index::WriteLockedNodeTypeIndex,
    segments::node_type_index::MemNodeTypeIndex,
};
use parking_lot::RwLockWriteGuard;
use raphtory_api::{ArcRwLockReadGuard, ArcRwLockWriteGuard};
use std::{fmt::Debug, ops::DerefMut, path::Path, sync::Arc};

pub trait NodeTypeIndexOps: Send + Sync + Debug + 'static
where
    Self: Sized,
{
    type Extension;

    type Entry;

    fn new(path: Option<&Path>, ext: Self::Extension) -> Result<Self, StorageError>;

    fn load(path: impl AsRef<Path>, ext: Self::Extension) -> Result<Self, StorageError>;

    /// **Deadlock safety:** node type index must always be locked before node segments.
    fn head_shared_arc(&self) -> ArcRwLockReadGuard<MemNodeTypeIndex>;

    /// **Deadlock safety:** node type index must always be locked before node segments.
    fn head_exclusive(&self) -> RwLockWriteGuard<'_, MemNodeTypeIndex>;

    /// **Deadlock safety:** node type index must always be locked before node segments.
    fn head_exclusive_arc(&self) -> ArcRwLockWriteGuard<MemNodeTypeIndex>;

    fn entry(&self, type_ids: &[usize]) -> Self::Entry;

    /// Returns `true` if the index has no `(type_id, VID)` entries.
    fn is_empty(&self) -> bool;

    /// Returns `true` if queries can be served from the index.
    fn is_queryable(&self) -> bool;

    /// The size of `head` in bytes, cached so it can be read without locking.
    fn est_size_cached(&self) -> usize;

    /// Cache the size of `head` so it can be read without locking.
    fn set_est_size_cached(&self, size: usize);

    fn is_dirty(&self) -> bool;

    fn set_dirty(&self, dirty: bool);

    /// Called to decide if the index should be flushed to disk.
    /// If `head` is None, may try to acquire an exclusive lock on `head` to flush.
    fn notify_write(&self, head: Option<impl DerefMut<Target = MemNodeTypeIndex>>);

    fn write_locked(self: &Arc<Self>) -> WriteLockedNodeTypeIndex<Self>;

    fn flush(&self, head: impl DerefMut<Target = MemNodeTypeIndex>) -> Result<(), StorageError>;

    fn copy_to(&self, dst: &Path) -> Result<(), StorageError>;
}
