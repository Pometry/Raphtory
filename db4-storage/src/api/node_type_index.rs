use crate::{
    error::StorageError, pages::locked::node_type_index::WriteLockedNodeTypeIndex,
    segments::node_type_index::MemNodeTypeIndex,
};
use ahash::RandomState;
use indexmap::IndexSet;
use parking_lot::{RawRwLock, RwLockReadGuard, RwLockWriteGuard, lock_api::ArcRwLockWriteGuard};
use raphtory_core::entities::VID;
use std::{fmt::Debug, ops::DerefMut, path::Path, sync::Arc};

pub trait NodeTypeIndexOps: Send + Sync + Debug + 'static
where
    Self: Sized,
{
    type Extension;

    fn new(path: Option<&Path>, ext: Self::Extension) -> Self;

    fn load(path: impl AsRef<Path>, ext: Self::Extension) -> Result<Self, StorageError>;

    fn head_shared(&self) -> RwLockReadGuard<'_, MemNodeTypeIndex>;

    fn head_exclusive(&self) -> RwLockWriteGuard<'_, MemNodeTypeIndex>;

    fn head_exclusive_arc(&self) -> ArcRwLockWriteGuard<RawRwLock, MemNodeTypeIndex>;

    /// Returns the sorted `VID`s of nodes whose type is in `type_ids`.
    // TODO: See if we can return an iterator here instead.
    fn nodes_of_type(&self, type_ids: &[usize]) -> IndexSet<VID, RandomState>;

    /// Returns `true` if the index has no `(type_id, VID)` entries.
    fn is_empty(&self) -> bool;

    fn est_size(&self) -> usize;

    fn is_dirty(&self) -> bool;

    fn set_dirty(&self, dirty: bool);

    fn notify_write(&self);

    fn write_locked(self: &Arc<Self>) -> WriteLockedNodeTypeIndex<Self>;

    fn flush(
        &self,
        head_exclusive: impl DerefMut<Target = MemNodeTypeIndex>,
    ) -> Result<(), StorageError>;

    fn copy_to(&self, dst: &Path) -> Result<(), StorageError>;
}
