use crate::{error::StorageError, segments::node_type_index::index::MemNodeTypeIndex};
use parking_lot::{RwLockReadGuard, RwLockWriteGuard};
use raphtory_api::core::storage::ArcRwLockReadGuard;
use std::{fmt::Debug, path::Path};

pub trait NodeTypeIndexOps: Send + Sync + Debug + 'static
where
    Self: Sized,
{
    type Extension;

    type NodeTypeEntry<'a>
    where
        Self: 'a;

    type ArcNodeTypeEntry;

    fn new(path: Option<&Path>, ext: Self::Extension) -> Self;

    fn load(path: impl AsRef<Path>, ext: Self::Extension) -> Result<Self, StorageError>;

    fn head(&self) -> RwLockReadGuard<'_, MemNodeTypeIndex>;

    fn head_arc(&self) -> ArcRwLockReadGuard<MemNodeTypeIndex>;

    fn head_mut(&self) -> RwLockWriteGuard<'_, MemNodeTypeIndex>;

    fn node_type_entry(&self, type_ids: &[usize]) -> Self::NodeTypeEntry<'_>;
    fn arc_node_type_entry(&self, type_ids: &[usize]) -> Self::ArcNodeTypeEntry;

    /// Returns `true` if the index has no `(type_id, VID)` entries.
    fn is_empty(&self) -> bool;

    fn est_size(&self) -> usize;

    fn is_dirty(&self) -> bool;

    fn set_dirty(&self, dirty: bool);

    fn notify_write(&self);

    fn flush(&self) -> Result<(), StorageError>;
}
