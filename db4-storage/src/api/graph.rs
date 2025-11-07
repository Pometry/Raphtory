use std::fmt::Debug;
use std::path::Path;
use crate::error::StorageError;
use raphtory_core::entities::properties::tprop::TPropCell;
use raphtory_api::core::entities::properties::prop::Prop;
use parking_lot::{RwLockReadGuard, RwLockWriteGuard};
use crate::segments::graph::segment::MemGraphSegment;

pub trait GraphSegmentOps: Send + Sync + Debug + 'static
where
    Self: Sized,
{
    type Entry<'a>: GraphEntryOps<'a>;

    fn new() -> Self;

    fn load(path: impl AsRef<Path>) -> Result<Self, StorageError>;

    fn head(&self) -> RwLockReadGuard<'_, MemGraphSegment>;

    fn head_mut(&self) -> RwLockWriteGuard<'_, MemGraphSegment>;

    fn entry(&self) -> Self::Entry<'_>;
}

/// Methods for reading graph properties and metadata from storage.
pub trait GraphEntryOps<'a>: Send + Sync + 'a {
    fn get_temporal_prop(&self, prop_id: usize) -> Option<TPropCell<'_>>;

    fn get_metadata(&self, prop_id: usize) -> Option<Prop>;
}
