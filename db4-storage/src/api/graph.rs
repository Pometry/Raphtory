use std::fmt::Debug;
use std::path::Path;
use crate::error::StorageError;
use raphtory_api::core::entities::properties::prop::Prop;
use parking_lot::{RwLockReadGuard, RwLockWriteGuard};
use crate::segments::graph::segment::MemGraphSegment;
use raphtory_api::core::entities::properties::tprop::TPropOps;

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
    type TProp: TPropOps<'a>;

    fn get_temporal_prop(&'a self, prop_id: usize) -> Option<Self::TProp>;

    fn get_metadata(&'a self, prop_id: usize) -> Option<Prop>;
}
