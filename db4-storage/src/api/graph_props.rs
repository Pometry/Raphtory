use crate::{error::StorageError, segments::graph_prop::segment::MemGraphPropSegment, wal::LSN};
use parking_lot::{RwLockReadGuard, RwLockWriteGuard};
use raphtory_api::core::entities::properties::{meta::Meta, prop::Prop, tprop::TPropOps};
use std::{fmt::Debug, path::Path, sync::Arc};

pub trait GraphPropSegmentOps: Send + Sync + Debug + 'static
where
    Self: Sized,
{
    type Extension;

    type Entry<'a>: GraphPropEntryOps<'a>;

    fn new(meta: Arc<Meta>, path: Option<&Path>, ext: Self::Extension) -> Self;

    fn load(
        meta: Arc<Meta>,
        path: impl AsRef<Path>,
        ext: Self::Extension,
    ) -> Result<Self, StorageError>;

    fn head(&self) -> RwLockReadGuard<'_, MemGraphPropSegment>;

    fn head_mut(&self) -> RwLockWriteGuard<'_, MemGraphPropSegment>;

    fn entry(&self) -> Self::Entry<'_>;

    fn increment_est_size(&self, size: usize);

    fn est_size(&self) -> usize;

    fn set_dirty(&self, dirty: bool);

    /// Returns the latest lsn for the immutable part of this segment.
    fn immut_lsn(&self) -> LSN;

    fn notify_write(
        &self,
        mem_segment: &mut RwLockWriteGuard<'_, MemGraphPropSegment>,
    ) -> Result<(), StorageError>;

    fn flush(&self) -> Result<(), StorageError>;
}

/// Trait for returning a guard-free, copyable reference to graph properties
/// and metadata.
pub trait GraphPropEntryOps<'a>: Send + Sync + 'a {
    type Ref<'b>: GraphPropRefOps<'b>
    where
        'a: 'b,
        Self: 'b;

    fn as_ref<'b>(&'b self) -> Self::Ref<'b>
    where
        'a: 'b;
}

/// Methods for reading graph properties and metadata from a reference on storage.
pub trait GraphPropRefOps<'a>: Copy + Clone + Send + Sync + 'a {
    type TProps: TPropOps<'a>;

    fn get_temporal_prop(self, prop_id: usize) -> Self::TProps;

    fn get_metadata(self, prop_id: usize) -> Option<Prop>;
}
