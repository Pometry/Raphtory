use crate::error::StorageError;
use crate::segments::graph::segment::MemGraphProps;
use parking_lot::{RwLockReadGuard, RwLockWriteGuard};
use raphtory_api::core::entities::properties::meta::Meta;
use raphtory_api::core::entities::properties::prop::Prop;
use raphtory_api::core::entities::properties::tprop::TPropOps;
use std::fmt::Debug;
use std::path::Path;
use std::sync::Arc;

pub trait GraphPropOps: Send + Sync + Debug + 'static
where
    Self: Sized,
{
    type Extension;

    type Entry<'a>: GraphEntryOps<'a>;

    fn new(meta: Arc<Meta>, path: Option<&Path>, ext: Self::Extension) -> Self;

    fn load(
        meta: Arc<Meta>,
        path: impl AsRef<Path>,
        ext: Self::Extension,
    ) -> Result<Self, StorageError>;

    fn head(&self) -> RwLockReadGuard<'_, MemGraphProps>;

    fn head_mut(&self) -> RwLockWriteGuard<'_, MemGraphProps>;

    fn entry(&self) -> Self::Entry<'_>;

    fn increment_est_size(&self, size: usize);

    fn est_size(&self) -> usize;

    fn mark_dirty(&self);

    fn notify_write(
        &self,
        mem_segment: &mut RwLockWriteGuard<'_, MemGraphProps>,
    ) -> Result<(), StorageError>;
}

/// Methods for reading graph properties and metadata from storage.
pub trait GraphEntryOps<'a>: Send + Sync + 'a {
    type Ref<'b>: GraphRefOps<'b>
    where
        'a: 'b,
        Self: 'b;

    fn as_ref<'b>(&'b self) -> Self::Ref<'b>
    where
        'a: 'b;
}

/// Lightweight reference for reading graph properties and metadata.
pub trait GraphRefOps<'a>: Copy + Clone + Send + Sync + 'a {
    type TProp: TPropOps<'a>;

    fn get_temporal_prop(self, prop_id: usize) -> Option<Self::TProp>;

    fn get_metadata(self, prop_id: usize) -> Option<Prop>;
}
