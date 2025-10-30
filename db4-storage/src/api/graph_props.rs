use std::fmt::Debug;
use std::path::Path;
use crate::error::StorageError;
use raphtory_core::entities::properties::props::MetadataError;
use raphtory_core::storage::locked_view::LockedView;
use raphtory_core::entities::properties::tprop::TProp;
use raphtory_core::entities::properties::tprop::IllegalPropType;
use raphtory_core::storage::timeindex::TimeIndexEntry;
use raphtory_api::core::entities::properties::prop::Prop;

pub trait GraphPropSegmentOps: Send + Sync + Debug + 'static
where
    Self: Sized,
{
    fn new() -> Self;

    fn load(path: impl AsRef<Path>) -> Result<Self, StorageError>;
}

/// Methods for reading/writing graph properties and graph metadata from storage.
pub trait GraphPropOps: Send + Sync {
    fn get_temporal_prop(&self, prop_id: usize) -> Option<LockedView<'_, TProp>>;

    fn add_prop(
        &self,
        t: TimeIndexEntry,
        prop_id: usize,
        prop: Prop,
    ) -> Result<(), IllegalPropType>;

    fn get_metadata(&self, id: usize) -> Option<Prop>;

    fn add_metadata(&self, prop_id: usize, prop: Prop) -> Result<(), MetadataError>;

    fn update_metadata(&self, prop_id: usize, prop: Prop);
}
