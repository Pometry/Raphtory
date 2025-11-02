use std::fmt::Debug;
use std::path::Path;
use crate::error::StorageError;
use raphtory_core::entities::properties::props::MetadataError;
use raphtory_core::entities::properties::tprop::IllegalPropType;
use raphtory_core::storage::timeindex::TimeIndexEntry;
use raphtory_core::entities::properties::tprop::TPropCell;
use raphtory_api::core::entities::properties::prop::Prop;


pub trait GraphSegmentOps: Send + Sync + Debug + 'static
where
    Self: Sized,
{
    type Entry<'a>: GraphEntryOps<'a>;

    fn new() -> Self;

    fn load(path: impl AsRef<Path>) -> Result<Self, StorageError>;

    fn entry(&self) -> Self::Entry<'_>;
}

/// Methods for reading graph properties and metadata from storage.
pub trait GraphEntryOps<'a>: Send + Sync + 'a {
    fn get_temporal_prop(&self, prop_id: usize) -> Option<TPropCell<'_>>;

    fn get_metadata(&self, prop_id: usize) -> Option<Prop>;
}

/// Methods for writing graph properties and metadata to storage.
pub trait GraphEntryMutOps: Send + Sync {
    fn add_prop(
        &self,
        t: TimeIndexEntry,
        prop_id: usize,
        prop: Prop,
    ) -> Result<(), IllegalPropType>;

    fn add_metadata(&self, prop_id: usize, prop: Prop) -> Result<(), MetadataError>;

    fn update_metadata(&self, prop_id: usize, prop: Prop);
}
