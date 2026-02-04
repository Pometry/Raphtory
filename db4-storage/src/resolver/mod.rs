use crate::error::StorageError;
use raphtory_api::core::{
    entities::{GidRef, GidType, VID},
    storage::dict_mapper::MaybeNew,
};
use std::path::Path;

pub mod mapping_resolver;

pub trait GIDResolverOps {
    fn new() -> Result<Self, StorageError>
    where
        Self: Sized;

    fn new_with_path(path: impl AsRef<Path>, dtype: Option<GidType>) -> Result<Self, StorageError>
    where
        Self: Sized;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn dtype(&self) -> Option<GidType>;

    fn set(&self, gid: GidRef, vid: VID) -> Result<(), StorageError>;

    fn get_or_init<NFN: FnMut() -> VID>(
        &self,
        gid: GidRef,
        next_id: NFN,
    ) -> Result<MaybeNew<VID>, StorageError>;

    fn validate_gids<'a>(
        &self,
        gids: impl IntoIterator<Item = GidRef<'a>>,
    ) -> Result<(), StorageError>;

    fn get_str(&self, gid: &str) -> Option<VID>;

    fn get_u64(&self, gid: u64) -> Option<VID>;

    fn bulk_set_str<S: AsRef<str>>(
        &self,
        gids: impl IntoIterator<Item = (S, VID)>,
    ) -> Result<(), StorageError>;

    fn bulk_set_u64(&self, gids: impl IntoIterator<Item = (u64, VID)>) -> Result<(), StorageError>;

    fn iter_str(&self) -> impl Iterator<Item = (String, VID)> + '_;

    fn iter_u64(&self) -> impl Iterator<Item = (u64, VID)> + '_;
}
