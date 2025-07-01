use std::path::Path;

use crate::error::DBV4Error;
use raphtory_api::core::{
    entities::{GidRef, GidType, VID},
    storage::dict_mapper::MaybeNew,
};
use raphtory_core::entities::graph::logical_to_physical::InvalidNodeId;

pub mod mapping_resolver;

#[derive(thiserror::Error, Debug)]
pub enum GIDResolverError {
    #[error(transparent)]
    DBV4Error(#[from] DBV4Error),
    #[error(transparent)]
    InvalidNodeId(#[from] InvalidNodeId),
}

pub trait GIDResolverOps {
    fn new(path: impl AsRef<Path>) -> Result<Self, GIDResolverError>
    where
        Self: Sized;
    fn len(&self) -> usize;
    fn dtype(&self) -> Option<GidType>;
    fn set(&self, gid: GidRef, vid: VID) -> Result<(), GIDResolverError>;
    fn get_or_init(
        &self,
        gid: GidRef,
        next_id: impl FnOnce() -> VID,
    ) -> Result<MaybeNew<VID>, GIDResolverError>;
    fn validate_gids<'a>(
        &self,
        gids: impl IntoIterator<Item = GidRef<'a>>,
    ) -> Result<(), GIDResolverError>;
    fn get_str(&self, gid: &str) -> Option<VID>;
    fn get_u64(&self, gid: u64) -> Option<VID>;
}
