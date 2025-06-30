use std::path::Path;
use raphtory_api::core::{
    entities::{GidRef, VID, GidType},
    storage::dict_mapper::MaybeNew,
};
use raphtory_core::entities::graph::logical_to_physical::{Mapping};
use crate::resolver::{GIDResolverOps, GIDResolverError};

#[derive(Debug)]
pub struct MappingResolver {
    mapping: Mapping,
}

impl GIDResolverOps for MappingResolver {
    fn new(_path: impl AsRef<Path>) -> Result<Self, GIDResolverError> {
        Ok(Self { mapping: Mapping::new() })
    }

    fn len(&self) -> usize {
        self.mapping.len()
    }

    fn dtype(&self) -> Option<GidType> {
        self.mapping.dtype()
    }

    fn set(&self, gid: GidRef, vid: VID) -> Result<(), GIDResolverError> {
        self.mapping.set(gid, vid)?;
        Ok(())
    }

    fn get_or_init(
        &self,
        gid: GidRef,
        next_id: impl FnOnce() -> VID,
    ) -> Result<MaybeNew<VID>, GIDResolverError> {
        let result = self.mapping.get_or_init(gid, next_id)?;
        Ok(result)
    }

    fn validate_gids<'a>(
        &self,
        gids: impl IntoIterator<Item = GidRef<'a>>,
    ) -> Result<(), GIDResolverError> {
        let result = self.mapping.validate_gids(gids)?;
        Ok(result)
    }

    fn get_str(&self, gid: &str) -> Option<VID> {
        self.mapping.get_str(gid)
    }

    fn get_u64(&self, gid: u64) -> Option<VID> {
        self.mapping.get_u64(gid)
    }
}
