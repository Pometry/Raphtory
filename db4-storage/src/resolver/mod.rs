use crate::error::StorageError;
use raphtory_api::core::entities::{GidRef, GidType, VID};
use std::path::Path;

pub mod mapping_resolver;

/// Either an initialiser or a `VID`. For equality checks, only VIDs are compared, initialisers are
/// never considered equal.
pub enum MaybeInit<I> {
    VID(VID),
    Init(I),
}

impl<I> MaybeInit<I> {
    pub fn needs_init(&self) -> bool {
        matches!(self, MaybeInit::Init(_))
    }
}

impl<I> PartialEq for MaybeInit<I> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (MaybeInit::VID(left), MaybeInit::VID(right)) => left == right,
            _ => false,
        }
    }
}

pub trait Initialiser {
    fn init(self, vid: VID) -> Result<(), StorageError>;
}

pub trait GIDResolverOps {
    type Init<'a>: Initialiser
    where
        Self: 'a;
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

    fn get_or_init<'a>(
        &'a self,
        gid: GidRef<'a>,
    ) -> Result<MaybeInit<Self::Init<'a>>, StorageError>;

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
