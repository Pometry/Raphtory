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

    /// Open a resolver in read-only mode against an existing path.
    ///
    /// Multiple read-only resolvers can attach to the same directory
    /// concurrently and may coexist with a separate writer. Writes through
    /// a read-only resolver return errors.
    ///
    /// The default implementation falls back to `new_with_path` for
    /// backends that do not need a separate read-only path.
    fn new_readonly_with_path(
        path: impl AsRef<Path>,
        dtype: Option<GidType>,
    ) -> Result<Self, StorageError>
    where
        Self: Sized,
    {
        Self::new_with_path(path, dtype)
    }

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

    fn get(&self, gid: GidRef) -> Option<VID> {
        match gid {
            GidRef::Str(s) => self.get_str(s),
            GidRef::U64(u) => self.get_u64(u),
        }
    }

    fn flush(&self) -> Result<(), StorageError>;
}
