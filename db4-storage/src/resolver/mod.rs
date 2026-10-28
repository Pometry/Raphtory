use crate::error::StorageError;
use arrow_array::Array;
use raphtory_api::core::entities::{GidRef, GidType, VID};
use std::path::Path;

pub mod batch;
pub mod mapping_resolver;

pub use batch::{GidCol, VidAllocator};

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

    /// Resolves a bulk-load chunk: `out` is resized to `2 * src.len()` holding the src VIDs
    /// followed by the dst VIDs. New nodes get VIDs from `alloc`, and `new_nodes` is cleared and
    /// filled with `(row, vid)` for the first row of every node created, where `row` indexes the
    /// concatenated `[src, dst]` columns.
    ///
    /// Callers must hold the graph's exclusive write lock: no online inserts may run concurrently.
    fn resolve_batch(
        &self,
        src: &dyn Array,
        dst: &dyn Array,
        alloc: &dyn VidAllocator,
        out: &mut Vec<VID>,
        new_nodes: &mut Vec<(usize, VID)>,
    ) -> Result<(), StorageError> {
        let n = src.len();
        assert_eq!(n, dst.len(), "src and dst must have the same length");
        let src = GidCol::new(src)?;
        let dst = GidCol::new(dst)?;
        out.clear();
        new_nodes.clear();
        for row in 0..2 * n {
            let gid = if row < n {
                src.get(row)
            } else {
                dst.get(row - n)
            };
            let vid = match self.get_or_init(gid)? {
                MaybeInit::VID(vid) => vid,
                MaybeInit::Init(init) => {
                    let (vid, _) = alloc.reserve(row, 1);
                    init.init(vid)?;
                    new_nodes.push((row, vid));
                    vid
                }
            };
            out.push(vid);
        }
        Ok(())
    }

    fn flush(&self) -> Result<(), StorageError>;

    /// Copy this resolver to `path`.
    fn copy_to(&self, _path: impl AsRef<Path>) -> Result<(), StorageError>;
}
