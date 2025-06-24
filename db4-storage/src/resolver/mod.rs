use std::path::Path;
use thiserror::Error;

pub trait ResolverEntryOps<K, V>: Send + Sync {
    fn set(&self, value: impl Into<V>) -> Result<(), ResolverError<K, V>>;
}

pub trait ResolverOps<K, V>: Send + Sync {
    type Entry: ResolverEntryOps<K, V>;

    fn new(path: impl AsRef<Path>) -> Self
    where
        Self: Sized;

    fn generate_id(&self) -> V;

    /// Reserve an entry for a key in the resolver.
    fn entry(&self, key: impl Into<K>) -> Result<Self::Entry, ResolverError<K, V>>;

    fn get(&self, key: impl AsRef<K>) -> Option<V>;
}

#[derive(Debug, Error)]
pub enum ResolverError<K, V> {
    #[error("Key {0} already exists with value {1}")]
    KeyExists(K, V),

    #[error("Operation failed: {0}")]
    OperationFailed(String),
}
