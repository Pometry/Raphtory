use crate::errors::{GraphError, LoadError};
use arrow_schema::DataType;
use either::Either;
use raphtory_api::core::{entities::GidType, storage::dict_mapper::MaybeNew};
use raphtory_core::entities::GidRef;
use rayon::prelude::*;
use rustc_hash::FxHasher;
use std::hash::{Hash, Hasher};

/// A sharded gid -> value cache used during bulk loading.
///
/// Each shard is an LRU owned by a single rayon task, so a gid is always looked up by
/// the one shard that owns its hash and no locking is needed.
pub(crate) enum NodeResolveCache<V> {
    U64 {
        len: usize,
        caches: Vec<quick_cache::unsync::Cache<u64, V>>,
    },
    Str {
        len: usize,
        caches: Vec<quick_cache::unsync::Cache<String, V>>,
    },
}

impl<V> NodeResolveCache<V> {
    pub(crate) fn new(chunk_rows: usize, gid_type: GidType) -> Self {
        let num_cores = std::thread::available_parallelism().unwrap().get();
        let rows_per_shard = chunk_rows.max(100_000);

        match gid_type {
            GidType::U64 => {
                let mut caches = Vec::with_capacity(num_cores);

                caches.resize_with(num_cores, || {
                    quick_cache::unsync::Cache::new(rows_per_shard)
                });

                NodeResolveCache::U64 {
                    len: caches.len(),
                    caches,
                }
            }
            GidType::Str => {
                let mut caches = Vec::with_capacity(num_cores);

                caches.resize_with(num_cores, || {
                    quick_cache::unsync::Cache::new(rows_per_shard)
                });

                NodeResolveCache::Str {
                    len: caches.len(),
                    caches,
                }
            }
        }
    }
}

impl<V: Copy + Send> NodeResolveCache<V> {
    pub(crate) fn par_iter_mut(&mut self) -> impl ParallelIterator<Item = LockedCacheShard<'_, V>> {
        match self {
            NodeResolveCache::U64 { len, caches } => {
                let iter = caches
                    .par_iter_mut()
                    .enumerate()
                    .map(move |(shard_id, lock)| LockedCacheShard {
                        cache: CacheShard::U64(lock),
                        id: shard_id as u64,
                        len: *len,
                    });

                Either::Left(iter)
            }
            NodeResolveCache::Str { len, caches } => {
                let iter = caches
                    .par_iter_mut()
                    .enumerate()
                    .map(move |(shard_id, lock)| LockedCacheShard {
                        cache: CacheShard::Str(lock),
                        id: shard_id as u64,
                        len: *len,
                    });

                Either::Right(iter)
            }
        }
    }
}

enum CacheShard<'a, V> {
    U64(&'a mut quick_cache::unsync::Cache<u64, V>),
    Str(&'a mut quick_cache::unsync::Cache<String, V>),
}

pub(crate) struct LockedCacheShard<'a, V> {
    cache: CacheShard<'a, V>,
    id: u64,
    len: usize,
}

impl<'a, V: Copy> LockedCacheShard<'a, V> {
    /// Looks `gid` up in this shard, calling `on_miss` to produce the value if it is absent.
    pub(crate) fn resolve_with(
        &mut self,
        gid: GidRef<'_>,
        on_miss: impl FnOnce() -> Result<V, GraphError>,
    ) -> Result<MaybeNew<V>, GraphError> {
        match (gid, &mut self.cache) {
            (GidRef::U64(gid), CacheShard::U64(cache)) => match cache.get_ref_or_guard(&gid) {
                Ok(value) => Ok(MaybeNew::Existing(*value)),
                Err(guard) => {
                    let value = on_miss()?;
                    guard.insert(value);
                    Ok(MaybeNew::New(value))
                }
            },
            (GidRef::Str(gid), CacheShard::Str(cache)) => match cache.get_ref_or_guard(gid) {
                Ok(value) => Ok(MaybeNew::Existing(*value)),
                Err(guard) => {
                    let value = on_miss()?;
                    guard.insert(value);
                    Ok(MaybeNew::New(value))
                }
            },
            (gid, _) => Err(GraphError::LoadError {
                source: LoadError::InvalidNodeIdType(match gid {
                    GidRef::U64(_) => DataType::UInt64,
                    GidRef::Str(_) => DataType::Utf8View,
                }),
            }),
        }
    }

    pub(crate) fn is_in_shard(&self, gid: GidRef<'_>) -> bool {
        let mut hasher = FxHasher::default();
        gid.hash(&mut hasher);
        let hash = hasher.finish();
        let idx = hash % self.len as u64;
        idx == self.id
    }
}
