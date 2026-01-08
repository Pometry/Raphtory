use dashmap::{mapref::entry::Entry, RwLockWriteGuard, SharedValue};
use either::Either;
use hashbrown::raw::RawTable;
use once_cell::sync::OnceCell;
use raphtory_api::core::{
    entities::{GidRef, GidType, VID},
    storage::{dict_mapper::MaybeNew, FxDashMap},
};
use rayon::prelude::*;
use serde::{Deserialize, Deserializer, Serialize};
use std::{
    borrow::Borrow,
    hash::{BuildHasher, Hash},
};
use thiserror::Error;

#[derive(Debug, Deserialize, Serialize)]
enum Map {
    U64(FxDashMap<u64, VID>),
    Str(FxDashMap<String, VID>),
}

#[derive(Error, Debug)]
pub enum InvalidNodeId {
    #[error("Node id {0} does not have the correct type, expected String")]
    InvalidNodeIdU64(u64),
    #[error("Node id {0} does not have the correct type, expected Numeric")]
    InvalidNodeIdStr(String),
}

impl Map {
    fn as_u64(&self) -> Option<&FxDashMap<u64, VID>> {
        match self {
            Map::U64(map) => Some(map),
            _ => None,
        }
    }

    fn as_str(&self) -> Option<&FxDashMap<String, VID>> {
        match self {
            Map::Str(map) => Some(map),
            _ => None,
        }
    }

    pub fn run_with_locked<E: Send, FN: Fn(ResolverShard<'_>) -> Result<(), E> + Send + Sync>(
        &self,
        work_fn: FN,
    ) -> Result<(), E> {
        match self {
            Map::U64(map) => {
                let shards = map.shards();
                shards
                    .par_iter()
                    .enumerate()
                    .try_for_each(|(shard_id, shard)| {
                        work_fn(ResolverShard::U64(ResolverShardT::new(
                            shard.write(),
                            map,
                            shard_id,
                        )))
                    })
            }
            Map::Str(map) => {
                let shards = map.shards();
                shards
                    .par_iter()
                    .enumerate()
                    .try_for_each(|(shard_id, shard)| {
                        work_fn(ResolverShard::Str(ResolverShardT::new(
                            shard.write(),
                            map,
                            shard_id,
                        )))
                    })
            }
        }
    }
}

impl Default for Map {
    fn default() -> Self {
        Map::U64(FxDashMap::default())
    }
}

#[derive(Debug, Default)]
pub struct Mapping {
    map: OnceCell<Map>,
}

pub enum ResolverShard<'a> {
    U64(ResolverShardT<'a, u64>),
    Str(ResolverShardT<'a, String>),
}

impl<'a> ResolverShard<'a> {
    pub fn shard_id(&self) -> usize {
        match self {
            ResolverShard::U64(ResolverShardT { shard_id, .. }) => *shard_id,
            ResolverShard::Str(ResolverShardT { shard_id, .. }) => *shard_id,
        }
    }

    pub fn as_u64(&mut self) -> Option<&mut ResolverShardT<'a, u64>> {
        if let ResolverShard::U64(shard) = self {
            Some(shard)
        } else {
            None
        }
    }

    pub fn as_str(&mut self) -> Option<&mut ResolverShardT<'a, String>> {
        if let ResolverShard::Str(shard) = self {
            Some(shard)
        } else {
            None
        }
    }
}

pub struct ResolverShardT<'a, T> {
    guard: RwLockWriteGuard<'a, RawTable<(T, SharedValue<VID>)>>,
    map: &'a FxDashMap<T, VID>,
    shard_id: usize,
}

impl<'a, T: Eq + Hash + Clone> ResolverShardT<'a, T> {
    pub fn new(
        guard: RwLockWriteGuard<'a, RawTable<(T, SharedValue<VID>)>>,
        map: &'a FxDashMap<T, VID>,
        shard_id: usize,
    ) -> Self {
        Self {
            guard,
            map,
            shard_id,
        }
    }
    pub fn resolve_node<Q>(
        &mut self,
        id: &Q,
        next_id: impl FnOnce(&Q) -> Either<VID, VID>,
    ) -> Option<VID>
    where
        T: Borrow<Q>,
        Q: Eq + Hash + ToOwned<Owned = T> + ?Sized,
    {
        let shard_ind = self.map.determine_map(id.borrow());
        if shard_ind != self.shard_id {
            // This shard does not contain the id, return None
            return None;
        }
        let factory = self.map.hasher().clone();
        let hash = factory.hash_one(id);

        match self.guard.get(hash, |(k, _)| k.borrow() == id) {
            Some((_, vid)) => {
                // Node already exists, do nothing
                Some(*(vid.get()))
            }
            None => {
                // Node does not exist, create it
                let vid = next_id(id);

                if let Either::Left(vid) = vid {
                    self.guard
                        .insert(hash, (id.borrow().to_owned(), SharedValue::new(vid)), |t| {
                            factory.hash_one(&t.0)
                        });
                    Some(vid)
                } else {
                    vid.right()
                }
            }
        }
    }
}

impl Mapping {
    pub fn len(&self) -> usize {
        self.map.get().map_or(0, |map| match map {
            Map::U64(map) => map.len(),
            Map::Str(map) => map.len(),
        })
    }

    pub fn run_with_locked<E: Send, FN: Fn(ResolverShard<'_>) -> Result<(), E> + Send + Sync>(
        &self,
        work_fn: FN,
    ) -> Result<(), E> {
        let inner_map = self.map.get().unwrap();
        inner_map.run_with_locked(work_fn)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn dtype(&self) -> Option<GidType> {
        self.map.get().map(|map| match map {
            Map::U64(_) => GidType::U64,
            Map::Str(_) => GidType::Str,
        })
    }
    pub fn new() -> Self {
        Mapping {
            map: OnceCell::new(),
        }
    }

    pub fn new_u64() -> Self {
        Mapping {
            map: OnceCell::with_value(Map::U64(Default::default())),
        }
    }

    pub fn new_str() -> Self {
        Mapping {
            map: OnceCell::with_value(Map::Str(Default::default())),
        }
    }

    pub fn set(&self, gid: GidRef, vid: VID) -> Result<(), InvalidNodeId> {
        let map = self.map.get_or_init(|| match gid {
            GidRef::U64(_) => Map::U64(FxDashMap::default()),
            GidRef::Str(_) => Map::Str(FxDashMap::default()),
        });
        match gid {
            GidRef::U64(id) => {
                map.as_u64()
                    .ok_or(InvalidNodeId::InvalidNodeIdU64(id))?
                    .insert(id, vid);
            }
            GidRef::Str(id) => {
                let id = id.to_owned();
                match map.as_str() {
                    None => return Err(InvalidNodeId::InvalidNodeIdStr(id)),
                    Some(map) => {
                        map.insert(id, vid);
                    }
                }
            }
        }
        Ok(())
    }

    pub fn get_or_init(
        &self,
        gid: GidRef,
        next_id: impl FnOnce() -> VID,
    ) -> Result<MaybeNew<VID>, InvalidNodeId> {
        let map = self.map.get_or_init(|| match &gid {
            GidRef::U64(_) => Map::U64(FxDashMap::default()),
            GidRef::Str(_) => Map::Str(FxDashMap::default()),
        });
        let vid = match gid {
            GidRef::U64(id) => {
                let map = map.as_u64().ok_or(InvalidNodeId::InvalidNodeIdU64(id))?;
                match map.entry(id) {
                    Entry::Occupied(id) => MaybeNew::Existing(*id.get()),
                    Entry::Vacant(entry) => {
                        let vid = next_id();
                        entry.insert(vid);
                        MaybeNew::New(vid)
                    }
                }
            }
            GidRef::Str(id) => {
                let map = map
                    .as_str()
                    .ok_or_else(|| InvalidNodeId::InvalidNodeIdStr(id.into()))?;
                map.get(id)
                    .map(|vid| MaybeNew::Existing(*vid))
                    .unwrap_or_else(|| match map.entry(id.to_owned()) {
                        Entry::Occupied(entry) => MaybeNew::Existing(*entry.get()),
                        Entry::Vacant(entry) => {
                            let vid = next_id();
                            entry.insert(vid);
                            MaybeNew::New(vid)
                        }
                    })
            }
        };
        Ok(vid)
    }

    pub fn validate_gids<'a>(
        &self,
        gids: impl IntoIterator<Item = GidRef<'a>>,
    ) -> Result<(), InvalidNodeId> {
        for gid in gids {
            let map = self.map.get_or_init(|| match &gid {
                GidRef::U64(_) => Map::U64(FxDashMap::default()),
                GidRef::Str(_) => Map::Str(FxDashMap::default()),
            });
            match gid {
                GidRef::U64(id) => {
                    map.as_u64().ok_or(InvalidNodeId::InvalidNodeIdU64(id))?;
                }
                GidRef::Str(id) => {
                    map.as_str()
                        .ok_or_else(|| InvalidNodeId::InvalidNodeIdStr(id.into()))?;
                }
            }
        }

        Ok(())
    }

    #[inline]
    pub fn get_str(&self, gid: &str) -> Option<VID> {
        let map = self.map.get()?;
        map.as_str().and_then(|m| m.get(gid).map(|id| *id))
    }

    #[inline]
    pub fn get_u64(&self, gid: u64) -> Option<VID> {
        let map = self.map.get()?;
        map.as_u64().and_then(|m| m.get(&gid).map(|id| *id))
    }

    pub fn iter_str(&self) -> impl Iterator<Item = (String, VID)> + '_ {
        self.map
            .get()
            .and_then(|map| map.as_str())
            .into_iter()
            .flat_map(|m| {
                m.iter()
                    .map(|entry| (entry.key().to_owned(), *(entry.value())))
            })
    }

    pub fn iter_u64(&self) -> impl Iterator<Item = (u64, VID)> + '_ {
        self.map
            .get()
            .and_then(|map| map.as_u64())
            .into_iter()
            .flat_map(|m| m.iter().map(|entry| (*entry.key(), *(entry.value()))))
    }
}

impl<'de> Deserialize<'de> for Mapping {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        if let Some(map) = Option::<Map>::deserialize(deserializer)? {
            let once = OnceCell::with_value(map);
            Ok(Mapping { map: once })
        } else {
            Ok(Mapping {
                map: OnceCell::new(),
            })
        }
    }
}

impl Serialize for Mapping {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if let Some(map) = self.map.get() {
            Some(map).serialize(serializer)
        } else {
            serializer.serialize_none()
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use itertools::Itertools;
    use proptest::prelude::*;

    #[test]
    fn test_parallel_sharded_mapping() {
        let at_least_one_vec = proptest::collection::vec(any::<String>(), 1..100);
        proptest!(|(gids in at_least_one_vec)| {
            let mapping = Mapping::new();
            let vid_count = AtomicUsize::new(0);
            mapping.set(gids.first().map(|x|GidRef::Str(x)).unwrap(), VID(0)).unwrap();

            let resolved_col = gids.iter().map(|_| AtomicUsize::new(0)).collect::<Vec<_>>();
            mapping.run_with_locked(|mut shard| {
                for (id, gid) in gids.iter().enumerate() {
                    if let Some(vid) = shard.as_str().unwrap().resolve_node(gid, |_| Either::Left(VID(vid_count.fetch_add(1, Ordering::Relaxed)))) {
                       resolved_col[id].store(vid.index(), Ordering::Relaxed);
                    }
                }
                Ok::<_, String>(())
            }).unwrap();

            for (gid, expected_vid) in gids.iter().zip_eq(resolved_col.iter().map(|v| VID(v.load(Ordering::Relaxed)))) {
                assert_eq!(mapping.get_str(gid).unwrap(), expected_vid);
            }
        })
    }
}
