use crate::{
    entities::nodes::node_store::NodeStore,
    storage::{NodeSlot, UninitialisedEntry},
};
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
    hash::{BuildHasher, Hash},
    panic,
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
                        work_fn(ResolverShard::U64 {
                            guard: shard.write(),
                            map,
                            shard_id,
                        })
                    })
            }
            Map::Str(map) => {
                let shards = map.shards();
                shards
                    .par_iter()
                    .enumerate()
                    .try_for_each(|(shard_id, shard)| {
                        work_fn(ResolverShard::Str {
                            guard: shard.write(),
                            map,
                            shard_id,
                        })
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
    U64 {
        guard: RwLockWriteGuard<'a, RawTable<(u64, SharedValue<VID>)>>,
        map: &'a FxDashMap<u64, VID>,
        shard_id: usize,
    },
    Str {
        guard: RwLockWriteGuard<'a, RawTable<(String, SharedValue<VID>)>>,
        map: &'a FxDashMap<String, VID>,
        shard_id: usize,
    },
}

pub struct U64ResolverShard<'a> {
    guard: RwLockWriteGuard<'a, RawTable<(u64, SharedValue<VID>)>>,
    map: &'a FxDashMap<u64, VID>,
    shard_id: usize,
}

pub struct StrResolverShard<'a> {
    guard: RwLockWriteGuard<'a, RawTable<(String, SharedValue<VID>)>>,
    map: &'a FxDashMap<String, VID>,
    shard_id: usize,
}

impl<'a> ResolverShard<'a> {
    pub fn resolve_nodes_u64<'b, I: Iterator<Item = (usize, u64)>>(
        &mut self,
        nodes: impl Fn() -> I,
        next_id: impl FnOnce() -> VID,
    ) {
    }

    pub fn resolve_nodes_str<'b, I: Iterator<Item = (usize, &'b str)>>(
        &mut self,
        nodes: impl Fn() -> I,
        next_id: impl FnOnce() -> VID,
    ) {
    }

    pub fn resolve_node<'b>(&mut self, node_ref: GidRef<'b>, next_id: impl FnOnce() -> VID) -> VID {
        match self {
            ResolverShard::U64 {
                guard,
                map,
                shard_id,
            } => {
                if let GidRef::U64(id) = node_ref {
                    let shard_ind = map.determine_map(&id);
                    let mut factory = map.hasher().clone();
                    let hash = factory.hash_one(&id);
                    // let data = (id, SharedValue::new(value))

                    match guard.get(hash, |(k, _)| k == &id) {
                        Some((_, vid)) => {
                            // Node already exists, do nothing
                            *(vid.get())
                        }
                        None => {
                            // Node does not exist, create it
                            let vid = next_id();

                            guard.insert(hash, (id, SharedValue::new(vid)), |t| {
                                factory.hash_one(t.0)
                            });
                            vid
                        }
                    }
                } else {
                    panic!("Expected GidRef::U64, got {:?}", node_ref);
                }
            }
            ResolverShard::Str {
                guard,
                map,
                shard_id,
            } => {
                if let GidRef::Str(id) = node_ref {
                    let shard_ind = map.determine_map(id);
                    let mut factory = map.hasher().clone();
                    let hash = factory.hash_one(&id);

                    match guard.get(hash, |(k, _)| k == &id) {
                        Some((_, vid)) => {
                            // Node already exists, do nothing
                            *(vid.get())
                        }
                        None => {
                            // Node does not exist, create it
                            let vid = next_id();

                            guard.insert(hash, (id.to_owned(), SharedValue::new(vid)), |t| {
                                factory.hash_one(&t.0)
                            });
                            vid
                        }
                    }
                } else {
                    panic!("Expected GidRef::Str, got {:?}", node_ref);
                }
            }
        };
        VID(0)
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

    pub fn get_or_init_node<'a>(
        &self,
        gid: GidRef,
        f_init: impl FnOnce() -> UninitialisedEntry<'a, NodeStore, NodeSlot>,
    ) -> Result<MaybeNew<VID>, InvalidNodeId> {
        let map = self.map.get_or_init(|| match &gid {
            GidRef::U64(_) => Map::U64(FxDashMap::default()),
            GidRef::Str(_) => Map::Str(FxDashMap::default()),
        });
        match gid {
            GidRef::U64(id) => map
                .as_u64()
                .map(|m| get_or_new(m, id, f_init))
                .ok_or(InvalidNodeId::InvalidNodeIdU64(id)),
            GidRef::Str(id) => map
                .as_str()
                .map(|m| optim_get_or_insert(m, id, f_init))
                .ok_or_else(|| InvalidNodeId::InvalidNodeIdStr(id.into())),
        }
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
}

#[inline]
fn optim_get_or_insert<'a>(
    m: &FxDashMap<String, VID>,
    id: &str,
    f_init: impl FnOnce() -> UninitialisedEntry<'a, NodeStore, NodeSlot>,
) -> MaybeNew<VID> {
    m.get(id)
        .map(|vid| MaybeNew::Existing(*vid))
        .unwrap_or_else(|| get_or_new(m, id.to_owned(), f_init))
}

#[inline]
fn get_or_new<'a, K: Eq + Hash>(
    m: &FxDashMap<K, VID>,
    id: K,
    f_init: impl FnOnce() -> UninitialisedEntry<'a, NodeStore, NodeSlot>,
) -> MaybeNew<VID> {
    let entry = match m.entry(id) {
        Entry::Occupied(entry) => Either::Left(*entry.get()),
        Entry::Vacant(entry) => {
            // This keeps the underlying storage shard locked for deferred initialisation but
            // allows unlocking the map again.
            let node = f_init();
            entry.insert(node.value().vid);
            Either::Right(node)
        }
    };
    match entry {
        Either::Left(vid) => MaybeNew::Existing(vid),
        Either::Right(node_entry) => {
            let vid = node_entry.value().vid;
            node_entry.init();
            MaybeNew::New(vid)
        }
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
