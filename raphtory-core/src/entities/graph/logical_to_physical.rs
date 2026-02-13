use dashmap::{
    mapref::{entry::Entry, one::Ref},
    VacantEntry,
};
use lock_api::ArcMutexGuard;
use once_cell::sync::OnceCell;
use parking_lot::{Mutex, MutexGuard, RawMutex};
use raphtory_api::core::{
    entities::{GidRef, GidType, VID},
    storage::{dict_mapper::MaybeNew, FxDashMap},
};
use serde::{Deserialize, Deserializer, Serialize};
use std::{
    borrow::Borrow,
    hash::Hash,
    ops::Deref,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use thiserror::Error;

#[derive(Debug)]
enum Map {
    U64(FxDashMap<u64, MaybeInit>),
    Str(FxDashMap<String, MaybeInit>),
}

#[derive(Debug, Copy, Clone)]
enum MaybeInit {
    VID(VID),
    Init(usize),
}

impl MaybeInit {
    fn value(self) -> Option<VID> {
        match self {
            MaybeInit::VID(vid) => Some(vid),
            MaybeInit::Init(_) => None,
        }
    }
}

enum InitGuard<'a> {
    Init {
        init_id: usize,
        guard: ArcMutexGuard<RawMutex, VID>,
    },
    Read(Ref<'a, usize, Arc<Mutex<VID>>>),
}

#[derive(Error, Debug)]
pub enum InvalidNodeId {
    #[error("Node id {0} does not have the correct type, expected String")]
    InvalidNodeIdU64(u64),
    #[error("Node id {0} does not have the correct type, expected Numeric")]
    InvalidNodeIdStr(String),
}

impl Map {
    fn as_u64(&self) -> Option<&FxDashMap<u64, MaybeInit>> {
        match self {
            Map::U64(map) => Some(map),
            _ => None,
        }
    }

    fn as_str(&self) -> Option<&FxDashMap<String, MaybeInit>> {
        match self {
            Map::Str(map) => Some(map),
            _ => None,
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
    uninitialised: FxDashMap<usize, Arc<Mutex<VID>>>,
    init_counter: AtomicUsize,
}

impl Mapping {
    pub fn len(&self) -> usize {
        self.map.get().map_or(0, |map| match map {
            Map::U64(map) => map.len(),
            Map::Str(map) => map.len(),
        })
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
            uninitialised: Default::default(),
            init_counter: Default::default(),
        }
    }

    pub fn new_u64() -> Self {
        Mapping {
            map: OnceCell::with_value(Map::U64(Default::default())),
            uninitialised: Default::default(),
            init_counter: Default::default(),
        }
    }

    pub fn new_str() -> Self {
        Mapping {
            map: OnceCell::with_value(Map::Str(Default::default())),
            uninitialised: Default::default(),
            init_counter: Default::default(),
        }
    }

    fn push_uninit<K: Eq + Hash>(&self, entry: VacantEntry<K, MaybeInit>) -> InitGuard<'static> {
        let lock = Arc::new(Mutex::new(VID::default()));
        let guard = lock.lock_arc();
        let init_id = self.init_counter.fetch_add(1, Ordering::Relaxed);
        self.uninitialised.insert(init_id, lock);
        entry.insert(MaybeInit::Init(init_id));
        InitGuard::Init { init_id, guard }
    }

    fn get_uninit(&self, init_id: &usize) -> Ref<'_, usize, Arc<Mutex<VID>>> {
        self.uninitialised
            .get(init_id)
            .expect("initialisation guard should exist")
    }

    fn get_value_from_map<Q, K>(&self, map: &FxDashMap<K, MaybeInit>, key: &Q) -> Option<VID>
    where
        K: Borrow<Q> + Eq + Hash,
        Q: Hash + Eq + ?Sized,
    {
        map.get(key)?.value().value()
    }

    fn handle_init_guard<K: Eq + Hash>(
        &self,
        init_guard: InitGuard,
        map: &FxDashMap<K, MaybeInit>,
        key: K,
        next_id: impl FnOnce() -> VID,
    ) -> MaybeNew<VID> {
        match init_guard {
            InitGuard::Init { mut guard, init_id } => {
                let vid = next_id();
                *guard = vid;
                map.insert(key, MaybeInit::VID(vid));
                self.uninitialised.remove(&init_id);
                MaybeNew::New(vid)
            }
            InitGuard::Read(guard) => {
                let vid = *guard.lock();
                assert!(vid.is_initialised(), "VID should be initialised on read");
                MaybeNew::Existing(vid)
            }
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
                    .insert(id, MaybeInit::VID(vid));
            }
            GidRef::Str(id) => {
                let id = id.to_owned();
                match map.as_str() {
                    None => return Err(InvalidNodeId::InvalidNodeIdStr(id)),
                    Some(map) => {
                        map.insert(id, MaybeInit::VID(vid));
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
            GidRef::U64(key) => {
                let map = map.as_u64().ok_or(InvalidNodeId::InvalidNodeIdU64(key))?;
                let init_guard = match map.entry(key) {
                    Entry::Occupied(id) => match id.get() {
                        MaybeInit::VID(vid) => return Ok(MaybeNew::Existing(*vid)),
                        MaybeInit::Init(init_id) => InitGuard::Read(self.get_uninit(init_id)),
                    },
                    Entry::Vacant(entry) => self.push_uninit(entry),
                };
                self.handle_init_guard(init_guard, map, key, next_id)
            }
            GidRef::Str(key) => {
                let map = map
                    .as_str()
                    .ok_or_else(|| InvalidNodeId::InvalidNodeIdStr(key.into()))?;

                let init_guard = match map.get(key) {
                    None => match map.entry(key.to_owned()) {
                        Entry::Occupied(entry) => match entry.get() {
                            MaybeInit::VID(vid) => return Ok(MaybeNew::Existing(*vid)),
                            MaybeInit::Init(init_id) => InitGuard::Read(self.get_uninit(init_id)),
                        },
                        Entry::Vacant(entry) => self.push_uninit(entry),
                    },
                    Some(maybe_vid) => match maybe_vid.value() {
                        MaybeInit::VID(vid) => return Ok(MaybeNew::Existing(*vid)),
                        MaybeInit::Init(init_id) => InitGuard::Read(self.get_uninit(init_id)),
                    },
                };
                self.handle_init_guard(init_guard, map, key.to_owned(), next_id)
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
        map.as_str().and_then(|m| self.get_value_from_map(m, gid))
    }

    #[inline]
    pub fn get_u64(&self, gid: u64) -> Option<VID> {
        let map = self.map.get()?;
        map.as_u64().and_then(|m| self.get_value_from_map(m, &gid))
    }

    pub fn iter_str(&self) -> impl Iterator<Item = (String, VID)> + '_ {
        self.map
            .get()
            .and_then(|map| map.as_str())
            .into_iter()
            .flat_map(|m| {
                m.iter()
                    .filter_map(|entry| Some((entry.key().to_owned(), (entry.value().value()?))))
            })
    }

    pub fn iter_u64(&self) -> impl Iterator<Item = (u64, VID)> + '_ {
        self.map
            .get()
            .and_then(|map| map.as_u64())
            .into_iter()
            .flat_map(|m| {
                m.iter()
                    .filter_map(|entry| Some((*entry.key(), (entry.value().value()?))))
            })
    }
}
