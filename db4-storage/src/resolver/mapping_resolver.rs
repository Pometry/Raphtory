use crate::resolver::{GIDResolverOps, Initialiser, MaybeInit, StorageError};
use dashmap::{VacantEntry, mapref::entry::Entry};
use lock_api::ArcMutexGuard;
use once_cell::sync::OnceCell;
use parking_lot::{Mutex, RawMutex};
use raphtory_api::core::{
    entities::{GID, GidRef, GidType, VID},
    storage::FxDashMap,
};
use std::{
    borrow::Borrow,
    hash::Hash,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use thiserror::Error;

use std::path::Path;

#[derive(Debug)]
enum Map {
    U64(FxDashMap<u64, MaybeVID>),
    Str(FxDashMap<String, MaybeVID>),
}

#[derive(Debug, Copy, Clone)]
enum MaybeVID {
    VID(VID),
    Init(usize),
}

impl MaybeVID {
    fn value(self) -> Option<VID> {
        match self {
            MaybeVID::VID(vid) => Some(vid),
            MaybeVID::Init(_) => None,
        }
    }
}

enum InitGuard {
    Init {
        init_id: usize,
        guard: ArcMutexGuard<RawMutex, VID>,
    },
    Read(Arc<Mutex<VID>>),
}

#[derive(Error, Debug)]
pub enum InvalidNodeId {
    #[error("Node id {0} does not have the correct type, expected String")]
    InvalidNodeIdU64(u64),
    #[error("Node id {0} does not have the correct type, expected Numeric")]
    InvalidNodeIdStr(String),
}

impl Map {
    fn as_u64(&self) -> Option<&FxDashMap<u64, MaybeVID>> {
        match self {
            Map::U64(map) => Some(map),
            _ => None,
        }
    }

    fn as_str(&self) -> Option<&FxDashMap<String, MaybeVID>> {
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
pub struct MappingResolver {
    map: OnceCell<Map>,
    uninitialised: FxDashMap<usize, Arc<Mutex<VID>>>,
    init_counter: AtomicUsize,
}

pub struct Init<'a> {
    mapping: &'a MappingResolver,
    init_id: usize,
    gid: GID,
    guard: ArcMutexGuard<RawMutex, VID>,
}

impl<'a> Initialiser for Init<'a> {
    fn init(mut self, vid: VID) -> Result<(), StorageError> {
        *self.guard = vid;
        self.mapping
            .set(self.gid.as_ref(), vid)
            .expect("gid should have been validated");
        self.mapping.uninitialised.remove(&self.init_id);
        Ok(())
    }
}

impl MappingResolver {
    pub fn new_u64() -> Self {
        MappingResolver {
            map: OnceCell::with_value(Map::U64(Default::default())),
            uninitialised: Default::default(),
            init_counter: Default::default(),
        }
    }

    pub fn new_str() -> Self {
        MappingResolver {
            map: OnceCell::with_value(Map::Str(Default::default())),
            uninitialised: Default::default(),
            init_counter: Default::default(),
        }
    }

    fn push_uninit<K: Eq + Hash>(&self, entry: VacantEntry<K, MaybeVID>) -> InitGuard {
        let lock = Arc::new(Mutex::new(VID::default()));
        let guard = lock.lock_arc();
        let init_id = self.init_counter.fetch_add(1, Ordering::Relaxed);
        self.uninitialised.insert(init_id, lock);
        entry.insert(MaybeVID::Init(init_id));
        InitGuard::Init { init_id, guard }
    }

    fn get_uninit(&self, init_id: &usize) -> Arc<Mutex<VID>> {
        self.uninitialised
            .get(init_id)
            .expect("initialisation guard should exist")
            .clone()
    }

    fn get_value_from_map<Q, K>(&self, map: &FxDashMap<K, MaybeVID>, key: &Q) -> Option<VID>
    where
        K: Borrow<Q> + Eq + Hash,
        Q: Hash + Eq + ?Sized,
    {
        map.get(key)?.value().value()
    }

    fn handle_init_guard(&self, init_guard: InitGuard, gid: GidRef) -> MaybeInit<Init<'_>> {
        match init_guard {
            InitGuard::Init { guard, init_id } => MaybeInit::Init(Init {
                mapping: self,
                init_id,
                gid: gid.to_owned(),
                guard,
            }),
            InitGuard::Read(guard) => MaybeInit::VID(*guard.lock()),
        }
    }
}

impl GIDResolverOps for MappingResolver {
    type Init<'a> = Init<'a>;

    fn new() -> Result<Self, StorageError>
    where
        Self: Sized,
    {
        Ok(MappingResolver {
            map: OnceCell::new(),
            uninitialised: Default::default(),
            init_counter: Default::default(),
        })
    }

    fn new_with_path(
        _path: impl AsRef<Path>,
        dtype: Option<GidType>,
    ) -> Result<Self, StorageError> {
        match dtype {
            None => Self::new(),
            Some(dtype) => {
                let mapping = match dtype {
                    GidType::U64 => MappingResolver::new_u64(),
                    GidType::Str => MappingResolver::new_str(),
                };
                Ok(mapping)
            }
        }
    }

    fn len(&self) -> usize {
        self.map.get().map_or(0, |map| match map {
            Map::U64(map) => map.len(),
            Map::Str(map) => map.len(),
        })
    }

    fn dtype(&self) -> Option<GidType> {
        self.map.get().map(|map| match map {
            Map::U64(_) => GidType::U64,
            Map::Str(_) => GidType::Str,
        })
    }

    fn set(&self, gid: GidRef, vid: VID) -> Result<(), StorageError> {
        let map = self.map.get_or_init(|| match gid {
            GidRef::U64(_) => Map::U64(FxDashMap::default()),
            GidRef::Str(_) => Map::Str(FxDashMap::default()),
        });
        match gid {
            GidRef::U64(id) => {
                map.as_u64()
                    .ok_or(InvalidNodeId::InvalidNodeIdU64(id))?
                    .insert(id, MaybeVID::VID(vid));
            }
            GidRef::Str(id) => {
                let id = id.to_owned();
                match map.as_str() {
                    None => Err(InvalidNodeId::InvalidNodeIdStr(id))?,
                    Some(map) => {
                        map.insert(id, MaybeVID::VID(vid));
                    }
                }
            }
        }
        Ok(())
    }

    fn get_or_init(&self, gid: GidRef) -> Result<MaybeInit<Self::Init<'_>>, StorageError> {
        let map = self.map.get_or_init(|| match &gid {
            GidRef::U64(_) => Map::U64(FxDashMap::default()),
            GidRef::Str(_) => Map::Str(FxDashMap::default()),
        });
        let vid_init = match gid {
            GidRef::U64(key) => {
                let map = map.as_u64().ok_or(InvalidNodeId::InvalidNodeIdU64(key))?;
                let init_guard = match map.entry(key) {
                    Entry::Occupied(id) => match id.get() {
                        MaybeVID::VID(vid) => return Ok(MaybeInit::VID(*vid)),
                        MaybeVID::Init(init_id) => InitGuard::Read(self.get_uninit(init_id)),
                    },
                    Entry::Vacant(entry) => self.push_uninit(entry),
                };
                self.handle_init_guard(init_guard, gid)
            }
            GidRef::Str(key) => {
                let map = map
                    .as_str()
                    .ok_or_else(|| InvalidNodeId::InvalidNodeIdStr(key.into()))?;

                let init_guard = match map.get(key) {
                    None => match map.entry(key.to_owned()) {
                        Entry::Occupied(entry) => match entry.get() {
                            MaybeVID::VID(vid) => return Ok(MaybeInit::VID(*vid)),
                            MaybeVID::Init(init_id) => InitGuard::Read(self.get_uninit(init_id)),
                        },
                        Entry::Vacant(entry) => self.push_uninit(entry),
                    },
                    Some(maybe_vid) => match maybe_vid.value() {
                        MaybeVID::VID(vid) => return Ok(MaybeInit::VID(*vid)),
                        MaybeVID::Init(init_id) => InitGuard::Read(self.get_uninit(init_id)),
                    },
                };
                self.handle_init_guard(init_guard, gid)
            }
        };
        Ok(vid_init)
    }

    fn validate_gids<'a>(
        &self,
        gids: impl IntoIterator<Item = GidRef<'a>>,
    ) -> Result<(), StorageError> {
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

    fn get_str(&self, gid: &str) -> Option<VID> {
        let map = self.map.get()?;
        map.as_str().and_then(|m| self.get_value_from_map(m, gid))
    }

    fn get_u64(&self, gid: u64) -> Option<VID> {
        let map = self.map.get()?;
        map.as_u64().and_then(|m| self.get_value_from_map(m, &gid))
    }

    fn bulk_set_str<S: AsRef<str>>(
        &self,
        gids: impl IntoIterator<Item = (S, VID)>,
    ) -> Result<(), StorageError> {
        for (gid, vid) in gids {
            self.set(gid.as_ref().into(), vid)?;
        }
        Ok(())
    }

    fn bulk_set_u64(&self, gids: impl IntoIterator<Item = (u64, VID)>) -> Result<(), StorageError> {
        for (gid, vid) in gids {
            self.set(gid.into(), vid)?;
        }
        Ok(())
    }

    fn iter_str(&self) -> impl Iterator<Item = (String, VID)> + '_ {
        self.map
            .get()
            .and_then(|map| map.as_str())
            .into_iter()
            .flat_map(|m| {
                m.iter()
                    .filter_map(|entry| Some((entry.key().to_owned(), (entry.value().value()?))))
            })
    }

    fn iter_u64(&self) -> impl Iterator<Item = (u64, VID)> + '_ {
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
