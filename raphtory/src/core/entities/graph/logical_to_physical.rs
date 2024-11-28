use crate::core::{
    entities::nodes::node_store::NodeStore,
    storage::{NodeSlot, UninitialisedEntry},
    utils::errors::{GraphError, MutateGraphError},
};
use dashmap::mapref::entry::Entry;
use either::Either;
use once_cell::sync::OnceCell;
use raphtory_api::core::{
    entities::{GidRef, GidType, VID},
    storage::{dict_mapper::MaybeNew, FxDashMap},
};
use serde::{Deserialize, Deserializer, Serialize};
use std::hash::Hash;

#[derive(Debug, Deserialize, Serialize)]
enum Map {
    U64(FxDashMap<u64, VID>),
    Str(FxDashMap<String, VID>),
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
}

impl Default for Map {
    fn default() -> Self {
        Map::U64(FxDashMap::default())
    }
}

#[derive(Debug)]
pub(crate) struct Mapping {
    map: OnceCell<Map>,
}

impl Mapping {
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

    pub fn set(&self, gid: GidRef, vid: VID) -> Result<(), GraphError> {
        let map = self.map.get_or_init(|| match gid {
            GidRef::U64(_) => Map::U64(FxDashMap::default()),
            GidRef::Str(_) => Map::Str(FxDashMap::default()),
        });
        match gid {
            GidRef::U64(id) => map.as_u64().map(|map| {
                map.insert(id, vid);
            }),
            GidRef::Str(id) => map.as_str().map(|map| {
                map.insert(id.to_owned(), vid);
            }),
        }
        .ok_or_else(|| MutateGraphError::InvalidNodeId(gid.into()).into())
    }

    pub fn get_or_init(
        &self,
        gid: GidRef,
        next_id: impl FnOnce() -> VID,
    ) -> Result<MaybeNew<VID>, GraphError> {
        let map = self.map.get_or_init(|| match &gid {
            GidRef::U64(_) => Map::U64(FxDashMap::default()),
            GidRef::Str(_) => Map::Str(FxDashMap::default()),
        });
        let vid = match gid {
            GidRef::U64(id) => map.as_u64().map(|m| match m.entry(id) {
                Entry::Occupied(id) => MaybeNew::Existing(*id.get()),
                Entry::Vacant(entry) => {
                    let vid = next_id();
                    entry.insert(vid);
                    MaybeNew::New(vid)
                }
            }),
            GidRef::Str(id) => map.as_str().map(|m| {
                m.get(id)
                    .map(|vid| MaybeNew::Existing(*vid))
                    .unwrap_or_else(|| match m.entry(id.to_owned()) {
                        Entry::Occupied(entry) => MaybeNew::Existing(*entry.get()),
                        Entry::Vacant(entry) => {
                            let vid = next_id();
                            entry.insert(vid);
                            MaybeNew::New(vid)
                        }
                    })
            }),
        };

        vid.ok_or_else(|| GraphError::FailedToMutateGraph {
            source: MutateGraphError::InvalidNodeId(gid.into()),
        })
    }

    pub fn get_or_init_node<'a>(
        &self,
        gid: GidRef,
        f_init: impl FnOnce() -> UninitialisedEntry<'a, NodeStore, NodeSlot>,
    ) -> Result<MaybeNew<VID>, GraphError> {
        let map = self.map.get_or_init(|| match &gid {
            GidRef::U64(_) => Map::U64(FxDashMap::default()),
            GidRef::Str(_) => Map::Str(FxDashMap::default()),
        });
        let vid = match gid {
            GidRef::U64(id) => map.as_u64().map(|m| get_or_new(m, id, f_init)),
            GidRef::Str(id) => map.as_str().map(|m| optim_get_or_insert(m, id, f_init)),
        };
        vid.ok_or_else(|| GraphError::FailedToMutateGraph {
            source: MutateGraphError::InvalidNodeId(gid.into()),
        })
    }

    pub fn get_str(&self, gid: &str) -> Option<VID> {
        let map = self.map.get()?;
        map.as_str().and_then(|m| m.get(gid).map(|id| *id))
    }

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
