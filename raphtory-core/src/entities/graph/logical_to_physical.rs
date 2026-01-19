use dashmap::mapref::entry::Entry;
use once_cell::sync::OnceCell;
use raphtory_api::core::{
    entities::{GidRef, GidType, VID},
    storage::{dict_mapper::MaybeNew, FxDashMap},
};
use serde::{Deserialize, Deserializer, Serialize};
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
