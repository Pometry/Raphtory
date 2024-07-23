use once_cell::sync::OnceCell;

use raphtory_api::core::{
    entities::{GID, VID},
    storage::FxDashMap,
};
use serde::{Deserialize, Deserializer, Serialize};

use crate::core::utils::errors::{GraphError, MutateGraphError};

#[derive(Debug, Deserialize, Serialize)]
enum Map {
    U64(FxDashMap<u64, VID>),
    I64(FxDashMap<i64, VID>),
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

    fn as_i64(&self) -> Option<&FxDashMap<i64, VID>> {
        match self {
            Map::I64(map) => Some(map),
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
    pub fn new() -> Self {
        Mapping {
            map: OnceCell::new(),
        }
    }

    pub fn get_or_init(&self, gid: GID, f_init: impl FnOnce() -> VID) -> Result<VID, GraphError> {
        let map = self.map.get_or_init(|| match &gid {
            GID::U64(_) => Map::U64(FxDashMap::default()),
            GID::I64(_) => Map::I64(FxDashMap::default()),
            GID::Str(_) => Map::Str(FxDashMap::default()),
        });
        let vid = match &gid {
            GID::U64(id) => map.as_u64().map(|m| *(m.entry(*id).or_insert_with(f_init))),
            GID::I64(id) => map.as_i64().map(|m| *(m.entry(*id).or_insert_with(f_init))),
            GID::Str(id) => map
                .as_str()
                .map(|m| *(m.entry(id.clone()).or_insert_with(f_init))),
        };
        vid.ok_or(GraphError::FailedToMutateGraph {
            source: MutateGraphError::InvalidNodeId(gid),
        })
    }

    pub fn get(&self, gid: &GID) -> Option<VID> {
        let map = self.map.get()?;
        match gid {
            GID::U64(id) => map.as_u64().and_then(|m| m.get(id).map(|id| *id)),
            GID::I64(id) => map.as_i64().and_then(|m| m.get(id).map(|id| *id)),
            GID::Str(id) => map.as_str().and_then(|m| m.get(id).map(|id| *id)),
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
