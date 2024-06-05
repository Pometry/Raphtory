use crate::core::storage::{arc_str::ArcStr, locked_vec::ArcReadLockedVec, FxDashMap};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::{borrow::Borrow, hash::Hash, sync::Arc};

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct DictMapper {
    map: FxDashMap<ArcStr, usize>,
    reverse_map: Arc<RwLock<Vec<ArcStr>>>, //FIXME: a boxcar vector would be a great fit if it was serializable...
}

impl DictMapper {
    pub fn get_or_create_id<Q, T>(&self, name: &Q) -> usize
    where
        Q: Hash + Eq + ?Sized + ToOwned<Owned = T> + Borrow<str>,
        T: Into<ArcStr>,
    {
        if let Some(existing_id) = self.map.get(name.borrow()) {
            return *existing_id;
        }

        let name = name.to_owned().into();
        let new_id = self.map.entry(name.clone()).or_insert_with(|| {
            let mut reverse = self.reverse_map.write();
            let id = reverse.len();
            reverse.push(name);
            id
        });
        *new_id
    }

    pub fn get_id(&self, name: &str) -> Option<usize> {
        self.map.get(name).map(|id| *id)
    }

    pub fn has_name(&self, id: usize) -> bool {
        let guard = self.reverse_map.read();
        guard.get(id).is_some()
    }

    pub fn get_name(&self, id: usize) -> ArcStr {
        let guard = self.reverse_map.read();
        guard
            .get(id)
            .cloned()
            .expect("internal ids should always be mapped to a name")
    }

    pub fn get_keys(&self) -> ArcReadLockedVec<ArcStr> {
        ArcReadLockedVec {
            guard: self.reverse_map.read_arc(),
        }
    }

    pub fn get_values(&self) -> Vec<usize> {
        self.map.iter().map(|entry| *entry.value()).collect()
    }

    pub fn len(&self) -> usize {
        self.reverse_map.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.reverse_map.read().is_empty()
    }
}
