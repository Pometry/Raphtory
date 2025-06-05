use crate::core::storage::{arc_str::ArcStr, locked_vec::ArcReadLockedVec};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use std::{
    borrow::{Borrow, BorrowMut},
    collections::hash_map::Entry,
    hash::Hash,
    ops::DerefMut,
    sync::Arc,
};

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct DictMapper {
    map: Arc<RwLock<FxHashMap<ArcStr, usize>>>,
    reverse_map: Arc<RwLock<Vec<ArcStr>>>, //FIXME: a boxcar vector would be a great fit if it was serializable...
}

#[derive(Copy, Clone, Debug)]
pub enum MaybeNew<Index> {
    New(Index),
    Existing(Index),
}

impl<Index, T> PartialEq<T> for MaybeNew<Index>
where
    Index: PartialEq<Index>,
    T: Borrow<Index>,
{
    fn eq(&self, other: &T) -> bool {
        other.borrow() == self.borrow()
    }
}

impl<Index> MaybeNew<Index> {
    #[inline]
    pub fn is_new(&self) -> bool {
        matches!(self, MaybeNew::New(_))
    }

    #[inline]
    pub fn inner(self) -> Index {
        match self {
            MaybeNew::New(inner) => inner,
            MaybeNew::Existing(inner) => inner,
        }
    }

    #[inline]
    pub fn map<R>(self, map_fn: impl FnOnce(Index) -> R) -> MaybeNew<R> {
        match self {
            MaybeNew::New(inner) => MaybeNew::New(map_fn(inner)),
            MaybeNew::Existing(inner) => MaybeNew::Existing(map_fn(inner)),
        }
    }

    #[inline]
    pub fn try_map<R, E>(
        self,
        map_fn: impl FnOnce(Index) -> Result<R, E>,
    ) -> Result<MaybeNew<R>, E> {
        match self {
            MaybeNew::New(inner) => Ok(MaybeNew::New(map_fn(inner)?)),
            MaybeNew::Existing(inner) => Ok(MaybeNew::Existing(map_fn(inner)?)),
        }
    }

    #[inline]
    pub fn as_ref(&self) -> MaybeNew<&Index> {
        match self {
            MaybeNew::New(inner) => MaybeNew::New(inner),
            MaybeNew::Existing(inner) => MaybeNew::Existing(inner),
        }
    }

    #[inline]
    pub fn as_mut(&mut self) -> MaybeNew<&mut Index> {
        match self {
            MaybeNew::New(inner) => MaybeNew::New(inner),
            MaybeNew::Existing(inner) => MaybeNew::Existing(inner),
        }
    }

    #[inline]
    pub fn if_new<R>(self, map_fn: impl FnOnce(Index) -> R) -> Option<R> {
        match self {
            MaybeNew::New(inner) => Some(map_fn(inner)),
            MaybeNew::Existing(_) => None,
        }
    }
}

impl<Index> Borrow<Index> for MaybeNew<Index> {
    #[inline]
    fn borrow(&self) -> &Index {
        self.as_ref().inner()
    }
}

impl<Index> BorrowMut<Index> for MaybeNew<Index> {
    #[inline]
    fn borrow_mut(&mut self) -> &mut Index {
        self.as_mut().inner()
    }
}

pub struct LockedDictMapper<'a> {
    map: RwLockReadGuard<'a, FxHashMap<ArcStr, usize>>,
    reverse_map: RwLockReadGuard<'a, Vec<ArcStr>>,
}

pub struct WriteLockedDictMapper<'a> {
    map: RwLockWriteGuard<'a, FxHashMap<ArcStr, usize>>,
    reverse_map: RwLockWriteGuard<'a, Vec<ArcStr>>,
}

impl LockedDictMapper<'_> {
    pub fn get_id(&self, name: &str) -> Option<usize> {
        self.map.get(name).copied()
    }

    pub fn map(&self) -> &FxHashMap<ArcStr, usize> {
        &self.map
    }
}

impl WriteLockedDictMapper<'_> {
    pub fn get_or_create_id<Q, T>(&mut self, name: &Q) -> MaybeNew<usize>
    where
        Q: Hash + Eq + ?Sized + ToOwned<Owned = T> + Borrow<str>,
        T: Into<ArcStr>,
    {
        let name = name.to_owned().into();
        let new_id = match self.map.entry(name.clone()) {
            Entry::Occupied(entry) => MaybeNew::Existing(*entry.get()),
            Entry::Vacant(entry) => {
                let id = self.reverse_map.len();
                self.reverse_map.push(name);
                entry.insert(id);
                MaybeNew::New(id)
            }
        };
        new_id
    }

    pub fn set_id(&mut self, name: impl Into<ArcStr>, id: usize) {
        let arc_name = name.into();
        let map_entry = self.map.entry(arc_name.clone());
        let keys = self.reverse_map.deref_mut();
        if keys.len() <= id {
            keys.resize(id + 1, Default::default())
        }
        keys[id] = arc_name;
        map_entry.insert_entry(id);
    }

    pub fn map(&self) -> &FxHashMap<ArcStr, usize> {
        &self.map
    }
}

impl DictMapper {
    pub fn deep_clone(&self) -> Self {
        let reverse_map = self.reverse_map.read().clone();

        Self {
            map: self.map.clone(),
            reverse_map: Arc::new(RwLock::new(reverse_map)),
        }
    }

    pub fn read(&self) -> LockedDictMapper {
        LockedDictMapper {
            map: self.map.read(),
            reverse_map: self.reverse_map.read(),
        }
    }

    pub fn write(&self) -> WriteLockedDictMapper {
        WriteLockedDictMapper {
            map: self.map.write(),
            reverse_map: self.reverse_map.write(),
        }
    }

    pub fn get_or_create_id<Q, T>(&self, name: &Q) -> MaybeNew<usize>
    where
        Q: Hash + Eq + ?Sized + ToOwned<Owned = T> + Borrow<str>,
        T: Into<ArcStr>,
    {
        let map = self.map.read();
        if let Some(existing_id) = map.get(name.borrow()) {
            return MaybeNew::Existing(*existing_id);
        }
        drop(map);

        let mut map = self.map.write();

        let name = name.to_owned().into();
        let new_id = match map.entry(name.clone()) {
            Entry::Occupied(entry) => MaybeNew::Existing(*entry.get()),
            Entry::Vacant(entry) => {
                let mut reverse = self.reverse_map.write();
                let id = reverse.len();
                reverse.push(name);
                entry.insert(id);
                MaybeNew::New(id)
            }
        };
        new_id
    }

    pub fn get_id(&self, name: &str) -> Option<usize> {
        self.map.read().get(name).map(|id| *id)
    }

    /// Explicitly set the id for a key (useful for initialising the map in parallel)
    pub fn set_id(&self, name: impl Into<ArcStr>, id: usize) {
        let mut map = self.map.write();
        let arc_name = name.into();
        let map_entry = map.entry(arc_name.clone());
        let mut keys = self.reverse_map.write();
        if keys.len() <= id {
            keys.resize(id + 1, Default::default())
        }
        keys[id] = arc_name;
        map_entry.insert_entry(id);
    }

    pub fn set_reverse_id(&self, id: usize, name: impl Into<ArcStr>) {
        let mut keys = self.reverse_map.write();
        if keys.len() <= id {
            keys.resize(id + 1, Default::default())
        }
        keys[id] = name.into();
    }

    pub fn has_name(&self, id: usize) -> bool {
        let guard = self.reverse_map.read();
        guard.get(id).is_some()
    }

    pub fn get_name(&self, id: usize) -> ArcStr {
        let guard = self.reverse_map.read();
        guard.get(id).cloned().expect(&format!(
            "internal ids should always be mapped to a name {id}"
        ))
    }

    pub fn get_keys(&self) -> ArcReadLockedVec<ArcStr> {
        ArcReadLockedVec {
            guard: self.reverse_map.read_arc(),
        }
    }

    pub fn get_values(&self) -> Vec<usize> {
        self.map.read().iter().map(|(_, &entry)| entry).collect()
    }

    pub fn len(&self) -> usize {
        self.reverse_map.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.reverse_map.read().is_empty()
    }
}

#[cfg(test)]
mod test {
    use crate::core::storage::dict_mapper::DictMapper;
    use quickcheck_macros::quickcheck;
    use rand::seq::SliceRandom;
    use rayon::prelude::*;
    use std::collections::HashMap;

    #[test]
    fn test_dict_mapper() {
        let mapper = DictMapper::default();
        assert_eq!(mapper.get_or_create_id("test"), 0usize);
        assert_eq!(mapper.get_or_create_id("test").inner(), 0);
        assert_eq!(mapper.get_or_create_id("test2").inner(), 1);
        assert_eq!(mapper.get_or_create_id("test2").inner(), 1);
        assert_eq!(mapper.get_or_create_id("test").inner(), 0);
    }

    #[quickcheck]
    fn check_dict_mapper_concurrent_write(write: Vec<String>) -> bool {
        let n = 100;
        let mapper: DictMapper = DictMapper::default();

        // create n maps from strings to ids in parallel
        let res: Vec<HashMap<String, usize>> = (0..n)
            .into_par_iter()
            .map(|_| {
                let mut ids: HashMap<String, usize> = Default::default();
                let mut rng = rand::thread_rng();
                let mut write_s = write.clone();
                write_s.shuffle(&mut rng);
                for s in write_s {
                    let id = mapper.get_or_create_id(s.as_str());
                    ids.insert(s, id.inner());
                }
                ids
            })
            .collect();

        // check that all maps are the same and that all strings have been assigned an id
        let res_0 = &res[0];
        res[1..n].iter().all(|v| res_0 == v) && write.iter().all(|v| mapper.get_id(v).is_some())
    }

    // map 5 strings to 5 ids from 4 threads concurrently 1000 times
    #[test]
    fn test_dict_mapper_concurrent() {
        use std::{sync::Arc, thread};

        let mapper = Arc::new(DictMapper::default());
        let mut threads = Vec::new();
        for _ in 0..4 {
            let mapper = Arc::clone(&mapper);
            threads.push(thread::spawn(move || {
                for _ in 0..1000 {
                    mapper.get_or_create_id("test");
                    mapper.get_or_create_id("test2");
                    mapper.get_or_create_id("test3");
                    mapper.get_or_create_id("test4");
                    mapper.get_or_create_id("test5");
                }
            }));
        }

        for thread in threads {
            thread.join().unwrap();
        }

        let mut actual = vec!["test", "test2", "test3", "test4", "test5"]
            .into_iter()
            .map(|name| mapper.get_or_create_id(name).inner())
            .collect::<Vec<_>>();
        actual.sort();

        assert_eq!(actual, vec![0, 1, 2, 3, 4]);
    }
}
