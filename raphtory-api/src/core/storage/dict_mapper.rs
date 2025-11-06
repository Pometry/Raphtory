use crate::core::{
    entities::properties::meta::STATIC_GRAPH_LAYER,
    storage::{arc_str::ArcStr, ArcRwLockReadGuard},
};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use std::{
    borrow::{Borrow, BorrowMut},
    collections::hash_map::Entry,
    hash::Hash,
    ops::{Deref, DerefMut},
    sync::Arc,
};

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct DictMapper {
    map: Arc<RwLock<FxHashMap<ArcStr, usize>>>,
    reverse_map: Arc<RwLock<Vec<ArcStr>>>,
    num_private_fields: usize,
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
    num_private_fields: usize,
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

    pub fn iter_ids(&self) -> impl Iterator<Item = (usize, &ArcStr)> + '_ {
        self.reverse_map
            .iter()
            .enumerate()
            .skip(self.num_private_fields)
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
    pub fn new_layer_mapper() -> Self {
        Self::new_with_private_fields([STATIC_GRAPH_LAYER])
    }

    pub fn new_with_private_fields(fields: impl IntoIterator<Item = impl Into<ArcStr>>) -> Self {
        let fields: Vec<_> = fields.into_iter().map(|s| s.into()).collect();
        let num_private_fields = fields.len();
        DictMapper {
            map: Arc::new(Default::default()),
            reverse_map: Arc::new(RwLock::new(fields)),
            num_private_fields,
        }
    }
    pub fn contains(&self, key: &str) -> bool {
        self.map.read().contains_key(key)
    }

    pub fn deep_clone(&self) -> Self {
        let reverse_map = self.reverse_map.read_recursive().clone();

        Self {
            map: self.map.clone(),
            reverse_map: Arc::new(RwLock::new(reverse_map)),
            num_private_fields: self.num_private_fields,
        }
    }

    pub fn read(&self) -> LockedDictMapper<'_> {
        LockedDictMapper {
            map: self.map.read(),
            reverse_map: self.reverse_map.read(),
            num_private_fields: self.num_private_fields,
        }
    }

    pub fn write(&self) -> WriteLockedDictMapper<'_> {
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
        self.map.read().get(name).copied()
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

    pub fn has_id(&self, id: usize) -> bool {
        let guard = self.reverse_map.read_recursive();
        guard.get(id).is_some()
    }

    pub fn get_name(&self, id: usize) -> ArcStr {
        let guard = self.reverse_map.read_recursive();
        guard
            .get(id)
            .cloned()
            .expect("internal ids should always be mapped to a name")
    }

    /// Public ids
    pub fn ids(&self) -> impl Iterator<Item = usize> {
        self.num_private_fields..self.num_all_fields()
    }

    /// All ids, including private fields
    pub fn all_ids(&self) -> impl Iterator<Item = usize> {
        0..self.num_all_fields()
    }

    /// Public keys
    pub fn keys(&self) -> PublicKeys<ArcStr> {
        PublicKeys {
            guard: self.reverse_map.read_arc_recursive(),
            num_private_fields: self.num_private_fields,
        }
    }

    /// All keys including private fields
    pub fn all_keys(&self) -> AllKeys<ArcStr> {
        AllKeys {
            guard: self.reverse_map.read_arc(),
        }
    }

    pub fn num_all_fields(&self) -> usize {
        self.reverse_map.read_recursive().len()
    }

    pub fn num_fields(&self) -> usize {
        self.map.read_recursive().len()
    }

    pub fn num_private_fields(&self) -> usize {
        self.num_private_fields
    }
}

#[cfg(test)]
mod test {
    use crate::core::storage::dict_mapper::DictMapper;
    use proptest::prelude::*;
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

    #[test]
    fn check_dict_mapper_concurrent_write() {
        proptest!(|(write: Vec<String>)| {
            let n = 100;
            let mapper: DictMapper = DictMapper::default();

            // create n maps from strings to ids in parallel
            let res: Vec<HashMap<String, usize>> = (0..n)
                .into_par_iter()
                .map(|_| {
                    let mut ids: HashMap<String, usize> = Default::default();
                    let mut rng = rand::rng();
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
            prop_assert!(res[1..n].iter().all(|v| res_0 == v) && write.iter().all(|v| mapper.get_id(v).is_some()));
        });
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

#[derive(Debug)]
pub struct AllKeys<T> {
    pub(crate) guard: ArcRwLockReadGuard<Vec<T>>,
}

impl<T> Deref for AllKeys<T> {
    type Target = [T];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.guard.deref().deref()
    }
}

impl<T: Clone> IntoIterator for AllKeys<T> {
    type Item = T;
    type IntoIter = LockedIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        let guard = self.guard;
        let len = guard.len();
        let pos = 0;
        LockedIter { guard, pos, len }
    }
}

pub struct PublicKeys<T> {
    guard: ArcRwLockReadGuard<Vec<T>>,
    num_private_fields: usize,
}

impl<T> PublicKeys<T> {
    fn items(&self) -> &[T] {
        &self.guard[self.num_private_fields..]
    }
    pub fn iter(&self) -> impl Iterator<Item = &T> + '_ {
        self.items().iter()
    }

    pub fn len(&self) -> usize {
        self.items().len()
    }

    pub fn is_empty(&self) -> bool {
        self.items().is_empty()
    }
}

impl<T: Clone> IntoIterator for PublicKeys<T> {
    type Item = T;
    type IntoIter = LockedIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        let guard = self.guard;
        let len = guard.len();
        let pos = self.num_private_fields;
        LockedIter { guard, pos, len }
    }
}

pub struct LockedIter<T> {
    guard: ArcRwLockReadGuard<Vec<T>>,
    pos: usize,
    len: usize,
}

impl<T: Clone> Iterator for LockedIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos < self.len {
            let next_val = Some(self.guard[self.pos].clone());
            self.pos += 1;
            next_val
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.len - self.pos;
        (len, Some(len))
    }
}

impl<T: Clone> ExactSizeIterator for LockedIter<T> {}
