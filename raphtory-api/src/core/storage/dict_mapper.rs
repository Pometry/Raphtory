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

    /// Explicitly set the id for a key (useful for initialising the map in parallel)
    pub fn set_id(&self, name: impl Into<ArcStr>, id: usize) {
        let arc_name = name.into();
        self.map.insert(arc_name.clone(), id);
        let mut keys = self.reverse_map.write();
        if keys.len() <= id {
            keys.resize(id + 1, "".into())
        }
        keys[id] = arc_name;
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
        assert_eq!(mapper.get_or_create_id("test"), 0);
        assert_eq!(mapper.get_or_create_id("test"), 0);
        assert_eq!(mapper.get_or_create_id("test2"), 1);
        assert_eq!(mapper.get_or_create_id("test2"), 1);
        assert_eq!(mapper.get_or_create_id("test"), 0);
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
                    ids.insert(s, id);
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
            .map(|name| mapper.get_or_create_id(name))
            .collect::<Vec<_>>();
        actual.sort();

        assert_eq!(actual, vec![0, 1, 2, 3, 4]);
    }
}
