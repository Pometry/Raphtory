use crate::core::{
    entities::{graph::tgraph::FxDashMap, properties::tprop::TProp},
    storage::{
        lazy_vec::{IllegalSet, LazyVec},
        locked_view::LockedView,
        timeindex::TimeIndexEntry,
    },
    utils::errors::{GraphError, IllegalMutate, MutateGraphError},
    ArcStr, Prop, PropType,
};
use lock_api;
use parking_lot::{RwLock, RwLockReadGuard};
use serde::{Deserialize, Serialize};
use std::{
    borrow::Borrow,
    fmt::Debug,
    hash::Hash,
    ops::Deref,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tantivy::HasLen;

type ArcRwLockReadGuard<T> = lock_api::ArcRwLockReadGuard<parking_lot::RawRwLock, T>;

#[derive(Serialize, Deserialize, Default, Debug, PartialEq)]
pub struct Props {
    // properties
    constant_props: LazyVec<Option<Prop>>,
    temporal_props: LazyVec<TProp>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
enum PropId {
    Static(usize),
    Temporal(usize),
}

impl Props {
    pub fn new() -> Self {
        Self {
            constant_props: LazyVec::Empty,
            temporal_props: LazyVec::Empty,
        }
    }

    pub fn add_prop(
        &mut self,
        t: TimeIndexEntry,
        prop_id: usize,
        prop: Prop,
    ) -> Result<(), GraphError> {
        self.temporal_props.update(prop_id, |p| p.set(t, prop))
    }

    pub fn add_constant_prop(
        &mut self,
        prop_id: usize,
        prop: Prop,
    ) -> Result<(), IllegalSet<Option<Prop>>> {
        self.constant_props.set(prop_id, Some(prop))
    }

    pub fn update_constant_prop(&mut self, prop_id: usize, prop: Prop) -> Result<(), GraphError> {
        self.constant_props.update(prop_id, |mut n| {
            *n = Some(prop);
            Ok(())
        })
    }

    pub fn temporal_props(&self, prop_id: usize) -> Box<dyn Iterator<Item = (i64, Prop)> + '_> {
        let o = self.temporal_props.get(prop_id);
        if let Some(t_prop) = o {
            Box::new(t_prop.iter())
        } else {
            Box::new(std::iter::empty())
        }
    }

    pub fn temporal_props_window(
        &self,
        prop_id: usize,
        start: i64,
        end: i64,
    ) -> Box<dyn Iterator<Item = (i64, Prop)> + '_> {
        let o = self.temporal_props.get(prop_id);
        if let Some(t_prop) = o {
            Box::new(t_prop.iter_window_t(start..end))
        } else {
            Box::new(std::iter::empty())
        }
    }

    pub fn const_prop(&self, prop_id: usize) -> Option<&Prop> {
        let prop = self.constant_props.get(prop_id)?;
        prop.as_ref()
    }

    pub fn temporal_prop(&self, prop_id: usize) -> Option<&TProp> {
        self.temporal_props.get(prop_id)
    }

    pub fn const_prop_ids(&self) -> impl Iterator<Item = usize> + '_ {
        self.constant_props.filled_ids()
    }

    pub fn temporal_prop_ids(&self) -> impl Iterator<Item = usize> + '_ {
        self.temporal_props.filled_ids()
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Meta {
    meta_prop_temporal: PropMapper,
    meta_prop_constant: PropMapper,
    meta_layer: DictMapper,
}

impl Meta {
    pub fn const_prop_meta(&self) -> &PropMapper {
        &self.meta_prop_constant
    }

    pub fn temporal_prop_meta(&self) -> &PropMapper {
        &self.meta_prop_temporal
    }

    pub fn layer_meta(&self) -> &DictMapper {
        &self.meta_layer
    }

    pub fn new() -> Self {
        let meta_layer = DictMapper::default();
        meta_layer.get_or_create_id("_default");
        Self {
            meta_prop_temporal: PropMapper::default(),
            meta_prop_constant: PropMapper::default(),
            meta_layer, // layer 0 is the default layer
        }
    }

    #[inline]
    pub fn resolve_prop_id(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<usize, GraphError> {
        if is_static {
            self.meta_prop_constant
                .get_or_create_and_validate(prop, dtype)
        } else {
            self.meta_prop_temporal
                .get_or_create_and_validate(prop, dtype)
        }
    }

    #[inline]
    pub fn get_prop_id(&self, name: &str, is_static: bool) -> Option<usize> {
        if is_static {
            self.meta_prop_constant.get_id(name)
        } else {
            self.meta_prop_temporal.get_id(name)
        }
    }

    #[inline]
    pub fn get_or_create_layer_id(&self, name: &str) -> usize {
        self.meta_layer.get_or_create_id(name)
    }

    #[inline]
    pub fn get_layer_id(&self, name: &str) -> Option<usize> {
        self.meta_layer.map.get(name).as_deref().copied()
    }

    pub fn get_layer_name_by_id(&self, id: usize) -> ArcStr {
        self.meta_layer.get_name(id)
    }

    pub fn get_all_layers(&self) -> Vec<usize> {
        self.meta_layer
            .map
            .iter()
            .map(|entry| *entry.value())
            .collect()
    }

    pub fn get_all_property_names(&self, is_static: bool) -> ArcReadLockedVec<ArcStr> {
        if is_static {
            self.meta_prop_constant.get_keys()
        } else {
            self.meta_prop_temporal.get_keys()
        }
    }

    pub fn get_prop_name(&self, prop_id: usize, is_static: bool) -> ArcStr {
        if is_static {
            self.meta_prop_constant.get_name(prop_id)
        } else {
            self.meta_prop_temporal.get_name(prop_id)
        }
    }
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct DictMapper {
    map: FxDashMap<ArcStr, usize>,
    reverse_map: Arc<RwLock<Vec<ArcStr>>>, //FIXME: a boxcar vector would be a great fit if it was serializable...
}

#[derive(Debug)]
pub struct ArcReadLockedVec<T> {
    guard: ArcRwLockReadGuard<Vec<T>>,
}

impl<T> Deref for ArcReadLockedVec<T> {
    type Target = Vec<T>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.guard.deref()
    }
}

impl<T: Clone> IntoIterator for ArcReadLockedVec<T> {
    type Item = T;
    type IntoIter = LockedIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        let guard = self.guard;
        let len = guard.len();
        let pos = 0;
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
}

impl DictMapper {
    pub fn get_or_create_id<Q, T>(&self, name: &Q) -> usize
    where
        ArcStr: Borrow<Q>,
        Q: Hash + Eq + ?Sized + ToOwned<Owned = T>,
        T: Into<ArcStr>,
    {
        if let Some(existing_id) = self.map.get(name) {
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

    pub fn get_name(&self, id: usize) -> ArcStr {
        let guard = self.reverse_map.read();
        guard
            .get(id)
            .map(|v| v.clone())
            .expect("internal ids should always be mapped to a name")
    }

    pub fn get_keys(&self) -> ArcReadLockedVec<ArcStr> {
        ArcReadLockedVec {
            guard: self.reverse_map.read_arc(),
        }
    }

    pub fn len(&self) -> usize {
        self.reverse_map.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.reverse_map.read().is_empty()
    }
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct PropMapper {
    id_mapper: DictMapper,
    dtypes: Arc<RwLock<Vec<PropType>>>,
}

impl Deref for PropMapper {
    type Target = DictMapper;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.id_mapper
    }
}

impl PropMapper {
    fn get_or_create_and_validate(&self, prop: &str, dtype: PropType) -> Result<usize, GraphError> {
        let id = self.id_mapper.get_or_create_id(prop);
        let dtype_read = self.dtypes.read_recursive();
        if let Some(old_type) = dtype_read.get(id) {
            if !matches!(old_type, PropType::Empty) {
                return if *old_type == dtype {
                    Ok(id)
                } else {
                    Err(GraphError::PropertyTypeError {
                        name: prop.to_owned(),
                        expected: *old_type,
                        actual: dtype,
                    })
                };
            }
        }
        drop(dtype_read); // drop the read lock and wait for write lock as type did not exist yet
        let mut dtype_write = self.dtypes.write();
        match dtype_write.get(id) {
            Some(&old_type) => {
                if matches!(old_type, PropType::Empty) {
                    // vector already resized but this id is not filled yet, set the dtype and return id
                    dtype_write[id] = dtype;
                    Ok(id)
                } else {
                    // already filled because a different thread won the race for this id, check the type matches
                    if old_type == dtype {
                        Ok(id)
                    } else {
                        Err(GraphError::PropertyTypeError {
                            name: prop.to_owned(),
                            expected: old_type,
                            actual: dtype,
                        })
                    }
                }
            }
            None => {
                // vector not resized yet, resize it and set the dtype and return id
                dtype_write.resize(id + 1, PropType::Empty);
                dtype_write[id] = dtype;
                Ok(id)
            }
        }
    }

    pub fn get_dtype(&self, prop_id: usize) -> Option<PropType> {
        self.dtypes.read_recursive().get(prop_id).copied()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use rand::seq::SliceRandom;
    use rayon::prelude::*;
    use std::{collections::HashMap, sync::Arc, thread};

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

    #[test]
    fn test_prop_mapper_concurrent() {
        let values = [Prop::I64(1), Prop::U16(0), Prop::Bool(true), Prop::F64(0.0)];
        let input_len = values.len();

        let mapper = Arc::new(PropMapper::default());
        let threads: Vec<_> = values
            .into_iter()
            .map(move |v| {
                let mapper = mapper.clone();
                thread::spawn(move || mapper.get_or_create_and_validate("test", v.dtype()))
            })
            .flat_map(|t| t.join())
            .collect();

        assert_eq!(threads.len(), input_len); // no errors
        assert_eq!(threads.into_iter().flatten().count(), 1); // only one result (which ever was first)
    }
}
