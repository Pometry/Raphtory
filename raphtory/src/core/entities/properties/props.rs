use crate::core::{
    entities::{graph::tgraph::FxDashMap, properties::tprop::TProp},
    storage::{
        lazy_vec::{IllegalSet, LazyVec},
        locked_view::LockedView,
        timeindex::TimeIndexEntry,
    },
    utils::errors::{GraphError, IllegalMutate, MutateGraphError},
    Prop, PropType,
};
use parking_lot::{RwLock, RwLockReadGuard};
use serde::{Deserialize, Serialize};
use std::{
    borrow::Borrow,
    fmt::Debug,
    hash::Hash,
    ops::Deref,
    sync::atomic::{AtomicUsize, Ordering},
};

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

    pub fn temporal_props(&self, prop_id: usize) -> Box<dyn Iterator<Item = (i64, Prop)> + '_> {
        let o = self.temporal_props.get(prop_id);
        if let Some(t_prop) = o {
            Box::new(t_prop.iter().map(|(t, p)| (t, p.clone())))
        } else {
            Box::new(std::iter::empty())
        }
    }

    pub fn temporal_props_window(
        &self,
        prop_id: usize,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = (i64, Prop)> + '_> {
        let o = self.temporal_props.get(prop_id);
        if let Some(t_prop) = o {
            Box::new(
                t_prop
                    .iter_window(t_start..t_end)
                    .map(|(t, p)| (t, p.clone())),
            )
        } else {
            Box::new(std::iter::empty())
        }
    }

    pub fn static_prop(&self, prop_id: usize) -> Option<&Prop> {
        let prop = self.constant_props.get(prop_id)?;
        prop.as_ref()
    }

    pub fn temporal_prop(&self, prop_id: usize) -> Option<&TProp> {
        self.temporal_props.get(prop_id)
    }

    pub fn static_prop_ids(&self) -> Vec<usize> {
        self.constant_props.filled_ids()
    }

    pub fn temporal_prop_ids(&self) -> Vec<usize> {
        self.temporal_props.filled_ids()
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Meta {
    meta_prop_temporal: PropMapper,
    meta_prop_static: PropMapper,
    meta_layer: DictMapper<String>,
}

impl Meta {
    pub fn static_prop_meta(&self) -> &PropMapper {
        &self.meta_prop_static
    }

    pub fn temporal_prop_meta(&self) -> &PropMapper {
        &self.meta_prop_temporal
    }

    pub fn layer_meta(&self) -> &DictMapper<String> {
        &self.meta_layer
    }

    pub fn new() -> Self {
        let meta_layer = DictMapper::default();
        meta_layer.get_or_create_id("_default");
        Self {
            meta_prop_temporal: PropMapper::default(),
            meta_prop_static: PropMapper::default(),
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
            self.meta_prop_static
                .get_or_create_and_validate(prop, dtype)
        } else {
            self.meta_prop_temporal
                .get_or_create_and_validate(prop, dtype)
        }
    }

    #[inline]
    pub fn find_prop_id(&self, name: &str, is_static: bool) -> Option<usize> {
        if is_static {
            self.meta_prop_static.get(&name.to_owned())
        } else {
            self.meta_prop_temporal.get(&name.to_owned())
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

    pub fn get_layer_name_by_id(&self, id: usize) -> Option<String> {
        self.meta_layer.reverse_lookup(id).map(|v| v.to_string())
    }

    pub fn get_all_layers(&self) -> Vec<usize> {
        self.meta_layer
            .map
            .iter()
            .map(|entry| *entry.value())
            .collect()
    }

    pub fn get_all_property_names(&self, is_static: bool) -> Vec<String> {
        if is_static {
            self.meta_prop_static
                .map
                .iter()
                .map(|entry| entry.key().clone())
                .collect()
        } else {
            self.meta_prop_temporal
                .map
                .iter()
                .map(|entry| entry.key().clone())
                .collect()
        }
    }

    pub fn reverse_prop_id(&self, prop_id: usize, is_static: bool) -> Option<LockedView<String>> {
        if is_static {
            self.meta_prop_static.reverse_lookup(prop_id)
        } else {
            self.meta_prop_temporal.reverse_lookup(prop_id)
        }
    }
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct DictMapper<T: Hash + Eq> {
    map: FxDashMap<T, usize>,
    reverse_map: RwLock<Vec<T>>, //FIXME: a boxcar vector would be a great fit if it was serializable...
}

impl<T: Hash + Eq + Clone + Debug> DictMapper<T> {
    pub fn get_or_create_id<Q>(&self, name: &Q) -> usize
    where
        T: Borrow<Q>,
        Q: Hash + Eq + ?Sized + ToOwned<Owned = T>,
    {
        if let Some(existing_id) = self.map.get(name) {
            return *existing_id;
        }

        let name = name.to_owned();
        let new_id = self.map.entry(name.clone()).or_insert_with(|| {
            let mut reverse = self.reverse_map.write();
            let id = reverse.len();
            reverse.push(name);
            id
        });
        *new_id
    }

    pub fn get(&self, name: &T) -> Option<usize> {
        self.map.get(name).map(|id| *id)
    }

    pub fn reverse_lookup(&self, id: usize) -> Option<LockedView<T>> {
        let guard = self.reverse_map.read();
        (id < guard.len()).then(|| RwLockReadGuard::map(guard, |v| &v[id]).into())
    }

    pub fn get_keys(&self) -> RwLockReadGuard<Vec<T>> {
        self.reverse_map.read()
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
    id_mapper: DictMapper<String>,
    dtypes: RwLock<Vec<PropType>>,
}

impl Deref for PropMapper {
    type Target = DictMapper<String>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.id_mapper
    }
}

impl PropMapper {
    pub fn get_or_create_id<Q>(&self, name: &Q) -> usize
    where
        Q: Hash + Eq + ?Sized + ToOwned<Owned = String>,
    {
        todo!()
    }
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
        let mapper: DictMapper<String> = DictMapper::default();

        let res: Vec<HashMap<String, usize>> = (0..n)
            .into_par_iter()
            .map(|_| {
                let mut ids: HashMap<String, usize> = Default::default();
                let mut rng = rand::thread_rng();
                let mut write_s = write.clone();
                write_s.shuffle(&mut rng);
                for s in write_s {
                    let id = mapper.get_or_create_id(&s);
                    ids.insert(s, id);
                }
                ids
            })
            .collect();
        let res_0 = &res[0];
        res[1..n].iter().all(|v| res_0 == v) && write.iter().all(|v| mapper.get(v).is_some())
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
