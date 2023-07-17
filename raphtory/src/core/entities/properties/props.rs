use crate::core::{
    entities::{graph::tgraph::FxDashMap, properties::tprop::TProp},
    storage::lazy_vec::LazyVec,
    utils::errors::{IllegalMutate, MutateGraphError},
    Prop,
};
use serde::{Deserialize, Serialize};
use std::{
    hash::Hash,
    sync::atomic::{AtomicUsize, Ordering},
};

#[derive(Serialize, Deserialize, Default, Debug, PartialEq)]
pub struct Props {
    // properties
    static_props: LazyVec<Option<Prop>>,
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
            static_props: LazyVec::Empty,
            temporal_props: LazyVec::Empty,
        }
    }

    pub fn add_prop(&mut self, t: i64, prop_id: usize, prop: Prop) {
        self.temporal_props
            .update_or_set(prop_id, |p| p.set(t, &prop), TProp::from(t, &prop))
    }

    pub fn add_static_prop(
        &mut self,
        prop_id: usize,
        prop_name: &str,
        prop: Prop,
    ) -> Result<(), MutateGraphError> {
        self.static_props.set(prop_id, Some(prop)).map_err(|err| {
            MutateGraphError::IllegalGraphPropertyChange {
                source: IllegalMutate::from_source(err, prop_name),
            }
        })
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
        let prop = self.static_props.get(prop_id)?;
        prop.as_ref()
    }

    pub fn temporal_prop(&self, prop_id: usize) -> Option<&TProp> {
        self.temporal_props.get(prop_id)
    }

    pub fn static_prop_ids(&self) -> Vec<usize> {
        self.static_props.filled_ids()
    }

    pub fn temporal_prop_ids(&self) -> Vec<usize> {
        self.temporal_props.filled_ids()
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Meta {
    meta_prop_temporal: DictMapper<String>,
    meta_prop_static: DictMapper<String>,
    meta_layer: DictMapper<String>,
}

impl Meta {
    pub fn new() -> Self {
        let meta_layer = DictMapper::default();
        meta_layer.get_or_create_id("_default".to_owned());
        Self {
            meta_prop_temporal: DictMapper::default(),
            meta_prop_static: DictMapper::default(),
            meta_layer, // layer 0 is the default layer
        }
    }

    pub fn resolve_prop_ids(
        &self,
        props: Vec<(String, Prop)>,
        is_static: bool,
    ) -> impl Iterator<Item = (usize, String, Prop)> + '_ {
        props.into_iter().map(move |(name, prop)| {
            let prop_id = if !is_static {
                self.meta_prop_temporal.get_or_create_id(name.clone())
            } else {
                self.meta_prop_static.get_or_create_id(name.clone())
            };
            (prop_id, name, prop)
        })
    }

    pub fn resolve_prop_id(&self, name: &str, is_static: bool) -> usize {
        if is_static {
            self.meta_prop_static.get_or_create_id(name.to_string())
        } else {
            self.meta_prop_temporal.get_or_create_id(name.to_string())
        }
    }

    pub fn find_prop_id(&self, name: &str, is_static: bool) -> Option<usize> {
        if is_static {
            self.meta_prop_static.get(&name.to_owned())
        } else {
            self.meta_prop_temporal.get(&name.to_owned())
        }
    }

    pub fn get_or_create_layer_id(&self, name: String) -> usize {
        self.meta_layer.get_or_create_id(name)
    }

    pub fn get_layer_id(&self, name: &str) -> Option<usize> {
        self.meta_layer.map.get(name).as_deref().copied()
    }

    pub fn get_layer_name_by_id(&self, id: usize) -> Option<String> {
        self.meta_layer
            .map
            .iter()
            .find(|entry| entry.value() == &id)
            .map(|entry| entry.key().clone())
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

    pub fn reverse_prop_id(&self, prop_id: usize, is_static: bool) -> Option<String> {
        if is_static {
            self.meta_prop_static.reverse_lookup(&prop_id)
        } else {
            self.meta_prop_temporal.reverse_lookup(&prop_id)
        }
    }
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct DictMapper<T: Hash + Eq> {
    map: FxDashMap<T, usize>,
    reverse_map: FxDashMap<usize, T>,
    counter: AtomicUsize,
}

impl<T: Hash + Eq + Clone> DictMapper<T> {
    pub fn get_or_create_id(&self, name: T) -> usize {
        if let Some(existing_id) = self.map.get(&name) {
            return *existing_id;
        }

        let new_id = self.map.entry(name.clone()).or_insert_with(|| {
            let id = self.counter.fetch_add(1, Ordering::Relaxed);
            self.reverse_map.insert(id, name);
            id
        });
        *new_id
    }

    pub fn get(&self, name: &T) -> Option<usize> {
        self.map.get(name).map(|id| *id)
    }

    fn reverse_lookup(&self, id: &usize) -> Option<T> {
        self.reverse_map.get(id).map(|name| name.clone())
    }

    pub fn get_keys(&self) -> Vec<T> {
        self.map.iter().map(|entry| entry.key().clone()).collect()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_dict_mapper() {
        let mapper = DictMapper::default();
        assert_eq!(mapper.get_or_create_id("test"), 0);
        assert_eq!(mapper.get_or_create_id("test"), 0);
        assert_eq!(mapper.get_or_create_id("test2"), 1);
        assert_eq!(mapper.get_or_create_id("test2"), 1);
        assert_eq!(mapper.get_or_create_id("test"), 0);
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
