use std::{
    hash::Hash,
    sync::atomic::{AtomicUsize, Ordering},
};

use serde::{Deserialize, Serialize};

use crate::core::{lazy_vec::LazyVec, tprop::TProp, Prop};

use super::tgraph::FxDashMap;

#[derive(Serialize, Deserialize, Default, Debug, PartialEq)]
pub(crate) struct Props {
    // properties
    static_props: LazyVec<Option<Prop>>,
    temporal_props: LazyVec<TProp>,
}

impl Props {
    pub fn new() -> Self {
        Self {
            static_props: LazyVec::Empty,
            temporal_props: LazyVec::Empty,
        }
    }

    pub(crate) fn add_prop(&mut self, t: i64, prop_id: usize, prop: Prop) {
        self.temporal_props
            .update_or_set(prop_id, |p| p.set(t, &prop), TProp::from(t, &prop))
    }

    pub(crate) fn temporal_props(
        &self,
        prop_id: usize,
    ) -> Box<dyn Iterator<Item = (i64, Prop)> + '_> {
        let o = self.temporal_props.get(prop_id);
        if let Some(t_prop) = o {
            Box::new(t_prop.iter().map(|(t, p)| (t, p.clone())))
        } else {
            Box::new(std::iter::empty())
        }
    }

    pub(crate) fn static_prop(&self, prop_id: usize) -> Option<&Prop> {
        let prop = self.static_props.get(prop_id)?;
        prop.as_ref()
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct Meta {
    meta_prop_temporal: DictMapper<String>,
    meta_prop_static: DictMapper<String>,
    meta_layer: DictMapper<String>,
}

impl Meta {
    pub(crate) fn new() -> Self {
        Self {
            meta_prop_temporal: DictMapper::default(),
            meta_prop_static: DictMapper::default(),
            meta_layer: DictMapper::new(1), // layer 0 is the default layer
        }
    }

    pub(crate) fn resolve_prop_ids(
        &self,
        props: Vec<(String, Prop)>,
    ) -> impl Iterator<Item = (usize, Prop)> + '_ {
        props.into_iter().map(move |(name, prop)| {
            let prop_id = self.meta_prop_temporal.get_or_create_id(name.clone());
            (prop_id, prop)
        })
    }

    pub(crate) fn resolve_prop_id(&self, name: &str, is_static: bool) -> usize {
        if is_static {
            self.meta_prop_static.get_or_create_id(name.to_string())
        } else {
            self.meta_prop_temporal.get_or_create_id(name.to_string())
        }
    }

    pub(crate) fn get_or_create_layer_id(&self, name: String) -> usize {
        self.meta_layer.get_or_create_id(name)
    }

    pub(crate) fn get_layer_id(&self, name: &str) -> Option<usize> {
        self.meta_layer.map.get(name).as_deref().copied()
    }

    pub(crate) fn get_layer_name_by_id(&self, id: usize) -> Option<String> {
        self.meta_layer
            .map
            .iter()
            .find(|entry| entry.value() == &id)
            .map(|entry| entry.key().clone())
    }

    pub(crate) fn get_all_layers(&self) -> Vec<usize> {
        self.meta_layer.map.iter().map(|entry| *entry.value()).collect()
    }
}

#[derive(Serialize, Deserialize, Default, Debug)]
struct DictMapper<T: Hash + Eq> {
    map: FxDashMap<T, usize>,
    counter: AtomicUsize,
}

impl<T: Hash + Eq> DictMapper<T> {

    pub(crate) fn new(start_at: usize) -> Self {
        Self {
            map: FxDashMap::default(),
            counter: AtomicUsize::new(start_at),
        }
    }

    fn get_or_create_id(&self, name: T) -> usize {
        if let Some(existing_id) = self.map.get(&name) {
            return *existing_id;
        }

        let new_id = self
            .map
            .entry(name)
            .or_insert_with(|| self.counter.fetch_add(1, Ordering::Relaxed));
        *new_id
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
        use std::sync::Arc;
        use std::thread;

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
