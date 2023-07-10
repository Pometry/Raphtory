use crate::core::entities::properties::props::DictMapper;
use crate::core::entities::properties::tprop::TProp;
use crate::core::{Prop, PropUnwrap};
use crate::db::api::mutation::Properties;
use crate::prelude::Graph;
use chrono::NaiveDateTime;
use std::iter::Zip;
use std::marker::PhantomData;

pub trait CorePropertiesOps {
    fn static_prop_meta(&self) -> &DictMapper<String>;
    fn temporal_prop_meta(&self) -> &DictMapper<String>;
    fn temporal_prop(&self, id: usize) -> Option<&TProp>;
    fn static_prop(&self, id: usize) -> Option<&Prop>;
}

pub trait TemporalPropertyViewOps<Key = String> {
    fn temporal_value(&self, id: &Key) -> Option<Prop> {
        self.temporal_values(id).last().cloned()
    }
    fn temporal_history(&self, id: &Key) -> Vec<i64>;
    fn temporal_values(&self, id: &Key) -> Vec<Prop>;
    fn temporal_value_at(&self, id: &Key, t: i64) -> Option<Prop> {
        let history = self.temporal_history(id);
        match history.binary_search(&t) {
            Ok(index) => Some(self.temporal_values(id)[index].clone()),
            Err(index) => (index > 0).then(|| self.temporal_values(id)[index - 1].clone()),
        }
    }
}

pub trait StaticPropertiesOps {
    fn static_property_keys(&self) -> Vec<String>;
    fn static_property_values(&self) -> Vec<Prop> {
        self.static_property_keys()
            .into_iter()
            .map(|k| self.get_static_property(&k).expect("should exist"))
            .collect()
    }
    fn get_static_property(&self, key: &str) -> Option<Prop>;
}

pub trait TemporalPropertiesOps<Key: 'static = String>: TemporalPropertyViewOps<Key> {
    fn temporal_property_keys(&self) -> Vec<String>;
    fn temporal_property_values(&self) -> Box<dyn Iterator<Item = Key> + '_> {
        Box::new(
            self.temporal_property_keys()
                .into_iter()
                .flat_map(|k| self.get_temporal_property(&k)),
        )
    }
    fn get_temporal_property(&self, key: &str) -> Option<Key>;
}

pub struct TemporalPropertyView<P: TemporalPropertyViewOps<K>, K = String> {
    pub(crate) id: K,
    pub(crate) props: P,
}

impl<P: TemporalPropertyViewOps<K>, K> TemporalPropertyView<P, K> {
    pub(crate) fn new(props: P, key: K) -> Self {
        TemporalPropertyView { props, id: key }
    }
    pub fn history(&self) -> Vec<i64> {
        self.props.temporal_history(&self.id)
    }
    pub fn values(&self) -> Vec<Prop> {
        self.props.temporal_values(&self.id)
    }
    pub fn iter(&self) -> impl Iterator<Item = (i64, Prop)> {
        self.into_iter()
    }
    pub fn at(&self, t: i64) -> Option<Prop> {
        self.props.temporal_value_at(&self.id, t)
    }
    pub fn value(&self) -> Option<Prop> {
        self.props.temporal_value(&self.id)
    }
}

impl<P: TemporalPropertyViewOps<K>, K> IntoIterator for TemporalPropertyView<P, K> {
    type Item = (i64, Prop);
    type IntoIter = Zip<std::vec::IntoIter<i64>, std::vec::IntoIter<Prop>>;

    fn into_iter(self) -> Self::IntoIter {
        let hist = self.history();
        let vals = self.values();
        hist.into_iter().zip(vals)
    }
}

impl<P: TemporalPropertyViewOps<K>, K> IntoIterator for &TemporalPropertyView<P, K> {
    type Item = (i64, Prop);
    type IntoIter = Zip<std::vec::IntoIter<i64>, std::vec::IntoIter<Prop>>;

    fn into_iter(self) -> Self::IntoIter {
        let hist = self.history();
        let vals = self.values();
        hist.into_iter().zip(vals)
    }
}

pub struct StaticProperties<P: StaticPropertiesOps> {
    pub(crate) props: P,
}

impl<P: StaticPropertiesOps> StaticProperties<P> {
    pub(crate) fn new(props: P) -> Self {
        Self { props }
    }
    pub fn keys(&self) -> Vec<String> {
        self.props.static_property_keys()
    }

    pub fn values(&self) -> Vec<Prop> {
        self.props.static_property_values()
    }

    pub fn iter(&self) -> Box<dyn Iterator<Item = (String, Prop)> + '_> {
        Box::new(self.into_iter())
    }

    pub fn get<Q: AsRef<str>>(&self, key: Q) -> Option<Prop> {
        self.props.get_static_property(key.as_ref())
    }
}

impl<P: StaticPropertiesOps> IntoIterator for StaticProperties<P> {
    type Item = (String, Prop);
    type IntoIter = Zip<std::vec::IntoIter<String>, std::vec::IntoIter<Prop>>;

    fn into_iter(self) -> Self::IntoIter {
        let keys = self.keys();
        let vals = self.values();
        keys.into_iter().zip(vals)
    }
}

impl<P: StaticPropertiesOps> IntoIterator for &StaticProperties<P> {
    type Item = (String, Prop);
    type IntoIter = Zip<std::vec::IntoIter<String>, std::vec::IntoIter<Prop>>;

    fn into_iter(self) -> Self::IntoIter {
        let keys = self.keys();
        let vals = self.values();
        keys.into_iter().zip(vals)
    }
}

pub struct TemporalProperties<P: TemporalPropertiesOps<K> + Clone, K: 'static = String> {
    pub(crate) props: P,
    key_marker: PhantomData<K>,
}

impl<P: TemporalPropertiesOps<K> + Clone, K: 'static> Properties for TemporalProperties<P, K> {
    fn collect_properties(self) -> Vec<(String, Prop)> {
        self.iter()
            .flat_map(|(k, v)| v.value().map(|v| (k, v)))
            .collect()
    }
}

impl<P: TemporalPropertiesOps<K> + Clone, K: 'static> IntoIterator for TemporalProperties<P, K> {
    type Item = (String, TemporalPropertyView<P, K>);
    type IntoIter = Zip<std::vec::IntoIter<String>, std::vec::IntoIter<TemporalPropertyView<P, K>>>;

    fn into_iter(self) -> Self::IntoIter {
        let keys = self.keys();
        let values = self.values();
        keys.into_iter().zip(values)
    }
}

impl<P: TemporalPropertiesOps<K> + Clone, K: 'static> IntoIterator for &TemporalProperties<P, K> {
    type Item = (String, TemporalPropertyView<P, K>);
    type IntoIter = Zip<std::vec::IntoIter<String>, std::vec::IntoIter<TemporalPropertyView<P, K>>>;

    fn into_iter(self) -> Self::IntoIter {
        let keys = self.keys();
        let values = self.values();
        keys.into_iter().zip(values)
    }
}

impl<P: TemporalPropertiesOps<K> + Clone, K: 'static> TemporalProperties<P, K> {
    pub(crate) fn new(props: P) -> Self {
        Self {
            props,
            key_marker: PhantomData,
        }
    }
    pub fn keys(&self) -> Vec<String> {
        self.props.temporal_property_keys()
    }

    pub fn values(&self) -> Vec<TemporalPropertyView<P, K>> {
        self.props
            .temporal_property_values()
            .map(|k| TemporalPropertyView::new(self.props.clone(), k))
            .collect()
    }

    pub fn iter(&self) -> impl Iterator<Item = (String, TemporalPropertyView<P, K>)> + '_ {
        self.into_iter()
    }

    pub fn get<Q: AsRef<str>>(&self, key: Q) -> Option<TemporalPropertyView<P, K>> {
        self.props
            .get_temporal_property(key.as_ref())
            .map(|k| TemporalPropertyView::new(self.props.clone(), k))
    }
}

impl<P: TemporalPropertyViewOps<K>, K> PropUnwrap for TemporalPropertyView<P, K> {
    fn into_str(self) -> Option<String> {
        self.value().into_str()
    }

    fn into_i32(self) -> Option<i32> {
        self.value().into_i32()
    }

    fn into_i64(self) -> Option<i64> {
        self.value().into_i64()
    }

    fn into_u32(self) -> Option<u32> {
        self.value().into_u32()
    }

    fn into_u64(self) -> Option<u64> {
        self.value().into_u64()
    }

    fn into_f32(self) -> Option<f32> {
        self.value().into_f32()
    }

    fn into_f64(self) -> Option<f64> {
        self.value().into_f64()
    }

    fn into_bool(self) -> Option<bool> {
        self.value().into_bool()
    }

    fn into_dtime(self) -> Option<NaiveDateTime> {
        self.value().into_dtime()
    }

    fn into_graph(self) -> Option<Graph> {
        self.value().into_graph()
    }
}
