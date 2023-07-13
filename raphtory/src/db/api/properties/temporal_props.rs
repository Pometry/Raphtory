use crate::core::{Prop, PropUnwrap};
use crate::db::api::properties::internal::{Key, PropertiesOps};
use crate::prelude::Graph;
use chrono::NaiveDateTime;
use std::iter::Zip;

pub struct TemporalPropertyView<P: PropertiesOps> {
    pub(crate) id: Key,
    pub(crate) props: P,
}

impl<P: PropertiesOps> TemporalPropertyView<P> {
    pub(crate) fn new(props: P, key: Key) -> Self {
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
    pub fn latest(&self) -> Option<Prop> {
        self.props.temporal_value(&self.id)
    }
}

impl<P: PropertiesOps> IntoIterator for TemporalPropertyView<P> {
    type Item = (i64, Prop);
    type IntoIter = Zip<std::vec::IntoIter<i64>, std::vec::IntoIter<Prop>>;

    fn into_iter(self) -> Self::IntoIter {
        let hist = self.history();
        let vals = self.values();
        hist.into_iter().zip(vals)
    }
}

impl<P: PropertiesOps> IntoIterator for &TemporalPropertyView<P> {
    type Item = (i64, Prop);
    type IntoIter = Zip<std::vec::IntoIter<i64>, std::vec::IntoIter<Prop>>;

    fn into_iter(self) -> Self::IntoIter {
        let hist = self.history();
        let vals = self.values();
        hist.into_iter().zip(vals)
    }
}

pub struct TemporalProperties<P: PropertiesOps + Clone> {
    pub(crate) props: P,
}

impl<P: PropertiesOps + Clone> IntoIterator for TemporalProperties<P> {
    type Item = (String, TemporalPropertyView<P>);
    type IntoIter = Zip<std::vec::IntoIter<String>, std::vec::IntoIter<TemporalPropertyView<P>>>;

    fn into_iter(self) -> Self::IntoIter {
        let keys = self.keys();
        let values = self.values();
        keys.into_iter().zip(values)
    }
}

impl<P: PropertiesOps + Clone> IntoIterator for &TemporalProperties<P> {
    type Item = (String, TemporalPropertyView<P>);
    type IntoIter = Zip<std::vec::IntoIter<String>, std::vec::IntoIter<TemporalPropertyView<P>>>;

    fn into_iter(self) -> Self::IntoIter {
        let keys = self.keys();
        let values = self.values();
        keys.into_iter().zip(values)
    }
}

impl<P: PropertiesOps + Clone> TemporalProperties<P> {
    pub(crate) fn new(props: P) -> Self {
        Self { props }
    }
    pub fn keys(&self) -> Vec<String> {
        self.props
            .temporal_property_keys()
            .map(|v| v.clone())
            .collect()
    }

    pub fn contains<Q: AsRef<str>>(&self, key: Q) -> bool {
        self.get(key).is_some()
    }

    pub fn values(&self) -> Vec<TemporalPropertyView<P>> {
        self.props
            .temporal_property_values()
            .map(|k| TemporalPropertyView::new(self.props.clone(), k))
            .collect()
    }

    pub fn iter_latest(&self) -> impl Iterator<Item = (String, Prop)> + '_ {
        self.iter().flat_map(|(k, v)| v.latest().map(|v| (k, v)))
    }

    pub fn iter(&self) -> impl Iterator<Item = (String, TemporalPropertyView<P>)> + '_ {
        self.into_iter()
    }

    pub fn get<Q: AsRef<str>>(&self, key: Q) -> Option<TemporalPropertyView<P>> {
        self.props
            .get_temporal_property(key.as_ref())
            .map(|k| TemporalPropertyView::new(self.props.clone(), k))
    }

    pub fn collect_properties(self) -> Vec<(String, Prop)> {
        self.iter()
            .flat_map(|(k, v)| v.latest().map(|v| (k, v)))
            .collect()
    }
}

impl<P: PropertiesOps> PropUnwrap for TemporalPropertyView<P> {
    fn into_str(self) -> Option<String> {
        self.latest().into_str()
    }

    fn into_i32(self) -> Option<i32> {
        self.latest().into_i32()
    }

    fn into_i64(self) -> Option<i64> {
        self.latest().into_i64()
    }

    fn into_u32(self) -> Option<u32> {
        self.latest().into_u32()
    }

    fn into_u64(self) -> Option<u64> {
        self.latest().into_u64()
    }

    fn into_f32(self) -> Option<f32> {
        self.latest().into_f32()
    }

    fn into_f64(self) -> Option<f64> {
        self.latest().into_f64()
    }

    fn into_bool(self) -> Option<bool> {
        self.latest().into_bool()
    }

    fn into_dtime(self) -> Option<NaiveDateTime> {
        self.latest().into_dtime()
    }

    fn into_graph(self) -> Option<Graph> {
        self.latest().into_graph()
    }
}
