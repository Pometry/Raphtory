use crate::{
    core::{ArcStr, DocumentInput, Prop, PropUnwrap},
    db::api::properties::internal::PropertiesOps,
    prelude::Graph,
};
use chrono::{DateTime, NaiveDateTime, Utc};
use std::{collections::HashMap, iter::Zip, sync::Arc};

pub struct TemporalPropertyView<P: PropertiesOps> {
    pub(crate) id: usize,
    pub(crate) props: P,
}

impl<P: PropertiesOps> TemporalPropertyView<P> {
    pub(crate) fn new(props: P, key: usize) -> Self {
        TemporalPropertyView { props, id: key }
    }
    pub fn history(&self) -> Vec<i64> {
        self.props.temporal_history(self.id)
    }
    pub fn history_date_time(&self) -> Option<Vec<DateTime<Utc>>> {
        self.props.temporal_history_date_time(self.id)
    }
    pub fn values(&self) -> Vec<Prop> {
        self.props.temporal_values(self.id)
    }
    pub fn iter(&self) -> impl Iterator<Item = (i64, Prop)> {
        self.into_iter()
    }

    pub fn histories(&self) -> impl Iterator<Item = (i64, Prop)> {
        self.iter()
    }

    pub fn histories_date_time(&self) -> Option<impl Iterator<Item = (DateTime<Utc>, Prop)>> {
        let hist = self.history_date_time()?;
        let vals = self.values();
        Some(hist.into_iter().zip(vals))
    }

    pub fn at(&self, t: i64) -> Option<Prop> {
        self.props.temporal_value_at(self.id, t)
    }
    pub fn latest(&self) -> Option<Prop> {
        self.props.temporal_value(self.id)
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
    type Item = (ArcStr, TemporalPropertyView<P>);
    type IntoIter = Zip<std::vec::IntoIter<ArcStr>, std::vec::IntoIter<TemporalPropertyView<P>>>;

    fn into_iter(self) -> Self::IntoIter {
        let keys: Vec<_> = self.keys().collect();
        let values: Vec<_> = self.values().collect();
        keys.into_iter().zip(values)
    }
}

impl<P: PropertiesOps + Clone> TemporalProperties<P> {
    pub(crate) fn new(props: P) -> Self {
        Self { props }
    }
    pub fn keys(&self) -> impl Iterator<Item = ArcStr> + '_ {
        self.props.temporal_prop_keys()
    }

    pub fn contains(&self, key: &str) -> bool {
        self.props.get_temporal_prop_id(key).is_some()
    }

    pub fn values(&self) -> impl Iterator<Item = TemporalPropertyView<P>> + '_ {
        self.props
            .temporal_prop_ids()
            .map(|k| TemporalPropertyView::new(self.props.clone(), k))
    }

    pub fn iter_latest(&self) -> impl Iterator<Item = (ArcStr, Prop)> + '_ {
        self.iter().flat_map(|(k, v)| v.latest().map(|v| (k, v)))
    }

    pub fn iter(&self) -> impl Iterator<Item = (ArcStr, TemporalPropertyView<P>)> + '_ {
        self.keys().zip(self.values())
    }

    pub fn get(&self, key: &str) -> Option<TemporalPropertyView<P>> {
        self.props
            .get_temporal_prop_id(key)
            .map(|k| TemporalPropertyView::new(self.props.clone(), k))
    }

    pub fn histories(&self) -> Vec<(ArcStr, (i64, Prop))> {
        self.iter()
            .flat_map(|(k, v)| v.histories().map(move |v| (k.clone(), v.clone())))
            .collect()
    }

    pub fn collect_properties(self) -> Vec<(ArcStr, Prop)> {
        self.iter()
            .flat_map(|(k, v)| v.latest().map(|v| (k.clone(), v)))
            .collect()
    }
}

impl<P: PropertiesOps> PropUnwrap for TemporalPropertyView<P> {
    fn into_u8(self) -> Option<u8> {
        self.latest().into_u8()
    }

    fn into_u16(self) -> Option<u16> {
        self.latest().into_u16()
    }

    fn into_str(self) -> Option<ArcStr> {
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

    fn into_list(self) -> Option<Arc<Vec<Prop>>> {
        self.latest().into_list()
    }

    fn into_map(self) -> Option<Arc<HashMap<ArcStr, Prop>>> {
        self.latest().into_map()
    }

    fn into_ndtime(self) -> Option<NaiveDateTime> {
        self.latest().into_ndtime()
    }

    fn into_graph(self) -> Option<Graph> {
        self.latest().into_graph()
    }

    fn into_document(self) -> Option<DocumentInput> {
        self.latest().into_document()
    }
}
