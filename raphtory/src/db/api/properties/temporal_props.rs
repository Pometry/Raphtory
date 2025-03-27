use crate::{
    core::{Prop, PropType, PropUnwrap},
    db::api::{properties::internal::PropertiesOps, view::BoxedLIter},
};
use arrow_array::ArrayRef;
use chrono::{DateTime, NaiveDateTime, Utc};
use raphtory_api::core::storage::arc_str::ArcStr;
use rustc_hash::FxHashMap;
use std::{
    collections::{HashMap, HashSet},
    iter::Zip,
    sync::Arc,
};

#[derive(Clone)]
pub struct TemporalPropertyView<P: PropertiesOps> {
    pub(crate) id: usize,
    pub(crate) props: P,
}

impl<P: PropertiesOps> TemporalPropertyView<P> {
    pub(crate) fn new(props: P, key: usize) -> Self {
        TemporalPropertyView { props, id: key }
    }

    pub fn is_empty(&self) -> bool {
        self.iter().next().is_none()
    }

    pub fn dtype(&self) -> PropType {
        self.props.dtype(self.id)
    }
    pub fn history(&self) -> BoxedLIter<i64> {
        self.props.temporal_history_iter(self.id)
    }

    pub fn history_rev(&self) -> BoxedLIter<i64> {
        self.props.temporal_history_iter_rev(self.id)
    }

    pub fn history_date_time(&self) -> Option<Vec<DateTime<Utc>>> {
        self.props.temporal_history_date_time(self.id)
    }
    pub fn values(&self) -> BoxedLIter<Prop> {
        self.props.temporal_values_iter(self.id)
    }

    pub fn iter(&self) -> impl Iterator<Item = (i64, Prop)> + '_ {
        self.history().zip(self.values())
    }

    pub fn histories(&self) -> impl Iterator<Item = (i64, Prop)> + '_ {
        self.iter()
    }

    pub fn histories_date_time(&self) -> Option<impl Iterator<Item = (DateTime<Utc>, Prop)>> {
        let hist = self.history_date_time()?;
        let vals = self.values().collect::<Vec<_>>();
        Some(hist.into_iter().zip(vals))
    }

    pub fn at(&self, t: i64) -> Option<Prop> {
        self.props.temporal_value_at(self.id, t)
    }
    pub fn latest(&self) -> Option<Prop> {
        self.props.temporal_value(self.id)
    }

    pub fn unique(&self) -> Vec<Prop> {
        let unique_props: HashSet<_> = self.values().into_iter().collect();
        unique_props.into_iter().collect()
    }

    pub fn ordered_dedupe(&self, latest_time: bool) -> Vec<(i64, Prop)> {
        let mut last_seen_value: Option<Prop> = None;
        let mut result: Vec<(i64, Prop)> = vec![];

        let mut current_entry: Option<(i64, Prop)> = None;

        for (t, prop) in self {
            if latest_time {
                if last_seen_value == Some(prop.clone()) {
                    current_entry = Some((t, prop.clone()));
                } else {
                    if let Some(entry) = current_entry.take() {
                        result.push(entry);
                    }
                    current_entry = Some((t, prop.clone()));
                }
                last_seen_value = Some(prop.clone());
            } else if last_seen_value != Some(prop.clone()) {
                result.push((t, prop.clone()));
                last_seen_value = Some(prop.clone());
            }
        }

        if let Some(entry) = current_entry {
            result.push(entry);
        }

        result
    }
}

impl<P: PropertiesOps> IntoIterator for TemporalPropertyView<P> {
    type Item = (i64, Prop);
    type IntoIter = Zip<std::vec::IntoIter<i64>, std::vec::IntoIter<Prop>>;

    fn into_iter(self) -> Self::IntoIter {
        let hist = self.history().collect::<Vec<_>>();
        let vals = self.values().collect::<Vec<_>>();
        hist.into_iter().zip(vals)
    }
}

impl<P: PropertiesOps> IntoIterator for &TemporalPropertyView<P> {
    type Item = (i64, Prop);
    type IntoIter = Zip<std::vec::IntoIter<i64>, std::vec::IntoIter<Prop>>;

    fn into_iter(self) -> Self::IntoIter {
        let hist = self.history().collect::<Vec<_>>();
        let vals = self.values().collect::<Vec<_>>();
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

    pub fn get_by_id(&self, id: usize) -> Option<TemporalPropertyView<P>> {
        Some(TemporalPropertyView::new(self.props.clone(), id))
    }

    pub fn histories(&self) -> Vec<(ArcStr, (i64, Prop))> {
        self.iter()
            .flat_map(|(k, v)| v.into_iter().map(move |v| (k.clone(), v.clone())))
            .collect()
    }

    pub fn collect_properties(self) -> Vec<(ArcStr, Prop)> {
        self.iter()
            .flat_map(|(k, v)| v.latest().map(|v| (k.clone(), v)))
            .collect()
    }

    pub fn as_map(&self) -> HashMap<ArcStr, Vec<(i64, Prop)>> {
        self.iter()
            .map(|(key, value)| (key, value.histories().collect()))
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

    fn into_map(self) -> Option<Arc<FxHashMap<ArcStr, Prop>>> {
        self.latest().into_map()
    }

    fn into_ndtime(self) -> Option<NaiveDateTime> {
        self.latest().into_ndtime()
    }

    fn into_array(self) -> Option<ArrayRef> {
        self.latest().into_array()
    }

    fn as_f64(&self) -> Option<f64> {
        self.latest().as_f64()
    }
}
