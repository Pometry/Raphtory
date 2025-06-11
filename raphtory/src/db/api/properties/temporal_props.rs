use crate::core::utils::errors::GraphError;
use crate::{
    core::{Prop, PropType, PropUnwrap},
    db::api::{
        properties::internal::PropertiesOps,
        view::{
            history::{History, InternalHistoryOps},
            BoxedLIter,
        },
    },
};
use arrow_array::ArrayRef;
use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDateTime, Utc};
use raphtory_api::core::{
    entities::properties::prop::{Prop, PropType, PropUnwrap},
    storage::{arc_str::ArcStr, timeindex::{TimeIndexEntry, AsTime}},
};
use rustc_hash::FxHashMap;
use std::{
    collections::{HashMap, HashSet},
    fmt::{Debug, Formatter},
    iter::Zip,
    sync::Arc,
};

#[cfg(feature = "arrow")]
use {arrow_array::ArrayRef, raphtory_api::core::entities::properties::prop::PropArrayUnwrap};

#[derive(Clone)]
pub struct TemporalPropertyView<P: PropertiesOps + Clone> {
    pub(crate) id: usize,
    pub(crate) props: P,
}

impl<P: PropertiesOps + Clone> Debug for TemporalPropertyView<P> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TemporalPropertyView")
            .field("history", &self.iter().collect::<Vec<_>>())
            .finish()
    }
}

impl<P: PropertiesOps + Clone> PartialEq for TemporalPropertyView<P> {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}

impl<P: PropertiesOps + Clone, RHS, V> PartialEq<RHS> for TemporalPropertyView<P>
where
    for<'a> &'a RHS: IntoIterator<Item = &'a (i64, V)>,
    V: Clone + Into<Prop>,
{
    fn eq(&self, other: &RHS) -> bool {
        self.iter()
            .eq(other.into_iter().map(|(t, v)| (*t, v.clone().into())))
    }
}

impl<P: PropertiesOps + Clone> TemporalPropertyView<P> {
    pub(crate) fn new(props: P, key: usize) -> Self {
        TemporalPropertyView { props, id: key }
    }

    pub fn is_empty(&self) -> bool {
        self.iter().next().is_none()
    }

    pub fn dtype(&self) -> PropType {
        self.props.dtype(self.id)
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn history(&self) -> History<TemporalPropertyView<P>> {
        // TODO: Change this to history object?
        History::new((*self).clone())
    }


    pub fn history_rev(&self) -> BoxedLIter<i64> {
        self.props.temporal_history_iter_rev(self.id)
    }

    pub fn history_date_time(&self) -> Result<Vec<DateTime<Utc>>, GraphError> {
        self.props.temporal_history_date_time(self.id)
    }
    pub fn values(&self) -> BoxedLIter<Prop> {
        self.props.temporal_values_iter(self.id)
    }

    pub fn iter(&self) -> impl Iterator<Item = (i64, Prop)> + '_ {
        InternalHistoryOps::iter(self)
            .map(|t| t.t())
            .zip(self.values())
    }

    pub fn iter_indexed(&self) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + use<'_, P> {
        self.props.temporal_iter(self.id)
    }

    pub fn histories(&self) -> impl Iterator<Item = (i64, Prop)> + '_ {
        self.iter()
    } // TODO: What about histories?

    pub fn histories_date_time(
        &self,
    ) -> Result<impl Iterator<Item = (DateTime<Utc>, Prop)>, GraphError> {
        let hist = self.history_date_time()?;
        let vals = self.values().collect::<Vec<_>>();
        Ok(hist.into_iter().zip(vals))
    }

    pub fn at(&self, t: i64) -> Option<Prop> {
        self.props.temporal_value_at(self.id, t)
    }
    pub fn latest(&self) -> Option<Prop> {
        self.props.temporal_value(self.id)
    }

    pub fn unique(&self) -> Vec<Prop> {
        let unique_props: HashSet<_> = self.values().collect();
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

impl<P: PropertiesOps + Clone> IntoIterator for TemporalPropertyView<P> {
    type Item = (i64, Prop);
    type IntoIter = Zip<std::vec::IntoIter<i64>, std::vec::IntoIter<Prop>>;

    fn into_iter(self) -> Self::IntoIter {
        let hist = self.history().iter().map(|t| t.t()).collect::<Vec<_>>();
        let vals = self.values().collect::<Vec<_>>();
        hist.into_iter().zip(vals)
    }
}

impl<P: PropertiesOps + Clone> IntoIterator for &TemporalPropertyView<P> {
    type Item = (i64, Prop);
    type IntoIter = Zip<std::vec::IntoIter<i64>, std::vec::IntoIter<Prop>>;

    fn into_iter(self) -> Self::IntoIter {
        let hist = self.history().iter().map(|t| t.t()).collect::<Vec<_>>();
        let vals = self.values().collect::<Vec<_>>();
        hist.into_iter().zip(vals)
    }
}
#[derive(Clone)]
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

impl<P: PropertiesOps + Clone> PropUnwrap for TemporalPropertyView<P> {
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

    fn as_f64(&self) -> Option<f64> {
        self.latest().as_f64()
    }

    fn into_decimal(self) -> Option<BigDecimal> {
        self.latest().into_decimal()
    }
}

#[cfg(feature = "arrow")]
impl<P: PropertiesOps> PropArrayUnwrap for TemporalPropertyView<P> {
    fn into_array(self) -> Option<ArrayRef> {
        self.latest().into_array()
    }
}
