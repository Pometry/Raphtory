use crate::db::api::{properties::internal::InternalPropertiesOps, view::history::History};
use arrow::array::ArrayRef;
use bigdecimal::BigDecimal;
<<<<<<< fix_list_types
use chrono::{DateTime, NaiveDateTime, Utc};
=======
use chrono::NaiveDateTime;
>>>>>>> db_v4
use raphtory_api::{
    core::{
        entities::properties::prop::{Prop, PropArray, PropArrayUnwrap, PropType, PropUnwrap},
        storage::{
            arc_str::ArcStr,
            timeindex::{AsTime, EventTime},
        },
        utils::time::IntoTime,
    },
    iter::BoxedLIter,
};
use rustc_hash::FxHashMap;
use std::{
    collections::{HashMap, HashSet},
    fmt::{Debug, Formatter},
    iter::Zip,
    sync::Arc,
};

#[derive(Clone)]
pub struct TemporalPropertyView<P: InternalPropertiesOps> {
    pub(crate) id: usize,
    pub(crate) props: P,
}

impl<P: InternalPropertiesOps + Clone> Debug for TemporalPropertyView<P> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TemporalPropertyView")
            .field("history", &self.iter().collect::<Vec<_>>())
            .finish()
    }
}

impl<P: InternalPropertiesOps + Clone> PartialEq for TemporalPropertyView<P> {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}

impl<P: InternalPropertiesOps + Clone, RHS, V> PartialEq<RHS> for TemporalPropertyView<P>
where
    for<'a> &'a RHS: IntoIterator<Item = &'a (EventTime, V)>,
    V: Clone + Into<Prop>,
{
    fn eq(&self, other: &RHS) -> bool {
        self.iter()
            .eq(other.into_iter().map(|(t, v)| (*t, v.clone().into())))
    }
}

impl<P: InternalPropertiesOps + Clone> TemporalPropertyView<P> {
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

    pub fn history(&self) -> History<'_, Self> {
        History::new(self.clone())
    }

    pub fn values(&self) -> BoxedLIter<'_, Prop> {
        self.props.temporal_values_iter(self.id)
    }

    pub fn values_rev(&self) -> BoxedLIter<'_, Prop> {
        self.props.temporal_values_iter_rev(self.id)
    }

    pub fn iter(&self) -> impl Iterator<Item = (EventTime, Prop)> + '_ {
        self.history().into_iter().zip(self.values())
    }

    pub fn first(&self) -> Option<Prop> {
        self.props.temporal_values_iter(self.id).next()
    }

    pub fn iter_rev(&self) -> impl Iterator<Item = (EventTime, Prop)> + '_ {
        self.history().into_iter_rev().zip(self.values_rev())
    }

    pub fn iter_indexed(&self) -> impl Iterator<Item = (EventTime, Prop)> + use<'_, P> {
        self.props.temporal_iter(self.id)
    }

    pub fn iter_indexed_rev(&self) -> impl Iterator<Item = (EventTime, Prop)> + use<'_, P> {
        self.props.temporal_iter_rev(self.id)
    }

    pub fn at<T: IntoTime>(&self, t: T) -> Option<Prop> {
        let t = EventTime::end(t.into_time().t());
        self.props.temporal_value_at(self.id, t)
    }
    pub fn latest(&self) -> Option<Prop> {
        self.props.temporal_value(self.id)
    }

    pub fn unique(&self) -> Vec<Prop> {
        let unique_props: HashSet<_> = self.values().collect();
        unique_props.into_iter().collect()
    }

    /// Compute the sum of all property values, or `None` if the dtype is not additive or the
    /// property is empty.
    pub fn sum(&self) -> Option<Prop> {
        generalised_reduce(self.values(), |a, b| a.add(b), |p| p.dtype().has_add())
    }

    /// Find the minimum `(time, value)` pair, or `None` if the dtype is not comparable or the
    /// property is empty.
    pub fn min(&self) -> Option<(EventTime, Prop)> {
        generalised_reduce(
            self.iter(),
            |a, b| {
                if a.1.partial_cmp(&b.1)?.is_le() {
                    Some(a)
                } else {
                    Some(b)
                }
            },
            |(_, v)| v.dtype().has_cmp(),
        )
    }

    /// Find the maximum `(time, value)` pair, or `None` if the dtype is not comparable or the
    /// property is empty.
    pub fn max(&self) -> Option<(EventTime, Prop)> {
        generalised_reduce(
            self.iter(),
            |a, b| {
                if a.1.partial_cmp(&b.1)?.is_ge() {
                    Some(a)
                } else {
                    Some(b)
                }
            },
            |(_, v)| v.dtype().has_cmp(),
        )
    }

    /// Count the number of property updates.
    pub fn count(&self) -> usize {
        self.iter().count()
    }

    /// Compute the mean of all property values as an `F64` Prop, or `None` if any value cannot be
    /// converted to `f64` or the property is empty.
    pub fn mean(&self) -> Option<Prop> {
        let mut iter = self.values();
        let mut sum = iter.next()?.as_f64()?;
        let mut count = 1usize;
        for value in iter {
            sum += value.as_f64()?;
            count += 1;
        }
        Some(Prop::F64(sum / count as f64))
    }

    /// Alias for `mean`.
    pub fn average(&self) -> Option<Prop> {
        self.mean()
    }

    /// Compute the median `(time, value)` pair (lower median on even-length inputs), or `None` if
    /// the dtype is not comparable or the property is empty.
    pub fn median(&self) -> Option<(EventTime, Prop)> {
        let mut sorted: Vec<(EventTime, Prop)> = self.iter().collect();
        if !sorted.first()?.1.dtype().has_cmp() {
            return None;
        }
        sorted.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        let len = sorted.len();
        Some(sorted.swap_remove((len - 1) / 2))
    }

    pub fn ordered_dedupe(&self, latest_time: bool) -> Vec<(EventTime, Prop)> {
        let mut last_seen_value: Option<Prop> = None;
        let mut result: Vec<(EventTime, Prop)> = vec![];

        let mut current_entry: Option<(EventTime, Prop)> = None;

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

impl<P: InternalPropertiesOps + Clone> IntoIterator for TemporalPropertyView<P> {
    type Item = (EventTime, Prop);
    type IntoIter = Zip<std::vec::IntoIter<EventTime>, std::vec::IntoIter<Prop>>;

    fn into_iter(self) -> Self::IntoIter {
        let hist = self.history().iter().collect::<Vec<_>>();
        let vals = self.values().collect::<Vec<_>>();
        hist.into_iter().zip(vals)
    }
}

impl<P: InternalPropertiesOps + Clone> IntoIterator for &TemporalPropertyView<P> {
    type Item = (EventTime, Prop);
    type IntoIter = Zip<std::vec::IntoIter<EventTime>, std::vec::IntoIter<Prop>>;

    fn into_iter(self) -> Self::IntoIter {
        let hist = self.history().iter().collect::<Vec<_>>();
        let vals = self.values().collect::<Vec<_>>();
        hist.into_iter().zip(vals)
    }
}
#[derive(Clone)]
pub struct TemporalProperties<P: InternalPropertiesOps + Clone> {
    pub(crate) props: P,
}

impl<P: InternalPropertiesOps + Clone> IntoIterator for TemporalProperties<P> {
    type Item = (ArcStr, TemporalPropertyView<P>);
    type IntoIter = Zip<std::vec::IntoIter<ArcStr>, std::vec::IntoIter<TemporalPropertyView<P>>>;

    fn into_iter(self) -> Self::IntoIter {
        let keys: Vec<_> = self.keys().collect();
        let values: Vec<_> = self.values().collect();
        keys.into_iter().zip(values)
    }
}

impl<P: InternalPropertiesOps + Clone> TemporalProperties<P> {
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

    pub fn iter_ids(&self) -> impl Iterator<Item = (usize, TemporalPropertyView<P>)> + '_ {
        self.props.temporal_prop_ids().zip(self.values())
    }

    pub fn iter_filtered(&self) -> impl Iterator<Item = (ArcStr, TemporalPropertyView<P>)> + '_ {
        self.iter().filter(|(_, v)| !v.is_empty())
    }

    pub fn get(&self, key: &str) -> Option<TemporalPropertyView<P>> {
        self.props
            .get_temporal_prop_id(key)
            .map(|k| TemporalPropertyView::new(self.props.clone(), k))
    }

    pub fn get_by_id(&self, id: usize) -> Option<TemporalPropertyView<P>> {
        Some(TemporalPropertyView::new(self.props.clone(), id))
    }

    pub fn histories(&self) -> Vec<(ArcStr, (EventTime, Prop))> {
        self.iter()
            .flat_map(|(k, v)| v.into_iter().map(move |v| (k.clone(), v.clone())))
            .collect()
    }

    pub fn collect_properties(self) -> Vec<(ArcStr, Prop)> {
        self.iter()
            .flat_map(|(k, v)| v.latest().map(|v| (k.clone(), v)))
            .collect()
    }

    pub fn as_map(&self) -> HashMap<ArcStr, Vec<(EventTime, Prop)>> {
        self.iter()
            .map(|(key, value)| (key, value.iter().collect()))
            .collect()
    }
}

impl<P: InternalPropertiesOps + Clone> PropUnwrap for TemporalPropertyView<P> {
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

    fn into_list(self) -> Option<PropArray> {
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

    fn into_dtime(self) -> Option<DateTime<Utc>> {
        self.latest().into_dtime()
    }
}

impl<P: InternalPropertiesOps + Clone> PropArrayUnwrap for TemporalPropertyView<P> {
    fn into_array(self) -> Option<ArrayRef> {
        self.latest().into_array()
    }
}

fn generalised_reduce<V>(
    data: impl IntoIterator<Item = V>,
    op: impl Fn(V, V) -> Option<V>,
    check: impl Fn(&V) -> bool,
) -> Option<V> {
    let mut iter = data.into_iter();
    let first = iter.next()?;
    if !check(&first) {
        return None;
    }
    iter.try_fold(first, op)
}
