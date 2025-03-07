use raphtory_api::{
    core::{
        entities::{LayerIds, VID},
        storage::timeindex::{AsTime, TimeIndexEntry, TimeIndexIntoOps, TimeIndexOps},
    },
    iter::IntoDynBoxed,
};
use rayon::prelude::*;
use std::fmt::{Debug, Formatter};

use super::chunked_array::chunked_array::{ChunkedArray, NonNull};
use crate::{
    arrow2::array::PrimitiveArray,
    chunked_array::{
        array_ops::{ArrayOps, BaseArrayOps},
        slice::DoubleEndedExactSizeIterator,
        ChunkedArraySlice,
    },
    graph::TemporalGraph,
    Time,
};
use raphtory_api::iter::BoxedLIter;
use std::ops::Range;

#[derive(Copy, Clone)]
pub struct TimeStamps<'a, T> {
    pub(crate) timestamps: ChunkedArraySlice<'a, &'a ChunkedArray<PrimitiveArray<i64>, NonNull>>,
    pub(crate) sec_index:
        Option<ChunkedArraySlice<'a, &'a ChunkedArray<PrimitiveArray<u64>, NonNull>>>,
    _t: std::marker::PhantomData<T>,
}

impl<'a, T: Debug> Debug for TimeStamps<'a, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TimeStamps")
            .field("timestamps", &self.timestamps)
            .field("sec_index", &self.sec_index)
            .finish()
    }
}

impl<'a, T> TimeStamps<'a, T> {
    pub fn len(&self) -> usize {
        self.timestamps.len()
    }

    pub fn timestamps(
        self,
    ) -> ChunkedArraySlice<'a, &'a ChunkedArray<PrimitiveArray<i64>, NonNull>> {
        self.timestamps
    }

    pub fn sec_index(
        self,
    ) -> Option<ChunkedArraySlice<'a, &'a ChunkedArray<PrimitiveArray<u64>, NonNull>>> {
        self.sec_index
    }

    pub fn into_inner(
        self,
    ) -> (
        ChunkedArraySlice<'a, &'a ChunkedArray<PrimitiveArray<i64>, NonNull>>,
        Option<ChunkedArraySlice<'a, &'a ChunkedArray<PrimitiveArray<u64>, NonNull>>>,
    ) {
        (self.timestamps, self.sec_index)
    }
}

impl<'a, T: AsTime> TimeStamps<'a, T> {
    pub fn times(self) -> TimeStamps<'a, TimeIndexEntry> {
        TimeStamps::new(self.timestamps, None)
    }

    pub fn get(&self, index: usize) -> T {
        let t = self.timestamps.get(index);
        let s = self
            .sec_index
            .as_ref()
            .map(|s| s.get(index) as usize)
            .unwrap_or(self.timestamps.range().start + index);
        T::new(t, s)
    }
}

impl<'a, T: AsTime> TimeStamps<'a, T> {
    pub fn new(
        timestamps: ChunkedArraySlice<'a, &'a ChunkedArray<PrimitiveArray<i64>, NonNull>>,
        sec_index: Option<ChunkedArraySlice<'a, &'a ChunkedArray<PrimitiveArray<u64>, NonNull>>>,
    ) -> Self {
        Self {
            timestamps,
            sec_index,
            _t: std::marker::PhantomData,
        }
    }

    pub fn position(&self, ti: &T) -> usize {
        if self.sec_index.is_none() && self.timestamps.range().contains(&ti.i()) {
            // Direct row-number lookup if it is a match
            let t_index = ti.i() - self.timestamps.range().start;
            if self.timestamps.get(t_index) == ti.t() {
                return t_index;
            }
        }
        let t_index = self.timestamps.partition_point(|t| t < ti.t());
        if t_index >= self.timestamps.len() {
            return t_index;
        }
        let t = self.timestamps.get(t_index);
        if t > ti.t() {
            return t_index;
        }
        match &self.sec_index {
            Some(sec_index) => self
                .timestamps
                .slice(t_index..)
                .iter()
                .zip(sec_index.slice(t_index..))
                .map(|(t, sec)| T::new(t, sec as usize))
                .position(|te| te >= *ti)
                .map(|offset| t_index + offset)
                .unwrap_or_else(|| self.timestamps.len()),
            None => self
                .timestamps
                .slice(t_index..)
                .iter()
                .zip(self.timestamps.range().clone().skip(t_index))
                .map(|(t, sec)| T::new(t, sec))
                .position(|te| te >= *ti)
                .map(|offset| t_index + offset)
                .unwrap_or_else(|| self.timestamps.len()),
        }
    }

    pub fn partition_points(&self, r: Range<Time>) -> (usize, usize) {
        let start = self.timestamps.partition_point(|t| t < r.start);
        let end = self.timestamps.partition_point(|t| t < r.end);

        (start, end)
    }

    pub fn find(&self, t: Time) -> Result<usize, usize> {
        self.timestamps.binary_search_by(|t2| t.cmp(&t2))
    }

    pub fn last_before(&self, t: TimeIndexEntry) -> Option<(T, usize)> {
        let t_index = self.timestamps.partition_point(|ts| ts < t.t());
        if t_index == 0 {
            return None;
        }
        let t = self.timestamps.get(t_index - 1);
        let sec = self
            .sec_index
            .as_ref()
            .map(|arr| arr.get(t_index - 1) as usize)
            .unwrap_or(self.timestamps.range().start + t_index - 1);

        Some((T::new(t, sec), t_index - 1))
    }

    pub fn into_iter_t(self) -> impl Iterator<Item = Time> + 'a {
        self.timestamps.into_iter()
    }

    pub fn into_iter(self) -> impl DoubleEndedExactSizeIterator<Item = T> + Send + Sync + 'a {
        let sec_iter: Box<dyn DoubleEndedExactSizeIterator<Item = u64> + Send + Sync + 'a> =
            match self.sec_index() {
                Some(sec) => Box::new(sec.into_iter()),
                None => Box::new(self.timestamps().range().map(|x| x as u64)),
            };
        self.timestamps()
            .into_iter()
            .zip(sec_iter)
            .map(move |(t, s)| T::new(t, s as usize))
    }

    pub fn slice(&'a self, r: Range<usize>) -> Self {
        let TimeStamps {
            timestamps,
            sec_index,
            ..
        } = self;
        Self {
            timestamps: (*timestamps).sliced(r.clone()),
            sec_index: sec_index.as_ref().map(|sec_index| (*sec_index).sliced(r)),
            _t: std::marker::PhantomData,
        }
    }

    pub fn sliced(self, r: Range<usize>) -> Self {
        let TimeStamps {
            timestamps,
            sec_index,
            ..
        } = self;
        Self {
            timestamps: timestamps.sliced(r.clone()),
            sec_index: sec_index.map(|sec_index| sec_index.sliced(r)),
            _t: std::marker::PhantomData,
        }
    }
}

impl<'a> TimeIndexIntoOps for TimeStamps<'a, TimeIndexEntry> {
    type IndexType = TimeIndexEntry;

    type RangeType = Self;

    fn into_range(self, w: Range<TimeIndexEntry>) -> Self {
        let start = self.position(&w.start);
        let end = self.position(&w.end);
        let (timestamps, sec_index) = self.into_inner();
        TimeStamps::new(
            timestamps.sliced(start..end),
            sec_index.map(|sec_index| sec_index.sliced(start..end)),
        )
    }

    #[allow(refining_impl_trait)]
    fn into_iter(self) -> impl Iterator<Item = TimeIndexEntry> + Send + Sync + 'static {
        let (timestamps, sec_index) = self.into_inner();
        let sec_iter = sec_index
            .map(|v| v.into_owned().map(|i| i as usize).into_dyn_boxed())
            .unwrap_or(self.timestamps().range().clone().into_dyn_boxed());
        timestamps
            .into_owned()
            .zip(sec_iter)
            .map(|(t, s)| TimeIndexEntry(t, s))
    }
}

impl<'a> TimeIndexIntoOps for TimeStamps<'a, i64> {
    type IndexType = i64;

    type RangeType = Self;

    fn into_range(self, w: Range<i64>) -> Self {
        let start = self.timestamps().partition_point(|i| i < w.start);
        let end = self.timestamps().partition_point(|i| i < w.end);
        let (timestamps, _) = self.into_inner();
        TimeStamps::new(timestamps.sliced(start..end), None)
    }
    fn into_iter(self) -> impl Iterator<Item = i64> + Send {
        let (timestamps, _) = self.into_inner();
        timestamps
    }
}

impl<'a> TimeIndexOps for TimeStamps<'a, TimeIndexEntry> {
    type IndexType = TimeIndexEntry;
    type RangeType<'b>
        = TimeStamps<'b, TimeIndexEntry>
    where
        Self: 'b;

    fn len(&self) -> usize {
        self.timestamps().len()
    }

    fn active(&self, w: Range<TimeIndexEntry>) -> bool {
        let i = self.position(&w.start);
        i < self.timestamps().len() && self.get(i) < w.end
    }

    fn range(&self, w: Range<TimeIndexEntry>) -> Self::RangeType<'_> {
        let start = self.position(&w.start);
        let end = self.position(&w.end);
        TimeStamps::new(
            self.timestamps().sliced(start..end),
            self.sec_index()
                .map(|sec_index| sec_index.sliced(start..end)),
        )
    }

    fn first_t(&self) -> Option<i64> {
        (self.timestamps().len() > 0).then(|| self.timestamps().get(0))
    }

    fn first(&self) -> Option<Self::IndexType> {
        if self.timestamps().len() == 0 {
            return None;
        }
        let t = self.timestamps().get(0);
        let sec = self.sec_index().as_ref().map(|arr| arr.get(0)).unwrap_or(0);

        Some(TimeIndexEntry::new(t, sec as usize))
    }

    fn last_t(&self) -> Option<i64> {
        (self.timestamps().len() > 0).then(|| self.timestamps().get(self.timestamps().len() - 1))
    }

    fn last(&self) -> Option<Self::IndexType> {
        if self.timestamps().len() == 0 {
            return None;
        }
        let last_idx = self.timestamps().len() - 1;

        let t = self.timestamps().get(last_idx);
        let sec = self
            .sec_index()
            .as_ref()
            .map(|arr| arr.get(last_idx))
            .unwrap_or(0);

        Some(TimeIndexEntry::new(t, sec as usize))
    }

    fn iter(&self) -> Box<dyn Iterator<Item = Self::IndexType> + Send + Sync + 'a> {
        let sec_iter = self
            .sec_index()
            .map(|v| v.map(|i| i as usize).into_dyn_boxed())
            .unwrap_or(self.timestamps().range().clone().into_dyn_boxed());
        Box::new(
            self.timestamps()
                .into_iter()
                .zip(sec_iter)
                .map(|(t, s)| TimeIndexEntry(t, s)),
        )
    }
}
impl<'a> TimeIndexOps for TimeStamps<'a, i64> {
    type IndexType = i64;
    type RangeType<'b>
        = TimeStamps<'b, i64>
    where
        Self: 'b;

    fn len(&self) -> usize {
        self.timestamps().len()
    }
    fn active(&self, w: Range<i64>) -> bool {
        let i = self.timestamps().insertion_point(w.start);
        i < self.timestamps().len() && self.timestamps().get(i) < w.end
    }

    fn range(&self, w: Range<i64>) -> Self::RangeType<'_> {
        let start = self.timestamps().partition_point(|i| i < w.start);
        let end = self.timestamps().partition_point(|i| i < w.end);
        TimeStamps::new(
            self.timestamps().sliced(start..end),
            self.sec_index()
                .map(|sec_index| sec_index.sliced(start..end)),
        )
    }

    fn first(&self) -> Option<Self::IndexType> {
        if self.timestamps().len() == 0 {
            return None;
        }
        let t = self.timestamps().get(0);
        Some(t)
    }

    fn last(&self) -> Option<Self::IndexType> {
        if self.timestamps().len() == 0 {
            return None;
        }

        let last_idx = self.timestamps().len() - 1;

        let t = self.timestamps().get(last_idx);
        Some(t)
    }

    fn iter(&self) -> BoxedLIter<i64> {
        Box::new(self.timestamps().into_iter())
    }
}

#[derive(Clone)]
pub struct LayerAdditions<'a> {
    graph: &'a TemporalGraph,
    id: usize,
    range: Option<Range<TimeIndexEntry>>,
    layer_ids: LayerIds,
}

impl<'a> LayerAdditions<'a> {
    pub fn new(
        graph: &'a TemporalGraph,
        id: VID,
        layer_ids: LayerIds,
        range: Option<Range<TimeIndexEntry>>,
    ) -> Self {
        LayerAdditions {
            graph,
            id: id.0,
            layer_ids,
            range,
        }
    }

    pub fn prop_events(self) -> impl Iterator<Item = TimeStamps<'a, TimeIndexEntry>> {
        self.graph
            .layers()
            .into_iter()
            .enumerate()
            .filter_map(move |(id, l)| self.layer_ids.contains(&id).then_some(l))
            .map(move |l| TimeStamps::new(l.nodes_storage().additions().value(self.id), None))
            .map(move |t| {
                self.range
                    .as_ref()
                    .map(move |w| t.into_range(w.clone()))
                    .unwrap_or(t)
            })
    }

    pub fn edge_events(self) -> impl Iterator<Item = TimeStamps<'a, TimeIndexEntry>> {
        self.graph
            .node_properties()
            .temporal_props()
            .into_iter()
            .map(move |t_props| t_props.timestamps::<TimeIndexEntry>(VID(self.id)))
            .map(move |t| {
                self.range
                    .as_ref()
                    .map(move |w| t.into_range(w.clone()))
                    .unwrap_or(t)
            })
    }

    pub fn iter(&self) -> impl Iterator<Item = TimeStamps<'a, TimeIndexEntry>> + '_ {
        self.graph
            .layers()
            .into_iter()
            .enumerate()
            .filter_map(move |(id, l)| self.layer_ids.contains(&id).then_some(l))
            .map(move |l| TimeStamps::new(l.nodes_storage().additions().value(self.id), None))
            .chain(
                self.graph
                    .node_properties()
                    .temporal_props()
                    .into_iter()
                    .map(|t_props| t_props.timestamps::<TimeIndexEntry>(VID(self.id))),
            )
            .map(|t| {
                self.range
                    .as_ref()
                    .map(move |w| t.into_range(w.clone()))
                    .unwrap_or(t)
            })
    }

    pub fn into_iter(self) -> impl Iterator<Item = TimeStamps<'a, TimeIndexEntry>> {
        self.graph
            .layers()
            .into_iter()
            .enumerate()
            .filter_map(move |(id, l)| self.layer_ids.contains(&id).then_some(l))
            .map(move |l| TimeStamps::new(l.nodes_storage().additions().value(self.id), None))
            .chain(
                self.graph
                    .node_properties()
                    .temporal_props()
                    .into_iter()
                    .map(move |t_props| t_props.timestamps::<TimeIndexEntry>(VID(self.id))),
            )
            .map(move |t| {
                self.range
                    .as_ref()
                    .map(move |w| t.into_range(w.clone()))
                    .unwrap_or(t)
            })
    }

    pub fn par_iter(&self) -> impl ParallelIterator<Item = TimeStamps<'a, TimeIndexEntry>> + '_ {
        self.graph
            .layers()
            .into_par_iter()
            .enumerate()
            .filter_map(move |(id, l)| self.layer_ids.contains(&id).then_some(l))
            .map(move |l| TimeStamps::new(l.nodes_storage().additions().value(self.id), None))
            .chain(
                self.graph
                    .node_properties()
                    .temporal_props()
                    .into_par_iter()
                    .map(|t_props| t_props.timestamps::<TimeIndexEntry>(VID(self.id))),
            )
            .map(|t| {
                self.range
                    .as_ref()
                    .map(move |w| t.into_range(w.clone()))
                    .unwrap_or(t)
            })
    }

    pub fn with_range(&self, w: Range<TimeIndexEntry>) -> Self {
        let start = self
            .range
            .as_ref()
            .map(|range| range.start.max(w.start))
            .unwrap_or(w.start);
        let end = self
            .range
            .as_ref()
            .map(|range| range.start.min(w.start))
            .unwrap_or(w.end);
        LayerAdditions {
            graph: self.graph,
            id: self.id,
            layer_ids: self.layer_ids.clone(),
            range: Some(start..end),
        }
    }

    pub fn len(&self) -> usize {
        self.iter().map(|l| l.len()).sum()
    }
}
