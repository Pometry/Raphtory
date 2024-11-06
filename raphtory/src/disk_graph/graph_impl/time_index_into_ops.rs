use crate::{
    core::storage::timeindex::{TimeIndexIntoOps, TimeIndexOps},
    db::api::view::IntoDynBoxed,
};
use pometry_storage::{
    prelude::{ArrayOps, BaseArrayOps},
    timestamps::TimeStamps,
};
use raphtory_api::core::storage::timeindex::TimeIndexEntry;
use std::ops::Range;

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
    fn into_iter(self) -> impl Iterator<Item = TimeIndexEntry> + Send + 'static {
        let (timestamps, sec_index) = self.into_inner();
        let sec_iter: Box<dyn Iterator<Item = usize> + Send> = sec_index
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

    fn iter(&self) -> Box<dyn Iterator<Item = Self::IndexType> + Send + 'a> {
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

    fn iter(&self) -> Box<dyn Iterator<Item = i64> + Send + '_> {
        Box::new(self.timestamps().into_iter())
    }
}
