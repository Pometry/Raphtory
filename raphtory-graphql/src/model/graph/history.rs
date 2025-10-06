use crate::{
    model::graph::timeindex::{dt_format_str_is_valid, GqlEventTime},
    rayon::blocking_compute,
};
use async_graphql::Error;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::db::api::view::history::{
    History, HistoryDateTime, HistoryEventId, HistoryTimestamp, InternalHistoryOps, Intervals,
};
use raphtory_api::core::storage::timeindex::TimeError;
use std::{any::Any, sync::Arc};

/// History of updates for an object in Raphtory.
/// Provides access to temporal properties.
#[derive(ResolvedObject, Clone)]
#[graphql(name = "History")]
pub struct GqlHistory {
    pub(crate) history: History<'static, Arc<dyn InternalHistoryOps>>,
}

/// Creates GqlHistory from History<T> object, note that this consumes the History<T> object.
impl<T: InternalHistoryOps + 'static> From<History<'_, T>> for GqlHistory {
    fn from(history: History<T>) -> Self {
        let arc_ops: Arc<dyn InternalHistoryOps> = {
            // Check if T is already Arc<dyn InternalHistoryOps>
            let any_ref: &dyn Any = &history.0;
            if let Some(arc_obj) = any_ref.downcast_ref::<Arc<dyn InternalHistoryOps>>() {
                Arc::clone(arc_obj)
            } else {
                Arc::new(history.0)
            }
        };
        Self {
            history: History::new(arc_ops),
        }
    }
}

#[ResolvedObjectFields]
impl GqlHistory {
    /// Get the earliest time entry associated with this history or None if the history is empty.
    async fn earliest_time(&self) -> Option<GqlEventTime> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.history.earliest_time().map(|t| t.into())).await
    }

    /// Get the latest time entry associated with this history or None if the history is empty.
    async fn latest_time(&self) -> Option<GqlEventTime> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.history.latest_time().map(|t| t.into())).await
    }

    /// List all time entries present in this history.
    async fn list(&self) -> Vec<GqlEventTime> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.history.iter().map(|t| t.into()).collect()).await
    }

    /// List all time entries present in this history in reverse order.
    async fn list_rev(&self) -> Vec<GqlEventTime> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.history.iter_rev().map(|t| t.into()).collect()).await
    }

    /// Fetch one page of EventTime entries with a number of items up to a specified limit,
    /// optionally offset by a specified amount. The page_index sets the number of pages to skip (defaults to 0).
    ///
    /// For example, if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
    /// will be returned.
    async fn page(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Vec<GqlEventTime> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let start = page_index.unwrap_or(0) * limit + offset.unwrap_or(0);
            self_clone
                .history
                .iter()
                .skip(start)
                .take(limit)
                .map(|t| t.into())
                .collect()
        })
        .await
    }

    /// Fetch one page of EventTime entries with a number of items up to a specified limit,
    /// optionally offset by a specified amount. The page_index sets the number of pages to skip (defaults to 0).
    ///
    /// For example, if page_rev(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
    /// will be returned.
    async fn page_rev(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Vec<GqlEventTime> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let start = page_index.unwrap_or(0) * limit + offset.unwrap_or(0);
            self_clone
                .history
                .iter_rev()
                .skip(start)
                .take(limit)
                .map(|t| t.into())
                .collect()
        })
        .await
    }

    /// Returns True if the history is empty.
    async fn is_empty(&self) -> bool {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.history.is_empty()).await
    }

    /// Get the number of entries contained in the history.
    async fn count(&self) -> u64 {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.history.len() as u64).await
    }

    /// Returns a HistoryTimestamp object which accesses timestamps as Unix epochs in milliseconds
    /// instead of EventTime entries.
    async fn timestamps(&self) -> GqlHistoryTimestamp {
        let self_clone = self.clone();
        blocking_compute(move || GqlHistoryTimestamp {
            history_t: HistoryTimestamp::new(self_clone.history.0.clone()), // clone the Arc, not the underlying object
        })
        .await
    }

    /// Returns a HistoryDateTime object which accesses datetimes instead of EventTime entries.
    /// Useful for converting millisecond timestamps into easily readable datetime strings.
    /// Optionally, a format string can be passed to format the output. Defaults to RFC 3339 if not provided (e.g., "2023-12-25T10:30:45.123Z").
    /// Refer to chrono::format::strftime for formatting specifiers and escape sequences.
    async fn datetimes(&self, format_string: Option<String>) -> GqlHistoryDateTime {
        let self_clone = self.clone();
        blocking_compute(move || GqlHistoryDateTime {
            history_dt: HistoryDateTime::new(self_clone.history.0.clone()), // clone the Arc, not the underlying object
            format_string,
        })
        .await
    }

    /// Returns a HistoryEventId object which accesses event ids of EventTime entries.
    /// They are used for ordering within the same timestamp.
    async fn event_id(&self) -> GqlHistoryEventId {
        let self_clone = self.clone();
        blocking_compute(move || GqlHistoryEventId {
            history_s: HistoryEventId::new(self_clone.history.0.clone()), // clone the Arc, not the underlying object
        })
        .await
    }

    /// Returns an Intervals object which calculates the intervals between consecutive EventTime timestamps.
    async fn intervals(&self) -> GqlIntervals {
        let self_clone = self.clone();
        blocking_compute(move || GqlIntervals {
            intervals: Intervals::new(self_clone.history.0.clone()), // clone the Arc, not the underlying object
        })
        .await
    }
}

/// History object that provides access to timestamps (as Unix epochs in milliseconds) instead of `EventTime` entries.
#[derive(ResolvedObject, Clone)]
#[graphql(name = "HistoryTimestamp")]
pub struct GqlHistoryTimestamp {
    history_t: HistoryTimestamp<Arc<dyn InternalHistoryOps>>,
}

#[ResolvedObjectFields]
impl GqlHistoryTimestamp {
    /// List all timestamps.
    async fn list(&self) -> Vec<i64> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.history_t.collect()).await
    }

    /// List all timestamps in reverse order.
    async fn list_rev(&self) -> Vec<i64> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.history_t.collect_rev()).await
    }

    /// Fetch one page of timestamps with a number of items up to a specified limit, optionally offset by a specified amount.
    /// The page_index sets the number of pages to skip (defaults to 0).
    ///
    /// For example, if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
    /// will be returned.
    async fn page(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Vec<i64> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let start = page_index.unwrap_or(0) * limit + offset.unwrap_or(0);
            self_clone
                .history_t
                .iter()
                .skip(start)
                .take(limit)
                .collect()
        })
        .await
    }

    /// Fetch one page of timestamps in reverse order with a number of items up to a specified limit,
    /// optionally offset by a specified amount. The page_index sets the number of pages to skip (defaults to 0).
    ///
    /// For example, if page_rev(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
    /// will be returned.
    async fn page_rev(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Vec<i64> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let start = page_index.unwrap_or(0) * limit + offset.unwrap_or(0);
            self_clone
                .history_t
                .iter_rev()
                .skip(start)
                .take(limit)
                .collect()
        })
        .await
    }
}

/// History object that provides access to datetimes instead of `EventTime` entries.
#[derive(ResolvedObject, Clone)]
#[graphql(name = "HistoryDateTime")]
pub struct GqlHistoryDateTime {
    history_dt: HistoryDateTime<Arc<dyn InternalHistoryOps>>,
    format_string: Option<String>,
}

#[ResolvedObjectFields]
impl GqlHistoryDateTime {
    /// List all datetimes formatted as strings.
    /// If filter_broken is set to True, time conversion errors will be ignored. If set to False, a TimeError
    /// will be raised on time conversion error. Defaults to False.
    async fn list(&self, filter_broken: Option<bool>) -> Result<Vec<String>, Error> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let fmt_string = self_clone.format_string.as_deref().unwrap_or("%+"); // %+ is RFC 3339
            if !dt_format_str_is_valid(fmt_string) {
                return Err(Error::new(format!(
                    "Invalid datetime format string: '{}'",
                    fmt_string
                )));
            }
            self_clone
                .history_dt
                .iter()
                .filter_map(|t| match t {
                    Ok(dt) => Some(Ok(dt.format(fmt_string).to_string())),
                    Err(e) => {
                        if filter_broken.unwrap_or(false) {
                            None
                        } else {
                            Some(Err(Error::new(e.to_string())))
                        }
                    }
                })
                .collect()
        })
        .await
    }

    /// List all datetimes formatted as strings in reverse chronological order.
    /// If filter_broken is set to True, time conversion errors will be ignored. If set to False, a TimeError
    /// will be raised on time conversion error. Defaults to False.
    async fn list_rev(&self, filter_broken: Option<bool>) -> Result<Vec<String>, Error> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let fmt_string = self_clone.format_string.as_deref().unwrap_or("%+"); // %+ is RFC 3339
            if !dt_format_str_is_valid(fmt_string) {
                return Err(Error::new(format!(
                    "Invalid datetime format string: '{}'",
                    fmt_string
                )));
            }
            self_clone
                .history_dt
                .iter_rev()
                .filter_map(|t| match t {
                    Ok(dt) => Some(Ok(dt.format(fmt_string).to_string())),
                    Err(e) => {
                        if filter_broken.unwrap_or(false) {
                            None
                        } else {
                            Some(Err(Error::new(e.to_string())))
                        }
                    }
                })
                .collect()
        })
        .await
    }

    /// Fetch one page of datetimes formatted as string with a number of items up to a specified limit,
    /// optionally offset by a specified amount. The page_index sets the number of pages to skip (defaults to 0).
    /// If filter_broken is set to True, time conversion errors will be ignored. If set to False, a TimeError
    /// will be raised on time conversion error. Defaults to False.
    ///
    /// For example, if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
    /// will be returned.
    async fn page(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
        filter_broken: Option<bool>,
    ) -> Result<Vec<String>, Error> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let start = page_index.unwrap_or(0) * limit + offset.unwrap_or(0);
            let fmt_string = self_clone.format_string.as_deref().unwrap_or("%+"); // %+ is RFC 3339
            if !dt_format_str_is_valid(fmt_string) {
                return Err(Error::new(format!(
                    "Invalid datetime format string: '{}'",
                    fmt_string
                )));
            }
            self_clone
                .history_dt
                .iter()
                .filter_map(|t| match t {
                    // filter_map first to make sure we take the right number of items
                    Ok(dt) => Some(Ok(dt.format(fmt_string).to_string())),
                    Err(e) => {
                        if filter_broken.unwrap_or(false) {
                            None
                        } else {
                            Some(Err(Error::new(e.to_string())))
                        }
                    }
                })
                .skip(start)
                .take(limit)
                .collect()
        })
        .await
    }

    /// Fetch one page of datetimes formatted as string in reverse chronological order with a number of items up to a specified limit,
    /// optionally offset by a specified amount. The page_index sets the number of pages to skip (defaults to 0).
    /// If filter_broken is set to True, time conversion errors will be ignored. If set to False, a TimeError
    /// will be raised on time conversion error. Defaults to False.
    ///
    /// For example, if page_rev(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
    /// will be returned.
    async fn page_rev(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
        filter_broken: Option<bool>,
    ) -> Result<Vec<String>, Error> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let start = page_index.unwrap_or(0) * limit + offset.unwrap_or(0);
            let fmt_string = self_clone.format_string.as_deref().unwrap_or("%+"); // %+ is RFC 3339
            if !dt_format_str_is_valid(fmt_string) {
                return Err(Error::new(format!(
                    "Invalid datetime format string: '{}'",
                    fmt_string
                )));
            }
            self_clone
                .history_dt
                .iter_rev()
                .filter_map(|t| match t {
                    // filter_map first to make sure we take the right number of items
                    Ok(dt) => Some(Ok(dt.format(fmt_string).to_string())),
                    Err(e) => {
                        if filter_broken.unwrap_or(false) {
                            None
                        } else {
                            Some(Err(Error::new(e.to_string())))
                        }
                    }
                })
                .skip(start)
                .take(limit)
                .collect()
        })
        .await
    }
}

/// History object that provides access to event ids instead of `EventTime` entries.
#[derive(ResolvedObject, Clone)]
#[graphql(name = "HistoryEventId")]
pub struct GqlHistoryEventId {
    history_s: HistoryEventId<Arc<dyn InternalHistoryOps>>,
}

#[ResolvedObjectFields]
impl GqlHistoryEventId {
    /// List event ids.
    async fn list(&self) -> Vec<u64> {
        let self_clone = self.clone();
        blocking_compute(move || {
            self_clone
                .history_s
                .iter()
                .map(|s: usize| s as u64)
                .collect()
        })
        .await
    }

    /// List event ids in reverse order.
    async fn list_rev(&self) -> Vec<u64> {
        let self_clone = self.clone();
        blocking_compute(move || {
            self_clone
                .history_s
                .iter_rev()
                .map(|s: usize| s as u64)
                .collect()
        })
        .await
    }

    /// Fetch one page of event ids with a number of items up to a specified limit,
    /// optionally offset by a specified amount. The page_index sets the number of pages to skip (defaults to 0).
    ///
    /// For example, if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
    /// will be returned.
    async fn page(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Vec<u64> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let start = page_index.unwrap_or(0) * limit + offset.unwrap_or(0);
            self_clone
                .history_s
                .iter()
                .skip(start)
                .take(limit)
                .map(|s: usize| s as u64)
                .collect()
        })
        .await
    }

    /// Fetch one page of event ids in reverse chronological order with a number of items up to a specified limit,
    /// optionally offset by a specified amount. The page_index sets the number of pages to skip (defaults to 0).
    ///
    /// For example, if page_rev(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
    /// will be returned.
    async fn page_rev(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Vec<u64> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let start = page_index.unwrap_or(0) * limit + offset.unwrap_or(0);
            self_clone
                .history_s
                .iter_rev()
                .skip(start)
                .take(limit)
                .map(|s: usize| s as u64)
                .collect()
        })
        .await
    }
}

/// Provides access to the intervals between temporal entries of an object.
#[derive(ResolvedObject, Clone)]
#[graphql(name = "Intervals")]
pub struct GqlIntervals {
    pub(crate) intervals: Intervals<Arc<dyn InternalHistoryOps>>,
}

#[ResolvedObjectFields]
impl GqlIntervals {
    /// List time intervals between consecutive timestamps in milliseconds.
    async fn list(&self) -> Vec<i64> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.intervals.collect()).await
    }

    /// List millisecond time intervals between consecutive timestamps in reverse order.
    async fn list_rev(&self) -> Vec<i64> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.intervals.collect_rev()).await
    }

    /// Fetch one page of intervals between consecutive timestamps with a number of items up to a specified limit,
    /// optionally offset by a specified amount. The page_index sets the number of pages to skip (defaults to 0).
    ///
    /// For example, if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
    /// will be returned.
    async fn page(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Vec<i64> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let start = page_index.unwrap_or(0) * limit + offset.unwrap_or(0);
            self_clone
                .intervals
                .iter()
                .skip(start)
                .take(limit)
                .collect()
        })
        .await
    }

    /// Fetch one page of intervals between consecutive timestamps in reverse order with a number of items up to a specified limit,
    /// optionally offset by a specified amount. The page_index sets the number of pages to skip (defaults to 0).
    ///
    /// For example, if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
    /// will be returned.
    async fn page_rev(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Vec<i64> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let start = page_index.unwrap_or(0) * limit + offset.unwrap_or(0);
            self_clone
                .intervals
                .iter_rev()
                .skip(start)
                .take(limit)
                .collect()
        })
        .await
    }

    /// Compute the mean interval between consecutive timestamps. Returns None if fewer than 1 timestamp.
    async fn mean(&self) -> Option<f64> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.intervals.mean()).await
    }

    /// Compute the median interval between consecutive timestamps. Returns None if fewer than 1 timestamp.
    async fn median(&self) -> Option<i64> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.intervals.median()).await
    }

    /// Compute the maximum interval between consecutive timestamps. Returns None if fewer than 1 timestamp.
    async fn max(&self) -> Option<i64> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.intervals.max()).await
    }

    /// Compute the minimum interval between consecutive timestamps. Returns None if fewer than 1 timestamp.
    async fn min(&self) -> Option<i64> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.intervals.min()).await
    }
}
