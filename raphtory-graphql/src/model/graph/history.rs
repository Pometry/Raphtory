use crate::{model::graph::timeindex::GqlTimeIndexEntry, rayon::blocking_compute};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::db::api::view::history::{
    History, HistoryDateTime, HistorySecondaryIndex, HistoryTimestamp, InternalHistoryOps,
    Intervals,
};
use raphtory_api::core::storage::timeindex::TimeError;
use std::{any::Any, sync::Arc};

/// Represents the history of updates for an object in Raphtory.
/// It provides access to the temporal properties of the object.
#[derive(ResolvedObject, Clone)]
#[graphql(name = "History")]
pub struct GqlHistory {
    pub(crate) history: History<'static, Arc<dyn InternalHistoryOps>>,
}

/// Creates GqlHistory from History<T> object, note that this consumes the History<T> object
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
    /// The earliest time entry (as a `TimeIndexEntry`) associated with this history.
    /// Returns `null` if the history is empty.
    async fn earliest_time(&self) -> Option<GqlTimeIndexEntry> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.history.earliest_time().map(|t| t.into())).await
    }

    /// The latest time entry (as a `TimeIndexEntry`) associated with this history.
    /// Returns `null` if the history is empty.
    async fn latest_time(&self) -> Option<GqlTimeIndexEntry> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.history.latest_time().map(|t| t.into())).await
    }

    /// A list of all time entries (as `TimeIndexEntry` items) present in this history.
    async fn list(&self) -> Vec<GqlTimeIndexEntry> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.history.iter().map(|t| t.into()).collect()).await
    }

    /// A list of all time entries (as `TimeIndexEntry` items) present in this history, in reverse order.
    async fn list_rev(&self) -> Vec<GqlTimeIndexEntry> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.history.iter_rev().map(|t| t.into()).collect()).await
    }

    /// Fetch one "page" of `TimeIndexEntry` entries, optionally offset by a specified amount.
    ///
    /// * `limit` - The size of the page (number of items to fetch).
    /// * `offset` - The number of items to skip (defaults to 0).
    /// * `page_index` - The number of pages (of size `limit`) to skip (defaults to 0).
    ///
    /// e.g. if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
    /// will be returned.
    async fn page(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Vec<GqlTimeIndexEntry> {
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

    /// Fetch one "page" of `TimeIndexEntry` items in reverse chronological order, optionally offset by a specified amount.
    ///
    /// * `limit` - The size of the page (number of items to fetch).
    /// * `offset` - The number of items to skip (defaults to 0).
    /// * `page_index` - The number of pages (of size `limit`) to skip (defaults to 0).
    ///
    /// e.g. if page_rev(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
    /// will be returned.
    async fn page_rev(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Vec<GqlTimeIndexEntry> {
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

    /// Returns true if the History object is empty
    async fn is_empty(&self) -> bool {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.history.is_empty()).await
    }

    /// Returns the number of entries contained in the History object
    async fn count(&self) -> u64 {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.history.len() as u64).await
    }

    /// A HistoryTimestamp object which provides access to timestamps (as Unix epochs in milliseconds) instead of `TimeIndexEntry` entries
    async fn timestamps(&self) -> GqlHistoryTimestamp {
        let self_clone = self.clone();
        blocking_compute(move || GqlHistoryTimestamp {
            history_t: HistoryTimestamp::new(self_clone.history.0.clone()), // clone the Arc, not the underlying object
        })
        .await
    }

    /// A HistoryDateTime object which provides access to datetimes instead of `TimeIndexEntry` entries.
    /// Uses RFC 3339 format by default (e.g., "2023-12-25T10:30:45.123Z").
    /// Optionally takes a format string as argument to format the output of datetimes.
    /// Refer to chrono::format::strftime for formatting specifiers and escape sequences.
    async fn datetimes(&self, format_string: Option<String>) -> GqlHistoryDateTime {
        let self_clone = self.clone();
        blocking_compute(move || GqlHistoryDateTime {
            history_dt: HistoryDateTime::new(self_clone.history.0.clone()), // clone the Arc, not the underlying object
            format_string,
        })
        .await
    }

    /// A HistorySecondaryIndex object which provides access to the secondary indices of `TimeIndexEntry` entries
    async fn secondary_index(&self) -> GqlHistorySecondaryIndex {
        let self_clone = self.clone();
        blocking_compute(move || GqlHistorySecondaryIndex {
            history_s: HistorySecondaryIndex::new(self_clone.history.0.clone()), // clone the Arc, not the underlying object
        })
        .await
    }

    /// Returns an Intervals object that provides access to the intervals between temporal entries
    async fn intervals(&self) -> GqlIntervals {
        let self_clone = self.clone();
        blocking_compute(move || GqlIntervals {
            intervals: Intervals::new(self_clone.history.0.clone()), // clone the Arc, not the underlying object
        })
        .await
    }
}

/// History object that provides access to timestamps (as Unix epochs in milliseconds) instead of `TimeIndexEntry` entries
#[derive(ResolvedObject, Clone)]
#[graphql(name = "HistoryTimestamp")]
pub struct GqlHistoryTimestamp {
    history_t: HistoryTimestamp<Arc<dyn InternalHistoryOps>>,
}

#[ResolvedObjectFields]
impl GqlHistoryTimestamp {
    /// A list of all timestamps (as Unix epochs in milliseconds) present in this history,
    async fn list(&self) -> Vec<i64> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.history_t.collect()).await
    }

    /// A list of all timestamps (as Unix epochs in milliseconds) present in this history, in reverse order.
    async fn list_rev(&self) -> Vec<i64> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.history_t.collect_rev()).await
    }

    /// Fetch one "page" of history timestamps (as Unix epochs in milliseconds), optionally offset by a specified amount.
    ///
    /// * `limit` - The size of the page (number of items to fetch).
    /// * `offset` - The number of items to skip (defaults to 0).
    /// * `page_index` - The number of pages (of size `limit`) to skip (defaults to 0).
    ///
    /// e.g. if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
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

    /// Fetch one "page" of history timestamps (as Unix epochs in milliseconds) in reverse chronological order, optionally offset by a specified amount.
    ///
    /// * `limit` - The size of the page (number of items to fetch).
    /// * `offset` - The number of items to skip (defaults to 0).
    /// * `page_index` - The number of pages (of size `limit`) to skip (defaults to 0).
    ///
    /// e.g. if page_rev(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
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

/// History object that provides access to datetimes instead of `TimeIndexEntry` entries
#[derive(ResolvedObject, Clone)]
#[graphql(name = "HistoryDateTime")]
pub struct GqlHistoryDateTime {
    history_dt: HistoryDateTime<Arc<dyn InternalHistoryOps>>,
    format_string: Option<String>,
}

#[ResolvedObjectFields]
impl GqlHistoryDateTime {
    /// A list of all datetimes present in this history, formatted as strings.
    /// Uses RFC 3339 format by default (e.g., "2023-12-25T10:30:45.123Z").
    /// Throws an error if a time conversion fails
    /// filter_broken continues on time conversion errors, filtering out the errors. Defaults to `false`
    async fn list(&self, filter_broken: Option<bool>) -> Result<Vec<String>, TimeError> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let fmt_string = self_clone.format_string.as_deref().unwrap_or("%+"); // %+ is RFC 3339
            self_clone
                .history_dt
                .iter()
                .filter_map(|t| match t {
                    Ok(dt) => Some(Ok(dt.format(fmt_string).to_string())),
                    Err(e) => {
                        if filter_broken.unwrap_or(false) {
                            None
                        } else {
                            Some(Err(e))
                        }
                    }
                })
                .collect()
        })
        .await
    }

    /// A list of all datetimes present in this history in reverse order, formatted as strings.
    /// Uses RFC 3339 format by default (e.g., "2023-12-25T10:30:45.123Z").
    /// Throws an error if a time conversion fails
    /// filter_broken continues on time conversion errors, filtering out the errors. Defaults to `false`
    async fn list_rev(&self, filter_broken: Option<bool>) -> Result<Vec<String>, TimeError> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let fmt_string = self_clone.format_string.as_deref().unwrap_or("%+"); // %+ is RFC 3339
            self_clone
                .history_dt
                .iter_rev()
                .filter_map(|t| match t {
                    Ok(dt) => Some(Ok(dt.format(fmt_string).to_string())),
                    Err(e) => {
                        if filter_broken.unwrap_or(false) {
                            None
                        } else {
                            Some(Err(e))
                        }
                    }
                })
                .collect()
        })
        .await
    }

    /// Fetch one "page" of history datetimes formatted as strings, optionally offset by a specified amount.
    ///
    /// * `limit` - The size of the page (number of items to fetch).
    /// * `offset` - The number of items to skip (defaults to 0).
    /// * `page_index` - The number of pages (of size `limit`) to skip (defaults to 0).
    /// * `filter_broken` - continues on time conversion errors, filtering out the errors (defaults to `false`).
    ///
    /// e.g. if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
    /// will be returned.
    async fn page(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
        filter_broken: Option<bool>,
    ) -> Result<Vec<String>, TimeError> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let start = page_index.unwrap_or(0) * limit + offset.unwrap_or(0);
            let fmt_string = self_clone.format_string.as_deref().unwrap_or("%+"); // %+ is RFC 3339
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
                            Some(Err(e))
                        }
                    }
                })
                .skip(start)
                .take(limit)
                .collect()
        })
        .await
    }

    /// Fetch one "page" of history datetimes formatted as strings in reverse chronological order, optionally offset by a specified amount.
    ///
    /// * `limit` - The size of the page (number of items to fetch).
    /// * `offset` - The number of items to skip (defaults to 0).
    /// * `page_index` - The number of pages (of size `limit`) to skip (defaults to 0).
    /// * `filter_broken` - continues on time conversion errors, filtering out the errors (defaults to `false`)
    ///
    /// e.g. if page_rev(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
    /// will be returned.
    async fn page_rev(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
        filter_broken: Option<bool>,
    ) -> Result<Vec<String>, TimeError> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let start = page_index.unwrap_or(0) * limit + offset.unwrap_or(0);
            let fmt_string = self_clone.format_string.as_deref().unwrap_or("%+"); // %+ is RFC 3339
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
                            Some(Err(e))
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

/// History object that provides access to secondary indices instead of `TimeIndexEntry` entries
#[derive(ResolvedObject, Clone)]
#[graphql(name = "HistorySecondaryIndex")]
pub struct GqlHistorySecondaryIndex {
    history_s: HistorySecondaryIndex<Arc<dyn InternalHistoryOps>>,
}

#[ResolvedObjectFields]
impl GqlHistorySecondaryIndex {
    /// A list of secondary indices from `TimeIndexEntry` entries
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

    /// A list of secondary indices from `TimeIndexEntry` entries in reverse order
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

    /// Fetch one "page" of history items, optionally offset by a specified amount.
    ///
    /// * `limit` - The size of the page (number of items to fetch).
    /// * `offset` - The number of items to skip (defaults to 0).
    /// * `page_index` - The number of pages (of size `limit`) to skip (defaults to 0).
    ///
    /// e.g. if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
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

    /// Fetch one "page" of history items in reverse chronological order, optionally offset by a specified amount.
    ///
    /// * `limit` - The size of the page (number of items to fetch).
    /// * `offset` - The number of items to skip (defaults to 0).
    /// * `page_index` - The number of pages (of size `limit`) to skip (defaults to 0).
    ///
    /// e.g. if page_rev(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
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
    /// Returns a list of time intervals between consecutive timestamps
    async fn list(&self) -> Vec<i64> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.intervals.collect()).await
    }

    /// Returns a list of time intervals between consecutive timestamps in reverse
    async fn list_rev(&self) -> Vec<i64> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.intervals.collect_rev()).await
    }

    /// Fetch one "page" of history items, optionally offset by a specified amount.
    ///
    /// * `limit` - The size of the page (number of items to fetch).
    /// * `offset` - The number of items to skip (defaults to 0).
    /// * `page_index` - The number of pages (of size `limit`) to skip (defaults to 0).
    ///
    /// e.g. if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
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

    /// Fetch one "page" of history items in reverse chronological order, optionally offset by a specified amount.
    ///
    /// * `limit` - The size of the page (number of items to fetch).
    /// * `offset` - The number of items to skip (defaults to 0).
    /// * `page_index` - The number of pages (of size `limit`) to skip (defaults to 0).
    ///
    /// e.g. if page_rev(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
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

    /// The mean (average) interval between consecutive timestamps.
    /// Returns `null` if there are fewer than 2 timestamps.
    async fn mean(&self) -> Option<f64> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.intervals.mean()).await
    }

    /// The median interval between consecutive timestamps.
    /// Returns `null` if there are fewer than 2 timestamps.
    async fn median(&self) -> Option<f64> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.intervals.median()).await
    }

    /// The maximum interval between consecutive timestamps.
    /// Returns `null` if there are fewer than 2 timestamps.
    async fn max(&self) -> Option<i64> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.intervals.max()).await
    }

    /// The minimum interval between consecutive timestamps.
    /// Returns `null` if there are fewer than 2 timestamps.
    async fn min(&self) -> Option<i64> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.intervals.min()).await
    }
}
