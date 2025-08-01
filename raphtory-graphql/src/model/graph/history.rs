use crate::model::graph::timeindex::GqlTimeIndexEntry;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::db::api::view::history::{
    History, HistoryDateTime, HistorySecondaryIndex, HistoryTimestamp, InternalHistoryOps,
    Intervals,
};
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
        self.history.earliest_time().map(|t| t.into())
    }

    /// The latest time entry (as a `TimeIndexEntry`) associated with this history.
    /// Returns `null` if the history is empty.
    async fn latest_time(&self) -> Option<GqlTimeIndexEntry> {
        self.history.latest_time().map(|t| t.into())
    }

    /// A list of all time entries (as `TimeIndexEntry` items) present in this history,
    async fn collect(&self) -> Vec<GqlTimeIndexEntry> {
        self.history.iter().map(|t| t.into()).collect()
    }

    /// A list of all time entries (as `TimeIndexEntry` items) present in this history, in reverse order.
    async fn collect_reversed(&self) -> Vec<GqlTimeIndexEntry> {
        self.history.iter_rev().map(|t| t.into()).collect()
    }

    /// Returns true if the History object is empty
    async fn is_empty(&self) -> bool {
        self.history.is_empty()
    }

    /// Returns the number of entries contained in the History object
    async fn len(&self) -> u64 {
        self.history.len() as u64
    }

    /// A HistoryTimestamp object which provides access to timestamps (as Unix epochs in milliseconds) instead of `TimeIndexEntry` entries
    async fn timestamps(&self) -> GqlHistoryTimestamp {
        GqlHistoryTimestamp {
            history_t: HistoryTimestamp::new(self.history.0.clone()), // clone the Arc, not the underlying object
        }
    }

    /// A HistoryDateTime object which provides access to datetimes instead of `TimeIndexEntry` entries.
    /// Uses RFC 3339 format by default (e.g., "2023-12-25T10:30:45.123Z").
    /// Optionally takes a format string as argument to format the output of datetimes.
    /// Refer to chrono::format::strftime for formatting specifiers and escape sequences.
    async fn datetimes(&self, format_string: Option<String>) -> GqlHistoryDateTime {
        GqlHistoryDateTime {
            history_dt: HistoryDateTime::new(self.history.0.clone()), // clone the Arc, not the underlying object
            format_string,
        }
    }

    /// A HistorySecondaryIndex object which provides access to the secondary indices of `TimeIndexEntry` entries
    async fn secondary_index(&self) -> GqlHistorySecondaryIndex {
        GqlHistorySecondaryIndex {
            history_s: HistorySecondaryIndex::new(self.history.0.clone()), // clone the Arc, not the underlying object
        }
    }

    /// Returns an Intervals object that provides access to the intervals between temporal entries
    async fn intervals(&self) -> GqlIntervals {
        GqlIntervals {
            intervals: Intervals::new(self.history.0.clone()), // clone the Arc, not the underlying object
        }
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
    async fn collect(&self) -> Vec<i64> {
        self.history_t.collect()
    }

    /// A list of all timestamps (as Unix epochs in milliseconds) present in this history, in reverse order.
    async fn collect_reversed(&self) -> Vec<i64> {
        self.history_t.collect_rev()
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
    /// If any timestamp cannot be converted to a valid datetime, it will be replaced with an error message string.
    async fn collect(&self) -> Vec<String> {
        let fmt_string = self.format_string.as_deref().unwrap_or("%+"); // %+ is RFC 3339
        self.history_dt
            .iter()
            .map(|t| match t {
                Ok(dt) => dt.format(fmt_string).to_string(),
                Err(e) => format!("Time conversion error: {}", e.to_string()),
            })
            .collect()
    }

    /// A list of all datetimes present in this history in reverse order, formatted as strings.
    /// Uses RFC 3339 format by default (e.g., "2023-12-25T10:30:45.123Z").
    /// If any timestamp cannot be converted to a valid datetime, it will be replaced with an error message string.
    async fn collect_reversed(&self) -> Vec<String> {
        let fmt_string = self.format_string.as_deref().unwrap_or("%+"); // %+ is RFC 3339
        self.history_dt
            .iter_rev()
            .map(|t| match t {
                Ok(dt) => dt.format(fmt_string).to_string(),
                Err(e) => format!("Time conversion error: {}", e.to_string()),
            })
            .collect()
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
    async fn collect(&self) -> Vec<u64> {
        self.history_s.iter().map(|s: usize| s as u64).collect()
    }

    /// A list of secondary indices from `TimeIndexEntry` entries in reverse order
    async fn collect_reversed(&self) -> Vec<u64> {
        self.history_s.iter_rev().map(|s: usize| s as u64).collect()
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
    async fn collect(&self) -> Vec<i64> {
        self.intervals.collect()
    }

    /// Returns a list of time intervals between consecutive timestamps in reverse
    async fn collect_reversed(&self) -> Vec<i64> {
        self.intervals.collect_rev()
    }

    /// The mean (average) interval between consecutive timestamps.
    /// Returns `null` if there are fewer than 2 timestamps.
    async fn mean(&self) -> Option<f64> {
        self.intervals.mean()
    }

    /// The median interval between consecutive timestamps.
    /// Returns `null` if there are fewer than 2 timestamps.
    async fn median(&self) -> Option<f64> {
        self.intervals.median()
    }

    /// The maximum interval between consecutive timestamps.
    /// Returns `null` if there are fewer than 2 timestamps.
    async fn max(&self) -> Option<i64> {
        self.intervals.max()
    }

    /// The minimum interval between consecutive timestamps.
    /// Returns `null` if there are fewer than 2 timestamps.
    async fn min(&self) -> Option<i64> {
        self.intervals.min()
    }
}
