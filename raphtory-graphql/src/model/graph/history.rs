use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::{
    core::storage::timeindex::AsTime,
    db::api::view::history::{History, InternalHistoryOps, Intervals},
};
use std::sync::Arc;

/// Represents the history of updates for an object in Raphtory.
/// It provides access to the temporal properties of the object.
#[derive(ResolvedObject, Clone)]
#[graphql(name = "History")]
pub struct GqlHistory {
    pub(crate) history: History<'static, Arc<dyn InternalHistoryOps>>,
}

// /// Converts a Raphtory `History<Arc<dyn InternalHistoryOps>>` into a `GqlHistory` object.
// impl From<History<Arc<dyn InternalHistoryOps>>> for GqlHistory {
//     fn from(history: History<Arc<dyn InternalHistoryOps>>) -> Self {
//         Self { history }
//     }
// }

/// Creates GqlHistory from History<T> object, note that this consumes the History<T> object
impl<'a, T: InternalHistoryOps + 'static> From<History<'a, T>> for GqlHistory {
    fn from(history: History<T>) -> Self {
        Self {
            history: History::new(Arc::new(history.0)),
        }
    }
}

#[ResolvedObjectFields]
impl GqlHistory {
    /// The earliest timestamp (as a Unix epoch in milliseconds) associated with this history.
    /// Returns `null` if the history is empty.
    async fn earliest_time(&self) -> Option<i64> {
        self.history
            .earliest_time()
            .map(|time_entry| time_entry.t())
    }

    /// The latest timestamp (as a Unix epoch in milliseconds) associated with this history.
    /// Returns `null` if the history is empty.
    async fn latest_time(&self) -> Option<i64> {
        self.history.latest_time().map(|time_entry| time_entry.t())
    }

    /// A list of all timestamps (as Unix epochs in milliseconds) present in this history,
    async fn timestamps(&self) -> Vec<i64> {
        self.history
            .iter()
            .map(|time_entry| time_entry.t())
            .collect()
    }

    /// A list of all timestamps (as Unix epochs in milliseconds) present in this history, in reverse chronological order.
    async fn timestamps_reversed(&self) -> Vec<i64> {
        self.history
            .iter_rev()
            .map(|time_entry| time_entry.t())
            .collect()
    }

    /// Returns true if the History object is empty
    async fn is_empty(&self) -> bool {
        self.history.is_empty()
    }

    /// Returns the number of entries contained in the History object
    async fn len(&self) -> i64 {
        self.history.len() as i64
    }

    /// Returns an Intervals object that provides access to the intervals between temporal entries
    async fn intervals(&self) -> GqlIntervals {
        GqlIntervals {
            intervals: Intervals::new(self.history.0.clone()), // clone the Arc, not the underlying object
        }
    }
}

/// Provides access to the intervals between temporal entries of an object.
#[derive(ResolvedObject, Clone)]
#[graphql(name = "Intervals")]
pub struct GqlIntervals {
    pub(crate) intervals: Intervals<Arc<dyn InternalHistoryOps>>,
}

/// Creates GqlIntervals from Intervals<T> object, note that this consumes the Intervals<T> object
impl<'a, T: InternalHistoryOps + 'static> From<Intervals<T>> for GqlIntervals {
    fn from(intervals: Intervals<T>) -> Self {
        Self {
            intervals: Intervals::new(Arc::new(intervals.0)),
        }
    }
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
