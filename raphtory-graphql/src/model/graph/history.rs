use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::core::storage::timeindex::{AsTime, TimeIndexEntry};
use raphtory::db::api::view::history::{History, InternalHistoryOps};
use std::sync::Arc;
use dynamic_graphql::internal::TypeRefBuilder;

/// Represents the history of updates for an object (such as a Node or Edge) in Raphtory.
/// It provides access to the temporal properties of the object.
#[derive(ResolvedObject, Clone)]
#[graphql(name = "History")]
pub struct GqlHistory {
    pub(crate) history: History<Arc<dyn InternalHistoryOps>>,
}

// /// Converts a Raphtory `History<Arc<dyn InternalHistoryOps>>` into a `GqlHistory` object.
// impl From<History<Arc<dyn InternalHistoryOps>>> for GqlHistory {
//     fn from(history: History<Arc<dyn InternalHistoryOps>>) -> Self {
//         Self { history }
//     }
// }

/// Creates GqlHistory from History<T> object, note that this consumes the History<T> object
impl<T: InternalHistoryOps + 'static> From<History<T>> for GqlHistory {
    fn from(history: History<T>) -> Self {
        Self {
            history: History::new(Arc::new(history.0)),
        }
    }
}

#[ResolvedObjectFields]
impl GqlHistory {
    // pub fn from_arc(arced_history: History<Arc<dyn InternalHistoryOps>>) -> Self {
    //     Self { history: arced_history }
    // }
    /// The earliest timestamp (as a Unix epoch in milliseconds) associated with this history.
    /// Returns `null` if the history is empty.
    async fn earliest_time(&self) -> Option<i64> {
        self.history.earliest_time().map(|time_entry| time_entry.t())
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
}

