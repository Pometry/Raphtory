use crate::client::{
    op::{HandleCtx, Op, ReadExpr},
    transport::{
        expect_bool, expect_event_time_list, expect_i64, expect_i64_list,
        expect_optional_event_time, expect_optional_f64, expect_optional_i64, Transport,
    },
    ClientError,
};
use chrono::{DateTime, Utc};
use raphtory_api::core::storage::timeindex::{AsTime, EventTime};
use std::sync::Arc;

/// A handle to the event history of a node or edge on the server.
///
/// Produced by:
/// - `RemoteNode.history` — event times for a node under the current view.
/// - `RemoteEdge.history` — event times for an edge under the current view.
/// - `RemoteEdge.deletions` — deletion event times for an edge.
///
/// Holds the accumulated read expression (`expr`) so terminals like `.count()`
/// and `.collect()` evaluate under the full view chain, plus a
/// materialization context (`ctx`) recording the parent graph view — used
/// when materializing members (via sub-container list/page terminals) so
/// descendants inherit the same view.
///
/// Mirrors the shape of the local Python API's `History` type. Exposes the
/// list/page terminals plus the `timestamps` / `datetimes` / `event_id` /
/// `intervals` sub-container accessors (surfaced in Python as `.t` / `.dt` /
/// `.event_id` / `.intervals`).
#[derive(Clone)]
pub struct RemoteHistory {
    pub path: String,
    pub transport: Arc<dyn Transport>,
    pub expr: Arc<ReadExpr>,
    /// The parent graph view — used by sub-containers and list materialization
    /// to rebase descendants under the same view chain.
    pub ctx: HandleCtx,
}

impl RemoteHistory {
    /// Construct with an explicit transport, pre-built read expression, and
    /// parent graph view.
    pub fn with_expr(
        path: String,
        transport: Arc<dyn Transport>,
        expr: impl Into<Arc<ReadExpr>>,
        ctx: HandleCtx,
    ) -> Self {
        Self {
            path,
            transport,
            expr: expr.into(),
            ctx,
        }
    }

    /// Terminal: number of events in this history. Fires one RPC.
    pub async fn count(&self) -> Result<i64, ClientError> {
        let op = Op::Read(ReadExpr::Count {
            input: self.expr.clone(),
        });
        expect_i64(self.transport.execute(&op).await?, "count")
    }

    /// Terminal: whether this history has no events. Fires one RPC.
    pub async fn is_empty(&self) -> Result<bool, ClientError> {
        let op = Op::Read(ReadExpr::IsEmpty {
            input: self.expr.clone(),
        });
        expect_bool(self.transport.execute(&op).await?, "isEmpty")
    }

    /// Terminal: earliest event time in this history. Returns `None` if the
    /// history is empty. Fires one RPC.
    pub async fn earliest_time(&self) -> Result<Option<EventTime>, ClientError> {
        let op = Op::Read(ReadExpr::EarliestTime {
            input: self.expr.clone(),
        });
        expect_optional_event_time(self.transport.execute(&op).await?, "earliestTime")
    }

    /// Terminal: latest event time in this history. Returns `None` if the
    /// history is empty. Fires one RPC.
    pub async fn latest_time(&self) -> Result<Option<EventTime>, ClientError> {
        let op = Op::Read(ReadExpr::LatestTime {
            input: self.expr.clone(),
        });
        expect_optional_event_time(self.transport.execute(&op).await?, "latestTime")
    }

    /// Terminal: all events in this history in ascending time order.
    /// Fires one RPC. Each event carries its timestamp, ISO 8601 datetime
    /// string, and internal event id (all optional).
    pub async fn collect(&self) -> Result<Vec<EventTime>, ClientError> {
        let op = Op::Read(ReadExpr::HistoryList {
            input: self.expr.clone(),
        });
        expect_event_time_list(self.transport.execute(&op).await?, "list")
    }

    /// Terminal: all events in this history in descending time order.
    /// Fires one RPC.
    pub async fn collect_rev(&self) -> Result<Vec<EventTime>, ClientError> {
        let op = Op::Read(ReadExpr::HistoryListRev {
            input: self.expr.clone(),
        });
        expect_event_time_list(self.transport.execute(&op).await?, "listRev")
    }

    /// Terminal: a page of events in ascending time order — at most `limit`
    /// items, starting `page_index * limit + offset` items in. `offset` and
    /// `page_index` each default to `0` when `None`. Fires one RPC.
    pub async fn page(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Result<Vec<EventTime>, ClientError> {
        let op = Op::Read(ReadExpr::HistoryPage {
            input: self.expr.clone(),
            limit,
            offset,
            page_index,
        });
        expect_event_time_list(self.transport.execute(&op).await?, "page")
    }

    /// Terminal: a page of events in descending time order. Same args as
    /// `page()`. Fires one RPC.
    pub async fn page_rev(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Result<Vec<EventTime>, ClientError> {
        let op = Op::Read(ReadExpr::HistoryPageRev {
            input: self.expr.clone(),
            limit,
            offset,
            page_index,
        });
        expect_event_time_list(self.transport.execute(&op).await?, "pageRev")
    }

    /// Returns a new history with the iteration order of its entries reversed.
    /// Lazy — no RPC.
    pub fn reverse(&self) -> RemoteHistory {
        RemoteHistory::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::HistoryReverse {
                input: self.expr.clone(),
            },
            self.ctx.clone(),
        )
    }

    /// Sub-container: timestamps view of this history — plain integer
    /// timestamps instead of full `EventTime` records. Lazy — no RPC.
    pub fn timestamps(&self) -> RemoteHistoryTimestamps {
        RemoteHistoryTimestamps {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: Arc::new(ReadExpr::HistoryTimestamps {
                input: self.expr.clone(),
            }),
            ctx: self.ctx.clone(),
        }
    }

    /// Sub-container: event-id view of this history. Lazy — no RPC.
    pub fn event_id(&self) -> RemoteHistoryEventIds {
        RemoteHistoryEventIds {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: Arc::new(ReadExpr::HistoryEventIds {
                input: self.expr.clone(),
            }),
            ctx: self.ctx.clone(),
        }
    }

    /// Sub-container: datetime view of this history — RFC 3339 strings.
    /// Lazy — no RPC.
    pub fn datetimes(&self) -> RemoteHistoryDateTimes {
        RemoteHistoryDateTimes {
            path: self.path.clone(),
            transport: self.transport.clone(),
            // Reads the *timestamps* container: the datetime is derived from
            // the timestamp client-side (`AsTime::dt`), so the server never
            // renders and ships a datetime string we can compute ourselves.
            expr: Arc::new(ReadExpr::HistoryTimestamps {
                input: self.expr.clone(),
            }),
            ctx: self.ctx.clone(),
        }
    }

    /// Sub-container: inter-event intervals — deltas between consecutive
    /// events, plus summary statistics. Lazy — no RPC.
    pub fn intervals(&self) -> RemoteIntervals {
        RemoteIntervals {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: Arc::new(ReadExpr::HistoryIntervals {
                input: self.expr.clone(),
            }),
            ctx: self.ctx.clone(),
        }
    }
}

// ============ Sub-container types ============
//
// All four sub-containers share the same shape: `expr` + `ctx` +
// four list/page terminals rendered as `list` / `listRev` / `page(...)` /
// `pageRev(...)` on the server. Return type differs by parent — parsed
// polymorphically in `parse_read` via dispatch on the parent selection.

/// A handle to the timestamps view of a `RemoteHistory` — plain integer
/// timestamps. Produced by `RemoteHistory::timestamps()`.
#[derive(Clone)]
pub struct RemoteHistoryTimestamps {
    pub path: String,
    pub transport: Arc<dyn Transport>,
    pub expr: Arc<ReadExpr>,
    pub ctx: HandleCtx,
}

impl RemoteHistoryTimestamps {
    /// Reversed view of this container. Lazy — no RPC: the reversal wraps the
    /// parent history (the server's `reverse` field), so downstream reads and
    /// any future optimised iterators compose with it automatically.
    pub fn reverse(&self) -> RemoteHistoryTimestamps {
        let input = match &*self.expr {
            ReadExpr::HistoryTimestamps { input } => input.clone(),
            // Constructed only in this file with the variant above.
            _ => unreachable!("RemoteHistoryTimestamps always wraps HistoryTimestamps"),
        };
        RemoteHistoryTimestamps {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: Arc::new(ReadExpr::HistoryTimestamps {
                input: Arc::new(ReadExpr::HistoryReverse { input }),
            }),
            ctx: self.ctx.clone(),
        }
    }

    /// Terminal: all timestamps in ascending order. Fires one RPC.
    pub async fn collect(&self) -> Result<Vec<i64>, ClientError> {
        let op = Op::Read(ReadExpr::SubList {
            input: self.expr.clone(),
        });
        expect_i64_list(self.transport.execute(&op).await?, "list")
    }

    /// Terminal: all timestamps in descending order. Fires one RPC.
    pub async fn collect_rev(&self) -> Result<Vec<i64>, ClientError> {
        let op = Op::Read(ReadExpr::SubListRev {
            input: self.expr.clone(),
        });
        expect_i64_list(self.transport.execute(&op).await?, "listRev")
    }

    /// Terminal: paginated timestamps in ascending order. Fires one RPC.
    pub async fn page(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Result<Vec<i64>, ClientError> {
        let op = Op::Read(ReadExpr::SubPage {
            input: self.expr.clone(),
            limit,
            offset,
            page_index,
        });
        expect_i64_list(self.transport.execute(&op).await?, "page")
    }

    /// Terminal: paginated timestamps in descending order. Fires one RPC.
    pub async fn page_rev(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Result<Vec<i64>, ClientError> {
        let op = Op::Read(ReadExpr::SubPageRev {
            input: self.expr.clone(),
            limit,
            offset,
            page_index,
        });
        expect_i64_list(self.transport.execute(&op).await?, "pageRev")
    }
}

/// A handle to the event-id view of a `RemoteHistory`. Produced by
/// `RemoteHistory::event_id()`. Same shape as `RemoteHistoryTimestamps`.
#[derive(Clone)]
pub struct RemoteHistoryEventIds {
    pub path: String,
    pub transport: Arc<dyn Transport>,
    pub expr: Arc<ReadExpr>,
    pub ctx: HandleCtx,
}

impl RemoteHistoryEventIds {
    /// Reversed view of this container. Lazy — no RPC: the reversal wraps the
    /// parent history (the server's `reverse` field), so downstream reads and
    /// any future optimised iterators compose with it automatically.
    pub fn reverse(&self) -> RemoteHistoryEventIds {
        let input = match &*self.expr {
            ReadExpr::HistoryEventIds { input } => input.clone(),
            // Constructed only in this file with the variant above.
            _ => unreachable!("RemoteHistoryEventIds always wraps HistoryEventIds"),
        };
        RemoteHistoryEventIds {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: Arc::new(ReadExpr::HistoryEventIds {
                input: Arc::new(ReadExpr::HistoryReverse { input }),
            }),
            ctx: self.ctx.clone(),
        }
    }

    /// Terminal: all event ids in ascending order. Fires one RPC.
    pub async fn collect(&self) -> Result<Vec<i64>, ClientError> {
        let op = Op::Read(ReadExpr::SubList {
            input: self.expr.clone(),
        });
        expect_i64_list(self.transport.execute(&op).await?, "list")
    }

    /// Terminal: all event ids in descending order. Fires one RPC.
    pub async fn collect_rev(&self) -> Result<Vec<i64>, ClientError> {
        let op = Op::Read(ReadExpr::SubListRev {
            input: self.expr.clone(),
        });
        expect_i64_list(self.transport.execute(&op).await?, "listRev")
    }

    /// Terminal: paginated event ids in ascending order. Fires one RPC.
    pub async fn page(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Result<Vec<i64>, ClientError> {
        let op = Op::Read(ReadExpr::SubPage {
            input: self.expr.clone(),
            limit,
            offset,
            page_index,
        });
        expect_i64_list(self.transport.execute(&op).await?, "page")
    }

    /// Terminal: paginated event ids in descending order. Fires one RPC.
    pub async fn page_rev(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Result<Vec<i64>, ClientError> {
        let op = Op::Read(ReadExpr::SubPageRev {
            input: self.expr.clone(),
            limit,
            offset,
            page_index,
        });
        expect_i64_list(self.transport.execute(&op).await?, "pageRev")
    }
}

/// A handle to the datetime view of a `RemoteHistory` — RFC 3339 strings.
/// Produced by `RemoteHistory::datetimes()`.
#[derive(Clone)]
pub struct RemoteHistoryDateTimes {
    pub path: String,
    pub transport: Arc<dyn Transport>,
    pub expr: Arc<ReadExpr>,
    pub ctx: HandleCtx,
}

/// Convert timestamps (epoch millis) to UTC datetimes using the same
/// [`AsTime::dt`] conversion the local API uses — no server round-trip.
fn to_datetimes(timestamps: Vec<i64>) -> Result<Vec<DateTime<Utc>>, ClientError> {
    timestamps
        .into_iter()
        .map(|t| {
            EventTime::new(t, 0).dt().map_err(|e| {
                ClientError::InvalidResponse(format!("timestamp {t} is not a valid datetime: {e}"))
            })
        })
        .collect()
}

impl RemoteHistoryDateTimes {
    /// Reversed view of this container. Lazy — no RPC: the reversal wraps the
    /// parent history (the server's `reverse` field), so downstream reads and
    /// any future optimised iterators compose with it automatically.
    pub fn reverse(&self) -> RemoteHistoryDateTimes {
        let input = match &*self.expr {
            ReadExpr::HistoryTimestamps { input } => input.clone(),
            // Constructed only in this file with the variant above.
            _ => unreachable!("RemoteHistoryDateTimes always wraps HistoryTimestamps"),
        };
        RemoteHistoryDateTimes {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: Arc::new(ReadExpr::HistoryTimestamps {
                input: Arc::new(ReadExpr::HistoryReverse { input }),
            }),
            ctx: self.ctx.clone(),
        }
    }

    /// Terminal: all datetimes in ascending order. Fires one RPC.
    pub async fn collect(&self) -> Result<Vec<DateTime<Utc>>, ClientError> {
        let op = Op::Read(ReadExpr::SubList {
            input: self.expr.clone(),
        });
        to_datetimes(expect_i64_list(self.transport.execute(&op).await?, "list")?)
    }

    /// Terminal: all datetimes in descending order. Fires one RPC.
    pub async fn collect_rev(&self) -> Result<Vec<DateTime<Utc>>, ClientError> {
        let op = Op::Read(ReadExpr::SubListRev {
            input: self.expr.clone(),
        });
        to_datetimes(expect_i64_list(
            self.transport.execute(&op).await?,
            "listRev",
        )?)
    }

    /// Terminal: paginated datetimes in ascending order. Fires one RPC.
    pub async fn page(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Result<Vec<DateTime<Utc>>, ClientError> {
        let op = Op::Read(ReadExpr::SubPage {
            input: self.expr.clone(),
            limit,
            offset,
            page_index,
        });
        to_datetimes(expect_i64_list(self.transport.execute(&op).await?, "page")?)
    }

    /// Terminal: paginated datetimes in descending order. Fires one RPC.
    pub async fn page_rev(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Result<Vec<DateTime<Utc>>, ClientError> {
        let op = Op::Read(ReadExpr::SubPageRev {
            input: self.expr.clone(),
            limit,
            offset,
            page_index,
        });
        to_datetimes(expect_i64_list(
            self.transport.execute(&op).await?,
            "pageRev",
        )?)
    }
}

/// A handle to the intervals view of a `RemoteHistory` — inter-event gaps.
/// Produced by `RemoteHistory::intervals()`. Adds stats terminals
/// (mean/median/max/min) on top of the shared list/page shape.
#[derive(Clone)]
pub struct RemoteIntervals {
    pub path: String,
    pub transport: Arc<dyn Transport>,
    pub expr: Arc<ReadExpr>,
    pub ctx: HandleCtx,
}

impl RemoteIntervals {
    /// Reversed view of this container. Lazy — no RPC: the reversal wraps the
    /// parent history (the server's `reverse` field), so downstream reads and
    /// any future optimised iterators compose with it automatically.
    pub fn reverse(&self) -> RemoteIntervals {
        let input = match &*self.expr {
            ReadExpr::HistoryIntervals { input } => input.clone(),
            // Constructed only in this file with the variant above.
            _ => unreachable!("RemoteIntervals always wraps HistoryIntervals"),
        };
        RemoteIntervals {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: Arc::new(ReadExpr::HistoryIntervals {
                input: Arc::new(ReadExpr::HistoryReverse { input }),
            }),
            ctx: self.ctx.clone(),
        }
    }

    /// Terminal: all intervals in ascending order. Fires one RPC.
    pub async fn collect(&self) -> Result<Vec<i64>, ClientError> {
        let op = Op::Read(ReadExpr::SubList {
            input: self.expr.clone(),
        });
        expect_i64_list(self.transport.execute(&op).await?, "list")
    }

    /// Terminal: all intervals in descending order. Fires one RPC.
    pub async fn collect_rev(&self) -> Result<Vec<i64>, ClientError> {
        let op = Op::Read(ReadExpr::SubListRev {
            input: self.expr.clone(),
        });
        expect_i64_list(self.transport.execute(&op).await?, "listRev")
    }

    /// Terminal: paginated intervals in ascending order. Fires one RPC.
    pub async fn page(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Result<Vec<i64>, ClientError> {
        let op = Op::Read(ReadExpr::SubPage {
            input: self.expr.clone(),
            limit,
            offset,
            page_index,
        });
        expect_i64_list(self.transport.execute(&op).await?, "page")
    }

    /// Terminal: paginated intervals in descending order. Fires one RPC.
    pub async fn page_rev(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Result<Vec<i64>, ClientError> {
        let op = Op::Read(ReadExpr::SubPageRev {
            input: self.expr.clone(),
            limit,
            offset,
            page_index,
        });
        expect_i64_list(self.transport.execute(&op).await?, "pageRev")
    }

    /// Terminal: mean interval. `None` if fewer than 2 events (no intervals).
    /// Fires one RPC.
    pub async fn mean(&self) -> Result<Option<f64>, ClientError> {
        let op = Op::Read(ReadExpr::IntervalsMean {
            input: self.expr.clone(),
        });
        expect_optional_f64(self.transport.execute(&op).await?, "mean")
    }

    /// Terminal: median interval. `None` if fewer than 2 events. Fires one RPC.
    pub async fn median(&self) -> Result<Option<i64>, ClientError> {
        let op = Op::Read(ReadExpr::IntervalsMedian {
            input: self.expr.clone(),
        });
        expect_optional_i64(self.transport.execute(&op).await?, "median")
    }

    /// Terminal: max interval. `None` if fewer than 2 events. Fires one RPC.
    pub async fn max(&self) -> Result<Option<i64>, ClientError> {
        let op = Op::Read(ReadExpr::IntervalsMax {
            input: self.expr.clone(),
        });
        expect_optional_i64(self.transport.execute(&op).await?, "max")
    }

    /// Terminal: min interval. `None` if fewer than 2 events. Fires one RPC.
    pub async fn min(&self) -> Result<Option<i64>, ClientError> {
        let op = Op::Read(ReadExpr::IntervalsMin {
            input: self.expr.clone(),
        });
        expect_optional_i64(self.transport.execute(&op).await?, "min")
    }
}
