use crate::client::{
    op::{Op, ReadExpr},
    remote_graph::{expect_bool, expect_event_time_list, expect_i64, expect_optional_i64},
    transport::Transport,
    ClientError,
};
use std::sync::Arc;

/// A single event on a node/edge's history — the value type each entry in
/// `RemoteHistory.list()` / `.list_rev()` decodes to.
///
/// All three fields are optional because the server can return null for any
/// of them (synthetic events, sparse metadata). Matches the shape of the
/// local Python API's `EventTime` type.
///
/// `dt` is an RFC 3339 datetime string (the server default) — parse to a
/// typed datetime client-side if you need one.
#[derive(Clone, Debug, PartialEq)]
pub struct RemoteEventTime {
    /// The event's timestamp in the graph's native time unit (usually ms).
    pub timestamp: Option<i64>,
    /// RFC 3339 datetime string for the event (e.g. `"1970-01-01T00:00:00.003+00:00"`).
    pub dt: Option<String>,
    /// The event's internal id — a monotonically-increasing counter used to
    /// disambiguate multiple events at the same timestamp.
    pub event_id: Option<i64>,
}

/// A handle to the event history of a node or edge on the server.
///
/// Produced by:
/// - `RemoteNode.history` — event times for a node under the current view.
/// - `RemoteEdge.history` — event times for an edge under the current view.
/// - `RemoteEdge.deletions` — deletion event times for an edge.
///
/// Holds the accumulated read expression (`expr`) so terminals like `.count()`
/// and `.list()` evaluate under the full view chain, plus a `base_graph`
/// expression representing the parent graph view — used when materializing
/// members (via sub-container list/page terminals) so descendants inherit the
/// same view.
///
/// Mirrors the shape of the local Python API's `History` type. Sub-container
/// accessors (`.timestamps`, `.datetimes`, `.event_id`, `.intervals`) and
/// list/page terminals ship in follow-up batches.
#[derive(Clone)]
pub struct RemoteHistory {
    pub path: String,
    pub transport: Arc<dyn Transport>,
    pub expr: ReadExpr,
    /// The parent graph view — used by sub-containers and list materialization
    /// to rebase descendants under the same view chain.
    pub base_graph: ReadExpr,
}

impl RemoteHistory {
    /// Construct with an explicit transport, pre-built read expression, and
    /// parent graph view.
    pub fn with_expr(
        path: String,
        transport: Arc<dyn Transport>,
        expr: ReadExpr,
        base_graph: ReadExpr,
    ) -> Self {
        Self {
            path,
            transport,
            expr,
            base_graph,
        }
    }

    /// Terminal: number of events in this history. Fires one RPC.
    pub async fn count(&self) -> Result<i64, ClientError> {
        let op = Op::Read(ReadExpr::Count {
            input: Box::new(self.expr.clone()),
        });
        expect_i64(self.transport.execute(&op).await?, "count")
    }

    /// Terminal: whether this history has no events. Fires one RPC.
    pub async fn is_empty(&self) -> Result<bool, ClientError> {
        let op = Op::Read(ReadExpr::IsEmpty {
            input: Box::new(self.expr.clone()),
        });
        expect_bool(self.transport.execute(&op).await?, "isEmpty")
    }

    /// Terminal: earliest event time in this history. Returns `None` if the
    /// history is empty. Fires one RPC.
    pub async fn earliest_time(&self) -> Result<Option<i64>, ClientError> {
        let op = Op::Read(ReadExpr::EarliestTime {
            input: Box::new(self.expr.clone()),
        });
        expect_optional_i64(self.transport.execute(&op).await?, "earliestTime")
    }

    /// Terminal: latest event time in this history. Returns `None` if the
    /// history is empty. Fires one RPC.
    pub async fn latest_time(&self) -> Result<Option<i64>, ClientError> {
        let op = Op::Read(ReadExpr::LatestTime {
            input: Box::new(self.expr.clone()),
        });
        expect_optional_i64(self.transport.execute(&op).await?, "latestTime")
    }

    /// Terminal: all events in this history in ascending time order.
    /// Fires one RPC. Each event carries its timestamp, ISO 8601 datetime
    /// string, and internal event id (all optional).
    pub async fn list(&self) -> Result<Vec<RemoteEventTime>, ClientError> {
        let op = Op::Read(ReadExpr::HistoryList {
            input: Box::new(self.expr.clone()),
        });
        expect_event_time_list(self.transport.execute(&op).await?, "list")
    }

    /// Terminal: all events in this history in descending time order.
    /// Fires one RPC.
    pub async fn list_rev(&self) -> Result<Vec<RemoteEventTime>, ClientError> {
        let op = Op::Read(ReadExpr::HistoryListRev {
            input: Box::new(self.expr.clone()),
        });
        expect_event_time_list(self.transport.execute(&op).await?, "listRev")
    }
}
