use crate::client::{
    op::{Op, ReadExpr},
    remote_graph::{expect_bool, expect_i64, expect_optional_i64},
    transport::Transport,
    ClientError,
};
use std::sync::Arc;

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
}
