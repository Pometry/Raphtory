use crate::client::{
    op::{Op, ReadExpr},
    remote_graph::{expect_i64, expect_optional_i64, expect_string_list},
    remote_node::RemoteNode,
    transport::Transport,
    ClientError,
};
use std::sync::Arc;

/// A handle to a remote collection of nodes on the server.
///
/// Produced by:
/// - `RemoteGraph::nodes()` — all nodes in the current view.
/// - `RemoteNode::neighbours()` / `.in_neighbours()` / `.out_neighbours()` —
///   the neighbours of a specific node.
///
/// Holds the accumulated read expression (`expr`) so terminals like `.ids()`
/// and `.count()` evaluate under the full view chain built up on the parent,
/// plus a `base_graph` expression representing the parent graph view — used
/// by `.list()` so materialized `RemoteNode`s carry the same view chain.
#[derive(Clone)]
pub struct RemoteNodes {
    pub path: String,
    pub transport: Arc<dyn Transport>,
    pub expr: ReadExpr,
    /// The parent graph view under which this collection lives — used when
    /// materializing members via `.list()` so returned nodes are rebased under
    /// the same view.
    pub base_graph: ReadExpr,
}

impl RemoteNodes {
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

    /// Terminal: the list of node ids in this collection. Fires one RPC.
    pub async fn ids(&self) -> Result<Vec<String>, ClientError> {
        let op = Op::Read(ReadExpr::Ids {
            input: Box::new(self.expr.clone()),
        });
        expect_string_list(self.transport.execute(&op).await?, "ids")
    }

    /// Terminal: the number of nodes in this collection. Fires one RPC.
    pub async fn count(&self) -> Result<i64, ClientError> {
        let op = Op::Read(ReadExpr::Count {
            input: Box::new(self.expr.clone()),
        });
        expect_i64(self.transport.execute(&op).await?, "count")
    }

    /// Terminal: view start bound for this collection — `None` if unbounded.
    /// Fires one RPC.
    pub async fn start(&self) -> Result<Option<i64>, ClientError> {
        let op = Op::Read(ReadExpr::Start {
            input: Box::new(self.expr.clone()),
        });
        expect_optional_i64(self.transport.execute(&op).await?, "start")
    }

    /// Terminal: view end bound for this collection — `None` if unbounded.
    /// Fires one RPC.
    pub async fn end(&self) -> Result<Option<i64>, ClientError> {
        let op = Op::Read(ReadExpr::End {
            input: Box::new(self.expr.clone()),
        });
        expect_optional_i64(self.transport.execute(&op).await?, "end")
    }

    /// Materialize this collection as a `Vec<RemoteNode>`. Fires one RPC to
    /// fetch the ids; each returned node wraps its id with a
    /// `ReadExpr::Node { input: base_graph, id }` — meaning terminals on
    /// returned nodes evaluate under the same view chain that produced this
    /// collection.
    pub async fn list(&self) -> Result<Vec<RemoteNode>, ClientError> {
        let ids = self.ids().await?;
        Ok(ids
            .into_iter()
            .map(|id| {
                RemoteNode::with_expr(
                    self.path.clone(),
                    id.clone(),
                    self.transport.clone(),
                    ReadExpr::Node {
                        input: Box::new(self.base_graph.clone()),
                        id,
                    },
                    self.base_graph.clone(),
                )
            })
            .collect())
    }
}
