use crate::client::{
    op::{Op, ReadExpr},
    remote_edge::RemoteEdge,
    remote_graph::{expect_edge_list, expect_i64},
    transport::Transport,
    ClientError,
};
use std::sync::Arc;

/// A handle to a remote collection of edges on the server.
///
/// Produced by:
/// - `RemoteGraph::edges()` — all edges in the current view.
/// - `RemoteNode::edges()` / `.in_edges()` / `.out_edges()` — the edges
///   incident to a specific node.
///
/// Holds the accumulated read expression (`expr`) so terminals like `.count()`
/// and `.list()` evaluate under the full view chain built up on the parent,
/// plus a `base_graph` expression representing the parent graph view — used
/// by `.list()` so materialized `RemoteEdge`s carry the same view chain.
///
/// Note: edges are identified by `(src, dst)` pairs — there's no
/// single-string id, so this collection exposes `.count()` and `.list()`
/// but no `.ids()`.
#[derive(Clone)]
pub struct RemoteEdges {
    pub path: String,
    pub transport: Arc<dyn Transport>,
    pub expr: ReadExpr,
    /// The parent graph view — used when materializing members via `.list()`
    /// so returned edges are rebased under the same view.
    pub base_graph: ReadExpr,
}

impl RemoteEdges {
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

    /// Terminal: the number of edges in this collection. Fires one RPC.
    pub async fn count(&self) -> Result<i64, ClientError> {
        let op = Op::Read(ReadExpr::Count {
            input: Box::new(self.expr.clone()),
        });
        expect_i64(self.transport.execute(&op).await?, "count")
    }

    /// Materialize this collection as a `Vec<RemoteEdge>`. Fires one RPC to
    /// fetch each edge's `(src, dst)` pair; each returned edge wraps its pair
    /// with `ReadExpr::Edge { input: base_graph, src, dst }` — meaning
    /// terminals on returned edges evaluate under the same view chain that
    /// produced this collection.
    pub async fn list(&self) -> Result<Vec<RemoteEdge>, ClientError> {
        let op = Op::Read(ReadExpr::EdgesList {
            input: Box::new(self.expr.clone()),
        });
        let pairs = expect_edge_list(self.transport.execute(&op).await?, "list")?;
        Ok(pairs
            .into_iter()
            .map(|(src, dst)| {
                RemoteEdge::with_expr(
                    self.path.clone(),
                    src.clone(),
                    dst.clone(),
                    self.transport.clone(),
                    ReadExpr::Edge {
                        input: Box::new(self.base_graph.clone()),
                        src,
                        dst,
                    },
                    self.base_graph.clone(),
                )
            })
            .collect())
    }
}
