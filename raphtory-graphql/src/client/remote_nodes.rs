use crate::client::{
    op::{Op, ReadExpr},
    remote_graph::{expect_i64, expect_string_list},
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
/// and `.count()` evaluate under the full view chain built up on the parent.
///
/// **Known limitation**: `.list()` materializes members as `RemoteNode`s
/// rooted at the graph path (via `ReadExpr::Root`). This means terminals
/// invoked on those returned nodes see the *unwindowed* view, not the view
/// chain that produced this collection. For view-dependent terminals under
/// a view chain, prefer `.ids()` and re-select nodes explicitly via
/// `parent_graph.node(id)`.
#[derive(Clone)]
pub struct RemoteNodes {
    pub path: String,
    pub transport: Arc<dyn Transport>,
    pub expr: ReadExpr,
}

impl RemoteNodes {
    /// Construct with an explicit transport and pre-built read expression.
    /// Used when a parent (`RemoteGraph`/`RemoteNode`) propagates its
    /// accumulated view chain into a collection reference.
    pub fn with_expr(path: String, transport: Arc<dyn Transport>, expr: ReadExpr) -> Self {
        Self {
            path,
            transport,
            expr,
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

    /// Materialize this collection as a `Vec<RemoteNode>`. Fires one RPC to
    /// fetch the ids; each returned node wraps its id with a fresh
    /// `ReadExpr::Node { input: Root, id }` (see the known limitation on the
    /// struct doc).
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
                        input: Box::new(ReadExpr::Root {
                            path: self.path.clone(),
                        }),
                        id,
                    },
                )
            })
            .collect())
    }
}
