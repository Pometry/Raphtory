use crate::{
    client::{
        op::{EdgeSortBy, Op, ReadExpr},
        remote_edge::RemoteEdge,
        remote_graph::{expect_edge_list, expect_i64, expect_optional_i64},
        transport::Transport,
        ClientError,
    },
    model::graph::filtering::GqlEdgeFilter,
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

    /// Internal helper: apply the same view op to both `expr` and
    /// `base_graph`. Applying to `expr` narrows the collection's own view;
    /// applying to `base_graph` ensures materialized descendants (via
    /// `.list()`) inherit the same narrowed graph view.
    fn with_view_op<F>(&self, wrap: F) -> RemoteEdges
    where
        F: Fn(ReadExpr) -> ReadExpr,
    {
        RemoteEdges {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: wrap(self.expr.clone()),
            base_graph: wrap(self.base_graph.clone()),
        }
    }

    /// Time-window this collection. Lazy — no RPC.
    pub fn window(&self, start: i64, end: i64) -> RemoteEdges {
        self.with_view_op(|input| ReadExpr::Window {
            input: Box::new(input),
            start,
            end,
        })
    }

    /// Restrict to a single named layer. Lazy — no RPC.
    pub fn layer(&self, name: impl ToString) -> RemoteEdges {
        let name = name.to_string();
        self.with_view_op(|input| ReadExpr::Layer {
            input: Box::new(input),
            name: name.clone(),
        })
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    pub fn at(&self, time: i64) -> RemoteEdges {
        self.with_view_op(|input| ReadExpr::At {
            input: Box::new(input),
            time,
        })
    }

    /// Restrict to events strictly before the given time. Lazy — no RPC.
    pub fn before(&self, time: i64) -> RemoteEdges {
        self.with_view_op(|input| ReadExpr::Before {
            input: Box::new(input),
            time,
        })
    }

    /// Restrict to events strictly after the given time. Lazy — no RPC.
    pub fn after(&self, time: i64) -> RemoteEdges {
        self.with_view_op(|input| ReadExpr::After {
            input: Box::new(input),
            time,
        })
    }

    /// Latest state. Lazy — no RPC.
    pub fn latest(&self) -> RemoteEdges {
        self.with_view_op(|input| ReadExpr::Latest {
            input: Box::new(input),
        })
    }

    /// Snapshot at the latest time. Lazy — no RPC.
    pub fn snapshot_latest(&self) -> RemoteEdges {
        self.with_view_op(|input| ReadExpr::SnapshotLatest {
            input: Box::new(input),
        })
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    pub fn snapshot_at(&self, time: i64) -> RemoteEdges {
        self.with_view_op(|input| ReadExpr::SnapshotAt {
            input: Box::new(input),
            time,
        })
    }

    /// Exclude a specific layer. Lazy — no RPC.
    pub fn exclude_layer(&self, name: impl ToString) -> RemoteEdges {
        let name = name.to_string();
        self.with_view_op(|input| ReadExpr::ExcludeLayer {
            input: Box::new(input),
            name: name.clone(),
        })
    }

    /// Shrink both start and end of the current window. Lazy — no RPC.
    pub fn shrink_window(&self, start: i64, end: i64) -> RemoteEdges {
        self.with_view_op(|input| ReadExpr::ShrinkWindow {
            input: Box::new(input),
            start,
            end,
        })
    }

    /// Shrink the start of the current window. Lazy — no RPC.
    pub fn shrink_start(&self, start: i64) -> RemoteEdges {
        self.with_view_op(|input| ReadExpr::ShrinkStart {
            input: Box::new(input),
            start,
        })
    }

    /// Shrink the end of the current window. Lazy — no RPC.
    pub fn shrink_end(&self, end: i64) -> RemoteEdges {
        self.with_view_op(|input| ReadExpr::ShrinkEnd {
            input: Box::new(input),
            end,
        })
    }

    /// Restrict to the default layer. Lazy — no RPC.
    pub fn default_layer(&self) -> RemoteEdges {
        self.with_view_op(|input| ReadExpr::DefaultLayer {
            input: Box::new(input),
        })
    }

    /// Restrict to the given set of layers. Lazy — no RPC.
    pub fn layers(&self, names: Vec<String>) -> RemoteEdges {
        self.with_view_op(|input| ReadExpr::Layers {
            input: Box::new(input),
            names: names.clone(),
        })
    }

    /// Exclude the given set of layers. Lazy — no RPC.
    pub fn exclude_layers(&self, names: Vec<String>) -> RemoteEdges {
        self.with_view_op(|input| ReadExpr::ExcludeLayers {
            input: Box::new(input),
            names: names.clone(),
        })
    }

    /// Fan out this collection into one entry per event — returns a new
    /// `RemoteEdges` where each member is a single-event edge instance.
    /// Lazy — no RPC.
    ///
    /// Only updates `expr`, not `base_graph` — `explode` is an Edges-only
    /// server operation and doesn't compose with the parent graph view.
    pub fn explode(&self) -> RemoteEdges {
        RemoteEdges {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: ReadExpr::Explode {
                input: Box::new(self.expr.clone()),
            },
            base_graph: self.base_graph.clone(),
        }
    }

    /// Fan out this collection into one entry per layer per edge — returns
    /// a new `RemoteEdges`. Only updates `expr`, not `base_graph` (same
    /// reasoning as `explode`). Lazy — no RPC.
    pub fn explode_layers(&self) -> RemoteEdges {
        RemoteEdges {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: ReadExpr::ExplodeLayers {
                input: Box::new(self.expr.clone()),
            },
            base_graph: self.base_graph.clone(),
        }
    }

    /// Reorder this collection by the given sort keys (lexicographic — ties
    /// on the first key break to the second, etc.). Returns a new
    /// `RemoteEdges` handle carrying the sort; the RPC only fires on a
    /// downstream terminal. Lazy — no RPC. `base_graph` is unchanged.
    pub fn sorted(&self, sort_bys: Vec<EdgeSortBy>) -> RemoteEdges {
        RemoteEdges {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: ReadExpr::SortedEdges {
                input: Box::new(self.expr.clone()),
                sort_bys,
            },
            base_graph: self.base_graph.clone(),
        }
    }

    /// Filter this collection by a filter expression. **The filter
    /// propagates**: it applies to the current collection's membership
    /// *and* to downstream traversals from the matching edges. For a
    /// narrow-here-only variant, use `.select(...)`. Lazy — no RPC.
    pub fn filter(&self, filter: GqlEdgeFilter) -> RemoteEdges {
        RemoteEdges {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: ReadExpr::FilterEdges {
                input: Box::new(self.expr.clone()),
                filter,
            },
            base_graph: self.base_graph.clone(),
        }
    }

    /// Narrow this collection's membership by a filter expression. Unlike
    /// `.filter()`, the filter applies **only at this step** — downstream
    /// traversals from the matching edges see the unfiltered graph.
    /// Lazy — no RPC.
    pub fn select(&self, filter: GqlEdgeFilter) -> RemoteEdges {
        RemoteEdges {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: ReadExpr::SelectEdges {
                input: Box::new(self.expr.clone()),
                filter,
            },
            base_graph: self.base_graph.clone(),
        }
    }

    /// Terminal: the number of edges in this collection. Fires one RPC.
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
