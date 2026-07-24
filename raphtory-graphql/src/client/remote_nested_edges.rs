use crate::{
    client::{
        op::{Op, ReadExpr},
        remote_edge::RemoteEdge,
        remote_graph::{
            expect_bool, expect_double_nested_string_list, expect_i64, expect_nested_bool_list,
            expect_nested_edge_list, expect_nested_optional_event_time_list,
            expect_nested_string_list, expect_optional_event_time, expect_optional_i64,
        },
        remote_history::RemoteEventTime,
        transport::Transport,
        ClientError,
    },
    model::graph::filtering::GqlEdgeFilter,
};
use std::sync::Arc;

/// A handle to a nested edges collection on the server — the edges incident to
/// *each* node in a `RemoteNodes` collection, in a given direction. Produced by:
/// - `RemoteNodes::edges()` — both directions
/// - `RemoteNodes::in_edges()`
/// - `RemoteNodes::out_edges()`
///
/// Distinct from `RemoteEdges` because it is **nested**: the server type
/// (`GqlNestedEdges`) groups results per source node. `collect()` returns
/// `Vec<Vec<RemoteEdge>>` (one inner list per source node), and `count()` is
/// the number of source edge collections.
///
/// Structurally identical to `RemoteEdges` — same `expr` + `base_graph` fields,
/// same view-op wiring — but the terminals return nested shapes and there is no
/// `ids()` (edges are identified by `(src, dst)` pairs, not a single string id).
#[derive(Clone)]
pub struct RemoteNestedEdges {
    pub path: String,
    pub transport: Arc<dyn Transport>,
    pub expr: ReadExpr,
    /// The parent graph view — see `RemoteEdges` for details.
    pub base_graph: ReadExpr,
}

impl RemoteNestedEdges {
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

    fn with_view_op<F>(&self, wrap: F) -> RemoteNestedEdges
    where
        F: Fn(ReadExpr) -> ReadExpr,
    {
        RemoteNestedEdges {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: wrap(self.expr.clone()),
            base_graph: wrap(self.base_graph.clone()),
        }
    }

    /// Time-window this collection. Lazy — no RPC.
    pub fn window(&self, start: i64, end: i64) -> RemoteNestedEdges {
        self.with_view_op(|input| ReadExpr::Window {
            input: Box::new(input),
            start,
            end,
        })
    }

    /// Restrict to a single named layer. Lazy — no RPC.
    pub fn layer(&self, name: impl ToString) -> RemoteNestedEdges {
        let name = name.to_string();
        self.with_view_op(|input| ReadExpr::Layer {
            input: Box::new(input),
            name: name.clone(),
        })
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    pub fn at(&self, time: i64) -> RemoteNestedEdges {
        self.with_view_op(|input| ReadExpr::At {
            input: Box::new(input),
            time,
        })
    }

    /// Restrict to events strictly before the given time. Lazy — no RPC.
    pub fn before(&self, time: i64) -> RemoteNestedEdges {
        self.with_view_op(|input| ReadExpr::Before {
            input: Box::new(input),
            time,
        })
    }

    /// Restrict to events strictly after the given time. Lazy — no RPC.
    pub fn after(&self, time: i64) -> RemoteNestedEdges {
        self.with_view_op(|input| ReadExpr::After {
            input: Box::new(input),
            time,
        })
    }

    /// Latest state. Lazy — no RPC.
    pub fn latest(&self) -> RemoteNestedEdges {
        self.with_view_op(|input| ReadExpr::Latest {
            input: Box::new(input),
        })
    }

    /// Snapshot at the latest time. Lazy — no RPC.
    pub fn snapshot_latest(&self) -> RemoteNestedEdges {
        self.with_view_op(|input| ReadExpr::SnapshotLatest {
            input: Box::new(input),
        })
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    pub fn snapshot_at(&self, time: i64) -> RemoteNestedEdges {
        self.with_view_op(|input| ReadExpr::SnapshotAt {
            input: Box::new(input),
            time,
        })
    }

    /// Exclude a specific layer. Lazy — no RPC.
    pub fn exclude_layer(&self, name: impl ToString) -> RemoteNestedEdges {
        let name = name.to_string();
        self.with_view_op(|input| ReadExpr::ExcludeLayer {
            input: Box::new(input),
            name: name.clone(),
        })
    }

    /// Shrink both start and end of the current window. Lazy — no RPC.
    pub fn shrink_window(&self, start: i64, end: i64) -> RemoteNestedEdges {
        self.with_view_op(|input| ReadExpr::ShrinkWindow {
            input: Box::new(input),
            start,
            end,
        })
    }

    /// Shrink the start of the current window. Lazy — no RPC.
    pub fn shrink_start(&self, start: i64) -> RemoteNestedEdges {
        self.with_view_op(|input| ReadExpr::ShrinkStart {
            input: Box::new(input),
            start,
        })
    }

    /// Shrink the end of the current window. Lazy — no RPC.
    pub fn shrink_end(&self, end: i64) -> RemoteNestedEdges {
        self.with_view_op(|input| ReadExpr::ShrinkEnd {
            input: Box::new(input),
            end,
        })
    }

    /// Restrict to the default layer. Lazy — no RPC.
    pub fn default_layer(&self) -> RemoteNestedEdges {
        self.with_view_op(|input| ReadExpr::DefaultLayer {
            input: Box::new(input),
        })
    }

    /// Restrict to the given set of layers. Lazy — no RPC.
    pub fn layers(&self, names: Vec<String>) -> RemoteNestedEdges {
        self.with_view_op(|input| ReadExpr::Layers {
            input: Box::new(input),
            names: names.clone(),
        })
    }

    /// Exclude the given set of layers. Lazy — no RPC.
    pub fn exclude_layers(&self, names: Vec<String>) -> RemoteNestedEdges {
        self.with_view_op(|input| ReadExpr::ExcludeLayers {
            input: Box::new(input),
            names: names.clone(),
        })
    }

    /// Restrict to the given set of valid layers. Lazy — no RPC.
    pub fn valid_layers(&self, names: Vec<String>) -> RemoteNestedEdges {
        self.with_view_op(|input| ReadExpr::ValidLayers {
            input: Box::new(input),
            names: names.clone(),
        })
    }

    /// Exclude a specific valid layer from the view. Lazy — no RPC.
    pub fn exclude_valid_layer(&self, name: impl ToString) -> RemoteNestedEdges {
        let name = name.to_string();
        self.with_view_op(|input| ReadExpr::ExcludeValidLayer {
            input: Box::new(input),
            name: name.clone(),
        })
    }

    /// Exclude the given set of valid layers from the view. Lazy — no RPC.
    pub fn exclude_valid_layers(&self, names: Vec<String>) -> RemoteNestedEdges {
        self.with_view_op(|input| ReadExpr::ExcludeValidLayers {
            input: Box::new(input),
            names: names.clone(),
        })
    }

    /// Filter this collection by an edge filter. **Propagates** to downstream
    /// traversals from the matching edges. Wraps only `expr`. Lazy — no RPC.
    pub fn filter(&self, filter: GqlEdgeFilter) -> RemoteNestedEdges {
        RemoteNestedEdges {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: ReadExpr::FilterEdges {
                input: Box::new(self.expr.clone()),
                filter,
            },
            base_graph: self.base_graph.clone(),
        }
    }

    /// Narrow this collection's membership by an edge filter — applies only at
    /// this step; downstream traversals see the unfiltered graph. Lazy — no RPC.
    pub fn select(&self, filter: GqlEdgeFilter) -> RemoteNestedEdges {
        RemoteNestedEdges {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: ReadExpr::SelectEdges {
                input: Box::new(self.expr.clone()),
                filter,
            },
            base_graph: self.base_graph.clone(),
        }
    }

    /// Fan out each source's edges into one entry per event — returns a new
    /// `RemoteNestedEdges` where every member is a single-event edge instance.
    /// Mirrors the local `NestedEdges.explode`. Only updates `expr`, not
    /// `base_graph` (same reasoning as the flat `RemoteEdges.explode`). Lazy —
    /// no RPC.
    pub fn explode(&self) -> RemoteNestedEdges {
        RemoteNestedEdges {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: ReadExpr::Explode {
                input: Box::new(self.expr.clone()),
            },
            base_graph: self.base_graph.clone(),
        }
    }

    /// Fan out each source's edges into one entry per layer per edge — returns
    /// a new `RemoteNestedEdges`. Mirrors the local `NestedEdges.explode_layers`.
    /// Only updates `expr`, not `base_graph`. Lazy — no RPC.
    pub fn explode_layers(&self) -> RemoteNestedEdges {
        RemoteNestedEdges {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: ReadExpr::ExplodeLayers {
                input: Box::new(self.expr.clone()),
            },
            base_graph: self.base_graph.clone(),
        }
    }

    /// Terminal: the number of source edge collections in this collection.
    /// Fires one RPC.
    pub async fn count(&self) -> Result<i64, ClientError> {
        let op = Op::Read(ReadExpr::Count {
            input: Box::new(self.expr.clone()),
        });
        expect_i64(self.transport.execute(&op).await?, "count")
    }

    /// Terminal: whether this view contains a layer named `name`. Fires one RPC.
    pub async fn has_layer(&self, name: impl ToString) -> Result<bool, ClientError> {
        let op = Op::Read(ReadExpr::HasLayer {
            input: Box::new(self.expr.clone()),
            name: name.to_string(),
        });
        expect_bool(self.transport.execute(&op).await?, "hasLayer")
    }

    /// Terminal: the size of the window covered by this view (`end - start`),
    /// or `None` for an unbounded view. Fires one RPC.
    pub async fn window_size(&self) -> Result<Option<i64>, ClientError> {
        let op = Op::Read(ReadExpr::WindowSize {
            input: Box::new(self.expr.clone()),
        });
        expect_optional_i64(self.transport.execute(&op).await?, "windowSize")
    }

    /// Columnar accessor: each source's edge `(src, dst)` id pairs — one inner
    /// list per source node. Mirrors the local `NestedEdges.id`. Fires one RPC.
    pub async fn id(&self) -> Result<Vec<Vec<(String, String)>>, ClientError> {
        let op = Op::Read(ReadExpr::NestedEdgesList {
            input: Box::new(self.expr.clone()),
        });
        expect_nested_edge_list(self.transport.execute(&op).await?, "id")
    }

    /// Columnar accessor: each source's per-edge layer names — one inner list
    /// per source node. Mirrors the local `NestedEdges.layer_names`. Fires one RPC.
    pub async fn layer_names(&self) -> Result<Vec<Vec<Vec<String>>>, ClientError> {
        let op = Op::Read(ReadExpr::NestedLayerNames {
            input: Box::new(self.expr.clone()),
        });
        expect_double_nested_string_list(self.transport.execute(&op).await?, "layerNames")
    }

    /// Columnar accessor: each source's per-edge single layer name — one inner
    /// list per source node. Only valid on exploded edges; the server raises a
    /// GraphQL error otherwise. Mirrors the local `NestedEdges.layer_name`.
    /// Fires one RPC.
    pub async fn layer_name(&self) -> Result<Vec<Vec<String>>, ClientError> {
        let op = Op::Read(ReadExpr::NestedLayerName {
            input: Box::new(self.expr.clone()),
        });
        expect_nested_string_list(self.transport.execute(&op).await?, "layerName")
    }

    /// Columnar accessor: each source's per-edge earliest event time — one
    /// inner list per source node. Mirrors the local `NestedEdges.earliest_time`.
    /// Fires one RPC.
    pub async fn earliest_time(&self) -> Result<Vec<Vec<Option<RemoteEventTime>>>, ClientError> {
        let op = Op::Read(ReadExpr::NestedEarliestTime {
            input: Box::new(self.expr.clone()),
        });
        expect_nested_optional_event_time_list(self.transport.execute(&op).await?, "earliestTime")
    }

    /// Columnar accessor: each source's per-edge latest event time — one inner
    /// list per source node. Mirrors the local `NestedEdges.latest_time`. Fires
    /// one RPC.
    pub async fn latest_time(&self) -> Result<Vec<Vec<Option<RemoteEventTime>>>, ClientError> {
        let op = Op::Read(ReadExpr::NestedLatestTime {
            input: Box::new(self.expr.clone()),
        });
        expect_nested_optional_event_time_list(self.transport.execute(&op).await?, "latestTime")
    }

    /// Columnar accessor: each source's per-edge event time — one inner list
    /// per source node. Only valid on exploded edges; the server raises a
    /// GraphQL error otherwise. Mirrors the local `NestedEdges.time`. Fires one RPC.
    pub async fn time(&self) -> Result<Vec<Vec<Option<RemoteEventTime>>>, ClientError> {
        let op = Op::Read(ReadExpr::NestedTime {
            input: Box::new(self.expr.clone()),
        });
        expect_nested_optional_event_time_list(self.transport.execute(&op).await?, "time")
    }

    /// Columnar accessor: whether each edge is active (has an event) in the
    /// current view, grouped per source node — mirrors the local
    /// `NestedEdges.is_active`. Fires one RPC.
    pub async fn is_active(&self) -> Result<Vec<Vec<bool>>, ClientError> {
        let op = Op::Read(ReadExpr::NestedIsActive {
            input: Box::new(self.expr.clone()),
        });
        expect_nested_bool_list(self.transport.execute(&op).await?, "isActive")
    }

    /// Columnar accessor: whether each edge is valid (not deleted) at the
    /// current time, grouped per source node — mirrors the local
    /// `NestedEdges.is_valid`. Fires one RPC.
    pub async fn is_valid(&self) -> Result<Vec<Vec<bool>>, ClientError> {
        let op = Op::Read(ReadExpr::NestedIsValid {
            input: Box::new(self.expr.clone()),
        });
        expect_nested_bool_list(self.transport.execute(&op).await?, "isValid")
    }

    /// Columnar accessor: whether each edge has been deleted at the current
    /// time, grouped per source node — mirrors the local
    /// `NestedEdges.is_deleted`. Fires one RPC.
    pub async fn is_deleted(&self) -> Result<Vec<Vec<bool>>, ClientError> {
        let op = Op::Read(ReadExpr::NestedIsDeleted {
            input: Box::new(self.expr.clone()),
        });
        expect_nested_bool_list(self.transport.execute(&op).await?, "isDeleted")
    }

    /// Columnar accessor: whether each edge is a self-loop (`src == dst`),
    /// grouped per source node — mirrors the local `NestedEdges.is_self_loop`.
    /// Fires one RPC.
    pub async fn is_self_loop(&self) -> Result<Vec<Vec<bool>>, ClientError> {
        let op = Op::Read(ReadExpr::NestedIsSelfLoop {
            input: Box::new(self.expr.clone()),
        });
        expect_nested_bool_list(self.transport.execute(&op).await?, "isSelfLoop")
    }

    /// Terminal: view start bound for this collection — `None` if unbounded.
    /// Fires one RPC.
    pub async fn start(&self) -> Result<Option<RemoteEventTime>, ClientError> {
        let op = Op::Read(ReadExpr::Start {
            input: Box::new(self.expr.clone()),
        });
        expect_optional_event_time(self.transport.execute(&op).await?, "start")
    }

    /// Terminal: view end bound for this collection — `None` if unbounded.
    /// Fires one RPC.
    pub async fn end(&self) -> Result<Option<RemoteEventTime>, ClientError> {
        let op = Op::Read(ReadExpr::End {
            input: Box::new(self.expr.clone()),
        });
        expect_optional_event_time(self.transport.execute(&op).await?, "end")
    }

    /// Materialize as `Vec<Vec<RemoteEdge>>` — one inner list per source node.
    /// Fires one RPC (to fetch the nested `(src, dst)` pairs); each returned
    /// edge is rebased under the same view chain that produced this collection.
    pub async fn collect(&self) -> Result<Vec<Vec<RemoteEdge>>, ClientError> {
        let op = Op::Read(ReadExpr::NestedEdgesList {
            input: Box::new(self.expr.clone()),
        });
        let nested = expect_nested_edge_list(self.transport.execute(&op).await?, "list")?;
        Ok(nested
            .into_iter()
            .map(|row| {
                row.into_iter()
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
                    .collect()
            })
            .collect())
    }
}
