use crate::{
    client::{
        op::{HandleCtx, HandleOp, InputTime, Op, ReadExpr},
        remote_collection_metadata::{RemoteMetadataView, RemotePropertiesView},
        remote_edges::RemoteEdges,
        remote_graph::{
            expect_bool, expect_i64, expect_i64_list, expect_optional_event_time,
            expect_optional_event_time_list, expect_optional_i64, expect_optional_string_list,
            expect_string_list,
        },
        remote_history::{RemoteEventTime, RemoteHistory},
        remote_node::RemoteNode,
        transport::Transport,
        ClientError,
    },
    model::graph::filtering::GqlNodeFilter,
};
use std::sync::Arc;

/// A handle to a "path from node" collection on the server — the nodes
/// reachable one hop from a specific node in a given direction. Produced by:
/// - `RemoteNode::neighbours()` — both directions
/// - `RemoteNode::in_neighbours()`
/// - `RemoteNode::out_neighbours()`
///
/// Distinct from `RemoteNodes` because the server-side type
/// (`GqlPathFromNode`) exposes a **subset** of `GqlNodes`' fields:
/// - **Missing**: `sorted` — this method is simply not available; the server
///   has no field for it here.
/// - **Present**: view chain (`window`, `at`, `layer`, `default_layer`, ...),
///   `type_filter`, and terminals (`ids`, `count`, `list`, `start`, `end`).
///
/// Structurally identical to `RemoteNodes` — same `expr` + `ctx`
/// fields, same view-op wiring — but the type distinction is what gives
/// clients compile-time protection from calling unsupported methods.
#[derive(Clone)]
pub struct RemotePathFromNode {
    pub path: String,
    pub transport: Arc<dyn Transport>,
    pub expr: ReadExpr,
    /// Materialization context — see `RemoteNodes` for details.
    pub ctx: HandleCtx,
}

impl RemotePathFromNode {
    /// Construct with an explicit transport, pre-built read expression, and
    /// materialization context.
    pub fn with_expr(
        path: String,
        transport: Arc<dyn Transport>,
        expr: ReadExpr,
        ctx: HandleCtx,
    ) -> Self {
        Self {
            path,
            transport,
            expr,
            ctx,
        }
    }

    fn with_view_op<F>(&self, wrap: F) -> RemotePathFromNode
    where
        F: Fn(ReadExpr) -> ReadExpr + Send + Sync + 'static,
    {
        let wrap = Arc::new(wrap);
        RemotePathFromNode {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: wrap(self.expr.clone()),
            ctx: self.ctx.with_op(HandleOp::View(wrap)),
        }
    }

    /// Time-window this collection. Lazy — no RPC.
    pub fn window(&self, start: InputTime, end: InputTime) -> RemotePathFromNode {
        self.with_view_op(move |input| ReadExpr::Window {
            input: Arc::new(input),
            start,
            end,
        })
    }

    /// Restrict to a single named layer. Lazy — no RPC.
    pub fn layer(&self, name: impl ToString) -> RemotePathFromNode {
        let name = name.to_string();
        self.with_view_op(move |input| ReadExpr::Layer {
            input: Arc::new(input),
            name: name.clone(),
        })
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    pub fn at(&self, time: InputTime) -> RemotePathFromNode {
        self.with_view_op(move |input| ReadExpr::At {
            input: Arc::new(input),
            time,
        })
    }

    /// Restrict to events strictly before the given time. Lazy — no RPC.
    pub fn before(&self, time: InputTime) -> RemotePathFromNode {
        self.with_view_op(move |input| ReadExpr::Before {
            input: Arc::new(input),
            time,
        })
    }

    /// Restrict to events strictly after the given time. Lazy — no RPC.
    pub fn after(&self, time: InputTime) -> RemotePathFromNode {
        self.with_view_op(move |input| ReadExpr::After {
            input: Arc::new(input),
            time,
        })
    }

    /// Latest state. Lazy — no RPC.
    pub fn latest(&self) -> RemotePathFromNode {
        self.with_view_op(move |input| ReadExpr::Latest {
            input: Arc::new(input),
        })
    }

    /// Snapshot at the latest time. Lazy — no RPC.
    pub fn snapshot_latest(&self) -> RemotePathFromNode {
        self.with_view_op(move |input| ReadExpr::SnapshotLatest {
            input: Arc::new(input),
        })
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    pub fn snapshot_at(&self, time: InputTime) -> RemotePathFromNode {
        self.with_view_op(move |input| ReadExpr::SnapshotAt {
            input: Arc::new(input),
            time,
        })
    }

    /// Exclude a specific layer. Lazy — no RPC.
    pub fn exclude_layer(&self, name: impl ToString) -> RemotePathFromNode {
        let name = name.to_string();
        self.with_view_op(move |input| ReadExpr::ExcludeLayer {
            input: Arc::new(input),
            name: name.clone(),
        })
    }

    /// Shrink both start and end of the current window. Lazy — no RPC.
    pub fn shrink_window(&self, start: InputTime, end: InputTime) -> RemotePathFromNode {
        self.with_view_op(move |input| ReadExpr::ShrinkWindow {
            input: Arc::new(input),
            start,
            end,
        })
    }

    /// Shrink the start of the current window. Lazy — no RPC.
    pub fn shrink_start(&self, start: InputTime) -> RemotePathFromNode {
        self.with_view_op(move |input| ReadExpr::ShrinkStart {
            input: Arc::new(input),
            start,
        })
    }

    /// Shrink the end of the current window. Lazy — no RPC.
    pub fn shrink_end(&self, end: InputTime) -> RemotePathFromNode {
        self.with_view_op(move |input| ReadExpr::ShrinkEnd {
            input: Arc::new(input),
            end,
        })
    }

    /// Restrict to the default layer. Lazy — no RPC.
    pub fn default_layer(&self) -> RemotePathFromNode {
        self.with_view_op(move |input| ReadExpr::DefaultLayer {
            input: Arc::new(input),
        })
    }

    /// Restrict to the given set of layers. Lazy — no RPC.
    pub fn layers(&self, names: Vec<String>) -> RemotePathFromNode {
        let names: Arc<[String]> = names.into();
        self.with_view_op(move |input| ReadExpr::Layers {
            input: Arc::new(input),
            names: names.clone(),
        })
    }

    /// Exclude the given set of layers. Lazy — no RPC.
    pub fn exclude_layers(&self, names: Vec<String>) -> RemotePathFromNode {
        let names: Arc<[String]> = names.into();
        self.with_view_op(move |input| ReadExpr::ExcludeLayers {
            input: Arc::new(input),
            names: names.clone(),
        })
    }

    /// Restrict to the given set of valid layers. Lazy — no RPC.
    pub fn valid_layers(&self, names: Vec<String>) -> RemotePathFromNode {
        let names: Arc<[String]> = names.into();
        self.with_view_op(move |input| ReadExpr::ValidLayers {
            input: Arc::new(input),
            names: names.clone(),
        })
    }

    /// Exclude a specific valid layer from the view. Lazy — no RPC.
    pub fn exclude_valid_layer(&self, name: impl ToString) -> RemotePathFromNode {
        let name = name.to_string();
        self.with_view_op(move |input| ReadExpr::ExcludeValidLayer {
            input: Arc::new(input),
            name: name.clone(),
        })
    }

    /// Exclude the given set of valid layers from the view. Lazy — no RPC.
    pub fn exclude_valid_layers(&self, names: Vec<String>) -> RemotePathFromNode {
        let names: Arc<[String]> = names.into();
        self.with_view_op(move |input| ReadExpr::ExcludeValidLayers {
            input: Arc::new(input),
            names: names.clone(),
        })
    }

    /// Restrict this collection to members whose node type is in the given
    /// list. Filters membership — the returned collection has fewer members.
    /// Lazy — no RPC. Only updates `expr`; see `RemoteNodes::type_filter`
    /// for reasoning.
    pub fn type_filter(&self, node_types: Vec<String>) -> RemotePathFromNode {
        RemotePathFromNode {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: ReadExpr::TypeFilter {
                input: Arc::new(self.expr.clone()),
                node_types: node_types.into(),
            },
            ctx: self.ctx.clone(),
        }
    }

    /// Filter this collection by a node filter. **Propagates** to downstream
    /// traversals from the matching nodes. Mirrors the local
    /// `PathFromNode.filter(FilterExpr)`. Recorded in `ctx` so members
    /// materialized via `.collect()` replay it per handle. Lazy — no RPC.
    pub fn filter(&self, filter: GqlNodeFilter) -> RemotePathFromNode {
        let filter = Arc::new(filter);
        RemotePathFromNode {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: ReadExpr::FilterNodes {
                input: Arc::new(self.expr.clone()),
                filter: filter.clone(),
            },
            ctx: self.ctx.with_op(HandleOp::NodeFilter(filter)),
        }
    }

    /// Narrow this collection's membership by a node filter — applies only at
    /// this step; downstream traversals see the unfiltered graph. Server-only
    /// (`select` has no local `PathFromNode` equivalent). Lazy — no RPC.
    pub fn select(&self, filter: GqlNodeFilter) -> RemotePathFromNode {
        let filter = Arc::new(filter);
        RemotePathFromNode {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: ReadExpr::SelectNodes {
                input: Arc::new(self.expr.clone()),
                filter,
            },
            ctx: self.ctx.clone(),
        }
    }

    /// Traverse one further hop to the neighbours (both directions) of this
    /// path, as a flat `RemotePathFromNode`. Lazy — no RPC.
    pub fn neighbours(&self) -> RemotePathFromNode {
        RemotePathFromNode::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::Neighbours {
                input: Arc::new(self.expr.clone()),
            },
            self.ctx.clone(),
        )
    }

    /// Traverse one further hop to the in-neighbours of this path, as a flat
    /// `RemotePathFromNode`. Lazy — no RPC.
    pub fn in_neighbours(&self) -> RemotePathFromNode {
        RemotePathFromNode::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::InNeighbours {
                input: Arc::new(self.expr.clone()),
            },
            self.ctx.clone(),
        )
    }

    /// Traverse one further hop to the out-neighbours of this path, as a flat
    /// `RemotePathFromNode`. Lazy — no RPC.
    pub fn out_neighbours(&self) -> RemotePathFromNode {
        RemotePathFromNode::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::OutNeighbours {
                input: Arc::new(self.expr.clone()),
            },
            self.ctx.clone(),
        )
    }

    /// Returns the incident edges (both directions) of this path, as a flat
    /// `RemoteEdges` collection. Lazy — no RPC.
    pub fn edges(&self) -> RemoteEdges {
        RemoteEdges::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::NodeEdges {
                input: Arc::new(self.expr.clone()),
            },
            self.ctx.clone(),
        )
    }

    /// Returns the incoming edges of this path, as a flat `RemoteEdges`
    /// collection. Lazy — no RPC.
    pub fn in_edges(&self) -> RemoteEdges {
        RemoteEdges::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::InEdges {
                input: Arc::new(self.expr.clone()),
            },
            self.ctx.clone(),
        )
    }

    /// Returns the outgoing edges of this path, as a flat `RemoteEdges`
    /// collection. Lazy — no RPC.
    pub fn out_edges(&self) -> RemoteEdges {
        RemoteEdges::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::OutEdges {
                input: Arc::new(self.expr.clone()),
            },
            self.ctx.clone(),
        )
    }

    /// Terminal: the list of node ids in this collection. Fires one RPC.
    pub async fn ids(&self) -> Result<Vec<String>, ClientError> {
        let op = Op::Read(ReadExpr::Ids {
            input: Arc::new(self.expr.clone()),
        });
        expect_string_list(self.transport.execute(&op).await?, "ids")
    }

    /// Columnar accessor: each node's id — mirrors the local `PathFromNode.id`.
    /// Fires one RPC.
    pub async fn id(&self) -> Result<Vec<String>, ClientError> {
        let op = Op::Read(ReadExpr::Ids {
            input: Arc::new(self.expr.clone()),
        });
        expect_string_list(self.transport.execute(&op).await?, "id")
    }

    /// Columnar accessor: each node's name — mirrors the local
    /// `PathFromNode.name`. Fires one RPC.
    pub async fn name(&self) -> Result<Vec<String>, ClientError> {
        let op = Op::Read(ReadExpr::CollectionNames {
            input: Arc::new(self.expr.clone()),
        });
        expect_string_list(self.transport.execute(&op).await?, "name")
    }

    /// Columnar accessor: each node's type (`None` when unset) — mirrors the
    /// local `PathFromNode.node_type`. Fires one RPC.
    pub async fn node_type(&self) -> Result<Vec<Option<String>>, ClientError> {
        let op = Op::Read(ReadExpr::CollectionNodeTypes {
            input: Arc::new(self.expr.clone()),
        });
        expect_optional_string_list(self.transport.execute(&op).await?, "nodeType")
    }

    /// Columnar accessor: each node's earliest event time — mirrors the local
    /// `PathFromNode.earliest_time`. Fires one RPC.
    pub async fn earliest_time(&self) -> Result<Vec<Option<RemoteEventTime>>, ClientError> {
        let op = Op::Read(ReadExpr::CollectionEarliestTime {
            input: Arc::new(self.expr.clone()),
        });
        expect_optional_event_time_list(self.transport.execute(&op).await?, "earliestTime")
    }

    /// Columnar accessor: each node's latest event time — mirrors the local
    /// `PathFromNode.latest_time`. Fires one RPC.
    pub async fn latest_time(&self) -> Result<Vec<Option<RemoteEventTime>>, ClientError> {
        let op = Op::Read(ReadExpr::CollectionLatestTime {
            input: Arc::new(self.expr.clone()),
        });
        expect_optional_event_time_list(self.transport.execute(&op).await?, "latestTime")
    }

    /// The non-temporal metadata of this path as a columnar view — mirrors the
    /// local `PathFromNode.metadata`. Lazy — no RPC.
    pub fn metadata(&self) -> RemoteMetadataView {
        RemoteMetadataView::with_expr(
            self.path.clone(),
            self.transport.clone(),
            self.expr.clone(),
            self.ctx.clone(),
            false,
        )
    }

    /// The properties of this path as a columnar view — mirrors the local
    /// `PathFromNode.properties`. Lazy — no RPC.
    pub fn properties(&self) -> RemotePropertiesView {
        RemotePropertiesView::with_expr(
            self.path.clone(),
            self.transport.clone(),
            self.expr.clone(),
            self.ctx.clone(),
            false,
        )
    }

    /// Terminal: the per-node degree (number of incident edges) of every node
    /// in this path, in order — a flat `Vec<i64>`. Fires one RPC.
    pub async fn degree(&self) -> Result<Vec<i64>, ClientError> {
        let op = Op::Read(ReadExpr::CollectionDegree {
            input: Arc::new(self.expr.clone()),
        });
        expect_i64_list(self.transport.execute(&op).await?, "degree")
    }

    /// Terminal: the per-node in-degree of every node in this path, in order —
    /// a flat `Vec<i64>`. Fires one RPC.
    pub async fn in_degree(&self) -> Result<Vec<i64>, ClientError> {
        let op = Op::Read(ReadExpr::CollectionInDegree {
            input: Arc::new(self.expr.clone()),
        });
        expect_i64_list(self.transport.execute(&op).await?, "inDegree")
    }

    /// Terminal: the per-node out-degree of every node in this path, in order —
    /// a flat `Vec<i64>`. Fires one RPC.
    pub async fn out_degree(&self) -> Result<Vec<i64>, ClientError> {
        let op = Op::Read(ReadExpr::CollectionOutDegree {
            input: Arc::new(self.expr.clone()),
        });
        expect_i64_list(self.transport.execute(&op).await?, "outDegree")
    }

    /// Terminal: the per-node count of incident edge updates of every node in
    /// this path, in order — a flat `Vec<i64>`. Fires one RPC.
    pub async fn edge_history_count(&self) -> Result<Vec<i64>, ClientError> {
        let op = Op::Read(ReadExpr::CollectionEdgeHistoryCount {
            input: Arc::new(self.expr.clone()),
        });
        expect_i64_list(self.transport.execute(&op).await?, "edgeHistoryCount")
    }

    /// Terminal: the number of nodes in this collection. Fires one RPC.
    pub async fn count(&self) -> Result<i64, ClientError> {
        let op = Op::Read(ReadExpr::Count {
            input: Arc::new(self.expr.clone()),
        });
        expect_i64(self.transport.execute(&op).await?, "count")
    }

    /// Terminal: whether this view contains a layer named `name`. Fires one RPC.
    pub async fn has_layer(&self, name: impl ToString) -> Result<bool, ClientError> {
        let op = Op::Read(ReadExpr::HasLayer {
            input: Arc::new(self.expr.clone()),
            name: name.to_string(),
        });
        expect_bool(self.transport.execute(&op).await?, "hasLayer")
    }

    /// Terminal: the size of the window covered by this view (`end - start`),
    /// or `None` for an unbounded view. Fires one RPC.
    pub async fn window_size(&self) -> Result<Option<i64>, ClientError> {
        let op = Op::Read(ReadExpr::WindowSize {
            input: Arc::new(self.expr.clone()),
        });
        expect_optional_i64(self.transport.execute(&op).await?, "windowSize")
    }

    /// Returns a single combined event history for all nodes reachable from
    /// the source in this view — a `RemoteHistory` container. Lazy — no RPC.
    pub fn combined_history(&self) -> RemoteHistory {
        RemoteHistory::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::CombinedHistory {
                input: Arc::new(self.expr.clone()),
            },
            self.ctx.clone(),
        )
    }

    /// Terminal: view start bound for this collection — `None` if unbounded.
    /// Fires one RPC.
    pub async fn start(&self) -> Result<Option<RemoteEventTime>, ClientError> {
        let op = Op::Read(ReadExpr::Start {
            input: Arc::new(self.expr.clone()),
        });
        expect_optional_event_time(self.transport.execute(&op).await?, "start")
    }

    /// Terminal: view end bound for this collection — `None` if unbounded.
    /// Fires one RPC.
    pub async fn end(&self) -> Result<Option<RemoteEventTime>, ClientError> {
        let op = Op::Read(ReadExpr::End {
            input: Arc::new(self.expr.clone()),
        });
        expect_optional_event_time(self.transport.execute(&op).await?, "end")
    }

    /// Materialize as `Vec<RemoteNode>`. Fires one RPC. Each returned node
    /// anchors on the parent graph view and replays the collection-level ops
    /// in application order.
    pub async fn collect(&self) -> Result<Vec<RemoteNode>, ClientError> {
        let ids = self.ids().await?;
        Ok(ids
            .into_iter()
            .map(|id| {
                RemoteNode::with_expr(
                    self.path.clone(),
                    id.clone(),
                    self.transport.clone(),
                    self.ctx.node_handle_expr(id),
                    self.ctx.clone(),
                )
            })
            .collect())
    }
}
