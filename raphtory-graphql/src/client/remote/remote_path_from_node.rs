use crate::{
    client::{
        op::{HandleCtx, HandleOp, InputTime, Op, ReadExpr, ViewOp},
        remote_collection_metadata::{RemoteMetadataView, RemotePropertiesView},
        remote_edges::RemoteEdges,
        remote_history::RemoteHistory,
        remote_node::RemoteNode,
        transport::{
            expect_bool, expect_i64, expect_i64_list, expect_optional_event_time,
            expect_optional_event_time_list, expect_optional_i64, expect_optional_string_list,
            expect_string_list, Transport,
        },
        ClientError,
    },
    model::graph::filtering::{GqlFilter, GqlNodeFilter},
};
use raphtory::errors::GraphError;
use raphtory_api::core::storage::timeindex::EventTime;
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
    pub expr: Arc<ReadExpr>,
    /// Materialization context — see `RemoteNodes` for details.
    pub ctx: HandleCtx,
}

impl RemotePathFromNode {
    /// Construct with an explicit transport, pre-built read expression, and
    /// materialization context.
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

    fn with_view_op(&self, op: ViewOp) -> RemotePathFromNode {
        RemotePathFromNode {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: Arc::new(op.apply(self.expr.clone())),
            ctx: self.ctx.with_op(HandleOp::View(op)),
        }
    }

    /// Time-window this collection. Lazy — no RPC.
    pub fn window(&self, start: InputTime, end: InputTime) -> RemotePathFromNode {
        self.with_view_op(ViewOp::Window { start, end })
    }

    /// Restrict to a single named layer. Lazy — no RPC.
    pub fn layer(&self, name: impl ToString) -> RemotePathFromNode {
        self.with_view_op(ViewOp::Layer {
            name: name.to_string(),
        })
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    pub fn at(&self, time: InputTime) -> RemotePathFromNode {
        self.with_view_op(ViewOp::At { time })
    }

    /// Restrict to events strictly before the given time. Lazy — no RPC.
    pub fn before(&self, time: InputTime) -> RemotePathFromNode {
        self.with_view_op(ViewOp::Before { time })
    }

    /// Restrict to events strictly after the given time. Lazy — no RPC.
    pub fn after(&self, time: InputTime) -> RemotePathFromNode {
        self.with_view_op(ViewOp::After { time })
    }

    /// Latest state. Lazy — no RPC.
    pub fn latest(&self) -> RemotePathFromNode {
        self.with_view_op(ViewOp::Latest)
    }

    /// Snapshot at the latest time. Lazy — no RPC.
    pub fn snapshot_latest(&self) -> RemotePathFromNode {
        self.with_view_op(ViewOp::SnapshotLatest)
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    pub fn snapshot_at(&self, time: InputTime) -> RemotePathFromNode {
        self.with_view_op(ViewOp::SnapshotAt { time })
    }

    /// Exclude a specific layer. Lazy — no RPC.
    pub fn exclude_layer(&self, name: impl ToString) -> RemotePathFromNode {
        self.with_view_op(ViewOp::ExcludeLayer {
            name: name.to_string(),
        })
    }

    /// Shrink both start and end of the current window. Lazy — no RPC.
    pub fn shrink_window(&self, start: InputTime, end: InputTime) -> RemotePathFromNode {
        self.with_view_op(ViewOp::ShrinkWindow { start, end })
    }

    /// Shrink the start of the current window. Lazy — no RPC.
    pub fn shrink_start(&self, start: InputTime) -> RemotePathFromNode {
        self.with_view_op(ViewOp::ShrinkStart { start })
    }

    /// Shrink the end of the current window. Lazy — no RPC.
    pub fn shrink_end(&self, end: InputTime) -> RemotePathFromNode {
        self.with_view_op(ViewOp::ShrinkEnd { end })
    }

    /// Restrict to the default layer. Lazy — no RPC.
    pub fn default_layer(&self) -> RemotePathFromNode {
        self.with_view_op(ViewOp::DefaultLayer)
    }

    /// Restrict to the given set of layers. Lazy — no RPC.
    pub fn layers(&self, names: Vec<String>) -> RemotePathFromNode {
        self.with_view_op(ViewOp::Layers {
            names: names.into(),
        })
    }

    /// Exclude the given set of layers. Lazy — no RPC.
    pub fn exclude_layers(&self, names: Vec<String>) -> RemotePathFromNode {
        self.with_view_op(ViewOp::ExcludeLayers {
            names: names.into(),
        })
    }

    /// Restrict to the given set of valid layers. Lazy — no RPC.
    pub fn valid_layers(&self, names: Vec<String>) -> RemotePathFromNode {
        self.with_view_op(ViewOp::ValidLayers {
            names: names.into(),
        })
    }

    /// Exclude a specific valid layer from the view. Lazy — no RPC.
    pub fn exclude_valid_layer(&self, name: impl ToString) -> RemotePathFromNode {
        self.with_view_op(ViewOp::ExcludeValidLayer {
            name: name.to_string(),
        })
    }

    /// Exclude the given set of valid layers from the view. Lazy — no RPC.
    pub fn exclude_valid_layers(&self, names: Vec<String>) -> RemotePathFromNode {
        self.with_view_op(ViewOp::ExcludeValidLayers {
            names: names.into(),
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
            expr: Arc::new(ReadExpr::TypeFilter {
                input: self.expr.clone(),
                node_types: node_types.into(),
            }),
            ctx: self.ctx.clone(),
        }
    }

    /// Filter this collection by a node filter. **Propagates** to downstream
    /// traversals from the matching nodes. Mirrors the local
    /// `PathFromNode.filter(FilterExpr)`. Recorded in `ctx` so members
    /// materialized via `.collect()` replay it per handle. Lazy — no RPC.
    pub fn filter(
        &self,
        filter: impl TryInto<GqlFilter, Error = GraphError>,
    ) -> Result<RemotePathFromNode, ClientError> {
        let filter = Arc::new(filter.try_into()?);
        Ok(RemotePathFromNode {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: Arc::new(ReadExpr::Filtered {
                input: self.expr.clone(),
                filter: filter.clone(),
            }),
            ctx: self.ctx.with_op(HandleOp::Filter(filter)),
        })
    }

    /// Narrow this collection's membership by a node filter — applies only at
    /// this step; downstream traversals see the unfiltered graph. Server-only
    /// (`select` has no local `PathFromNode` equivalent). Lazy — no RPC.
    pub fn select(
        &self,
        filter: impl TryInto<GqlNodeFilter, Error = GraphError>,
    ) -> Result<RemotePathFromNode, ClientError> {
        let filter = Arc::new(filter.try_into()?);
        Ok(RemotePathFromNode {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: Arc::new(ReadExpr::SelectNodes {
                input: self.expr.clone(),
                filter,
            }),
            ctx: self.ctx.clone(),
        })
    }

    /// Traverse one further hop to the neighbours (both directions) of this
    /// path, as a flat `RemotePathFromNode`. Lazy — no RPC.
    pub fn neighbours(&self) -> RemotePathFromNode {
        RemotePathFromNode::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::Neighbours {
                input: self.expr.clone(),
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
                input: self.expr.clone(),
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
                input: self.expr.clone(),
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
                input: self.expr.clone(),
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
                input: self.expr.clone(),
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
                input: self.expr.clone(),
            },
            self.ctx.clone(),
        )
    }

    /// Terminal: the list of node ids in this collection. Fires one RPC.
    pub async fn ids(&self) -> Result<Vec<String>, ClientError> {
        let op = Op::Read(ReadExpr::Ids {
            input: self.expr.clone(),
        });
        expect_string_list(self.transport.execute(&op).await?, "ids")
    }

    /// Columnar accessor: each node's id — mirrors the local `PathFromNode.id`.
    /// Fires one RPC.
    pub async fn id(&self) -> Result<Vec<String>, ClientError> {
        let op = Op::Read(ReadExpr::Ids {
            input: self.expr.clone(),
        });
        expect_string_list(self.transport.execute(&op).await?, "id")
    }

    /// Columnar accessor: each node's name — mirrors the local
    /// `PathFromNode.name`. Fires one RPC.
    pub async fn name(&self) -> Result<Vec<String>, ClientError> {
        let op = Op::Read(ReadExpr::CollectionNames {
            input: self.expr.clone(),
        });
        expect_string_list(self.transport.execute(&op).await?, "name")
    }

    /// Columnar accessor: each node's type (`None` when unset) — mirrors the
    /// local `PathFromNode.node_type`. Fires one RPC.
    pub async fn node_type(&self) -> Result<Vec<Option<String>>, ClientError> {
        let op = Op::Read(ReadExpr::CollectionNodeTypes {
            input: self.expr.clone(),
        });
        expect_optional_string_list(self.transport.execute(&op).await?, "nodeType")
    }

    /// Columnar accessor: each node's earliest event time — mirrors the local
    /// `PathFromNode.earliest_time`. Fires one RPC.
    pub async fn earliest_time(&self) -> Result<Vec<Option<EventTime>>, ClientError> {
        let op = Op::Read(ReadExpr::CollectionEarliestTime {
            input: self.expr.clone(),
        });
        expect_optional_event_time_list(self.transport.execute(&op).await?, "earliestTime")
    }

    /// Columnar accessor: each node's latest event time — mirrors the local
    /// `PathFromNode.latest_time`. Fires one RPC.
    pub async fn latest_time(&self) -> Result<Vec<Option<EventTime>>, ClientError> {
        let op = Op::Read(ReadExpr::CollectionLatestTime {
            input: self.expr.clone(),
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
            input: self.expr.clone(),
        });
        expect_i64_list(self.transport.execute(&op).await?, "degree")
    }

    /// Terminal: the per-node in-degree of every node in this path, in order —
    /// a flat `Vec<i64>`. Fires one RPC.
    pub async fn in_degree(&self) -> Result<Vec<i64>, ClientError> {
        let op = Op::Read(ReadExpr::CollectionInDegree {
            input: self.expr.clone(),
        });
        expect_i64_list(self.transport.execute(&op).await?, "inDegree")
    }

    /// Terminal: the per-node out-degree of every node in this path, in order —
    /// a flat `Vec<i64>`. Fires one RPC.
    pub async fn out_degree(&self) -> Result<Vec<i64>, ClientError> {
        let op = Op::Read(ReadExpr::CollectionOutDegree {
            input: self.expr.clone(),
        });
        expect_i64_list(self.transport.execute(&op).await?, "outDegree")
    }

    /// Terminal: the per-node count of incident edge updates of every node in
    /// this path, in order — a flat `Vec<i64>`. Fires one RPC.
    pub async fn edge_history_count(&self) -> Result<Vec<i64>, ClientError> {
        let op = Op::Read(ReadExpr::CollectionEdgeHistoryCount {
            input: self.expr.clone(),
        });
        expect_i64_list(self.transport.execute(&op).await?, "edgeHistoryCount")
    }

    /// Terminal: the number of nodes in this collection. Fires one RPC.
    pub async fn count(&self) -> Result<i64, ClientError> {
        let op = Op::Read(ReadExpr::Count {
            input: self.expr.clone(),
        });
        expect_i64(self.transport.execute(&op).await?, "count")
    }

    /// Terminal: whether this view contains a layer named `name`. Fires one RPC.
    pub async fn has_layer(&self, name: impl ToString) -> Result<bool, ClientError> {
        let op = Op::Read(ReadExpr::HasLayer {
            input: self.expr.clone(),
            name: name.to_string(),
        });
        expect_bool(self.transport.execute(&op).await?, "hasLayer")
    }

    /// Terminal: the size of the window covered by this view (`end - start`),
    /// or `None` for an unbounded view. Fires one RPC.
    pub async fn window_size(&self) -> Result<Option<i64>, ClientError> {
        let op = Op::Read(ReadExpr::WindowSize {
            input: self.expr.clone(),
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
                input: self.expr.clone(),
            },
            self.ctx.clone(),
        )
    }

    /// Terminal: view start bound for this collection — `None` if unbounded.
    /// Fires one RPC.
    pub async fn start(&self) -> Result<Option<EventTime>, ClientError> {
        let op = Op::Read(ReadExpr::Start {
            input: self.expr.clone(),
        });
        expect_optional_event_time(self.transport.execute(&op).await?, "start")
    }

    /// Terminal: view end bound for this collection — `None` if unbounded.
    /// Fires one RPC.
    pub async fn end(&self) -> Result<Option<EventTime>, ClientError> {
        let op = Op::Read(ReadExpr::End {
            input: self.expr.clone(),
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
