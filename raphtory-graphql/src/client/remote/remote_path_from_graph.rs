use crate::{
    client::{
        op::{HandleCtx, HandleOp, InputTime, Op, ReadExpr, ViewOp},
        remote_collection_metadata::{RemoteMetadataView, RemotePropertiesView},
        remote_history::RemoteHistory,
        remote_nested_edges::RemoteNestedEdges,
        remote_node::RemoteNode,
        remote_path_from_node::RemotePathFromNode,
        transport::{
            expect_bool, expect_i64, expect_nested_gid_list, expect_nested_i64_list,
            expect_nested_optional_event_time_list, expect_nested_optional_string_list,
            expect_nested_string_list, expect_optional_event_time, expect_optional_i64,
            expect_string_list, Transport,
        },
        ClientError,
    },
    model::graph::filtering::GqlFilter,
};
use raphtory::errors::GraphError;
use raphtory_api::core::{entities::GID, storage::timeindex::EventTime};
use std::sync::Arc;

/// A handle to a "path from graph" collection on the server — the neighbours
/// reachable one hop from *each* node in a `RemoteNodes` collection, in a
/// given direction. Produced by:
/// - `RemoteNodes::neighbours()` — both directions
/// - `RemoteNodes::in_neighbours()`
/// - `RemoteNodes::out_neighbours()`
///
/// Distinct from `RemotePathFromNode` because it is **nested**: the server
/// type (`GqlPathFromGraph`) groups results per source node. `ids()` returns
/// `Vec<Vec<String>>` (one inner list per source node), `collect()` returns
/// `Vec<Vec<RemoteNode>>`, and `count()` is the number of source paths.
///
/// Structurally identical to `RemotePathFromNode` — same `expr` + `ctx`
/// fields, same view-op wiring — but the terminals return nested shapes.
#[derive(Clone)]
pub struct RemotePathFromGraph {
    pub path: String,
    pub transport: Arc<dyn Transport>,
    pub expr: Arc<ReadExpr>,
    /// Materialization context — see `RemoteNodes` for details.
    pub ctx: HandleCtx,
}

impl RemotePathFromGraph {
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

    fn with_view_op(&self, op: ViewOp) -> RemotePathFromGraph {
        RemotePathFromGraph {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: Arc::new(op.apply(self.expr.clone())),
            ctx: self.ctx.with_op(HandleOp::View(op)),
        }
    }

    /// Time-window this collection. Lazy — no RPC.
    pub fn window(&self, start: InputTime, end: InputTime) -> RemotePathFromGraph {
        self.with_view_op(ViewOp::Window { start, end })
    }

    /// Restrict to a single named layer. Lazy — no RPC.
    pub fn layer(&self, name: impl ToString) -> RemotePathFromGraph {
        self.with_view_op(ViewOp::Layer {
            name: name.to_string(),
        })
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    pub fn at(&self, time: InputTime) -> RemotePathFromGraph {
        self.with_view_op(ViewOp::At { time })
    }

    /// Restrict to events strictly before the given time. Lazy — no RPC.
    pub fn before(&self, time: InputTime) -> RemotePathFromGraph {
        self.with_view_op(ViewOp::Before { time })
    }

    /// Restrict to events strictly after the given time. Lazy — no RPC.
    pub fn after(&self, time: InputTime) -> RemotePathFromGraph {
        self.with_view_op(ViewOp::After { time })
    }

    /// Latest state. Lazy — no RPC.
    pub fn latest(&self) -> RemotePathFromGraph {
        self.with_view_op(ViewOp::Latest)
    }

    /// Snapshot at the latest time. Lazy — no RPC.
    pub fn snapshot_latest(&self) -> RemotePathFromGraph {
        self.with_view_op(ViewOp::SnapshotLatest)
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    pub fn snapshot_at(&self, time: InputTime) -> RemotePathFromGraph {
        self.with_view_op(ViewOp::SnapshotAt { time })
    }

    /// Exclude a specific layer. Lazy — no RPC.
    pub fn exclude_layer(&self, name: impl ToString) -> RemotePathFromGraph {
        self.with_view_op(ViewOp::ExcludeLayer {
            name: name.to_string(),
        })
    }

    /// Shrink the start of the current window. Lazy — no RPC.
    pub fn shrink_start(&self, start: InputTime) -> RemotePathFromGraph {
        self.with_view_op(ViewOp::ShrinkStart { start })
    }

    /// Shrink the end of the current window. Lazy — no RPC.
    pub fn shrink_end(&self, end: InputTime) -> RemotePathFromGraph {
        self.with_view_op(ViewOp::ShrinkEnd { end })
    }

    /// Restrict to the default layer. Lazy — no RPC.
    pub fn default_layer(&self) -> RemotePathFromGraph {
        self.with_view_op(ViewOp::DefaultLayer)
    }

    /// Restrict to the given set of layers. Lazy — no RPC.
    pub fn layers(&self, names: Vec<String>) -> RemotePathFromGraph {
        self.with_view_op(ViewOp::Layers {
            names: names.into(),
        })
    }

    /// Exclude the given set of layers. Lazy — no RPC.
    pub fn exclude_layers(&self, names: Vec<String>) -> RemotePathFromGraph {
        self.with_view_op(ViewOp::ExcludeLayers {
            names: names.into(),
        })
    }

    /// Restrict to the given set of valid layers. Lazy — no RPC.
    pub fn valid_layers(&self, names: Vec<String>) -> RemotePathFromGraph {
        self.with_view_op(ViewOp::ValidLayers {
            names: names.into(),
        })
    }

    /// Exclude a specific valid layer from the view. Lazy — no RPC.
    pub fn exclude_valid_layer(&self, name: impl ToString) -> RemotePathFromGraph {
        self.with_view_op(ViewOp::ExcludeValidLayer {
            name: name.to_string(),
        })
    }

    /// Exclude the given set of valid layers from the view. Lazy — no RPC.
    pub fn exclude_valid_layers(&self, names: Vec<String>) -> RemotePathFromGraph {
        self.with_view_op(ViewOp::ExcludeValidLayers {
            names: names.into(),
        })
    }

    /// Restrict this collection to members whose node type is in the given
    /// list. Filters membership. Lazy — no RPC. Only updates `expr`; see
    /// `RemoteNodes::type_filter` for reasoning.
    pub fn type_filter(&self, node_types: Vec<String>) -> RemotePathFromGraph {
        RemotePathFromGraph {
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
    /// traversals from the matching nodes. Recorded in `ctx` so members
    /// materialized via `.collect()` replay it per handle. Lazy — no RPC.
    pub fn filter(
        &self,
        filter: impl TryInto<GqlFilter, Error = GraphError>,
    ) -> Result<RemotePathFromGraph, ClientError> {
        let filter = Arc::new(filter.try_into()?);
        Ok(RemotePathFromGraph {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: Arc::new(ReadExpr::Filtered {
                input: self.expr.clone(),
                filter: filter.clone(),
            }),
            ctx: self.ctx.with_op(HandleOp::Filter(filter)),
        })
    }

    /// Narrow this collection's membership by a filter expression (node
    /// predicates, graph views, and/or/not combinations — expressions that
    /// test edges are rejected by the server) — applies only at this step;
    /// downstream traversals see the unfiltered graph. Lazy — no RPC.
    pub fn select(
        &self,
        filter: impl TryInto<GqlFilter, Error = GraphError>,
    ) -> Result<RemotePathFromGraph, ClientError> {
        let filter = Arc::new(filter.try_into()?);
        Ok(RemotePathFromGraph {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: Arc::new(ReadExpr::SelectNodes {
                input: self.expr.clone(),
                filter,
            }),
            ctx: self.ctx.clone(),
        })
    }

    /// Traverse one further hop to the neighbours (both directions) of each
    /// source path, as a nested `RemotePathFromGraph`. Lazy — no RPC.
    pub fn neighbours(&self) -> RemotePathFromGraph {
        RemotePathFromGraph::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::Neighbours {
                input: self.expr.clone(),
            },
            self.ctx.clone(),
        )
    }

    /// Traverse one further hop to the in-neighbours of each source path, as a
    /// nested `RemotePathFromGraph`. Lazy — no RPC.
    pub fn in_neighbours(&self) -> RemotePathFromGraph {
        RemotePathFromGraph::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::InNeighbours {
                input: self.expr.clone(),
            },
            self.ctx.clone(),
        )
    }

    /// Traverse one further hop to the out-neighbours of each source path, as a
    /// nested `RemotePathFromGraph`. Lazy — no RPC.
    pub fn out_neighbours(&self) -> RemotePathFromGraph {
        RemotePathFromGraph::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::OutNeighbours {
                input: self.expr.clone(),
            },
            self.ctx.clone(),
        )
    }

    /// Returns the incident edges (both directions) of each source path, as a
    /// nested `RemoteNestedEdges` collection. Lazy — no RPC.
    pub fn edges(&self) -> RemoteNestedEdges {
        RemoteNestedEdges::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::NodeEdges {
                input: self.expr.clone(),
            },
            self.ctx.clone(),
        )
    }

    /// Returns the incoming edges of each source path, as a nested
    /// `RemoteNestedEdges` collection. Lazy — no RPC.
    pub fn in_edges(&self) -> RemoteNestedEdges {
        RemoteNestedEdges::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::InEdges {
                input: self.expr.clone(),
            },
            self.ctx.clone(),
        )
    }

    /// Returns the outgoing edges of each source path, as a nested
    /// `RemoteNestedEdges` collection. Lazy — no RPC.
    pub fn out_edges(&self) -> RemoteNestedEdges {
        RemoteNestedEdges::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::OutEdges {
                input: self.expr.clone(),
            },
            self.ctx.clone(),
        )
    }

    /// Terminal: the ids of the SOURCE nodes these paths hang off — one per
    /// source, aligned with `ids()`' outer index. Fires one RPC.
    pub async fn source_ids(&self) -> Result<Vec<String>, ClientError> {
        let op = Op::Read(ReadExpr::SourceIds {
            input: self.expr.clone(),
        });
        expect_string_list(self.transport.execute(&op).await?, "sourceIds")
    }

    /// Materialize as `(source, path)` pairs — the remote analogue of the local
    /// `PathFromGraph` iteration, which yields the source node alongside that
    /// source's own `PathFromNode`.
    ///
    /// Fires ONE RPC (the source ids). Both halves of each pair are lazy
    /// handles: the source node anchors on the parent graph view like a
    /// `collect()` member, and the path re-derives this collection's own op
    /// chain from that single source (see `HandleCtx::path_handle_expr`), so it
    /// keeps chaining — `path.window(..)`, `path.degree()`, further hops.
    pub async fn pairs(&self) -> Result<Vec<(RemoteNode, RemotePathFromNode)>, ClientError> {
        self.source_ids()
            .await?
            .into_iter()
            .map(|id| {
                let id = GID::Str(id);
                let path_expr = self.ctx.path_handle_expr(&self.expr, &id).ok_or_else(|| {
                    ClientError::InvalidInput(
                        "this collection cannot be re-rooted at a single source node, so \
                         (source, path) pairs are unavailable — use `collect()` instead"
                            .to_string(),
                    )
                })?;
                Ok((
                    RemoteNode::with_expr(
                        self.path.clone(),
                        id.clone(),
                        self.transport.clone(),
                        self.ctx.node_handle_expr(id.clone()),
                        self.ctx.clone(),
                    ),
                    RemotePathFromNode::with_expr(
                        self.path.clone(),
                        self.transport.clone(),
                        path_expr,
                        self.ctx.clone(),
                    ),
                ))
            })
            .collect()
    }

    /// Columnar accessor: each source's neighbour ids — one inner list per
    /// source node. Mirrors the local `PathFromGraph.id`. Fires one RPC.
    pub async fn id(&self) -> Result<Vec<Vec<GID>>, ClientError> {
        let op = Op::Read(ReadExpr::NestedIds {
            input: self.expr.clone(),
        });
        expect_nested_gid_list(self.transport.execute(&op).await?, "id")
    }

    /// Columnar accessor: each source's neighbour names — one inner list per
    /// source node. Mirrors the local `PathFromGraph.name`. Fires one RPC.
    pub async fn name(&self) -> Result<Vec<Vec<String>>, ClientError> {
        let op = Op::Read(ReadExpr::NestedNames {
            input: self.expr.clone(),
        });
        expect_nested_string_list(self.transport.execute(&op).await?, "name")
    }

    /// Columnar accessor: each source's neighbour types (`None` when unset) —
    /// one inner list per source node. Mirrors the local
    /// `PathFromGraph.node_type`. Fires one RPC.
    pub async fn node_type(&self) -> Result<Vec<Vec<Option<String>>>, ClientError> {
        let op = Op::Read(ReadExpr::NestedNodeTypes {
            input: self.expr.clone(),
        });
        expect_nested_optional_string_list(self.transport.execute(&op).await?, "nodeType")
    }

    /// Columnar accessor: the nested per-node earliest event time — one inner
    /// list per source node — mirrors the local `PathFromGraph.earliest_time`.
    /// Fires one RPC.
    pub async fn earliest_time(&self) -> Result<Vec<Vec<Option<EventTime>>>, ClientError> {
        let op = Op::Read(ReadExpr::NestedEarliestTime {
            input: self.expr.clone(),
        });
        expect_nested_optional_event_time_list(self.transport.execute(&op).await?, "earliestTime")
    }

    /// Columnar accessor: the nested per-node latest event time — one inner
    /// list per source node — mirrors the local `PathFromGraph.latest_time`.
    /// Fires one RPC.
    pub async fn latest_time(&self) -> Result<Vec<Vec<Option<EventTime>>>, ClientError> {
        let op = Op::Read(ReadExpr::NestedLatestTime {
            input: self.expr.clone(),
        });
        expect_nested_optional_event_time_list(self.transport.execute(&op).await?, "latestTime")
    }

    /// The non-temporal metadata of this collection as a nested columnar view —
    /// mirrors the local `PathFromGraph.metadata`. Lazy — no RPC.
    pub fn metadata(&self) -> RemoteMetadataView {
        RemoteMetadataView::with_expr(
            self.path.clone(),
            self.transport.clone(),
            self.expr.clone(),
            self.ctx.clone(),
            true,
        )
    }

    /// The properties of this collection as a nested columnar view — mirrors
    /// the local `PathFromGraph.properties`. Lazy — no RPC.
    pub fn properties(&self) -> RemotePropertiesView {
        RemotePropertiesView::with_expr(
            self.path.clone(),
            self.transport.clone(),
            self.expr.clone(),
            self.ctx.clone(),
            true,
        )
    }

    /// Terminal: the nested per-node degree — one inner list per source node,
    /// each holding that source's per-neighbour degrees — `Vec<Vec<i64>>`.
    /// Fires one RPC.
    pub async fn degree(&self) -> Result<Vec<Vec<i64>>, ClientError> {
        let op = Op::Read(ReadExpr::NestedDegree {
            input: self.expr.clone(),
        });
        expect_nested_i64_list(self.transport.execute(&op).await?, "degree")
    }

    /// Terminal: the nested per-node in-degree — one inner list per source
    /// node — `Vec<Vec<i64>>`. Fires one RPC.
    pub async fn in_degree(&self) -> Result<Vec<Vec<i64>>, ClientError> {
        let op = Op::Read(ReadExpr::NestedInDegree {
            input: self.expr.clone(),
        });
        expect_nested_i64_list(self.transport.execute(&op).await?, "inDegree")
    }

    /// Terminal: the nested per-node out-degree — one inner list per source
    /// node — `Vec<Vec<i64>>`. Fires one RPC.
    pub async fn out_degree(&self) -> Result<Vec<Vec<i64>>, ClientError> {
        let op = Op::Read(ReadExpr::NestedOutDegree {
            input: self.expr.clone(),
        });
        expect_nested_i64_list(self.transport.execute(&op).await?, "outDegree")
    }

    /// Terminal: the nested per-node count of incident edge updates — one
    /// inner list per source node — `Vec<Vec<i64>>`. Fires one RPC.
    pub async fn edge_history_count(&self) -> Result<Vec<Vec<i64>>, ClientError> {
        let op = Op::Read(ReadExpr::NestedEdgeHistoryCount {
            input: self.expr.clone(),
        });
        expect_nested_i64_list(self.transport.execute(&op).await?, "edgeHistoryCount")
    }

    /// Terminal: the number of *sources* — the outer length, not the total
    /// neighbour count. Named `len` for that reason, matching the local
    /// `PathFromGraph`, whose `len()` is the same outer count.
    /// Fires one RPC.
    pub async fn len(&self) -> Result<i64, ClientError> {
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

    /// Returns a single combined event history for all nodes in this view —
    /// a `RemoteHistory` container. Lazy — no RPC.
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

    /// Materialize as `Vec<Vec<RemoteNode>>` — one inner list per source node.
    /// Fires one RPC (to fetch the nested ids); each returned node anchors on
    /// the parent graph view and replays the collection-level ops in
    /// application order.
    pub async fn collect(&self) -> Result<Vec<Vec<RemoteNode>>, ClientError> {
        let nested = self.id().await?;
        Ok(nested
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|id| {
                        RemoteNode::with_expr(
                            self.path.clone(),
                            id.clone(),
                            self.transport.clone(),
                            self.ctx.node_handle_expr(id),
                            self.ctx.clone(),
                        )
                    })
                    .collect()
            })
            .collect())
    }
}
