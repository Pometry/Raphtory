use crate::{
    client::{
        op::{EdgePin, Fanout, HandleCtx, HandleOp, InputTime, Op, ReadExpr, ViewOp},
        remote_collection_metadata::{RemoteMetadataView, RemotePropertiesView},
        remote_edge::RemoteEdge,
        remote_history::RemoteEventTime,
        remote_path_from_graph::RemotePathFromGraph,
        transport::{
            expect_bool, expect_double_nested_string_list, expect_i64, expect_nested_bool_list,
            expect_nested_edge_list, expect_nested_exploded_edge_list,
            expect_nested_exploded_layers_edge_list, expect_nested_optional_event_time_list,
            expect_nested_string_list, expect_optional_event_time, expect_optional_i64, Transport,
        },
        ClientError,
    },
    model::graph::filtering::{GqlEdgeFilter, GqlFilter},
};
use raphtory::errors::GraphError;
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
/// Structurally identical to `RemoteEdges` — same `expr` + `ctx` fields,
/// same view-op wiring — but the terminals return nested shapes and there is no
/// `ids()` (edges are identified by `(src, dst)` pairs, not a single string id).
#[derive(Clone)]
pub struct RemoteNestedEdges {
    pub path: String,
    pub transport: Arc<dyn Transport>,
    pub expr: Arc<ReadExpr>,
    /// Materialization context — see `RemoteEdges` for details.
    pub ctx: HandleCtx,
}

impl RemoteNestedEdges {
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

    fn with_view_op(&self, op: ViewOp) -> RemoteNestedEdges {
        RemoteNestedEdges {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: Arc::new(op.apply(self.expr.clone())),
            ctx: self.ctx.with_op(HandleOp::View(op)),
        }
    }

    /// Time-window this collection. Lazy — no RPC.
    pub fn window(&self, start: InputTime, end: InputTime) -> RemoteNestedEdges {
        self.with_view_op(ViewOp::Window { start, end })
    }

    /// Restrict to a single named layer. Lazy — no RPC.
    pub fn layer(&self, name: impl ToString) -> RemoteNestedEdges {
        self.with_view_op(ViewOp::Layer {
            name: name.to_string(),
        })
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    pub fn at(&self, time: InputTime) -> RemoteNestedEdges {
        self.with_view_op(ViewOp::At { time })
    }

    /// Restrict to events strictly before the given time. Lazy — no RPC.
    pub fn before(&self, time: InputTime) -> RemoteNestedEdges {
        self.with_view_op(ViewOp::Before { time })
    }

    /// Restrict to events strictly after the given time. Lazy — no RPC.
    pub fn after(&self, time: InputTime) -> RemoteNestedEdges {
        self.with_view_op(ViewOp::After { time })
    }

    /// Latest state. Lazy — no RPC.
    pub fn latest(&self) -> RemoteNestedEdges {
        self.with_view_op(ViewOp::Latest)
    }

    /// Snapshot at the latest time. Lazy — no RPC.
    pub fn snapshot_latest(&self) -> RemoteNestedEdges {
        self.with_view_op(ViewOp::SnapshotLatest)
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    pub fn snapshot_at(&self, time: InputTime) -> RemoteNestedEdges {
        self.with_view_op(ViewOp::SnapshotAt { time })
    }

    /// Exclude a specific layer. Lazy — no RPC.
    pub fn exclude_layer(&self, name: impl ToString) -> RemoteNestedEdges {
        self.with_view_op(ViewOp::ExcludeLayer {
            name: name.to_string(),
        })
    }

    /// Shrink both start and end of the current window. Lazy — no RPC.
    pub fn shrink_window(&self, start: InputTime, end: InputTime) -> RemoteNestedEdges {
        self.with_view_op(ViewOp::ShrinkWindow { start, end })
    }

    /// Shrink the start of the current window. Lazy — no RPC.
    pub fn shrink_start(&self, start: InputTime) -> RemoteNestedEdges {
        self.with_view_op(ViewOp::ShrinkStart { start })
    }

    /// Shrink the end of the current window. Lazy — no RPC.
    pub fn shrink_end(&self, end: InputTime) -> RemoteNestedEdges {
        self.with_view_op(ViewOp::ShrinkEnd { end })
    }

    /// Restrict to the default layer. Lazy — no RPC.
    pub fn default_layer(&self) -> RemoteNestedEdges {
        self.with_view_op(ViewOp::DefaultLayer)
    }

    /// Restrict to the given set of layers. Lazy — no RPC.
    pub fn layers(&self, names: Vec<String>) -> RemoteNestedEdges {
        self.with_view_op(ViewOp::Layers {
            names: names.into(),
        })
    }

    /// Exclude the given set of layers. Lazy — no RPC.
    pub fn exclude_layers(&self, names: Vec<String>) -> RemoteNestedEdges {
        self.with_view_op(ViewOp::ExcludeLayers {
            names: names.into(),
        })
    }

    /// Restrict to the given set of valid layers. Lazy — no RPC.
    pub fn valid_layers(&self, names: Vec<String>) -> RemoteNestedEdges {
        self.with_view_op(ViewOp::ValidLayers {
            names: names.into(),
        })
    }

    /// Exclude a specific valid layer from the view. Lazy — no RPC.
    pub fn exclude_valid_layer(&self, name: impl ToString) -> RemoteNestedEdges {
        self.with_view_op(ViewOp::ExcludeValidLayer {
            name: name.to_string(),
        })
    }

    /// Exclude the given set of valid layers from the view. Lazy — no RPC.
    pub fn exclude_valid_layers(&self, names: Vec<String>) -> RemoteNestedEdges {
        self.with_view_op(ViewOp::ExcludeValidLayers {
            names: names.into(),
        })
    }

    /// Filter this collection by an edge filter. **Propagates** to downstream
    /// traversals from the matching edges. Recorded in `ctx` so members
    /// materialized via `.collect()` replay it per handle. Lazy — no RPC.
    pub fn filter(
        &self,
        filter: impl TryInto<GqlFilter, Error = GraphError>,
    ) -> Result<RemoteNestedEdges, ClientError> {
        let filter = Arc::new(filter.try_into()?);
        Ok(RemoteNestedEdges {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: Arc::new(ReadExpr::Filtered {
                input: self.expr.clone(),
                filter: filter.clone(),
            }),
            ctx: self.ctx.with_op(HandleOp::Filter(filter)),
        })
    }

    /// Narrow this collection's membership by an edge filter — applies only at
    /// this step; downstream traversals see the unfiltered graph. Lazy — no RPC.
    pub fn select(
        &self,
        filter: impl TryInto<GqlEdgeFilter, Error = GraphError>,
    ) -> Result<RemoteNestedEdges, ClientError> {
        let filter = Arc::new(filter.try_into()?);
        Ok(RemoteNestedEdges {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: Arc::new(ReadExpr::SelectEdges {
                input: self.expr.clone(),
                filter,
            }),
            ctx: self.ctx.clone(),
        })
    }

    /// Fan out each source's edges into one entry per event — returns a new
    /// `RemoteNestedEdges` where every member is a single-event edge instance.
    /// Mirrors the local `NestedEdges.explode`. Records a fanout marker in
    /// `ctx` so `.collect()` pins each member to its event via the server's
    /// `event` field. Lazy — no RPC.
    pub fn explode(&self) -> RemoteNestedEdges {
        RemoteNestedEdges {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: Arc::new(ReadExpr::Explode {
                input: self.expr.clone(),
            }),
            ctx: self.ctx.with_op(HandleOp::Fanout(Fanout::Events)),
        }
    }

    /// Fan out each source's edges into one entry per layer per edge — returns
    /// a new `RemoteNestedEdges`. Mirrors the local `NestedEdges.explode_layers`.
    /// Records a layer fanout marker in `ctx`; `.collect()` pins each member to
    /// its layer via the server's `eventLayer` field, so materialized handles
    /// resolve `layer_name` (with `time` unavailable, matching local). Lazy —
    /// no RPC.
    pub fn explode_layers(&self) -> RemoteNestedEdges {
        RemoteNestedEdges {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: Arc::new(ReadExpr::ExplodeLayers {
                input: self.expr.clone(),
            }),
            ctx: self.ctx.with_op(HandleOp::Fanout(Fanout::Layers)),
        }
    }

    /// The source node of each edge, grouped per source node, as a nested
    /// `RemotePathFromGraph`. Mirrors the local `NestedEdges.src`. Lazy — no
    /// RPC; building the handle only wraps the accumulated expression.
    pub fn src(&self) -> RemotePathFromGraph {
        RemotePathFromGraph::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::Src {
                input: self.expr.clone(),
            },
            self.ctx.clone(),
        )
    }

    /// The destination node of each edge, grouped per source node, as a nested
    /// `RemotePathFromGraph`. Mirrors the local `NestedEdges.dst`. Lazy — no RPC.
    pub fn dst(&self) -> RemotePathFromGraph {
        RemotePathFromGraph::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::Dst {
                input: self.expr.clone(),
            },
            self.ctx.clone(),
        )
    }

    /// The node at the other end of each edge (destination for out-edges,
    /// source for in-edges), grouped per source node, as a nested
    /// `RemotePathFromGraph`. Mirrors the local `NestedEdges.nbr`. Lazy — no RPC.
    pub fn nbr(&self) -> RemotePathFromGraph {
        RemotePathFromGraph::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::Nbr {
                input: self.expr.clone(),
            },
            self.ctx.clone(),
        )
    }

    /// Terminal: the number of source edge collections in this collection.
    /// Fires one RPC.
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

    /// The non-temporal metadata of this collection as a nested columnar view —
    /// mirrors the local `NestedEdges.metadata`. Lazy — no RPC.
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
    /// the local `NestedEdges.properties`. Lazy — no RPC.
    pub fn properties(&self) -> RemotePropertiesView {
        RemotePropertiesView::with_expr(
            self.path.clone(),
            self.transport.clone(),
            self.expr.clone(),
            self.ctx.clone(),
            true,
        )
    }

    /// Columnar accessor: each source's edge `(src, dst)` id pairs — one inner
    /// list per source node. Mirrors the local `NestedEdges.id`. Fires one RPC.
    pub async fn id(&self) -> Result<Vec<Vec<(String, String)>>, ClientError> {
        let op = Op::Read(ReadExpr::NestedEdgesList {
            input: self.expr.clone(),
        });
        expect_nested_edge_list(self.transport.execute(&op).await?, "id")
    }

    /// Columnar accessor: each source's per-edge layer names — one inner list
    /// per source node. Mirrors the local `NestedEdges.layer_names`. Fires one RPC.
    pub async fn layer_names(&self) -> Result<Vec<Vec<Vec<String>>>, ClientError> {
        let op = Op::Read(ReadExpr::NestedLayerNames {
            input: self.expr.clone(),
        });
        expect_double_nested_string_list(self.transport.execute(&op).await?, "layerNames")
    }

    /// Columnar accessor: each source's per-edge single layer name — one inner
    /// list per source node. Only valid on exploded edges; the server raises a
    /// GraphQL error otherwise. Mirrors the local `NestedEdges.layer_name`.
    /// Fires one RPC.
    pub async fn layer_name(&self) -> Result<Vec<Vec<String>>, ClientError> {
        let op = Op::Read(ReadExpr::NestedLayerName {
            input: self.expr.clone(),
        });
        expect_nested_string_list(self.transport.execute(&op).await?, "layerName")
    }

    /// Columnar accessor: each source's per-edge earliest event time — one
    /// inner list per source node. Mirrors the local `NestedEdges.earliest_time`.
    /// Fires one RPC.
    pub async fn earliest_time(&self) -> Result<Vec<Vec<Option<RemoteEventTime>>>, ClientError> {
        let op = Op::Read(ReadExpr::NestedEarliestTime {
            input: self.expr.clone(),
        });
        expect_nested_optional_event_time_list(self.transport.execute(&op).await?, "earliestTime")
    }

    /// Columnar accessor: each source's per-edge latest event time — one inner
    /// list per source node. Mirrors the local `NestedEdges.latest_time`. Fires
    /// one RPC.
    pub async fn latest_time(&self) -> Result<Vec<Vec<Option<RemoteEventTime>>>, ClientError> {
        let op = Op::Read(ReadExpr::NestedLatestTime {
            input: self.expr.clone(),
        });
        expect_nested_optional_event_time_list(self.transport.execute(&op).await?, "latestTime")
    }

    /// Columnar accessor: each source's per-edge event time — one inner list
    /// per source node. Only valid on exploded edges; the server raises a
    /// GraphQL error otherwise. Mirrors the local `NestedEdges.time`. Fires one RPC.
    pub async fn time(&self) -> Result<Vec<Vec<Option<RemoteEventTime>>>, ClientError> {
        let op = Op::Read(ReadExpr::NestedTime {
            input: self.expr.clone(),
        });
        expect_nested_optional_event_time_list(self.transport.execute(&op).await?, "time")
    }

    /// Columnar accessor: whether each edge is active (has an event) in the
    /// current view, grouped per source node — mirrors the local
    /// `NestedEdges.is_active`. Fires one RPC.
    pub async fn is_active(&self) -> Result<Vec<Vec<bool>>, ClientError> {
        let op = Op::Read(ReadExpr::NestedIsActive {
            input: self.expr.clone(),
        });
        expect_nested_bool_list(self.transport.execute(&op).await?, "isActive")
    }

    /// Columnar accessor: whether each edge is valid (not deleted) at the
    /// current time, grouped per source node — mirrors the local
    /// `NestedEdges.is_valid`. Fires one RPC.
    pub async fn is_valid(&self) -> Result<Vec<Vec<bool>>, ClientError> {
        let op = Op::Read(ReadExpr::NestedIsValid {
            input: self.expr.clone(),
        });
        expect_nested_bool_list(self.transport.execute(&op).await?, "isValid")
    }

    /// Columnar accessor: whether each edge has been deleted at the current
    /// time, grouped per source node — mirrors the local
    /// `NestedEdges.is_deleted`. Fires one RPC.
    pub async fn is_deleted(&self) -> Result<Vec<Vec<bool>>, ClientError> {
        let op = Op::Read(ReadExpr::NestedIsDeleted {
            input: self.expr.clone(),
        });
        expect_nested_bool_list(self.transport.execute(&op).await?, "isDeleted")
    }

    /// Columnar accessor: whether each edge is a self-loop (`src == dst`),
    /// grouped per source node — mirrors the local `NestedEdges.is_self_loop`.
    /// Fires one RPC.
    pub async fn is_self_loop(&self) -> Result<Vec<Vec<bool>>, ClientError> {
        let op = Op::Read(ReadExpr::NestedIsSelfLoop {
            input: self.expr.clone(),
        });
        expect_nested_bool_list(self.transport.execute(&op).await?, "isSelfLoop")
    }

    /// Terminal: view start bound for this collection — `None` if unbounded.
    /// Fires one RPC.
    pub async fn start(&self) -> Result<Option<RemoteEventTime>, ClientError> {
        let op = Op::Read(ReadExpr::Start {
            input: self.expr.clone(),
        });
        expect_optional_event_time(self.transport.execute(&op).await?, "start")
    }

    /// Terminal: view end bound for this collection — `None` if unbounded.
    /// Fires one RPC.
    pub async fn end(&self) -> Result<Option<RemoteEventTime>, ClientError> {
        let op = Op::Read(ReadExpr::End {
            input: self.expr.clone(),
        });
        expect_optional_event_time(self.transport.execute(&op).await?, "end")
    }

    /// Materialize as `Vec<Vec<RemoteEdge>>` — one inner list per source node.
    /// Fires one RPC; each returned edge anchors on the parent graph view and
    /// replays the collection-level ops in application order. On an exploded
    /// collection each handle is additionally pinned to its event via the
    /// server's `event` field; on a layer-exploded collection each handle is
    /// pinned to its layer via the server's `eventLayer` field (so
    /// `.layer_name()` resolves and `.time()` is unavailable, matching local).
    pub async fn collect(&self) -> Result<Vec<Vec<RemoteEdge>>, ClientError> {
        match self.ctx.fanout() {
            None => {
                let op = Op::Read(ReadExpr::NestedEdgesList {
                    input: self.expr.clone(),
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
                                    self.ctx.edge_handle_expr(src, dst, None),
                                    self.ctx.clone(),
                                )
                            })
                            .collect()
                    })
                    .collect())
            }
            Some(Fanout::Events) => {
                let op = Op::Read(ReadExpr::NestedExplodedEdgesList {
                    input: self.expr.clone(),
                });
                let nested =
                    expect_nested_exploded_edge_list(self.transport.execute(&op).await?, "list")?;
                Ok(nested
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(|(src, dst, time, event_id, layer)| {
                                RemoteEdge::with_expr(
                                    self.path.clone(),
                                    src.clone(),
                                    dst.clone(),
                                    self.transport.clone(),
                                    self.ctx.edge_handle_expr(
                                        src,
                                        dst,
                                        Some(EdgePin::Event {
                                            time,
                                            event_id: Some(event_id),
                                            layer: Some(layer),
                                        }),
                                    ),
                                    self.ctx.clone(),
                                )
                            })
                            .collect()
                    })
                    .collect())
            }
            Some(Fanout::Layers) => {
                let op = Op::Read(ReadExpr::NestedExplodedLayersEdgesList {
                    input: self.expr.clone(),
                });
                let nested = expect_nested_exploded_layers_edge_list(
                    self.transport.execute(&op).await?,
                    "list",
                )?;
                Ok(nested
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(|(src, dst, layer)| {
                                RemoteEdge::with_expr(
                                    self.path.clone(),
                                    src.clone(),
                                    dst.clone(),
                                    self.transport.clone(),
                                    self.ctx.edge_handle_expr(
                                        src,
                                        dst,
                                        Some(EdgePin::Layer { layer }),
                                    ),
                                    self.ctx.clone(),
                                )
                            })
                            .collect()
                    })
                    .collect())
            }
        }
    }
}
