use crate::{
    client::{
        op::{EdgePin, EdgeSortBy, Fanout, HandleCtx, HandleOp, InputTime, Op, ReadExpr, ViewOp},
        remote_collection_metadata::{RemoteMetadataView, RemotePropertiesView},
        remote_edge::RemoteEdge,
        remote_history::RemoteEventTime,
        remote_path_from_node::RemotePathFromNode,
        transport::{
            expect_bool, expect_bool_list, expect_edge_list, expect_exploded_edge_list,
            expect_exploded_layers_edge_list, expect_i64, expect_nested_string_list,
            expect_optional_event_time, expect_optional_event_time_list, expect_optional_i64,
            expect_string_list, Transport,
        },
        ClientError,
    },
    model::graph::filtering::{GqlEdgeFilter, GqlFilter},
};
use raphtory::errors::GraphError;
use std::sync::Arc;

/// A handle to a remote collection of edges on the server.
///
/// Produced by:
/// - `RemoteGraph::edges()` — all edges in the current view.
/// - `RemoteNode::edges()` / `.in_edges()` / `.out_edges()` — the edges
///   incident to a specific node.
///
/// Holds the accumulated read expression (`expr`) so terminals like `.count()`
/// and `.collect()` evaluate under the full view chain built up on the parent,
/// plus a materialization context (`ctx`) recording the parent graph view and
/// the ordered collection-level ops — used by `.collect()` so materialized
/// `RemoteEdge`s evaluate under the same composed view.
///
/// Note: edges are identified by `(src, dst)` pairs — there's no
/// single-string id, so this collection exposes `.count()` and `.collect()`
/// but no `.ids()`.
#[derive(Clone)]
pub struct RemoteEdges {
    pub path: String,
    pub transport: Arc<dyn Transport>,
    pub expr: Arc<ReadExpr>,
    /// Materialization context: the parent graph view plus the ordered
    /// collection-level ops (view ops, filters, explode markers) replayed
    /// per member by `.collect()`.
    pub ctx: HandleCtx,
}

impl RemoteEdges {
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

    /// Internal helper: apply a view op to `expr` (narrowing the collection's
    /// own view) and record it in `ctx` in application order, so members
    /// materialized via `.collect()` replay it at the same position relative
    /// to any filters or explode markers.
    fn with_view_op(&self, op: ViewOp) -> RemoteEdges {
        RemoteEdges {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: Arc::new(op.apply(self.expr.clone())),
            ctx: self.ctx.with_op(HandleOp::View(op)),
        }
    }

    /// Time-window this collection. Lazy — no RPC.
    pub fn window(&self, start: InputTime, end: InputTime) -> RemoteEdges {
        self.with_view_op(ViewOp::Window { start, end })
    }

    /// Restrict to a single named layer. Lazy — no RPC.
    pub fn layer(&self, name: impl ToString) -> RemoteEdges {
        self.with_view_op(ViewOp::Layer {
            name: name.to_string(),
        })
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    pub fn at(&self, time: InputTime) -> RemoteEdges {
        self.with_view_op(ViewOp::At { time })
    }

    /// Restrict to events strictly before the given time. Lazy — no RPC.
    pub fn before(&self, time: InputTime) -> RemoteEdges {
        self.with_view_op(ViewOp::Before { time })
    }

    /// Restrict to events strictly after the given time. Lazy — no RPC.
    pub fn after(&self, time: InputTime) -> RemoteEdges {
        self.with_view_op(ViewOp::After { time })
    }

    /// Latest state. Lazy — no RPC.
    pub fn latest(&self) -> RemoteEdges {
        self.with_view_op(ViewOp::Latest)
    }

    /// Snapshot at the latest time. Lazy — no RPC.
    pub fn snapshot_latest(&self) -> RemoteEdges {
        self.with_view_op(ViewOp::SnapshotLatest)
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    pub fn snapshot_at(&self, time: InputTime) -> RemoteEdges {
        self.with_view_op(ViewOp::SnapshotAt { time })
    }

    /// Exclude a specific layer. Lazy — no RPC.
    pub fn exclude_layer(&self, name: impl ToString) -> RemoteEdges {
        self.with_view_op(ViewOp::ExcludeLayer {
            name: name.to_string(),
        })
    }

    /// Shrink both start and end of the current window. Lazy — no RPC.
    pub fn shrink_window(&self, start: InputTime, end: InputTime) -> RemoteEdges {
        self.with_view_op(ViewOp::ShrinkWindow { start, end })
    }

    /// Shrink the start of the current window. Lazy — no RPC.
    pub fn shrink_start(&self, start: InputTime) -> RemoteEdges {
        self.with_view_op(ViewOp::ShrinkStart { start })
    }

    /// Shrink the end of the current window. Lazy — no RPC.
    pub fn shrink_end(&self, end: InputTime) -> RemoteEdges {
        self.with_view_op(ViewOp::ShrinkEnd { end })
    }

    /// Restrict to the default layer. Lazy — no RPC.
    pub fn default_layer(&self) -> RemoteEdges {
        self.with_view_op(ViewOp::DefaultLayer)
    }

    /// Restrict to the given set of layers. Lazy — no RPC.
    pub fn layers(&self, names: Vec<String>) -> RemoteEdges {
        self.with_view_op(ViewOp::Layers {
            names: names.into(),
        })
    }

    /// Exclude the given set of layers. Lazy — no RPC.
    pub fn exclude_layers(&self, names: Vec<String>) -> RemoteEdges {
        self.with_view_op(ViewOp::ExcludeLayers {
            names: names.into(),
        })
    }

    /// Restrict to the given set of valid layers. Lazy — no RPC.
    pub fn valid_layers(&self, names: Vec<String>) -> RemoteEdges {
        self.with_view_op(ViewOp::ValidLayers {
            names: names.into(),
        })
    }

    /// Exclude a specific valid layer from the view. Lazy — no RPC.
    pub fn exclude_valid_layer(&self, name: impl ToString) -> RemoteEdges {
        self.with_view_op(ViewOp::ExcludeValidLayer {
            name: name.to_string(),
        })
    }

    /// Exclude the given set of valid layers from the view. Lazy — no RPC.
    pub fn exclude_valid_layers(&self, names: Vec<String>) -> RemoteEdges {
        self.with_view_op(ViewOp::ExcludeValidLayers {
            names: names.into(),
        })
    }

    /// Fan out this collection into one entry per event — returns a new
    /// `RemoteEdges` where each member is a single-event edge instance.
    /// Records a fanout marker in `ctx` so `.collect()` pins each member to
    /// its event via the server's `event` field. Lazy — no RPC.
    pub fn explode(&self) -> RemoteEdges {
        RemoteEdges {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: Arc::new(ReadExpr::Explode {
                input: self.expr.clone(),
            }),
            ctx: self.ctx.with_op(HandleOp::Fanout(Fanout::Events)),
        }
    }

    /// Fan out this collection into one entry per layer per edge — returns
    /// a new `RemoteEdges`. Records a layer fanout marker in `ctx`; `.collect()`
    /// pins each member to its layer via the server's `eventLayer` field, so
    /// materialized handles resolve `layer_name` (with `time` unavailable,
    /// matching local). Lazy — no RPC.
    pub fn explode_layers(&self) -> RemoteEdges {
        RemoteEdges {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: Arc::new(ReadExpr::ExplodeLayers {
                input: self.expr.clone(),
            }),
            ctx: self.ctx.with_op(HandleOp::Fanout(Fanout::Layers)),
        }
    }

    /// Reorder this collection by the given sort keys (lexicographic — ties
    /// on the first key break to the second, etc.). Returns a new
    /// `RemoteEdges` handle carrying the sort; the RPC only fires on a
    /// downstream terminal. Lazy — no RPC. `ctx` is unchanged.
    pub fn sorted(&self, sort_bys: Vec<EdgeSortBy>) -> RemoteEdges {
        RemoteEdges {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: Arc::new(ReadExpr::SortedEdges {
                input: self.expr.clone(),
                sort_bys,
            }),
            ctx: self.ctx.clone(),
        }
    }

    /// Filter this collection by a filter expression. **The filter
    /// propagates**: it applies to the current collection's membership
    /// *and* to downstream traversals from the matching edges. For a
    /// narrow-here-only variant, use `.select(...)`. Recorded in `ctx` so
    /// members materialized via `.collect()` replay it per handle (server
    /// field `filter` on `Edge`). Lazy — no RPC.
    pub fn filter(
        &self,
        filter: impl TryInto<GqlFilter, Error = GraphError>,
    ) -> Result<RemoteEdges, ClientError> {
        let filter = Arc::new(filter.try_into()?);
        Ok(RemoteEdges {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: Arc::new(ReadExpr::Filtered {
                input: self.expr.clone(),
                filter: filter.clone(),
            }),
            ctx: self.ctx.with_op(HandleOp::Filter(filter)),
        })
    }

    /// Narrow this collection's membership by a filter expression. Unlike
    /// `.filter()`, the filter applies **only at this step** — downstream
    /// traversals from the matching edges see the unfiltered graph.
    /// Lazy — no RPC.
    pub fn select(
        &self,
        filter: impl TryInto<GqlEdgeFilter, Error = GraphError>,
    ) -> Result<RemoteEdges, ClientError> {
        let filter = Arc::new(filter.try_into()?);
        Ok(RemoteEdges {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: Arc::new(ReadExpr::SelectEdges {
                input: self.expr.clone(),
                filter,
            }),
            ctx: self.ctx.clone(),
        })
    }

    /// The source node of each edge in this collection, as a flat
    /// `RemotePathFromNode`. Mirrors the local `Edges.src`. Lazy — no RPC;
    /// building the handle only wraps the accumulated expression.
    pub fn src(&self) -> RemotePathFromNode {
        RemotePathFromNode::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::Src {
                input: self.expr.clone(),
            },
            self.ctx.clone(),
        )
    }

    /// The destination node of each edge in this collection, as a flat
    /// `RemotePathFromNode`. Mirrors the local `Edges.dst`. Lazy — no RPC.
    pub fn dst(&self) -> RemotePathFromNode {
        RemotePathFromNode::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::Dst {
                input: self.expr.clone(),
            },
            self.ctx.clone(),
        )
    }

    /// The node at the other end of each edge (destination for out-edges,
    /// source for in-edges), as a flat `RemotePathFromNode`. Mirrors the local
    /// `Edges.nbr`. Lazy — no RPC.
    pub fn nbr(&self) -> RemotePathFromNode {
        RemotePathFromNode::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::Nbr {
                input: self.expr.clone(),
            },
            self.ctx.clone(),
        )
    }

    /// Terminal: the number of edges in this collection. Fires one RPC.
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

    /// Columnar accessor: each edge's `(src, dst)` id pair — mirrors the local
    /// `Edges.id`. Fires one RPC.
    pub async fn id(&self) -> Result<Vec<(String, String)>, ClientError> {
        let op = Op::Read(ReadExpr::EdgesList {
            input: self.expr.clone(),
        });
        expect_edge_list(self.transport.execute(&op).await?, "id")
    }

    /// Columnar accessor: each edge's layer names — mirrors the local
    /// `Edges.layer_names`. Fires one RPC.
    pub async fn layer_names(&self) -> Result<Vec<Vec<String>>, ClientError> {
        let op = Op::Read(ReadExpr::CollectionLayerNames {
            input: self.expr.clone(),
        });
        expect_nested_string_list(self.transport.execute(&op).await?, "layerNames")
    }

    /// Columnar accessor: each edge's single layer name — mirrors the local
    /// `Edges.layer_name`. Only valid on exploded edges; the server raises a
    /// GraphQL error otherwise. Fires one RPC.
    pub async fn layer_name(&self) -> Result<Vec<String>, ClientError> {
        let op = Op::Read(ReadExpr::CollectionLayerName {
            input: self.expr.clone(),
        });
        expect_string_list(self.transport.execute(&op).await?, "layerName")
    }

    /// Columnar accessor: each edge's earliest event time — mirrors the local
    /// `Edges.earliest_time`. Fires one RPC.
    pub async fn earliest_time(&self) -> Result<Vec<Option<RemoteEventTime>>, ClientError> {
        let op = Op::Read(ReadExpr::CollectionEarliestTime {
            input: self.expr.clone(),
        });
        expect_optional_event_time_list(self.transport.execute(&op).await?, "earliestTime")
    }

    /// Columnar accessor: each edge's latest event time — mirrors the local
    /// `Edges.latest_time`. Fires one RPC.
    pub async fn latest_time(&self) -> Result<Vec<Option<RemoteEventTime>>, ClientError> {
        let op = Op::Read(ReadExpr::CollectionLatestTime {
            input: self.expr.clone(),
        });
        expect_optional_event_time_list(self.transport.execute(&op).await?, "latestTime")
    }

    /// Columnar accessor: each edge's event time — mirrors the local
    /// `Edges.time`. Only valid on exploded edges; the server raises a GraphQL
    /// error otherwise. Fires one RPC.
    pub async fn time(&self) -> Result<Vec<Option<RemoteEventTime>>, ClientError> {
        let op = Op::Read(ReadExpr::CollectionTime {
            input: self.expr.clone(),
        });
        expect_optional_event_time_list(self.transport.execute(&op).await?, "time")
    }

    /// Columnar accessor: whether each edge is active (has an event) in the
    /// current view — mirrors the local `Edges.is_active`. Fires one RPC.
    pub async fn is_active(&self) -> Result<Vec<bool>, ClientError> {
        let op = Op::Read(ReadExpr::CollectionIsActive {
            input: self.expr.clone(),
        });
        expect_bool_list(self.transport.execute(&op).await?, "isActive")
    }

    /// Columnar accessor: whether each edge is valid (not deleted) at the
    /// current time — mirrors the local `Edges.is_valid`. Fires one RPC.
    pub async fn is_valid(&self) -> Result<Vec<bool>, ClientError> {
        let op = Op::Read(ReadExpr::CollectionIsValid {
            input: self.expr.clone(),
        });
        expect_bool_list(self.transport.execute(&op).await?, "isValid")
    }

    /// Columnar accessor: whether each edge has been deleted at the current
    /// time — mirrors the local `Edges.is_deleted`. Fires one RPC.
    pub async fn is_deleted(&self) -> Result<Vec<bool>, ClientError> {
        let op = Op::Read(ReadExpr::CollectionIsDeleted {
            input: self.expr.clone(),
        });
        expect_bool_list(self.transport.execute(&op).await?, "isDeleted")
    }

    /// Columnar accessor: whether each edge is a self-loop (`src == dst`) —
    /// mirrors the local `Edges.is_self_loop`. Fires one RPC.
    pub async fn is_self_loop(&self) -> Result<Vec<bool>, ClientError> {
        let op = Op::Read(ReadExpr::CollectionIsSelfLoop {
            input: self.expr.clone(),
        });
        expect_bool_list(self.transport.execute(&op).await?, "isSelfLoop")
    }

    /// The non-temporal metadata of this collection as a columnar view —
    /// mirrors the local `Edges.metadata`. Lazy — no RPC.
    pub fn metadata(&self) -> RemoteMetadataView {
        RemoteMetadataView::with_expr(
            self.path.clone(),
            self.transport.clone(),
            self.expr.clone(),
            self.ctx.clone(),
            false,
        )
    }

    /// The properties of this collection as a columnar view — mirrors the local
    /// `Edges.properties`. Lazy — no RPC.
    pub fn properties(&self) -> RemotePropertiesView {
        RemotePropertiesView::with_expr(
            self.path.clone(),
            self.transport.clone(),
            self.expr.clone(),
            self.ctx.clone(),
            false,
        )
    }

    /// Terminal: the size of the window covered by this view (`end - start`),
    /// or `None` for an unbounded view. Fires one RPC.
    pub async fn window_size(&self) -> Result<Option<i64>, ClientError> {
        let op = Op::Read(ReadExpr::WindowSize {
            input: self.expr.clone(),
        });
        expect_optional_i64(self.transport.execute(&op).await?, "windowSize")
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

    /// Materialize this collection as a `Vec<RemoteEdge>`. Fires one RPC;
    /// each returned edge anchors on the parent graph view and replays the
    /// collection-level ops (view ops, filters) in application order — so
    /// terminals on returned edges evaluate under the same composed view as
    /// collection-level reads.
    ///
    /// On an exploded collection the RPC also fetches each member's event
    /// identity (time, event id, layer) and pins the returned handle to that
    /// event via the server's `event` field, so `.time()` / `.layer_name()`
    /// behave like local exploded edges. On a layer-exploded collection
    /// (`explode_layers`) it fetches `(src, dst, layer)` per member and pins
    /// via the server's `eventLayer` field, so `.layer_name()` resolves (and
    /// `.time()` is unavailable, matching local).
    pub async fn collect(&self) -> Result<Vec<RemoteEdge>, ClientError> {
        match self.ctx.fanout() {
            None => {
                let op = Op::Read(ReadExpr::EdgesList {
                    input: self.expr.clone(),
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
                            self.ctx.edge_handle_expr(src, dst, None),
                            self.ctx.clone(),
                        )
                    })
                    .collect())
            }
            Some(Fanout::Events) => {
                let op = Op::Read(ReadExpr::ExplodedEdgesList {
                    input: self.expr.clone(),
                });
                let records =
                    expect_exploded_edge_list(self.transport.execute(&op).await?, "list")?;
                Ok(records
                    .into_iter()
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
                    .collect())
            }
            Some(Fanout::Layers) => {
                let op = Op::Read(ReadExpr::ExplodedLayersEdgesList {
                    input: self.expr.clone(),
                });
                let records =
                    expect_exploded_layers_edge_list(self.transport.execute(&op).await?, "list")?;
                Ok(records
                    .into_iter()
                    .map(|(src, dst, layer)| {
                        RemoteEdge::with_expr(
                            self.path.clone(),
                            src.clone(),
                            dst.clone(),
                            self.transport.clone(),
                            self.ctx
                                .edge_handle_expr(src, dst, Some(EdgePin::Layer { layer })),
                            self.ctx.clone(),
                        )
                    })
                    .collect())
            }
        }
    }
}
