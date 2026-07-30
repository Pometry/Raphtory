use crate::client::{
    op::{
        input_time_from_parts, AddEdgeMetadata as AddEdgeMetadataOp,
        AddEdgeUpdates as AddEdgeUpdatesOp, DeleteEdgeAtTime as DeleteEdgeAtTimeOp, Fanout,
        HandleCtx, HandleOp, InputTime, Op, ReadExpr, UpdateEdgeMetadata as UpdateEdgeMetadataOp,
        WriteOp,
    },
    remote_edges::RemoteEdges,
    remote_graph::{
        expect_bool, expect_optional_event_time, expect_optional_i64, expect_string,
        expect_string_list,
    },
    remote_history::{RemoteEventTime, RemoteHistory},
    remote_metadata::{RemoteMetadata, RemoteProperties},
    remote_node::RemoteNode,
    transport::Transport,
    ClientError,
};
use raphtory_api::core::{
    entities::properties::prop::Prop, storage::timeindex::AsTime, utils::time::IntoTime,
};
use std::{collections::HashMap, sync::Arc};

/// A handle to a remote edge on the server.
///
/// Holds the accumulated read expression (`expr`) so that navigations like
/// `.src()` / `.dst()` compose under the full view chain, plus a
/// materialization context (`ctx`) recording the parent graph view and the
/// entity-level ops applied to this handle — inherited by descendants (e.g.
/// `src().neighbours().collect()`) so they replay the same ops.
#[derive(Clone)]
pub struct RemoteEdge {
    pub path: String,
    pub src: String,
    pub dst: String,
    pub transport: Arc<dyn Transport>,
    pub expr: ReadExpr,
    /// Materialization context — inherited by descendants so their
    /// `.collect()` handles replay this edge's ops (view ops, filters).
    pub ctx: HandleCtx,
}

impl RemoteEdge {
    /// Construct with an explicit transport, pre-built read expression, and
    /// materialization context.
    pub fn with_expr(
        path: String,
        src: String,
        dst: String,
        transport: Arc<dyn Transport>,
        expr: ReadExpr,
        ctx: HandleCtx,
    ) -> Self {
        Self {
            path,
            src,
            dst,
            transport,
            expr,
            ctx,
        }
    }

    /// Internal helper: apply a view op to `expr` (narrowing the edge's own
    /// view) and record it in `ctx` so descendants navigated via `.src()` /
    /// `.dst()` / `.nbr()` replay it when materializing handles.
    fn with_view_op<F>(&self, wrap: F) -> RemoteEdge
    where
        F: Fn(ReadExpr) -> ReadExpr + Send + Sync + 'static,
    {
        let wrap = Arc::new(wrap);
        RemoteEdge {
            path: self.path.clone(),
            src: self.src.clone(),
            dst: self.dst.clone(),
            transport: self.transport.clone(),
            expr: wrap(self.expr.clone()),
            ctx: self.ctx.with_op(HandleOp::View(wrap)),
        }
    }

    /// Time-window this edge. Lazy — no RPC.
    pub fn window(&self, start: InputTime, end: InputTime) -> RemoteEdge {
        self.with_view_op(move |input| ReadExpr::Window {
            input: Arc::new(input),
            start,
            end,
        })
    }

    /// Restrict to a single named layer. Lazy — no RPC.
    pub fn layer(&self, name: impl ToString) -> RemoteEdge {
        let name = name.to_string();
        self.with_view_op(move |input| ReadExpr::Layer {
            input: Arc::new(input),
            name: name.clone(),
        })
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    pub fn at(&self, time: InputTime) -> RemoteEdge {
        self.with_view_op(move |input| ReadExpr::At {
            input: Arc::new(input),
            time,
        })
    }

    /// Restrict to events strictly before the given time. Lazy — no RPC.
    pub fn before(&self, time: InputTime) -> RemoteEdge {
        self.with_view_op(move |input| ReadExpr::Before {
            input: Arc::new(input),
            time,
        })
    }

    /// Restrict to events strictly after the given time (exclusive). Lazy — no RPC.
    pub fn after(&self, time: InputTime) -> RemoteEdge {
        self.with_view_op(move |input| ReadExpr::After {
            input: Arc::new(input),
            time,
        })
    }

    /// Latest state. Lazy — no RPC.
    pub fn latest(&self) -> RemoteEdge {
        self.with_view_op(move |input| ReadExpr::Latest {
            input: Arc::new(input),
        })
    }

    /// Snapshot at the latest time. Lazy — no RPC.
    pub fn snapshot_latest(&self) -> RemoteEdge {
        self.with_view_op(move |input| ReadExpr::SnapshotLatest {
            input: Arc::new(input),
        })
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    pub fn snapshot_at(&self, time: InputTime) -> RemoteEdge {
        self.with_view_op(move |input| ReadExpr::SnapshotAt {
            input: Arc::new(input),
            time,
        })
    }

    /// Exclude a specific layer from the view. Lazy — no RPC.
    pub fn exclude_layer(&self, name: impl ToString) -> RemoteEdge {
        let name = name.to_string();
        self.with_view_op(move |input| ReadExpr::ExcludeLayer {
            input: Arc::new(input),
            name: name.clone(),
        })
    }

    /// Shrink both start and end of the current window. Lazy — no RPC.
    pub fn shrink_window(&self, start: InputTime, end: InputTime) -> RemoteEdge {
        self.with_view_op(move |input| ReadExpr::ShrinkWindow {
            input: Arc::new(input),
            start,
            end,
        })
    }

    /// Shrink the start of the current window. Lazy — no RPC.
    pub fn shrink_start(&self, start: InputTime) -> RemoteEdge {
        self.with_view_op(move |input| ReadExpr::ShrinkStart {
            input: Arc::new(input),
            start,
        })
    }

    /// Shrink the end of the current window. Lazy — no RPC.
    pub fn shrink_end(&self, end: InputTime) -> RemoteEdge {
        self.with_view_op(move |input| ReadExpr::ShrinkEnd {
            input: Arc::new(input),
            end,
        })
    }

    /// Restrict to the default layer. Lazy — no RPC.
    pub fn default_layer(&self) -> RemoteEdge {
        self.with_view_op(move |input| ReadExpr::DefaultLayer {
            input: Arc::new(input),
        })
    }

    /// Restrict to the given set of layers. Lazy — no RPC.
    pub fn layers(&self, names: Vec<String>) -> RemoteEdge {
        let names: Arc<[String]> = names.into();
        self.with_view_op(move |input| ReadExpr::Layers {
            input: Arc::new(input),
            names: names.clone(),
        })
    }

    /// Exclude the given set of layers from the view. Lazy — no RPC.
    pub fn exclude_layers(&self, names: Vec<String>) -> RemoteEdge {
        let names: Arc<[String]> = names.into();
        self.with_view_op(move |input| ReadExpr::ExcludeLayers {
            input: Arc::new(input),
            names: names.clone(),
        })
    }

    /// Restrict to the given set of valid layers. Lazy — no RPC.
    pub fn valid_layers(&self, names: Vec<String>) -> RemoteEdge {
        let names: Arc<[String]> = names.into();
        self.with_view_op(move |input| ReadExpr::ValidLayers {
            input: Arc::new(input),
            names: names.clone(),
        })
    }

    /// Exclude a specific valid layer from the view. Lazy — no RPC.
    pub fn exclude_valid_layer(&self, name: impl ToString) -> RemoteEdge {
        let name = name.to_string();
        self.with_view_op(move |input| ReadExpr::ExcludeValidLayer {
            input: Arc::new(input),
            name: name.clone(),
        })
    }

    /// Exclude the given set of valid layers from the view. Lazy — no RPC.
    pub fn exclude_valid_layers(&self, names: Vec<String>) -> RemoteEdge {
        let names: Arc<[String]> = names.into();
        self.with_view_op(move |input| ReadExpr::ExcludeValidLayers {
            input: Arc::new(input),
            names: names.clone(),
        })
    }

    /// Navigate to the edge's source node, carrying the view chain forward.
    /// Lazy — builds up the read expression, no RPC.
    pub fn src(&self) -> RemoteNode {
        RemoteNode::with_expr(
            self.path.clone(),
            self.src.clone(),
            self.transport.clone(),
            ReadExpr::Src {
                input: Arc::new(self.expr.clone()),
            },
            self.ctx.clone(),
        )
    }

    /// Navigate to the edge's destination node, carrying the view chain forward.
    /// Lazy — builds up the read expression, no RPC.
    pub fn dst(&self) -> RemoteNode {
        RemoteNode::with_expr(
            self.path.clone(),
            self.dst.clone(),
            self.transport.clone(),
            ReadExpr::Dst {
                input: Arc::new(self.expr.clone()),
            },
            self.ctx.clone(),
        )
    }

    /// Navigate to the "other end" node — destination on an out-edge, source
    /// on an in-edge. Lazy — no RPC. The resulting `RemoteNode`'s cached id
    /// is empty because it's context-sensitive; call `.id()` or `.name()` on
    /// the returned handle to resolve it via one RPC.
    pub fn nbr(&self) -> RemoteNode {
        RemoteNode::with_expr(
            self.path.clone(),
            String::new(),
            self.transport.clone(),
            ReadExpr::Nbr {
                input: Arc::new(self.expr.clone()),
            },
            self.ctx.clone(),
        )
    }

    /// Returns the metadata container of this edge — non-temporal
    /// properties whose values don't depend on time. Lazy — no RPC.
    pub fn metadata(&self) -> RemoteMetadata {
        RemoteMetadata::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::Metadata {
                input: Arc::new(self.expr.clone()),
            },
            self.ctx.clone(),
        )
    }

    /// Returns the full properties container of this edge — includes both
    /// temporal properties and metadata. Lazy — no RPC.
    pub fn properties(&self) -> RemoteProperties {
        RemoteProperties::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::Properties {
                input: Arc::new(self.expr.clone()),
            },
            self.ctx.clone(),
        )
    }

    /// Returns the event history of this edge — a `RemoteHistory` container
    /// with terminals like `.count()`, `.collect()`, `.earliest_time()`, and
    /// sub-container accessors. Lazy — no RPC.
    pub fn history(&self) -> RemoteHistory {
        RemoteHistory::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::History {
                input: Arc::new(self.expr.clone()),
            },
            self.ctx.clone(),
        )
    }

    /// Returns the deletion history of this edge — a `RemoteHistory`
    /// container tracking the times at which the edge was marked deleted.
    /// Distinct from `.history()` which tracks all events. Lazy — no RPC.
    pub fn deletions(&self) -> RemoteHistory {
        RemoteHistory::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::Deletions {
                input: Arc::new(self.expr.clone()),
            },
            self.ctx.clone(),
        )
    }

    /// Fan out this edge into one entry per event — returns a `RemoteEdges`
    /// collection where each member is a single-event edge instance. The
    /// returned collection carries a fanout marker so its `.collect()` pins
    /// each member to its event. Lazy — no RPC.
    pub fn explode(&self) -> RemoteEdges {
        RemoteEdges::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::Explode {
                input: Arc::new(self.expr.clone()),
            },
            self.ctx.with_op(HandleOp::Fanout(Fanout::Events)),
        )
    }

    /// Fan out this edge into one entry per layer — returns a `RemoteEdges`
    /// collection where each member is a single-layer edge instance.
    /// `.collect()` materializes each layer-pinned member (via the server's
    /// `eventLayer` field), and columnar accessors work as usual. Lazy — no RPC.
    pub fn explode_layers(&self) -> RemoteEdges {
        RemoteEdges::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::ExplodeLayers {
                input: Arc::new(self.expr.clone()),
            },
            self.ctx.with_op(HandleOp::Fanout(Fanout::Layers)),
        )
    }

    /// Terminal: earliest event time on this edge under the current view.
    /// Returns `None` if the edge has no events in the view. Fires one RPC.
    pub async fn earliest_time(&self) -> Result<Option<RemoteEventTime>, ClientError> {
        let op = Op::Read(ReadExpr::EarliestTime {
            input: Arc::new(self.expr.clone()),
        });
        expect_optional_event_time(self.transport.execute(&op).await?, "earliestTime")
    }

    /// Terminal: latest event time on this edge under the current view.
    /// Fires one RPC.
    pub async fn latest_time(&self) -> Result<Option<RemoteEventTime>, ClientError> {
        let op = Op::Read(ReadExpr::LatestTime {
            input: Arc::new(self.expr.clone()),
        });
        expect_optional_event_time(self.transport.execute(&op).await?, "latestTime")
    }

    /// Terminal: first update timestamp on this edge under the current view.
    /// Fires one RPC.
    pub async fn first_update(&self) -> Result<Option<i64>, ClientError> {
        let op = Op::Read(ReadExpr::FirstUpdate {
            input: Arc::new(self.expr.clone()),
        });
        expect_optional_i64(self.transport.execute(&op).await?, "firstUpdate")
    }

    /// Terminal: last update timestamp on this edge under the current view.
    /// Fires one RPC.
    pub async fn last_update(&self) -> Result<Option<i64>, ClientError> {
        let op = Op::Read(ReadExpr::LastUpdate {
            input: Arc::new(self.expr.clone()),
        });
        expect_optional_i64(self.transport.execute(&op).await?, "lastUpdate")
    }

    /// Terminal: the specific event time this exploded edge event happened at.
    /// Meaningful primarily on `explode()`'d edge views. Fires one RPC.
    pub async fn time(&self) -> Result<Option<RemoteEventTime>, ClientError> {
        let op = Op::Read(ReadExpr::Time {
            input: Arc::new(self.expr.clone()),
        });
        expect_optional_event_time(self.transport.execute(&op).await?, "time")
    }

    /// Terminal: view start bound as seen by this edge. Fires one RPC.
    pub async fn start(&self) -> Result<Option<RemoteEventTime>, ClientError> {
        let op = Op::Read(ReadExpr::Start {
            input: Arc::new(self.expr.clone()),
        });
        expect_optional_event_time(self.transport.execute(&op).await?, "start")
    }

    /// Terminal: view end bound as seen by this edge. Fires one RPC.
    pub async fn end(&self) -> Result<Option<RemoteEventTime>, ClientError> {
        let op = Op::Read(ReadExpr::End {
            input: Arc::new(self.expr.clone()),
        });
        expect_optional_event_time(self.transport.execute(&op).await?, "end")
    }

    /// Terminal: edge id as `(src, dst)` pair of endpoint ids. Fires one RPC.
    pub async fn id(&self) -> Result<(String, String), ClientError> {
        let op = Op::Read(ReadExpr::EdgeIdPair {
            input: Arc::new(self.expr.clone()),
        });
        let list = expect_string_list(self.transport.execute(&op).await?, "id")?;
        let mut it = list.into_iter();
        let src = it
            .next()
            .ok_or_else(|| ClientError::InvalidResponse("edge id list missing src".into()))?;
        let dst = it
            .next()
            .ok_or_else(|| ClientError::InvalidResponse("edge id list missing dst".into()))?;
        Ok((src, dst))
    }

    /// Terminal: layer names this edge is present in. Fires one RPC.
    pub async fn layer_names(&self) -> Result<Vec<String>, ClientError> {
        let op = Op::Read(ReadExpr::LayerNames {
            input: Arc::new(self.expr.clone()),
        });
        expect_string_list(self.transport.execute(&op).await?, "layerNames")
    }

    /// Terminal: single layer name for a layer-restricted view of this edge.
    /// Server returns an error if the edge isn't scoped to exactly one layer;
    /// that surfaces as `ClientError::GraphQLErrors`. Fires one RPC.
    pub async fn layer_name(&self) -> Result<String, ClientError> {
        let op = Op::Read(ReadExpr::LayerName {
            input: Arc::new(self.expr.clone()),
        });
        expect_string(self.transport.execute(&op).await?, "layerName")
    }

    /// Terminal: whether the edge has any events in the current view. Fires one RPC.
    pub async fn is_active(&self) -> Result<bool, ClientError> {
        let op = Op::Read(ReadExpr::IsActive {
            input: Arc::new(self.expr.clone()),
        });
        expect_bool(self.transport.execute(&op).await?, "isActive")
    }

    /// Terminal: whether the edge is valid at the current time. Fires one RPC.
    pub async fn is_valid(&self) -> Result<bool, ClientError> {
        let op = Op::Read(ReadExpr::IsValid {
            input: Arc::new(self.expr.clone()),
        });
        expect_bool(self.transport.execute(&op).await?, "isValid")
    }

    /// Terminal: whether the edge has been deleted at the current time. Fires one RPC.
    pub async fn is_deleted(&self) -> Result<bool, ClientError> {
        let op = Op::Read(ReadExpr::IsDeleted {
            input: Arc::new(self.expr.clone()),
        });
        expect_bool(self.transport.execute(&op).await?, "isDeleted")
    }

    /// Terminal: whether the edge is a self-loop (src == dst). Fires one RPC.
    pub async fn is_self_loop(&self) -> Result<bool, ClientError> {
        let op = Op::Read(ReadExpr::IsSelfLoop {
            input: Arc::new(self.expr.clone()),
        });
        expect_bool(self.transport.execute(&op).await?, "isSelfLoop")
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

    /// Add temporal updates to the edge at the specified time. `event_id` locks
    /// the secondary index; `None` lets the server auto-increment.
    pub async fn add_updates<T: IntoTime>(
        &self,
        t: T,
        properties: Option<HashMap<String, Prop>>,
        layer: Option<String>,
        event_id: Option<usize>,
    ) -> Result<(), ClientError> {
        let op = Op::Write(WriteOp::AddEdgeUpdates(AddEdgeUpdatesOp {
            path: self.path.clone(),
            src: self.src.clone(),
            dst: self.dst.clone(),
            time: input_time_from_parts(t.into_time().t(), event_id),
            properties,
            layer,
        }));
        self.transport.execute(&op).await?;
        Ok(())
    }

    /// Mark the edge as deleted at the specified time. `event_id` locks the
    /// secondary index; `None` lets the server auto-increment.
    pub async fn delete<T: IntoTime>(
        &self,
        t: T,
        layer: Option<String>,
        event_id: Option<usize>,
    ) -> Result<(), ClientError> {
        let op = Op::Write(WriteOp::DeleteEdgeAtTime(DeleteEdgeAtTimeOp {
            path: self.path.clone(),
            src: self.src.clone(),
            dst: self.dst.clone(),
            time: input_time_from_parts(t.into_time().t(), event_id),
            layer,
        }));
        self.transport.execute(&op).await?;
        Ok(())
    }

    /// Add metadata to the edge (properties that do not change over time).
    pub async fn add_metadata(
        &self,
        properties: HashMap<String, Prop>,
        layer: Option<String>,
    ) -> Result<(), ClientError> {
        let op = Op::Write(WriteOp::AddEdgeMetadata(AddEdgeMetadataOp {
            path: self.path.clone(),
            src: self.src.clone(),
            dst: self.dst.clone(),
            properties,
            layer,
        }));
        self.transport.execute(&op).await?;
        Ok(())
    }

    /// Update metadata of the edge, overwriting existing values.
    pub async fn update_metadata(
        &self,
        properties: HashMap<String, Prop>,
        layer: Option<String>,
    ) -> Result<(), ClientError> {
        let op = Op::Write(WriteOp::UpdateEdgeMetadata(UpdateEdgeMetadataOp {
            path: self.path.clone(),
            src: self.src.clone(),
            dst: self.dst.clone(),
            properties,
            layer,
        }));
        self.transport.execute(&op).await?;
        Ok(())
    }
}
