use super::view_ops::remote_view_ops;
use crate::{
    client::{
        collect_opt_props, collect_props,
        op::{
            AddEdgeMetadata as AddEdgeMetadataOp, AddEdgeUpdates as AddEdgeUpdatesOp,
            DeleteEdgeAtTime as DeleteEdgeAtTimeOp, Fanout, HandleCtx, HandleOp, InputTime, Op,
            ReadExpr, UpdateEdgeMetadata as UpdateEdgeMetadataOp, ViewOp, WriteOp,
        },
        remote_edges::RemoteEdges,
        remote_history::RemoteHistory,
        remote_metadata::{RemoteMetadata, RemoteProperties},
        remote_node::RemoteNode,
        transport::{
            expect_bool, expect_gid_list, expect_optional_event_time, expect_optional_i64,
            expect_string, expect_string_list, Transport,
        },
        ClientError,
    },
    model::graph::filtering::GqlFilter,
};
use raphtory::errors::GraphError;
use raphtory_api::core::{
    entities::{properties::prop::Prop, GID},
    storage::timeindex::EventTime,
    utils::time::TryIntoInputTime,
};
use std::sync::Arc;

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
    pub src: GID,
    pub dst: GID,
    pub transport: Arc<dyn Transport>,
    pub expr: Arc<ReadExpr>,
    /// Materialization context — inherited by descendants so their
    /// `.collect()` handles replay this edge's ops (view ops, filters).
    pub ctx: HandleCtx,
}

impl RemoteEdge {
    /// Construct with an explicit transport, pre-built read expression, and
    /// materialization context.
    pub fn with_expr(
        path: String,
        src: GID,
        dst: GID,
        transport: Arc<dyn Transport>,
        expr: impl Into<Arc<ReadExpr>>,
        ctx: HandleCtx,
    ) -> Self {
        Self {
            path,
            src,
            dst,
            transport,
            expr: expr.into(),
            ctx,
        }
    }

    /// Internal helper: apply a view op to `expr` (narrowing the edge's own
    /// view) and record it in `ctx` so descendants navigated via `.src()` /
    /// `.dst()` / `.nbr()` replay it when materializing handles.
    fn with_view_op(&self, op: ViewOp) -> RemoteEdge {
        RemoteEdge {
            path: self.path.clone(),
            src: self.src.clone(),
            dst: self.dst.clone(),
            transport: self.transport.clone(),
            expr: Arc::new(op.apply(self.expr.clone())),
            ctx: self.ctx.with_op(HandleOp::View(op)),
        }
    }

    /// Return a filtered view of this edge — mirrors the local
    /// `Edge.filter(FilterExpr)`. Wraps `expr` (the server field
    /// `filter(expr:)` on `Edge`) and records the filter in `ctx` so
    /// descendants materialized through this edge replay it. Lazy — no RPC.
    pub fn filter(
        &self,
        filter: impl TryInto<GqlFilter, Error = GraphError>,
    ) -> Result<RemoteEdge, ClientError> {
        let filter = Arc::new(filter.try_into()?);
        Ok(RemoteEdge {
            path: self.path.clone(),
            src: self.src.clone(),
            dst: self.dst.clone(),
            transport: self.transport.clone(),
            expr: Arc::new(ReadExpr::Filtered {
                input: self.expr.clone(),
                filter: filter.clone(),
            }),
            ctx: self.ctx.with_op(HandleOp::Filter(filter)),
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
                input: self.expr.clone(),
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
                input: self.expr.clone(),
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
            GID::Str(String::new()),
            self.transport.clone(),
            ReadExpr::Nbr {
                input: self.expr.clone(),
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
                input: self.expr.clone(),
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
                input: self.expr.clone(),
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
                input: self.expr.clone(),
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
                input: self.expr.clone(),
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
                input: self.expr.clone(),
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
                input: self.expr.clone(),
            },
            self.ctx.with_op(HandleOp::Fanout(Fanout::Layers)),
        )
    }

    /// Terminal: earliest event time on this edge under the current view.
    /// Returns `None` if the edge has no events in the view. Fires one RPC.
    pub async fn earliest_time(&self) -> Result<Option<EventTime>, ClientError> {
        let op = Op::Read(ReadExpr::EarliestTime {
            input: self.expr.clone(),
        });
        expect_optional_event_time(self.transport.execute(&op).await?, "earliestTime")
    }

    /// Terminal: latest event time on this edge under the current view.
    /// Fires one RPC.
    pub async fn latest_time(&self) -> Result<Option<EventTime>, ClientError> {
        let op = Op::Read(ReadExpr::LatestTime {
            input: self.expr.clone(),
        });
        expect_optional_event_time(self.transport.execute(&op).await?, "latestTime")
    }

    /// Terminal: first update timestamp on this edge under the current view.
    /// Fires one RPC.
    pub async fn first_update(&self) -> Result<Option<i64>, ClientError> {
        let op = Op::Read(ReadExpr::FirstUpdate {
            input: self.expr.clone(),
        });
        expect_optional_i64(self.transport.execute(&op).await?, "firstUpdate")
    }

    /// Terminal: last update timestamp on this edge under the current view.
    /// Fires one RPC.
    pub async fn last_update(&self) -> Result<Option<i64>, ClientError> {
        let op = Op::Read(ReadExpr::LastUpdate {
            input: self.expr.clone(),
        });
        expect_optional_i64(self.transport.execute(&op).await?, "lastUpdate")
    }

    /// Terminal: the specific event time this exploded edge event happened at.
    /// Meaningful primarily on `explode()`'d edge views. Fires one RPC.
    pub async fn time(&self) -> Result<Option<EventTime>, ClientError> {
        let op = Op::Read(ReadExpr::Time {
            input: self.expr.clone(),
        });
        expect_optional_event_time(self.transport.execute(&op).await?, "time")
    }

    /// Terminal: view start bound as seen by this edge. Fires one RPC.
    pub async fn start(&self) -> Result<Option<EventTime>, ClientError> {
        let op = Op::Read(ReadExpr::Start {
            input: self.expr.clone(),
        });
        expect_optional_event_time(self.transport.execute(&op).await?, "start")
    }

    /// Terminal: view end bound as seen by this edge. Fires one RPC.
    pub async fn end(&self) -> Result<Option<EventTime>, ClientError> {
        let op = Op::Read(ReadExpr::End {
            input: self.expr.clone(),
        });
        expect_optional_event_time(self.transport.execute(&op).await?, "end")
    }

    /// Terminal: edge id as `(src, dst)` pair of endpoint ids — typed like
    /// the local `.id` (integers on integer-indexed graphs). Fires one RPC.
    pub async fn id(&self) -> Result<(GID, GID), ClientError> {
        let op = Op::Read(ReadExpr::EdgeIdPair {
            input: self.expr.clone(),
        });
        let list = expect_gid_list(self.transport.execute(&op).await?, "id")?;
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
            input: self.expr.clone(),
        });
        expect_string_list(self.transport.execute(&op).await?, "layerNames")
    }

    /// Terminal: single layer name for a layer-restricted view of this edge.
    /// Server returns an error if the edge isn't scoped to exactly one layer;
    /// that surfaces as `ClientError::GraphQLErrors`. Fires one RPC.
    pub async fn layer_name(&self) -> Result<String, ClientError> {
        let op = Op::Read(ReadExpr::LayerName {
            input: self.expr.clone(),
        });
        expect_string(self.transport.execute(&op).await?, "layerName")
    }

    /// Terminal: whether the edge has any events in the current view. Fires one RPC.
    pub async fn is_active(&self) -> Result<bool, ClientError> {
        let op = Op::Read(ReadExpr::IsActive {
            input: self.expr.clone(),
        });
        expect_bool(self.transport.execute(&op).await?, "isActive")
    }

    /// Terminal: whether the edge is valid at the current time. Fires one RPC.
    pub async fn is_valid(&self) -> Result<bool, ClientError> {
        let op = Op::Read(ReadExpr::IsValid {
            input: self.expr.clone(),
        });
        expect_bool(self.transport.execute(&op).await?, "isValid")
    }

    /// Terminal: whether the edge has been deleted at the current time. Fires one RPC.
    pub async fn is_deleted(&self) -> Result<bool, ClientError> {
        let op = Op::Read(ReadExpr::IsDeleted {
            input: self.expr.clone(),
        });
        expect_bool(self.transport.execute(&op).await?, "isDeleted")
    }

    /// Terminal: whether the edge is a self-loop (src == dst). Fires one RPC.
    pub async fn is_self_loop(&self) -> Result<bool, ClientError> {
        let op = Op::Read(ReadExpr::IsSelfLoop {
            input: self.expr.clone(),
        });
        expect_bool(self.transport.execute(&op).await?, "isSelfLoop")
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

    /// Add temporal updates to the edge at the specified time. `event_id` locks
    /// the secondary index; `None` lets the server auto-increment.
    /// Refuse a write on a viewed handle — a write always targets the stored
    /// edge, so accepting one here would silently ignore the view (locally
    /// impossible: view types have no mutation methods). See #2716 for the
    /// structural fix; until then the attempt is loud instead of wrong.
    fn require_base_for_write(&self, what: &str) -> Result<(), ClientError> {
        let is_base = matches!(
            &*self.expr,
            ReadExpr::Edge { input, .. } if matches!(&**input, ReadExpr::Root { .. })
        );
        if is_base {
            Ok(())
        } else {
            Err(ClientError::InvalidInput(format!(
                "{what} applies to the base edge handle — this handle carries a \
                 view, and writes on views are not supported (as with the local \
                 API, where views have no mutation methods)"
            )))
        }
    }

    pub async fn add_updates<
        T: TryIntoInputTime,
        PN: AsRef<str>,
        P: Into<Prop>,
        PII: IntoIterator<Item = (PN, P)>,
    >(
        &self,
        t: T,
        properties: PII,
        layer: Option<String>,
    ) -> Result<(), ClientError> {
        self.require_base_for_write("add_updates")?;
        let op = Op::Write(WriteOp::AddEdgeUpdates(AddEdgeUpdatesOp {
            path: self.path.clone(),
            src: self.src.clone(),
            dst: self.dst.clone(),
            time: t.try_into_input_time()?,
            properties: collect_opt_props(properties),
            layer,
        }));
        self.transport.execute(&op).await?;
        Ok(())
    }

    /// Mark the edge as deleted at the specified time. `event_id` locks the
    /// secondary index; `None` lets the server auto-increment.
    pub async fn delete<T: TryIntoInputTime>(
        &self,
        t: T,
        layer: Option<String>,
    ) -> Result<(), ClientError> {
        self.require_base_for_write("delete")?;
        let op = Op::Write(WriteOp::DeleteEdgeAtTime(DeleteEdgeAtTimeOp {
            path: self.path.clone(),
            src: self.src.clone(),
            dst: self.dst.clone(),
            time: t.try_into_input_time()?,
            layer,
        }));
        self.transport.execute(&op).await?;
        Ok(())
    }

    /// Add metadata to the edge (properties that do not change over time).
    pub async fn add_metadata<PN: AsRef<str>, P: Into<Prop>>(
        &self,
        properties: impl IntoIterator<Item = (PN, P)>,
        layer: Option<String>,
    ) -> Result<(), ClientError> {
        self.require_base_for_write("add_metadata")?;
        let op = Op::Write(WriteOp::AddEdgeMetadata(AddEdgeMetadataOp {
            path: self.path.clone(),
            src: self.src.clone(),
            dst: self.dst.clone(),
            properties: collect_props(properties),
            layer,
        }));
        self.transport.execute(&op).await?;
        Ok(())
    }

    /// Update metadata of the edge, overwriting existing values.
    pub async fn update_metadata<PN: AsRef<str>, P: Into<Prop>>(
        &self,
        properties: impl IntoIterator<Item = (PN, P)>,
        layer: Option<String>,
    ) -> Result<(), ClientError> {
        self.require_base_for_write("update_metadata")?;
        let op = Op::Write(WriteOp::UpdateEdgeMetadata(UpdateEdgeMetadataOp {
            path: self.path.clone(),
            src: self.src.clone(),
            dst: self.dst.clone(),
            properties: collect_props(properties),
            layer,
        }));
        self.transport.execute(&op).await?;
        Ok(())
    }
}

remote_view_ops!(RemoteEdge);
