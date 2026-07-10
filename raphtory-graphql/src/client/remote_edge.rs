use crate::client::{
    op::{
        AddEdgeMetadata as AddEdgeMetadataOp, AddEdgeUpdates as AddEdgeUpdatesOp,
        DeleteEdgeAtTime as DeleteEdgeAtTimeOp, Op, ReadExpr,
        UpdateEdgeMetadata as UpdateEdgeMetadataOp, WriteOp,
    },
    remote_graph::{expect_bool, expect_optional_i64, expect_string, expect_string_list},
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
/// `.src()` / `.dst()` compose under the full view chain, plus a `base_graph`
/// expression representing the parent graph view — used to correctly rebase
/// materialized descendants (e.g. `src().neighbours().list()`).
#[derive(Clone)]
pub struct RemoteEdge {
    pub path: String,
    pub src: String,
    pub dst: String,
    pub transport: Arc<dyn Transport>,
    pub expr: ReadExpr,
    /// The parent graph view.
    pub base_graph: ReadExpr,
}

impl RemoteEdge {
    /// Construct with an explicit transport, pre-built read expression, and
    /// parent graph view.
    pub fn with_expr(
        path: String,
        src: String,
        dst: String,
        transport: Arc<dyn Transport>,
        expr: ReadExpr,
        base_graph: ReadExpr,
    ) -> Self {
        Self {
            path,
            src,
            dst,
            transport,
            expr,
            base_graph,
        }
    }

    /// Internal helper: clone `self` with a new `expr`. Keeps view-op builder
    /// methods as one-liners; `base_graph` is preserved.
    fn with_view(&self, expr: ReadExpr) -> RemoteEdge {
        RemoteEdge {
            path: self.path.clone(),
            src: self.src.clone(),
            dst: self.dst.clone(),
            transport: self.transport.clone(),
            expr,
            base_graph: self.base_graph.clone(),
        }
    }

    /// Time-window this edge. Lazy — no RPC.
    pub fn window(&self, start: i64, end: i64) -> RemoteEdge {
        self.with_view(ReadExpr::Window {
            input: Box::new(self.expr.clone()),
            start,
            end,
        })
    }

    /// Restrict to a single named layer. Lazy — no RPC.
    pub fn layer(&self, name: impl ToString) -> RemoteEdge {
        self.with_view(ReadExpr::Layer {
            input: Box::new(self.expr.clone()),
            name: name.to_string(),
        })
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    pub fn at(&self, time: i64) -> RemoteEdge {
        self.with_view(ReadExpr::At {
            input: Box::new(self.expr.clone()),
            time,
        })
    }

    /// Restrict to events strictly before the given time. Lazy — no RPC.
    pub fn before(&self, time: i64) -> RemoteEdge {
        self.with_view(ReadExpr::Before {
            input: Box::new(self.expr.clone()),
            time,
        })
    }

    /// Restrict to events at or after the given time. Lazy — no RPC.
    pub fn after(&self, time: i64) -> RemoteEdge {
        self.with_view(ReadExpr::After {
            input: Box::new(self.expr.clone()),
            time,
        })
    }

    /// Latest state. Lazy — no RPC.
    pub fn latest(&self) -> RemoteEdge {
        self.with_view(ReadExpr::Latest {
            input: Box::new(self.expr.clone()),
        })
    }

    /// Snapshot at the latest time. Lazy — no RPC.
    pub fn snapshot_latest(&self) -> RemoteEdge {
        self.with_view(ReadExpr::SnapshotLatest {
            input: Box::new(self.expr.clone()),
        })
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    pub fn snapshot_at(&self, time: i64) -> RemoteEdge {
        self.with_view(ReadExpr::SnapshotAt {
            input: Box::new(self.expr.clone()),
            time,
        })
    }

    /// Exclude a specific layer from the view. Lazy — no RPC.
    pub fn exclude_layer(&self, name: impl ToString) -> RemoteEdge {
        self.with_view(ReadExpr::ExcludeLayer {
            input: Box::new(self.expr.clone()),
            name: name.to_string(),
        })
    }

    /// Shrink both start and end of the current window. Lazy — no RPC.
    pub fn shrink_window(&self, start: i64, end: i64) -> RemoteEdge {
        self.with_view(ReadExpr::ShrinkWindow {
            input: Box::new(self.expr.clone()),
            start,
            end,
        })
    }

    /// Shrink the start of the current window. Lazy — no RPC.
    pub fn shrink_start(&self, start: i64) -> RemoteEdge {
        self.with_view(ReadExpr::ShrinkStart {
            input: Box::new(self.expr.clone()),
            start,
        })
    }

    /// Shrink the end of the current window. Lazy — no RPC.
    pub fn shrink_end(&self, end: i64) -> RemoteEdge {
        self.with_view(ReadExpr::ShrinkEnd {
            input: Box::new(self.expr.clone()),
            end,
        })
    }

    /// Restrict to the default layer. Lazy — no RPC.
    pub fn default_layer(&self) -> RemoteEdge {
        self.with_view(ReadExpr::DefaultLayer {
            input: Box::new(self.expr.clone()),
        })
    }

    /// Restrict to the given set of layers. Lazy — no RPC.
    pub fn layers(&self, names: Vec<String>) -> RemoteEdge {
        self.with_view(ReadExpr::Layers {
            input: Box::new(self.expr.clone()),
            names,
        })
    }

    /// Exclude the given set of layers from the view. Lazy — no RPC.
    pub fn exclude_layers(&self, names: Vec<String>) -> RemoteEdge {
        self.with_view(ReadExpr::ExcludeLayers {
            input: Box::new(self.expr.clone()),
            names,
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
                input: Box::new(self.expr.clone()),
            },
            self.base_graph.clone(),
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
                input: Box::new(self.expr.clone()),
            },
            self.base_graph.clone(),
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
                input: Box::new(self.expr.clone()),
            },
            self.base_graph.clone(),
        )
    }

    /// Terminal: earliest event time on this edge under the current view.
    /// Returns `None` if the edge has no events in the view. Fires one RPC.
    pub async fn earliest_time(&self) -> Result<Option<i64>, ClientError> {
        let op = Op::Read(ReadExpr::EarliestTime {
            input: Box::new(self.expr.clone()),
        });
        expect_optional_i64(self.transport.execute(&op).await?, "earliestTime")
    }

    /// Terminal: latest event time on this edge under the current view.
    /// Fires one RPC.
    pub async fn latest_time(&self) -> Result<Option<i64>, ClientError> {
        let op = Op::Read(ReadExpr::LatestTime {
            input: Box::new(self.expr.clone()),
        });
        expect_optional_i64(self.transport.execute(&op).await?, "latestTime")
    }

    /// Terminal: first update timestamp on this edge under the current view.
    /// Fires one RPC.
    pub async fn first_update(&self) -> Result<Option<i64>, ClientError> {
        let op = Op::Read(ReadExpr::FirstUpdate {
            input: Box::new(self.expr.clone()),
        });
        expect_optional_i64(self.transport.execute(&op).await?, "firstUpdate")
    }

    /// Terminal: last update timestamp on this edge under the current view.
    /// Fires one RPC.
    pub async fn last_update(&self) -> Result<Option<i64>, ClientError> {
        let op = Op::Read(ReadExpr::LastUpdate {
            input: Box::new(self.expr.clone()),
        });
        expect_optional_i64(self.transport.execute(&op).await?, "lastUpdate")
    }

    /// Terminal: the specific event time this exploded edge event happened at.
    /// Meaningful primarily on `explode()`'d edge views. Fires one RPC.
    pub async fn time(&self) -> Result<Option<i64>, ClientError> {
        let op = Op::Read(ReadExpr::Time {
            input: Box::new(self.expr.clone()),
        });
        expect_optional_i64(self.transport.execute(&op).await?, "time")
    }

    /// Terminal: view start bound as seen by this edge. Fires one RPC.
    pub async fn start(&self) -> Result<Option<i64>, ClientError> {
        let op = Op::Read(ReadExpr::Start {
            input: Box::new(self.expr.clone()),
        });
        expect_optional_i64(self.transport.execute(&op).await?, "start")
    }

    /// Terminal: view end bound as seen by this edge. Fires one RPC.
    pub async fn end(&self) -> Result<Option<i64>, ClientError> {
        let op = Op::Read(ReadExpr::End {
            input: Box::new(self.expr.clone()),
        });
        expect_optional_i64(self.transport.execute(&op).await?, "end")
    }

    /// Terminal: edge id as `(src, dst)` pair of endpoint ids. Fires one RPC.
    pub async fn id(&self) -> Result<(String, String), ClientError> {
        let op = Op::Read(ReadExpr::EdgeIdPair {
            input: Box::new(self.expr.clone()),
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
            input: Box::new(self.expr.clone()),
        });
        expect_string_list(self.transport.execute(&op).await?, "layerNames")
    }

    /// Terminal: single layer name for a layer-restricted view of this edge.
    /// Server returns an error if the edge isn't scoped to exactly one layer;
    /// that surfaces as `ClientError::GraphQLErrors`. Fires one RPC.
    pub async fn layer_name(&self) -> Result<String, ClientError> {
        let op = Op::Read(ReadExpr::LayerName {
            input: Box::new(self.expr.clone()),
        });
        expect_string(self.transport.execute(&op).await?, "layerName")
    }

    /// Terminal: whether the edge has any events in the current view. Fires one RPC.
    pub async fn is_active(&self) -> Result<bool, ClientError> {
        let op = Op::Read(ReadExpr::IsActive {
            input: Box::new(self.expr.clone()),
        });
        expect_bool(self.transport.execute(&op).await?, "isActive")
    }

    /// Terminal: whether the edge is valid at the current time. Fires one RPC.
    pub async fn is_valid(&self) -> Result<bool, ClientError> {
        let op = Op::Read(ReadExpr::IsValid {
            input: Box::new(self.expr.clone()),
        });
        expect_bool(self.transport.execute(&op).await?, "isValid")
    }

    /// Terminal: whether the edge has been deleted at the current time. Fires one RPC.
    pub async fn is_deleted(&self) -> Result<bool, ClientError> {
        let op = Op::Read(ReadExpr::IsDeleted {
            input: Box::new(self.expr.clone()),
        });
        expect_bool(self.transport.execute(&op).await?, "isDeleted")
    }

    /// Terminal: whether the edge is a self-loop (src == dst). Fires one RPC.
    pub async fn is_self_loop(&self) -> Result<bool, ClientError> {
        let op = Op::Read(ReadExpr::IsSelfLoop {
            input: Box::new(self.expr.clone()),
        });
        expect_bool(self.transport.execute(&op).await?, "isSelfLoop")
    }

    /// Add temporal updates to the edge at the specified time.
    pub async fn add_updates<T: IntoTime>(
        &self,
        t: T,
        properties: Option<HashMap<String, Prop>>,
        layer: Option<String>,
    ) -> Result<(), ClientError> {
        let op = Op::Write(WriteOp::AddEdgeUpdates(AddEdgeUpdatesOp {
            path: self.path.clone(),
            src: self.src.clone(),
            dst: self.dst.clone(),
            time: t.into_time().t(),
            properties,
            layer,
        }));
        self.transport.execute(&op).await?;
        Ok(())
    }

    /// Mark the edge as deleted at the specified time.
    pub async fn delete<T: IntoTime>(
        &self,
        t: T,
        layer: Option<String>,
    ) -> Result<(), ClientError> {
        let op = Op::Write(WriteOp::DeleteEdgeAtTime(DeleteEdgeAtTimeOp {
            path: self.path.clone(),
            src: self.src.clone(),
            dst: self.dst.clone(),
            time: t.into_time().t(),
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
