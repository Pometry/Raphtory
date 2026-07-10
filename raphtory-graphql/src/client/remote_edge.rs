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
        let src = it.next().ok_or_else(|| {
            ClientError::InvalidResponse("edge id list missing src".into())
        })?;
        let dst = it.next().ok_or_else(|| {
            ClientError::InvalidResponse("edge id list missing dst".into())
        })?;
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
