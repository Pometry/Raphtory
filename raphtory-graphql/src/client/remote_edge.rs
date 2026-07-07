use crate::client::{
    op::{
        AddEdgeMetadata as AddEdgeMetadataOp, AddEdgeUpdates as AddEdgeUpdatesOp,
        DeleteEdgeAtTime as DeleteEdgeAtTimeOp, Op, ReadExpr,
        UpdateEdgeMetadata as UpdateEdgeMetadataOp, WriteOp,
    },
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
/// `.src()` / `.dst()` compose under the full view chain built up on the
/// parent `RemoteGraph`.
#[derive(Clone)]
pub struct RemoteEdge {
    pub path: String,
    pub src: String,
    pub dst: String,
    pub transport: Arc<dyn Transport>,
    pub expr: ReadExpr,
}

impl RemoteEdge {
    /// Construct with an explicit transport and pre-built read expression.
    /// Used when a `RemoteGraph` propagates its accumulated view chain into a
    /// child edge reference.
    pub fn with_expr(
        path: String,
        src: String,
        dst: String,
        transport: Arc<dyn Transport>,
        expr: ReadExpr,
    ) -> Self {
        Self {
            path,
            src,
            dst,
            transport,
            expr,
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
        )
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
