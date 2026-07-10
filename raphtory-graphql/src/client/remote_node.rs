use crate::client::{
    op::{
        AddNodeMetadata as AddNodeMetadataOp, AddNodeUpdates as AddNodeUpdatesOp, Op, ReadExpr,
        SetNodeType as SetNodeTypeOp, UpdateNodeMetadata as UpdateNodeMetadataOp, WriteOp,
    },
    remote_edges::RemoteEdges,
    remote_graph::{
        expect_bool, expect_i64, expect_optional_i64, expect_optional_string, expect_string,
    },
    remote_nodes::RemoteNodes,
    transport::Transport,
    ClientError,
};
use raphtory_api::core::{
    entities::properties::prop::Prop, storage::timeindex::AsTime, utils::time::IntoTime,
};
use std::{collections::HashMap, sync::Arc};

/// A handle to a remote node on the server.
///
/// Holds the accumulated read expression (`expr`) so that terminals like
/// `degree()` evaluate under the full view chain built up on the parent
/// `RemoteGraph`, plus a `base_graph` expression representing the graph view
/// this node lives under — used when navigating to child collections
/// (`neighbours`, etc.) so those children evaluate under the same view chain.
#[derive(Clone)]
pub struct RemoteNode {
    pub path: String,
    pub id: String,
    pub transport: Arc<dyn Transport>,
    pub expr: ReadExpr,
    /// The parent graph view — used by child collections (`neighbours`, etc.)
    /// to correctly rebase materialized descendants under the same view chain.
    pub base_graph: ReadExpr,
}

impl RemoteNode {
    /// Construct with an explicit transport, pre-built read expression, and
    /// parent graph view.
    pub fn with_expr(
        path: String,
        id: String,
        transport: Arc<dyn Transport>,
        expr: ReadExpr,
        base_graph: ReadExpr,
    ) -> Self {
        Self {
            path,
            id,
            transport,
            expr,
            base_graph,
        }
    }

    /// Terminal: node degree (in + out, deduplicated). Fires one RPC.
    pub async fn degree(&self) -> Result<i64, ClientError> {
        let op = Op::Read(ReadExpr::Degree {
            input: Box::new(self.expr.clone()),
        });
        expect_i64(self.transport.execute(&op).await?, "degree")
    }

    /// Terminal: node in-degree. Fires one RPC.
    pub async fn in_degree(&self) -> Result<i64, ClientError> {
        let op = Op::Read(ReadExpr::InDegree {
            input: Box::new(self.expr.clone()),
        });
        expect_i64(self.transport.execute(&op).await?, "inDegree")
    }

    /// Terminal: node out-degree. Fires one RPC.
    pub async fn out_degree(&self) -> Result<i64, ClientError> {
        let op = Op::Read(ReadExpr::OutDegree {
            input: Box::new(self.expr.clone()),
        });
        expect_i64(self.transport.execute(&op).await?, "outDegree")
    }

    /// Terminal: node name. Fires one RPC.
    pub async fn name(&self) -> Result<String, ClientError> {
        let op = Op::Read(ReadExpr::Name {
            input: Box::new(self.expr.clone()),
        });
        expect_string(self.transport.execute(&op).await?, "name")
    }

    /// Terminal: earliest event timestamp on this node under the current view.
    /// Returns `None` if the node has no events in the view. Fires one RPC.
    pub async fn earliest_time(&self) -> Result<Option<i64>, ClientError> {
        let op = Op::Read(ReadExpr::EarliestTime {
            input: Box::new(self.expr.clone()),
        });
        expect_optional_i64(self.transport.execute(&op).await?, "earliestTime")
    }

    /// Terminal: latest event timestamp on this node under the current view.
    /// Fires one RPC.
    pub async fn latest_time(&self) -> Result<Option<i64>, ClientError> {
        let op = Op::Read(ReadExpr::LatestTime {
            input: Box::new(self.expr.clone()),
        });
        expect_optional_i64(self.transport.execute(&op).await?, "latestTime")
    }

    /// Terminal: view start bound as seen by this node. Fires one RPC.
    pub async fn start(&self) -> Result<Option<i64>, ClientError> {
        let op = Op::Read(ReadExpr::Start {
            input: Box::new(self.expr.clone()),
        });
        expect_optional_i64(self.transport.execute(&op).await?, "start")
    }

    /// Terminal: view end bound as seen by this node. Fires one RPC.
    pub async fn end(&self) -> Result<Option<i64>, ClientError> {
        let op = Op::Read(ReadExpr::End {
            input: Box::new(self.expr.clone()),
        });
        expect_optional_i64(self.transport.execute(&op).await?, "end")
    }

    /// Terminal: the node's id (as a string, even if the graph uses int GIDs).
    /// Fires one RPC.
    pub async fn id(&self) -> Result<String, ClientError> {
        let op = Op::Read(ReadExpr::Id {
            input: Box::new(self.expr.clone()),
        });
        expect_string(self.transport.execute(&op).await?, "id")
    }

    /// Terminal: the node's type. `None` if not set. Fires one RPC.
    pub async fn node_type(&self) -> Result<Option<String>, ClientError> {
        let op = Op::Read(ReadExpr::NodeType {
            input: Box::new(self.expr.clone()),
        });
        expect_optional_string(self.transport.execute(&op).await?, "nodeType")
    }

    /// Terminal: whether the node has any events in the current view. Fires one RPC.
    pub async fn is_active(&self) -> Result<bool, ClientError> {
        let op = Op::Read(ReadExpr::IsActive {
            input: Box::new(self.expr.clone()),
        });
        expect_bool(self.transport.execute(&op).await?, "isActive")
    }

    /// Terminal: count of temporal edge events on this node. Fires one RPC.
    pub async fn edge_history_count(&self) -> Result<i64, ClientError> {
        let op = Op::Read(ReadExpr::EdgeHistoryCount {
            input: Box::new(self.expr.clone()),
        });
        expect_i64(self.transport.execute(&op).await?, "edgeHistoryCount")
    }

    /// Terminal: first update timestamp on this node under the current view.
    /// Returns `None` if the node has no updates in the view. Fires one RPC.
    pub async fn first_update(&self) -> Result<Option<i64>, ClientError> {
        let op = Op::Read(ReadExpr::FirstUpdate {
            input: Box::new(self.expr.clone()),
        });
        expect_optional_i64(self.transport.execute(&op).await?, "firstUpdate")
    }

    /// Terminal: last update timestamp on this node under the current view.
    /// Returns `None` if the node has no updates in the view. Fires one RPC.
    pub async fn last_update(&self) -> Result<Option<i64>, ClientError> {
        let op = Op::Read(ReadExpr::LastUpdate {
            input: Box::new(self.expr.clone()),
        });
        expect_optional_i64(self.transport.execute(&op).await?, "lastUpdate")
    }

    /// Returns the collection of this node's neighbours (both directions).
    /// Lazy — no RPC. Propagates the base graph view so materialized nodes
    /// are correctly rebased.
    pub fn neighbours(&self) -> RemoteNodes {
        RemoteNodes::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::Neighbours {
                input: Box::new(self.expr.clone()),
            },
            self.base_graph.clone(),
        )
    }

    /// Returns the collection of this node's in-neighbours. Lazy — no RPC.
    pub fn in_neighbours(&self) -> RemoteNodes {
        RemoteNodes::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::InNeighbours {
                input: Box::new(self.expr.clone()),
            },
            self.base_graph.clone(),
        )
    }

    /// Returns the collection of this node's out-neighbours. Lazy — no RPC.
    pub fn out_neighbours(&self) -> RemoteNodes {
        RemoteNodes::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::OutNeighbours {
                input: Box::new(self.expr.clone()),
            },
            self.base_graph.clone(),
        )
    }

    /// Returns the collection of this node's edges (both directions). Lazy — no RPC.
    /// Propagates the base graph view so materialized edges are correctly rebased.
    pub fn edges(&self) -> RemoteEdges {
        RemoteEdges::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::NodeEdges {
                input: Box::new(self.expr.clone()),
            },
            self.base_graph.clone(),
        )
    }

    /// Returns the collection of this node's incoming edges. Lazy — no RPC.
    pub fn in_edges(&self) -> RemoteEdges {
        RemoteEdges::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::InEdges {
                input: Box::new(self.expr.clone()),
            },
            self.base_graph.clone(),
        )
    }

    /// Returns the collection of this node's outgoing edges. Lazy — no RPC.
    pub fn out_edges(&self) -> RemoteEdges {
        RemoteEdges::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::OutEdges {
                input: Box::new(self.expr.clone()),
            },
            self.base_graph.clone(),
        )
    }

    /// Set the type on the node. This only works if the type has not been previously set.
    pub async fn set_node_type(&self, new_type: String) -> Result<(), ClientError> {
        let op = Op::Write(WriteOp::SetNodeType(SetNodeTypeOp {
            path: self.path.clone(),
            id: self.id.clone(),
            new_type,
        }));
        self.transport.execute(&op).await?;
        Ok(())
    }

    /// Add temporal updates to the node at the specified time.
    pub async fn add_updates<T: IntoTime>(
        &self,
        t: T,
        properties: Option<HashMap<String, Prop>>,
    ) -> Result<(), ClientError> {
        let op = Op::Write(WriteOp::AddNodeUpdates(AddNodeUpdatesOp {
            path: self.path.clone(),
            id: self.id.clone(),
            time: t.into_time().t(),
            properties,
        }));
        self.transport.execute(&op).await?;
        Ok(())
    }

    /// Add metadata to the node (properties that do not change over time).
    pub async fn add_metadata(&self, properties: HashMap<String, Prop>) -> Result<(), ClientError> {
        let op = Op::Write(WriteOp::AddNodeMetadata(AddNodeMetadataOp {
            path: self.path.clone(),
            id: self.id.clone(),
            properties,
        }));
        self.transport.execute(&op).await?;
        Ok(())
    }

    /// Update metadata of the node, overwriting existing values.
    pub async fn update_metadata(
        &self,
        properties: HashMap<String, Prop>,
    ) -> Result<(), ClientError> {
        let op = Op::Write(WriteOp::UpdateNodeMetadata(UpdateNodeMetadataOp {
            path: self.path.clone(),
            id: self.id.clone(),
            properties,
        }));
        self.transport.execute(&op).await?;
        Ok(())
    }
}
