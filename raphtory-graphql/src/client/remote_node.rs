use crate::client::{
    graphql_transport::GraphqlTransport,
    op::{
        AddNodeMetadata as AddNodeMetadataOp, AddNodeUpdates as AddNodeUpdatesOp, Op, ReadExpr,
        SetNodeType as SetNodeTypeOp, UpdateNodeMetadata as UpdateNodeMetadataOp, WriteOp,
    },
    remote_client::RemoteClient,
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
/// `RemoteGraph`.
#[derive(Clone)]
pub struct RemoteNode {
    pub path: String,
    /// Kept for now — behavior preservation during migration.
    pub client: RemoteClient,
    pub id: String,
    pub transport: Arc<dyn Transport>,
    pub expr: ReadExpr,
}

impl RemoteNode {
    /// Legacy constructor: builds a fresh transport and a minimal `Root → Node`
    /// expression. Use `with_expr` when a parent `RemoteGraph` has already
    /// built up view state.
    pub fn new(path: String, client: RemoteClient, id: String) -> Self {
        let transport: Arc<dyn Transport> = Arc::new(GraphqlTransport::new(client.clone()));
        let expr = ReadExpr::Node {
            input: Box::new(ReadExpr::Root { path: path.clone() }),
            id: id.clone(),
        };
        Self {
            path,
            client,
            id,
            transport,
            expr,
        }
    }

    /// Construct with an explicit transport and pre-built read expression.
    /// Used when a `RemoteGraph` propagates its accumulated view chain into a
    /// child node reference.
    pub fn with_expr(
        path: String,
        client: RemoteClient,
        id: String,
        transport: Arc<dyn Transport>,
        expr: ReadExpr,
    ) -> Self {
        Self {
            path,
            client,
            id,
            transport,
            expr,
        }
    }

    /// Terminal: fires an RPC to evaluate the accumulated expression and
    /// returns the node's degree.
    pub async fn degree(&self) -> Result<i64, ClientError> {
        let op = Op::Read(ReadExpr::Degree {
            input: Box::new(self.expr.clone()),
        });
        match self.transport.execute(&op).await? {
            Some(Prop::I64(n)) => Ok(n),
            _ => Err(ClientError::InvalidResponse(
                "`degree` returned unexpected value type".into(),
            )),
        }
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
