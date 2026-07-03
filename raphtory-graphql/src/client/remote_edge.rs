use crate::client::{
    graphql_transport::GraphqlTransport,
    op::{
        AddEdgeMetadata as AddEdgeMetadataOp, AddEdgeUpdates as AddEdgeUpdatesOp,
        DeleteEdgeAtTime as DeleteEdgeAtTimeOp, Op, UpdateEdgeMetadata as UpdateEdgeMetadataOp,
        WriteOp,
    },
    remote_client::RemoteClient,
    transport::Transport,
    ClientError,
};
use raphtory_api::core::{
    entities::properties::prop::Prop, storage::timeindex::AsTime, utils::time::IntoTime,
};
use std::{collections::HashMap, sync::Arc};

/// A handle to a remote edge on the server.
#[derive(Clone)]
pub struct RemoteEdge {
    pub path: String,
    /// Kept for now — parity with `RemoteGraph`/`RemoteNode` during the write
    /// migration. Removed once no method here uses it directly.
    pub client: RemoteClient,
    pub src: String,
    pub dst: String,
    pub transport: Arc<dyn Transport>,
}

impl RemoteEdge {
    pub fn new(path: String, client: RemoteClient, src: String, dst: String) -> Self {
        let transport: Arc<dyn Transport> = Arc::new(GraphqlTransport::new(client.clone()));
        Self {
            path,
            client,
            src,
            dst,
            transport,
        }
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
