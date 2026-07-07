use crate::client::{
    graphql_transport::GraphqlTransport,
    op::{
        AddEdge as AddEdgeOp, AddGraphMetadata as AddGraphMetadataOp,
        AddGraphProperty as AddGraphPropertyOp, AddNode as AddNodeOp, CreateNode as CreateNodeOp,
        DeleteEdge as DeleteEdgeOp, Op, ReadExpr, UpdateGraphMetadata as UpdateGraphMetadataOp,
        WriteOp,
    },
    remote_client::RemoteClient,
    remote_edge::RemoteEdge,
    remote_node::RemoteNode,
    transport::Transport,
    ClientError,
};
use minijinja::{Environment, Value};
use raphtory_api::core::{
    entities::{properties::prop::Prop, GID},
    storage::timeindex::{AsTime, EventTime},
    utils::time::IntoTime,
};
use std::{collections::HashMap, sync::Arc};

pub fn build_query(template: &str, context: Value) -> Result<String, ClientError> {
    let mut env = Environment::new();
    env.add_template("template", template)
        .map_err(|e| ClientError::JinjaError(e.to_string()))?;
    let query = env
        .get_template("template")
        .map_err(|e| ClientError::JinjaError(e.to_string()))?
        .render(context)
        .map_err(|e| ClientError::JinjaError(e.to_string()))?;
    Ok(query)
}

/// A handle to a remote graph on the server.
///
/// Holds an accumulating `ReadExpr` for lazy view construction — `.window()`,
/// `.node()` etc. append to it without firing an RPC. Terminals on the child
/// types (e.g. `RemoteNode::degree`) evaluate the accumulated expression via
/// the transport.
#[derive(Clone)]
pub struct RemoteGraph {
    pub path: String,
    pub transport: Arc<dyn Transport>,
    /// The read expression built so far. Starts as `Root { path }`.
    pub expr: ReadExpr,
}

impl RemoteGraph {
    pub fn new(path: String, client: RemoteClient) -> Self {
        let transport: Arc<dyn Transport> = Arc::new(GraphqlTransport::new(client));
        let expr = ReadExpr::Root { path: path.clone() };
        Self {
            path,
            transport,
            expr,
        }
    }

    /// Time-window the graph. Lazy — builds up the read expression, no RPC.
    pub fn window(&self, start: i64, end: i64) -> RemoteGraph {
        RemoteGraph {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: ReadExpr::Window {
                input: Box::new(self.expr.clone()),
                start,
                end,
            },
        }
    }

    /// Returns a remote node reference for the given node id.
    /// Carries the built-up read expression forward, so subsequent terminals
    /// (e.g. `degree()`) evaluate under the same view chain.
    pub fn node(&self, id: impl ToString) -> RemoteNode {
        let id_str = id.to_string();
        RemoteNode::with_expr(
            self.path.clone(),
            id_str.clone(),
            self.transport.clone(),
            ReadExpr::Node {
                input: Box::new(self.expr.clone()),
                id: id_str,
            },
        )
    }

    /// Returns a remote edge reference for the given source and destination node ids.
    pub fn edge(&self, src: impl ToString, dst: impl ToString) -> RemoteEdge {
        RemoteEdge::new(
            self.path.clone(),
            self.transport.clone(),
            src.to_string(),
            dst.to_string(),
        )
    }

    pub async fn add_node<G: Into<GID> + ToString, T: IntoTime>(
        &self,
        timestamp: T,
        id: G,
        properties: Option<HashMap<String, Prop>>,
        node_type: Option<String>,
        layer: Option<String>,
    ) -> Result<RemoteNode, ClientError> {
        let id_str = id.to_string();
        let op = Op::Write(WriteOp::AddNode(AddNodeOp {
            path: self.path.clone(),
            time: timestamp.into_time().t(),
            id: id_str.clone(),
            properties,
            node_type,
            layer,
        }));
        self.transport.execute(&op).await?;
        Ok(RemoteNode::with_expr(
            self.path.clone(),
            id_str.clone(),
            self.transport.clone(),
            ReadExpr::Node {
                input: Box::new(self.expr.clone()),
                id: id_str,
            },
        ))
    }

    /// Create a new node (fails if the node already exists). Uses the createNode mutation.
    pub async fn create_node<G: Into<GID> + ToString, T: IntoTime>(
        &self,
        timestamp: T,
        id: G,
        properties: Option<HashMap<String, Prop>>,
        node_type: Option<String>,
    ) -> Result<RemoteNode, ClientError> {
        let id_str = id.to_string();
        let op = Op::Write(WriteOp::CreateNode(CreateNodeOp {
            path: self.path.clone(),
            time: timestamp.into_time().t(),
            id: id_str.clone(),
            properties,
            node_type,
        }));
        self.transport.execute(&op).await?;
        Ok(RemoteNode::with_expr(
            self.path.clone(),
            id_str.clone(),
            self.transport.clone(),
            ReadExpr::Node {
                input: Box::new(self.expr.clone()),
                id: id_str,
            },
        ))
    }

    pub async fn add_edge<G: Into<GID> + ToString, T: IntoTime>(
        &self,
        timestamp: T,
        src: G,
        dst: G,
        properties: Option<HashMap<String, Prop>>,
        layer: Option<String>,
    ) -> Result<RemoteEdge, ClientError> {
        let src_str = src.to_string();
        let dst_str = dst.to_string();
        let op = Op::Write(WriteOp::AddEdge(AddEdgeOp {
            path: self.path.clone(),
            time: timestamp.into_time().t(),
            src: src_str.clone(),
            dst: dst_str.clone(),
            properties,
            layer,
        }));
        self.transport.execute(&op).await?;
        Ok(RemoteEdge::new(
            self.path.clone(),
            self.transport.clone(),
            src_str,
            dst_str,
        ))
    }

    pub async fn add_property(
        &self,
        timestamp: EventTime,
        properties: HashMap<String, Prop>,
    ) -> Result<(), ClientError> {
        let op = Op::Write(WriteOp::AddGraphProperty(AddGraphPropertyOp {
            path: self.path.clone(),
            time: timestamp.into_time().t(),
            properties,
        }));
        self.transport.execute(&op).await?;
        Ok(())
    }

    pub async fn add_metadata(&self, properties: HashMap<String, Prop>) -> Result<(), ClientError> {
        let op = Op::Write(WriteOp::AddGraphMetadata(AddGraphMetadataOp {
            path: self.path.clone(),
            properties,
        }));
        self.transport.execute(&op).await?;
        Ok(())
    }

    pub async fn update_metadata(
        &self,
        properties: HashMap<String, Prop>,
    ) -> Result<(), ClientError> {
        let op = Op::Write(WriteOp::UpdateGraphMetadata(UpdateGraphMetadataOp {
            path: self.path.clone(),
            properties,
        }));
        self.transport.execute(&op).await?;
        Ok(())
    }

    /// Deletes an edge at the given time, src, dst and optional layer.
    pub async fn delete_edge<G: Into<GID> + ToString, T: IntoTime>(
        &self,
        timestamp: T,
        src: G,
        dst: G,
        layer: Option<String>,
    ) -> Result<RemoteEdge, ClientError> {
        let src_str = src.to_string();
        let dst_str = dst.to_string();
        let op = Op::Write(WriteOp::DeleteEdge(DeleteEdgeOp {
            path: self.path.clone(),
            time: timestamp.into_time().t(),
            src: src_str.clone(),
            dst: dst_str.clone(),
            layer,
        }));
        self.transport.execute(&op).await?;
        Ok(RemoteEdge::new(
            self.path.clone(),
            self.transport.clone(),
            src_str,
            dst_str,
        ))
    }
}
