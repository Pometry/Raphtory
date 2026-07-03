use crate::client::{
    build_property_string,
    graphql_transport::GraphqlTransport,
    op::{AddNode as AddNodeOp, CreateNode as CreateNodeOp, Op, ReadExpr, WriteOp},
    remote_client::RemoteClient,
    remote_edge::RemoteEdge,
    remote_node::RemoteNode,
    transport::Transport,
    ClientError,
};
use minijinja::{context, Environment, Value};
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
    /// Kept for now — used by writes that haven't yet been migrated through
    /// the transport. Removed once all writes route through it.
    pub client: RemoteClient,
    pub transport: Arc<dyn Transport>,
    /// The read expression built so far. Starts as `Root { path }`.
    pub expr: ReadExpr,
}

impl RemoteGraph {
    pub fn new(path: String, client: RemoteClient) -> Self {
        let transport: Arc<dyn Transport> = Arc::new(GraphqlTransport::new(client.clone()));
        let expr = ReadExpr::Root { path: path.clone() };
        Self {
            path,
            client,
            transport,
            expr,
        }
    }

    /// Time-window the graph. Lazy — builds up the read expression, no RPC.
    pub fn window(&self, start: i64, end: i64) -> RemoteGraph {
        RemoteGraph {
            path: self.path.clone(),
            client: self.client.clone(),
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
            self.client.clone(),
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
            self.client.clone(),
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
            self.client.clone(),
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
            self.client.clone(),
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
        let template = r#"
        {
            updateGraph(path: "{{ path }}") {
                addEdge(
                    time: {{ time }},
                    src: "{{ src }}",
                    dst: "{{ dst }}"
                    {% if properties is not none %}, properties: {{ properties | safe }}{% endif %}
                    {% if layer is not none %}, layer: "{{ layer }}"{% endif %}
                ) {
                    success
                }
            }
        }
        "#;

        let ctx = context! {
            path => self.path,
            time => timestamp.into_time().t(),
            src => src.to_string(),
            dst => dst.to_string(),
            properties => properties.map(|p| build_property_string(p)),
            layer => layer,
        };

        let query = build_query(template, ctx)?;
        let res = self.client.query(&query, HashMap::new()).await?;
        if res
            .get("updateGraph")
            .and_then(|x| x.as_object())
            .and_then(|x| x.get("addEdge"))
            .and_then(|x| x.as_object())
            .and_then(|x| x.get("success"))
            .and_then(|x| x.as_bool())
            .is_some_and(|x| x == true)
        {
            Ok(RemoteEdge::new(
                self.path.clone(),
                self.client.clone(),
                src.to_string(),
                dst.to_string(),
            ))
        } else {
            Err(ClientError::UnsuccessfulResponse)
        }
    }

    pub async fn add_property(
        &self,
        timestamp: EventTime,
        properties: HashMap<String, Prop>,
    ) -> Result<(), ClientError> {
        let template = r#"
        {
          updateGraph(path: "{{ path }}") {
            addProperties(t: {{t}} properties: {{ properties | safe }})
          }
        }
        "#;

        let ctx = context! {
            path => self.path,
            t => timestamp.into_time().t(),
            properties => build_property_string(properties),
        };

        let query = build_query(template, ctx)?;
        let res = self.client.query(&query, HashMap::new()).await?;
        if res
            .get("updateGraph")
            .and_then(|x| x.as_object())
            .and_then(|x| x.get("addProperties"))
            .and_then(|x| x.as_bool())
            .is_some_and(|x| x == true)
        {
            Ok(())
        } else {
            Err(ClientError::UnsuccessfulResponse)
        }
    }

    pub async fn add_metadata(&self, properties: HashMap<String, Prop>) -> Result<(), ClientError> {
        let template = r#"
        {
          updateGraph(path: "{{ path }}") {
            addMetadata(properties: {{ properties | safe }})
          }
        }
        "#;

        let ctx = context! {
            path => self.path,
            properties => build_property_string(properties),
        };

        let query = build_query(template, ctx)?;
        let res = self.client.query(&query, HashMap::new()).await?;
        if res
            .get("updateGraph")
            .and_then(|x| x.as_object())
            .and_then(|x| x.get("addMetadata"))
            .and_then(|x| x.as_bool())
            .is_some_and(|x| x == true)
        {
            Ok(())
        } else {
            Err(ClientError::UnsuccessfulResponse)
        }
    }

    pub async fn update_metadata(
        &self,
        properties: HashMap<String, Prop>,
    ) -> Result<(), ClientError> {
        let template = r#"
        {
          updateGraph(path: "{{ path }}") {
            updateMetadata(properties: {{ properties | safe }})
          }
        }
        "#;

        let ctx = context! {
            path => self.path,
            properties => build_property_string(properties),
        };

        let query = build_query(template, ctx)?;
        let res = self.client.query(&query, HashMap::new()).await?;
        if res
            .get("updateGraph")
            .and_then(|x| x.as_object())
            .and_then(|x| x.get("updateMetadata"))
            .and_then(|x| x.as_bool())
            .is_some_and(|x| x == true)
        {
            Ok(())
        } else {
            Err(ClientError::UnsuccessfulResponse)
        }
    }

    /// Deletes an edge at the given time, src, dst and optional layer.
    pub async fn delete_edge<G: Into<GID> + ToString, T: IntoTime>(
        &self,
        timestamp: T,
        src: G,
        dst: G,
        layer: Option<String>,
    ) -> Result<RemoteEdge, ClientError> {
        let template = r#"
        {
            updateGraph(path: "{{ path }}") {
                deleteEdge(
                    time: {{ time }},
                    src: "{{ src }}",
                    dst: "{{ dst }}"
                    {% if layer is not none %}, layer: "{{ layer }}"{% endif %}
                ) {
                    success
                }
            }
        }
        "#;

        let ctx = context! {
            path => self.path,
            time => timestamp.into_time().t(),
            src => src.to_string(),
            dst => dst.to_string(),
            layer => layer,
        };

        let query = build_query(template, ctx)?;
        let res = self.client.query(&query, HashMap::new()).await?;
        if res
            .get("updateGraph")
            .and_then(|x| x.as_object())
            .and_then(|x| x.get("deleteEdge"))
            .and_then(|x| x.as_object())
            .and_then(|x| x.get("success"))
            .and_then(|x| x.as_bool())
            .is_some_and(|x| x == true)
        {
            Ok(RemoteEdge::new(
                self.path.clone(),
                self.client.clone(),
                src.to_string(),
                dst.to_string(),
            ))
        } else {
            Err(ClientError::UnsuccessfulResponse)
        }
    }
}
