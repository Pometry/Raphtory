use crate::client::{
    build_property_string,
    graphql_transport::GraphqlTransport,
    op::{Op, ReadExpr},
    raphtory_client::RaphtoryGraphQLClient,
    remote_graph::build_query,
    transport::Transport,
    ClientError,
};
use minijinja::context;
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
    pub client: RaphtoryGraphQLClient,
    pub id: String,
    pub transport: Arc<dyn Transport>,
    pub expr: ReadExpr,
}

impl RemoteNode {
    /// Legacy constructor: builds a fresh transport and a minimal `Root → Node`
    /// expression. Use `with_expr` when a parent `RemoteGraph` has already
    /// built up view state.
    pub fn new(path: String, client: RaphtoryGraphQLClient, id: String) -> Self {
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
        client: RaphtoryGraphQLClient,
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
        let template = r#"
            {
              updateGraph(path: "{{path}}") {
                node(name: "{{name}}") {
                  setNodeType(newType: "{{new_type}}")
                }
              }
            }
        "#;

        let ctx = context! {
            path => self.path,
            name => self.id,
            new_type => new_type
        };

        let query = build_query(template, ctx).map_err(ClientError::from)?;
        self.client.query(&query, HashMap::new()).await.map(|_| ())
    }

    /// Add temporal updates to the node at the specified time.
    pub async fn add_updates<T: IntoTime>(
        &self,
        t: T,
        properties: Option<HashMap<String, Prop>>,
    ) -> Result<(), ClientError> {
        let template = r#"
            {
              updateGraph(path: "{{path}}") {
                node(name: "{{name}}") {
                  addUpdates(time: {{t}} {% if properties is not none %}, properties:  {{ properties | safe }} {% endif %})
                }
              }
            }
        "#;

        let ctx = context! {
            path => self.path,
            name => self.id,
            t => t.into_time().t(),
            properties => properties.map(|p| build_property_string(p)),
        };

        let query = build_query(template, ctx).map_err(ClientError::from)?;
        self.client.query(&query, HashMap::new()).await.map(|_| ())
    }

    /// Add metadata to the node (properties that do not change over time).
    pub async fn add_metadata(&self, properties: HashMap<String, Prop>) -> Result<(), ClientError> {
        let template = r#"
            {
              updateGraph(path: "{{path}}") {
                node(name: "{{name}}") {
                  addMetadata(properties: {{ properties | safe }} )
                }
              }
            }
        "#;

        let ctx = context! {
            path => self.path,
            name => self.id,
            properties => build_property_string(properties),
        };

        let query = build_query(template, ctx).map_err(ClientError::from)?;
        self.client.query(&query, HashMap::new()).await.map(|_| ())
    }

    /// Update metadata of the node, overwriting existing values.
    pub async fn update_metadata(
        &self,
        properties: HashMap<String, Prop>,
    ) -> Result<(), ClientError> {
        let template = r#"
            {
              updateGraph(path: "{{path}}") {
                node(name: "{{name}}") {
                  updateMetadata(properties: {{ properties | safe }} )
                }
              }
            }
        "#;

        let ctx = context! {
            path => self.path,
            name => self.id,
            properties => build_property_string(properties)
        };

        let query = build_query(template, ctx).map_err(ClientError::from)?;
        self.client.query(&query, HashMap::new()).await.map(|_| ())
    }
}
