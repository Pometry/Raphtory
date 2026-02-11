//! Pure Rust remote node client for GraphQL updateGraph.node(...) operations.

use crate::client::{
    build_property_string, raphtory_client::RaphtoryGraphQLClient, remote_graph::build_query,
    ClientError,
};
use minijinja::context;
use raphtory_api::core::{
    entities::properties::prop::Prop,
    storage::timeindex::{AsTime, EventTime},
    utils::time::IntoTime,
};
use std::collections::HashMap;

/// Pure Rust remote node wrapper around `RaphtoryGraphQLClient`.
#[derive(Clone)]
pub struct GraphQLRemoteNode {
    pub path: String,
    pub client: RaphtoryGraphQLClient,
    pub id: String,
}

impl GraphQLRemoteNode {
    pub fn new(path: String, client: RaphtoryGraphQLClient, id: String) -> Self {
        Self { path, client, id }
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
        self.client
            .query_async(&query, HashMap::new())
            .await
            .map(|_| ())
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
        self.client
            .query_async(&query, HashMap::new())
            .await
            .map(|_| ())
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
        self.client
            .query_async(&query, HashMap::new())
            .await
            .map(|_| ())
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
        self.client
            .query_async(&query, HashMap::new())
            .await
            .map(|_| ())
    }
}
