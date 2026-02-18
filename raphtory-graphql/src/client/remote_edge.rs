//! Pure Rust remote edge client for GraphQL updateGraph.edge(...) operations.

use crate::client::{
    build_property_string, raphtory_client::RaphtoryGraphQLClient, remote_graph::build_query,
    ClientError,
};
use minijinja::context;
use raphtory_api::core::{
    entities::properties::prop::Prop, storage::timeindex::AsTime, utils::time::IntoTime,
};
use std::collections::HashMap;

/// Pure Rust remote edge wrapper around `RaphtoryGraphQLClient`.
#[derive(Clone)]
pub struct GraphQLRemoteEdge {
    pub path: String,
    pub client: RaphtoryGraphQLClient,
    pub src: String,
    pub dst: String,
}

impl GraphQLRemoteEdge {
    pub fn new(path: String, client: RaphtoryGraphQLClient, src: String, dst: String) -> Self {
        Self {
            path,
            client,
            src,
            dst,
        }
    }

    /// Add temporal updates to the edge at the specified time.
    pub async fn add_updates<T: IntoTime>(
        &self,
        t: T,
        properties: Option<HashMap<String, Prop>>,
        layer: Option<String>,
    ) -> Result<(), ClientError> {
        let template = r#"
            {
              updateGraph(path: "{{path}}") {
                edge(src: "{{src}}",dst: "{{dst}}") {
                  addUpdates(time: {{t}} {% if properties is not none %}, properties: {{ properties | safe }} {% endif %} {% if layer is not none %}, layer:  "{{layer}}" {% endif %})
                }
              }
            }
        "#;

        let ctx = context! {
            path => self.path,
            src => self.src,
            dst => self.dst,
            t => t.into_time().t(),
            properties => properties.map(|p| build_property_string(p)),
            layer => layer
        };

        let query = build_query(template, ctx).map_err(ClientError::from)?;
        self.client.query(&query, HashMap::new()).await.map(|_| ())
    }

    /// Mark the edge as deleted at the specified time.
    pub async fn delete<T: IntoTime>(
        &self,
        t: T,
        layer: Option<String>,
    ) -> Result<(), ClientError> {
        let template = r#"
            {
              updateGraph(path: "{{path}}") {
                edge(src: "{{src}}",dst: "{{dst}}") {
                  delete(time: {{t}}{% if layer is not none %}, layer:  "{{layer}}"{% endif %})
                }
              }
            }
        "#;

        let ctx = context! {
            path => self.path,
            src => self.src,
            dst => self.dst,
            t => t.into_time().t(),
            layer => layer
        };

        let query = build_query(template, ctx).map_err(ClientError::from)?;
        self.client.query(&query, HashMap::new()).await.map(|_| ())
    }

    /// Add metadata to the edge (properties that do not change over time).
    pub async fn add_metadata(
        &self,
        properties: HashMap<String, Prop>,
        layer: Option<String>,
    ) -> Result<(), ClientError> {
        let template = r#"
            {
              updateGraph(path: "{{path}}") {
                edge(src: "{{src}}",dst: "{{dst}}") {
                  addMetadata(properties:  {{ properties | safe }} {% if layer is not none %}, layer:  "{{layer}}" {% endif %})
                }
              }
            }
        "#;

        let ctx = context! {
            path => self.path,
            src => self.src,
            dst => self.dst,
            properties => build_property_string(properties),
            layer => layer
        };

        let query = build_query(template, ctx).map_err(ClientError::from)?;
        self.client.query(&query, HashMap::new()).await.map(|_| ())
    }

    /// Update metadata of the edge, overwriting existing values.
    pub async fn update_metadata(
        &self,
        properties: HashMap<String, Prop>,
        layer: Option<String>,
    ) -> Result<(), ClientError> {
        let template = r#"
            {
              updateGraph(path: "{{path}}") {
                edge(src: "{{src}}",dst: "{{dst}}") {
                  updateMetadata(properties:  {{ properties | safe }} {% if layer is not none %}, layer:  "{{layer}}" {% endif %})
                }
              }
            }
        "#;

        let ctx = context! {
            path => self.path,
            src => self.src,
            dst => self.dst,
            properties => build_property_string(properties),
            layer => layer
        };

        let query = build_query(template, ctx).map_err(ClientError::from)?;
        self.client.query(&query, HashMap::new()).await.map(|_| ())
    }
}
