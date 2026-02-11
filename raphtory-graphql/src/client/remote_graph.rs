use crate::client::{build_property_string, raphtory_client::RaphtoryGraphQLClient, ClientError};
use minijinja::{context, Environment, Value};
use raphtory::errors::GraphError;
use raphtory_api::core::{
    entities::{properties::prop::Prop, GID},
    storage::timeindex::{AsTime, EventTime},
    utils::time::IntoTime,
};
use std::collections::HashMap;

pub fn build_query(template: &str, context: Value) -> Result<String, GraphError> {
    let mut env = Environment::new();
    env.add_template("template", template)
        .map_err(|e| GraphError::JinjaError(e.to_string()))?;
    let query = env
        .get_template("template")
        .map_err(|e| GraphError::JinjaError(e.to_string()))?
        .render(context)
        .map_err(|e| GraphError::JinjaError(e.to_string()))?;
    Ok(query)
}

/// Pure Rust remote graph wrapper around `RaphtoryGraphQLClient`.
#[derive(Clone)]
pub struct GraphQLRemoteGraph {
    pub path: String,
    pub client: RaphtoryGraphQLClient,
}

impl GraphQLRemoteGraph {
    pub fn new(path: String, client: RaphtoryGraphQLClient) -> Self {
        Self { path, client }
    }

    pub async fn add_node<G: Into<GID> + ToString, T: IntoTime>(
        &self,
        timestamp: T,
        id: G,
        properties: Option<HashMap<String, Prop>>,
        node_type: Option<String>,
    ) -> Result<(), ClientError> {
        let template = r#"
        {
            updateGraph(path: "{{ path }}") {
                addNode(time: {{ time }}, name: "{{ name }}" {% if properties is not none %}, properties:  {{ properties | safe }} {% endif %}{% if node_type is not none %}, nodeType: "{{ node_type }}"{% endif %}) {
                    success
                }
            }
        }
        "#;

        let ctx = context! {
            path => self.path,
            time => timestamp.into_time().t(),
            name => id.to_string(),
            properties => properties.map(|p| build_property_string(p)),
            node_type => node_type,
        };

        let query = build_query(template, ctx).map_err(ClientError::from)?;
        self.client
            .query_async(&query, HashMap::new())
            .await
            .map(|_| ())
    }

    pub async fn add_edge<G: Into<GID> + ToString, T: IntoTime>(
        &self,
        timestamp: T,
        src: G,
        dst: G,
        properties: Option<HashMap<String, Prop>>,
        layer: Option<String>,
    ) -> Result<(), ClientError> {
        let template = r#"
        {
            updateGraph(path: "{{ path }}") {
                addEdge(time: {{ time }}, src: "{{ src }}", dst: "{{ dst }}" {% if properties is not none %}, properties:  {{ properties | safe }} {% endif %}{% if layer is not none %}, layer: "{{ layer }}"{% endif %}) {
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

        let query = build_query(template, ctx).map_err(ClientError::from)?;
        self.client
            .query_async(&query, HashMap::new())
            .await
            .map(|_| ())
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

        let query = build_query(template, ctx).map_err(ClientError::from)?;
        self.client
            .query_async(&query, HashMap::new())
            .await
            .map(|_| ())
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

        let query = build_query(template, ctx).map_err(ClientError::from)?;
        self.client
            .query_async(&query, HashMap::new())
            .await
            .map(|_| ())
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

        let query = build_query(template, ctx).map_err(ClientError::from)?;
        self.client
            .query_async(&query, HashMap::new())
            .await
            .map(|_| ())
    }
}
