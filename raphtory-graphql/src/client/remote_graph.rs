use crate::client::{
    build_property_string,
    graphql_client::RaphtoryGraphQLClient,
    json_eq_relaxed, props_to_json_values, props_to_property_inputs,
    queries::{
        add_edge, add_edge_with_metadata, add_node, add_node_with_metadata, delete_edge,
        has_deleted_edge, has_edge, has_node, AddEdge, AddEdgeWithMetadata, AddNode,
        AddNodeWithMetadata, DeleteEdge, EdgePropertyTypes, EdgeWithMetadataPropertyTypes,
        HasDeletedEdge, HasEdge, HasNode, NodePropertyTypes, NodeWithMetadataPropertyTypes,
    },
    ClientError,
};
use minijinja::{context, Environment, Value};
use raphtory::errors::GraphError;
use raphtory_api::core::{
    entities::{properties::prop::Prop, GID},
    storage::timeindex::{AsTime, EventTime},
    utils::time::IntoTime,
};
use serde_json::Value as JsonValue;
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
        metadata: Option<HashMap<String, Prop>>,
        node_type: Option<String>,
    ) -> Result<(), ClientError> {
        let name = id.to_string();

        if metadata.is_none() || metadata.as_ref().is_some_and(|m| m.is_empty()) {
            let properties = properties
                .as_ref()
                .map(|p| props_to_property_inputs::<NodePropertyTypes>(p));
            let vars = add_node::Variables {
                path: self.path.clone(),
                time: timestamp.into_time().t(),
                name,
                node_type,
                properties,
            };

            let resp = self.client.try_graphql_query_async::<AddNode>(vars).await?;
            if resp.update_graph.add_node.success {
                Ok(())
            } else {
                Err(ClientError::OperationFailed(
                    "Node could not be added".to_string(),
                ))
            }
        } else {
            let properties = properties
                .as_ref()
                .map(|p| props_to_property_inputs::<NodeWithMetadataPropertyTypes>(p));
            let metadata = metadata
                .as_ref()
                .map(|m| props_to_property_inputs::<NodeWithMetadataPropertyTypes>(m))
                .unwrap();
            let vars = add_node_with_metadata::Variables {
                path: self.path.clone(),
                time: timestamp.into_time().t(),
                name,
                node_type,
                properties,
                metadata,
            };

            let resp = self
                .client
                .try_graphql_query_async::<AddNodeWithMetadata>(vars)
                .await?;
            if resp.update_graph.add_node.add_metadata {
                Ok(())
            } else {
                Err(ClientError::OperationFailed(
                    "Node could not be added".to_string(),
                ))
            }
        }
    }

    /// Returns Ok(True) if a node exists on the remote graph containing all the data passed to the function.
    pub async fn contains_node_update<G: Into<GID> + ToString, T: IntoTime>(
        &self,
        timestamp: T,
        id: G,
        properties: Option<HashMap<String, Prop>>,
        metadata: Option<HashMap<String, Prop>>,
        node_type: Option<String>,
    ) -> Result<bool, ClientError> {
        let name = id.to_string();
        let layer_name = node_type.clone().unwrap_or("_default".to_string());
        let prop_keys: Vec<String> = properties
            .as_ref()
            .map(|p| p.iter().map(|(k, _)| k.to_string()).collect())
            .unwrap_or_default();
        let meta_keys: Vec<String> = metadata
            .as_ref()
            .map(|m| m.iter().map(|(k, _)| k.to_string()).collect())
            .unwrap_or_default();

        let vars = has_node::Variables {
            path: self.path.clone(),
            time: timestamp.into_time().t(),
            name,
            layer_name,
            prop_keys,
            meta_keys,
        };
        let resp = self.client.try_graphql_query_async::<HasNode>(vars).await?;
        let n = match resp.graph.node {
            Some(n) => n,
            None => return Ok(false),
        };
        let expected_props = properties
            .as_ref()
            .map(|p| props_to_json_values(p))
            .unwrap_or_default();
        let expected_metadata = metadata
            .as_ref()
            .map(|m| props_to_json_values(m))
            .unwrap_or_default();

        let observed_props: HashMap<String, JsonValue> =
            n.at.layer
                .properties
                .values
                .into_iter()
                .map(|p| (p.key, p.value))
                .collect();
        let observed_metadata: HashMap<String, JsonValue> =
            n.at.layer
                .metadata
                .values
                .into_iter()
                .map(|p| (p.key, p.value))
                .collect();

        let node_types_match = match node_type {
            Some(node_type) => {
                n.at.layer
                    .node_type
                    .map(|nt| node_type == nt)
                    .unwrap_or(false)
            }
            None => n.at.layer.node_type.is_none(),
        };

        let props_match = expected_props.len() == observed_props.len()
            && expected_props.iter().all(|(ek, ev)| {
                observed_props
                    .get(ek)
                    .is_some_and(|ov| json_eq_relaxed(ev, ov))
            });
        let metadata_match = expected_metadata.len() == observed_metadata.len()
            && expected_metadata.iter().all(|(ek, ev)| {
                observed_metadata
                    .get(ek)
                    .is_some_and(|ov| json_eq_relaxed(ev, ov))
            });

        Ok(n.at.layer.is_active && node_types_match && props_match && metadata_match)
    }

    pub async fn add_edge<G: Into<GID> + ToString, T: IntoTime>(
        &self,
        timestamp: T,
        src: G,
        dst: G,
        properties: Option<HashMap<String, Prop>>,
        metadata: Option<HashMap<String, Prop>>,
        layer: Option<String>,
    ) -> Result<(), ClientError> {
        let src = src.to_string();
        let dst = dst.to_string();

        if metadata.is_none() || metadata.as_ref().is_some_and(|m| m.is_empty()) {
            let properties = properties
                .as_ref()
                .map(|p| props_to_property_inputs::<EdgePropertyTypes>(p));
            let vars = add_edge::Variables {
                path: self.path.clone(),
                time: timestamp.into_time().t(),
                src,
                dst,
                layer,
                properties,
            };

            let resp = self.client.try_graphql_query_async::<AddEdge>(vars).await?;
            if resp.update_graph.add_edge.success {
                Ok(())
            } else {
                Err(ClientError::OperationFailed(
                    "Edge could not be added".to_string(),
                ))
            }
        } else {
            let properties = properties
                .as_ref()
                .map(|p| props_to_property_inputs::<EdgeWithMetadataPropertyTypes>(p));
            let metadata = metadata
                .as_ref()
                .map(|m| props_to_property_inputs::<EdgeWithMetadataPropertyTypes>(m))
                .unwrap();
            let vars = add_edge_with_metadata::Variables {
                path: self.path.clone(),
                time: timestamp.into_time().t(),
                src,
                dst,
                layer,
                properties,
                metadata,
            };

            let resp = self
                .client
                .try_graphql_query_async::<AddEdgeWithMetadata>(vars)
                .await?;
            if resp.update_graph.add_edge.add_metadata {
                Ok(())
            } else {
                Err(ClientError::OperationFailed(
                    "Edge could not be added".to_string(),
                ))
            }
        }
    }

    /// Returns Ok(True) if an edge exists on the remote graph containing all the data passed to the function.
    pub async fn contains_edge_update<G: Into<GID> + ToString, T: IntoTime>(
        &self,
        timestamp: T,
        src: G,
        dst: G,
        properties: Option<HashMap<String, Prop>>,
        metadata: Option<HashMap<String, Prop>>,
        layer: Option<String>,
    ) -> Result<bool, ClientError> {
        let src = src.to_string();
        let dst = dst.to_string();
        let layer_name = layer.clone().unwrap_or("_default".to_string());
        let prop_keys: Vec<String> = properties
            .as_ref()
            .map(|p| p.iter().map(|(k, _)| k.to_string()).collect())
            .unwrap_or_default();
        let meta_keys: Vec<String> = metadata
            .as_ref()
            .map(|m| m.iter().map(|(k, _)| k.to_string()).collect())
            .unwrap_or_default();

        let vars = has_edge::Variables {
            path: self.path.clone(),
            time: timestamp.into_time().t(),
            src,
            dst,
            layer_name,
            prop_keys,
            meta_keys,
        };
        let resp = self.client.try_graphql_query_async::<HasEdge>(vars).await?;
        let e = match resp.graph.edge {
            Some(e) => e,
            None => return Ok(false),
        };
        let expected_props = properties
            .as_ref()
            .map(|p| props_to_json_values(p))
            .unwrap_or_default();
        let expected_metadata = metadata
            .as_ref()
            .map(|m| props_to_json_values(m))
            .unwrap_or_default();

        let observed_props: HashMap<String, JsonValue> =
            e.at.layer
                .properties
                .values
                .into_iter()
                .map(|p| (p.key, p.value))
                .collect();
        let observed_metadata: HashMap<String, JsonValue> =
            e.at.layer
                .metadata
                .values
                .into_iter()
                .map(|p| (p.key, p.value))
                .collect();

        let layer_names_match = match layer {
            Some(layer_name) => e.at.layer.layer_names == vec![layer_name],
            None => e.at.layer.layer_names == vec!["_default".to_string()],
        };

        let props_match = expected_props.len() == observed_props.len()
            && expected_props.iter().all(|(ek, ev)| {
                observed_props
                    .get(ek)
                    .is_some_and(|ov| json_eq_relaxed(ev, ov))
            });
        let metadata_match = expected_metadata.len() == observed_metadata.len()
            && expected_metadata.iter().all(|(ek, ev)| {
                observed_metadata
                    .get(ek)
                    .is_some_and(|ov| json_eq_relaxed(ev, ov))
            });

        Ok(e.at.layer.is_active && layer_names_match && props_match && metadata_match)
    }

    pub async fn delete_edge<G: Into<GID> + ToString, T: IntoTime>(
        &self,
        timestamp: T,
        src: G,
        dst: G,
        layer: Option<String>,
    ) -> Result<(), ClientError> {
        let src = src.to_string();
        let dst = dst.to_string();
        let vars = delete_edge::Variables {
            path: self.path.clone(),
            time: timestamp.into_time().t(),
            src,
            dst,
            layer,
        };

        let resp = self
            .client
            .try_graphql_query_async::<DeleteEdge>(vars)
            .await?;
        if resp.update_graph.delete_edge.success {
            Ok(())
        } else {
            Err(ClientError::OperationFailed(
                "Edge could not be deleted".to_string(),
            ))
        }
    }

    pub async fn contains_edge_deletion<G: Into<GID> + ToString, T: IntoTime>(
        &self,
        timestamp: T,
        src: G,
        dst: G,
        layer: Option<String>,
    ) -> Result<bool, ClientError> {
        let src = src.to_string();
        let dst = dst.to_string();
        let layer_name = layer.clone().unwrap_or("_default".to_string());

        let vars = has_deleted_edge::Variables {
            path: self.path.clone(),
            time: timestamp.into_time().t(),
            src,
            dst,
            layer_name,
        };
        let resp = self
            .client
            .try_graphql_query_async::<HasDeletedEdge>(vars)
            .await?;
        let del_e = match resp.graph.edge {
            Some(del_e) => del_e,
            None => return Ok(false),
        };

        let layer_names_match = match layer {
            Some(layer_name) => del_e.at.layer.layer_names == vec![layer_name],
            None => del_e.at.layer.layer_names == vec!["_default".to_string()],
        };

        Ok(del_e.at.layer.is_deleted && layer_names_match)
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
            .json_query_async(&query, HashMap::new())
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
            .json_query_async(&query, HashMap::new())
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
            .json_query_async(&query, HashMap::new())
            .await
            .map(|_| ())
    }
}
