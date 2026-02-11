use crate::client::{
    build_property_string, inner_collection, raphtory_client::RaphtoryGraphQLClient,
    remote_edge::GraphQLRemoteEdge, remote_node::GraphQLRemoteNode, ClientError,
};
use minijinja::{context, Environment, Value};
use raphtory::errors::GraphError;
use raphtory_api::core::{
    entities::{properties::prop::Prop, GID},
    storage::timeindex::{AsTime, EventTime},
    utils::time::IntoTime,
};
use serde::Serialize;
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

/// A single temporal update for batch node/edge additions (time + optional properties).
#[derive(Clone, Debug)]
pub struct NodeUpdate {
    pub time: i64,
    pub properties: Option<HashMap<String, Prop>>,
}

/// Input for one node in a batch add (name, optional type, optional updates and metadata).
#[derive(Clone, Debug)]
pub struct NodeAddition {
    pub name: String,
    pub node_type: Option<String>,
    pub updates: Option<Vec<NodeUpdate>>,
    pub metadata: Option<HashMap<String, Prop>>,
}

/// Input for one edge in a batch add (src, dst, optional layer, updates, metadata).
#[derive(Clone, Debug)]
pub struct EdgeAddition {
    pub src: String,
    pub dst: String,
    pub layer: Option<String>,
    pub updates: Option<Vec<NodeUpdate>>,
    pub metadata: Option<HashMap<String, Prop>>,
}

#[derive(Serialize)]
struct KeyValueTmpl {
    key: String,
    value: String,
}

#[derive(Serialize)]
struct NodeUpdateTmpl {
    time: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    properties: Option<Vec<KeyValueTmpl>>,
}

#[derive(Serialize)]
struct NodeTmpl {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    node_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    updates: Option<Vec<NodeUpdateTmpl>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<Vec<KeyValueTmpl>>,
}

#[derive(Serialize)]
struct EdgeTmpl {
    src: String,
    dst: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    layer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    updates: Option<Vec<NodeUpdateTmpl>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<Vec<KeyValueTmpl>>,
}

fn node_addition_to_tmpl(n: &NodeAddition) -> NodeTmpl {
    NodeTmpl {
        name: n.name.clone(),
        node_type: n.node_type.clone(),
        updates: n.updates.as_ref().map(|u| {
            u.iter()
                .map(|up| NodeUpdateTmpl {
                    time: up.time,
                    properties: up.properties.as_ref().map(|p| {
                        p.iter()
                            .map(|(k, v)| KeyValueTmpl {
                                key: k.clone(),
                                value: inner_collection(v),
                            })
                            .collect()
                    }),
                })
                .collect()
        }),
        metadata: n.metadata.as_ref().map(|m| {
            m.iter()
                .map(|(k, v)| KeyValueTmpl {
                    key: k.clone(),
                    value: inner_collection(v),
                })
                .collect()
        }),
    }
}

fn edge_addition_to_tmpl(e: &EdgeAddition) -> EdgeTmpl {
    EdgeTmpl {
        src: e.src.clone(),
        dst: e.dst.clone(),
        layer: e.layer.clone(),
        updates: e.updates.as_ref().map(|u| {
            u.iter()
                .map(|up| NodeUpdateTmpl {
                    time: up.time,
                    properties: up.properties.as_ref().map(|p| {
                        p.iter()
                            .map(|(k, v)| KeyValueTmpl {
                                key: k.clone(),
                                value: inner_collection(v),
                            })
                            .collect()
                    }),
                })
                .collect()
        }),
        metadata: e.metadata.as_ref().map(|m| {
            m.iter()
                .map(|(k, v)| KeyValueTmpl {
                    key: k.clone(),
                    value: inner_collection(v),
                })
                .collect()
        }),
    }
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

    /// Returns a remote node reference for the given node id.
    pub fn node(&self, id: impl ToString) -> GraphQLRemoteNode {
        GraphQLRemoteNode::new(self.path.clone(), self.client.clone(), id.to_string())
    }

    /// Returns a remote edge reference for the given source and destination node ids.
    pub fn edge(&self, src: impl ToString, dst: impl ToString) -> GraphQLRemoteEdge {
        GraphQLRemoteEdge::new(
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

    /// Create a new node (fails if the node already exists). Uses the createNode mutation.
    pub async fn create_node<G: Into<GID> + ToString, T: IntoTime>(
        &self,
        timestamp: T,
        id: G,
        properties: Option<HashMap<String, Prop>>,
        node_type: Option<String>,
    ) -> Result<(), ClientError> {
        let template = r#"
        {
            updateGraph(path: "{{ path }}") {
                createNode(time: {{ time }}, name: "{{ name }}" {% if properties is not none %}, properties:  {{ properties | safe }} {% endif %}{% if node_type is not none %}, nodeType: "{{ node_type }}"{% endif %}) {
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

    /// Batch add node updates. Each item can have name, optional node_type, updates (time + properties), and metadata.
    pub async fn add_nodes(&self, nodes: Vec<NodeAddition>) -> Result<(), ClientError> {
        let template = r#"
        {
        updateGraph(path: "{{ path }}") {
            addNodes(
                nodes: [
                    {% for node in nodes %}
                    {
                        name: "{{ node.name }}"
                        {% if node.node_type %}, nodeType: "{{ node.node_type }}"{% endif %}
                        {% if node.updates %},
                        updates: [
                            {% for tprop in node.updates %}
                            {
                                time: {{ tprop.time }},
                                {% if tprop.properties %}
                                properties: [
                                    {% for prop in tprop.properties %}
                                    {
                                        key: "{{ prop.key }}",
                                        value:{{ prop.value | safe }}
                                    }
                                    {% if not loop.last %},{% endif %}
                                    {% endfor %}
                                ]
                                {% endif %}
                            }
                            {% if not loop.last %},{% endif %}
                            {% endfor %}
                        ]
                        {% endif %}
                        {% if node.metadata %},
                        metadata: [
                            {% for cprop in node.metadata %}
                            {
                                key: "{{ cprop.key }}",
                                value:{{ cprop.value | safe }}
                            }
                            {% if not loop.last %},{% endif %}
                            {% endfor %}
                        ]
                        {% endif %}
                    }
                    {% if not loop.last %},{% endif %}
                    {% endfor %}
                ]
            )
        }
    }
        "#;

        let nodes_tmpl: Vec<NodeTmpl> = nodes.iter().map(node_addition_to_tmpl).collect();
        let ctx = context! {
            path => self.path,
            nodes => nodes_tmpl,
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

    /// Batch add edge updates. Each item has src, dst, optional layer, updates (time + properties), and metadata.
    pub async fn add_edges(&self, edges: Vec<EdgeAddition>) -> Result<(), ClientError> {
        let template = r#"
                {
                updateGraph(path: "{{ path }}") {
                    addEdges(
                        edges: [
                            {% for edge in edges %}
                            {
                                src: "{{ edge.src }}"
                                dst: "{{ edge.dst }}"
                                {% if edge.layer %}, layer: "{{ edge.layer }}"{% endif %}
                                {% if edge.updates %},
                                updates: [
                                    {% for tprop in edge.updates %}
                                    {
                                        time: {{ tprop.time }},
                                        {% if tprop.properties %}
                                        properties: [
                                            {% for prop in tprop.properties %}
                                            {
                                                key: "{{ prop.key }}",
                                                value:{{ prop.value | safe }}
                                            }
                                            {% if not loop.last %},{% endif %}
                                            {% endfor %}
                                        ]
                                        {% endif %}
                                    }
                                    {% if not loop.last %},{% endif %}
                                    {% endfor %}
                                ]
                                {% endif %}
                                {% if edge.metadata %},
                                metadata: [
                                    {% for cprop in edge.metadata %}
                                    {
                                        key: "{{ cprop.key }}",
                                        value:{{ cprop.value | safe }}
                                    }
                                    {% if not loop.last %},{% endif %}
                                    {% endfor %}
                                ]
                                {% endif %}
                            }
                            {% if not loop.last %},{% endif %}
                            {% endfor %}
                        ]
                    )
                }
            }
        "#;

        let edges_tmpl: Vec<EdgeTmpl> = edges.iter().map(edge_addition_to_tmpl).collect();
        let ctx = context! {
            path => self.path,
            edges => edges_tmpl,
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

    /// Deletes an edge at the given time, src, dst and optional layer.
    pub async fn delete_edge<G: Into<GID> + ToString, T: IntoTime>(
        &self,
        timestamp: T,
        src: G,
        dst: G,
        layer: Option<String>,
    ) -> Result<(), ClientError> {
        let template = r#"
        {
            updateGraph(path: "{{ path }}") {
                deleteEdge(time: {{ time }}, src: "{{ src }}", dst: "{{ dst }}" {% if layer is not none %}, layer: "{{ layer }}"{% endif %}) {
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

        let query = build_query(template, ctx).map_err(ClientError::from)?;
        self.client
            .query_async(&query, HashMap::new())
            .await
            .map(|_| ())
    }
}
