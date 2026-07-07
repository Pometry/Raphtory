//! GraphQL implementation of `Transport`.
//!
//! Renders `Op` variants into GraphQL queries against the existing server and
//! parses responses back into `Option<Prop>`. All wire logic lives here so
//! client wrappers (`RemoteGraph`, `RemoteNode`, ...) stay transport-agnostic.

use crate::client::{
    build_property_string,
    op::{
        AddEdge, AddEdgeMetadata, AddEdgeUpdates, AddEdges, AddGraphMetadata, AddGraphProperty,
        AddNode, AddNodeMetadata, AddNodeUpdates, AddNodes, CreateNode, DeleteEdge,
        DeleteEdgeAtTime, Op, ReadExpr, SetNodeType, UpdateEdgeMetadata, UpdateGraphMetadata,
        UpdateNodeMetadata, WriteOp,
    },
    remote_client::RemoteClient,
    remote_graph::build_query,
    transport::Transport,
    ClientError,
};
use async_graphql::async_trait;
use minijinja::context;
use raphtory_api::core::entities::properties::prop::Prop;
use serde_json::Value as JsonValue;
use std::collections::HashMap;

/// V1 transport: renders ops as GraphQL, sends over HTTP via `RemoteClient`.
pub struct GraphqlTransport {
    client: RemoteClient,
}

impl GraphqlTransport {
    pub fn new(client: RemoteClient) -> Self {
        Self { client }
    }
}

#[async_trait::async_trait]
impl Transport for GraphqlTransport {
    async fn execute(&self, op: &Op) -> Result<Option<Prop>, ClientError> {
        match op {
            Op::Write(w) => self.apply_write(w).await,
            Op::Read(expr) => self.eval_read(expr).await,
        }
    }
}

// ============ Write path ============

impl GraphqlTransport {
    async fn apply_write(&self, op: &WriteOp) -> Result<Option<Prop>, ClientError> {
        match op {
            WriteOp::AddNode(args) => self.apply_add_node(args).await,
            WriteOp::CreateNode(args) => self.apply_create_node(args).await,
            WriteOp::AddEdge(args) => self.apply_add_edge(args).await,
            WriteOp::AddGraphProperty(args) => self.apply_add_graph_property(args).await,
            WriteOp::AddGraphMetadata(args) => self.apply_add_graph_metadata(args).await,
            WriteOp::UpdateGraphMetadata(args) => self.apply_update_graph_metadata(args).await,
            WriteOp::DeleteEdge(args) => self.apply_delete_edge(args).await,
            WriteOp::SetNodeType(args) => self.apply_set_node_type(args).await,
            WriteOp::AddNodeUpdates(args) => self.apply_add_node_updates(args).await,
            WriteOp::AddNodeMetadata(args) => self.apply_add_node_metadata(args).await,
            WriteOp::UpdateNodeMetadata(args) => self.apply_update_node_metadata(args).await,
            WriteOp::AddEdgeUpdates(args) => self.apply_add_edge_updates(args).await,
            WriteOp::DeleteEdgeAtTime(args) => self.apply_delete_edge_at_time(args).await,
            WriteOp::AddEdgeMetadata(args) => self.apply_add_edge_metadata(args).await,
            WriteOp::UpdateEdgeMetadata(args) => self.apply_update_edge_metadata(args).await,
            WriteOp::AddNodes(args) => self.apply_add_nodes(args).await,
            WriteOp::AddEdges(args) => self.apply_add_edges(args).await,
        }
    }

    async fn apply_add_node(&self, args: &AddNode) -> Result<Option<Prop>, ClientError> {
        let template = r#"
        {
            updateGraph(path: "{{ path }}") {
                addNode(
                    time: {{ time }},
                    name: "{{ name }}"
                    {% if properties is not none %}, properties: {{ properties | safe }}{% endif %}
                    {% if node_type is not none %}, nodeType: "{{ node_type }}"{% endif %}
                    {% if layer is not none %}, layer: "{{ layer }}"{% endif %}
                ) {
                    success
                }
            }
        }
        "#;

        let ctx = context! {
            path => args.path,
            time => args.time,
            name => args.id,
            properties => args.properties.as_ref().map(|p| build_property_string(p.clone())),
            node_type => args.node_type,
            layer => args.layer,
        };

        let query = build_query(template, ctx)?;
        let res = self.client.query(&query, HashMap::new()).await?;

        let success = res
            .get("updateGraph")
            .and_then(|x| x.as_object())
            .and_then(|x| x.get("addNode"))
            .and_then(|x| x.as_object())
            .and_then(|x| x.get("success"))
            .and_then(|x| x.as_bool())
            .is_some_and(|x| x);

        if success {
            Ok(None)
        } else {
            Err(ClientError::UnsuccessfulResponse)
        }
    }

    async fn apply_create_node(&self, args: &CreateNode) -> Result<Option<Prop>, ClientError> {
        let template = r#"
        {
            updateGraph(path: "{{ path }}") {
                createNode(
                    time: {{ time }},
                    name: "{{ name }}"
                    {% if properties is not none %}, properties: {{ properties | safe }}{% endif %}
                    {% if node_type is not none %}, nodeType: "{{ node_type }}"{% endif %}
                ) {
                    success
                }
            }
        }
        "#;

        let ctx = context! {
            path => args.path,
            time => args.time,
            name => args.id,
            properties => args.properties.as_ref().map(|p| build_property_string(p.clone())),
            node_type => args.node_type,
        };

        let query = build_query(template, ctx)?;
        let res = self.client.query(&query, HashMap::new()).await?;

        let success = res
            .get("updateGraph")
            .and_then(|x| x.as_object())
            .and_then(|x| x.get("createNode"))
            .and_then(|x| x.as_object())
            .and_then(|x| x.get("success"))
            .and_then(|x| x.as_bool())
            .is_some_and(|x| x);

        if success {
            Ok(None)
        } else {
            Err(ClientError::UnsuccessfulResponse)
        }
    }

    async fn apply_add_edge(&self, args: &AddEdge) -> Result<Option<Prop>, ClientError> {
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
            path => args.path,
            time => args.time,
            src => args.src,
            dst => args.dst,
            properties => args.properties.as_ref().map(|p| build_property_string(p.clone())),
            layer => args.layer,
        };

        let query = build_query(template, ctx)?;
        let res = self.client.query(&query, HashMap::new()).await?;

        let success = res
            .get("updateGraph")
            .and_then(|x| x.as_object())
            .and_then(|x| x.get("addEdge"))
            .and_then(|x| x.as_object())
            .and_then(|x| x.get("success"))
            .and_then(|x| x.as_bool())
            .is_some_and(|x| x);

        if success {
            Ok(None)
        } else {
            Err(ClientError::UnsuccessfulResponse)
        }
    }

    async fn apply_add_graph_property(
        &self,
        args: &AddGraphProperty,
    ) -> Result<Option<Prop>, ClientError> {
        let template = r#"
        {
          updateGraph(path: "{{ path }}") {
            addProperties(t: {{t}} properties: {{ properties | safe }})
          }
        }
        "#;

        let ctx = context! {
            path => args.path,
            t => args.time,
            properties => build_property_string(args.properties.clone()),
        };

        let query = build_query(template, ctx)?;
        let res = self.client.query(&query, HashMap::new()).await?;

        let success = res
            .get("updateGraph")
            .and_then(|x| x.as_object())
            .and_then(|x| x.get("addProperties"))
            .and_then(|x| x.as_bool())
            .is_some_and(|x| x);

        if success {
            Ok(None)
        } else {
            Err(ClientError::UnsuccessfulResponse)
        }
    }

    async fn apply_add_graph_metadata(
        &self,
        args: &AddGraphMetadata,
    ) -> Result<Option<Prop>, ClientError> {
        let template = r#"
        {
          updateGraph(path: "{{ path }}") {
            addMetadata(properties: {{ properties | safe }})
          }
        }
        "#;

        let ctx = context! {
            path => args.path,
            properties => build_property_string(args.properties.clone()),
        };

        let query = build_query(template, ctx)?;
        let res = self.client.query(&query, HashMap::new()).await?;

        let success = res
            .get("updateGraph")
            .and_then(|x| x.as_object())
            .and_then(|x| x.get("addMetadata"))
            .and_then(|x| x.as_bool())
            .is_some_and(|x| x);

        if success {
            Ok(None)
        } else {
            Err(ClientError::UnsuccessfulResponse)
        }
    }

    async fn apply_update_graph_metadata(
        &self,
        args: &UpdateGraphMetadata,
    ) -> Result<Option<Prop>, ClientError> {
        let template = r#"
        {
          updateGraph(path: "{{ path }}") {
            updateMetadata(properties: {{ properties | safe }})
          }
        }
        "#;

        let ctx = context! {
            path => args.path,
            properties => build_property_string(args.properties.clone()),
        };

        let query = build_query(template, ctx)?;
        let res = self.client.query(&query, HashMap::new()).await?;

        let success = res
            .get("updateGraph")
            .and_then(|x| x.as_object())
            .and_then(|x| x.get("updateMetadata"))
            .and_then(|x| x.as_bool())
            .is_some_and(|x| x);

        if success {
            Ok(None)
        } else {
            Err(ClientError::UnsuccessfulResponse)
        }
    }

    async fn apply_delete_edge(&self, args: &DeleteEdge) -> Result<Option<Prop>, ClientError> {
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
            path => args.path,
            time => args.time,
            src => args.src,
            dst => args.dst,
            layer => args.layer,
        };

        let query = build_query(template, ctx)?;
        let res = self.client.query(&query, HashMap::new()).await?;

        let success = res
            .get("updateGraph")
            .and_then(|x| x.as_object())
            .and_then(|x| x.get("deleteEdge"))
            .and_then(|x| x.as_object())
            .and_then(|x| x.get("success"))
            .and_then(|x| x.as_bool())
            .is_some_and(|x| x);

        if success {
            Ok(None)
        } else {
            Err(ClientError::UnsuccessfulResponse)
        }
    }

    async fn apply_set_node_type(&self, args: &SetNodeType) -> Result<Option<Prop>, ClientError> {
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
            path => args.path,
            name => args.id,
            new_type => args.new_type,
        };

        let query = build_query(template, ctx)?;
        self.client.query(&query, HashMap::new()).await?;
        Ok(None)
    }

    async fn apply_add_node_updates(
        &self,
        args: &AddNodeUpdates,
    ) -> Result<Option<Prop>, ClientError> {
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
            path => args.path,
            name => args.id,
            t => args.time,
            properties => args.properties.as_ref().map(|p| build_property_string(p.clone())),
        };

        let query = build_query(template, ctx)?;
        self.client.query(&query, HashMap::new()).await?;
        Ok(None)
    }

    async fn apply_add_node_metadata(
        &self,
        args: &AddNodeMetadata,
    ) -> Result<Option<Prop>, ClientError> {
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
            path => args.path,
            name => args.id,
            properties => build_property_string(args.properties.clone()),
        };

        let query = build_query(template, ctx)?;
        self.client.query(&query, HashMap::new()).await?;
        Ok(None)
    }

    async fn apply_update_node_metadata(
        &self,
        args: &UpdateNodeMetadata,
    ) -> Result<Option<Prop>, ClientError> {
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
            path => args.path,
            name => args.id,
            properties => build_property_string(args.properties.clone()),
        };

        let query = build_query(template, ctx)?;
        self.client.query(&query, HashMap::new()).await?;
        Ok(None)
    }

    async fn apply_add_edge_updates(
        &self,
        args: &AddEdgeUpdates,
    ) -> Result<Option<Prop>, ClientError> {
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
            path => args.path,
            src => args.src,
            dst => args.dst,
            t => args.time,
            properties => args.properties.as_ref().map(|p| build_property_string(p.clone())),
            layer => args.layer,
        };

        let query = build_query(template, ctx)?;
        self.client.query(&query, HashMap::new()).await?;
        Ok(None)
    }

    async fn apply_delete_edge_at_time(
        &self,
        args: &DeleteEdgeAtTime,
    ) -> Result<Option<Prop>, ClientError> {
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
            path => args.path,
            src => args.src,
            dst => args.dst,
            t => args.time,
            layer => args.layer,
        };

        let query = build_query(template, ctx)?;
        self.client.query(&query, HashMap::new()).await?;
        Ok(None)
    }

    async fn apply_add_edge_metadata(
        &self,
        args: &AddEdgeMetadata,
    ) -> Result<Option<Prop>, ClientError> {
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
            path => args.path,
            src => args.src,
            dst => args.dst,
            properties => build_property_string(args.properties.clone()),
            layer => args.layer,
        };

        let query = build_query(template, ctx)?;
        self.client.query(&query, HashMap::new()).await?;
        Ok(None)
    }

    async fn apply_update_edge_metadata(
        &self,
        args: &UpdateEdgeMetadata,
    ) -> Result<Option<Prop>, ClientError> {
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
            path => args.path,
            src => args.src,
            dst => args.dst,
            properties => build_property_string(args.properties.clone()),
            layer => args.layer,
        };

        let query = build_query(template, ctx)?;
        self.client.query(&query, HashMap::new()).await?;
        Ok(None)
    }

    async fn apply_add_nodes(&self, args: &AddNodes) -> Result<Option<Prop>, ClientError> {
        let template = r#"
        {
            updateGraph(path: "{{ path }}") {
                addNodes(
                    nodes: [
                        {% for node in nodes %}
                        {
                            name: "{{ node.name }}"
                            {% if node.node_type is not none %}, nodeType: "{{ node.node_type }}"{% endif %}
                            {% if node.updates is not none %},
                            updates: [
                                {% for tprop in node.updates %}
                                {
                                    time: {{ tprop.time }}
                                    {% if tprop.properties is not none %}, properties: [
                                        {% for prop in tprop.properties %}
                                        { key: "{{ prop.key }}", value: {{ prop.value | safe }} }
                                        {% if not loop.last %},{% endif %}
                                        {% endfor %}
                                    ]{% endif %}
                                }
                                {% if not loop.last %},{% endif %}
                                {% endfor %}
                            ]
                            {% endif %}
                            {% if node.metadata is not none %},
                            metadata: [
                                {% for cprop in node.metadata %}
                                { key: "{{ cprop.key }}", value: {{ cprop.value | safe }} }
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

        let ctx = context! {
            path => args.path,
            nodes => args.nodes,
        };

        let query = build_query(template, ctx)?;
        self.client.query(&query, HashMap::new()).await?;
        Ok(None)
    }

    async fn apply_add_edges(&self, args: &AddEdges) -> Result<Option<Prop>, ClientError> {
        let template = r#"
        {
            updateGraph(path: "{{ path }}") {
                addEdges(
                    edges: [
                        {% for edge in edges %}
                        {
                            src: "{{ edge.src }}"
                            dst: "{{ edge.dst }}"
                            {% if edge.layer is not none %}, layer: "{{ edge.layer }}"{% endif %}
                            {% if edge.updates is not none %},
                            updates: [
                                {% for tprop in edge.updates %}
                                {
                                    time: {{ tprop.time }}
                                    {% if tprop.properties is not none %}, properties: [
                                        {% for prop in tprop.properties %}
                                        { key: "{{ prop.key }}", value: {{ prop.value | safe }} }
                                        {% if not loop.last %},{% endif %}
                                        {% endfor %}
                                    ]{% endif %}
                                }
                                {% if not loop.last %},{% endif %}
                                {% endfor %}
                            ]
                            {% endif %}
                            {% if edge.metadata is not none %},
                            metadata: [
                                {% for cprop in edge.metadata %}
                                { key: "{{ cprop.key }}", value: {{ cprop.value | safe }} }
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

        let ctx = context! {
            path => args.path,
            edges => args.edges,
        };

        let query = build_query(template, ctx)?;
        self.client.query(&query, HashMap::new()).await?;
        Ok(None)
    }
}

// ============ Read path ============

impl GraphqlTransport {
    async fn eval_read(&self, expr: &ReadExpr) -> Result<Option<Prop>, ClientError> {
        let query = render_read(expr);
        let res = self.client.query(&query, HashMap::new()).await?;
        let root =
            serde_json::to_value(&res).map_err(|e| ClientError::InvalidResponse(e.to_string()))?;
        parse_read(expr, &root)
    }
}

/// Renders a read expression tree as a nested GraphQL query.
///
/// Example: `Degree(Node(Window(Root("g"), 0, 10), "ben"))` becomes
/// `{ graph(path: "g") { window(start: 0, end: 10) { node(name: "ben") { degree } } } }`.
fn render_read(expr: &ReadExpr) -> String {
    let body = render_read_body(expr);
    let closes = "}".repeat(read_depth(expr));
    format!("{{ {} {} }}", body, closes)
}

fn render_read_body(expr: &ReadExpr) -> String {
    match expr {
        ReadExpr::Root { path } => format!("graph(path: \"{}\")", path),
        ReadExpr::Window { input, start, end } => format!(
            "{} {{ window(start: {}, end: {})",
            render_read_body(input),
            start,
            end
        ),
        ReadExpr::Node { input, id } => {
            format!("{} {{ node(name: \"{}\")", render_read_body(input), id)
        }
        ReadExpr::Degree { input } => format!("{} {{ degree", render_read_body(input)),
    }
}

fn read_depth(expr: &ReadExpr) -> usize {
    match expr {
        ReadExpr::Root { .. } => 0,
        ReadExpr::Window { input, .. } => 1 + read_depth(input),
        ReadExpr::Node { input, .. } => 1 + read_depth(input),
        ReadExpr::Degree { input } => 1 + read_depth(input),
    }
}

/// Parses the terminal value out of the GraphQL response.
///
/// Strategy: build a root-to-terminal JSON key path from the expression tree,
/// walk the response along that path, then interpret the terminal value
/// according to the outermost expression variant.
fn parse_read(expr: &ReadExpr, root: &JsonValue) -> Result<Option<Prop>, ClientError> {
    let path = build_json_path(expr);
    let mut cursor = root;
    for key in &path[..path.len() - 1] {
        cursor = cursor.get(*key).ok_or_else(|| {
            ClientError::InvalidResponse(format!("missing `{}` in response", key))
        })?;
    }
    let terminal_key = path[path.len() - 1];
    let terminal_val = cursor.get(terminal_key).ok_or_else(|| {
        ClientError::InvalidResponse(format!("missing terminal `{}` in response", terminal_key))
    })?;

    match expr {
        ReadExpr::Degree { .. } => terminal_val
            .as_i64()
            .map(|n| Some(Prop::I64(n)))
            .ok_or_else(|| ClientError::InvalidResponse("`degree` not an i64".into())),
        // The outermost expression must be a terminal — Root/Window/Node
        // alone don't fire an RPC. This branch is unreachable for well-formed
        // trees built by the client wrappers.
        _ => Err(ClientError::InvalidResponse(
            "expression tree has no terminal".into(),
        )),
    }
}

fn build_json_path(expr: &ReadExpr) -> Vec<&'static str> {
    fn go(expr: &ReadExpr, out: &mut Vec<&'static str>) {
        match expr {
            ReadExpr::Root { .. } => out.push("graph"),
            ReadExpr::Window { input, .. } => {
                go(input, out);
                out.push("window");
            }
            ReadExpr::Node { input, .. } => {
                go(input, out);
                out.push("node");
            }
            ReadExpr::Degree { input } => {
                go(input, out);
                out.push("degree");
            }
        }
    }
    let mut out = Vec::new();
    go(expr, &mut out);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    // ============ Unit tests for the read pipeline ============

    #[test]
    fn render_read_produces_nested_graphql() {
        let expr = ReadExpr::Degree {
            input: Box::new(ReadExpr::Node {
                input: Box::new(ReadExpr::Window {
                    input: Box::new(ReadExpr::Root { path: "g".into() }),
                    start: 0,
                    end: 10,
                }),
                id: "ben".into(),
            }),
        };
        let query = render_read(&expr);
        // Not asserting exact whitespace — just the structural shape.
        assert!(query.contains("graph(path: \"g\")"));
        assert!(query.contains("window(start: 0, end: 10)"));
        assert!(query.contains("node(name: \"ben\")"));
        assert!(query.contains("degree"));
        // Balanced braces
        let opens = query.matches('{').count();
        let closes = query.matches('}').count();
        assert_eq!(opens, closes, "unbalanced braces in: {query}");
    }

    #[test]
    fn parse_read_walks_to_terminal_value() {
        let expr = ReadExpr::Degree {
            input: Box::new(ReadExpr::Node {
                input: Box::new(ReadExpr::Root { path: "g".into() }),
                id: "ben".into(),
            }),
        };
        let response = serde_json::json!({
            "graph": { "node": { "degree": 42 } }
        });
        let value = parse_read(&expr, &response).unwrap();
        match value {
            Some(Prop::I64(n)) => assert_eq!(n, 42),
            _ => panic!("expected Some(Prop::I64)"),
        }
    }

    // ============ End-to-end integration: server + client + transport ============

    #[tokio::test]
    async fn test_end_to_end_add_node_and_degree() {
        use crate::{client::remote_client::RemoteClient, server::GraphServer};
        use raphtory::db::api::storage::storage::Config;
        use reqwest::Url;
        use tempfile::tempdir;

        let tmp_dir = tempdir().unwrap();
        let server = GraphServer::new(tmp_dir.path().to_path_buf(), None, Config::default())
            .await
            .unwrap();
        let running = server.start_with_port(0).await.unwrap();
        let port = running.port();

        let url = Url::parse(&format!("http://localhost:{port}")).unwrap();
        let client = RemoteClient::new(url, None);

        // Create the graph
        client.new_graph("test-graph", "EVENT").await.unwrap();

        let rg = client.remote_graph("test-graph".into());

        // Write path: add_node routes through Transport
        rg.add_node(1i64, "ben", None, None, None).await.unwrap();
        rg.add_node(2i64, "hamza", None, None, None).await.unwrap();
        rg.add_edge(3i64, "ben", "hamza", None, None).await.unwrap();

        // Read path: composed expression through Transport
        // g.node("ben").degree() — after edge (ben -> hamza), ben has degree 1.
        let degree = rg.node("ben").degree().await.unwrap();
        assert_eq!(degree, 1, "ben should have degree 1 (single edge to hamza)");

        // With a windowed view, we can restrict to a time range.
        // Window (0, 5) includes the edge added at time 3, so degree is still 1.
        let degree_windowed = rg.window(0, 5).node("ben").degree().await.unwrap();
        assert_eq!(degree_windowed, 1);

        // Window (0, 2) excludes the edge (added at time 3), so degree is 0.
        let degree_before_edge = rg.window(0, 2).node("ben").degree().await.unwrap();
        assert_eq!(degree_before_edge, 0);

        running.stop().await;
    }
}
