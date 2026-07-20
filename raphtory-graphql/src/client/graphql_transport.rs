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
        DeleteEdgeAtTime, EdgeSortBy, NodeSortBy, Op, ReadExpr, SetNodeType, SortByTime,
        UpdateEdgeMetadata, UpdateGraphMetadata, UpdateNodeMetadata, WriteOp,
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

/// Render the argument list for a `page` / `page_rev` server field:
/// - `limit` always present
/// - `offset` and `pageIndex` omitted when `None` (server defaults to 0)
///
/// Returns e.g. `"limit: 10"`, `"limit: 10, offset: 5"`, or
/// `"limit: 10, offset: 5, pageIndex: 2"`. Caller wraps in `(...)`.
fn render_page_args(limit: usize, offset: Option<usize>, page_index: Option<usize>) -> String {
    let mut parts = vec![format!("limit: {}", limit)];
    if let Some(o) = offset {
        parts.push(format!("offset: {}", o));
    }
    if let Some(p) = page_index {
        parts.push(format!("pageIndex: {}", p));
    }
    parts.join(", ")
}

/// Render a `Vec<String>` as the contents of a GraphQL list arg, e.g.
/// `["a", "b", "c"]`. Returns the comma-joined body only — the caller wraps
/// with `[` and `]`.
fn render_string_list(items: &[String]) -> String {
    items
        .iter()
        .map(|s| format!("\"{}\"", s))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Render `SortByTime` as its GraphQL enum literal — the async_graphql
/// `Enum` derive emits SCREAMING_SNAKE_CASE variants.
fn render_sort_by_time(t: SortByTime) -> &'static str {
    match t {
        SortByTime::Latest => "LATEST",
        SortByTime::Earliest => "EARLIEST",
    }
}

/// Render a list of `NodeSortBy` records into GraphQL literal syntax, e.g.
/// `[{property: "score", reverse: true}, {id: true}]`. Empty list renders
/// as `[]` — server accepts it as a no-op sort.
fn render_node_sort_bys(sort_bys: &[NodeSortBy]) -> String {
    let entries: Vec<String> = sort_bys
        .iter()
        .map(|sb| {
            let mut fields = Vec::new();
            if let Some(rev) = sb.reverse {
                fields.push(format!("reverse: {}", rev));
            }
            if let Some(id) = sb.id {
                fields.push(format!("id: {}", id));
            }
            if let Some(t) = sb.time {
                fields.push(format!("time: {}", render_sort_by_time(t)));
            }
            if let Some(ref p) = sb.property {
                fields.push(format!("property: \"{}\"", p));
            }
            format!("{{{}}}", fields.join(", "))
        })
        .collect();
    format!("[{}]", entries.join(", "))
}

// ============ GqlNodeFilter → GraphQL literal rendering ============
//
// We can't go through serde JSON because enum-typed fields (`NodeField`,
// `DegreeDirection`) need to be emitted as unquoted SCREAMING_SNAKE_CASE
// identifiers in GQL literal syntax — serde's `rename_all = "camelCase"`
// gives us `"nodeName"` (quoted, camelCase), which the server rejects.
//
// So we walk the typed structure directly, emitting the correct GQL
// syntax for each variant / field / enum value.
use crate::model::graph::{
    filtering::{
        DegreeDirection, DegreeFilterNew, GqlNodeFilter, NodeField, NodeFieldCondition,
        NodeFieldFilterNew, NodeLayersExpr, NodeTimeExpr, NodeUnaryExpr, NodeWindowExpr,
        PropCondition, PropertyFilterNew,
    },
    property::{ObjectEntry, Value as GqlValue},
    timeindex::GqlTimeInput,
};

fn render_gql_str(s: &str) -> String {
    // JSON string escape rules — same as GQL string literal.
    format!("{:?}", s)
}

fn render_gql_value(v: &GqlValue) -> String {
    match v {
        GqlValue::U8(n) => format!("{{u8: {}}}", n),
        GqlValue::U16(n) => format!("{{u16: {}}}", n),
        GqlValue::U32(n) => format!("{{u32: {}}}", n),
        GqlValue::U64(n) => format!("{{u64: {}}}", n),
        GqlValue::I32(n) => format!("{{i32: {}}}", n),
        GqlValue::I64(n) => format!("{{i64: {}}}", n),
        GqlValue::F32(n) => format!("{{f32: {}}}", n),
        GqlValue::F64(n) => format!("{{f64: {}}}", n),
        GqlValue::Str(s) => format!("{{str: {}}}", render_gql_str(s)),
        GqlValue::Bool(b) => format!("{{bool: {}}}", b),
        GqlValue::List(vs) => {
            let items: Vec<_> = vs.iter().map(render_gql_value).collect();
            format!("{{list: [{}]}}", items.join(", "))
        }
        GqlValue::Object(entries) => {
            let items: Vec<_> = entries
                .iter()
                .map(|ObjectEntry { key, value }| {
                    format!(
                        "{{key: {}, value: {}}}",
                        render_gql_str(key),
                        render_gql_value(value)
                    )
                })
                .collect();
            format!("{{object: [{}]}}", items.join(", "))
        }
        GqlValue::DTime(s) => format!("{{dTime: {}}}", render_gql_str(s)),
        GqlValue::NDTime(s) => format!("{{nDTime: {}}}", render_gql_str(s)),
        GqlValue::Decimal(s) => format!("{{decimal: {}}}", render_gql_str(s)),
    }
}

fn render_node_field(f: NodeField) -> &'static str {
    match f {
        NodeField::NodeId => "NODE_ID",
        NodeField::NodeName => "NODE_NAME",
        NodeField::NodeType => "NODE_TYPE",
    }
}

fn render_degree_direction(d: DegreeDirection) -> &'static str {
    match d {
        DegreeDirection::In => "IN",
        DegreeDirection::Out => "OUT",
        DegreeDirection::Both => "BOTH",
    }
}

fn render_gql_time_input(t: &GqlTimeInput) -> String {
    // For read-side use we only need the timestamp — GqlTimeInput is a
    // scalar accepting int or object; server accepts int literal directly.
    format!("{}", t.t())
}

fn render_node_field_condition(c: &NodeFieldCondition) -> String {
    let (tag, val) = match c {
        NodeFieldCondition::Eq(v) => ("eq", v),
        NodeFieldCondition::Ne(v) => ("ne", v),
        NodeFieldCondition::Gt(v) => ("gt", v),
        NodeFieldCondition::Ge(v) => ("ge", v),
        NodeFieldCondition::Lt(v) => ("lt", v),
        NodeFieldCondition::Le(v) => ("le", v),
        NodeFieldCondition::StartsWith(v) => ("startsWith", v),
        NodeFieldCondition::EndsWith(v) => ("endsWith", v),
        NodeFieldCondition::Contains(v) => ("contains", v),
        NodeFieldCondition::NotContains(v) => ("notContains", v),
        NodeFieldCondition::IsIn(v) => ("isIn", v),
        NodeFieldCondition::IsNotIn(v) => ("isNotIn", v),
    };
    format!("{{{}: {}}}", tag, render_gql_value(val))
}

fn render_prop_condition(c: &PropCondition) -> String {
    match c {
        PropCondition::Eq(v) => format!("{{eq: {}}}", render_gql_value(v)),
        PropCondition::Ne(v) => format!("{{ne: {}}}", render_gql_value(v)),
        PropCondition::Gt(v) => format!("{{gt: {}}}", render_gql_value(v)),
        PropCondition::Ge(v) => format!("{{ge: {}}}", render_gql_value(v)),
        PropCondition::Lt(v) => format!("{{lt: {}}}", render_gql_value(v)),
        PropCondition::Le(v) => format!("{{le: {}}}", render_gql_value(v)),
        PropCondition::StartsWith(v) => format!("{{startsWith: {}}}", render_gql_value(v)),
        PropCondition::EndsWith(v) => format!("{{endsWith: {}}}", render_gql_value(v)),
        PropCondition::Contains(v) => format!("{{contains: {}}}", render_gql_value(v)),
        PropCondition::NotContains(v) => format!("{{notContains: {}}}", render_gql_value(v)),
        PropCondition::IsIn(v) => format!("{{isIn: {}}}", render_gql_value(v)),
        PropCondition::IsNotIn(v) => format!("{{isNotIn: {}}}", render_gql_value(v)),
        PropCondition::IsSome(b) => format!("{{isSome: {}}}", b),
        PropCondition::IsNone(b) => format!("{{isNone: {}}}", b),
        PropCondition::And(cs) => {
            let items: Vec<_> = cs.iter().map(render_prop_condition).collect();
            format!("{{and: [{}]}}", items.join(", "))
        }
        PropCondition::Or(cs) => {
            let items: Vec<_> = cs.iter().map(render_prop_condition).collect();
            format!("{{or: [{}]}}", items.join(", "))
        }
        PropCondition::Not(inner) => format!("{{not: {}}}", render_prop_condition(inner)),
        PropCondition::First(inner) => format!("{{first: {}}}", render_prop_condition(inner)),
        PropCondition::Last(inner) => format!("{{last: {}}}", render_prop_condition(inner)),
        PropCondition::Any(inner) => format!("{{any: {}}}", render_prop_condition(inner)),
        PropCondition::All(inner) => format!("{{all: {}}}", render_prop_condition(inner)),
        PropCondition::Sum(inner) => format!("{{sum: {}}}", render_prop_condition(inner)),
        PropCondition::Avg(inner) => format!("{{avg: {}}}", render_prop_condition(inner)),
        PropCondition::Min(inner) => format!("{{min: {}}}", render_prop_condition(inner)),
        PropCondition::Max(inner) => format!("{{max: {}}}", render_prop_condition(inner)),
        PropCondition::Len(inner) => format!("{{len: {}}}", render_prop_condition(inner)),
    }
}

fn render_node_field_filter(f: &NodeFieldFilterNew) -> String {
    format!(
        "{{field: {}, where: {}}}",
        render_node_field(f.field),
        render_node_field_condition(&f.where_),
    )
}

fn render_property_filter_new(f: &PropertyFilterNew) -> String {
    format!(
        "{{name: {}, where: {}}}",
        render_gql_str(&f.name),
        render_prop_condition(&f.where_),
    )
}

fn render_degree_filter_new(f: &DegreeFilterNew) -> String {
    format!(
        "{{direction: {}, where: {}}}",
        render_degree_direction(f.direction),
        render_prop_condition(&f.where_),
    )
}

fn render_node_window_expr(e: &NodeWindowExpr) -> String {
    format!(
        "{{start: {}, end: {}, expr: {}}}",
        render_gql_time_input(&e.start),
        render_gql_time_input(&e.end),
        render_gql_node_filter(&e.expr),
    )
}

fn render_node_time_expr(e: &NodeTimeExpr) -> String {
    format!(
        "{{time: {}, expr: {}}}",
        render_gql_time_input(&e.time),
        render_gql_node_filter(&e.expr),
    )
}

fn render_node_unary_expr(e: &NodeUnaryExpr) -> String {
    format!("{{expr: {}}}", render_gql_node_filter(&e.expr))
}

fn render_node_layers_expr(e: &NodeLayersExpr) -> String {
    let names: Vec<_> = e.names.iter().map(|s| render_gql_str(s)).collect();
    format!(
        "{{names: [{}], expr: {}}}",
        names.join(", "),
        render_gql_node_filter(&e.expr),
    )
}

/// Render a `GqlNodeFilter` as a GraphQL input-object literal.
/// Walks the typed structure directly — enum-typed fields (`NodeField`,
/// `DegreeDirection`) come out as unquoted SCREAMING_SNAKE identifiers,
/// which serde JSON serialization can't produce.
fn render_gql_node_filter(f: &GqlNodeFilter) -> String {
    match f {
        GqlNodeFilter::Node(inner) => format!("{{node: {}}}", render_node_field_filter(inner)),
        GqlNodeFilter::Property(pf) => {
            format!("{{property: {}}}", render_property_filter_new(pf))
        }
        GqlNodeFilter::Metadata(pf) => {
            format!("{{metadata: {}}}", render_property_filter_new(pf))
        }
        GqlNodeFilter::TemporalProperty(pf) => {
            format!("{{temporalProperty: {}}}", render_property_filter_new(pf))
        }
        GqlNodeFilter::Degree(df) => {
            format!("{{degree: {}}}", render_degree_filter_new(df))
        }
        GqlNodeFilter::IsActive(b) => format!("{{isActive: {}}}", b),
        GqlNodeFilter::And(fs) => {
            let items: Vec<_> = fs.iter().map(render_gql_node_filter).collect();
            format!("{{and: [{}]}}", items.join(", "))
        }
        GqlNodeFilter::Or(fs) => {
            let items: Vec<_> = fs.iter().map(render_gql_node_filter).collect();
            format!("{{or: [{}]}}", items.join(", "))
        }
        GqlNodeFilter::Not(inner) => format!("{{not: {}}}", render_gql_node_filter(inner)),
        GqlNodeFilter::Window(e) => format!("{{window: {}}}", render_node_window_expr(e)),
        GqlNodeFilter::At(e) => format!("{{at: {}}}", render_node_time_expr(e)),
        GqlNodeFilter::Before(e) => format!("{{before: {}}}", render_node_time_expr(e)),
        GqlNodeFilter::After(e) => format!("{{after: {}}}", render_node_time_expr(e)),
        GqlNodeFilter::Latest(e) => format!("{{latest: {}}}", render_node_unary_expr(e)),
        GqlNodeFilter::SnapshotAt(e) => format!("{{snapshotAt: {}}}", render_node_time_expr(e)),
        GqlNodeFilter::SnapshotLatest(e) => {
            format!("{{snapshotLatest: {}}}", render_node_unary_expr(e))
        }
        GqlNodeFilter::Layers(e) => format!("{{layers: {}}}", render_node_layers_expr(e)),
    }
}

/// Same as `render_node_sort_bys` but for `EdgeSortBy` — includes the extra
/// `src` / `dst` boolean keys.
fn render_edge_sort_bys(sort_bys: &[EdgeSortBy]) -> String {
    let entries: Vec<String> = sort_bys
        .iter()
        .map(|sb| {
            let mut fields = Vec::new();
            if let Some(rev) = sb.reverse {
                fields.push(format!("reverse: {}", rev));
            }
            if let Some(src) = sb.src {
                fields.push(format!("src: {}", src));
            }
            if let Some(dst) = sb.dst {
                fields.push(format!("dst: {}", dst));
            }
            if let Some(t) = sb.time {
                fields.push(format!("time: {}", render_sort_by_time(t)));
            }
            if let Some(ref p) = sb.property {
                fields.push(format!("property: \"{}\"", p));
            }
            format!("{{{}}}", fields.join(", "))
        })
        .collect();
    format!("[{}]", entries.join(", "))
}

fn render_read_body(expr: &ReadExpr) -> String {
    match expr {
        ReadExpr::Root { path } => format!("graph(path: \"{}\")", path),
        // View chaining
        ReadExpr::Window { input, start, end } => format!(
            "{} {{ window(start: {}, end: {})",
            render_read_body(input),
            start,
            end
        ),
        ReadExpr::Layer { input, name } => {
            format!("{} {{ layer(name: \"{}\")", render_read_body(input), name)
        }
        ReadExpr::At { input, time } => {
            format!("{} {{ at(time: {})", render_read_body(input), time)
        }
        ReadExpr::Before { input, time } => {
            format!("{} {{ before(time: {})", render_read_body(input), time)
        }
        ReadExpr::After { input, time } => {
            format!("{} {{ after(time: {})", render_read_body(input), time)
        }
        ReadExpr::Latest { input } => format!("{} {{ latest", render_read_body(input)),
        ReadExpr::SnapshotLatest { input } => {
            format!("{} {{ snapshotLatest", render_read_body(input))
        }
        ReadExpr::SnapshotAt { input, time } => {
            format!("{} {{ snapshotAt(time: {})", render_read_body(input), time)
        }
        ReadExpr::ExcludeLayer { input, name } => format!(
            "{} {{ excludeLayer(name: \"{}\")",
            render_read_body(input),
            name
        ),
        ReadExpr::ShrinkWindow { input, start, end } => format!(
            "{} {{ shrinkWindow(start: {}, end: {})",
            render_read_body(input),
            start,
            end
        ),
        ReadExpr::ShrinkStart { input, start } => format!(
            "{} {{ shrinkStart(start: {})",
            render_read_body(input),
            start
        ),
        ReadExpr::ShrinkEnd { input, end } => {
            format!("{} {{ shrinkEnd(end: {})", render_read_body(input), end)
        }
        ReadExpr::Valid { input } => format!("{} {{ valid", render_read_body(input)),
        ReadExpr::DefaultLayer { input } => {
            format!("{} {{ defaultLayer", render_read_body(input))
        }
        ReadExpr::Layers { input, names } => format!(
            "{} {{ layers(names: [{}])",
            render_read_body(input),
            render_string_list(names)
        ),
        ReadExpr::ExcludeLayers { input, names } => format!(
            "{} {{ excludeLayers(names: [{}])",
            render_read_body(input),
            render_string_list(names)
        ),
        ReadExpr::Subgraph { input, nodes } => format!(
            "{} {{ subgraph(nodes: [{}])",
            render_read_body(input),
            render_string_list(nodes)
        ),
        ReadExpr::SubgraphNodeTypes { input, node_types } => format!(
            "{} {{ subgraphNodeTypes(nodeTypes: [{}])",
            render_read_body(input),
            render_string_list(node_types)
        ),
        ReadExpr::ExcludeNodes { input, nodes } => format!(
            "{} {{ excludeNodes(nodes: [{}])",
            render_read_body(input),
            render_string_list(nodes)
        ),
        ReadExpr::TypeFilter { input, node_types } => format!(
            "{} {{ typeFilter(nodeTypes: [{}])",
            render_read_body(input),
            render_string_list(node_types)
        ),
        // Selection
        ReadExpr::Node { input, id } => {
            format!("{} {{ node(name: \"{}\")", render_read_body(input), id)
        }
        ReadExpr::Edge { input, src, dst } => format!(
            "{} {{ edge(src: \"{}\", dst: \"{}\")",
            render_read_body(input),
            src,
            dst
        ),
        ReadExpr::Src { input } => format!("{} {{ src", render_read_body(input)),
        ReadExpr::Dst { input } => format!("{} {{ dst", render_read_body(input)),
        ReadExpr::Nbr { input } => format!("{} {{ nbr", render_read_body(input)),
        ReadExpr::History { input } => format!("{} {{ history", render_read_body(input)),
        ReadExpr::Deletions { input } => format!("{} {{ deletions", render_read_body(input)),
        // Sub-container navigations
        ReadExpr::HistoryTimestamps { input } => {
            format!("{} {{ timestamps", render_read_body(input))
        }
        ReadExpr::HistoryEventIds { input } => {
            format!("{} {{ eventId", render_read_body(input))
        }
        ReadExpr::HistoryDateTimes { input } => {
            format!("{} {{ datetimes", render_read_body(input))
        }
        ReadExpr::HistoryIntervals { input } => {
            format!("{} {{ intervals", render_read_body(input))
        }
        // Polymorphic sub-container terminals — render field names only;
        // return type is decided by the parent selection in `parse_read`.
        ReadExpr::SubList { input } => format!("{} {{ list", render_read_body(input)),
        ReadExpr::SubListRev { input } => format!("{} {{ listRev", render_read_body(input)),
        ReadExpr::SubPage {
            input,
            limit,
            offset,
            page_index,
        } => format!(
            "{} {{ page({})",
            render_read_body(input),
            render_page_args(*limit, *offset, *page_index),
        ),
        ReadExpr::SubPageRev {
            input,
            limit,
            offset,
            page_index,
        } => format!(
            "{} {{ pageRev({})",
            render_read_body(input),
            render_page_args(*limit, *offset, *page_index),
        ),
        // Intervals stats
        ReadExpr::IntervalsMean { input } => format!("{} {{ mean", render_read_body(input)),
        ReadExpr::IntervalsMedian { input } => format!("{} {{ median", render_read_body(input)),
        ReadExpr::IntervalsMax { input } => format!("{} {{ max", render_read_body(input)),
        ReadExpr::IntervalsMin { input } => format!("{} {{ min", render_read_body(input)),
        ReadExpr::Nodes { input } => format!("{} {{ nodes", render_read_body(input)),
        ReadExpr::Neighbours { input } => format!("{} {{ neighbours", render_read_body(input)),
        ReadExpr::InNeighbours { input } => {
            format!("{} {{ inNeighbours", render_read_body(input))
        }
        ReadExpr::OutNeighbours { input } => {
            format!("{} {{ outNeighbours", render_read_body(input))
        }
        ReadExpr::Edges { input } => format!("{} {{ edges", render_read_body(input)),
        ReadExpr::NodeEdges { input } => format!("{} {{ edges", render_read_body(input)),
        ReadExpr::InEdges { input } => format!("{} {{ inEdges", render_read_body(input)),
        ReadExpr::OutEdges { input } => format!("{} {{ outEdges", render_read_body(input)),
        ReadExpr::InComponent { input } => {
            format!("{} {{ inComponent", render_read_body(input))
        }
        ReadExpr::OutComponent { input } => {
            format!("{} {{ outComponent", render_read_body(input))
        }
        ReadExpr::Explode { input } => format!("{} {{ explode", render_read_body(input)),
        ReadExpr::ExplodeLayers { input } => {
            format!("{} {{ explodeLayers", render_read_body(input))
        }
        ReadExpr::SortedNodes { input, sort_bys } => format!(
            "{} {{ sorted(sortBys: {})",
            render_read_body(input),
            render_node_sort_bys(sort_bys)
        ),
        ReadExpr::SortedEdges { input, sort_bys } => format!(
            "{} {{ sorted(sortBys: {})",
            render_read_body(input),
            render_edge_sort_bys(sort_bys)
        ),
        ReadExpr::FilterNodes { input, filter } => {
            // Server field `filter(expr: NodeFilter!)`: applies to this
            // collection AND propagates to downstream traversals from these
            // nodes. Contrast with `SelectNodes` which renders `select` —
            // narrows membership at this step only.
            format!(
                "{} {{ filter(expr: {})",
                render_read_body(input),
                render_gql_node_filter(filter),
            )
        }
        ReadExpr::SelectNodes { input, filter } => {
            // Server field `select(expr: NodeFilter!)`: narrows the current
            // collection's membership only; downstream traversals see the
            // unfiltered graph.
            format!(
                "{} {{ select(expr: {})",
                render_read_body(input),
                render_gql_node_filter(filter),
            )
        }
        // Metadata / Properties navigation
        ReadExpr::Metadata { input } => format!("{} {{ metadata", render_read_body(input)),
        ReadExpr::Properties { input } => format!("{} {{ properties", render_read_body(input)),
        // Property terminals — `get`/`values` are compound (return {key, value}
        // records). Inner braces `{ key value }` are self-balanced; outer
        // `get` / `values` opens one net brace, contributing 1 to read_depth.
        ReadExpr::PropertyGet { input, key } => format!(
            "{} {{ get(key: \"{}\") {{ key value }}",
            render_read_body(input),
            key
        ),
        ReadExpr::PropertyContains { input, key } => {
            format!("{} {{ contains(key: \"{}\")", render_read_body(input), key)
        }
        ReadExpr::PropertyKeys { input } => format!("{} {{ keys", render_read_body(input)),
        ReadExpr::PropertyValues { input, keys } => match keys {
            Some(ks) => format!(
                "{} {{ values(keys: [{}]) {{ key value }}",
                render_read_body(input),
                render_string_list(ks)
            ),
            None => format!("{} {{ values {{ key value }}", render_read_body(input)),
        },
        ReadExpr::TemporalProperties { input } => {
            format!("{} {{ temporal", render_read_body(input))
        }
        ReadExpr::TemporalPropertyByKey { input, key } => {
            format!("{} {{ get(key: \"{}\")", render_read_body(input), key)
        }
        // `values(keys?) { key }` — we only fetch the key from each record;
        // clients build a `RemoteTemporalProperty` handle around each key.
        ReadExpr::TemporalPropertyList { input, keys } => match keys {
            Some(ks) => format!(
                "{} {{ values(keys: [{}]) {{ key }}",
                render_read_body(input),
                render_string_list(ks)
            ),
            None => format!("{} {{ values {{ key }}", render_read_body(input)),
        },
        ReadExpr::TemporalPropertyValueList { input } => {
            format!("{} {{ values", render_read_body(input))
        }
        ReadExpr::TemporalPropertyAt { input, time } => {
            format!("{} {{ at(t: {})", render_read_body(input), time)
        }
        ReadExpr::TemporalPropertyLatest { input } => {
            format!("{} {{ latest", render_read_body(input))
        }
        ReadExpr::TemporalPropertyUnique { input } => {
            format!("{} {{ unique", render_read_body(input))
        }
        ReadExpr::TemporalPropertyOrderedDedupe { input, latest_time } => format!(
            "{} {{ orderedDedupe(latestTime: {}) {{ time {{ timestamp datetime eventId }} value }}",
            render_read_body(input),
            latest_time
        ),
        ReadExpr::TemporalPropertySum { input } => {
            format!("{} {{ sum", render_read_body(input))
        }
        ReadExpr::TemporalPropertyMean { input } => {
            format!("{} {{ mean", render_read_body(input))
        }
        ReadExpr::TemporalPropertyAverage { input } => {
            format!("{} {{ average", render_read_body(input))
        }
        ReadExpr::TemporalPropertyMin { input } => format!(
            "{} {{ min {{ time {{ timestamp datetime eventId }} value }}",
            render_read_body(input)
        ),
        ReadExpr::TemporalPropertyMax { input } => format!(
            "{} {{ max {{ time {{ timestamp datetime eventId }} value }}",
            render_read_body(input)
        ),
        ReadExpr::TemporalPropertyMedian { input } => format!(
            "{} {{ median {{ time {{ timestamp datetime eventId }} value }}",
            render_read_body(input)
        ),
        // Compound-structured tree — one RPC fetches everything.
        ReadExpr::Schema { input } => format!(
            "{} {{ schema {{ \
                nodes {{ typeName properties {{ key propertyType variants }} \
                    metadata {{ key propertyType variants }} }} \
                layers {{ name edges {{ srcType dstType \
                    properties {{ key propertyType variants }} \
                    metadata {{ key propertyType variants }} }} }} }}",
            render_read_body(input),
        ),
        // Terminals — no args after the field name
        ReadExpr::CountNodes { input } => format!("{} {{ countNodes", render_read_body(input)),
        ReadExpr::CountEdges { input } => format!("{} {{ countEdges", render_read_body(input)),
        ReadExpr::Degree { input } => format!("{} {{ degree", render_read_body(input)),
        ReadExpr::InDegree { input } => format!("{} {{ inDegree", render_read_body(input)),
        ReadExpr::OutDegree { input } => format!("{} {{ outDegree", render_read_body(input)),
        ReadExpr::Name { input } => format!("{} {{ name", render_read_body(input)),
        ReadExpr::HasNode { input, id } => {
            format!("{} {{ hasNode(name: \"{}\")", render_read_body(input), id)
        }
        ReadExpr::HasEdge { input, src, dst } => format!(
            "{} {{ hasEdge(src: \"{}\", dst: \"{}\")",
            render_read_body(input),
            src,
            dst
        ),
        ReadExpr::CountTemporalEdges { input } => {
            format!("{} {{ countTemporalEdges", render_read_body(input))
        }
        ReadExpr::Path { input } => format!("{} {{ path", render_read_body(input)),
        ReadExpr::Namespace { input } => format!("{} {{ namespace", render_read_body(input)),
        ReadExpr::Created { input } => format!("{} {{ created", render_read_body(input)),
        ReadExpr::LastOpened { input } => format!("{} {{ lastOpened", render_read_body(input)),
        ReadExpr::LastUpdated { input } => format!("{} {{ lastUpdated", render_read_body(input)),
        ReadExpr::UniqueLayers { input } => format!("{} {{ uniqueLayers", render_read_body(input)),
        ReadExpr::Ids { input } => format!("{} {{ ids", render_read_body(input)),
        ReadExpr::Count { input } => format!("{} {{ count", render_read_body(input)),
        // Compound structured terminal: renders as `list { src { name } dst { name } }`.
        // The `list` field opens ONE brace that gets closed by the outer `read_depth`;
        // the inner `src { name }` / `dst { name }` groups are self-balanced.
        ReadExpr::EdgesList { input } => format!(
            "{} {{ list {{ src {{ name }} dst {{ name }} }}",
            render_read_body(input)
        ),
        // Compound structured terminal on Graph: `sharedNeighbours(selectedNodes: [ids]) { name }`
        // — opens ONE net brace (the outer, before `sharedNeighbours`); the inner
        // `{ name }` is self-balanced.
        ReadExpr::SharedNeighbours { input, ids } => format!(
            "{} {{ sharedNeighbours(selectedNodes: [{}]) {{ name }}",
            render_read_body(input),
            render_string_list(ids)
        ),
        ReadExpr::Id { input } => format!("{} {{ id", render_read_body(input)),
        ReadExpr::NodeType { input } => format!("{} {{ nodeType", render_read_body(input)),
        ReadExpr::IsActive { input } => format!("{} {{ isActive", render_read_body(input)),
        ReadExpr::IsEmpty { input } => format!("{} {{ isEmpty", render_read_body(input)),
        // Compound structured terminal: `list { timestamp datetime eventId }`
        // returns a list of records. Inner braces are self-balanced; the outer
        // `list` brace opens one net brace, contributing 1 to read_depth.
        //
        // The server's `datetime` field takes an optional format-string arg
        // (defaults to RFC 3339). We pass no arg to get the default.
        ReadExpr::HistoryList { input } => format!(
            "{} {{ list {{ timestamp datetime eventId }}",
            render_read_body(input)
        ),
        ReadExpr::HistoryListRev { input } => format!(
            "{} {{ listRev {{ timestamp datetime eventId }}",
            render_read_body(input)
        ),
        ReadExpr::HistoryPage {
            input,
            limit,
            offset,
            page_index,
        } => format!(
            "{} {{ page({}) {{ timestamp datetime eventId }}",
            render_read_body(input),
            render_page_args(*limit, *offset, *page_index),
        ),
        ReadExpr::HistoryPageRev {
            input,
            limit,
            offset,
            page_index,
        } => format!(
            "{} {{ pageRev({}) {{ timestamp datetime eventId }}",
            render_read_body(input),
            render_page_args(*limit, *offset, *page_index),
        ),
        ReadExpr::EdgeHistoryCount { input } => {
            format!("{} {{ edgeHistoryCount", render_read_body(input))
        }
        // Edge-specific terminals
        ReadExpr::EdgeIdPair { input } => format!("{} {{ id", render_read_body(input)),
        ReadExpr::LayerNames { input } => format!("{} {{ layerNames", render_read_body(input)),
        ReadExpr::LayerName { input } => format!("{} {{ layerName", render_read_body(input)),
        ReadExpr::IsValid { input } => format!("{} {{ isValid", render_read_body(input)),
        ReadExpr::IsDeleted { input } => format!("{} {{ isDeleted", render_read_body(input)),
        ReadExpr::IsSelfLoop { input } => format!("{} {{ isSelfLoop", render_read_body(input)),
        // Compound terminals — open TWO braces (outer field + `timestamp` sub-field)
        ReadExpr::EarliestTime { input } => {
            format!("{} {{ earliestTime {{ timestamp", render_read_body(input))
        }
        ReadExpr::LatestTime { input } => {
            format!("{} {{ latestTime {{ timestamp", render_read_body(input))
        }
        ReadExpr::Start { input } => {
            format!("{} {{ start {{ timestamp", render_read_body(input))
        }
        ReadExpr::End { input } => {
            format!("{} {{ end {{ timestamp", render_read_body(input))
        }
        ReadExpr::EarliestEdgeTime { input } => format!(
            "{} {{ earliestEdgeTime {{ timestamp",
            render_read_body(input)
        ),
        ReadExpr::LatestEdgeTime { input } => {
            format!("{} {{ latestEdgeTime {{ timestamp", render_read_body(input))
        }
        ReadExpr::FirstUpdate { input } => {
            format!("{} {{ firstUpdate {{ timestamp", render_read_body(input))
        }
        ReadExpr::LastUpdate { input } => {
            format!("{} {{ lastUpdate {{ timestamp", render_read_body(input))
        }
        ReadExpr::Time { input } => {
            format!("{} {{ time {{ timestamp", render_read_body(input))
        }
    }
}

fn read_depth(expr: &ReadExpr) -> usize {
    match expr {
        ReadExpr::Root { .. } => 0,
        // Single-brace variants — open one `{` each.
        ReadExpr::Window { input, .. }
        | ReadExpr::Layer { input, .. }
        | ReadExpr::At { input, .. }
        | ReadExpr::Before { input, .. }
        | ReadExpr::After { input, .. }
        | ReadExpr::Latest { input }
        | ReadExpr::SnapshotLatest { input }
        | ReadExpr::SnapshotAt { input, .. }
        | ReadExpr::ExcludeLayer { input, .. }
        | ReadExpr::ShrinkWindow { input, .. }
        | ReadExpr::ShrinkStart { input, .. }
        | ReadExpr::ShrinkEnd { input, .. }
        | ReadExpr::Valid { input }
        | ReadExpr::DefaultLayer { input }
        | ReadExpr::Layers { input, .. }
        | ReadExpr::ExcludeLayers { input, .. }
        | ReadExpr::Subgraph { input, .. }
        | ReadExpr::SubgraphNodeTypes { input, .. }
        | ReadExpr::ExcludeNodes { input, .. }
        | ReadExpr::TypeFilter { input, .. }
        | ReadExpr::Node { input, .. }
        | ReadExpr::Edge { input, .. }
        | ReadExpr::Src { input }
        | ReadExpr::Dst { input }
        | ReadExpr::Nbr { input }
        | ReadExpr::History { input }
        | ReadExpr::Deletions { input }
        | ReadExpr::Nodes { input }
        | ReadExpr::Neighbours { input }
        | ReadExpr::InNeighbours { input }
        | ReadExpr::OutNeighbours { input }
        | ReadExpr::Edges { input }
        | ReadExpr::NodeEdges { input }
        | ReadExpr::InEdges { input }
        | ReadExpr::OutEdges { input }
        | ReadExpr::InComponent { input }
        | ReadExpr::OutComponent { input }
        | ReadExpr::Explode { input }
        | ReadExpr::ExplodeLayers { input }
        | ReadExpr::SortedNodes { input, .. }
        | ReadExpr::SortedEdges { input, .. }
        | ReadExpr::FilterNodes { input, .. }
        | ReadExpr::SelectNodes { input, .. }
        | ReadExpr::Metadata { input }
        | ReadExpr::Properties { input }
        | ReadExpr::PropertyGet { input, .. }
        | ReadExpr::PropertyContains { input, .. }
        | ReadExpr::PropertyKeys { input }
        | ReadExpr::PropertyValues { input, .. }
        | ReadExpr::TemporalProperties { input }
        | ReadExpr::TemporalPropertyByKey { input, .. }
        | ReadExpr::TemporalPropertyList { input, .. }
        | ReadExpr::TemporalPropertyValueList { input }
        | ReadExpr::TemporalPropertyAt { input, .. }
        | ReadExpr::TemporalPropertyLatest { input }
        | ReadExpr::TemporalPropertyUnique { input }
        | ReadExpr::TemporalPropertyOrderedDedupe { input, .. }
        | ReadExpr::TemporalPropertySum { input }
        | ReadExpr::TemporalPropertyMean { input }
        | ReadExpr::TemporalPropertyAverage { input }
        | ReadExpr::TemporalPropertyMin { input }
        | ReadExpr::TemporalPropertyMax { input }
        | ReadExpr::TemporalPropertyMedian { input }
        | ReadExpr::Schema { input }
        | ReadExpr::Ids { input }
        | ReadExpr::Count { input }
        | ReadExpr::EdgesList { input }
        | ReadExpr::SharedNeighbours { input, .. }
        | ReadExpr::CountNodes { input }
        | ReadExpr::CountEdges { input }
        | ReadExpr::Degree { input }
        | ReadExpr::InDegree { input }
        | ReadExpr::OutDegree { input }
        | ReadExpr::Name { input }
        | ReadExpr::HasNode { input, .. }
        | ReadExpr::HasEdge { input, .. }
        | ReadExpr::CountTemporalEdges { input }
        | ReadExpr::Path { input }
        | ReadExpr::Namespace { input }
        | ReadExpr::Id { input }
        | ReadExpr::NodeType { input }
        | ReadExpr::IsActive { input }
        | ReadExpr::EdgeHistoryCount { input }
        | ReadExpr::Created { input }
        | ReadExpr::LastOpened { input }
        | ReadExpr::LastUpdated { input }
        | ReadExpr::UniqueLayers { input }
        | ReadExpr::EdgeIdPair { input }
        | ReadExpr::LayerNames { input }
        | ReadExpr::LayerName { input }
        | ReadExpr::IsValid { input }
        | ReadExpr::IsDeleted { input }
        | ReadExpr::IsSelfLoop { input }
        | ReadExpr::IsEmpty { input }
        | ReadExpr::HistoryList { input }
        | ReadExpr::HistoryListRev { input }
        | ReadExpr::HistoryPage { input, .. }
        | ReadExpr::HistoryPageRev { input, .. }
        | ReadExpr::HistoryTimestamps { input }
        | ReadExpr::HistoryEventIds { input }
        | ReadExpr::HistoryDateTimes { input }
        | ReadExpr::HistoryIntervals { input }
        | ReadExpr::SubList { input }
        | ReadExpr::SubListRev { input }
        | ReadExpr::SubPage { input, .. }
        | ReadExpr::SubPageRev { input, .. }
        | ReadExpr::IntervalsMean { input }
        | ReadExpr::IntervalsMedian { input }
        | ReadExpr::IntervalsMax { input }
        | ReadExpr::IntervalsMin { input } => 1 + read_depth(input),
        // Compound terminals — open two `{` (outer field + `timestamp` sub-field).
        ReadExpr::EarliestTime { input }
        | ReadExpr::LatestTime { input }
        | ReadExpr::Start { input }
        | ReadExpr::End { input }
        | ReadExpr::EarliestEdgeTime { input }
        | ReadExpr::LatestEdgeTime { input }
        | ReadExpr::FirstUpdate { input }
        | ReadExpr::LastUpdate { input }
        | ReadExpr::Time { input } => 2 + read_depth(input),
    }
}

/// Parses the terminal value out of the GraphQL response.
///
/// Strategy: build a root-to-terminal JSON key path from the expression tree,
/// walk the response along that path, then interpret the terminal value
/// according to the outermost expression variant.
///
/// Null-intermediate handling: if `cursor` becomes JSON `null` mid-walk, the
/// selection at that step (Node/Edge/Graph) wasn't visible under the current
/// view. Rather than let the walk fail with a confused "missing key" error,
/// we walk the `expr` tree to find the responsible selection variant and
/// raise `ClientError::NotFound` with its id. This surfaces both "absent from
/// graph" and "absent from view" as the same `NotFound` — the server response
/// can't distinguish them, and neither should we.
///
/// Note: since `RemoteGraph.node()` / `.edge()` now eagerly validate via
/// `hasNode` / `hasEdge`, most `NotFound` errors surface there, not here.
/// This path stays as a safety net for the race window between eager
/// validation and terminal execution (server-side deletion in between), and
/// for any future callers that construct a `ReadExpr::Node` / `Edge` without
/// going through the validated builder.
fn parse_read(expr: &ReadExpr, root: &JsonValue) -> Result<Option<Prop>, ClientError> {
    let path = build_json_path(expr);
    let mut cursor = root;
    for key in &path[..path.len() - 1] {
        cursor = cursor.get(*key).ok_or_else(|| {
            ClientError::InvalidResponse(format!("missing `{}` in response", key))
        })?;
        if cursor.is_null() {
            return Err(build_not_found_error(expr, key));
        }
    }
    let terminal_key = path[path.len() - 1];
    let terminal_val = cursor.get(terminal_key).ok_or_else(|| {
        ClientError::InvalidResponse(format!("missing terminal `{}` in response", terminal_key))
    })?;

    match expr {
        // i64-shaped terminals (non-null on the wire).
        ReadExpr::Degree { .. }
        | ReadExpr::InDegree { .. }
        | ReadExpr::OutDegree { .. }
        | ReadExpr::CountNodes { .. }
        | ReadExpr::CountEdges { .. }
        | ReadExpr::CountTemporalEdges { .. }
        | ReadExpr::EdgeHistoryCount { .. }
        | ReadExpr::Count { .. }
        | ReadExpr::Created { .. }
        | ReadExpr::LastOpened { .. }
        | ReadExpr::LastUpdated { .. } => terminal_val
            .as_i64()
            .map(|n| Some(Prop::I64(n)))
            .ok_or_else(|| ClientError::InvalidResponse(format!("`{}` not an i64", terminal_key))),
        // Sub-container list/page terminals dispatch by parent selection:
        // DateTimes → string list; Timestamps/EventIds/Intervals → int list.
        ReadExpr::SubList { input }
        | ReadExpr::SubListRev { input }
        | ReadExpr::SubPage { input, .. }
        | ReadExpr::SubPageRev { input, .. } => {
            let arr = terminal_val.as_array().ok_or_else(|| {
                ClientError::InvalidResponse(format!("`{}` not a JSON array", terminal_key))
            })?;
            match &**input {
                ReadExpr::HistoryDateTimes { .. } => {
                    let items: Result<Vec<Prop>, ClientError> = arr
                        .iter()
                        .map(|v| {
                            v.as_str().map(|s| Prop::Str(s.into())).ok_or_else(|| {
                                ClientError::InvalidResponse(format!(
                                    "`{}` element not a string",
                                    terminal_key
                                ))
                            })
                        })
                        .collect();
                    Ok(Some(Prop::List(items?.into())))
                }
                ReadExpr::HistoryTimestamps { .. }
                | ReadExpr::HistoryEventIds { .. }
                | ReadExpr::HistoryIntervals { .. } => {
                    let items: Result<Vec<Prop>, ClientError> = arr
                        .iter()
                        .map(|v| {
                            v.as_i64().map(Prop::I64).ok_or_else(|| {
                                ClientError::InvalidResponse(format!(
                                    "`{}` element not an i64",
                                    terminal_key
                                ))
                            })
                        })
                        .collect();
                    Ok(Some(Prop::List(items?.into())))
                }
                _ => Err(ClientError::InvalidResponse(format!(
                    "`{}` on unknown sub-container parent",
                    terminal_key
                ))),
            }
        }
        // `IntervalsMean` — `Option<f64>` scalar.
        ReadExpr::IntervalsMean { .. } => {
            if terminal_val.is_null() {
                Ok(None)
            } else {
                terminal_val
                    .as_f64()
                    .map(|n| Some(Prop::F64(n)))
                    .ok_or_else(|| {
                        ClientError::InvalidResponse(format!("`{}` not an f64", terminal_key))
                    })
            }
        }
        // List-of-string terminal — the JSON is an array of strings.
        ReadExpr::Ids { .. }
        | ReadExpr::LayerNames { .. }
        | ReadExpr::UniqueLayers { .. }
        | ReadExpr::PropertyKeys { .. } => {
            let arr = terminal_val.as_array().ok_or_else(|| {
                ClientError::InvalidResponse(format!("`{}` not a JSON array", terminal_key))
            })?;
            let items: Result<Vec<Prop>, ClientError> = arr
                .iter()
                .map(|v| {
                    v.as_str().map(|s| Prop::Str(s.into())).ok_or_else(|| {
                        ClientError::InvalidResponse(format!(
                            "`{}` element not a string",
                            terminal_key
                        ))
                    })
                })
                .collect();
            Ok(Some(Prop::List(items?.into())))
        }
        // List-of-GID terminal — each element can be a JSON string or int.
        // Used for edge `id` which returns [src, dst] as `Vec<GqlNodeId>`.
        ReadExpr::EdgeIdPair { .. } => {
            let arr = terminal_val.as_array().ok_or_else(|| {
                ClientError::InvalidResponse(format!("`{}` not a JSON array", terminal_key))
            })?;
            let items: Result<Vec<Prop>, ClientError> = arr
                .iter()
                .map(|v| {
                    if let Some(s) = v.as_str() {
                        Ok(Prop::Str(s.into()))
                    } else if let Some(n) = v.as_i64() {
                        Ok(Prop::Str(n.to_string().into()))
                    } else if let Some(n) = v.as_u64() {
                        Ok(Prop::Str(n.to_string().into()))
                    } else {
                        Err(ClientError::InvalidResponse(format!(
                            "`{}` element not a string or int",
                            terminal_key
                        )))
                    }
                })
                .collect();
            Ok(Some(Prop::List(items?.into())))
        }
        // Compound structured list terminal — JSON shape is
        // `[{"timestamp":N,"dt":"...","eventId":N}, ...]`. Any field may be
        // null. Decode each element into a `Prop::Map` (missing keys → null
        // semantically); `expect_event_time_list` unwraps to a typed
        // `Vec<RemoteEventTime>`.
        ReadExpr::HistoryList { .. }
        | ReadExpr::HistoryListRev { .. }
        | ReadExpr::HistoryPage { .. }
        | ReadExpr::HistoryPageRev { .. } => {
            let arr = terminal_val.as_array().ok_or_else(|| {
                ClientError::InvalidResponse(format!("`{}` not a JSON array", terminal_key))
            })?;
            let items: Result<Vec<Prop>, ClientError> = arr
                .iter()
                .map(|v| {
                    let obj = v.as_object().ok_or_else(|| {
                        ClientError::InvalidResponse(
                            "history event element is not a JSON object".into(),
                        )
                    })?;
                    let mut pairs: Vec<(&'static str, Prop)> = Vec::new();
                    if let Some(t) = obj.get("timestamp").and_then(|x| x.as_i64()) {
                        pairs.push(("timestamp", Prop::I64(t)));
                    }
                    if let Some(d) = obj.get("datetime").and_then(|x| x.as_str()) {
                        pairs.push(("datetime", Prop::Str(d.into())));
                    }
                    if let Some(e) = obj.get("eventId").and_then(|x| x.as_i64()) {
                        pairs.push(("eventId", Prop::I64(e)));
                    }
                    Ok(Prop::map(pairs))
                })
                .collect();
            Ok(Some(Prop::List(items?.into())))
        }
        // `sharedNeighbours { name }` — array of `{"name": "..."}` records.
        // Decode to `Prop::List(Prop::Str, ...)` matching the shape used by
        // `Ids` — the client wraps each name in a `RemoteNode`.
        ReadExpr::SharedNeighbours { .. } => {
            let arr = terminal_val.as_array().ok_or_else(|| {
                ClientError::InvalidResponse(format!("`{}` not a JSON array", terminal_key))
            })?;
            let items: Result<Vec<Prop>, ClientError> = arr
                .iter()
                .map(|v| {
                    v.get("name")
                        .and_then(|x| x.as_str())
                        .map(|s| Prop::Str(s.into()))
                        .ok_or_else(|| {
                            ClientError::InvalidResponse(
                                "sharedNeighbours element missing `name`".into(),
                            )
                        })
                })
                .collect();
            Ok(Some(Prop::List(items?.into())))
        }
        // Property terminals — each entry is a `{key, value}` record where
        // value is an untagged Prop (JSON number/string/bool/array/object).
        //
        // `PropertyGet`: single record or null. Terminal value is null when
        // the key isn't present in the container — decode as `Ok(None)`.
        ReadExpr::PropertyGet { .. } => {
            if terminal_val.is_null() {
                Ok(None)
            } else {
                Ok(Some(json_to_property_record(terminal_val)?))
            }
        }
        // `PropertyValues`: array of `{key, value}` records → `Prop::List(...)`.
        ReadExpr::PropertyValues { .. } => {
            let arr = terminal_val.as_array().ok_or_else(|| {
                ClientError::InvalidResponse(format!("`{}` not a JSON array", terminal_key))
            })?;
            let items: Result<Vec<Prop>, ClientError> =
                arr.iter().map(json_to_property_record).collect();
            Ok(Some(Prop::List(items?.into())))
        }
        // `TemporalPropertyList`: array of `{key}` records → `Prop::List` of
        // `Prop::Str`. Only the key is fetched — clients build handles.
        ReadExpr::TemporalPropertyList { .. } => {
            let arr = terminal_val.as_array().ok_or_else(|| {
                ClientError::InvalidResponse(format!("`{}` not a JSON array", terminal_key))
            })?;
            let items: Result<Vec<Prop>, ClientError> = arr
                .iter()
                .map(|v| {
                    let key = v
                        .as_object()
                        .and_then(|o| o.get("key"))
                        .and_then(|k| k.as_str())
                        .ok_or_else(|| {
                            ClientError::InvalidResponse(
                                "temporal property record missing `key`".into(),
                            )
                        })?;
                    Ok(Prop::Str(key.into()))
                })
                .collect();
            Ok(Some(Prop::List(items?.into())))
        }
        // `TemporalPropertyValueList`: array of untagged Prop values.
        ReadExpr::TemporalPropertyValueList { .. } => {
            let arr = terminal_val.as_array().ok_or_else(|| {
                ClientError::InvalidResponse(format!("`{}` not a JSON array", terminal_key))
            })?;
            let items: Result<Vec<Prop>, ClientError> = arr.iter().map(json_to_prop).collect();
            Ok(Some(Prop::List(items?.into())))
        }
        // `TemporalPropertyAt` / `TemporalPropertyLatest` / stats returning
        // a nullable untagged Prop scalar.
        ReadExpr::TemporalPropertyAt { .. }
        | ReadExpr::TemporalPropertyLatest { .. }
        | ReadExpr::TemporalPropertySum { .. }
        | ReadExpr::TemporalPropertyMean { .. }
        | ReadExpr::TemporalPropertyAverage { .. } => {
            if terminal_val.is_null() {
                Ok(None)
            } else {
                Ok(Some(json_to_prop(terminal_val)?))
            }
        }
        // `TemporalPropertyUnique`: array of untagged Prop values.
        ReadExpr::TemporalPropertyUnique { .. } => {
            let arr = terminal_val.as_array().ok_or_else(|| {
                ClientError::InvalidResponse(format!("`{}` not a JSON array", terminal_key))
            })?;
            let items: Result<Vec<Prop>, ClientError> = arr.iter().map(json_to_prop).collect();
            Ok(Some(Prop::List(items?.into())))
        }
        // `TemporalPropertyMin` / `Max` / `Median`: nullable `{time, value}`
        // record. Decode to a `Prop::Map` with keys `time` (event-time-record)
        // and `value` (untagged Prop).
        ReadExpr::TemporalPropertyMin { .. }
        | ReadExpr::TemporalPropertyMax { .. }
        | ReadExpr::TemporalPropertyMedian { .. } => {
            if terminal_val.is_null() {
                Ok(None)
            } else {
                Ok(Some(json_to_property_tuple(terminal_val)?))
            }
        }
        // `TemporalPropertyOrderedDedupe`: array of `{time, value}` records.
        ReadExpr::TemporalPropertyOrderedDedupe { .. } => {
            let arr = terminal_val.as_array().ok_or_else(|| {
                ClientError::InvalidResponse(format!("`{}` not a JSON array", terminal_key))
            })?;
            let items: Result<Vec<Prop>, ClientError> =
                arr.iter().map(json_to_property_tuple).collect();
            Ok(Some(Prop::List(items?.into())))
        }
        // `Schema`: the full nested schema tree. We reuse `json_to_prop` —
        // the response is untagged JSON strings, arrays, and objects, all of
        // which `json_to_prop` decodes natively into a nested `Prop::Map` /
        // `Prop::List` tree. The call site walks that tree to build typed
        // `RemoteGraphSchema` structs.
        ReadExpr::Schema { .. } => Ok(Some(json_to_prop(terminal_val)?)),
        // Compound structured list terminal — JSON shape is
        // `[{"src":{"name":"X"},"dst":{"name":"Y"}}, ...]`. Decode each element
        // into a 2-element inner list `[src, dst]`, wrapped in an outer list.
        ReadExpr::EdgesList { .. } => {
            let arr = terminal_val.as_array().ok_or_else(|| {
                ClientError::InvalidResponse(format!("`{}` not a JSON array", terminal_key))
            })?;
            let items: Result<Vec<Prop>, ClientError> = arr
                .iter()
                .map(|v| {
                    let src = v
                        .get("src")
                        .and_then(|s| s.get("name"))
                        .and_then(|n| n.as_str())
                        .ok_or_else(|| {
                            ClientError::InvalidResponse("edge element missing `src.name`".into())
                        })?;
                    let dst = v
                        .get("dst")
                        .and_then(|d| d.get("name"))
                        .and_then(|n| n.as_str())
                        .ok_or_else(|| {
                            ClientError::InvalidResponse("edge element missing `dst.name`".into())
                        })?;
                    Ok(Prop::List(
                        vec![Prop::Str(src.into()), Prop::Str(dst.into())].into(),
                    ))
                })
                .collect();
            Ok(Some(Prop::List(items?.into())))
        }
        // Bool-shaped terminals.
        ReadExpr::HasNode { .. }
        | ReadExpr::HasEdge { .. }
        | ReadExpr::IsActive { .. }
        | ReadExpr::IsValid { .. }
        | ReadExpr::IsDeleted { .. }
        | ReadExpr::IsSelfLoop { .. }
        | ReadExpr::IsEmpty { .. }
        | ReadExpr::PropertyContains { .. } => terminal_val
            .as_bool()
            .map(|b| Some(Prop::Bool(b)))
            .ok_or_else(|| ClientError::InvalidResponse(format!("`{}` not a bool", terminal_key))),
        // `id` can be a JSON string or number (GID scalar); coerce to string.
        ReadExpr::Id { .. } => {
            if let Some(s) = terminal_val.as_str() {
                Ok(Some(Prop::Str(s.into())))
            } else if let Some(n) = terminal_val.as_i64() {
                Ok(Some(Prop::Str(n.to_string().into())))
            } else if let Some(n) = terminal_val.as_u64() {
                Ok(Some(Prop::Str(n.to_string().into())))
            } else {
                Err(ClientError::InvalidResponse(
                    "`id` not a string or int".into(),
                ))
            }
        }
        // Nullable String terminal — server can return JSON null.
        ReadExpr::NodeType { .. } => {
            if terminal_val.is_null() {
                Ok(None)
            } else {
                terminal_val
                    .as_str()
                    .map(|s| Some(Prop::Str(s.into())))
                    .ok_or_else(|| {
                        ClientError::InvalidResponse(format!("`{}` not a string", terminal_key))
                    })
            }
        }
        // Nullable i64-shaped terminals — server can return JSON `null`
        // (e.g. an empty graph has no `earliestTime.timestamp`). We map JSON
        // null → Ok(None); a valid number → Ok(Some(Prop::I64(n))).
        ReadExpr::EarliestTime { .. }
        | ReadExpr::LatestTime { .. }
        | ReadExpr::Start { .. }
        | ReadExpr::End { .. }
        | ReadExpr::EarliestEdgeTime { .. }
        | ReadExpr::LatestEdgeTime { .. }
        | ReadExpr::FirstUpdate { .. }
        | ReadExpr::LastUpdate { .. }
        | ReadExpr::Time { .. }
        | ReadExpr::IntervalsMedian { .. }
        | ReadExpr::IntervalsMax { .. }
        | ReadExpr::IntervalsMin { .. } => {
            if terminal_val.is_null() {
                Ok(None)
            } else {
                terminal_val
                    .as_i64()
                    .map(|n| Some(Prop::I64(n)))
                    .ok_or_else(|| {
                        ClientError::InvalidResponse(format!("`{}` not an i64", terminal_key))
                    })
            }
        }
        // String-shaped terminals
        ReadExpr::Name { .. }
        | ReadExpr::Path { .. }
        | ReadExpr::Namespace { .. }
        | ReadExpr::LayerName { .. } => terminal_val
            .as_str()
            .map(|s| Some(Prop::Str(s.into())))
            .ok_or_else(|| {
                ClientError::InvalidResponse(format!("`{}` not a string", terminal_key))
            }),
        // Non-terminals — outermost expr must be a terminal in a well-formed tree.
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
            ReadExpr::Layer { input, .. } => {
                go(input, out);
                out.push("layer");
            }
            ReadExpr::At { input, .. } => {
                go(input, out);
                out.push("at");
            }
            ReadExpr::Before { input, .. } => {
                go(input, out);
                out.push("before");
            }
            ReadExpr::After { input, .. } => {
                go(input, out);
                out.push("after");
            }
            ReadExpr::Latest { input } => {
                go(input, out);
                out.push("latest");
            }
            ReadExpr::SnapshotLatest { input } => {
                go(input, out);
                out.push("snapshotLatest");
            }
            ReadExpr::SnapshotAt { input, .. } => {
                go(input, out);
                out.push("snapshotAt");
            }
            ReadExpr::ExcludeLayer { input, .. } => {
                go(input, out);
                out.push("excludeLayer");
            }
            ReadExpr::ShrinkWindow { input, .. } => {
                go(input, out);
                out.push("shrinkWindow");
            }
            ReadExpr::ShrinkStart { input, .. } => {
                go(input, out);
                out.push("shrinkStart");
            }
            ReadExpr::ShrinkEnd { input, .. } => {
                go(input, out);
                out.push("shrinkEnd");
            }
            ReadExpr::Valid { input } => {
                go(input, out);
                out.push("valid");
            }
            ReadExpr::DefaultLayer { input } => {
                go(input, out);
                out.push("defaultLayer");
            }
            ReadExpr::Layers { input, .. } => {
                go(input, out);
                out.push("layers");
            }
            ReadExpr::ExcludeLayers { input, .. } => {
                go(input, out);
                out.push("excludeLayers");
            }
            ReadExpr::Subgraph { input, .. } => {
                go(input, out);
                out.push("subgraph");
            }
            ReadExpr::SubgraphNodeTypes { input, .. } => {
                go(input, out);
                out.push("subgraphNodeTypes");
            }
            ReadExpr::ExcludeNodes { input, .. } => {
                go(input, out);
                out.push("excludeNodes");
            }
            ReadExpr::TypeFilter { input, .. } => {
                go(input, out);
                out.push("typeFilter");
            }
            ReadExpr::Node { input, .. } => {
                go(input, out);
                out.push("node");
            }
            ReadExpr::Edge { input, .. } => {
                go(input, out);
                out.push("edge");
            }
            ReadExpr::Src { input } => {
                go(input, out);
                out.push("src");
            }
            ReadExpr::Dst { input } => {
                go(input, out);
                out.push("dst");
            }
            ReadExpr::Nbr { input } => {
                go(input, out);
                out.push("nbr");
            }
            ReadExpr::History { input } => {
                go(input, out);
                out.push("history");
            }
            ReadExpr::Deletions { input } => {
                go(input, out);
                out.push("deletions");
            }
            ReadExpr::Nodes { input } => {
                go(input, out);
                out.push("nodes");
            }
            ReadExpr::Neighbours { input } => {
                go(input, out);
                out.push("neighbours");
            }
            ReadExpr::InNeighbours { input } => {
                go(input, out);
                out.push("inNeighbours");
            }
            ReadExpr::OutNeighbours { input } => {
                go(input, out);
                out.push("outNeighbours");
            }
            ReadExpr::Edges { input } => {
                go(input, out);
                out.push("edges");
            }
            ReadExpr::NodeEdges { input } => {
                go(input, out);
                out.push("edges");
            }
            ReadExpr::InEdges { input } => {
                go(input, out);
                out.push("inEdges");
            }
            ReadExpr::OutEdges { input } => {
                go(input, out);
                out.push("outEdges");
            }
            ReadExpr::InComponent { input } => {
                go(input, out);
                out.push("inComponent");
            }
            ReadExpr::OutComponent { input } => {
                go(input, out);
                out.push("outComponent");
            }
            ReadExpr::Explode { input } => {
                go(input, out);
                out.push("explode");
            }
            ReadExpr::ExplodeLayers { input } => {
                go(input, out);
                out.push("explodeLayers");
            }
            ReadExpr::SortedNodes { input, .. } => {
                go(input, out);
                out.push("sorted");
            }
            ReadExpr::SortedEdges { input, .. } => {
                go(input, out);
                out.push("sorted");
            }
            ReadExpr::FilterNodes { input, .. } => {
                go(input, out);
                out.push("filter");
            }
            ReadExpr::SelectNodes { input, .. } => {
                go(input, out);
                out.push("select");
            }
            ReadExpr::Metadata { input } => {
                go(input, out);
                out.push("metadata");
            }
            ReadExpr::Properties { input } => {
                go(input, out);
                out.push("properties");
            }
            ReadExpr::PropertyGet { input, .. } => {
                go(input, out);
                out.push("get");
            }
            ReadExpr::PropertyContains { input, .. } => {
                go(input, out);
                out.push("contains");
            }
            ReadExpr::PropertyKeys { input } => {
                go(input, out);
                out.push("keys");
            }
            ReadExpr::PropertyValues { input, .. } => {
                go(input, out);
                out.push("values");
            }
            ReadExpr::TemporalProperties { input } => {
                go(input, out);
                out.push("temporal");
            }
            ReadExpr::TemporalPropertyByKey { input, .. } => {
                go(input, out);
                out.push("get");
            }
            ReadExpr::TemporalPropertyList { input, .. } => {
                go(input, out);
                out.push("values");
            }
            ReadExpr::TemporalPropertyValueList { input } => {
                go(input, out);
                out.push("values");
            }
            ReadExpr::TemporalPropertyAt { input, .. } => {
                go(input, out);
                out.push("at");
            }
            ReadExpr::TemporalPropertyLatest { input } => {
                go(input, out);
                out.push("latest");
            }
            ReadExpr::TemporalPropertyUnique { input } => {
                go(input, out);
                out.push("unique");
            }
            ReadExpr::TemporalPropertyOrderedDedupe { input, .. } => {
                go(input, out);
                out.push("orderedDedupe");
            }
            ReadExpr::TemporalPropertySum { input } => {
                go(input, out);
                out.push("sum");
            }
            ReadExpr::TemporalPropertyMean { input } => {
                go(input, out);
                out.push("mean");
            }
            ReadExpr::TemporalPropertyAverage { input } => {
                go(input, out);
                out.push("average");
            }
            ReadExpr::TemporalPropertyMin { input } => {
                go(input, out);
                out.push("min");
            }
            ReadExpr::TemporalPropertyMax { input } => {
                go(input, out);
                out.push("max");
            }
            ReadExpr::TemporalPropertyMedian { input } => {
                go(input, out);
                out.push("median");
            }
            ReadExpr::Schema { input } => {
                go(input, out);
                out.push("schema");
            }
            ReadExpr::Ids { input } => {
                go(input, out);
                out.push("ids");
            }
            ReadExpr::Count { input } => {
                go(input, out);
                out.push("count");
            }
            ReadExpr::EdgesList { input } => {
                go(input, out);
                out.push("list");
            }
            ReadExpr::SharedNeighbours { input, .. } => {
                go(input, out);
                out.push("sharedNeighbours");
            }
            ReadExpr::CountNodes { input } => {
                go(input, out);
                out.push("countNodes");
            }
            ReadExpr::CountEdges { input } => {
                go(input, out);
                out.push("countEdges");
            }
            ReadExpr::Degree { input } => {
                go(input, out);
                out.push("degree");
            }
            ReadExpr::InDegree { input } => {
                go(input, out);
                out.push("inDegree");
            }
            ReadExpr::OutDegree { input } => {
                go(input, out);
                out.push("outDegree");
            }
            ReadExpr::Name { input } => {
                go(input, out);
                out.push("name");
            }
            ReadExpr::HasNode { input, .. } => {
                go(input, out);
                out.push("hasNode");
            }
            ReadExpr::HasEdge { input, .. } => {
                go(input, out);
                out.push("hasEdge");
            }
            ReadExpr::CountTemporalEdges { input } => {
                go(input, out);
                out.push("countTemporalEdges");
            }
            ReadExpr::Path { input } => {
                go(input, out);
                out.push("path");
            }
            ReadExpr::Namespace { input } => {
                go(input, out);
                out.push("namespace");
            }
            ReadExpr::Created { input } => {
                go(input, out);
                out.push("created");
            }
            ReadExpr::LastOpened { input } => {
                go(input, out);
                out.push("lastOpened");
            }
            ReadExpr::LastUpdated { input } => {
                go(input, out);
                out.push("lastUpdated");
            }
            ReadExpr::UniqueLayers { input } => {
                go(input, out);
                out.push("uniqueLayers");
            }
            ReadExpr::Id { input } => {
                go(input, out);
                out.push("id");
            }
            ReadExpr::NodeType { input } => {
                go(input, out);
                out.push("nodeType");
            }
            ReadExpr::IsActive { input } => {
                go(input, out);
                out.push("isActive");
            }
            ReadExpr::IsEmpty { input } => {
                go(input, out);
                out.push("isEmpty");
            }
            ReadExpr::HistoryList { input } => {
                go(input, out);
                out.push("list");
            }
            ReadExpr::HistoryListRev { input } => {
                go(input, out);
                out.push("listRev");
            }
            ReadExpr::HistoryPage { input, .. } => {
                go(input, out);
                out.push("page");
            }
            ReadExpr::HistoryPageRev { input, .. } => {
                go(input, out);
                out.push("pageRev");
            }
            ReadExpr::HistoryTimestamps { input } => {
                go(input, out);
                out.push("timestamps");
            }
            ReadExpr::HistoryEventIds { input } => {
                go(input, out);
                out.push("eventId");
            }
            ReadExpr::HistoryDateTimes { input } => {
                go(input, out);
                out.push("datetimes");
            }
            ReadExpr::HistoryIntervals { input } => {
                go(input, out);
                out.push("intervals");
            }
            ReadExpr::SubList { input } => {
                go(input, out);
                out.push("list");
            }
            ReadExpr::SubListRev { input } => {
                go(input, out);
                out.push("listRev");
            }
            ReadExpr::SubPage { input, .. } => {
                go(input, out);
                out.push("page");
            }
            ReadExpr::SubPageRev { input, .. } => {
                go(input, out);
                out.push("pageRev");
            }
            ReadExpr::IntervalsMean { input } => {
                go(input, out);
                out.push("mean");
            }
            ReadExpr::IntervalsMedian { input } => {
                go(input, out);
                out.push("median");
            }
            ReadExpr::IntervalsMax { input } => {
                go(input, out);
                out.push("max");
            }
            ReadExpr::IntervalsMin { input } => {
                go(input, out);
                out.push("min");
            }
            ReadExpr::EdgeHistoryCount { input } => {
                go(input, out);
                out.push("edgeHistoryCount");
            }
            ReadExpr::EdgeIdPair { input } => {
                go(input, out);
                out.push("id");
            }
            ReadExpr::LayerNames { input } => {
                go(input, out);
                out.push("layerNames");
            }
            ReadExpr::LayerName { input } => {
                go(input, out);
                out.push("layerName");
            }
            ReadExpr::IsValid { input } => {
                go(input, out);
                out.push("isValid");
            }
            ReadExpr::IsDeleted { input } => {
                go(input, out);
                out.push("isDeleted");
            }
            ReadExpr::IsSelfLoop { input } => {
                go(input, out);
                out.push("isSelfLoop");
            }
            // Compound terminals — push TWO keys (outer field + "timestamp").
            ReadExpr::EarliestTime { input } => {
                go(input, out);
                out.push("earliestTime");
                out.push("timestamp");
            }
            ReadExpr::LatestTime { input } => {
                go(input, out);
                out.push("latestTime");
                out.push("timestamp");
            }
            ReadExpr::Start { input } => {
                go(input, out);
                out.push("start");
                out.push("timestamp");
            }
            ReadExpr::End { input } => {
                go(input, out);
                out.push("end");
                out.push("timestamp");
            }
            ReadExpr::EarliestEdgeTime { input } => {
                go(input, out);
                out.push("earliestEdgeTime");
                out.push("timestamp");
            }
            ReadExpr::LatestEdgeTime { input } => {
                go(input, out);
                out.push("latestEdgeTime");
                out.push("timestamp");
            }
            ReadExpr::FirstUpdate { input } => {
                go(input, out);
                out.push("firstUpdate");
                out.push("timestamp");
            }
            ReadExpr::LastUpdate { input } => {
                go(input, out);
                out.push("lastUpdate");
                out.push("timestamp");
            }
            ReadExpr::Time { input } => {
                go(input, out);
                out.push("time");
                out.push("timestamp");
            }
        }
    }
    let mut out = Vec::new();
    go(expr, &mut out);
    out
}

/// Decode an untagged JSON value into a `Prop`. Mirrors the server's
/// `prop_to_gql` — server serializes `Prop` as native JSON (number / string /
/// bool / array / object) with no type tag. Recovering the exact original
/// variant isn't possible for numbers (I64 vs F64 vs DTime all wire as
/// numbers) — we pick the widest fitting variant.
fn json_to_prop(v: &JsonValue) -> Result<Prop, ClientError> {
    match v {
        JsonValue::Bool(b) => Ok(Prop::Bool(*b)),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Prop::I64(i))
            } else if let Some(f) = n.as_f64() {
                Ok(Prop::F64(f))
            } else {
                Err(ClientError::InvalidResponse(
                    "prop number not representable as i64 or f64".into(),
                ))
            }
        }
        JsonValue::String(s) => Ok(Prop::Str(s.as_str().into())),
        JsonValue::Array(arr) => {
            let items: Result<Vec<Prop>, ClientError> = arr.iter().map(json_to_prop).collect();
            Ok(Prop::List(items?.into()))
        }
        JsonValue::Object(obj) => {
            let pairs: Result<Vec<(&str, Prop)>, ClientError> = obj
                .iter()
                .map(|(k, v)| json_to_prop(v).map(|p| (k.as_str(), p)))
                .collect();
            Ok(Prop::map(pairs?))
        }
        JsonValue::Null => Err(ClientError::InvalidResponse("prop is null".into())),
    }
}

/// Decode a `{ time: {timestamp, datetime, eventId}, value }` JSON record
/// into a `Prop::Map` with `"time"` (nested Prop::Map matching the event-
/// time shape used elsewhere) and `"value"` (arbitrary Prop). Used by
/// TemporalProperty stats (`min`/`max`/`median`) and `ordered_dedupe`.
fn json_to_property_tuple(v: &JsonValue) -> Result<Prop, ClientError> {
    let obj = v.as_object().ok_or_else(|| {
        ClientError::InvalidResponse("property tuple is not a JSON object".into())
    })?;
    let time_json = obj
        .get("time")
        .ok_or_else(|| ClientError::InvalidResponse("property tuple missing `time`".into()))?;
    let value_json = obj
        .get("value")
        .ok_or_else(|| ClientError::InvalidResponse("property tuple missing `value`".into()))?;

    let time_obj = time_json.as_object().ok_or_else(|| {
        ClientError::InvalidResponse("property tuple `time` is not a JSON object".into())
    })?;
    let mut time_pairs: Vec<(&'static str, Prop)> = Vec::new();
    if let Some(t) = time_obj.get("timestamp").and_then(|x| x.as_i64()) {
        time_pairs.push(("timestamp", Prop::I64(t)));
    }
    if let Some(d) = time_obj.get("datetime").and_then(|x| x.as_str()) {
        time_pairs.push(("datetime", Prop::Str(d.into())));
    }
    if let Some(e) = time_obj.get("eventId").and_then(|x| x.as_i64()) {
        time_pairs.push(("eventId", Prop::I64(e)));
    }
    let time_map = Prop::map(time_pairs);
    let value = json_to_prop(value_json)?;
    Ok(Prop::map(vec![("time", time_map), ("value", value)]))
}

/// Decode a `{ key, value }` JSON record into a `Prop::Map` with `"key"` (Prop::Str)
/// and `"value"` (arbitrary Prop). Used by property terminals.
fn json_to_property_record(v: &JsonValue) -> Result<Prop, ClientError> {
    let obj = v.as_object().ok_or_else(|| {
        ClientError::InvalidResponse("property record is not a JSON object".into())
    })?;
    let key = obj
        .get("key")
        .and_then(|k| k.as_str())
        .ok_or_else(|| ClientError::InvalidResponse("property record missing `key`".into()))?;
    let value_json = obj
        .get("value")
        .ok_or_else(|| ClientError::InvalidResponse("property record missing `value`".into()))?;
    let value = json_to_prop(value_json)?;
    Ok(Prop::map(vec![
        ("key", Prop::Str(key.into())),
        ("value", value),
    ]))
}

/// Build a `NotFound` error describing which Node/Edge/Graph selection
/// returned `null` in the response. Walks the `expr` tree from outermost
/// inward to find the variant whose json key matches `null_key`.
fn build_not_found_error(expr: &ReadExpr, null_key: &str) -> ClientError {
    let desc = find_selection(expr, null_key)
        .unwrap_or_else(|| format!("unexpected null at `{}`", null_key));
    ClientError::NotFound(desc)
}

/// Descend the expr tree, returning a describing string for the selection
/// variant whose `build_json_path` key matches `null_key`. Returns `None` if
/// no matching variant is found in the tree.
fn find_selection(expr: &ReadExpr, null_key: &str) -> Option<String> {
    let this = match expr {
        ReadExpr::Root { path } if null_key == "graph" => Some(format!("Graph '{}'", path)),
        ReadExpr::Node { id, .. } if null_key == "node" => Some(format!("Node '{}'", id)),
        ReadExpr::Edge { src, dst, .. } if null_key == "edge" => {
            Some(format!("Edge ('{}', '{}')", src, dst))
        }
        _ => None,
    };
    if this.is_some() {
        return this;
    }
    child_input(expr).and_then(|inp| find_selection(inp, null_key))
}

/// Returns the `input` field of an expr variant (i.e. its child in the tree),
/// or `None` for `Root` which has no input.
fn child_input(expr: &ReadExpr) -> Option<&ReadExpr> {
    match expr {
        ReadExpr::Root { .. } => None,
        ReadExpr::Window { input, .. }
        | ReadExpr::Layer { input, .. }
        | ReadExpr::At { input, .. }
        | ReadExpr::Before { input, .. }
        | ReadExpr::After { input, .. }
        | ReadExpr::Latest { input }
        | ReadExpr::SnapshotLatest { input }
        | ReadExpr::SnapshotAt { input, .. }
        | ReadExpr::ExcludeLayer { input, .. }
        | ReadExpr::ShrinkWindow { input, .. }
        | ReadExpr::ShrinkStart { input, .. }
        | ReadExpr::ShrinkEnd { input, .. }
        | ReadExpr::Valid { input }
        | ReadExpr::DefaultLayer { input }
        | ReadExpr::Layers { input, .. }
        | ReadExpr::ExcludeLayers { input, .. }
        | ReadExpr::Subgraph { input, .. }
        | ReadExpr::SubgraphNodeTypes { input, .. }
        | ReadExpr::ExcludeNodes { input, .. }
        | ReadExpr::TypeFilter { input, .. }
        | ReadExpr::Node { input, .. }
        | ReadExpr::Edge { input, .. }
        | ReadExpr::Src { input }
        | ReadExpr::Dst { input }
        | ReadExpr::Nbr { input }
        | ReadExpr::History { input }
        | ReadExpr::Deletions { input }
        | ReadExpr::Nodes { input }
        | ReadExpr::Neighbours { input }
        | ReadExpr::InNeighbours { input }
        | ReadExpr::OutNeighbours { input }
        | ReadExpr::Edges { input }
        | ReadExpr::NodeEdges { input }
        | ReadExpr::InEdges { input }
        | ReadExpr::OutEdges { input }
        | ReadExpr::InComponent { input }
        | ReadExpr::OutComponent { input }
        | ReadExpr::Explode { input }
        | ReadExpr::ExplodeLayers { input }
        | ReadExpr::SortedNodes { input, .. }
        | ReadExpr::SortedEdges { input, .. }
        | ReadExpr::FilterNodes { input, .. }
        | ReadExpr::SelectNodes { input, .. }
        | ReadExpr::Metadata { input }
        | ReadExpr::Properties { input }
        | ReadExpr::PropertyGet { input, .. }
        | ReadExpr::PropertyContains { input, .. }
        | ReadExpr::PropertyKeys { input }
        | ReadExpr::PropertyValues { input, .. }
        | ReadExpr::TemporalProperties { input }
        | ReadExpr::TemporalPropertyByKey { input, .. }
        | ReadExpr::TemporalPropertyList { input, .. }
        | ReadExpr::TemporalPropertyValueList { input }
        | ReadExpr::TemporalPropertyAt { input, .. }
        | ReadExpr::TemporalPropertyLatest { input }
        | ReadExpr::TemporalPropertyUnique { input }
        | ReadExpr::TemporalPropertyOrderedDedupe { input, .. }
        | ReadExpr::TemporalPropertySum { input }
        | ReadExpr::TemporalPropertyMean { input }
        | ReadExpr::TemporalPropertyAverage { input }
        | ReadExpr::TemporalPropertyMin { input }
        | ReadExpr::TemporalPropertyMax { input }
        | ReadExpr::TemporalPropertyMedian { input }
        | ReadExpr::Schema { input }
        | ReadExpr::Ids { input }
        | ReadExpr::Count { input }
        | ReadExpr::EdgesList { input }
        | ReadExpr::SharedNeighbours { input, .. }
        | ReadExpr::CountNodes { input }
        | ReadExpr::CountEdges { input }
        | ReadExpr::Degree { input }
        | ReadExpr::InDegree { input }
        | ReadExpr::OutDegree { input }
        | ReadExpr::Name { input }
        | ReadExpr::HasNode { input, .. }
        | ReadExpr::HasEdge { input, .. }
        | ReadExpr::CountTemporalEdges { input }
        | ReadExpr::Path { input }
        | ReadExpr::Namespace { input }
        | ReadExpr::Id { input }
        | ReadExpr::NodeType { input }
        | ReadExpr::IsActive { input }
        | ReadExpr::EdgeHistoryCount { input }
        | ReadExpr::Created { input }
        | ReadExpr::LastOpened { input }
        | ReadExpr::LastUpdated { input }
        | ReadExpr::UniqueLayers { input }
        | ReadExpr::EarliestTime { input }
        | ReadExpr::LatestTime { input }
        | ReadExpr::Start { input }
        | ReadExpr::End { input }
        | ReadExpr::EarliestEdgeTime { input }
        | ReadExpr::LatestEdgeTime { input }
        | ReadExpr::FirstUpdate { input }
        | ReadExpr::LastUpdate { input }
        | ReadExpr::Time { input }
        | ReadExpr::EdgeIdPair { input }
        | ReadExpr::LayerNames { input }
        | ReadExpr::LayerName { input }
        | ReadExpr::IsValid { input }
        | ReadExpr::IsDeleted { input }
        | ReadExpr::IsSelfLoop { input }
        | ReadExpr::IsEmpty { input }
        | ReadExpr::HistoryList { input }
        | ReadExpr::HistoryListRev { input }
        | ReadExpr::HistoryPage { input, .. }
        | ReadExpr::HistoryPageRev { input, .. }
        | ReadExpr::HistoryTimestamps { input }
        | ReadExpr::HistoryEventIds { input }
        | ReadExpr::HistoryDateTimes { input }
        | ReadExpr::HistoryIntervals { input }
        | ReadExpr::SubList { input }
        | ReadExpr::SubListRev { input }
        | ReadExpr::SubPage { input, .. }
        | ReadExpr::SubPageRev { input, .. }
        | ReadExpr::IntervalsMean { input }
        | ReadExpr::IntervalsMedian { input }
        | ReadExpr::IntervalsMax { input }
        | ReadExpr::IntervalsMin { input } => Some(input),
    }
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
        // `.node()` now fires a hasNode validation RPC before returning the handle.
        let degree = rg.node("ben").await.unwrap().degree().await.unwrap();
        assert_eq!(degree, 1, "ben should have degree 1 (single edge to hamza)");

        // With a windowed view, we can restrict to a time range.
        // Window (0, 5) includes the edge added at time 3, so degree is still 1.
        let degree_windowed = rg
            .window(0, 5)
            .node("ben")
            .await
            .unwrap()
            .degree()
            .await
            .unwrap();
        assert_eq!(degree_windowed, 1);

        // Window (0, 2) excludes the edge (added at time 3), but ben himself
        // was added at t=1 so he's still in the view — his degree is 0.
        let degree_before_edge = rg
            .window(0, 2)
            .node("ben")
            .await
            .unwrap()
            .degree()
            .await
            .unwrap();
        assert_eq!(degree_before_edge, 0);

        // A window that excludes ben's add_node event entirely — `.node()`
        // validates against the view chain and raises NotFound.
        match rg.window(100, 200).node("ben").await {
            Err(ClientError::NotFound(msg)) => {
                assert!(msg.contains("ben"), "expected 'ben' in message, got: {msg}");
            }
            Err(e) => panic!("expected NotFound for ben under window [100, 200), got {e:?}"),
            Ok(_) => panic!("expected NotFound for ben under window [100, 200), got Ok"),
        }

        running.stop().await;
    }
}
