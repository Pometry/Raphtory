//! GraphQL implementation of `Transport`.
//!
//! Renders `Op` variants into GraphQL queries against the existing server and
//! parses responses back into `Option<Prop>`. All wire logic lives here so
//! client wrappers (`RemoteGraph`, `RemoteNode`, ...) stay transport-agnostic.

use crate::{
    client::{
        op::{
            AddEdge, AddEdgeMetadata, AddEdgeUpdates, AddEdges, AddGraphMetadata, AddGraphProperty,
            AddNode, AddNodeMetadata, AddNodeUpdates, AddNodes, CreateNode, DeleteEdge,
            DeleteEdgeAtTime, EdgeSortBy, InputTime, NodeSortBy, Op, ReadExpr, SetNodeType,
            SortByTime, UpdateEdgeMetadata, UpdateGraphMetadata, UpdateNodeMetadata, ViewOp,
            WriteOp,
        },
        properties_to_input,
        remote_client::RemoteClient,
        transport::Transport,
        ClientError,
    },
    model::graph::property::gql_to_prop,
};
use async_graphql::{async_trait, Value as GqlValue};
use raphtory_api::core::entities::properties::prop::Prop;
use serde::Serialize;
use serde_json::{json, Value as JsonValue};
use std::collections::HashMap;

/// Build the `TimeInput` variable value: a bare int, or `{timestamp, eventId}`
/// when an explicit secondary index is given.
fn time_input_var(time: i64, event_id: Option<usize>) -> JsonValue {
    match event_id {
        Some(event_id) => json!({ "timestamp": time, "eventId": event_id }),
        None => json!(time),
    }
}

/// Render an `InputTime` as a `TimeInput` GraphQL **literal** (for view-op args
/// spliced into the query text): a bare int for `Simple`, or the object form
/// `{timestamp, eventId}` for `Indexed`. `Simple` vs `Indexed` comes straight
/// from what the caller passed (plain timestamp vs `(t, id)` tuple), so a plain
/// timestamp stays a bare int with no heuristic.
fn render_input_time(t: &InputTime) -> String {
    match t {
        InputTime::Simple(ts) => ts.to_string(),
        InputTime::Indexed(ts, id) => format!("{{timestamp: {ts}, eventId: {id}}}"),
    }
}

/// Build the `TimeInput` variable value from an `InputTime` (write-path times).
fn input_time_var(t: &InputTime) -> JsonValue {
    match t {
        InputTime::Simple(ts) => time_input_var(*ts, None),
        InputTime::Indexed(ts, id) => time_input_var(*ts, Some(*id)),
    }
}

/// Serialize a variable payload, surfacing serialization failures (e.g. a
/// non-finite float rejected by `Value`'s serializer) as `InvalidInput`.
/// `json!` would panic on the same failure — never use it for fallible types.
fn to_var<T: Serialize>(value: &T) -> Result<JsonValue, ClientError> {
    serde_json::to_value(value).map_err(|e| ClientError::InvalidInput(e.to_string()))
}

/// Build a `[PropertyInput!]` variable value from a property map.
fn properties_var(properties: &HashMap<String, Prop>) -> Result<JsonValue, ClientError> {
    to_var(&properties_to_input(properties)?)
}

/// Build an optional `[PropertyInput!]` variable — JSON `null` when absent,
/// which GraphQL treats the same as an omitted optional argument.
fn opt_properties_var(
    properties: &Option<HashMap<String, Prop>>,
) -> Result<JsonValue, ClientError> {
    match properties {
        Some(p) => properties_var(p),
        None => Ok(JsonValue::Null),
    }
}

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
        let query = r#"
        query($path: String!, $time: TimeInput!, $name: NodeId!,
                 $properties: [PropertyInput!], $nodeType: String, $layer: String) {
            updateGraph(path: $path) {
                addNode(time: $time, name: $name, properties: $properties,
                        nodeType: $nodeType, layer: $layer) {
                    success
                }
            }
        }
        "#;

        let variables = json!({
            "path": json!(args.path),
            "time": input_time_var(&args.time),
            "name": json!(args.id),
            "properties": opt_properties_var(&args.properties)?,
            "nodeType": json!(args.node_type),
            "layer": json!(args.layer),
        });
        let res = self.client.query(query, variables).await?;

        expect_update_success(&res, "addNode")?;
        Ok(None)
    }

    async fn apply_create_node(&self, args: &CreateNode) -> Result<Option<Prop>, ClientError> {
        let query = r#"
        query($path: String!, $time: TimeInput!, $name: NodeId!,
                 $properties: [PropertyInput!], $nodeType: String, $layer: String) {
            updateGraph(path: $path) {
                createNode(time: $time, name: $name, properties: $properties,
                           nodeType: $nodeType, layer: $layer) {
                    success
                }
            }
        }
        "#;

        let variables = json!({
            "path": json!(args.path),
            "time": input_time_var(&args.time),
            "name": json!(args.id),
            "properties": opt_properties_var(&args.properties)?,
            "nodeType": json!(args.node_type),
            "layer": json!(args.layer),
        });
        let res = self.client.query(query, variables).await?;

        expect_update_success(&res, "createNode")?;
        Ok(None)
    }

    async fn apply_add_edge(&self, args: &AddEdge) -> Result<Option<Prop>, ClientError> {
        let query = r#"
        query($path: String!, $time: TimeInput!, $src: NodeId!, $dst: NodeId!,
                 $properties: [PropertyInput!], $layer: String) {
            updateGraph(path: $path) {
                addEdge(time: $time, src: $src, dst: $dst,
                        properties: $properties, layer: $layer) {
                    success
                }
            }
        }
        "#;

        let variables = json!({
            "path": json!(args.path),
            "time": input_time_var(&args.time),
            "src": json!(args.src),
            "dst": json!(args.dst),
            "properties": opt_properties_var(&args.properties)?,
            "layer": json!(args.layer),
        });
        let res = self.client.query(query, variables).await?;

        expect_update_success(&res, "addEdge")?;
        Ok(None)
    }

    async fn apply_add_graph_property(
        &self,
        args: &AddGraphProperty,
    ) -> Result<Option<Prop>, ClientError> {
        let query = r#"
        query($path: String!, $t: TimeInput!, $properties: [PropertyInput!]!) {
          updateGraph(path: $path) {
            addProperties(t: $t, properties: $properties)
          }
        }
        "#;

        let variables = json!({
            "path": json!(args.path),
            "t": input_time_var(&args.time),
            "properties": properties_var(&args.properties)?,
        });
        let res = self.client.query(query, variables).await?;

        expect_update_bool(&res, "addProperties")?;
        Ok(None)
    }

    async fn apply_add_graph_metadata(
        &self,
        args: &AddGraphMetadata,
    ) -> Result<Option<Prop>, ClientError> {
        let query = r#"
        query($path: String!, $properties: [PropertyInput!]!) {
          updateGraph(path: $path) {
            addMetadata(properties: $properties)
          }
        }
        "#;

        let variables = json!({
            "path": json!(args.path),
            "properties": properties_var(&args.properties)?,
        });
        let res = self.client.query(query, variables).await?;

        expect_update_bool(&res, "addMetadata")?;
        Ok(None)
    }

    async fn apply_update_graph_metadata(
        &self,
        args: &UpdateGraphMetadata,
    ) -> Result<Option<Prop>, ClientError> {
        let query = r#"
        query($path: String!, $properties: [PropertyInput!]!) {
          updateGraph(path: $path) {
            updateMetadata(properties: $properties)
          }
        }
        "#;

        let variables = json!({
            "path": json!(args.path),
            "properties": properties_var(&args.properties)?,
        });
        let res = self.client.query(query, variables).await?;

        expect_update_bool(&res, "updateMetadata")?;
        Ok(None)
    }

    async fn apply_delete_edge(&self, args: &DeleteEdge) -> Result<Option<Prop>, ClientError> {
        let query = r#"
        query($path: String!, $time: TimeInput!, $src: NodeId!, $dst: NodeId!,
                 $layer: String) {
            updateGraph(path: $path) {
                deleteEdge(time: $time, src: $src, dst: $dst, layer: $layer) {
                    success
                }
            }
        }
        "#;

        let variables = json!({
            "path": json!(args.path),
            "time": input_time_var(&args.time),
            "src": json!(args.src),
            "dst": json!(args.dst),
            "layer": json!(args.layer),
        });
        let res = self.client.query(query, variables).await?;

        expect_update_success(&res, "deleteEdge")?;
        Ok(None)
    }

    async fn apply_set_node_type(&self, args: &SetNodeType) -> Result<Option<Prop>, ClientError> {
        let query = r#"
            query($path: String!, $name: NodeId!, $newType: String!) {
              updateGraph(path: $path) {
                node(name: $name) {
                  setNodeType(newType: $newType)
                }
              }
            }
        "#;

        let variables = json!({
            "path": json!(args.path),
            "name": json!(args.id),
            "newType": json!(args.new_type),
        });
        let res = self.client.query(query, variables).await?;
        ensure_write_target_present(&res, "node", format!("node '{}'", args.id))?;
        Ok(None)
    }

    async fn apply_add_node_updates(
        &self,
        args: &AddNodeUpdates,
    ) -> Result<Option<Prop>, ClientError> {
        let query = r#"
            query($path: String!, $name: NodeId!, $time: TimeInput!,
                     $properties: [PropertyInput!]) {
              updateGraph(path: $path) {
                node(name: $name) {
                  addUpdates(time: $time, properties: $properties)
                }
              }
            }
        "#;

        let variables = json!({
            "path": json!(args.path),
            "name": json!(args.id),
            "time": input_time_var(&args.time),
            "properties": opt_properties_var(&args.properties)?,
        });
        let res = self.client.query(query, variables).await?;
        ensure_write_target_present(&res, "node", format!("node '{}'", args.id))?;
        Ok(None)
    }

    async fn apply_add_node_metadata(
        &self,
        args: &AddNodeMetadata,
    ) -> Result<Option<Prop>, ClientError> {
        let query = r#"
            query($path: String!, $name: NodeId!, $properties: [PropertyInput!]!) {
              updateGraph(path: $path) {
                node(name: $name) {
                  addMetadata(properties: $properties)
                }
              }
            }
        "#;

        let variables = json!({
            "path": json!(args.path),
            "name": json!(args.id),
            "properties": properties_var(&args.properties)?,
        });
        let res = self.client.query(query, variables).await?;
        ensure_write_target_present(&res, "node", format!("node '{}'", args.id))?;
        Ok(None)
    }

    async fn apply_update_node_metadata(
        &self,
        args: &UpdateNodeMetadata,
    ) -> Result<Option<Prop>, ClientError> {
        let query = r#"
            query($path: String!, $name: NodeId!, $properties: [PropertyInput!]!) {
              updateGraph(path: $path) {
                node(name: $name) {
                  updateMetadata(properties: $properties)
                }
              }
            }
        "#;

        let variables = json!({
            "path": json!(args.path),
            "name": json!(args.id),
            "properties": properties_var(&args.properties)?,
        });
        let res = self.client.query(query, variables).await?;
        ensure_write_target_present(&res, "node", format!("node '{}'", args.id))?;
        Ok(None)
    }

    async fn apply_add_edge_updates(
        &self,
        args: &AddEdgeUpdates,
    ) -> Result<Option<Prop>, ClientError> {
        let query = r#"
            query($path: String!, $src: NodeId!, $dst: NodeId!, $time: TimeInput!,
                     $properties: [PropertyInput!], $layer: String) {
              updateGraph(path: $path) {
                edge(src: $src, dst: $dst) {
                  addUpdates(time: $time, properties: $properties, layer: $layer)
                }
              }
            }
        "#;

        let variables = json!({
            "path": json!(args.path),
            "src": json!(args.src),
            "dst": json!(args.dst),
            "time": input_time_var(&args.time),
            "properties": opt_properties_var(&args.properties)?,
            "layer": json!(args.layer),
        });
        let res = self.client.query(query, variables).await?;
        ensure_write_target_present(
            &res,
            "edge",
            format!("edge '{}' -> '{}'", args.src, args.dst),
        )?;
        Ok(None)
    }

    async fn apply_delete_edge_at_time(
        &self,
        args: &DeleteEdgeAtTime,
    ) -> Result<Option<Prop>, ClientError> {
        let query = r#"
            query($path: String!, $src: NodeId!, $dst: NodeId!, $time: TimeInput!,
                     $layer: String) {
              updateGraph(path: $path) {
                edge(src: $src, dst: $dst) {
                  delete(time: $time, layer: $layer)
                }
              }
            }
        "#;

        let variables = json!({
            "path": json!(args.path),
            "src": json!(args.src),
            "dst": json!(args.dst),
            "time": input_time_var(&args.time),
            "layer": json!(args.layer),
        });
        let res = self.client.query(query, variables).await?;
        ensure_write_target_present(
            &res,
            "edge",
            format!("edge '{}' -> '{}'", args.src, args.dst),
        )?;
        Ok(None)
    }

    async fn apply_add_edge_metadata(
        &self,
        args: &AddEdgeMetadata,
    ) -> Result<Option<Prop>, ClientError> {
        let query = r#"
            query($path: String!, $src: NodeId!, $dst: NodeId!,
                     $properties: [PropertyInput!]!, $layer: String) {
              updateGraph(path: $path) {
                edge(src: $src, dst: $dst) {
                  addMetadata(properties: $properties, layer: $layer)
                }
              }
            }
        "#;

        let variables = json!({
            "path": json!(args.path),
            "src": json!(args.src),
            "dst": json!(args.dst),
            "properties": properties_var(&args.properties)?,
            "layer": json!(args.layer),
        });
        let res = self.client.query(query, variables).await?;
        ensure_write_target_present(
            &res,
            "edge",
            format!("edge '{}' -> '{}'", args.src, args.dst),
        )?;
        Ok(None)
    }

    async fn apply_update_edge_metadata(
        &self,
        args: &UpdateEdgeMetadata,
    ) -> Result<Option<Prop>, ClientError> {
        let query = r#"
            query($path: String!, $src: NodeId!, $dst: NodeId!,
                     $properties: [PropertyInput!]!, $layer: String) {
              updateGraph(path: $path) {
                edge(src: $src, dst: $dst) {
                  updateMetadata(properties: $properties, layer: $layer)
                }
              }
            }
        "#;

        let variables = json!({
            "path": json!(args.path),
            "src": json!(args.src),
            "dst": json!(args.dst),
            "properties": properties_var(&args.properties)?,
            "layer": json!(args.layer),
        });
        let res = self.client.query(query, variables).await?;
        ensure_write_target_present(
            &res,
            "edge",
            format!("edge '{}' -> '{}'", args.src, args.dst),
        )?;
        Ok(None)
    }

    async fn apply_add_nodes(&self, args: &AddNodes) -> Result<Option<Prop>, ClientError> {
        // `NodeAddition` serializes to the schema input shape (camelCase fields,
        // `Value`-typed property values) — see its `Serialize` impl in `op.rs`.
        let query = r#"
        query($path: String!, $nodes: [NodeAddition!]!) {
            updateGraph(path: $path) {
                addNodes(nodes: $nodes)
            }
        }
        "#;

        let variables = json!({
            "path": json!(args.path),
            "nodes": to_var(&args.nodes)?,
        });
        let res = self.client.query(query, variables).await?;
        expect_update_bool(&res, "addNodes")?;
        Ok(None)
    }

    async fn apply_add_edges(&self, args: &AddEdges) -> Result<Option<Prop>, ClientError> {
        let query = r#"
        query($path: String!, $edges: [EdgeAddition!]!) {
            updateGraph(path: $path) {
                addEdges(edges: $edges)
            }
        }
        "#;

        let variables = json!({
            "path": json!(args.path),
            "edges": to_var(&args.edges)?,
        });
        let res = self.client.query(query, variables).await?;
        expect_update_bool(&res, "addEdges")?;
        Ok(None)
    }
}

// ============ Read path ============

impl GraphqlTransport {
    async fn eval_read(&self, expr: &ReadExpr) -> Result<Option<Prop>, ClientError> {
        let (query, variables) = render_read(expr)?;
        let res = self
            .client
            .query(&query, JsonValue::Object(variables))
            .await?;
        let root =
            serde_json::to_value(&res).map_err(|e| ClientError::InvalidResponse(e.to_string()))?;
        parse_read(expr, &root)
    }
}

/// Collects GraphQL query variables while rendering a read tree.
///
/// Complex arguments (node/edge filters) are shipped as JSON variables rather
/// than spliced into the query text: each `add_*_filter` serializes the typed
/// filter via serde (the single wire-format source of truth), stashes it under
/// a fresh `$fN` name, records the operation-signature declaration, and returns
/// the `$fN` reference to inline. Scalar view-op args (times, layer names) stay
/// as literals — they carry no injection surface.
#[derive(Default)]
struct VarCollector {
    vars: serde_json::Map<String, JsonValue>,
    /// Accumulated variable declarations, already comma-joined
    /// (`"$f0: NodeFilter!, $f1: EdgeFilter!"`) — appended in place rather than
    /// collected into a `Vec` and joined at the end.
    decls: String,
    counter: usize,
}

impl VarCollector {
    fn add_node_filter(&mut self, f: &GqlNodeFilter) -> Result<String, ClientError> {
        self.add("NodeFilter!", f)
    }

    fn add_edge_filter(&mut self, f: &GqlEdgeFilter) -> Result<String, ClientError> {
        self.add("EdgeFilter!", f)
    }

    /// Register a property dict (`findNodes`/`findEdges` `propertiesDict` arg)
    /// as a `[PropertyInput!]!` variable.
    fn add_properties(&mut self, props: &HashMap<String, Prop>) -> Result<String, ClientError> {
        self.add("[PropertyInput!]!", &properties_to_input(props)?)
    }

    /// Serialize `value`, register it as `$fN: <gql_type>`, and return `$fN`.
    /// A serialization failure (e.g. a non-finite float in a filter value) maps
    /// to `InvalidInput` — the same class the literal renderer rejected.
    fn add<T: serde::Serialize>(
        &mut self,
        gql_type: &str,
        value: &T,
    ) -> Result<String, ClientError> {
        let name = format!("f{}", self.counter);
        self.counter += 1;
        let json = serde_json::to_value(value).map_err(|e| {
            ClientError::InvalidInput(format!("filter value cannot be sent to the server: {e}"))
        })?;
        if !self.decls.is_empty() {
            self.decls.push_str(", ");
        }
        self.decls.push('$');
        self.decls.push_str(&name);
        self.decls.push_str(": ");
        self.decls.push_str(gql_type);
        self.vars.insert(name.clone(), json);
        Ok(format!("${name}"))
    }
}

/// Renders a read expression tree as a nested GraphQL query plus its variables.
///
/// Example: `Degree(Node(Window(Root("g"), 0, 10), "ben"))` becomes
/// `{ graph(path: "g") { window(start: 0, end: 10) { node(name: "ben") { degree } } } }`.
/// Filter-bearing reads gain a `query($f0: NodeFilter!, …) { … }` signature and
/// a matching variables map.
fn render_read(
    expr: &ReadExpr,
) -> Result<(String, serde_json::Map<String, JsonValue>), ClientError> {
    let mut vars = VarCollector::default();
    let body = render_read_body(expr, &mut vars)?;
    let closes = "}".repeat(read_depth(expr));
    let query = if vars.decls.is_empty() {
        format!("{{ {} {} }}", body, closes)
    } else {
        format!("query({}) {{ {} {} }}", vars.decls, body, closes)
    };
    Ok((query, vars.vars))
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
        .map(|s| render_gql_str(s))
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
                fields.push(format!("property: {}", render_gql_str(p)));
            }
            format!("{{{}}}", fields.join(", "))
        })
        .collect();
    format!("[{}]", entries.join(", "))
}

use crate::model::graph::filtering::{GqlEdgeFilter, GqlNodeFilter};

fn render_gql_str(s: &str) -> String {
    // A JSON string literal (including its surrounding quotes) is a valid
    // GraphQL string literal — quotes, backslashes, control chars, and unicode
    // are all escaped correctly. Callers must NOT add their own quotes.
    serde_json::to_string(s).expect("string serialization is infallible")
}

/// Node/edge-scoped writes address their target as `updateGraph { node(name) }`
/// / `edge(src, dst)`. When the target doesn't exist under the current view the
/// server resolves that field to `null` with no error and silently does
/// nothing — so a bare `Ok(())` would report success for a write that never
/// happened. Surface the missing target as `NotFound` instead.
fn ensure_write_target_present(
    res: &HashMap<String, serde_json::Value>,
    field: &str,
    target: String,
) -> Result<(), ClientError> {
    let present = res
        .get("updateGraph")
        .and_then(|g| g.as_object())
        .and_then(|g| g.get(field))
        .is_some_and(|v| !v.is_null());
    if present {
        Ok(())
    } else {
        Err(ClientError::NotFound(target))
    }
}

/// A graph-scoped write whose server field returns `{ success }` (single
/// `addNode`/`createNode`/`addEdge`/`deleteEdge`): read
/// `updateGraph.<field>.success` and surface a `false`/absent result as
/// `UnsuccessfulResponse`.
fn expect_update_success(
    res: &HashMap<String, serde_json::Value>,
    field: &str,
) -> Result<(), ClientError> {
    let ok = res
        .get("updateGraph")
        .and_then(|g| g.as_object())
        .and_then(|g| g.get(field))
        .and_then(|f| f.as_object())
        .and_then(|f| f.get("success"))
        .and_then(|s| s.as_bool())
        .is_some_and(|s| s);
    if ok {
        Ok(())
    } else {
        Err(ClientError::UnsuccessfulResponse)
    }
}

/// A graph-scoped write whose server field returns a bare `Boolean!`
/// (`addProperties`/`addMetadata`/`updateMetadata`/`addNodes`/`addEdges`):
/// read `updateGraph.<field>` and surface a `false`/absent result as
/// `UnsuccessfulResponse` — so a server `false` isn't a silent client success.
fn expect_update_bool(
    res: &HashMap<String, serde_json::Value>,
    field: &str,
) -> Result<(), ClientError> {
    let ok = res
        .get("updateGraph")
        .and_then(|g| g.as_object())
        .and_then(|g| g.get(field))
        .and_then(|v| v.as_bool())
        .is_some_and(|v| v);
    if ok {
        Ok(())
    } else {
        Err(ClientError::UnsuccessfulResponse)
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
                fields.push(format!("property: {}", render_gql_str(p)));
            }
            format!("{{{}}}", fields.join(", "))
        })
        .collect();
    format!("[{}]", entries.join(", "))
}

/// Render a view op as its server field plus arguments. Valid-layer ops
/// render to the same `layers` / `excludeLayer(s)` fields as the plain layer
/// ops — the server backs those fields with `valid_layers` /
/// `exclude_valid_layers` and exposes no separate `validLayers` field.
fn render_view_op(op: &ViewOp) -> String {
    match op {
        ViewOp::Window { start, end } => format!(
            "window(start: {}, end: {})",
            render_input_time(start),
            render_input_time(end)
        ),
        ViewOp::At { time } => format!("at(time: {})", render_input_time(time)),
        ViewOp::Before { time } => format!("before(time: {})", render_input_time(time)),
        ViewOp::After { time } => format!("after(time: {})", render_input_time(time)),
        ViewOp::Latest => "latest".to_string(),
        ViewOp::SnapshotLatest => "snapshotLatest".to_string(),
        ViewOp::SnapshotAt { time } => format!("snapshotAt(time: {})", render_input_time(time)),
        ViewOp::ShrinkWindow { start, end } => format!(
            "shrinkWindow(start: {}, end: {})",
            render_input_time(start),
            render_input_time(end)
        ),
        ViewOp::ShrinkStart { start } => {
            format!("shrinkStart(start: {})", render_input_time(start))
        }
        ViewOp::ShrinkEnd { end } => format!("shrinkEnd(end: {})", render_input_time(end)),
        ViewOp::Layer { name } => format!("layer(name: {})", render_gql_str(name)),
        ViewOp::ExcludeLayer { name } => format!("excludeLayer(name: {})", render_gql_str(name)),
        ViewOp::DefaultLayer => "defaultLayer".to_string(),
        ViewOp::Layers { names } => format!("layers(names: [{}])", render_string_list(names)),
        ViewOp::ExcludeLayers { names } => {
            format!("excludeLayers(names: [{}])", render_string_list(names))
        }
        ViewOp::ValidLayers { names } => format!("layers(names: [{}])", render_string_list(names)),
        ViewOp::ExcludeValidLayer { name } => {
            format!("excludeLayer(name: {})", render_gql_str(name))
        }
        ViewOp::ExcludeValidLayers { names } => {
            format!("excludeLayers(names: [{}])", render_string_list(names))
        }
    }
}

/// The response key a view op's field appears under — the field name emitted
/// by `render_view_op`, without arguments.
fn view_op_json_key(op: &ViewOp) -> &'static str {
    match op {
        ViewOp::Window { .. } => "window",
        ViewOp::At { .. } => "at",
        ViewOp::Before { .. } => "before",
        ViewOp::After { .. } => "after",
        ViewOp::Latest => "latest",
        ViewOp::SnapshotLatest => "snapshotLatest",
        ViewOp::SnapshotAt { .. } => "snapshotAt",
        ViewOp::ShrinkWindow { .. } => "shrinkWindow",
        ViewOp::ShrinkStart { .. } => "shrinkStart",
        ViewOp::ShrinkEnd { .. } => "shrinkEnd",
        ViewOp::Layer { .. } => "layer",
        ViewOp::ExcludeLayer { .. } => "excludeLayer",
        ViewOp::DefaultLayer => "defaultLayer",
        ViewOp::Layers { .. } => "layers",
        ViewOp::ExcludeLayers { .. } => "excludeLayers",
        ViewOp::ValidLayers { .. } => "layers",
        ViewOp::ExcludeValidLayer { .. } => "excludeLayer",
        ViewOp::ExcludeValidLayers { .. } => "excludeLayers",
    }
}

fn render_read_body(expr: &ReadExpr, vars: &mut VarCollector) -> Result<String, ClientError> {
    Ok(match expr {
        ReadExpr::Root { path } => format!("graph(path: {})", render_gql_str(path)),
        // View chaining
        ReadExpr::View { input, op } => format!(
            "{} {{ {}",
            render_read_body(input, vars)?,
            render_view_op(op)
        ),
        ReadExpr::Valid { input } => format!("{} {{ valid", render_read_body(input, vars)?),
        ReadExpr::Subgraph { input, nodes } => format!(
            "{} {{ subgraph(nodes: [{}])",
            render_read_body(input, vars)?,
            render_string_list(nodes)
        ),
        ReadExpr::SubgraphNodeTypes { input, node_types } => format!(
            "{} {{ subgraphNodeTypes(nodeTypes: [{}])",
            render_read_body(input, vars)?,
            render_string_list(node_types)
        ),
        ReadExpr::ExcludeNodes { input, nodes } => format!(
            "{} {{ excludeNodes(nodes: [{}])",
            render_read_body(input, vars)?,
            render_string_list(nodes)
        ),
        ReadExpr::TypeFilter { input, node_types } => format!(
            "{} {{ typeFilter(nodeTypes: [{}])",
            render_read_body(input, vars)?,
            render_string_list(node_types)
        ),
        // Selection
        ReadExpr::Node { input, id } => {
            format!(
                "{} {{ node(name: {})",
                render_read_body(input, vars)?,
                render_gql_str(id)
            )
        }
        ReadExpr::Edge { input, src, dst } => format!(
            "{} {{ edge(src: {}, dst: {})",
            render_read_body(input, vars)?,
            render_gql_str(src),
            render_gql_str(dst)
        ),
        ReadExpr::Src { input } => format!("{} {{ src", render_read_body(input, vars)?),
        ReadExpr::Dst { input } => format!("{} {{ dst", render_read_body(input, vars)?),
        ReadExpr::Nbr { input } => format!("{} {{ nbr", render_read_body(input, vars)?),
        ReadExpr::History { input } => format!("{} {{ history", render_read_body(input, vars)?),
        ReadExpr::CombinedHistory { input } => {
            format!("{} {{ combinedHistory", render_read_body(input, vars)?)
        }
        ReadExpr::HistoryReverse { input } => {
            format!("{} {{ reverse", render_read_body(input, vars)?)
        }
        ReadExpr::Deletions { input } => format!("{} {{ deletions", render_read_body(input, vars)?),
        // Sub-container navigations
        ReadExpr::HistoryTimestamps { input } => {
            format!("{} {{ timestamps", render_read_body(input, vars)?)
        }
        ReadExpr::HistoryEventIds { input } => {
            format!("{} {{ eventId", render_read_body(input, vars)?)
        }
        ReadExpr::HistoryDateTimes { input } => {
            format!("{} {{ datetimes", render_read_body(input, vars)?)
        }
        ReadExpr::HistoryIntervals { input } => {
            format!("{} {{ intervals", render_read_body(input, vars)?)
        }
        // Polymorphic sub-container terminals — render field names only;
        // return type is decided by the parent selection in `parse_read`.
        ReadExpr::SubList { input } => format!("{} {{ list", render_read_body(input, vars)?),
        ReadExpr::SubListRev { input } => format!("{} {{ listRev", render_read_body(input, vars)?),
        ReadExpr::SubPage {
            input,
            limit,
            offset,
            page_index,
        } => format!(
            "{} {{ page({})",
            render_read_body(input, vars)?,
            render_page_args(*limit, *offset, *page_index),
        ),
        ReadExpr::SubPageRev {
            input,
            limit,
            offset,
            page_index,
        } => format!(
            "{} {{ pageRev({})",
            render_read_body(input, vars)?,
            render_page_args(*limit, *offset, *page_index),
        ),
        // Intervals stats
        ReadExpr::IntervalsMean { input } => format!("{} {{ mean", render_read_body(input, vars)?),
        ReadExpr::IntervalsMedian { input } => format!("{} {{ median", render_read_body(input, vars)?),
        ReadExpr::IntervalsMax { input } => format!("{} {{ max", render_read_body(input, vars)?),
        ReadExpr::IntervalsMin { input } => format!("{} {{ min", render_read_body(input, vars)?),
        ReadExpr::Nodes { input } => format!("{} {{ nodes", render_read_body(input, vars)?),
        ReadExpr::Neighbours { input } => format!("{} {{ neighbours", render_read_body(input, vars)?),
        ReadExpr::InNeighbours { input } => {
            format!("{} {{ inNeighbours", render_read_body(input, vars)?)
        }
        ReadExpr::OutNeighbours { input } => {
            format!("{} {{ outNeighbours", render_read_body(input, vars)?)
        }
        ReadExpr::Edges { input } => format!("{} {{ edges", render_read_body(input, vars)?),
        ReadExpr::NodeEdges { input } => format!("{} {{ edges", render_read_body(input, vars)?),
        ReadExpr::InEdges { input } => format!("{} {{ inEdges", render_read_body(input, vars)?),
        ReadExpr::OutEdges { input } => format!("{} {{ outEdges", render_read_body(input, vars)?),
        ReadExpr::InComponent { input } => {
            format!("{} {{ inComponent", render_read_body(input, vars)?)
        }
        ReadExpr::OutComponent { input } => {
            format!("{} {{ outComponent", render_read_body(input, vars)?)
        }
        ReadExpr::Explode { input } => format!("{} {{ explode", render_read_body(input, vars)?),
        ReadExpr::ExplodeLayers { input } => {
            format!("{} {{ explodeLayers", render_read_body(input, vars)?)
        }
        ReadExpr::SortedNodes { input, sort_bys } => format!(
            "{} {{ sorted(sortBys: {})",
            render_read_body(input, vars)?,
            render_node_sort_bys(sort_bys)
        ),
        ReadExpr::SortedEdges { input, sort_bys } => format!(
            "{} {{ sorted(sortBys: {})",
            render_read_body(input, vars)?,
            render_edge_sort_bys(sort_bys)
        ),
        ReadExpr::FilterNodes { input, filter } => {
            // Server field `filter(expr: NodeFilter!)`: applies to this
            // collection AND propagates to downstream traversals from these
            // nodes. Contrast with `SelectNodes` which renders `select` —
            // narrows membership at this step only.
            format!(
                "{} {{ filter(expr: {})",
                render_read_body(input, vars)?,
                vars.add_node_filter(filter)?,
            )
        }
        ReadExpr::SelectNodes { input, filter } => {
            // Server field `select(expr: NodeFilter!)`: narrows the current
            // collection's membership only; downstream traversals see the
            // unfiltered graph.
            format!(
                "{} {{ select(expr: {})",
                render_read_body(input, vars)?,
                vars.add_node_filter(filter)?,
            )
        }
        ReadExpr::FilterEdges { input, filter } => {
            // Server field `filter(expr: EdgeFilter!)` on `Edges`: applies to
            // this collection AND propagates to downstream traversals.
            format!(
                "{} {{ filter(expr: {})",
                render_read_body(input, vars)?,
                vars.add_edge_filter(filter)?,
            )
        }
        ReadExpr::SelectEdges { input, filter } => {
            // Server field `select(expr: EdgeFilter!)` on `Edges`: narrows the
            // current collection's membership only.
            format!(
                "{} {{ select(expr: {})",
                render_read_body(input, vars)?,
                vars.add_edge_filter(filter)?,
            )
        }
        ReadExpr::FilterGraphNodes { input, filter } => {
            // Server field `filterNodes(expr: NodeFilter!)` on `Graph`.
            format!(
                "{} {{ filterNodes(expr: {})",
                render_read_body(input, vars)?,
                vars.add_node_filter(filter)?,
            )
        }
        ReadExpr::FilterGraphEdges { input, filter } => {
            // Server field `filterEdges(expr: EdgeFilter!)` on `Graph`.
            format!(
                "{} {{ filterEdges(expr: {})",
                render_read_body(input, vars)?,
                vars.add_edge_filter(filter)?,
            )
        }
        ReadExpr::NodeFilterEdges { input, filter } => {
            // Server field `filterEdges(expr: EdgeFilter!)` on `Node` —
            // filters the node's edge traversals; the node stays addressable.
            format!(
                "{} {{ filterEdges(expr: {})",
                render_read_body(input, vars)?,
                vars.add_edge_filter(filter)?,
            )
        }
        ReadExpr::EdgeFilterNodes { input, filter } => {
            // Server field `filterNodes(expr: NodeFilter!)` on `Edge` —
            // filters the edge's node traversals; the edge stays addressable.
            format!(
                "{} {{ filterNodes(expr: {})",
                render_read_body(input, vars)?,
                vars.add_node_filter(filter)?,
            )
        }
        ReadExpr::EdgeEvent {
            input,
            time,
            event_id,
            layer,
        } => {
            // Server field `event(time: TimeInput!, layer: String)` on `Edge`.
            // With an event id we render the exact `{timestamp, eventId}`
            // object form; otherwise the bare timestamp matches the first
            // event at that time.
            let time_arg = match event_id {
                Some(i) => format!("{{timestamp: {}, eventId: {}}}", time, i),
                None => time.to_string(),
            };
            match layer {
                Some(l) => format!(
                    "{} {{ event(time: {}, layer: {})",
                    render_read_body(input, vars)?,
                    time_arg,
                    render_gql_str(l)
                ),
                None => format!("{} {{ event(time: {})", render_read_body(input, vars)?, time_arg),
            }
        }
        // Server field `eventLayer(name: String!)` on `Edge` — pins a single
        // layer-exploded instance.
        ReadExpr::EdgeLayerEvent { input, layer } => format!(
            "{} {{ eventLayer(name: {})",
            render_read_body(input, vars)?,
            render_gql_str(layer)
        ),
        // Metadata / Properties navigation
        ReadExpr::Metadata { input } => format!("{} {{ metadata", render_read_body(input, vars)?),
        ReadExpr::Properties { input } => format!("{} {{ properties", render_read_body(input, vars)?),
        // Property terminals — `values` is compound (returns {key, value}
        // records); `get` selects only `{ value }` — the caller already knows
        // the key, so fetching it back is wasted bytes. Inner braces are
        // self-balanced; outer `get` / `values` opens one net brace,
        // contributing 1 to read_depth.
        ReadExpr::PropertyGet { input, key } => format!(
            "{} {{ get(key: {}) {{ value }}",
            render_read_body(input, vars)?,
            render_gql_str(key)
        ),
        ReadExpr::PropertyContains { input, key } => {
            format!(
                "{} {{ contains(key: {})",
                render_read_body(input, vars)?,
                render_gql_str(key)
            )
        }
        ReadExpr::PropertyKeys { input } => format!("{} {{ keys", render_read_body(input, vars)?),
        ReadExpr::PropertyGetDtypeOf { input, key } => {
            format!(
                "{} {{ getDtypeOf(key: {})",
                render_read_body(input, vars)?,
                render_gql_str(key)
            )
        }
        ReadExpr::PropertyValues { input, keys } => match keys {
            Some(ks) => format!(
                "{} {{ values(keys: [{}]) {{ value }}",
                render_read_body(input, vars)?,
                render_string_list(ks)
            ),
            None => format!("{} {{ values {{ value }}", render_read_body(input, vars)?),
        },
        ReadExpr::PropertyItems { input, keys } => match keys {
            Some(ks) => format!(
                "{} {{ values(keys: [{}]) {{ key value }}",
                render_read_body(input, vars)?,
                render_string_list(ks)
            ),
            None => format!("{} {{ values {{ key value }}", render_read_body(input, vars)?),
        },
        ReadExpr::TemporalProperties { input } => {
            format!("{} {{ temporal", render_read_body(input, vars)?)
        }
        ReadExpr::TemporalPropertyByKey { input, key } => {
            format!(
                "{} {{ get(key: {})",
                render_read_body(input, vars)?,
                render_gql_str(key)
            )
        }
        // `values(keys?) { key }` — we only fetch the key from each record;
        // clients build a `RemoteTemporalProperty` handle around each key.
        ReadExpr::TemporalPropertyList { input, keys } => match keys {
            Some(ks) => format!(
                "{} {{ values(keys: [{}]) {{ key }}",
                render_read_body(input, vars)?,
                render_string_list(ks)
            ),
            None => format!("{} {{ values {{ key }}", render_read_body(input, vars)?),
        },
        ReadExpr::TemporalPropertyValueList { input } => {
            format!("{} {{ values", render_read_body(input, vars)?)
        }
        ReadExpr::TemporalPropertyAt { input, time } => {
            format!("{} {{ at(t: {})", render_read_body(input, vars)?, time)
        }
        ReadExpr::TemporalPropertyLatest { input } => {
            format!("{} {{ latest", render_read_body(input, vars)?)
        }
        ReadExpr::TemporalPropertyUnique { input } => {
            format!("{} {{ unique", render_read_body(input, vars)?)
        }
        ReadExpr::TemporalPropertyOrderedDedupe { input, latest_time } => format!(
            "{} {{ orderedDedupe(latestTime: {}) {{ time {{ timestamp datetime eventId }} value }}",
            render_read_body(input, vars)?,
            latest_time
        ),
        ReadExpr::TemporalPropertySum { input } => {
            format!("{} {{ sum", render_read_body(input, vars)?)
        }
        ReadExpr::TemporalPropertyMean { input } => {
            format!("{} {{ mean", render_read_body(input, vars)?)
        }
        ReadExpr::TemporalPropertyAverage { input } => {
            format!("{} {{ average", render_read_body(input, vars)?)
        }
        ReadExpr::TemporalPropertyMin { input } => format!(
            "{} {{ min {{ time {{ timestamp datetime eventId }} value }}",
            render_read_body(input, vars)?
        ),
        ReadExpr::TemporalPropertyMax { input } => format!(
            "{} {{ max {{ time {{ timestamp datetime eventId }} value }}",
            render_read_body(input, vars)?
        ),
        ReadExpr::TemporalPropertyMedian { input } => format!(
            "{} {{ median {{ time {{ timestamp datetime eventId }} value }}",
            render_read_body(input, vars)?
        ),
        // Compound-structured tree — one RPC fetches everything.
        ReadExpr::Schema { input } => format!(
            "{} {{ schema {{ \
                nodes {{ typeName properties {{ key propertyType variants }} \
                    metadata {{ key propertyType variants }} }} \
                layers {{ name edges {{ srcType dstType \
                    properties {{ key propertyType variants }} \
                    metadata {{ key propertyType variants }} }} }} }}",
            render_read_body(input, vars)?,
        ),
        // Terminals — no args after the field name
        ReadExpr::CountNodes { input } => format!("{} {{ countNodes", render_read_body(input, vars)?),
        ReadExpr::CountEdges { input } => format!("{} {{ countEdges", render_read_body(input, vars)?),
        ReadExpr::Degree { input } => format!("{} {{ degree", render_read_body(input, vars)?),
        ReadExpr::InDegree { input } => format!("{} {{ inDegree", render_read_body(input, vars)?),
        ReadExpr::OutDegree { input } => format!("{} {{ outDegree", render_read_body(input, vars)?),
        ReadExpr::Name { input } => format!("{} {{ name", render_read_body(input, vars)?),
        ReadExpr::HasNode { input, id } => {
            format!(
                "{} {{ hasNode(name: {})",
                render_read_body(input, vars)?,
                render_gql_str(id)
            )
        }
        ReadExpr::HasEdge { input, src, dst } => format!(
            "{} {{ hasEdge(src: {}, dst: {})",
            render_read_body(input, vars)?,
            render_gql_str(src),
            render_gql_str(dst)
        ),
        ReadExpr::CountTemporalEdges { input } => {
            format!("{} {{ countTemporalEdges", render_read_body(input, vars)?)
        }
        ReadExpr::Path { input } => format!("{} {{ path", render_read_body(input, vars)?),
        ReadExpr::Namespace { input } => format!("{} {{ namespace", render_read_body(input, vars)?),
        ReadExpr::Created { input } => format!("{} {{ created", render_read_body(input, vars)?),
        ReadExpr::LastOpened { input } => format!("{} {{ lastOpened", render_read_body(input, vars)?),
        ReadExpr::LastUpdated { input } => format!("{} {{ lastUpdated", render_read_body(input, vars)?),
        ReadExpr::UniqueLayers { input } => format!("{} {{ uniqueLayers", render_read_body(input, vars)?),
        ReadExpr::HasLayer { input, name } => {
            format!(
                "{} {{ hasLayer(name: {})",
                render_read_body(input, vars)?,
                render_gql_str(name)
            )
        }
        ReadExpr::WindowSize { input } => format!("{} {{ windowSize", render_read_body(input, vars)?),
        ReadExpr::Ids { input } => format!("{} {{ ids", render_read_body(input, vars)?),
        // `PathFromGraph.ids` is a columnar `[[String]]` field computed in ONE
        // server-side `blocking_compute` (vs `list { ids }`, which resolves one
        // `PathFromNode` object + its own `blocking_compute` per source). Opens
        // ONE net brace, same as `Ids`.
        ReadExpr::NestedIds { input } => {
            format!("{} {{ ids", render_read_body(input, vars)?)
        }
        // Flat collection degree terminals — render the scalar-list field
        // directly on the `Nodes`/`PathFromNode` collection.
        ReadExpr::CollectionDegree { input } => {
            format!("{} {{ degree", render_read_body(input, vars)?)
        }
        ReadExpr::CollectionInDegree { input } => {
            format!("{} {{ inDegree", render_read_body(input, vars)?)
        }
        ReadExpr::CollectionOutDegree { input } => {
            format!("{} {{ outDegree", render_read_body(input, vars)?)
        }
        ReadExpr::CollectionEdgeHistoryCount { input } => {
            format!("{} {{ edgeHistoryCount", render_read_body(input, vars)?)
        }
        // Columnar `[[Int]]` fields on `PathFromGraph`, computed in ONE
        // server-side `blocking_compute` (vs `list { degree }` per source).
        // Mirror `NestedIds`.
        ReadExpr::NestedDegree { input } => {
            format!("{} {{ degree", render_read_body(input, vars)?)
        }
        ReadExpr::NestedInDegree { input } => {
            format!("{} {{ inDegree", render_read_body(input, vars)?)
        }
        ReadExpr::NestedOutDegree { input } => {
            format!("{} {{ outDegree", render_read_body(input, vars)?)
        }
        ReadExpr::NestedEdgeHistoryCount { input } => {
            format!(
                "{} {{ list {{ edgeHistoryCount }}",
                render_read_body(input, vars)?
            )
        }
        ReadExpr::Count { input } => format!("{} {{ count", render_read_body(input, vars)?),
        // Compound structured terminal: renders as `list { src { name } dst { name } }`.
        // The `list` field opens ONE brace that gets closed by the outer `read_depth`;
        // the inner `src { name }` / `dst { name }` groups are self-balanced.
        ReadExpr::EdgesList { input } => format!(
            "{} {{ list {{ src {{ name }} dst {{ name }} }}",
            render_read_body(input, vars)?
        ),
        // `NestedEdges.list` returns `[Edges!]!` — one object per source node.
        // We render `list { list { src { name } dst { name } } }` and read each
        // per-source `Edges.list` to rebuild the nested `[[(src, dst)]]` shape
        // client-side. The outer `list` field opens ONE net brace (closed by
        // the outer `read_depth`); the inner `list { src { name } dst { name } }`
        // group is self-balanced. Mirrors `EdgesList`, one level deeper.
        ReadExpr::NestedEdgesList { input } => format!(
            "{} {{ list {{ list {{ src {{ name }} dst {{ name }} }} }}",
            render_read_body(input, vars)?
        ),
        // Exploded-collection variant of `EdgesList`: adds each member's
        // event identity (`time { timestamp eventId }`, `layerName`) so
        // handles can be pinned from ONE response. Same brace accounting —
        // the outer `list` opens one net brace, inner groups self-balance.
        ReadExpr::ExplodedEdgesList { input } => format!(
            "{} {{ list {{ src {{ name }} dst {{ name }} time {{ timestamp eventId }} layerName }}",
            render_read_body(input, vars)?
        ),
        // Nested variant of `ExplodedEdgesList` — mirrors `NestedEdgesList`.
        ReadExpr::NestedExplodedEdgesList { input } => format!(
            "{} {{ list {{ list {{ src {{ name }} dst {{ name }} time {{ timestamp eventId }} layerName }} }}",
            render_read_body(input, vars)?
        ),
        // Layer-exploded members — `(src, dst, layer)` per member (no time).
        ReadExpr::ExplodedLayersEdgesList { input } => format!(
            "{} {{ list {{ src {{ name }} dst {{ name }} layerName }}",
            render_read_body(input, vars)?
        ),
        ReadExpr::NestedExplodedLayersEdgesList { input } => format!(
            "{} {{ list {{ list {{ src {{ name }} dst {{ name }} layerName }} }}",
            render_read_body(input, vars)?
        ),
        // Columnar accessors — FLAT collections render `list { <field> }`.
        ReadExpr::CollectionNames { input } => {
            format!("{} {{ list {{ name }}", render_read_body(input, vars)?)
        }
        ReadExpr::CollectionNodeTypes { input } => {
            format!("{} {{ list {{ nodeType }}", render_read_body(input, vars)?)
        }
        ReadExpr::CollectionLayerNames { input } => {
            format!("{} {{ list {{ layerNames }}", render_read_body(input, vars)?)
        }
        ReadExpr::CollectionLayerName { input } => {
            format!("{} {{ list {{ layerName }}", render_read_body(input, vars)?)
        }
        ReadExpr::CollectionEarliestTime { input } => format!(
            "{} {{ list {{ earliestTime {{ timestamp datetime eventId }} }}",
            render_read_body(input, vars)?
        ),
        ReadExpr::CollectionLatestTime { input } => format!(
            "{} {{ list {{ latestTime {{ timestamp datetime eventId }} }}",
            render_read_body(input, vars)?
        ),
        ReadExpr::CollectionTime { input } => format!(
            "{} {{ list {{ time {{ timestamp datetime eventId }} }}",
            render_read_body(input, vars)?
        ),
        // Columnar accessors — NESTED collections render `list { list { <field> } }`.
        ReadExpr::NestedNames { input } => {
            format!("{} {{ list {{ list {{ name }} }}", render_read_body(input, vars)?)
        }
        ReadExpr::NestedNodeTypes { input } => format!(
            "{} {{ list {{ list {{ nodeType }} }}",
            render_read_body(input, vars)?
        ),
        ReadExpr::NestedLayerNames { input } => format!(
            "{} {{ list {{ list {{ layerNames }} }}",
            render_read_body(input, vars)?
        ),
        ReadExpr::NestedLayerName { input } => format!(
            "{} {{ list {{ list {{ layerName }} }}",
            render_read_body(input, vars)?
        ),
        ReadExpr::NestedEarliestTime { input } => format!(
            "{} {{ list {{ list {{ earliestTime {{ timestamp datetime eventId }} }} }}",
            render_read_body(input, vars)?
        ),
        ReadExpr::NestedLatestTime { input } => format!(
            "{} {{ list {{ list {{ latestTime {{ timestamp datetime eventId }} }} }}",
            render_read_body(input, vars)?
        ),
        ReadExpr::NestedTime { input } => format!(
            "{} {{ list {{ list {{ time {{ timestamp datetime eventId }} }} }}",
            render_read_body(input, vars)?
        ),
        // Boolean columnar accessors — FLAT collections render `list { <field> }`.
        ReadExpr::CollectionIsActive { input } => {
            format!("{} {{ list {{ isActive }}", render_read_body(input, vars)?)
        }
        ReadExpr::CollectionIsValid { input } => {
            format!("{} {{ list {{ isValid }}", render_read_body(input, vars)?)
        }
        ReadExpr::CollectionIsDeleted { input } => {
            format!("{} {{ list {{ isDeleted }}", render_read_body(input, vars)?)
        }
        ReadExpr::CollectionIsSelfLoop { input } => {
            format!("{} {{ list {{ isSelfLoop }}", render_read_body(input, vars)?)
        }
        // Boolean columnar accessors — NESTED collections render `list { list { <field> } }`.
        ReadExpr::NestedIsActive { input } => {
            format!(
                "{} {{ list {{ list {{ isActive }} }}",
                render_read_body(input, vars)?
            )
        }
        ReadExpr::NestedIsValid { input } => {
            format!(
                "{} {{ list {{ list {{ isValid }} }}",
                render_read_body(input, vars)?
            )
        }
        ReadExpr::NestedIsDeleted { input } => {
            format!(
                "{} {{ list {{ list {{ isDeleted }} }}",
                render_read_body(input, vars)?
            )
        }
        ReadExpr::NestedIsSelfLoop { input } => {
            format!(
                "{} {{ list {{ list {{ isSelfLoop }} }}",
                render_read_body(input, vars)?
            )
        }
        // Columnar property / metadata accessors — descend per-member into the
        // `metadata` / `properties` container and read all `{key, value}`
        // entries. FLAT collections render `list { <container> { values { key
        // value } } }`.
        ReadExpr::CollectionMetadataValues { input } => format!(
            "{} {{ list {{ metadata {{ values {{ key value }} }} }}",
            render_read_body(input, vars)?
        ),
        ReadExpr::CollectionPropertiesValues { input } => format!(
            "{} {{ list {{ properties {{ values {{ key value }} }} }}",
            render_read_body(input, vars)?
        ),
        // NESTED collections render `list { list { <container> { values { key
        // value } } } }`.
        ReadExpr::NestedMetadataValues { input } => format!(
            "{} {{ list {{ list {{ metadata {{ values {{ key value }} }} }} }}",
            render_read_body(input, vars)?
        ),
        ReadExpr::NestedPropertiesValues { input } => format!(
            "{} {{ list {{ list {{ properties {{ values {{ key value }} }} }} }}",
            render_read_body(input, vars)?
        ),
        // Compound structured terminal on Graph: `sharedNeighbours(selectedNodes: [ids]) { name }`
        // — opens ONE net brace (the outer, before `sharedNeighbours`); the inner
        // `{ name }` is self-balanced.
        ReadExpr::SharedNeighbours { input, ids } => format!(
            "{} {{ sharedNeighbours(selectedNodes: [{}]) {{ name }}",
            render_read_body(input, vars)?,
            render_string_list(ids)
        ),
        // `findNodes(propertiesDict: [{key, value}]) { name }` — opens ONE net
        // brace (before `findNodes`); inner `{ name }` is self-balanced.
        ReadExpr::FindNodes { input, properties } => format!(
            "{} {{ findNodes(propertiesDict: {}) {{ name }}",
            render_read_body(input, vars)?,
            vars.add_properties(properties)?,
        ),
        // `findEdges(propertiesDict: [{key, value}]) { src { name } dst { name } }`
        // — opens ONE net brace; the inner `src`/`dst` groups are self-balanced.
        ReadExpr::FindEdges { input, properties } => format!(
            "{} {{ findEdges(propertiesDict: {}) {{ src {{ name }} dst {{ name }} }}",
            render_read_body(input, vars)?,
            vars.add_properties(properties)?,
        ),
        ReadExpr::GetAllNodeTypes { input } => {
            format!("{} {{ getAllNodeTypes", render_read_body(input, vars)?)
        }
        ReadExpr::Id { input } => format!("{} {{ id", render_read_body(input, vars)?),
        ReadExpr::NodeType { input } => format!("{} {{ nodeType", render_read_body(input, vars)?),
        ReadExpr::IsActive { input } => format!("{} {{ isActive", render_read_body(input, vars)?),
        ReadExpr::IsEmpty { input } => format!("{} {{ isEmpty", render_read_body(input, vars)?),
        // Compound structured terminal: `list { timestamp datetime eventId }`
        // returns a list of records. Inner braces are self-balanced; the outer
        // `list` brace opens one net brace, contributing 1 to read_depth.
        //
        // The server's `datetime` field takes an optional format-string arg
        // (defaults to RFC 3339). We pass no arg to get the default.
        ReadExpr::HistoryList { input } => format!(
            "{} {{ list {{ timestamp datetime eventId }}",
            render_read_body(input, vars)?
        ),
        ReadExpr::HistoryListRev { input } => format!(
            "{} {{ listRev {{ timestamp datetime eventId }}",
            render_read_body(input, vars)?
        ),
        ReadExpr::HistoryPage {
            input,
            limit,
            offset,
            page_index,
        } => format!(
            "{} {{ page({}) {{ timestamp datetime eventId }}",
            render_read_body(input, vars)?,
            render_page_args(*limit, *offset, *page_index),
        ),
        ReadExpr::HistoryPageRev {
            input,
            limit,
            offset,
            page_index,
        } => format!(
            "{} {{ pageRev({}) {{ timestamp datetime eventId }}",
            render_read_body(input, vars)?,
            render_page_args(*limit, *offset, *page_index),
        ),
        ReadExpr::EdgeHistoryCount { input } => {
            format!("{} {{ edgeHistoryCount", render_read_body(input, vars)?)
        }
        // Edge-specific terminals
        ReadExpr::EdgeIdPair { input } => format!("{} {{ id", render_read_body(input, vars)?),
        ReadExpr::LayerNames { input } => format!("{} {{ layerNames", render_read_body(input, vars)?),
        ReadExpr::LayerName { input } => format!("{} {{ layerName", render_read_body(input, vars)?),
        ReadExpr::IsValid { input } => format!("{} {{ isValid", render_read_body(input, vars)?),
        ReadExpr::IsDeleted { input } => format!("{} {{ isDeleted", render_read_body(input, vars)?),
        ReadExpr::IsSelfLoop { input } => format!("{} {{ isSelfLoop", render_read_body(input, vars)?),
        // EventTime terminals — fetch the full `{ timestamp datetime eventId }`
        // record so the client can return a `RemoteEventTime` (drop-in parity
        // with the local API's `EventTime`, which carries the `event_id`).
        ReadExpr::EarliestTime { input } => {
            format!(
                "{} {{ earliestTime {{ timestamp datetime eventId",
                render_read_body(input, vars)?
            )
        }
        ReadExpr::LatestTime { input } => {
            format!(
                "{} {{ latestTime {{ timestamp datetime eventId",
                render_read_body(input, vars)?
            )
        }
        ReadExpr::Start { input } => {
            format!(
                "{} {{ start {{ timestamp datetime eventId",
                render_read_body(input, vars)?
            )
        }
        ReadExpr::End { input } => {
            format!(
                "{} {{ end {{ timestamp datetime eventId",
                render_read_body(input, vars)?
            )
        }
        // Remaining timestamp terminals stay bare `i64` (no local @property
        // counterpart, so not part of the EventTime drop-in change).
        ReadExpr::EarliestEdgeTime { input } => format!(
            "{} {{ earliestEdgeTime {{ timestamp",
            render_read_body(input, vars)?
        ),
        ReadExpr::LatestEdgeTime { input } => {
            format!(
                "{} {{ latestEdgeTime {{ timestamp",
                render_read_body(input, vars)?
            )
        }
        ReadExpr::FirstUpdate { input } => {
            format!("{} {{ firstUpdate {{ timestamp", render_read_body(input, vars)?)
        }
        ReadExpr::LastUpdate { input } => {
            format!("{} {{ lastUpdate {{ timestamp", render_read_body(input, vars)?)
        }
        ReadExpr::Time { input } => {
            format!(
                "{} {{ time {{ timestamp datetime eventId",
                render_read_body(input, vars)?
            )
        }
    })
}

fn read_depth(expr: &ReadExpr) -> usize {
    match expr {
        ReadExpr::Root { .. } => 0,
        // Single-brace variants — open one `{` each.
        ReadExpr::View { input, .. }
        | ReadExpr::Valid { input }
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
        | ReadExpr::CombinedHistory { input }
        | ReadExpr::HistoryReverse { input }
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
        | ReadExpr::FilterEdges { input, .. }
        | ReadExpr::SelectEdges { input, .. }
        | ReadExpr::FilterGraphNodes { input, .. }
        | ReadExpr::FilterGraphEdges { input, .. }
        | ReadExpr::Metadata { input }
        | ReadExpr::Properties { input }
        | ReadExpr::PropertyGet { input, .. }
        | ReadExpr::PropertyContains { input, .. }
        | ReadExpr::PropertyKeys { input }
        | ReadExpr::PropertyValues { input, .. }
        | ReadExpr::PropertyItems { input, .. }
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
        | ReadExpr::NestedIds { input }
        | ReadExpr::CollectionDegree { input }
        | ReadExpr::CollectionInDegree { input }
        | ReadExpr::CollectionOutDegree { input }
        | ReadExpr::CollectionEdgeHistoryCount { input }
        | ReadExpr::NestedDegree { input }
        | ReadExpr::NestedInDegree { input }
        | ReadExpr::NestedOutDegree { input }
        | ReadExpr::NestedEdgeHistoryCount { input }
        | ReadExpr::Count { input }
        | ReadExpr::EdgesList { input }
        | ReadExpr::NestedEdgesList { input }
        | ReadExpr::ExplodedEdgesList { input }
        | ReadExpr::NestedExplodedEdgesList { input }
        | ReadExpr::ExplodedLayersEdgesList { input }
        | ReadExpr::NestedExplodedLayersEdgesList { input }
        | ReadExpr::NodeFilterEdges { input, .. }
        | ReadExpr::EdgeFilterNodes { input, .. }
        | ReadExpr::EdgeEvent { input, .. }
        | ReadExpr::EdgeLayerEvent { input, .. }
        | ReadExpr::CollectionNames { input }
        | ReadExpr::CollectionNodeTypes { input }
        | ReadExpr::CollectionLayerNames { input }
        | ReadExpr::CollectionLayerName { input }
        | ReadExpr::CollectionEarliestTime { input }
        | ReadExpr::CollectionLatestTime { input }
        | ReadExpr::CollectionTime { input }
        | ReadExpr::NestedNames { input }
        | ReadExpr::NestedNodeTypes { input }
        | ReadExpr::NestedLayerNames { input }
        | ReadExpr::NestedLayerName { input }
        | ReadExpr::NestedEarliestTime { input }
        | ReadExpr::NestedLatestTime { input }
        | ReadExpr::NestedTime { input }
        | ReadExpr::CollectionIsActive { input }
        | ReadExpr::CollectionIsValid { input }
        | ReadExpr::CollectionIsDeleted { input }
        | ReadExpr::CollectionIsSelfLoop { input }
        | ReadExpr::NestedIsActive { input }
        | ReadExpr::NestedIsValid { input }
        | ReadExpr::NestedIsDeleted { input }
        | ReadExpr::NestedIsSelfLoop { input }
        | ReadExpr::CollectionMetadataValues { input }
        | ReadExpr::CollectionPropertiesValues { input }
        | ReadExpr::NestedMetadataValues { input }
        | ReadExpr::NestedPropertiesValues { input }
        | ReadExpr::SharedNeighbours { input, .. }
        | ReadExpr::FindNodes { input, .. }
        | ReadExpr::FindEdges { input, .. }
        | ReadExpr::GetAllNodeTypes { input }
        | ReadExpr::PropertyGetDtypeOf { input, .. }
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
        | ReadExpr::HasLayer { input, .. }
        | ReadExpr::WindowSize { input }
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
// ============ Columnar-accessor element decoders ============
//
// Each `col_*_elem` decodes ONE element of a `list { <field> }` array into a
// `Prop`. Optional scalars use the `Prop::List` wrapper convention: `[]` =
// None, `[x]` = Some(x) — so the outer column stays a uniform `Prop::List`.
// `build_column` maps a flat `list` array; `build_nested_column` maps the
// outer per-source `list` array and each source's inner `list`.

/// `list { name }` element → `Prop::Str`.
fn col_name_elem(v: &JsonValue) -> Result<Prop, ClientError> {
    v.get("name")
        .and_then(|x| x.as_str())
        .map(|s| Prop::Str(s.into()))
        .ok_or_else(|| ClientError::InvalidResponse("collection element missing `name`".into()))
}

/// `list { nodeType }` element → `Prop::List([])` (None) or `[Str]` (Some).
fn col_node_type_elem(v: &JsonValue) -> Result<Prop, ClientError> {
    match v.get("nodeType") {
        None | Some(JsonValue::Null) => Ok(Prop::List(Vec::<Prop>::new().into())),
        Some(x) => x
            .as_str()
            .map(|s| Prop::List(vec![Prop::Str(s.into())].into()))
            .ok_or_else(|| ClientError::InvalidResponse("`nodeType` not a string".into())),
    }
}

/// `list { layerNames }` element → `Prop::List(Prop::Str, ...)`.
fn col_layer_names_elem(v: &JsonValue) -> Result<Prop, ClientError> {
    let arr = v
        .get("layerNames")
        .and_then(|x| x.as_array())
        .ok_or_else(|| {
            ClientError::InvalidResponse("collection element missing `layerNames`".into())
        })?;
    let items: Result<Vec<Prop>, ClientError> = arr
        .iter()
        .map(|e| {
            e.as_str().map(|s| Prop::Str(s.into())).ok_or_else(|| {
                ClientError::InvalidResponse("`layerNames` element not a string".into())
            })
        })
        .collect();
    Ok(Prop::List(items?.into()))
}

/// `list { layerName }` element → `Prop::Str`.
fn col_layer_name_elem(v: &JsonValue) -> Result<Prop, ClientError> {
    v.get("layerName")
        .and_then(|x| x.as_str())
        .map(|s| Prop::Str(s.into()))
        .ok_or_else(|| {
            ClientError::InvalidResponse("collection element missing `layerName`".into())
        })
}

/// `list { <field> }` element where `<field>` is a boolean → `Prop::Bool`.
fn col_bool_elem(v: &JsonValue, field: &'static str) -> Result<Prop, ClientError> {
    v.get(field)
        .and_then(|x| x.as_bool())
        .map(Prop::Bool)
        .ok_or_else(|| {
            ClientError::InvalidResponse(format!("collection element missing bool `{}`", field))
        })
}

/// `list { <field> { timestamp datetime eventId } }` element → `Prop::List([])`
/// (None — no event in view) or `[Prop::Map]` (Some). Mirrors the single
/// EventTime decode.
fn col_event_time_elem(v: &JsonValue, field: &str) -> Result<Prop, ClientError> {
    let obj = match v.get(field) {
        None | Some(JsonValue::Null) => return Ok(Prop::List(Vec::<Prop>::new().into())),
        Some(o) => o,
    };
    match obj.get("timestamp").and_then(|x| x.as_i64()) {
        None => Ok(Prop::List(Vec::<Prop>::new().into())),
        Some(t) => {
            let mut pairs: Vec<(&'static str, Prop)> = vec![("timestamp", Prop::I64(t))];
            if let Some(d) = obj.get("datetime").and_then(|x| x.as_str()) {
                pairs.push(("datetime", Prop::Str(d.into())));
            }
            if let Some(e) = obj.get("eventId").and_then(|x| x.as_i64()) {
                pairs.push(("eventId", Prop::I64(e)));
            }
            Ok(Prop::List(vec![Prop::map(pairs)].into()))
        }
    }
}

/// Map a flat `list` array with `elem_fn`, producing `Prop::List`.
fn build_column<F>(terminal_val: &JsonValue, elem_fn: F) -> Result<Option<Prop>, ClientError>
where
    F: Fn(&JsonValue) -> Result<Prop, ClientError>,
{
    let arr = terminal_val
        .as_array()
        .ok_or_else(|| ClientError::InvalidResponse("columnar `list` not a JSON array".into()))?;
    let items: Result<Vec<Prop>, ClientError> = arr.iter().map(elem_fn).collect();
    Ok(Some(Prop::List(items?.into())))
}

/// Map the outer per-source `list` array (each element carrying its own inner
/// `list`) with `elem_fn`, producing `Prop::List(Prop::List(..))`.
fn build_nested_column<F>(terminal_val: &JsonValue, elem_fn: F) -> Result<Option<Prop>, ClientError>
where
    F: Fn(&JsonValue) -> Result<Prop, ClientError>,
{
    let outer = terminal_val
        .as_array()
        .ok_or_else(|| ClientError::InvalidResponse("columnar `list` not a JSON array".into()))?;
    let rows: Result<Vec<Prop>, ClientError> = outer
        .iter()
        .map(|row| {
            let inner = row.get("list").and_then(|v| v.as_array()).ok_or_else(|| {
                ClientError::InvalidResponse("columnar element missing inner `list` array".into())
            })?;
            let items: Result<Vec<Prop>, ClientError> = inner.iter().map(&elem_fn).collect();
            Ok(Prop::List(items?.into()))
        })
        .collect();
    Ok(Some(Prop::List(rows?.into())))
}

/// Decode one collection member's property/metadata container into a
/// `Prop::List` of `{key, value}` records. `container` is the JSON field name
/// (`metadata` or `properties`); the element shape is
/// `{ <container>: { values: [ {key, value}, ... ] } }`.
fn member_property_entries(el: &JsonValue, container: &str) -> Result<Prop, ClientError> {
    let values = el
        .get(container)
        .and_then(|c| c.get("values"))
        .and_then(|v| v.as_array())
        .ok_or_else(|| {
            ClientError::InvalidResponse(format!(
                "columnar element missing `{}.values` array",
                container
            ))
        })?;
    let items: Result<Vec<Prop>, ClientError> =
        values.iter().map(json_to_property_record).collect();
    Ok(Prop::List(items?.into()))
}

/// Map a flat `list` array of members into a per-member `Prop::List` of
/// property/metadata `{key, value}` records.
fn build_property_column(
    terminal_val: &JsonValue,
    container: &str,
) -> Result<Option<Prop>, ClientError> {
    let arr = terminal_val
        .as_array()
        .ok_or_else(|| ClientError::InvalidResponse("columnar `list` not a JSON array".into()))?;
    let rows: Result<Vec<Prop>, ClientError> = arr
        .iter()
        .map(|el| member_property_entries(el, container))
        .collect();
    Ok(Some(Prop::List(rows?.into())))
}

/// Map the outer per-source `list` array (each element carrying its own inner
/// `list` of members) into a per-source per-member `Prop::List` of
/// property/metadata `{key, value}` records.
fn build_nested_property_column(
    terminal_val: &JsonValue,
    container: &str,
) -> Result<Option<Prop>, ClientError> {
    let outer = terminal_val
        .as_array()
        .ok_or_else(|| ClientError::InvalidResponse("columnar `list` not a JSON array".into()))?;
    let rows: Result<Vec<Prop>, ClientError> = outer
        .iter()
        .map(|row| {
            let inner = row.get("list").and_then(|v| v.as_array()).ok_or_else(|| {
                ClientError::InvalidResponse("columnar element missing inner `list` array".into())
            })?;
            let members: Result<Vec<Prop>, ClientError> = inner
                .iter()
                .map(|el| member_property_entries(el, container))
                .collect();
            Ok(Prop::List(members?.into()))
        })
        .collect();
    Ok(Some(Prop::List(rows?.into())))
}

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
        // Nested id terminal — `PathFromGraph.ids` is a columnar `[[String]]`
        // (outer = per source, inner = that source's ids). Parse straight into
        // `Prop::List(Prop::List(Prop::Str))`.
        ReadExpr::NestedIds { .. } => {
            let outer = terminal_val.as_array().ok_or_else(|| {
                ClientError::InvalidResponse(format!("`{}` not a JSON array", terminal_key))
            })?;
            let rows: Result<Vec<Prop>, ClientError> = outer
                .iter()
                .map(|inner_val| {
                    let inner = inner_val.as_array().ok_or_else(|| {
                        ClientError::InvalidResponse(format!(
                            "`{}` element not a JSON array",
                            terminal_key
                        ))
                    })?;
                    let items: Result<Vec<Prop>, ClientError> = inner
                        .iter()
                        .map(|v| {
                            v.as_str().map(|s| Prop::Str(s.into())).ok_or_else(|| {
                                ClientError::InvalidResponse(format!(
                                    "`{}` inner element not a string",
                                    terminal_key
                                ))
                            })
                        })
                        .collect();
                    Ok(Prop::List(items?.into()))
                })
                .collect();
            Ok(Some(Prop::List(rows?.into())))
        }
        // Flat collection degree terminals — the JSON is an array of ints
        // (`degree`/`inDegree`/`outDegree`/`edgeHistoryCount` on a `Nodes`
        // or `PathFromNode` collection). Parsed as `Prop::List(Prop::I64)`.
        ReadExpr::CollectionDegree { .. }
        | ReadExpr::CollectionInDegree { .. }
        | ReadExpr::CollectionOutDegree { .. }
        | ReadExpr::CollectionEdgeHistoryCount { .. } => {
            let arr = terminal_val.as_array().ok_or_else(|| {
                ClientError::InvalidResponse(format!("`{}` not a JSON array", terminal_key))
            })?;
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
        // Columnar nested degree terminals — `PathFromGraph.{degree,inDegree,
        // outDegree}` are `[[Int]]` fields (outer = per source, inner = that
        // source's per-node degrees). Parse straight into
        // `Prop::List(Prop::List(Prop::I64))`.
        ReadExpr::NestedDegree { .. }
        | ReadExpr::NestedInDegree { .. }
        | ReadExpr::NestedOutDegree { .. } => {
            let outer = terminal_val.as_array().ok_or_else(|| {
                ClientError::InvalidResponse(format!("`{}` not a JSON array", terminal_key))
            })?;
            let rows: Result<Vec<Prop>, ClientError> = outer
                .iter()
                .map(|inner_val| {
                    let inner = inner_val.as_array().ok_or_else(|| {
                        ClientError::InvalidResponse(format!(
                            "`{}` element not a JSON array",
                            terminal_key
                        ))
                    })?;
                    let items: Result<Vec<Prop>, ClientError> = inner
                        .iter()
                        .map(|v| {
                            v.as_i64().map(Prop::I64).ok_or_else(|| {
                                ClientError::InvalidResponse(format!(
                                    "`{}` inner element not an i64",
                                    terminal_key
                                ))
                            })
                        })
                        .collect();
                    Ok(Prop::List(items?.into()))
                })
                .collect();
            Ok(Some(Prop::List(rows?.into())))
        }
        // Nested edgeHistoryCount (PathFromGraph → per-source PathFromNode)
        // still resolves via the `list` array of records
        // `[{"edgeHistoryCount": [1,2]}, ...]` — GqlPathFromGraph has no
        // columnar `edgeHistoryCount` field (only ids/degree/inDegree/outDegree).
        ReadExpr::NestedEdgeHistoryCount { .. } => {
            let outer = terminal_val.as_array().ok_or_else(|| {
                ClientError::InvalidResponse(format!("`{}` not a JSON array", terminal_key))
            })?;
            let rows: Result<Vec<Prop>, ClientError> = outer
                .iter()
                .map(|row| {
                    let inner = row
                        .get("edgeHistoryCount")
                        .and_then(|v| v.as_array())
                        .ok_or_else(|| {
                            ClientError::InvalidResponse(format!(
                                "`{}` element missing `edgeHistoryCount` array",
                                terminal_key
                            ))
                        })?;
                    let items: Result<Vec<Prop>, ClientError> = inner
                        .iter()
                        .map(|v| {
                            v.as_i64().map(Prop::I64).ok_or_else(|| {
                                ClientError::InvalidResponse(format!(
                                    "`{}` inner element not an i64",
                                    terminal_key
                                ))
                            })
                        })
                        .collect();
                    Ok(Prop::List(items?.into()))
                })
                .collect();
            Ok(Some(Prop::List(rows?.into())))
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
        // `findNodes { name }` — array of `{"name": "..."}` records. Same shape
        // as `sharedNeighbours`; the client wraps each name in a `RemoteNode`.
        ReadExpr::FindNodes { .. } => {
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
                            ClientError::InvalidResponse("findNodes element missing `name`".into())
                        })
                })
                .collect();
            Ok(Some(Prop::List(items?.into())))
        }
        // `findEdges { src { name } dst { name } }` — array of edge records.
        // Decode each into a 2-element inner list `[src, dst]`, matching the
        // shape used by `EdgesList`; the client wraps each in a `RemoteEdge`.
        ReadExpr::FindEdges { .. } => {
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
                            ClientError::InvalidResponse(
                                "findEdges element missing `src.name`".into(),
                            )
                        })?;
                    let dst = v
                        .get("dst")
                        .and_then(|d| d.get("name"))
                        .and_then(|n| n.as_str())
                        .ok_or_else(|| {
                            ClientError::InvalidResponse(
                                "findEdges element missing `dst.name`".into(),
                            )
                        })?;
                    Ok(Prop::List(
                        vec![Prop::Str(src.into()), Prop::Str(dst.into())].into(),
                    ))
                })
                .collect();
            Ok(Some(Prop::List(items?.into())))
        }
        // `getAllNodeTypes` — a JSON array of strings.
        ReadExpr::GetAllNodeTypes { .. } => {
            let arr = terminal_val.as_array().ok_or_else(|| {
                ClientError::InvalidResponse(format!("`{}` not a JSON array", terminal_key))
            })?;
            let items: Result<Vec<Prop>, ClientError> = arr
                .iter()
                .map(|v| {
                    v.as_str().map(|s| Prop::Str(s.into())).ok_or_else(|| {
                        ClientError::InvalidResponse("getAllNodeTypes element not a string".into())
                    })
                })
                .collect();
            Ok(Some(Prop::List(items?.into())))
        }
        // `getDtypeOf(key)` — nullable string (the `PropType` display form).
        ReadExpr::PropertyGetDtypeOf { .. } => {
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
        // Property terminals — each entry is a `{key, value}` record where
        // value is an untagged Prop (JSON number/string/bool/array/object).
        //
        // `PropertyGet`: single `{ value }` record or null (only the value is
        // selected — the caller supplied the key). Terminal value is null when
        // the key isn't present in the container — decode as `Ok(None)`.
        ReadExpr::PropertyGet { .. } => {
            if terminal_val.is_null() {
                Ok(None)
            } else {
                let value_json = terminal_val
                    .as_object()
                    .and_then(|o| o.get("value"))
                    .ok_or_else(|| {
                        ClientError::InvalidResponse("property record missing `value`".into())
                    })?;
                Ok(Some(json_to_prop(value_json)?))
            }
        }
        // `PropertyValues`: array of `{value}` records (values only) →
        // `Prop::List(...)` of the bare values.
        ReadExpr::PropertyValues { .. } => {
            let arr = terminal_val.as_array().ok_or_else(|| {
                ClientError::InvalidResponse(format!("`{}` not a JSON array", terminal_key))
            })?;
            let items: Result<Vec<Prop>, ClientError> = arr
                .iter()
                .map(|v| {
                    let value_json =
                        v.as_object().and_then(|o| o.get("value")).ok_or_else(|| {
                            ClientError::InvalidResponse("property record missing `value`".into())
                        })?;
                    json_to_prop(value_json)
                })
                .collect();
            Ok(Some(Prop::List(items?.into())))
        }
        // `PropertyItems`: array of `{key, value}` records → `Prop::List(...)`
        // of `Prop::Map({key, value})`.
        ReadExpr::PropertyItems { .. } => {
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
        // Nested edge-list terminal — `NestedEdges.list` returns a JSON array of
        // `Edges` records `[{"list": [{"src":{"name":..},"dst":{"name":..}}, ..]}, ..]`,
        // one per source node. We pull each record's `list` and decode each
        // element into a 2-element inner list `[src, dst]`, rebuilding the
        // nested `Prop::List(Prop::List(Prop::List(Prop::Str)))` (outer = per
        // source, middle = that source's edges, inner = `[src, dst]`). Mirrors
        // `EdgesList`, one level deeper.
        ReadExpr::NestedEdgesList { .. } => {
            let outer = terminal_val.as_array().ok_or_else(|| {
                ClientError::InvalidResponse(format!("`{}` not a JSON array", terminal_key))
            })?;
            let rows: Result<Vec<Prop>, ClientError> = outer
                .iter()
                .map(|row| {
                    let inner = row.get("list").and_then(|v| v.as_array()).ok_or_else(|| {
                        ClientError::InvalidResponse(format!(
                            "`{}` element missing `list` array",
                            terminal_key
                        ))
                    })?;
                    let items: Result<Vec<Prop>, ClientError> = inner
                        .iter()
                        .map(|v| {
                            let src = v
                                .get("src")
                                .and_then(|s| s.get("name"))
                                .and_then(|n| n.as_str())
                                .ok_or_else(|| {
                                    ClientError::InvalidResponse(
                                        "edge element missing `src.name`".into(),
                                    )
                                })?;
                            let dst = v
                                .get("dst")
                                .and_then(|d| d.get("name"))
                                .and_then(|n| n.as_str())
                                .ok_or_else(|| {
                                    ClientError::InvalidResponse(
                                        "edge element missing `dst.name`".into(),
                                    )
                                })?;
                            Ok(Prop::List(
                                vec![Prop::Str(src.into()), Prop::Str(dst.into())].into(),
                            ))
                        })
                        .collect();
                    Ok(Prop::List(items?.into()))
                })
                .collect();
            Ok(Some(Prop::List(rows?.into())))
        }
        // Exploded edge-list terminal — JSON shape is
        // `[{"src":{"name":..},"dst":{"name":..},"time":{"timestamp":..,"eventId":..},"layerName":..}, ..]`.
        // Decode each element into a 5-element inner list
        // `[src, dst, timestamp, event_id, layer_name]`.
        ReadExpr::ExplodedEdgesList { .. } => {
            let arr = terminal_val.as_array().ok_or_else(|| {
                ClientError::InvalidResponse(format!("`{}` not a JSON array", terminal_key))
            })?;
            let items: Result<Vec<Prop>, ClientError> =
                arr.iter().map(exploded_edge_elem).collect();
            Ok(Some(Prop::List(items?.into())))
        }
        // Layer-exploded edge-list terminal — each element `{src, dst, layerName}`
        // decodes to `[src, dst, layer_name]`.
        ReadExpr::ExplodedLayersEdgesList { .. } => {
            let arr = terminal_val.as_array().ok_or_else(|| {
                ClientError::InvalidResponse(format!("`{}` not a JSON array", terminal_key))
            })?;
            let items: Result<Vec<Prop>, ClientError> =
                arr.iter().map(exploded_layers_edge_elem).collect();
            Ok(Some(Prop::List(items?.into())))
        }
        // Nested exploded edge-list terminal — one per-source record each
        // holding its own `list` of exploded elements. Mirrors
        // `NestedEdgesList`, with the exploded element decoding.
        ReadExpr::NestedExplodedEdgesList { .. } => {
            let outer = terminal_val.as_array().ok_or_else(|| {
                ClientError::InvalidResponse(format!("`{}` not a JSON array", terminal_key))
            })?;
            let rows: Result<Vec<Prop>, ClientError> = outer
                .iter()
                .map(|row| {
                    let inner = row.get("list").and_then(|v| v.as_array()).ok_or_else(|| {
                        ClientError::InvalidResponse(format!(
                            "`{}` element missing `list` array",
                            terminal_key
                        ))
                    })?;
                    let items: Result<Vec<Prop>, ClientError> =
                        inner.iter().map(exploded_edge_elem).collect();
                    Ok(Prop::List(items?.into()))
                })
                .collect();
            Ok(Some(Prop::List(rows?.into())))
        }
        // Nested layer-exploded edge-list — like `NestedExplodedEdgesList` but
        // each inner element is `{src, dst, layerName}`.
        ReadExpr::NestedExplodedLayersEdgesList { .. } => {
            let outer = terminal_val.as_array().ok_or_else(|| {
                ClientError::InvalidResponse(format!("`{}` not a JSON array", terminal_key))
            })?;
            let rows: Result<Vec<Prop>, ClientError> = outer
                .iter()
                .map(|row| {
                    let inner = row.get("list").and_then(|v| v.as_array()).ok_or_else(|| {
                        ClientError::InvalidResponse(format!(
                            "`{}` element missing `list` array",
                            terminal_key
                        ))
                    })?;
                    let items: Result<Vec<Prop>, ClientError> =
                        inner.iter().map(exploded_layers_edge_elem).collect();
                    Ok(Prop::List(items?.into()))
                })
                .collect();
            Ok(Some(Prop::List(rows?.into())))
        }
        // Columnar accessors — FLAT collections. `terminal_val` is the `list`
        // array; each element carries the requested per-member field.
        ReadExpr::CollectionNames { .. } => build_column(terminal_val, col_name_elem),
        ReadExpr::CollectionNodeTypes { .. } => build_column(terminal_val, col_node_type_elem),
        ReadExpr::CollectionLayerNames { .. } => build_column(terminal_val, col_layer_names_elem),
        ReadExpr::CollectionLayerName { .. } => build_column(terminal_val, col_layer_name_elem),
        ReadExpr::CollectionEarliestTime { .. } => {
            build_column(terminal_val, |v| col_event_time_elem(v, "earliestTime"))
        }
        ReadExpr::CollectionLatestTime { .. } => {
            build_column(terminal_val, |v| col_event_time_elem(v, "latestTime"))
        }
        ReadExpr::CollectionTime { .. } => {
            build_column(terminal_val, |v| col_event_time_elem(v, "time"))
        }
        // Columnar accessors — NESTED collections. `terminal_val` is the outer
        // `list` array of per-source records, each with its own inner `list`.
        ReadExpr::NestedNames { .. } => build_nested_column(terminal_val, col_name_elem),
        ReadExpr::NestedNodeTypes { .. } => build_nested_column(terminal_val, col_node_type_elem),
        ReadExpr::NestedLayerNames { .. } => {
            build_nested_column(terminal_val, col_layer_names_elem)
        }
        ReadExpr::NestedLayerName { .. } => build_nested_column(terminal_val, col_layer_name_elem),
        ReadExpr::NestedEarliestTime { .. } => {
            build_nested_column(terminal_val, |v| col_event_time_elem(v, "earliestTime"))
        }
        ReadExpr::NestedLatestTime { .. } => {
            build_nested_column(terminal_val, |v| col_event_time_elem(v, "latestTime"))
        }
        ReadExpr::NestedTime { .. } => {
            build_nested_column(terminal_val, |v| col_event_time_elem(v, "time"))
        }
        // Boolean columnar accessors — FLAT collections.
        ReadExpr::CollectionIsActive { .. } => {
            build_column(terminal_val, |v| col_bool_elem(v, "isActive"))
        }
        ReadExpr::CollectionIsValid { .. } => {
            build_column(terminal_val, |v| col_bool_elem(v, "isValid"))
        }
        ReadExpr::CollectionIsDeleted { .. } => {
            build_column(terminal_val, |v| col_bool_elem(v, "isDeleted"))
        }
        ReadExpr::CollectionIsSelfLoop { .. } => {
            build_column(terminal_val, |v| col_bool_elem(v, "isSelfLoop"))
        }
        // Boolean columnar accessors — NESTED collections.
        ReadExpr::NestedIsActive { .. } => {
            build_nested_column(terminal_val, |v| col_bool_elem(v, "isActive"))
        }
        ReadExpr::NestedIsValid { .. } => {
            build_nested_column(terminal_val, |v| col_bool_elem(v, "isValid"))
        }
        ReadExpr::NestedIsDeleted { .. } => {
            build_nested_column(terminal_val, |v| col_bool_elem(v, "isDeleted"))
        }
        ReadExpr::NestedIsSelfLoop { .. } => {
            build_nested_column(terminal_val, |v| col_bool_elem(v, "isSelfLoop"))
        }
        // Columnar property / metadata accessors — FLAT collections. Each `list`
        // element carries a `<container> { values { key value } }` sub-object;
        // decode into a per-member `Prop::List` of `{key, value}` records.
        ReadExpr::CollectionMetadataValues { .. } => {
            build_property_column(terminal_val, "metadata")
        }
        ReadExpr::CollectionPropertiesValues { .. } => {
            build_property_column(terminal_val, "properties")
        }
        // Columnar property / metadata accessors — NESTED collections. The outer
        // `list` array holds per-source records, each with its own inner `list`
        // of members.
        ReadExpr::NestedMetadataValues { .. } => {
            build_nested_property_column(terminal_val, "metadata")
        }
        ReadExpr::NestedPropertiesValues { .. } => {
            build_nested_property_column(terminal_val, "properties")
        }
        // Bool-shaped terminals.
        ReadExpr::HasNode { .. }
        | ReadExpr::HasEdge { .. }
        | ReadExpr::IsActive { .. }
        | ReadExpr::IsValid { .. }
        | ReadExpr::IsDeleted { .. }
        | ReadExpr::IsSelfLoop { .. }
        | ReadExpr::IsEmpty { .. }
        | ReadExpr::HasLayer { .. }
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
        // EventTime terminals — the terminal value is the whole
        // `{ timestamp, datetime, eventId }` object. Decode it into a
        // `Prop::Map` (missing fields → absent keys); the client unwraps to a
        // `RemoteEventTime` via `expect_optional_event_time`. A JSON `null`
        // object (e.g. an empty graph) maps to `Ok(None)`.
        ReadExpr::EarliestTime { .. }
        | ReadExpr::LatestTime { .. }
        | ReadExpr::Start { .. }
        | ReadExpr::End { .. }
        | ReadExpr::Time { .. } => {
            if terminal_val.is_null() {
                return Ok(None);
            }
            let obj = terminal_val.as_object().ok_or_else(|| {
                ClientError::InvalidResponse(format!("`{}` not a JSON object", terminal_key))
            })?;
            // A present object with a null `timestamp` means "no event in this
            // view" (e.g. an empty window) — collapse to `None`, matching the
            // local API's `OptionalEventTime` and the pre-EventTime behavior.
            match obj.get("timestamp").and_then(|x| x.as_i64()) {
                None => Ok(None),
                Some(t) => {
                    let mut pairs: Vec<(&'static str, Prop)> = vec![("timestamp", Prop::I64(t))];
                    if let Some(d) = obj.get("datetime").and_then(|x| x.as_str()) {
                        pairs.push(("datetime", Prop::Str(d.into())));
                    }
                    if let Some(e) = obj.get("eventId").and_then(|x| x.as_i64()) {
                        pairs.push(("eventId", Prop::I64(e)));
                    }
                    Ok(Some(Prop::map(pairs)))
                }
            }
        }
        // Nullable i64-shaped terminals — server can return JSON `null`
        // (e.g. an empty graph has no `earliestEdgeTime.timestamp`). We map
        // JSON null → Ok(None); a valid number → Ok(Some(Prop::I64(n))).
        ReadExpr::EarliestEdgeTime { .. }
        | ReadExpr::LatestEdgeTime { .. }
        | ReadExpr::FirstUpdate { .. }
        | ReadExpr::LastUpdate { .. }
        | ReadExpr::WindowSize { .. }
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
            ReadExpr::View { input, op } => {
                go(input, out);
                out.push(view_op_json_key(op));
            }
            ReadExpr::Valid { input } => {
                go(input, out);
                out.push("valid");
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
            ReadExpr::CombinedHistory { input } => {
                go(input, out);
                out.push("combinedHistory");
            }
            ReadExpr::HistoryReverse { input } => {
                go(input, out);
                out.push("reverse");
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
            ReadExpr::FilterEdges { input, .. } => {
                go(input, out);
                out.push("filter");
            }
            ReadExpr::SelectEdges { input, .. } => {
                go(input, out);
                out.push("select");
            }
            ReadExpr::FilterGraphNodes { input, .. } => {
                go(input, out);
                out.push("filterNodes");
            }
            ReadExpr::FilterGraphEdges { input, .. } => {
                go(input, out);
                out.push("filterEdges");
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
            ReadExpr::PropertyValues { input, .. } | ReadExpr::PropertyItems { input, .. } => {
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
            ReadExpr::NestedIds { input } => {
                go(input, out);
                out.push("ids");
            }
            ReadExpr::CollectionDegree { input } => {
                go(input, out);
                out.push("degree");
            }
            ReadExpr::CollectionInDegree { input } => {
                go(input, out);
                out.push("inDegree");
            }
            ReadExpr::CollectionOutDegree { input } => {
                go(input, out);
                out.push("outDegree");
            }
            ReadExpr::CollectionEdgeHistoryCount { input } => {
                go(input, out);
                out.push("edgeHistoryCount");
            }
            // Columnar nested degree terminals resolve to the `[[Int]]` field
            // directly (one `blocking_compute` server-side).
            ReadExpr::NestedDegree { input } => {
                go(input, out);
                out.push("degree");
            }
            ReadExpr::NestedInDegree { input } => {
                go(input, out);
                out.push("inDegree");
            }
            ReadExpr::NestedOutDegree { input } => {
                go(input, out);
                out.push("outDegree");
            }
            // `edgeHistoryCount` is on nested EDGES (not this columnar path) —
            // still resolves via the per-source `list` array.
            ReadExpr::NestedEdgeHistoryCount { input } => {
                go(input, out);
                out.push("list");
            }
            ReadExpr::Count { input } => {
                go(input, out);
                out.push("count");
            }
            ReadExpr::EdgesList { input } => {
                go(input, out);
                out.push("list");
            }
            ReadExpr::NestedEdgesList { input } => {
                go(input, out);
                out.push("list");
            }
            ReadExpr::ExplodedEdgesList { input } => {
                go(input, out);
                out.push("list");
            }
            ReadExpr::NestedExplodedEdgesList { input } => {
                go(input, out);
                out.push("list");
            }
            ReadExpr::ExplodedLayersEdgesList { input } => {
                go(input, out);
                out.push("list");
            }
            ReadExpr::NestedExplodedLayersEdgesList { input } => {
                go(input, out);
                out.push("list");
            }
            ReadExpr::NodeFilterEdges { input, .. } => {
                go(input, out);
                out.push("filterEdges");
            }
            ReadExpr::EdgeFilterNodes { input, .. } => {
                go(input, out);
                out.push("filterNodes");
            }
            ReadExpr::EdgeEvent { input, .. } => {
                go(input, out);
                out.push("event");
            }
            ReadExpr::EdgeLayerEvent { input, .. } => {
                go(input, out);
                out.push("eventLayer");
            }
            // Columnar accessors all resolve through the `list` array.
            ReadExpr::CollectionNames { input }
            | ReadExpr::CollectionNodeTypes { input }
            | ReadExpr::CollectionLayerNames { input }
            | ReadExpr::CollectionLayerName { input }
            | ReadExpr::CollectionEarliestTime { input }
            | ReadExpr::CollectionLatestTime { input }
            | ReadExpr::CollectionTime { input }
            | ReadExpr::NestedNames { input }
            | ReadExpr::NestedNodeTypes { input }
            | ReadExpr::NestedLayerNames { input }
            | ReadExpr::NestedLayerName { input }
            | ReadExpr::NestedEarliestTime { input }
            | ReadExpr::NestedLatestTime { input }
            | ReadExpr::NestedTime { input }
            | ReadExpr::CollectionIsActive { input }
            | ReadExpr::CollectionIsValid { input }
            | ReadExpr::CollectionIsDeleted { input }
            | ReadExpr::CollectionIsSelfLoop { input }
            | ReadExpr::NestedIsActive { input }
            | ReadExpr::NestedIsValid { input }
            | ReadExpr::NestedIsDeleted { input }
            | ReadExpr::NestedIsSelfLoop { input }
            | ReadExpr::CollectionMetadataValues { input }
            | ReadExpr::CollectionPropertiesValues { input }
            | ReadExpr::NestedMetadataValues { input }
            | ReadExpr::NestedPropertiesValues { input } => {
                go(input, out);
                out.push("list");
            }
            ReadExpr::SharedNeighbours { input, .. } => {
                go(input, out);
                out.push("sharedNeighbours");
            }
            ReadExpr::FindNodes { input, .. } => {
                go(input, out);
                out.push("findNodes");
            }
            ReadExpr::FindEdges { input, .. } => {
                go(input, out);
                out.push("findEdges");
            }
            ReadExpr::GetAllNodeTypes { input } => {
                go(input, out);
                out.push("getAllNodeTypes");
            }
            ReadExpr::PropertyGetDtypeOf { input, .. } => {
                go(input, out);
                out.push("getDtypeOf");
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
            ReadExpr::HasLayer { input, .. } => {
                go(input, out);
                out.push("hasLayer");
            }
            ReadExpr::WindowSize { input } => {
                go(input, out);
                out.push("windowSize");
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
            // EventTime terminals — push ONE key (the object); the parse arm
            // decodes the whole `{ timestamp, datetime, eventId }` record.
            ReadExpr::EarliestTime { input } => {
                go(input, out);
                out.push("earliestTime");
            }
            ReadExpr::LatestTime { input } => {
                go(input, out);
                out.push("latestTime");
            }
            ReadExpr::Start { input } => {
                go(input, out);
                out.push("start");
            }
            ReadExpr::End { input } => {
                go(input, out);
                out.push("end");
            }
            // Remaining timestamp terminals — push TWO keys (outer + "timestamp").
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
/// Decode a leaf property value from a JSON response. Delegates to the model's
/// `gql_to_prop` (the single source of truth for JSON→`Prop` value semantics)
/// after lifting `serde_json::Value` into `async_graphql::Value`.
fn json_to_prop(v: &JsonValue) -> Result<Prop, ClientError> {
    let gql =
        GqlValue::from_json(v.clone()).map_err(|e| ClientError::InvalidResponse(e.to_string()))?;
    gql_to_prop(gql).map_err(|e| ClientError::InvalidResponse(e.message))
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
/// Decode one exploded-edge record — `{"src":{"name":..},"dst":{"name":..},
/// "time":{"timestamp":..,"eventId":..},"layerName":..}` — into the
/// 5-element list `[src, dst, timestamp, event_id, layer_name]` used by the
/// `ExplodedEdgesList` / `NestedExplodedEdgesList` terminals.
fn exploded_edge_elem(v: &JsonValue) -> Result<Prop, ClientError> {
    let src = v
        .get("src")
        .and_then(|s| s.get("name"))
        .and_then(|n| n.as_str())
        .ok_or_else(|| ClientError::InvalidResponse("edge element missing `src.name`".into()))?;
    let dst = v
        .get("dst")
        .and_then(|d| d.get("name"))
        .and_then(|n| n.as_str())
        .ok_or_else(|| ClientError::InvalidResponse("edge element missing `dst.name`".into()))?;
    let time = v.get("time").ok_or_else(|| {
        ClientError::InvalidResponse("exploded edge element missing `time`".into())
    })?;
    let timestamp = time
        .get("timestamp")
        .and_then(|t| t.as_i64())
        .ok_or_else(|| {
            ClientError::InvalidResponse("exploded edge element missing `time.timestamp`".into())
        })?;
    let event_id = time
        .get("eventId")
        .and_then(|i| i.as_i64())
        .ok_or_else(|| {
            ClientError::InvalidResponse("exploded edge element missing `time.eventId`".into())
        })?;
    let layer = v.get("layerName").and_then(|l| l.as_str()).ok_or_else(|| {
        ClientError::InvalidResponse("exploded edge element missing `layerName`".into())
    })?;
    Ok(Prop::List(
        vec![
            Prop::Str(src.into()),
            Prop::Str(dst.into()),
            Prop::I64(timestamp),
            Prop::I64(event_id),
            Prop::Str(layer.into()),
        ]
        .into(),
    ))
}

/// Decode one `ExplodedLayersEdgesList` element — `{src{name}, dst{name},
/// layerName}` — into `[src, dst, layer]` (no time; layer-exploded members have
/// a layer but not a single event time).
fn exploded_layers_edge_elem(v: &JsonValue) -> Result<Prop, ClientError> {
    let src = v
        .get("src")
        .and_then(|s| s.get("name"))
        .and_then(|n| n.as_str())
        .ok_or_else(|| ClientError::InvalidResponse("edge element missing `src.name`".into()))?;
    let dst = v
        .get("dst")
        .and_then(|d| d.get("name"))
        .and_then(|n| n.as_str())
        .ok_or_else(|| ClientError::InvalidResponse("edge element missing `dst.name`".into()))?;
    let layer = v.get("layerName").and_then(|l| l.as_str()).ok_or_else(|| {
        ClientError::InvalidResponse("layer-exploded edge element missing `layerName`".into())
    })?;
    Ok(Prop::List(
        vec![
            Prop::Str(src.into()),
            Prop::Str(dst.into()),
            Prop::Str(layer.into()),
        ]
        .into(),
    ))
}

fn build_not_found_error(expr: &ReadExpr, null_key: &str) -> ClientError {
    let desc = find_selection(expr, null_key)
        .unwrap_or_else(|| format!("unexpected null at `{}`", null_key));
    // A null at the graph root means the graph is missing — or hidden from the
    // caller, which the server reports identically (RBAC non-disclosure). That
    // is not a view-scoping failure, so surface it as `GraphNotFound` (message
    // reads "... does not exist") rather than `NotFound` (suffixed "not found
    // in view", which only makes sense for a node/edge outside the view).
    if null_key == "graph" {
        ClientError::GraphNotFound(format!("{desc} does not exist"))
    } else {
        ClientError::NotFound(desc)
    }
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
        ReadExpr::View { input, .. }
        | ReadExpr::Valid { input }
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
        | ReadExpr::CombinedHistory { input }
        | ReadExpr::HistoryReverse { input }
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
        | ReadExpr::FilterEdges { input, .. }
        | ReadExpr::SelectEdges { input, .. }
        | ReadExpr::FilterGraphNodes { input, .. }
        | ReadExpr::FilterGraphEdges { input, .. }
        | ReadExpr::Metadata { input }
        | ReadExpr::Properties { input }
        | ReadExpr::PropertyGet { input, .. }
        | ReadExpr::PropertyContains { input, .. }
        | ReadExpr::PropertyKeys { input }
        | ReadExpr::PropertyValues { input, .. }
        | ReadExpr::PropertyItems { input, .. }
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
        | ReadExpr::NestedIds { input }
        | ReadExpr::CollectionDegree { input }
        | ReadExpr::CollectionInDegree { input }
        | ReadExpr::CollectionOutDegree { input }
        | ReadExpr::CollectionEdgeHistoryCount { input }
        | ReadExpr::NestedDegree { input }
        | ReadExpr::NestedInDegree { input }
        | ReadExpr::NestedOutDegree { input }
        | ReadExpr::NestedEdgeHistoryCount { input }
        | ReadExpr::Count { input }
        | ReadExpr::EdgesList { input }
        | ReadExpr::NestedEdgesList { input }
        | ReadExpr::ExplodedEdgesList { input }
        | ReadExpr::NestedExplodedEdgesList { input }
        | ReadExpr::ExplodedLayersEdgesList { input }
        | ReadExpr::NestedExplodedLayersEdgesList { input }
        | ReadExpr::NodeFilterEdges { input, .. }
        | ReadExpr::EdgeFilterNodes { input, .. }
        | ReadExpr::EdgeEvent { input, .. }
        | ReadExpr::EdgeLayerEvent { input, .. }
        | ReadExpr::CollectionNames { input }
        | ReadExpr::CollectionNodeTypes { input }
        | ReadExpr::CollectionLayerNames { input }
        | ReadExpr::CollectionLayerName { input }
        | ReadExpr::CollectionEarliestTime { input }
        | ReadExpr::CollectionLatestTime { input }
        | ReadExpr::CollectionTime { input }
        | ReadExpr::NestedNames { input }
        | ReadExpr::NestedNodeTypes { input }
        | ReadExpr::NestedLayerNames { input }
        | ReadExpr::NestedLayerName { input }
        | ReadExpr::NestedEarliestTime { input }
        | ReadExpr::NestedLatestTime { input }
        | ReadExpr::NestedTime { input }
        | ReadExpr::CollectionIsActive { input }
        | ReadExpr::CollectionIsValid { input }
        | ReadExpr::CollectionIsDeleted { input }
        | ReadExpr::CollectionIsSelfLoop { input }
        | ReadExpr::NestedIsActive { input }
        | ReadExpr::NestedIsValid { input }
        | ReadExpr::NestedIsDeleted { input }
        | ReadExpr::NestedIsSelfLoop { input }
        | ReadExpr::CollectionMetadataValues { input }
        | ReadExpr::CollectionPropertiesValues { input }
        | ReadExpr::NestedMetadataValues { input }
        | ReadExpr::NestedPropertiesValues { input }
        | ReadExpr::SharedNeighbours { input, .. }
        | ReadExpr::FindNodes { input, .. }
        | ReadExpr::FindEdges { input, .. }
        | ReadExpr::GetAllNodeTypes { input }
        | ReadExpr::PropertyGetDtypeOf { input, .. }
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
        | ReadExpr::HasLayer { input, .. }
        | ReadExpr::WindowSize { input }
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
    use crate::model::graph::{
        filtering::{PropCondition, PropertyFilterNew},
        property::Value as GqlValue,
    };
    use std::sync::Arc;

    // ============ Unit tests for the read pipeline ============

    #[test]
    fn render_read_produces_nested_graphql() {
        let expr = ReadExpr::Degree {
            input: Arc::new(ReadExpr::Node {
                input: Arc::new(ReadExpr::View {
                    input: Arc::new(ReadExpr::Root { path: "g".into() }),
                    op: ViewOp::Window {
                        start: InputTime::Simple(0),
                        end: InputTime::Simple(10),
                    },
                }),
                id: "ben".into(),
            }),
        };
        let (query, vars) = render_read(&expr).unwrap();
        // A filter-free read carries no variables.
        assert!(vars.is_empty());
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

    // ============ Unit tests for GraphQL string escaping ============

    #[test]
    fn render_gql_str_escapes_special_chars() {
        // A GraphQL string literal uses JSON escaping (and supplies its own
        // surrounding quotes) — quote, backslash, and newline.
        assert_eq!(render_gql_str("O\"Brien"), r#""O\"Brien""#);
        assert_eq!(render_gql_str("back\\slash"), r#""back\\slash""#);
        assert_eq!(render_gql_str("multi\nline"), r#""multi\nline""#);
        // Control char U+0007 (BEL): must be ``, never Rust-debug `\u{7}`.
        let bell = render_gql_str("a\u{7}b");
        assert!(bell.contains("\\u0007"), "bell not JSON-escaped: {bell}");
        assert!(!bell.contains("\\u{7}"), "leaked Rust-debug escape: {bell}");
        // Non-ASCII unicode passes through as-is inside the quoted literal.
        assert_eq!(render_gql_str("🌟"), "\"🌟\"");
        // Always wrapped in its own quotes — callers must not add more.
        let q = render_gql_str("x");
        assert!(q.starts_with('"') && q.ends_with('"'), "unquoted: {q}");
    }

    #[test]
    fn node_name_position_is_escaped() {
        let expr = ReadExpr::HasNode {
            input: Arc::new(ReadExpr::Root { path: "g".into() }),
            id: "O\"Brien".into(),
        };
        let (q, _vars) = render_read(&expr).unwrap();
        // The escaped form must appear — and the naive bare-quote form must not.
        assert!(q.contains(r#"hasNode(name: "O\"Brien")"#), "got: {q}");
        assert!(!q.contains(r#"name: "O"Brien""#), "bare quote leaked: {q}");
    }

    #[test]
    fn filter_rides_a_json_variable_not_a_literal() {
        // A filter with a quote-bearing string value: it must be shipped as a
        // `$fN` JSON variable (escaping inherent, no query-string splicing to
        // break out of), not rendered into the query text.
        let filter = GqlNodeFilter::Property(PropertyFilterNew {
            name: "score".into(),
            where_: PropCondition::Eq(GqlValue::Str("O\"Brien".into())),
        });
        let mut vars = VarCollector::default();
        let reference = vars.add_node_filter(&filter).unwrap();
        assert_eq!(reference, "$f0");
        assert_eq!(vars.decls, "$f0: NodeFilter!");
        // The value lives in the variables map as JSON data, quote intact.
        let json = serde_json::to_string(&vars.vars["f0"]).unwrap();
        assert!(
            json.contains(r#"O\"Brien"#),
            "value not carried as JSON: {json}"
        );
    }

    #[test]
    fn scoped_write_to_missing_target_is_not_found() {
        // A node/edge-scoped write against a target that doesn't exist under the
        // current view: the server resolves the field to `null` with no error.
        // The client must surface that as `NotFound`, not a silent success.
        let missing_node: HashMap<String, serde_json::Value> = [(
            "updateGraph".to_string(),
            serde_json::json!({ "node": null }),
        )]
        .into();
        assert!(matches!(
            ensure_write_target_present(&missing_node, "node", "node 'ghost'".into()),
            Err(ClientError::NotFound(_))
        ));

        let missing_edge: HashMap<String, serde_json::Value> = [(
            "updateGraph".to_string(),
            serde_json::json!({ "edge": null }),
        )]
        .into();
        assert!(matches!(
            ensure_write_target_present(&missing_edge, "edge", "edge 'a' -> 'z'".into()),
            Err(ClientError::NotFound(_))
        ));

        // A present target (the field is a non-null object) is a success.
        let present: HashMap<String, serde_json::Value> = [(
            "updateGraph".to_string(),
            serde_json::json!({ "node": { "setNodeType": true } }),
        )]
        .into();
        assert!(ensure_write_target_present(&present, "node", "node 'a'".into()).is_ok());
    }

    #[test]
    fn property_key_rides_json_variable_intact() {
        // A quote-bearing property KEY is carried as JSON data too.
        let filter = GqlNodeFilter::Property(PropertyFilterNew {
            name: "wei\"rd".into(),
            where_: PropCondition::Eq(GqlValue::Str("v".into())),
        });
        let mut vars = VarCollector::default();
        vars.add_node_filter(&filter).unwrap();
        let json = serde_json::to_string(&vars.vars["f0"]).unwrap();
        assert!(
            json.contains(r#"wei\"rd"#),
            "key not carried as JSON: {json}"
        );
    }

    #[test]
    fn two_filters_in_one_chain_get_distinct_variables() {
        // Two filters in one composed read must render as two declarations
        // with each field arg referencing its own variable — the payloads must
        // not collide or swap.
        let prop_filter = |name: &str| {
            GqlNodeFilter::Property(PropertyFilterNew {
                name: name.into(),
                where_: PropCondition::Eq(GqlValue::Str("x".into())),
            })
        };
        let expr = ReadExpr::Ids {
            input: Arc::new(ReadExpr::FilterNodes {
                input: Arc::new(ReadExpr::FilterNodes {
                    input: Arc::new(ReadExpr::Nodes {
                        input: Arc::new(ReadExpr::Root { path: "g".into() }),
                    }),
                    filter: Arc::new(prop_filter("inner")),
                }),
                filter: Arc::new(prop_filter("outer")),
            }),
        };

        let (query, vars) = render_read(&expr).unwrap();
        assert!(
            query.contains("$f0: NodeFilter!") && query.contains("$f1: NodeFilter!"),
            "missing declarations in: {query}"
        );
        assert!(
            query.contains("filter(expr: $f0)") && query.contains("filter(expr: $f1)"),
            "field args don't reference both variables: {query}"
        );
        // Inner filter renders first, so it owns $f0; payloads pinned per slot.
        let f0 = serde_json::to_string(&vars["f0"]).unwrap();
        let f1 = serde_json::to_string(&vars["f1"]).unwrap();
        assert!(f0.contains("inner"), "wrong payload in $f0: {f0}");
        assert!(f1.contains("outer"), "wrong payload in $f1: {f1}");
    }

    #[test]
    fn non_finite_write_property_is_invalid_input_not_a_panic() {
        // The write path must surface a NaN property as `InvalidInput`, not
        // panic inside `json!` when `Value`'s serializer rejects it.
        let props: HashMap<String, Prop> = [("bad".to_string(), Prop::F64(f64::NAN))].into();
        assert!(matches!(
            properties_var(&props),
            Err(ClientError::InvalidInput(_))
        ));
    }

    #[test]
    fn non_finite_filter_values_are_rejected() {
        // serde_json cannot represent NaN/Infinity, so a filter carrying one
        // fails serialization — surfaced as `InvalidInput`, the same class the
        // old literal renderer rejected.
        for bad in [
            GqlValue::F64(f64::NAN),
            GqlValue::F64(f64::INFINITY),
            GqlValue::F32(f32::NEG_INFINITY),
        ] {
            let filter = GqlNodeFilter::Property(PropertyFilterNew {
                name: "x".into(),
                where_: PropCondition::Eq(bad),
            });
            let mut vars = VarCollector::default();
            assert!(matches!(
                vars.add_node_filter(&filter),
                Err(ClientError::InvalidInput(_))
            ));
        }

        // A finite float serializes fine.
        let filter = GqlNodeFilter::Property(PropertyFilterNew {
            name: "x".into(),
            where_: PropCondition::Eq(GqlValue::F64(1.5)),
        });
        let mut vars = VarCollector::default();
        assert!(vars.add_node_filter(&filter).is_ok());
    }

    #[test]
    fn parse_read_walks_to_terminal_value() {
        let expr = ReadExpr::Degree {
            input: Arc::new(ReadExpr::Node {
                input: Arc::new(ReadExpr::Root { path: "g".into() }),
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
        rg.add_node(1i64, "ben", None, None, None, None)
            .await
            .unwrap();
        rg.add_node(2i64, "hamza", None, None, None, None)
            .await
            .unwrap();
        rg.add_edge(3i64, "ben", "hamza", None, None, None)
            .await
            .unwrap();

        // Read path: composed expression through Transport
        // g.node("ben").degree() — after edge (ben -> hamza), ben has degree 1.
        // `.node()` fires a hasNode check and returns `Some(node)` when present,
        // `None` when absent (mirrors the local `Graph.node -> Optional[Node]`).
        let degree = rg
            .node("ben")
            .await
            .unwrap()
            .unwrap()
            .degree()
            .await
            .unwrap();
        assert_eq!(degree, 1, "ben should have degree 1 (single edge to hamza)");

        // With a windowed view, we can restrict to a time range.
        // Window (0, 5) includes the edge added at time 3, so degree is still 1.
        let degree_windowed = rg
            .window(InputTime::Simple(0), InputTime::Simple(5))
            .node("ben")
            .await
            .unwrap()
            .unwrap()
            .degree()
            .await
            .unwrap();
        assert_eq!(degree_windowed, 1);

        // Window (0, 2) excludes the edge (added at time 3), but ben himself
        // was added at t=1 so he's still in the view — his degree is 0.
        let degree_before_edge = rg
            .window(InputTime::Simple(0), InputTime::Simple(2))
            .node("ben")
            .await
            .unwrap()
            .unwrap()
            .degree()
            .await
            .unwrap();
        assert_eq!(degree_before_edge, 0);

        // A window that excludes ben's add_node event entirely — `.node()`
        // validates against the view chain and returns `None` (not an error).
        let absent = rg
            .window(InputTime::Simple(100), InputTime::Simple(200))
            .node("ben")
            .await
            .unwrap();
        assert!(
            absent.is_none(),
            "expected None for ben under window [100, 200), got Some"
        );

        // stop() only signals shutdown; wait() awaits the server task, whose
        // completion drops the graph cache and flushes dirty graphs
        // (DataInner::drop → flush_and_clear). Without it the tempdir is
        // deleted while background flushes are still writing into it, which
        // panics under panic-on-drop builds.
        running.stop().await;
        running.wait().await.unwrap();
    }

    /// End-to-end parity: handles from `collect()` must evaluate under the
    /// same composed view as the columnar accessors — the anchor-relative
    /// one-hop semantics of the local `nodes.filter(f)`:
    /// every node stays a member (and addressable), but each node's
    /// traversals only see neighbours that match the filter.
    ///
    /// Fixture: a(score=10), b(score=20), c(score=30); edges a-b, b-c, c-a;
    /// f = score > 15. Local ground truth: membership [a, b, c];
    /// degrees a=2 (b, c both match), b=1 (a dropped), c=1 (a dropped).
    #[tokio::test]
    async fn test_filtered_collect_matches_columnar_reads() {
        use crate::{client::remote_client::RemoteClient, server::GraphServer};
        use raphtory::db::api::storage::storage::Config;
        use reqwest::Url;
        use std::collections::HashMap as Map;
        use tempfile::tempdir;

        let tmp_dir = tempdir().unwrap();
        let server = GraphServer::new(tmp_dir.path().to_path_buf(), None, Config::default())
            .await
            .unwrap();
        let running = server.start_with_port(0).await.unwrap();
        let url = Url::parse(&format!("http://localhost:{}", running.port())).unwrap();
        let client = RemoteClient::new(url, None);
        client.new_graph("parity-filter", "EVENT").await.unwrap();
        let rg = client.remote_graph("parity-filter".into());

        for (name, score) in [("a", 10i64), ("b", 20), ("c", 30)] {
            let props: Map<String, Prop> = [("score".to_string(), Prop::I64(score))].into();
            rg.add_node(1i64, name, Some(props), None, None, None)
                .await
                .unwrap();
        }
        rg.add_edge(1i64, "a", "b", None, None, None).await.unwrap();
        rg.add_edge(2i64, "b", "c", None, None, None).await.unwrap();
        rg.add_edge(3i64, "c", "a", None, None, None).await.unwrap();

        let score_gt_15 = GqlNodeFilter::Property(PropertyFilterNew {
            name: "score".into(),
            where_: PropCondition::Gt(GqlValue::I64(15)),
        });

        // Membership: filter keeps every node addressable — including `a`,
        // which fails the filter itself.
        let filtered = rg.nodes().filter(score_gt_15.clone());
        let mut ids = filtered.ids().await.unwrap();
        ids.sort();
        assert_eq!(ids, ["a", "b", "c"], "filter must not narrow membership");

        // Handles from collect() must agree with the columnar degree.
        let columnar: Map<String, i64> = filtered
            .ids()
            .await
            .unwrap()
            .into_iter()
            .zip(filtered.degree().await.unwrap())
            .collect();
        for handle in filtered.collect().await.unwrap() {
            let got = handle.degree().await.unwrap();
            assert_eq!(
                got, columnar[&handle.id],
                "collect()[{}].degree() disagrees with columnar",
                handle.id
            );
        }
        assert_eq!(columnar["a"], 2, "a keeps both matching neighbours");
        assert_eq!(columnar["b"], 1, "a (score=10) dropped from b's edges");
        assert_eq!(columnar["c"], 1, "a (score=10) dropped from c's edges");

        // The filter keeps propagating through traversals on the handle.
        let by_id: Map<String, _> = filtered
            .collect()
            .await
            .unwrap()
            .into_iter()
            .map(|n| (n.id.clone(), n))
            .collect();
        let b_neighbours = by_id["b"].neighbours().ids().await.unwrap();
        assert_eq!(b_neighbours, ["c"], "b's neighbours under f exclude a");

        // select() narrows membership only — handles see the unfiltered graph.
        let selected = rg.nodes().select(score_gt_15.clone());
        let mut selected_ids = selected.ids().await.unwrap();
        selected_ids.sort();
        assert_eq!(selected_ids, ["b", "c"], "select narrows membership");
        for handle in selected.collect().await.unwrap() {
            assert_eq!(
                handle.degree().await.unwrap(),
                2,
                "select() handles must see the unfiltered graph"
            );
        }

        // A directly-fetched node's filter must propagate into descendants
        // materialized through it: b.filter(f).neighbours() is [c], and the
        // materialized c still evaluates under f (degree 1, not 2).
        let b = rg.node("b").await.unwrap().unwrap().filter(score_gt_15);
        let c_handles = b.neighbours().collect().await.unwrap();
        assert_eq!(c_handles.len(), 1);
        assert_eq!(c_handles[0].id, "c");
        assert_eq!(
            c_handles[0].degree().await.unwrap(),
            1,
            "filter must survive materialization through node traversals"
        );

        // Cross-entity: edge handles materialized under a node filter replay
        // it via the server's `filterNodes` field. b's only surviving edge is
        // b-c, and its src (b) still evaluates under f.
        let nested = rg
            .nodes()
            .filter(GqlNodeFilter::Property(PropertyFilterNew {
                name: "score".into(),
                where_: PropCondition::Gt(GqlValue::I64(15)),
            }));
        let rows = nested.edges().collect().await.unwrap();
        let ids_in_order = nested.ids().await.unwrap();
        let b_row = &rows[ids_in_order.iter().position(|id| id == "b").unwrap()];
        assert_eq!(b_row.len(), 1, "b keeps only the edge to c under f");
        assert_eq!((b_row[0].src.as_str(), b_row[0].dst.as_str()), ("b", "c"));
        assert_eq!(
            b_row[0].src().degree().await.unwrap(),
            1,
            "edge handle's node traversals must evaluate under f"
        );

        // stop() only signals shutdown; wait() awaits the server task, whose
        // completion drops the graph cache and flushes dirty graphs
        // (DataInner::drop → flush_and_clear). Without it the tempdir is
        // deleted while background flushes are still writing into it, which
        // panics under panic-on-drop builds.
        running.stop().await;
        running.wait().await.unwrap();
    }

    /// End-to-end parity: `explode().collect()` handles must be pinned to
    /// their event — `.time()` / `.layer_name()` / properties answer like
    /// local exploded edges — and `explode_layers().collect()` handles are
    /// pinned to their layer (`.layer_name()` resolves, `.time()` unavailable).
    #[tokio::test]
    async fn test_exploded_collect_pins_events() {
        use crate::{client::remote_client::RemoteClient, server::GraphServer};
        use raphtory::db::api::storage::storage::Config;
        use reqwest::Url;
        use std::collections::HashMap as Map;
        use tempfile::tempdir;

        let tmp_dir = tempdir().unwrap();
        let server = GraphServer::new(tmp_dir.path().to_path_buf(), None, Config::default())
            .await
            .unwrap();
        let running = server.start_with_port(0).await.unwrap();
        let url = Url::parse(&format!("http://localhost:{}", running.port())).unwrap();
        let client = RemoteClient::new(url, None);
        client.new_graph("parity-explode", "EVENT").await.unwrap();
        let rg = client.remote_graph("parity-explode".into());

        for (t, w) in [(1i64, 1i64), (5, 2)] {
            let props: Map<String, Prop> = [("weight".to_string(), Prop::I64(w))].into();
            rg.add_edge(t, "x", "y", Some(props), None, None)
                .await
                .unwrap();
        }

        let exploded = rg.edges().explode();

        // Columnar ground truth: one member per event, in event order.
        let times: Vec<i64> = exploded
            .time()
            .await
            .unwrap()
            .into_iter()
            .map(|t| t.unwrap().timestamp.unwrap())
            .collect();
        assert_eq!(times, [1, 5]);

        // Handles are pinned: same times, per-event property values, and a
        // working layer_name — none of which a whole-edge handle can answer.
        let handles = exploded.collect().await.unwrap();
        assert_eq!(handles.len(), 2);
        for (handle, (expect_t, expect_w)) in handles.iter().zip([(1i64, 1i64), (5, 2)]) {
            let t = handle.time().await.unwrap().unwrap().timestamp.unwrap();
            assert_eq!(t, expect_t, "handle not pinned to its event");
            let w = handle
                .properties()
                .get("weight")
                .await
                .unwrap()
                .expect("weight present");
            assert_eq!(w, Prop::I64(expect_w), "per-event property value");
            let layer = handle.layer_name().await.unwrap();
            assert_eq!(layer, "_default");
        }

        // Single-edge explode goes through the same pinning path.
        let e = rg.edge("x", "y").await.unwrap().unwrap();
        let single = e.explode().collect().await.unwrap();
        assert_eq!(single.len(), 2);
        assert_eq!(
            single[1].time().await.unwrap().unwrap().timestamp.unwrap(),
            5
        );

        // Layer-exploded members are re-addressable via the server's
        // `eventLayer` field: each handle resolves its `layer_name`, while
        // `time()` is unavailable (a layer instance spans all its events —
        // matching local `explode_layers()` semantics).
        let layered = rg.edges().explode_layers().collect().await.unwrap();
        assert!(!layered.is_empty());
        for h in &layered {
            assert!(
                h.layer_name().await.is_ok(),
                "layer_name must resolve on a layer-pinned handle"
            );
            assert!(
                h.time().await.is_err(),
                "time() must be unavailable on a layer-exploded handle (matches local)"
            );
        }

        // stop() only signals shutdown; wait() awaits the server task, whose
        // completion drops the graph cache and flushes dirty graphs
        // (DataInner::drop → flush_and_clear). Without it the tempdir is
        // deleted while background flushes are still writing into it, which
        // panics under panic-on-drop builds.
        running.stop().await;
        running.wait().await.unwrap();
    }
}
