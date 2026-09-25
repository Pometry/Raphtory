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
    model::graph::{
        filtering::GqlFilter,
        property::{gql_to_prop, parse_special_float},
    },
};
use async_graphql::{async_trait, Value as GqlValue};
use raphtory_api::core::entities::{
    properties::prop::{Prop, PropArray, PropMap, PropType},
    GID,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use std::{collections::HashMap, sync::Arc};

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
    async fn execute(&self, op: &Op) -> Result<JsonValue, ClientError> {
        match op {
            Op::Write(w) => {
                self.apply_write(w).await?;
                Ok(JsonValue::Null)
            }
            Op::Read(expr) => self.eval_read(expr).await,
        }
    }
}

// ============ Write path ============

impl GraphqlTransport {
    async fn apply_write(&self, op: &WriteOp) -> Result<(), ClientError> {
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

    async fn apply_add_node(&self, args: &AddNode) -> Result<(), ClientError> {
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
            "path": args.path,
            "time": input_time_var(&args.time),
            "name": gid_var(&args.id),
            "properties": opt_properties_var(&args.properties)?,
            "nodeType": args.node_type,
            "layer": args.layer,
        });
        let res = self.client.query(query, variables).await?;

        expect_update_success(&res, "addNode")?;
        Ok(())
    }

    async fn apply_create_node(&self, args: &CreateNode) -> Result<(), ClientError> {
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
            "path": args.path,
            "time": input_time_var(&args.time),
            "name": gid_var(&args.id),
            "properties": opt_properties_var(&args.properties)?,
            "nodeType": args.node_type,
            "layer": args.layer,
        });
        let res = self.client.query(query, variables).await?;

        expect_update_success(&res, "createNode")?;
        Ok(())
    }

    async fn apply_add_edge(&self, args: &AddEdge) -> Result<(), ClientError> {
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
            "path": args.path,
            "time": input_time_var(&args.time),
            "src": gid_var(&args.src),
            "dst": gid_var(&args.dst),
            "properties": opt_properties_var(&args.properties)?,
            "layer": args.layer,
        });
        let res = self.client.query(query, variables).await?;

        expect_update_success(&res, "addEdge")?;
        Ok(())
    }

    async fn apply_add_graph_property(&self, args: &AddGraphProperty) -> Result<(), ClientError> {
        let query = r#"
        query($path: String!, $t: TimeInput!, $properties: [PropertyInput!]!) {
          updateGraph(path: $path) {
            addProperties(t: $t, properties: $properties)
          }
        }
        "#;

        let variables = json!({
            "path": args.path,
            "t": input_time_var(&args.time),
            "properties": properties_var(&args.properties)?,
        });
        let res = self.client.query(query, variables).await?;

        expect_update_bool(&res, "addProperties")?;
        Ok(())
    }

    async fn apply_add_graph_metadata(&self, args: &AddGraphMetadata) -> Result<(), ClientError> {
        let query = r#"
        query($path: String!, $properties: [PropertyInput!]!) {
          updateGraph(path: $path) {
            addMetadata(properties: $properties)
          }
        }
        "#;

        let variables = json!({
            "path": args.path,
            "properties": properties_var(&args.properties)?,
        });
        let res = self.client.query(query, variables).await?;

        expect_update_bool(&res, "addMetadata")?;
        Ok(())
    }

    async fn apply_update_graph_metadata(
        &self,
        args: &UpdateGraphMetadata,
    ) -> Result<(), ClientError> {
        let query = r#"
        query($path: String!, $properties: [PropertyInput!]!) {
          updateGraph(path: $path) {
            updateMetadata(properties: $properties)
          }
        }
        "#;

        let variables = json!({
            "path": args.path,
            "properties": properties_var(&args.properties)?,
        });
        let res = self.client.query(query, variables).await?;

        expect_update_bool(&res, "updateMetadata")?;
        Ok(())
    }

    async fn apply_delete_edge(&self, args: &DeleteEdge) -> Result<(), ClientError> {
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
            "path": args.path,
            "time": input_time_var(&args.time),
            "src": gid_var(&args.src),
            "dst": gid_var(&args.dst),
            "layer": args.layer,
        });
        let res = self.client.query(query, variables).await?;

        expect_update_success(&res, "deleteEdge")?;
        Ok(())
    }

    async fn apply_set_node_type(&self, args: &SetNodeType) -> Result<(), ClientError> {
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
            "path": args.path,
            "name": gid_var(&args.id),
            "newType": args.new_type,
        });
        let res = self.client.query(query, variables).await?;
        ensure_write_target_present(&res, "node", format!("node '{}'", args.id))?;
        Ok(())
    }

    async fn apply_add_node_updates(&self, args: &AddNodeUpdates) -> Result<(), ClientError> {
        let query = r#"
            query($path: String!, $name: NodeId!, $time: TimeInput!,
                     $properties: [PropertyInput!], $layer: String) {
              updateGraph(path: $path) {
                node(name: $name) {
                  addUpdates(time: $time, properties: $properties, layer: $layer)
                }
              }
            }
        "#;

        let variables = json!({
            "path": args.path,
            "name": gid_var(&args.id),
            "time": input_time_var(&args.time),
            "properties": opt_properties_var(&args.properties)?,
            "layer": args.layer,
        });
        let res = self.client.query(query, variables).await?;
        ensure_write_target_present(&res, "node", format!("node '{}'", args.id))?;
        Ok(())
    }

    async fn apply_add_node_metadata(&self, args: &AddNodeMetadata) -> Result<(), ClientError> {
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
            "path": args.path,
            "name": gid_var(&args.id),
            "properties": properties_var(&args.properties)?,
        });
        let res = self.client.query(query, variables).await?;
        ensure_write_target_present(&res, "node", format!("node '{}'", args.id))?;
        Ok(())
    }

    async fn apply_update_node_metadata(
        &self,
        args: &UpdateNodeMetadata,
    ) -> Result<(), ClientError> {
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
            "path": args.path,
            "name": gid_var(&args.id),
            "properties": properties_var(&args.properties)?,
        });
        let res = self.client.query(query, variables).await?;
        ensure_write_target_present(&res, "node", format!("node '{}'", args.id))?;
        Ok(())
    }

    async fn apply_add_edge_updates(&self, args: &AddEdgeUpdates) -> Result<(), ClientError> {
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
            "path": args.path,
            "src": gid_var(&args.src),
            "dst": gid_var(&args.dst),
            "time": input_time_var(&args.time),
            "properties": opt_properties_var(&args.properties)?,
            "layer": args.layer,
        });
        let res = self.client.query(query, variables).await?;
        ensure_write_target_present(
            &res,
            "edge",
            format!("edge '{}' -> '{}'", args.src, args.dst),
        )?;
        Ok(())
    }

    async fn apply_delete_edge_at_time(&self, args: &DeleteEdgeAtTime) -> Result<(), ClientError> {
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
            "path": args.path,
            "src": gid_var(&args.src),
            "dst": gid_var(&args.dst),
            "time": input_time_var(&args.time),
            "layer": args.layer,
        });
        let res = self.client.query(query, variables).await?;
        ensure_write_target_present(
            &res,
            "edge",
            format!("edge '{}' -> '{}'", args.src, args.dst),
        )?;
        Ok(())
    }

    async fn apply_add_edge_metadata(&self, args: &AddEdgeMetadata) -> Result<(), ClientError> {
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
            "path": args.path,
            "src": gid_var(&args.src),
            "dst": gid_var(&args.dst),
            "properties": properties_var(&args.properties)?,
            "layer": args.layer,
        });
        let res = self.client.query(query, variables).await?;
        ensure_write_target_present(
            &res,
            "edge",
            format!("edge '{}' -> '{}'", args.src, args.dst),
        )?;
        Ok(())
    }

    async fn apply_update_edge_metadata(
        &self,
        args: &UpdateEdgeMetadata,
    ) -> Result<(), ClientError> {
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
            "path": args.path,
            "src": gid_var(&args.src),
            "dst": gid_var(&args.dst),
            "properties": properties_var(&args.properties)?,
            "layer": args.layer,
        });
        let res = self.client.query(query, variables).await?;
        ensure_write_target_present(
            &res,
            "edge",
            format!("edge '{}' -> '{}'", args.src, args.dst),
        )?;
        Ok(())
    }

    async fn apply_add_nodes(&self, args: &AddNodes) -> Result<(), ClientError> {
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
            "path": args.path,
            "nodes": to_var(&args.nodes)?,
        });
        let res = self.client.query(query, variables).await?;
        expect_update_bool(&res, "addNodes")?;
        Ok(())
    }

    async fn apply_add_edges(&self, args: &AddEdges) -> Result<(), ClientError> {
        let query = r#"
        query($path: String!, $edges: [EdgeAddition!]!) {
            updateGraph(path: $path) {
                addEdges(edges: $edges)
            }
        }
        "#;

        let variables = json!({
            "path": args.path,
            "edges": to_var(&args.edges)?,
        });
        let res = self.client.query(query, variables).await?;
        expect_update_bool(&res, "addEdges")?;
        Ok(())
    }
}

// ============ Read path ============

impl GraphqlTransport {
    async fn eval_read(&self, expr: &ReadExpr) -> Result<JsonValue, ClientError> {
        let (query, variables) = render_read(expr)?;
        let res = self
            .client
            .query(&query, JsonValue::Object(variables))
            .await?;
        dbg!(&res);
        parse_read(expr, res)
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
    fn add_filter(&mut self, f: &GqlFilter) -> Result<String, ClientError> {
        self.add("GqlFilter!", f)
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

/// Render the requested property columns as aliased single-key `get`s:
/// `c0: get(key: "score") { value } c1: get(key: "tag") { value }`.
///
/// One field per column. The key is identical for every member, so shipping it
/// per member is pure repetition — it is dropped, and the alias index *is* the
/// column index, so the response needs no key matching to pivot: position
/// carries the meaning. `dtype` stays: it disambiguates the value union when
/// decoding (u64 vs i64, f32 vs f64).
fn render_property_columns(keys: &[String], out: &mut String) {
    use std::fmt::Write;
    // An empty selection set is invalid GraphQL; both callers guarantee at
    // least one key (`get` sends one, `fetch_all` returns early on none).
    debug_assert!(!keys.is_empty(), "columnar fetch with no columns");
    for (i, key) in keys.iter().enumerate() {
        if i > 0 {
            out.push(' ');
        }
        let _ = write!(
            out,
            "c{i}: get(key: {}) {{ value dtype }}",
            render_gql_str(key)
        );
    }
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
    let mut out = String::from("[");
    for (i, sb) in sort_bys.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        push_node_sort_by(&mut out, sb);
    }
    out.push(']');
    out
}

/// Write one `NodeSortBy` — braces included — into `out`. Shared by the
/// node sort-by list renderer and by the nested `src`/`dst`/`neighbour`
/// keys of `EdgeSortBy`.
fn push_node_sort_by(out: &mut String, sb: &NodeSortBy) {
    out.push('{');
    let mut first = true;
    if let Some(rev) = sb.reverse {
        push_sort_field(out, &mut first, format_args!("reverse: {rev}"));
    }
    if let Some(id) = sb.id {
        push_sort_field(out, &mut first, format_args!("id: {id}"));
    }
    if let Some(name) = sb.name {
        push_sort_field(out, &mut first, format_args!("name: {name}"));
    }
    if let Some(type_) = sb.type_ {
        // GraphQL field name is `type`; `type_` is only the Rust spelling.
        push_sort_field(out, &mut first, format_args!("type: {type_}"));
    }
    if let Some(t) = sb.time {
        push_sort_field(
            out,
            &mut first,
            format_args!("time: {}", render_sort_by_time(t)),
        );
    }
    if let Some(ref p) = sb.property {
        push_sort_field(
            out,
            &mut first,
            format_args!("property: {}", render_gql_str(p)),
        );
    }
    out.push('}');
}

/// Append one `key: value` field to a sort-by object being rendered into
/// `out`, comma-separating after the first.
fn push_sort_field(out: &mut String, first: &mut bool, args: std::fmt::Arguments) {
    use std::fmt::Write;
    if !*first {
        out.push_str(", ");
    }
    *first = false;
    // Writing into a String cannot fail.
    let _ = out.write_fmt(args);
}

/// Append one `key: {…}` field — a nested `NodeSortBy` object — to a sort-by
/// object being rendered into `out`.
fn push_nested_node_sort_field(out: &mut String, first: &mut bool, key: &str, sb: &NodeSortBy) {
    push_sort_field(out, first, format_args!("{key}: "));
    push_node_sort_by(out, sb);
}

fn render_gql_str(s: &str) -> String {
    // A JSON string literal (including its surrounding quotes) is a valid
    // GraphQL string literal — quotes, backslashes, control chars, and unicode
    // are all escaped correctly. Callers must NOT add their own quotes.
    serde_json::to_string(s).expect("string serialization is infallible")
}

/// Render a node id as a GraphQL `NodeId` literal — a bare number for integer
/// ids, a quoted string for string ids. Stringifying an integer id here would
/// silently turn an integer-indexed graph into a string-indexed one.
fn render_gql_gid(gid: &GID) -> String {
    match gid {
        GID::U64(v) => v.to_string(),
        GID::Str(s) => render_gql_str(s),
    }
}

fn render_gid_list(items: &[GID]) -> String {
    items
        .iter()
        .map(render_gql_gid)
        .collect::<Vec<_>>()
        .join(", ")
}

/// A node id as a JSON variable value for the server's `NodeId` scalar —
/// number or string, mirroring `render_gql_gid`. (`json!(gid)` would emit the
/// externally tagged serde form, which the scalar rejects.)
fn gid_var(gid: &GID) -> JsonValue {
    match gid {
        GID::U64(v) => json!(v),
        GID::Str(s) => json!(s),
    }
}

/// Decode a `NodeId` scalar from a response — a string or a number, with the
/// JSON type preserved (`Prop::Str` / `Prop::U64`), matching the local `.id`.
fn gid_prop(v: &JsonValue) -> Result<Prop, ClientError> {
    if let Some(s) = v.as_str() {
        Ok(Prop::Str(s.into()))
    } else if let Some(n) = v.as_u64() {
        Ok(Prop::U64(n))
    } else {
        Err(ClientError::InvalidResponse(
            "node id not a string or non-negative int".into(),
        ))
    }
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
/// `src` / `dst` / `neighbour` keys, each a nested `NodeSortBy` object.
/// The top-level `reverse` applies only to `time` / `property`; a node key's
/// direction lives in its own nested `reverse`.
fn render_edge_sort_bys(sort_bys: &[EdgeSortBy]) -> String {
    let mut out = String::from("[");
    for (i, sb) in sort_bys.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push('{');
        let mut first = true;
        if let Some(rev) = sb.reverse {
            push_sort_field(&mut out, &mut first, format_args!("reverse: {rev}"));
        }
        if let Some(ref src) = sb.src {
            push_nested_node_sort_field(&mut out, &mut first, "src", src);
        }
        if let Some(ref dst) = sb.dst {
            push_nested_node_sort_field(&mut out, &mut first, "dst", dst);
        }
        if let Some(ref neighbour) = sb.neighbour {
            push_nested_node_sort_field(&mut out, &mut first, "neighbour", neighbour);
        }
        if let Some(t) = sb.time {
            push_sort_field(
                &mut out,
                &mut first,
                format_args!("time: {}", render_sort_by_time(t)),
            );
        }
        if let Some(ref p) = sb.property {
            push_sort_field(
                &mut out,
                &mut first,
                format_args!("property: {}", render_gql_str(p)),
            );
        }
        out.push('}');
    }
    out.push(']');
    out
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
    let mut out = String::with_capacity(256);
    render_read_into(expr, vars, &mut out)?;
    Ok(out)
}

/// Recursive worker for `render_read_body`: renders the input chain first,
/// then appends this level's fragment, so the whole query accumulates in one
/// shared buffer.
fn render_read_into(
    expr: &ReadExpr,
    vars: &mut VarCollector,
    out: &mut String,
) -> Result<(), ClientError> {
    use std::fmt::Write;
    // Writing into a String cannot fail, but the results are propagated rather
    // than discarded so that no line here reads as a swallowed error.
    match expr {
        ReadExpr::Root { path, graph_type } => {
            // `graphType` is a GraphQL enum, so its value renders as a bare
            // token rather than a quoted string.
            match graph_type {
                Some(flavour) => {
                    write!(
                        out,
                        "graph(path: {}, graphType: {})",
                        render_gql_str(path),
                        flavour.as_gql()
                    )?;
                }
                None => write!(out, "graph(path: {})", render_gql_str(path))?,
            }
        }
        // View chaining
        ReadExpr::View { input, op } => {
            render_read_into(input, vars, out)?;
            write!(out, " {{ {}", render_view_op(op))?;
        }
        ReadExpr::Valid { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { valid");
        }
        ReadExpr::Subgraph { input, nodes } => {
            render_read_into(input, vars, out)?;
            write!(out, " {{ subgraph(nodes: [{}])", render_gid_list(nodes))?;
        }
        ReadExpr::SubgraphNodeTypes { input, node_types } => {
            render_read_into(input, vars, out)?;
            write!(
                out,
                " {{ subgraphNodeTypes(nodeTypes: [{}])",
                render_string_list(node_types)
            )?;
        }
        ReadExpr::ExcludeNodes { input, nodes } => {
            render_read_into(input, vars, out)?;
            write!(out, " {{ excludeNodes(nodes: [{}])", render_gid_list(nodes))?;
        }
        ReadExpr::TypeFilter { input, node_types } => {
            render_read_into(input, vars, out)?;
            write!(
                out,
                " {{ typeFilter(nodeTypes: [{}])",
                render_string_list(node_types)
            )?;
        }
        // Selection
        ReadExpr::Node { input, id } => {
            render_read_into(input, vars, out)?;
            write!(out, " {{ node(name: {})", render_gql_gid(id))?;
        }
        ReadExpr::Edge { input, src, dst } => {
            render_read_into(input, vars, out)?;
            write!(
                out,
                " {{ edge(src: {}, dst: {})",
                render_gql_gid(src),
                render_gql_gid(dst)
            )?;
        }
        ReadExpr::Src { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { src");
        }
        ReadExpr::Dst { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { dst");
        }
        ReadExpr::Nbr { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { nbr");
        }
        ReadExpr::History { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { history");
        }
        ReadExpr::CombinedHistory { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { combinedHistory");
        }
        ReadExpr::HistoryReverse { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { reverse");
        }
        ReadExpr::HistoryContains {
            input,
            timestamp,
            event_id,
        } => {
            render_read_into(input, vars, out)?;
            match event_id {
                Some(event_id) => write!(
                    out,
                    " {{ contains(timestamp: {timestamp}, eventId: {event_id})"
                )?,
                None => write!(out, " {{ contains(timestamp: {timestamp})")?,
            }
        }
        ReadExpr::HistoryValueContains { input, value } => {
            render_read_into(input, vars, out)?;
            write!(out, " {{ contains(value: {value})")?;
        }
        ReadExpr::Deletions { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { deletions");
        }
        // Sub-container navigations
        ReadExpr::HistoryTimestamps { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { timestamps");
        }
        ReadExpr::HistoryEventIds { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { eventId");
        }
        ReadExpr::HistoryIntervals { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { intervals");
        }
        // Polymorphic sub-container terminals — render field names only;
        // return type is decided by the parent selection in `parse_read`.
        ReadExpr::SubList { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { list");
        }
        ReadExpr::SubListRev { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { listRev");
        }
        ReadExpr::SubPage {
            input,
            limit,
            offset,
            page_index,
        } => {
            render_read_into(input, vars, out)?;
            write!(
                out,
                " {{ page({})",
                render_page_args(*limit, *offset, *page_index)
            )?;
        }
        ReadExpr::SubPageRev {
            input,
            limit,
            offset,
            page_index,
        } => {
            render_read_into(input, vars, out)?;
            write!(
                out,
                " {{ pageRev({})",
                render_page_args(*limit, *offset, *page_index)
            )?;
        }
        // Intervals stats
        ReadExpr::IntervalsMean { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { mean");
        }
        ReadExpr::IntervalsMedian { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { median");
        }
        ReadExpr::IntervalsMax { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { max");
        }
        ReadExpr::IntervalsMin { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { min");
        }
        ReadExpr::Nodes { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { nodes");
        }
        ReadExpr::Neighbours { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { neighbours");
        }
        ReadExpr::InNeighbours { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { inNeighbours");
        }
        ReadExpr::OutNeighbours { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { outNeighbours");
        }
        ReadExpr::Edges { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { edges");
        }
        ReadExpr::NodeEdges { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { edges");
        }
        ReadExpr::InEdges { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { inEdges");
        }
        ReadExpr::OutEdges { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { outEdges");
        }
        ReadExpr::InComponent { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { inComponent");
        }
        ReadExpr::OutComponent { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { outComponent");
        }
        ReadExpr::Explode { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { explode");
        }
        ReadExpr::ExplodeLayers { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { explodeLayers");
        }
        ReadExpr::SortedNodes { input, sort_bys } => {
            render_read_into(input, vars, out)?;
            write!(
                out,
                " {{ sorted(sortBys: {})",
                render_node_sort_bys(sort_bys)
            )?;
        }
        ReadExpr::SortedEdges { input, sort_bys } => {
            render_read_into(input, vars, out)?;
            write!(
                out,
                " {{ sorted(sortBys: {})",
                render_edge_sort_bys(sort_bys)
            )?;
        }
        ReadExpr::Filtered { input, filter } => {
            // Unified server field `filter(expr: GqlFilter!)` — the same field
            // on Graph, Node, Edge, and every collection. Applies to this view
            // AND propagates to downstream traversals (contrast `select`,
            // which narrows membership at one step only).
            render_read_into(input, vars, out)?;
            write!(out, " {{ filter(expr: {})", vars.add_filter(filter)?)?;
        }
        ReadExpr::SelectNodes { input, filter } => {
            // Server field `select(expr: GqlFilter!)`: narrows the current
            // collection's membership only; downstream traversals see the
            // unfiltered graph.
            render_read_into(input, vars, out)?;
            write!(out, " {{ select(expr: {})", vars.add_filter(filter)?)?;
        }
        ReadExpr::SelectEdges { input, filter } => {
            // Server field `select(expr: GqlFilter!)` on `Edges`: narrows the
            // current collection's membership only.
            render_read_into(input, vars, out)?;
            write!(out, " {{ select(expr: {})", vars.add_filter(filter)?)?;
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
            render_read_into(input, vars, out)?;
            match layer {
                Some(l) => {
                    write!(
                        out,
                        " {{ event(time: {}, layer: {})",
                        time_arg,
                        render_gql_str(l)
                    )?;
                }
                None => {
                    write!(out, " {{ event(time: {})", time_arg)?;
                }
            }
        }
        // Server field `eventLayer(name: String!)` on `Edge` — pins a single
        // layer-exploded instance.
        ReadExpr::EdgeLayerEvent { input, layer } => {
            render_read_into(input, vars, out)?;
            write!(out, " {{ eventLayer(name: {})", render_gql_str(layer))?;
        }
        // Metadata / Properties navigation
        ReadExpr::Metadata { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { metadata");
        }
        ReadExpr::Properties { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { properties");
        }
        // Property terminals — `values` is compound (returns {key, value}
        // records); `get` selects only `{ value }` — the caller already knows
        // the key, so fetching it back is wasted bytes. Inner braces are
        // self-balanced; outer `get` / `values` opens one net brace,
        // contributing 1 to read_depth.
        ReadExpr::PropertyGet { input, key } => {
            render_read_into(input, vars, out)?;
            write!(
                out,
                " {{ get(key: {}) {{ value dtype }}",
                render_gql_str(key)
            )?;
        }
        ReadExpr::PropertyContains { input, key } => {
            render_read_into(input, vars, out)?;
            write!(out, " {{ contains(key: {})", render_gql_str(key))?;
        }
        ReadExpr::PropertyKeys { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { keys");
        }
        ReadExpr::PropertyGetDtypeOf { input, key } => {
            render_read_into(input, vars, out)?;
            write!(out, " {{ get(key: {}) {{ dtype }}", render_gql_str(key))?;
        }
        ReadExpr::PropertyValues { input, keys } => {
            render_read_into(input, vars, out)?;
            match keys {
                Some(ks) => {
                    write!(
                        out,
                        " {{ values(keys: [{}]) {{ value dtype }}",
                        render_string_list(ks)
                    )?;
                }
                None => out.push_str(" { values { value dtype }"),
            }
        }
        ReadExpr::PropertyItems { input, keys } => {
            render_read_into(input, vars, out)?;
            match keys {
                Some(ks) => {
                    write!(
                        out,
                        " {{ values(keys: [{}]) {{ key value dtype }}",
                        render_string_list(ks)
                    )?;
                }
                None => out.push_str(" { values { key value dtype }"),
            }
        }
        ReadExpr::TemporalProperties { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { temporal");
        }
        ReadExpr::TemporalPropertyByKey { input, key } => {
            render_read_into(input, vars, out)?;
            write!(out, " {{ get(key: {})", render_gql_str(key))?;
        }
        // `values(keys?) { key }` — we only fetch the key from each record;
        // clients build a `RemoteTemporalProperty` handle around each key.
        ReadExpr::TemporalPropertyList { input, keys } => {
            render_read_into(input, vars, out)?;
            match keys {
                Some(ks) => {
                    write!(
                        out,
                        " {{ values(keys: [{}]) {{ key }}",
                        render_string_list(ks)
                    )?;
                }
                None => out.push_str(" { values { key }"),
            }
        }
        ReadExpr::TemporalPropertyValueList { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { dtype values");
        }
        ReadExpr::TemporalPropertyAt { input, time } => {
            render_read_into(input, vars, out)?;
            write!(out, " {{ dtype at(t: {})", time)?;
        }
        ReadExpr::TemporalPropertyLatest { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { dtype latest");
        }
        ReadExpr::TemporalPropertyUnique { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { dtype unique");
        }
        ReadExpr::TemporalPropertyOrderedDedupe { input, latest_time } => {
            render_read_into(input, vars, out)?;
            write!(
                out,
                " {{ dtype orderedDedupe(latestTime: {}) {{ time {{ timestamp eventId }} value }}",
                latest_time
            )?;
        }
        ReadExpr::TemporalPropertySum { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { sum");
        }
        ReadExpr::TemporalPropertyMean { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { mean");
        }
        ReadExpr::TemporalPropertyAverage { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { average");
        }
        ReadExpr::TemporalPropertyMin { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { dtype min { time { timestamp eventId } value }");
        }
        ReadExpr::TemporalPropertyMax { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { dtype max { time { timestamp eventId } value }");
        }
        ReadExpr::TemporalPropertyMedian { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { dtype median { time { timestamp eventId } value }");
        }
        // Compound-structured tree — one RPC fetches everything.
        ReadExpr::Schema { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(
                " { schema { \
                nodes { typeName properties { key dtype variants } \
                    metadata { key dtype variants } } \
                layers { name edges { srcType dstType \
                    properties { key dtype variants } \
                    metadata { key dtype variants } } } }",
            );
        }
        // Terminals — no args after the field name
        ReadExpr::CountNodes { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { countNodes");
        }
        ReadExpr::CountEdges { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { countEdges");
        }
        ReadExpr::Degree { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { degree");
        }
        ReadExpr::InDegree { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { inDegree");
        }
        ReadExpr::OutDegree { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { outDegree");
        }
        ReadExpr::Name { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { name");
        }
        ReadExpr::HasNode { input, id } => {
            render_read_into(input, vars, out)?;
            write!(out, " {{ hasNode(name: {})", render_gql_gid(id))?;
        }
        ReadExpr::HasEdge { input, src, dst } => {
            render_read_into(input, vars, out)?;
            write!(
                out,
                " {{ hasEdge(src: {}, dst: {})",
                render_gql_gid(src),
                render_gql_gid(dst)
            )?;
        }
        ReadExpr::CountTemporalEdges { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { countTemporalEdges");
        }
        ReadExpr::Path { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { path");
        }
        ReadExpr::Namespace { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { namespace");
        }
        ReadExpr::Created { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { created");
        }
        ReadExpr::LastOpened { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { lastOpened");
        }
        ReadExpr::LastUpdated { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { lastUpdated");
        }
        ReadExpr::UniqueLayers { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { uniqueLayers");
        }
        ReadExpr::HasLayer { input, name } => {
            render_read_into(input, vars, out)?;
            write!(out, " {{ hasLayer(name: {})", render_gql_str(name))?;
        }
        ReadExpr::WindowSize { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { windowSize");
        }
        // Typed per-node ids: the columnar `ids` field is `[String!]!` (the
        // server stringifies), so the id is read from each node object
        // instead — `Node.id` is the typed `NodeId` scalar.
        ReadExpr::Ids { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { list { id }");
        }
        // `PathFromGraph.ids` is a columnar `[[String]]` field computed in ONE
        // server-side `blocking_compute` (vs `list { ids }`, which resolves one
        // `PathFromNode` object + its own `blocking_compute` per source). Opens
        // ONE net brace, same as `Ids`.
        ReadExpr::NestedIds { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { list { list { id } }");
        }
        // `PathFromGraph.sourceIds` — the flat `[String]` of source node ids,
        // aligned with `ids`' outer index. Opens ONE net brace, same as `Ids`.
        ReadExpr::SourceIds { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { sourceIds");
        }
        // Flat collection degree terminals — render the scalar-list field
        // directly on the `Nodes`/`PathFromNode` collection.
        ReadExpr::CollectionDegree { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { degree");
        }
        ReadExpr::CollectionInDegree { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { inDegree");
        }
        ReadExpr::CollectionOutDegree { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { outDegree");
        }
        ReadExpr::CollectionEdgeHistoryCount { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { edgeHistoryCount");
        }
        // Columnar `[[Int]]` fields on `PathFromGraph`, computed in ONE
        // server-side `blocking_compute` (vs `list { degree }` per source).
        // Mirror `NestedIds`.
        ReadExpr::NestedDegree { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { degree");
        }
        ReadExpr::NestedInDegree { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { inDegree");
        }
        ReadExpr::NestedOutDegree { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { outDegree");
        }
        ReadExpr::NestedEdgeHistoryCount { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { list { edgeHistoryCount }");
        }
        ReadExpr::Count { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { count");
        }
        // Compound structured terminal: renders as `list { src { name } dst { name } }`.
        // The `list` field opens ONE brace that gets closed by the outer `read_depth`;
        // the inner `src { name }` / `dst { name }` groups are self-balanced.
        ReadExpr::EdgesList { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { list { src { id } dst { id } }");
        }
        // `NestedEdges.list` returns `[Edges!]!` — one object per source node.
        // We render `list { list { src { name } dst { name } } }` and read each
        // per-source `Edges.list` to rebuild the nested `[[(src, dst)]]` shape
        // client-side. The outer `list` field opens ONE net brace (closed by
        // the outer `read_depth`); the inner `list { src { name } dst { name } }`
        // group is self-balanced. Mirrors `EdgesList`, one level deeper.
        ReadExpr::NestedEdgesList { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { list { list { src { id } dst { id } } }");
        }
        // Exploded-collection variant of `EdgesList`: adds each member's
        // event identity (`time { timestamp eventId }`, `layerName`) so
        // handles can be pinned from ONE response. Same brace accounting —
        // the outer `list` opens one net brace, inner groups self-balance.
        ReadExpr::ExplodedEdgesList { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { list { src { id } dst { id } time { timestamp eventId } layerName }");
        }
        // Nested variant of `ExplodedEdgesList` — mirrors `NestedEdgesList`.
        ReadExpr::NestedExplodedEdgesList { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(
                " { list { list { src { id } dst { id } time { timestamp eventId } layerName } }",
            );
        }
        // Layer-exploded members — `(src, dst, layer)` per member (no time).
        ReadExpr::ExplodedLayersEdgesList { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { list { src { id } dst { id } layerName }");
        }
        ReadExpr::NestedExplodedLayersEdgesList { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { list { list { src { id } dst { id } layerName } }");
        }
        // Columnar accessors — FLAT collections render `list { <field> }`.
        ReadExpr::CollectionNames { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { list { name }");
        }
        ReadExpr::CollectionNodeTypes { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { list { nodeType }");
        }
        ReadExpr::CollectionLayerNames { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { list { layerNames }");
        }
        ReadExpr::CollectionLayerName { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { list { layerName }");
        }
        ReadExpr::CollectionEarliestTime { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { list { earliestTime { timestamp eventId } }");
        }
        ReadExpr::CollectionLatestTime { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { list { latestTime { timestamp eventId } }");
        }
        ReadExpr::CollectionTime { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { list { time { timestamp eventId } }");
        }
        // Columnar accessors — NESTED collections render `list { list { <field> } }`.
        ReadExpr::NestedNames { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { list { list { name } }");
        }
        ReadExpr::NestedNodeTypes { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { list { list { nodeType } }");
        }
        ReadExpr::NestedLayerNames { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { list { list { layerNames } }");
        }
        ReadExpr::NestedLayerName { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { list { list { layerName } }");
        }
        ReadExpr::NestedEarliestTime { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { list { list { earliestTime { timestamp eventId } } }");
        }
        ReadExpr::NestedLatestTime { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { list { list { latestTime { timestamp eventId } } }");
        }
        ReadExpr::NestedTime { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { list { list { time { timestamp eventId } } }");
        }
        // Boolean columnar accessors — FLAT collections render `list { <field> }`.
        ReadExpr::CollectionIsActive { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { list { isActive }");
        }
        ReadExpr::CollectionIsValid { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { list { isValid }");
        }
        ReadExpr::CollectionIsDeleted { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { list { isDeleted }");
        }
        ReadExpr::CollectionIsSelfLoop { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { list { isSelfLoop }");
        }
        // Boolean columnar accessors — NESTED collections render `list { list { <field> } }`.
        ReadExpr::NestedIsActive { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { list { list { isActive } }");
        }
        ReadExpr::NestedIsValid { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { list { list { isValid } }");
        }
        ReadExpr::NestedIsDeleted { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { list { list { isDeleted } }");
        }
        ReadExpr::NestedIsSelfLoop { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { list { list { isSelfLoop } }");
        }
        // Columnar property / metadata accessors — descend per-member into the
        // `metadata` / `properties` container and read the requested columns
        // as aliased single-key `get`s (see `render_property_columns`), so
        // only those columns travel and nothing else ships over the wire.
        ReadExpr::CollectionMetadataValues { input, keys } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { list { metadata { ");
            render_property_columns(keys, out);
            out.push_str(" } }");
        }
        ReadExpr::CollectionPropertiesValues { input, keys } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { list { properties { ");
            render_property_columns(keys, out);
            out.push_str(" } }");
        }
        // NESTED collections render `list { list { <container> { .. } } }`.
        ReadExpr::NestedMetadataValues { input, keys } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { list { list { metadata { ");
            render_property_columns(keys, out);
            out.push_str(" } } }");
        }
        ReadExpr::NestedPropertiesValues { input, keys } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { list { list { properties { ");
            render_property_columns(keys, out);
            out.push_str(" } } }");
        }
        // Collection key lookup — the server's registry-backed collection
        // fields, which mirror the local views' `keys()` (the graph's
        // registered keys for the entity kind; empty collection → empty list).
        ReadExpr::CollectionMetadataKeys { input } | ReadExpr::NestedMetadataKeys { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { metadataKeys");
        }
        ReadExpr::CollectionPropertiesKeys { input } | ReadExpr::NestedPropertiesKeys { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { propertyKeys");
        }
        // Compound structured terminal on Graph: `sharedNeighbours(selectedNodes: [ids]) { name }`
        // — opens ONE net brace (the outer, before `sharedNeighbours`); the inner
        // `{ name }` is self-balanced.
        ReadExpr::SharedNeighbours { input, ids } => {
            render_read_into(input, vars, out)?;
            write!(
                out,
                " {{ sharedNeighbours(selectedNodes: [{}]) {{ id }}",
                render_gid_list(ids)
            )?;
        }
        ReadExpr::GetAllNodeTypes { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { getAllNodeTypes");
        }
        ReadExpr::Id { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { id");
        }
        ReadExpr::NodeType { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { nodeType");
        }
        ReadExpr::IsActive { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { isActive");
        }
        ReadExpr::IsEmpty { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { isEmpty");
        }
        // Compound structured terminal: `list { timestamp eventId }`
        // returns a list of records. Inner braces are self-balanced; the outer
        // `list` brace opens one net brace, contributing 1 to read_depth.
        //
        // The server's `datetime` field takes an optional format-string arg
        // (defaults to RFC 3339). We pass no arg to get the default.
        ReadExpr::HistoryList { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { list { timestamp eventId }");
        }
        ReadExpr::HistoryListRev { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { listRev { timestamp eventId }");
        }
        ReadExpr::HistoryPage {
            input,
            limit,
            offset,
            page_index,
        } => {
            render_read_into(input, vars, out)?;
            write!(
                out,
                " {{ page({}) {{ timestamp eventId }}",
                render_page_args(*limit, *offset, *page_index)
            )?;
        }
        ReadExpr::HistoryPageRev {
            input,
            limit,
            offset,
            page_index,
        } => {
            render_read_into(input, vars, out)?;
            write!(
                out,
                " {{ pageRev({}) {{ timestamp eventId }}",
                render_page_args(*limit, *offset, *page_index)
            )?;
        }
        ReadExpr::EdgeHistoryCount { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { edgeHistoryCount");
        }
        // Edge-specific terminals
        ReadExpr::EdgeIdPair { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { id");
        }
        ReadExpr::LayerNames { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { layerNames");
        }
        ReadExpr::LayerName { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { layerName");
        }
        ReadExpr::IsValid { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { isValid");
        }
        ReadExpr::IsDeleted { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { isDeleted");
        }
        ReadExpr::IsSelfLoop { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { isSelfLoop");
        }
        // EventTime terminals — fetch the full `{ timestamp eventId }`
        // record so the client can return a `EventTime` (drop-in parity
        // with the local API's `EventTime`, which carries the `event_id`).
        ReadExpr::EarliestTime { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { earliestTime { timestamp eventId");
        }
        ReadExpr::LatestTime { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { latestTime { timestamp eventId");
        }
        ReadExpr::Start { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { start { timestamp eventId");
        }
        ReadExpr::End { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { end { timestamp eventId");
        }
        ReadExpr::EarliestEdgeTime { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { earliestEdgeTime { timestamp eventId");
        }
        ReadExpr::LatestEdgeTime { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { latestEdgeTime { timestamp eventId");
        }
        ReadExpr::FirstUpdate { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { firstUpdate { timestamp");
        }
        ReadExpr::LastUpdate { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { lastUpdate { timestamp");
        }
        ReadExpr::Time { input } => {
            render_read_into(input, vars, out)?;
            out.push_str(" { time { timestamp eventId");
        }
    }
    Ok(())
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
        | ReadExpr::HistoryContains { input, .. }
        | ReadExpr::HistoryValueContains { input, .. }
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
        | ReadExpr::Filtered { input, .. }
        | ReadExpr::SelectNodes { input, .. }
        | ReadExpr::SelectEdges { input, .. }
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
        | ReadExpr::SourceIds { input }
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
        | ReadExpr::CollectionMetadataValues { input, .. }
        | ReadExpr::CollectionPropertiesValues { input, .. }
        | ReadExpr::NestedMetadataValues { input, .. }
        | ReadExpr::NestedPropertiesValues { input, .. }
        | ReadExpr::CollectionMetadataKeys { input }
        | ReadExpr::CollectionPropertiesKeys { input }
        | ReadExpr::NestedMetadataKeys { input }
        | ReadExpr::NestedPropertiesKeys { input }
        | ReadExpr::SharedNeighbours { input, .. }
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

fn parse_read(
    expr: &ReadExpr,
    mut root: HashMap<String, JsonValue>,
) -> Result<JsonValue, ClientError> {
    // The response map is indexed as-is — no re-wrapping into a `JsonValue`
    // (which would rebuild the map) and no `serde_json::to_value` (which
    // would deep-copy the payload). Everything below the root field borrows.
    let path = build_json_path(expr);
    // Every executable read names the graph root field plus a terminal, so the
    // path has at least two segments. A bare `Root` has no terminal to read —
    // reject it rather than indexing past the end (the slice below would panic).

    let [first, rest @ ..] = path.as_slice() else {
        return Err(ClientError::InvalidInput(
            "read expression has no terminal to read".into(),
        ));
    };
    if rest.is_empty() {
        return Err(ClientError::InvalidInput(
            "read expression has no terminal to read".into(),
        ));
    }
    let mut cursor = root
        .remove(*first)
        .ok_or_else(|| ClientError::InvalidResponse(format!("missing `{}` in response", first)))?;
    if cursor.is_null() && path.len() > 1 {
        return Err(build_not_found_error(expr, first));
    }
    for key in rest {
        cursor = match cursor {
            JsonValue::Object(mut map) => map.remove(*key).ok_or_else(|| {
                ClientError::InvalidResponse(format!("missing `{}` in response", key))
            })?,
            _ => return Err(build_not_found_error(expr, key)),
        }
    }
    Ok(cursor)
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
            ReadExpr::HistoryContains { input, .. } => {
                go(input, out);
                out.push("contains");
            }
            ReadExpr::HistoryValueContains { input, .. } => {
                go(input, out);
                out.push("contains");
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
            ReadExpr::Filtered { input, .. } => {
                go(input, out);
                out.push("filter");
            }
            ReadExpr::SelectNodes { input, .. } => {
                go(input, out);
                out.push("select");
            }
            ReadExpr::SelectEdges { input, .. } => {
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
            }
            ReadExpr::TemporalPropertyAt { input, .. } => {
                go(input, out);
            }
            ReadExpr::TemporalPropertyLatest { input } => {
                go(input, out);
            }
            ReadExpr::TemporalPropertyUnique { input } => {
                go(input, out);
            }
            ReadExpr::TemporalPropertyOrderedDedupe { input, .. } => {
                go(input, out);
            }
            ReadExpr::TemporalPropertySum { input } => {
                go(input, out);
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
            }
            ReadExpr::TemporalPropertyMax { input } => {
                go(input, out);
            }
            ReadExpr::TemporalPropertyMedian { input } => {
                go(input, out);
            }
            ReadExpr::Schema { input } => {
                go(input, out);
                out.push("schema");
            }
            ReadExpr::Ids { input } => {
                go(input, out);
                out.push("list");
            }
            ReadExpr::NestedIds { input } => {
                go(input, out);
                out.push("list");
            }
            ReadExpr::SourceIds { input } => {
                go(input, out);
                out.push("sourceIds");
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
            | ReadExpr::CollectionMetadataValues { input, .. }
            | ReadExpr::CollectionPropertiesValues { input, .. }
            | ReadExpr::NestedMetadataValues { input, .. }
            | ReadExpr::NestedPropertiesValues { input, .. } => {
                go(input, out);
                out.push("list");
            }
            ReadExpr::CollectionMetadataKeys { input } | ReadExpr::NestedMetadataKeys { input } => {
                go(input, out);
                out.push("metadataKeys");
            }
            ReadExpr::CollectionPropertiesKeys { input }
            | ReadExpr::NestedPropertiesKeys { input } => {
                go(input, out);
                out.push("propertyKeys");
            }
            ReadExpr::SharedNeighbours { input, .. } => {
                go(input, out);
                out.push("sharedNeighbours");
            }
            ReadExpr::GetAllNodeTypes { input } => {
                go(input, out);
                out.push("getAllNodeTypes");
            }
            ReadExpr::PropertyGetDtypeOf { input, .. } => {
                go(input, out);
                out.push("get");
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
            // EventTime-shaped: the arm reads the whole object, so the path
            // stops at the field (as `earliestTime` above does).
            ReadExpr::EarliestEdgeTime { input } => {
                go(input, out);
                out.push("earliestEdgeTime");
            }
            ReadExpr::LatestEdgeTime { input } => {
                go(input, out);
                out.push("latestEdgeTime");
            }
            // Remaining timestamp terminals — push TWO keys (outer + "timestamp").
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
/// Replace every `dtype` value in a schema response with its own JSON text,
/// so the typed form rides the `Prop` tree as an opaque string instead of
/// being decoded as if it were property data.
fn stash_dtypes_as_json_text(v: &mut JsonValue) {
    match v {
        JsonValue::Object(map) => {
            for (key, value) in map.iter_mut() {
                if key == "dtype" {
                    *value = JsonValue::String(value.to_string());
                } else {
                    stash_dtypes_as_json_text(value);
                }
            }
        }
        JsonValue::Array(items) => items.iter_mut().for_each(stash_dtypes_as_json_text),
        _ => {}
    }
}

fn json_to_prop(v: JsonValue) -> Result<Prop, ClientError> {
    let gql = GqlValue::from_json(v).map_err(|e| ClientError::InvalidResponse(e.to_string()))?;
    gql_to_prop(gql).map_err(|e| ClientError::InvalidResponse(e.message))
}

/// Decode the serde JSON form of `PropType` served by the `dtype` fields.
pub(crate) fn json_to_prop_type(v: &JsonValue) -> Result<PropType, ClientError> {
    PropType::deserialize(v).map_err(|e| ClientError::InvalidResponse(format!("bad dtype: {e}")))
}

/// Type-directed decode of an untagged JSON property value: the server-declared
/// `dtype` recovers what the JSON shape alone cannot — exact numeric widths
/// (`U8` instead of `I64`), datetimes (epoch-millis numbers), decimals, and
/// non-finite floats (protobuf-style `"NaN"`/`"Infinity"`/`"-Infinity"`
/// sentinels in float positions).
pub(crate) fn json_to_prop_typed(dtype: &PropType, v: JsonValue) -> Result<Prop, ClientError> {
    let mismatch =
        |want: &str| ClientError::InvalidResponse(format!("dtype says {want}, got `{v}`"));
    let int = |want: &str| v.as_i64().ok_or_else(|| mismatch(want));
    let uint = |want: &str| v.as_u64().ok_or_else(|| mismatch(want));
    let float = |want: &str| {
        v.as_f64()
            .or_else(|| v.as_str().and_then(parse_special_float))
            .ok_or_else(|| mismatch(want))
    };
    Ok(match dtype {
        // No type information recorded — fall back to shape-based decoding.
        PropType::Empty => json_to_prop(v)?,
        PropType::Str => Prop::Str(v.as_str().ok_or_else(|| mismatch("Str"))?.into()),
        PropType::Bool => Prop::Bool(v.as_bool().ok_or_else(|| mismatch("Bool"))?),
        // Narrowing conversions check the range: a value outside the declared
        // width is a protocol error, not something to wrap silently.
        PropType::U8 => Prop::U8(u8::try_from(uint("U8")?).map_err(|_| mismatch("U8"))?),
        PropType::U16 => Prop::U16(u16::try_from(uint("U16")?).map_err(|_| mismatch("U16"))?),
        PropType::U32 => Prop::U32(u32::try_from(uint("U32")?).map_err(|_| mismatch("U32"))?),
        PropType::U64 => Prop::U64(uint("U64")?),
        PropType::I32 => Prop::I32(i32::try_from(int("I32")?).map_err(|_| mismatch("I32"))?),
        PropType::I64 => Prop::I64(int("I64")?),
        PropType::F32 => Prop::F32(float("F32")? as f32),
        PropType::F64 => Prop::F64(float("F64")?),
        // The wire carries datetimes as epoch-millis numbers.
        PropType::NDTime => Prop::NDTime(
            chrono::DateTime::from_timestamp_millis(int("NDTime")?)
                .ok_or_else(|| mismatch("NDTime"))?
                .naive_utc(),
        ),
        PropType::DTime => Prop::DTime(
            chrono::DateTime::from_timestamp_millis(int("DTime")?)
                .ok_or_else(|| mismatch("DTime"))?,
        ),
        PropType::Decimal { .. } => {
            let s = v.as_str().ok_or_else(|| mismatch("Decimal"))?;
            Prop::Decimal(s.parse().map_err(|_| mismatch("Decimal"))?)
        }
        PropType::List(inner) => match v {
            JsonValue::Array(arr) => Prop::List(
                PropArray::try_from(
                    arr.into_iter()
                        .map(|item| json_to_prop_typed(inner, item))
                        .collect::<Result<Vec<_>, _>>()?,
                )
                .map_err(|err| {
                    ClientError::InvalidResponse(format!(
                        "heterogeneous types {} and {} in List",
                        err.expected, err.actual
                    ))
                })?,
            ),
            _ => Err(mismatch("List"))?,
        },
        PropType::Map(fields) => match v {
            JsonValue::Object(obj) => {
                let entries = obj
                    .into_iter()
                    .map(|(k, item)| {
                        let prop = match fields.get(&k) {
                            Some(field_type) => json_to_prop_typed(field_type, item)?,
                            None => Err(ClientError::InvalidResponse(format!(
                                "unexpected field {k} in map"
                            )))?,
                        };
                        Ok::<_, ClientError>((k.into(), prop))
                    })
                    .collect::<Result<PropMap, _>>()?;
                Prop::Map(Arc::new(entries))
            }
            _ => Err(mismatch("Map"))?,
        },
    })
}

fn build_not_found_error(expr: &ReadExpr, null_key: &str) -> ClientError {
    let desc = find_selection(expr, null_key)
        .unwrap_or_else(|| format!("unexpected null at `{}`", null_key));
    // A null at the graph root means the graph is missing — or hidden from the
    // caller, which the server deliberately reports identically. That
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
        ReadExpr::Root { path, .. } if null_key == "graph" => Some(format!("Graph '{}'", path)),
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
        | ReadExpr::HistoryContains { input, .. }
        | ReadExpr::HistoryValueContains { input, .. }
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
        | ReadExpr::Filtered { input, .. }
        | ReadExpr::SelectNodes { input, .. }
        | ReadExpr::SelectEdges { input, .. }
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
        | ReadExpr::SourceIds { input }
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
        | ReadExpr::CollectionMetadataValues { input, .. }
        | ReadExpr::CollectionPropertiesValues { input, .. }
        | ReadExpr::NestedMetadataValues { input, .. }
        | ReadExpr::NestedPropertiesValues { input, .. }
        | ReadExpr::CollectionMetadataKeys { input }
        | ReadExpr::CollectionPropertiesKeys { input }
        | ReadExpr::NestedMetadataKeys { input }
        | ReadExpr::NestedPropertiesKeys { input }
        | ReadExpr::SharedNeighbours { input, .. }
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

    /// A read expression with no terminal (a bare `Root`) is rejected, not
    /// indexed past the end — the old slice arithmetic panicked in release.
    #[test]
    fn read_with_no_terminal_is_rejected() {
        let root = ReadExpr::Root {
            path: "g".into(),
            graph_type: None,
        };
        let err = parse_read(&root, HashMap::new()).unwrap_err();
        assert!(
            matches!(err, ClientError::InvalidInput(_)),
            "bare Root must be InvalidInput, got {err:?}"
        );
    }

    /// A value outside its declared width is a protocol error, never a silent
    /// wrap (`300 as u8` would decode to 44).
    #[test]
    fn out_of_range_typed_decode_is_an_error_not_a_wrap() {
        use raphtory_api::core::entities::properties::prop::PropType;
        for (dtype, val) in [
            (PropType::U8, json!(300)),
            (PropType::U16, json!(70_000)),
            (PropType::U32, json!(5_000_000_000u64)),
            (PropType::I32, json!(3_000_000_000i64)),
        ] {
            let err = json_to_prop_typed(&dtype, val.clone()).unwrap_err();
            assert!(
                matches!(err, ClientError::InvalidResponse(_)),
                "{dtype:?} {val} must be InvalidResponse, got {err:?}"
            );
        }
        assert_eq!(
            json_to_prop_typed(&PropType::U8, json!(255)).unwrap(),
            Prop::U8(255)
        );
    }
    use super::*;
    use crate::{
        client::{
            Column, RemoteEdgeSchema, RemoteGraphSchema, RemoteLayerSchema, RemoteMetadata,
            RemoteNodeSchema, RemotePropertySchema, RemotePropertyTuple,
        },
        data::GqlGraphType,
        model::graph::{
            filtering::{GqlNodeFilter, PropCondition, PropertyFilterNew},
            property::Value as GqlValue,
        },
        server::GraphServer,
    };
    use raphtory::{
        db::graph::views::filter::model::node_filter::CompositeNodeFilter,
        prelude::{Args, NO_PROPS},
    };
    use raphtory_api::core::storage::timeindex::{AsTime, EventTime};
    use reqwest::Url;
    use std::{collections::HashMap as Map, hash::Hash, str::FromStr, sync::Arc};
    use tempfile::tempdir;
    // ============ Unit tests for the read pipeline ============

    #[test]
    fn render_read_produces_nested_graphql() {
        let expr = ReadExpr::Degree {
            input: Arc::new(ReadExpr::Node {
                input: Arc::new(ReadExpr::View {
                    input: Arc::new(ReadExpr::Root {
                        path: "g".into(),
                        graph_type: None,
                    }),
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

    /// A flavour override renders as the server's `graphType:` argument — a
    /// bare enum token, not a quoted string.
    #[test]
    fn root_renders_graph_type_override() {
        let expr = ReadExpr::CountNodes {
            input: Arc::new(ReadExpr::Root {
                path: "g".into(),
                graph_type: Some(GqlGraphType::Event),
            }),
        };
        let (query, _) = render_read(&expr).unwrap();
        assert!(
            query.contains("graph(path: \"g\", graphType: EVENT)"),
            "graphType not rendered as enum token: {query}"
        );
        assert_eq!(query.matches('{').count(), query.matches('}').count());
    }

    /// Requested columns render as aliased single-key `get`s carrying only the
    /// value and its dtype — no per-member key, since the alias index says
    /// which column it is. `None` (every column) stays on `values`, which does
    /// need the key.
    #[test]
    fn columnar_values_render_aliased_columns() {
        let nodes = ReadExpr::Nodes {
            input: Arc::new(ReadExpr::Root {
                path: "g".into(),
                graph_type: None,
            }),
        };

        // One requested column: a single aliased `get`, value and dtype only.
        let one = ReadExpr::CollectionPropertiesValues {
            input: Arc::new(nodes.clone()),
            keys: vec!["score".to_string()].into(),
        };
        let (query, _) = render_read(&one).unwrap();
        assert!(
            query.contains("properties { c0: get(key: \"score\") { value dtype } }"),
            "single column not rendered as an aliased get: {query}"
        );
        assert!(
            !query.contains(" key "),
            "the per-member key should not be requested: {query}"
        );
        assert_eq!(
            query.matches('{').count(),
            query.matches('}').count(),
            "unbalanced braces in: {query}"
        );

        // Two columns: one aliased field each, indices in request order.
        let two = ReadExpr::CollectionPropertiesValues {
            input: Arc::new(nodes.clone()),
            keys: vec!["score".to_string(), "tag".to_string()].into(),
        };
        let (query, _) = render_read(&two).unwrap();
        assert!(
            query.contains(
                "properties { c0: get(key: \"score\") { value dtype } c1: get(key: \"tag\") { value dtype } }"
            ),
            "two columns not rendered as ordered aliases: {query}"
        );
    }

    /// Collection key lookup renders the registry-backed collection field —
    /// one field name, no member selection, flat and nested alike.
    #[test]
    fn columnar_keys_render_registry_field() {
        let nodes = ReadExpr::Nodes {
            input: Arc::new(ReadExpr::Root {
                path: "g".into(),
                graph_type: None,
            }),
        };
        let flat = ReadExpr::CollectionMetadataKeys {
            input: Arc::new(nodes.clone()),
        };
        let (query, _) = render_read(&flat).unwrap();
        assert!(
            query.contains("{ metadataKeys"),
            "registry keys field not rendered: {query}"
        );
        assert!(
            !query.contains("page(limit: 1)"),
            "keys must not fetch a member page: {query}"
        );
        assert_eq!(
            query.matches('{').count(),
            query.matches('}').count(),
            "unbalanced braces in: {query}"
        );

        let nested = ReadExpr::NestedPropertiesKeys {
            input: Arc::new(nodes),
        };
        let (query, _) = render_read(&nested).unwrap();
        assert!(
            query.contains("{ propertyKeys"),
            "registry keys field not rendered: {query}"
        );
        assert_eq!(
            query.matches('{').count(),
            query.matches('}').count(),
            "unbalanced braces in: {query}"
        );
    }

    // ============ Unit tests for sort-by rendering ============

    /// A `NodeSortBy` with no key set — a base for `..` struct update so each
    /// test names only the key it exercises.
    fn no_node_key() -> NodeSortBy {
        NodeSortBy {
            reverse: None,
            id: None,
            name: None,
            type_: None,
            time: None,
            property: None,
        }
    }

    fn no_edge_key() -> EdgeSortBy {
        EdgeSortBy {
            reverse: None,
            src: None,
            dst: None,
            neighbour: None,
            time: None,
            property: None,
        }
    }

    #[test]
    fn render_node_sort_bys_covers_every_key() {
        let out = render_node_sort_bys(&[
            NodeSortBy {
                reverse: Some(true),
                id: Some(true),
                ..no_node_key()
            },
            NodeSortBy {
                name: Some(true),
                ..no_node_key()
            },
            // Rust `type_` must render as the GraphQL field `type`.
            NodeSortBy {
                type_: Some(true),
                ..no_node_key()
            },
            NodeSortBy {
                time: Some(SortByTime::Latest),
                ..no_node_key()
            },
            NodeSortBy {
                property: Some("score".into()),
                ..no_node_key()
            },
        ]);
        assert_eq!(
            out,
            r#"[{reverse: true, id: true}, {name: true}, {type: true}, {time: LATEST}, {property: "score"}]"#
        );
        assert_eq!(render_node_sort_bys(&[]), "[]");
    }

    #[test]
    fn render_edge_sort_bys_nests_node_keys() {
        let out = render_edge_sort_bys(&[
            EdgeSortBy {
                src: Some(NodeSortBy {
                    type_: Some(true),
                    ..no_node_key()
                }),
                ..no_edge_key()
            },
            EdgeSortBy {
                dst: Some(NodeSortBy {
                    reverse: Some(true),
                    id: Some(true),
                    ..no_node_key()
                }),
                ..no_edge_key()
            },
            EdgeSortBy {
                neighbour: Some(NodeSortBy {
                    name: Some(true),
                    ..no_node_key()
                }),
                ..no_edge_key()
            },
            // Top-level `reverse` belongs to the time/property keys only.
            EdgeSortBy {
                reverse: Some(true),
                time: Some(SortByTime::Earliest),
                ..no_edge_key()
            },
        ]);
        assert_eq!(
            out,
            r#"[{src: {type: true}}, {dst: {reverse: true, id: true}}, {neighbour: {name: true}}, {reverse: true, time: EARLIEST}]"#
        );
    }

    #[test]
    fn sorted_edges_by_neighbour_renders_into_query() {
        let expr = ReadExpr::SortedEdges {
            input: Arc::new(ReadExpr::Edges {
                input: Arc::new(ReadExpr::Root {
                    path: "g".into(),
                    graph_type: None,
                }),
            }),
            sort_bys: vec![EdgeSortBy {
                neighbour: Some(NodeSortBy {
                    property: Some("score".into()),
                    ..no_node_key()
                }),
                ..no_edge_key()
            }],
        };
        let (query, _vars) = render_read(&expr).unwrap();
        assert!(
            query.contains(r#"sorted(sortBys: [{neighbour: {property: "score"}}])"#),
            "got: {query}"
        );
        // The nested object must not unbalance the surrounding selection set.
        assert_eq!(
            query.matches('{').count(),
            query.matches('}').count(),
            "unbalanced braces in: {query}"
        );
    }

    #[test]
    fn graph_type_parses_and_renders_as_a_gql_enum_literal() {
        // The value is spliced into the query as a bare token, so the typed
        // enum is what keeps an arbitrary string out of the query text.
        assert_eq!(GqlGraphType::Event.as_gql(), "EVENT");
        assert_eq!(GqlGraphType::Persistent.as_gql(), "PERSISTENT");
        assert_eq!(GqlGraphType::from_str("EVENT"), Ok(GqlGraphType::Event));
        assert_eq!(
            GqlGraphType::from_str("PERSISTENT"),
            Ok(GqlGraphType::Persistent)
        );
        // Anything else is rejected at the boundary rather than reaching the
        // server, including a lowercase spelling of a real variant.
        assert!(GqlGraphType::from_str("event").is_err());
        assert!(GqlGraphType::from_str("PERSISTENT) { evil }").is_err());
    }

    // ============ Unit tests for node-id rendering ============

    #[test]
    fn node_ids_keep_their_type_on_the_wire() {
        // The server's `NodeId` scalar is typed: a number selects an
        // integer-indexed graph, a quoted string a string-indexed one. An
        // integer id rendered as `"5"` would silently build the wrong kind of
        // graph, so the two forms must stay distinguishable in both the
        // inline-literal and the JSON-variable paths.
        assert_eq!(render_gql_gid(&GID::U64(5)), "5");
        assert_eq!(render_gql_gid(&GID::Str("5".into())), r#""5""#);
        assert_eq!(gid_var(&GID::U64(5)), json!(5));
        assert_eq!(gid_var(&GID::Str("5".into())), json!("5"));

        // `json!(gid)` would emit the derived, externally tagged serde form
        // (`{"U64":5}`), which the scalar rejects — hence `gid_var`.
        assert_ne!(gid_var(&GID::U64(5)), json!(GID::U64(5)));

        // Lists (`subgraph`, `sharedNeighbours`) render element-wise.
        assert_eq!(
            render_gid_list(&[GID::U64(5), GID::Str("a".into())]),
            r#"5, "a""#
        );
    }

    #[test]
    fn node_ids_are_decoded_back_to_their_type() {
        // The reverse direction: a JSON number decodes to an integer id and a
        // JSON string to a string id, so `.id` reports what the graph holds
        // rather than a stringification of it.
        assert_eq!(gid_prop(&json!(5)).unwrap(), Prop::U64(5));
        assert_eq!(gid_prop(&json!("5")).unwrap(), Prop::Str("5".into()));
        // Negative ids are not representable (`GID::U64`), so they are a
        // protocol error rather than a silent truncation.
        assert!(gid_prop(&json!(-1)).is_err());
        assert!(gid_prop(&json!(null)).is_err());
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
            input: Arc::new(ReadExpr::Root {
                path: "g".into(),
                graph_type: None,
            }),
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
        let filter = GqlFilter::Node(GqlNodeFilter::Property(PropertyFilterNew {
            name: "score".into(),
            where_: PropCondition::Eq(GqlValue::Str("O\"Brien".into())),
        }));
        let mut vars = VarCollector::default();
        let reference = vars.add_filter(&filter).unwrap();
        assert_eq!(reference, "$f0");
        assert_eq!(vars.decls, "$f0: GqlFilter!");
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
        let filter = GqlFilter::Node(GqlNodeFilter::Property(PropertyFilterNew {
            name: "wei\"rd".into(),
            where_: PropCondition::Eq(GqlValue::Str("v".into())),
        }));
        let mut vars = VarCollector::default();
        vars.add_filter(&filter).unwrap();
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
            input: Arc::new(ReadExpr::Filtered {
                input: Arc::new(ReadExpr::Filtered {
                    input: Arc::new(ReadExpr::Nodes {
                        input: Arc::new(ReadExpr::Root {
                            path: "g".into(),
                            graph_type: None,
                        }),
                    }),
                    filter: Arc::new(GqlFilter::Node(prop_filter("inner"))),
                }),
                filter: Arc::new(GqlFilter::Node(prop_filter("outer"))),
            }),
        };

        let (query, vars) = render_read(&expr).unwrap();
        assert!(
            query.contains("$f0: GqlFilter!") && query.contains("$f1: GqlFilter!"),
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
    fn non_finite_write_property_rides_the_special_variant() {
        // NaN has no JSON number form — the write path must ship it in the
        // tagged `f64Special` variant rather than erroring or emitting `null`.
        let props: HashMap<String, Prop> = [("v".to_string(), Prop::F64(f64::NAN))].into();
        let var = properties_var(&props).unwrap();
        assert_eq!(
            serde_json::to_value(&var).unwrap(),
            serde_json::json!([{ "key": "v", "value": { "f64Special": "NAN" } }])
        );
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
            let filter = GqlFilter::Node(GqlNodeFilter::Property(PropertyFilterNew {
                name: "x".into(),
                where_: PropCondition::Eq(bad),
            }));
            let mut vars = VarCollector::default();
            assert!(matches!(
                vars.add_filter(&filter),
                Err(ClientError::InvalidInput(_))
            ));
        }

        // A finite float serializes fine.
        let filter = GqlFilter::Node(GqlNodeFilter::Property(PropertyFilterNew {
            name: "x".into(),
            where_: PropCondition::Eq(GqlValue::F64(1.5)),
        }));
        let mut vars = VarCollector::default();
        assert!(vars.add_filter(&filter).is_ok());
    }

    #[test]
    fn parse_read_walks_to_terminal_value() {
        let expr = ReadExpr::Degree {
            input: Arc::new(ReadExpr::Node {
                input: Arc::new(ReadExpr::Root {
                    path: "g".into(),
                    graph_type: None,
                }),
                id: "ben".into(),
            }),
        };
        let response = HashMap::from([(
            "graph".to_string(),
            serde_json::json!({ "node": { "degree": 42 } }),
        )]);
        let value = parse_read(&expr, response).unwrap();
        assert_eq!(value.as_i64(), Some(42));
    }

    // ============ End-to-end integration: server + client + transport ============

    #[tokio::test]
    async fn test_end_to_end_add_node_and_degree() {
        let tmp_dir = tempdir().unwrap();
        let server = GraphServer::new(tmp_dir.path().to_path_buf(), None, Args::default())
            .await
            .unwrap();
        let running = server.start_with_port(0).await.unwrap();
        let port = running.port();

        let url = Url::parse(&format!("http://localhost:{port}")).unwrap();
        let client = RemoteClient::new(url, None);

        // Create the graph
        client
            .new_graph("test-graph", GqlGraphType::Event)
            .await
            .unwrap();

        let rg = client.remote_graph("test-graph".into());

        // Write path: add_node routes through Transport
        rg.add_node(1i64, "ben", NO_PROPS, None, None)
            .await
            .unwrap();
        rg.add_node(2i64, "hamza", NO_PROPS, None, None)
            .await
            .unwrap();
        rg.add_edge(3i64, "ben", "hamza", NO_PROPS, None)
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
        let tmp_dir = tempdir().unwrap();
        let server = GraphServer::new(tmp_dir.path().to_path_buf(), None, Args::default())
            .await
            .unwrap();
        let running = server.start_with_port(0).await.unwrap();
        let url = Url::parse(&format!("http://localhost:{}", running.port())).unwrap();
        let client = RemoteClient::new(url, None);
        client
            .new_graph("parity-filter", GqlGraphType::Event)
            .await
            .unwrap();
        let rg = client.remote_graph("parity-filter".into());

        for (name, score) in [("a", 10i64), ("b", 20), ("c", 30)] {
            // Local-style property argument: a literal of (&str, i64) pairs,
            // no HashMap<String, Prop> to assemble first.
            rg.add_node(1i64, name, [("score", score)], None, None)
                .await
                .unwrap();
        }
        rg.add_edge(1i64, "a", "b", NO_PROPS, None).await.unwrap();
        rg.add_edge(2i64, "b", "c", NO_PROPS, None).await.unwrap();
        rg.add_edge(3i64, "c", "a", NO_PROPS, None).await.unwrap();

        let score_gt_15 = GqlNodeFilter::Property(PropertyFilterNew {
            name: "score".into(),
            where_: PropCondition::Gt(GqlValue::I64(15)),
        });

        // Membership: filter keeps every node addressable — including `a`,
        // which fails the filter itself.
        let filtered = rg.nodes().filter(score_gt_15.clone()).unwrap();
        let mut ids = filtered.id().await.unwrap();
        ids.sort();
        assert_eq!(
            ids,
            ["a", "b", "c"].map(GID::from),
            "filter must not narrow membership"
        );

        // Handles from collect() must agree with the columnar degree.
        let columnar: Map<GID, i64> = filtered
            .id()
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
        assert_eq!(
            columnar[&GID::from("a")],
            2,
            "a keeps both matching neighbours"
        );
        assert_eq!(
            columnar[&GID::from("b")],
            1,
            "a (score=10) dropped from b's edges"
        );
        assert_eq!(
            columnar[&GID::from("c")],
            1,
            "a (score=10) dropped from c's edges"
        );

        // The filter keeps propagating through traversals on the handle.
        let by_id: Map<GID, _> = filtered
            .collect()
            .await
            .unwrap()
            .into_iter()
            .map(|n| (n.id.clone(), n))
            .collect();
        let b_neighbours = by_id[&GID::from("b")].neighbours().id().await.unwrap();
        assert_eq!(
            b_neighbours,
            ["c"].map(GID::from),
            "b's neighbours under f exclude a"
        );

        // select() narrows membership only — handles see the unfiltered graph.
        // Passed as a composite to pin that kind-typed callers still satisfy
        // the widened `TryInto<GqlFilter>` bound.
        let score_gt_15_composite = CompositeNodeFilter::try_from(score_gt_15.clone()).unwrap();
        let selected = rg.nodes().select(score_gt_15_composite).unwrap();
        let mut selected_ids = selected.id().await.unwrap();
        selected_ids.sort();
        assert_eq!(
            selected_ids,
            ["b", "c"].map(GID::from),
            "select narrows membership"
        );
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
        let b = rg
            .node("b")
            .await
            .unwrap()
            .unwrap()
            .filter(score_gt_15)
            .unwrap();
        let c_handles = b.neighbours().collect().await.unwrap();
        assert_eq!(c_handles.len(), 1);
        assert_eq!(c_handles[0].id, GID::from("c"));
        assert_eq!(
            c_handles[0].degree().await.unwrap(),
            1,
            "filter must survive materialization through node traversals"
        );

        // Cross-entity: edge handles materialized under a node filter replay
        // it via the server's unified `filter` field. b's only surviving edge
        // is b-c, and its src (b) still evaluates under f.
        let nested = rg
            .nodes()
            .filter(GqlNodeFilter::Property(PropertyFilterNew {
                name: "score".into(),
                where_: PropCondition::Gt(GqlValue::I64(15)),
            }))
            .unwrap();
        let rows = nested.edges().collect().await.unwrap();
        let ids_in_order = nested.id().await.unwrap();
        let b_row = &rows[ids_in_order
            .iter()
            .position(|id| id == &GID::from("b"))
            .unwrap()];
        assert_eq!(b_row.len(), 1, "b keeps only the edge to c under f");
        assert_eq!(
            (&b_row[0].src, &b_row[0].dst),
            (&GID::from("b"), &GID::from("c"))
        );
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

    #[tokio::test]
    async fn test_earliest_latest() {
        let tmp_dir = tempdir().unwrap();
        let server = GraphServer::new(tmp_dir.path().to_path_buf(), None, Args::default())
            .await
            .unwrap();
        let running = server.start_with_port(0).await.unwrap();
        let url = Url::parse(&format!("http://localhost:{}", running.port())).unwrap();
        let client = RemoteClient::new(url, None);
        client.new_graph("test", GqlGraphType::Event).await.unwrap();
        let rg = client.remote_graph("test".into());
        rg.add_edge(0, 0, 1, NO_PROPS, None).await.unwrap();

        // edges
        let edges = rg.edges();
        let earliest = edges.earliest_time().await.unwrap();
        assert_eq!(
            earliest
                .into_iter()
                .map(|t| t.map(|t| t.t()))
                .collect::<Vec<_>>(),
            [Some(0)]
        );

        let latest = edges.latest_time().await.unwrap();
        assert_eq!(
            latest
                .into_iter()
                .map(|t| t.map(|t| t.t()))
                .collect::<Vec<_>>(),
            [Some(0)]
        );

        let earliest = edges
            .window(InputTime::Simple(1), InputTime::Simple(2))
            .earliest_time()
            .await
            .unwrap();
        assert_eq!(earliest, [None]);

        let latest = edges
            .window(InputTime::Simple(1), InputTime::Simple(2))
            .latest_time()
            .await
            .unwrap();
        assert_eq!(latest, [None]);

        // nodes
        let nodes = rg.nodes();
        let earliest = nodes.earliest_time().await.unwrap();
        assert_eq!(
            earliest
                .into_iter()
                .map(|t| t.map(|t| t.t()))
                .collect::<Vec<_>>(),
            [Some(0), Some(0)]
        );

        let latest = nodes.latest_time().await.unwrap();
        assert_eq!(
            latest
                .into_iter()
                .map(|t| t.map(|t| t.t()))
                .collect::<Vec<_>>(),
            [Some(0), Some(0)]
        );

        let earliest = nodes
            .window(InputTime::Simple(1), InputTime::Simple(2))
            .earliest_time()
            .await
            .unwrap();
        assert_eq!(earliest, [None, None]);

        let latest = nodes
            .window(InputTime::Simple(1), InputTime::Simple(2))
            .latest_time()
            .await
            .unwrap();
        assert_eq!(latest, [None, None]);

        // path
        let neighbours = rg.nodes().out_neighbours();
        let earliest = neighbours
            .earliest_time()
            .await
            .unwrap()
            .into_iter()
            .flatten()
            .map(|v| v.map(|t| t.t()))
            .collect::<Vec<_>>();
        assert_eq!(earliest, [Some(0)]);

        let latest = neighbours
            .latest_time()
            .await
            .unwrap()
            .into_iter()
            .flatten()
            .map(|v| v.map(|t| t.t()))
            .collect::<Vec<_>>();
        assert_eq!(latest, [Some(0)]);

        let earliest = neighbours
            .window(InputTime::Simple(1), InputTime::Simple(2))
            .earliest_time()
            .await
            .unwrap()
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(earliest, [None]);

        let latest = neighbours
            .window(InputTime::Simple(1), InputTime::Simple(2))
            .latest_time()
            .await
            .unwrap()
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(latest, [None]);

        running.stop().await;
        running.wait().await.unwrap();
    }

    #[tokio::test]
    async fn test_id() {
        let tmp_dir = tempdir().unwrap();
        let server = GraphServer::new(tmp_dir.path().to_path_buf(), None, Args::default())
            .await
            .unwrap();
        let running = server.start_with_port(0).await.unwrap();
        let url = Url::parse(&format!("http://localhost:{}", running.port())).unwrap();
        let client = RemoteClient::new(url, None);
        client.new_graph("test", GqlGraphType::Event).await.unwrap();
        let rg = client.remote_graph("test".into());
        rg.add_edge(0, 0, 1, NO_PROPS, None).await.unwrap();

        // edges
        let edges = rg.edges();
        let id = edges.id().await.unwrap();
        assert_eq!(id, [(GID::U64(0), GID::U64(1))]);

        let neighours = rg.nodes().out_neighbours().id().await.unwrap();
        assert_eq!(
            neighours.into_iter().flatten().collect::<Vec<_>>(),
            [GID::U64(1)]
        );

        running.stop().await;
        running.wait().await.unwrap();
    }

    fn as_mapped_result<K: Clone + Hash + Eq, V>(ids: &[K], values: Vec<V>) -> HashMap<K, V> {
        ids.iter().cloned().zip(values).collect()
    }

    #[tokio::test]
    async fn test_properties() {
        let tmp_dir = tempdir().unwrap();
        let server = GraphServer::new(tmp_dir.path().to_path_buf(), None, Args::default())
            .await
            .unwrap();
        let running = server.start_with_port(0).await.unwrap();
        let url = Url::parse(&format!("http://localhost:{}", running.port())).unwrap();
        let client = RemoteClient::new(url, None);
        client.new_graph("test", GqlGraphType::Event).await.unwrap();
        let rg = client.remote_graph("test".into());
        rg.add_node(0, 0, [("test", Prop::I64(1))], None, None)
            .await
            .unwrap()
            .add_metadata([("meta", Prop::Bool(false))])
            .await
            .unwrap();
        rg.add_node(1, 1, [("test2", Prop::str("hi"))], None, None)
            .await
            .unwrap();
        rg.add_edge(0, 0, 0, [("test", Prop::I64(2))], None)
            .await
            .unwrap()
            .add_metadata([("meta", Prop::Bool(false))], None)
            .await
            .unwrap();

        let remote_node = rg.node(0).await.unwrap().unwrap();

        let dtype = remote_node
            .properties()
            .get_dtype_of("test")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(dtype, PropType::I64);

        assert!(remote_node
            .properties()
            .get_dtype_of("bla")
            .await
            .unwrap()
            .is_none());

        let prop_values = remote_node.properties().values(None).await.unwrap();
        assert_eq!(prop_values, [Prop::I64(1)]);

        let prop = remote_node.properties().get("test").await.unwrap().unwrap();
        assert_eq!(prop, Prop::I64(1));
        let keys = remote_node.properties().keys().await.unwrap();
        assert_eq!(keys, ["test"]);
        assert!(remote_node.properties().contains("test").await.unwrap());

        let meta = remote_node.metadata().get("meta").await.unwrap().unwrap();
        assert_eq!(meta, Prop::Bool(false));
        assert!(remote_node.metadata().contains("meta").await.unwrap());
        let keys = remote_node.metadata().keys().await.unwrap();
        assert_eq!(keys, ["meta"]);
        let values = remote_node.metadata().values(None).await.unwrap();
        assert_eq!(values, [Prop::Bool(false)]);
        let items = remote_node.metadata().items(None).await.unwrap();
        assert_eq!(items, [("meta".to_string(), Prop::Bool(false))]);

        let prop_items = remote_node.properties().items(None).await.unwrap();
        assert_eq!(prop_items, [("test".to_string(), Prop::I64(1))]);

        let tprops = remote_node
            .properties()
            .temporal()
            .values(None)
            .await
            .unwrap();

        assert_eq!(tprops.len(), 1);
        let tprop = tprops.into_iter().next().unwrap();
        let t_values = tprop.values().await.unwrap();
        assert_eq!(t_values, [Prop::I64(1)]);
        assert_eq!(
            tprop.history().collect().await.unwrap(),
            [EventTime::start(0)]
        );
        assert_eq!(tprop.at(1).await.unwrap(), Some(Prop::I64(1)));
        assert_eq!(tprop.at(-1).await.unwrap(), None);
        assert_eq!(tprop.latest().await.unwrap(), Some(Prop::I64(1)));
        assert_eq!(tprop.count().await.unwrap(), 1);
        assert_eq!(tprop.unique().await.unwrap(), [Prop::I64(1)]);
        assert_eq!(
            tprop.ordered_dedupe(true).await.unwrap(),
            [RemotePropertyTuple {
                time: EventTime::start(0),
                value: Prop::I64(1)
            }]
        );
        assert_eq!(tprop.sum().await.unwrap(), Some(Prop::I64(1)));
        assert_eq!(tprop.mean().await.unwrap(), Some(1.0));
        assert_eq!(tprop.average().await.unwrap(), Some(1.0));
        assert_eq!(
            tprop.min().await.unwrap(),
            Some(RemotePropertyTuple {
                time: EventTime::start(0),
                value: Prop::I64(1)
            })
        );
        assert_eq!(
            tprop.max().await.unwrap(),
            Some(RemotePropertyTuple {
                time: EventTime::start(0),
                value: Prop::I64(1)
            })
        );
        assert_eq!(
            tprop.median().await.unwrap(),
            Some(RemotePropertyTuple {
                time: EventTime::start(0),
                value: Prop::I64(1)
            })
        );

        let ids = rg.nodes().id().await.unwrap();
        let vals = rg.nodes().properties().get("test").await.unwrap().unwrap();
        match vals {
            Column::Flat(v) => {
                let res: HashMap<_, _> = ids.iter().cloned().zip(v).collect();
                assert_eq!(
                    res,
                    HashMap::from([(GID::U64(0), Some(Prop::I64(1))), (GID::U64(1), None)])
                );
            }
            Column::Nested(_) => {
                panic!("not nested")
            }
        }

        let vals = rg.nodes().metadata().get("meta").await.unwrap().unwrap();
        match vals {
            Column::Flat(v) => {
                let res: HashMap<_, _> = ids.iter().cloned().zip(v).collect();
                assert_eq!(
                    res,
                    HashMap::from([(GID::U64(0), Some(Prop::Bool(false))), (GID::U64(1), None)])
                );
            }
            Column::Nested(_) => {
                panic!("not nested")
            }
        }

        let out_neighoburs = rg.nodes().out_neighbours();

        let vals = out_neighoburs
            .properties()
            .get("test")
            .await
            .unwrap()
            .unwrap();
        match vals {
            Column::Nested(v) => {
                let res = as_mapped_result(&ids, v);
                assert_eq!(
                    res,
                    HashMap::from([
                        (GID::U64(0), vec![Some(Prop::I64(1))]),
                        (GID::U64(1), vec![])
                    ])
                );
            }
            Column::Flat(_) => {
                panic!("not flat")
            }
        }

        let res = as_mapped_result(&ids, out_neighoburs.edge_history_count().await.unwrap());
        assert_eq!(
            res,
            HashMap::from([(GID::U64(0), vec![1]), (GID::U64(1), vec![])])
        );

        let degree = as_mapped_result(&ids, out_neighoburs.degree().await.unwrap());
        assert_eq!(
            degree,
            HashMap::from([(GID::U64(0), vec![1]), (GID::U64(1), vec![])])
        );

        let in_degree = as_mapped_result(&ids, out_neighoburs.in_degree().await.unwrap());
        assert_eq!(
            in_degree,
            HashMap::from([(GID::U64(0), vec![1]), (GID::U64(1), vec![])])
        );

        let out_degree = as_mapped_result(&ids, out_neighoburs.out_degree().await.unwrap());
        assert_eq!(
            out_degree,
            HashMap::from([(GID::U64(0), vec![1]), (GID::U64(1), vec![])])
        );

        let name = as_mapped_result(&ids, out_neighoburs.name().await.unwrap());
        assert_eq!(
            name,
            HashMap::from([(GID::U64(0), vec!["0".to_string()]), (GID::U64(1), vec![])])
        );

        let edges = rg.edges();

        let edge_ids = edges.id().await.unwrap();
        assert_eq!(edge_ids, [(GID::U64(0), GID::U64(0))]);
        let layer_names = edges.layer_names().await.unwrap();
        assert_eq!(layer_names, [["_default"]]);
        let is_active = edges.is_active().await.unwrap();
        assert_eq!(is_active, [true]);
        let is_valid = edges.is_valid().await.unwrap();
        assert_eq!(is_valid, [true]);
        let is_deleted = edges.is_deleted().await.unwrap();
        assert_eq!(is_deleted, [false]);
        let is_self_loop = edges.is_self_loop().await.unwrap();
        assert_eq!(is_self_loop, [true]);

        let nested_edges = rg.nodes().out_edges();
        let edge_ids = as_mapped_result(&ids, nested_edges.id().await.unwrap());
        assert_eq!(
            edge_ids,
            HashMap::from([
                (GID::U64(0), vec![(GID::U64(0), GID::U64(0))]),
                (GID::U64(1), vec![])
            ])
        );

        let layer_names = as_mapped_result(&ids, nested_edges.layer_names().await.unwrap());
        assert_eq!(
            layer_names,
            HashMap::from([
                (GID::U64(0), vec![vec!["_default".to_string()]]),
                (GID::U64(1), vec![])
            ])
        );

        let layer_names = as_mapped_result(
            &ids,
            nested_edges.explode_layers().layer_name().await.unwrap(),
        );
        assert_eq!(
            layer_names,
            HashMap::from([
                (GID::U64(0), vec!["_default".to_string()]),
                (GID::U64(1), vec![])
            ])
        );
        let is_active = as_mapped_result(&ids, nested_edges.is_active().await.unwrap());
        assert_eq!(
            is_active,
            HashMap::from([(GID::U64(0), vec![true]), (GID::U64(1), vec![])])
        );

        let is_valid = as_mapped_result(&ids, nested_edges.is_valid().await.unwrap());
        assert_eq!(
            is_valid,
            HashMap::from([(GID::U64(0), vec![true]), (GID::U64(1), vec![])])
        );

        let is_deleted = as_mapped_result(&ids, nested_edges.is_deleted().await.unwrap());
        assert_eq!(
            is_deleted,
            HashMap::from([(GID::U64(0), vec![false]), (GID::U64(1), vec![])])
        );

        let is_self_loop = as_mapped_result(&ids, nested_edges.is_self_loop().await.unwrap());
        assert_eq!(
            is_self_loop,
            HashMap::from([(GID::U64(0), vec![true]), (GID::U64(1), vec![])])
        );

        running.stop().await;
        running.wait().await.unwrap();
    }

    #[tokio::test]
    async fn test_remote_graph() {
        let tmp_dir = tempdir().unwrap();
        let server = GraphServer::new(tmp_dir.path().to_path_buf(), None, Args::default())
            .await
            .unwrap();
        let running = server.start_with_port(0).await.unwrap();
        let url = Url::parse(&format!("http://localhost:{}", running.port())).unwrap();
        let client = RemoteClient::new(url, None);
        client.new_graph("test", GqlGraphType::Event).await.unwrap();
        let rg = client.remote_graph("test".into());
        rg.add_node(0, 0, [("test", Prop::I64(1))], None, None)
            .await
            .unwrap()
            .add_metadata([("meta", Prop::Bool(false))])
            .await
            .unwrap();
        rg.add_edge(0, 0, 1, [("test", Prop::I64(2))], None)
            .await
            .unwrap()
            .add_metadata([("meta", Prop::Bool(false))], None)
            .await
            .unwrap();
        rg.add_edge(0, 0, 2, NO_PROPS, None).await.unwrap();

        let schema = rg.schema().await.unwrap();
        assert_eq!(
            schema,
            RemoteGraphSchema {
                nodes: vec![RemoteNodeSchema {
                    type_name: "None".to_string(),
                    properties: vec![RemotePropertySchema {
                        key: "test".to_string(),
                        property_type: PropType::I64,
                        variants: vec!["1".to_string()],
                    }],
                    metadata: vec![RemotePropertySchema {
                        key: "meta".to_string(),
                        property_type: PropType::Bool,
                        variants: vec!["false".to_string()],
                    }],
                }],
                layers: vec![RemoteLayerSchema {
                    name: "_default".to_string(),
                    edges: vec![RemoteEdgeSchema {
                        src_type: "None".to_string(),
                        dst_type: "None".to_string(),
                        properties: vec![RemotePropertySchema {
                            key: "test".to_string(),
                            property_type: PropType::I64,
                            variants: vec!["2".to_string()],
                        }],
                        metadata: vec![RemotePropertySchema {
                            key: "meta".to_string(),
                            property_type: PropType::Bool,
                            variants: vec!["false".to_string()],
                        }],
                    }],
                }],
            }
        );

        let shared: Vec<_> = rg
            .shared_neighbours(vec![1.into(), 2.into()])
            .await
            .unwrap()
            .into_iter()
            .map(|v| v.id)
            .collect();
        assert_eq!(shared, [0]);
        running.stop().await;
        running.wait().await.unwrap();
    }

    /// End-to-end parity: `explode().collect()` handles must be pinned to
    /// their event — `.time()` / `.layer_name()` / properties answer like
    /// local exploded edges — and `explode_layers().collect()` handles are
    /// pinned to their layer (`.layer_name()` resolves, `.time()` unavailable).
    #[tokio::test]
    async fn test_exploded_collect_pins_events() {
        let tmp_dir = tempdir().unwrap();
        let server = GraphServer::new(tmp_dir.path().to_path_buf(), None, Args::default())
            .await
            .unwrap();
        let running = server.start_with_port(0).await.unwrap();
        let url = Url::parse(&format!("http://localhost:{}", running.port())).unwrap();
        let client = RemoteClient::new(url, None);
        client
            .new_graph("parity-explode", GqlGraphType::Event)
            .await
            .unwrap();
        let rg = client.remote_graph("parity-explode".into());

        for (t, w) in [(1i64, 1i64), (5, 2)] {
            rg.add_edge(t, "x", "y", [("weight", w)], None)
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
            .map(|t| t.unwrap().t())
            .collect();
        assert_eq!(times, [1, 5]);

        // Handles are pinned: same times, per-event property values, and a
        // working layer_name — none of which a whole-edge handle can answer.
        let handles = exploded.collect().await.unwrap();
        assert_eq!(handles.len(), 2);
        for (handle, (expect_t, expect_w)) in handles.iter().zip([(1i64, 1i64), (5, 2)]) {
            let t = handle.time().await.unwrap().unwrap().t();
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
        assert_eq!(single[1].time().await.unwrap().unwrap().t(), 5);

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
