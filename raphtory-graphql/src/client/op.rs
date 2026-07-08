//! Client-side operation types shipped to a `Transport` for execution.
//!
//! Every method on `RemoteGraph`/`RemoteNode`/`RemoteEdge` builds an `Op` and
//! hands it to the transport. This module is the single source of truth for
//! what "an operation" means on the wire.

use crate::client::inner_collection;
use raphtory_api::core::entities::properties::prop::Prop;
use serde::{ser::SerializeStruct, Serialize, Serializer};
use serde_json::json;
use std::collections::HashMap;

/// Top-level split between reads (recursive expressions returning values) and
/// writes (self-contained commands with side effects). Matches Ben's V2 doc
/// distinction between `Eval` and `Apply`.
pub enum Op {
    Read(ReadExpr),
    Write(WriteOp),
}

/// Recursive read expression. Composable: every non-terminal variant wraps its
/// input, forming a tree. Terminals (e.g. `Degree`, `CountNodes`, `Name`) fire
/// the RPC on the server.
///
/// New variants land as demand arises. Structural pattern per variant:
/// - `render_read_body` case in `graphql_transport.rs` — emit the GraphQL fragment
/// - `read_depth` case — count how many `{` this variant opens (usually 1)
/// - `build_json_path` case — push the JSON key(s) that navigate to this level
/// - For terminals only: `parse_read` case to unwrap the JSON value into a `Prop`
#[derive(Clone)]
pub enum ReadExpr {
    /// Start of every read tree — names the graph.
    Root { path: String },

    // ============ View chaining (Graph → Graph) ============
    /// Time-window a graph. Composes.
    Window {
        input: Box<ReadExpr>,
        start: i64,
        end: i64,
    },
    /// Restrict to a single layer.
    Layer { input: Box<ReadExpr>, name: String },
    /// Snapshot at a single timestamp.
    At { input: Box<ReadExpr>, time: i64 },
    /// Restrict to events strictly before the given time.
    Before { input: Box<ReadExpr>, time: i64 },
    /// Restrict to events at or after the given time.
    After { input: Box<ReadExpr>, time: i64 },

    // ============ Selection ============
    /// Narrow to a single node by id. Graph → Node.
    Node { input: Box<ReadExpr>, id: String },
    /// Narrow to a single edge by (src, dst). Graph → Edge.
    Edge {
        input: Box<ReadExpr>,
        src: String,
        dst: String,
    },
    /// Navigate to an edge's source node. Edge → Node.
    Src { input: Box<ReadExpr> },
    /// Navigate to an edge's destination node. Edge → Node.
    Dst { input: Box<ReadExpr> },

    // ============ Scalar terminals on Graph ============
    /// Terminal: total node count under the current view — `i64`.
    CountNodes { input: Box<ReadExpr> },
    /// Terminal: total edge count under the current view — `i64`.
    CountEdges { input: Box<ReadExpr> },

    // ============ Scalar terminals on Node ============
    /// Terminal: node degree — `i64`.
    Degree { input: Box<ReadExpr> },
    /// Terminal: in-degree — `i64`.
    InDegree { input: Box<ReadExpr> },
    /// Terminal: out-degree — `i64`.
    OutDegree { input: Box<ReadExpr> },
    /// Terminal: node name — `String`.
    Name { input: Box<ReadExpr> },

    // ============ Compound terminals on Graph or Node → Option<i64> ============
    // Server returns an `EventTime` object; we query `<field> { timestamp }`
    // and unwrap the (possibly-null) `timestamp` field.
    /// Terminal: earliest event time — `Option<i64>`. Works on Graph and Node.
    EarliestTime { input: Box<ReadExpr> },
    /// Terminal: latest event time — `Option<i64>`. Works on Graph and Node.
    LatestTime { input: Box<ReadExpr> },
    /// Terminal: view start bound — `Option<i64>`. Works on Graph and Node.
    Start { input: Box<ReadExpr> },
    /// Terminal: view end bound — `Option<i64>`. Works on Graph and Node.
    End { input: Box<ReadExpr> },
}

/// Write operations. Each variant is a self-contained command with all its
/// arguments upfront — no composition, no wrapping.
pub enum WriteOp {
    // On the graph — single mutations
    AddNode(AddNode),
    CreateNode(CreateNode),
    AddEdge(AddEdge),
    AddGraphProperty(AddGraphProperty),
    AddGraphMetadata(AddGraphMetadata),
    UpdateGraphMetadata(UpdateGraphMetadata),
    DeleteEdge(DeleteEdge),

    // On the graph — batch mutations
    AddNodes(AddNodes),
    AddEdges(AddEdges),

    // On a node
    SetNodeType(SetNodeType),
    AddNodeUpdates(AddNodeUpdates),
    AddNodeMetadata(AddNodeMetadata),
    UpdateNodeMetadata(UpdateNodeMetadata),

    // On an edge (via `RemoteEdge` handle — GraphQL path is
    // `updateGraph.edge(src, dst).xxx`, distinct from graph-scope mutations)
    AddEdgeUpdates(AddEdgeUpdates),
    DeleteEdgeAtTime(DeleteEdgeAtTime),
    AddEdgeMetadata(AddEdgeMetadata),
    UpdateEdgeMetadata(UpdateEdgeMetadata),
}

/// Arguments for `RemoteGraph::add_node`.
pub struct AddNode {
    pub path: String,
    pub time: i64,
    pub id: String,
    pub properties: Option<HashMap<String, Prop>>,
    pub node_type: Option<String>,
    pub layer: Option<String>,
}

/// Arguments for `RemoteGraph::create_node`. Same as `AddNode` minus `layer` —
/// distinct because it maps to the server's `createNode` mutation which fails
/// if the node already exists (vs `addNode` which is upsert-like).
pub struct CreateNode {
    pub path: String,
    pub time: i64,
    pub id: String,
    pub properties: Option<HashMap<String, Prop>>,
    pub node_type: Option<String>,
}

/// Arguments for `RemoteGraph::add_edge`.
pub struct AddEdge {
    pub path: String,
    pub time: i64,
    pub src: String,
    pub dst: String,
    pub properties: Option<HashMap<String, Prop>>,
    pub layer: Option<String>,
}

/// Arguments for `RemoteGraph::add_property` — adds temporal properties on the
/// graph itself (not on a node/edge).
pub struct AddGraphProperty {
    pub path: String,
    pub time: i64,
    pub properties: HashMap<String, Prop>,
}

/// Arguments for `RemoteGraph::add_metadata` — adds (non-temporal) metadata on
/// the graph itself.
pub struct AddGraphMetadata {
    pub path: String,
    pub properties: HashMap<String, Prop>,
}

/// Arguments for `RemoteGraph::update_metadata` — overwrites existing metadata
/// on the graph.
pub struct UpdateGraphMetadata {
    pub path: String,
    pub properties: HashMap<String, Prop>,
}

/// Arguments for `RemoteGraph::delete_edge`. Marks the edge as deleted at the
/// given time (optionally on a specific layer).
pub struct DeleteEdge {
    pub path: String,
    pub time: i64,
    pub src: String,
    pub dst: String,
    pub layer: Option<String>,
}

/// Arguments for `RemoteNode::set_node_type` — sets the node's type (only
/// works if the type has not been previously set).
pub struct SetNodeType {
    pub path: String,
    pub id: String,
    pub new_type: String,
}

/// Arguments for `RemoteNode::add_updates` — adds temporal updates to a node
/// at a specific time.
pub struct AddNodeUpdates {
    pub path: String,
    pub id: String,
    pub time: i64,
    pub properties: Option<HashMap<String, Prop>>,
}

/// Arguments for `RemoteNode::add_metadata` — adds non-temporal metadata to a
/// specific node.
pub struct AddNodeMetadata {
    pub path: String,
    pub id: String,
    pub properties: HashMap<String, Prop>,
}

/// Arguments for `RemoteNode::update_metadata` — overwrites existing metadata
/// on a specific node.
pub struct UpdateNodeMetadata {
    pub path: String,
    pub id: String,
    pub properties: HashMap<String, Prop>,
}

/// Arguments for `RemoteEdge::add_updates` — adds temporal updates to an
/// existing edge at a specific time.
pub struct AddEdgeUpdates {
    pub path: String,
    pub src: String,
    pub dst: String,
    pub time: i64,
    pub properties: Option<HashMap<String, Prop>>,
    pub layer: Option<String>,
}

/// Arguments for `RemoteEdge::delete` — marks an edge as deleted at a specific
/// time. Distinct from graph-scope `DeleteEdge` because it uses the nested
/// `updateGraph.edge(src,dst).delete(time, layer)` mutation.
pub struct DeleteEdgeAtTime {
    pub path: String,
    pub src: String,
    pub dst: String,
    pub time: i64,
    pub layer: Option<String>,
}

/// Arguments for `RemoteEdge::add_metadata` — adds non-temporal metadata to a
/// specific edge (optionally on a specific layer).
pub struct AddEdgeMetadata {
    pub path: String,
    pub src: String,
    pub dst: String,
    pub properties: HashMap<String, Prop>,
    pub layer: Option<String>,
}

/// Arguments for `RemoteEdge::update_metadata` — overwrites existing metadata
/// on a specific edge (optionally on a specific layer).
pub struct UpdateEdgeMetadata {
    pub path: String,
    pub src: String,
    pub dst: String,
    pub properties: HashMap<String, Prop>,
    pub layer: Option<String>,
}

// ============ Batch mutation types ============

/// Arguments for `RemoteGraph::add_nodes` — batch node updates.
pub struct AddNodes {
    pub path: String,
    pub nodes: Vec<NodeAddition>,
}

/// Arguments for `RemoteGraph::add_edges` — batch edge updates.
pub struct AddEdges {
    pub path: String,
    pub edges: Vec<EdgeAddition>,
}

/// One node in a batch add. `metadata` = non-temporal props; `updates` =
/// temporal events attached to the node at specific times.
pub struct NodeAddition {
    pub name: String,
    pub node_type: Option<String>,
    pub metadata: Option<HashMap<String, Prop>>,
    pub updates: Option<Vec<TemporalUpdate>>,
}

/// One edge in a batch add.
pub struct EdgeAddition {
    pub src: String,
    pub dst: String,
    pub layer: Option<String>,
    pub metadata: Option<HashMap<String, Prop>>,
    pub updates: Option<Vec<TemporalUpdate>>,
}

/// A temporal update on a node or edge — property values attached at a
/// specific event time.
pub struct TemporalUpdate {
    pub time: i64,
    pub properties: Option<HashMap<String, Prop>>,
}

// ============ Serialize impls for batch mutation types ============
// These produce the JSON shape the Jinja templates in `graphql_transport.rs`
// expect: `metadata` and `properties` render as `[{ key, value }, ...]` where
// `value` is the pre-baked GraphQL syntax string produced by `inner_collection`
// (e.g. `{ str: "foo" }`, `{ i64: 3 }`).

impl Serialize for TemporalUpdate {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut count = 1;
        if self.properties.is_some() {
            count += 1;
        }
        let mut state = serializer.serialize_struct("TemporalUpdate", count)?;
        state.serialize_field("time", &self.time)?;
        if let Some(ref props) = self.properties {
            let items: Vec<serde_json::Value> = props
                .iter()
                .map(|(k, v)| json!({ "key": k, "value": inner_collection(v) }))
                .collect();
            state.serialize_field("properties", &items)?;
        }
        state.end()
    }
}

impl Serialize for NodeAddition {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut count = 1;
        if self.node_type.is_some() {
            count += 1;
        }
        if self.metadata.is_some() {
            count += 1;
        }
        if self.updates.is_some() {
            count += 1;
        }
        let mut state = serializer.serialize_struct("NodeAddition", count)?;
        state.serialize_field("name", &self.name)?;
        if let Some(ref nt) = self.node_type {
            state.serialize_field("node_type", nt)?;
        }
        if let Some(ref meta) = self.metadata {
            let items: Vec<serde_json::Value> = meta
                .iter()
                .map(|(k, v)| json!({ "key": k, "value": inner_collection(v) }))
                .collect();
            state.serialize_field("metadata", &items)?;
        }
        if let Some(ref updates) = self.updates {
            state.serialize_field("updates", updates)?;
        }
        state.end()
    }
}

impl Serialize for EdgeAddition {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut count = 2;
        if self.layer.is_some() {
            count += 1;
        }
        if self.metadata.is_some() {
            count += 1;
        }
        if self.updates.is_some() {
            count += 1;
        }
        let mut state = serializer.serialize_struct("EdgeAddition", count)?;
        state.serialize_field("src", &self.src)?;
        state.serialize_field("dst", &self.dst)?;
        if let Some(ref layer) = self.layer {
            state.serialize_field("layer", layer)?;
        }
        if let Some(ref meta) = self.metadata {
            let items: Vec<serde_json::Value> = meta
                .iter()
                .map(|(k, v)| json!({ "key": k, "value": inner_collection(v) }))
                .collect();
            state.serialize_field("metadata", &items)?;
        }
        if let Some(ref updates) = self.updates {
            state.serialize_field("updates", updates)?;
        }
        state.end()
    }
}
