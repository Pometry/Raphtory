//! Client-side operation types shipped to a `Transport` for execution.
//!
//! Every method on `RemoteGraph`/`RemoteNode`/`RemoteEdge` builds an `Op` and
//! hands it to the transport. This module is the single source of truth for
//! what "an operation" means on the wire.

use raphtory_api::core::entities::properties::prop::Prop;
use std::collections::HashMap;

/// Top-level split between reads (recursive expressions returning values) and
/// writes (self-contained commands with side effects). Matches Ben's V2 doc
/// distinction between `Eval` and `Apply`.
pub enum Op {
    Read(ReadExpr),
    Write(WriteOp),
}

/// Recursive read expression. Composable: every non-terminal variant wraps its
/// input, forming a tree. Terminals (e.g. `Degree`) fire the RPC on the server.
///
/// This is a minimal starting set — enough to demo `g.window(s,e).node(id).degree()`
/// end-to-end. More variants (layer, at, rolling, nodes, edges, properties,
/// history, count_*, ...) added incrementally, driven by real demand.
#[derive(Clone)]
pub enum ReadExpr {
    /// Start of every read tree — names the graph.
    Root { path: String },

    /// Time-window a graph (or nested view). Composes.
    Window {
        input: Box<ReadExpr>,
        start: i64,
        end: i64,
    },

    /// Narrow to a single node by id. Consumes a graph, produces a node.
    Node { input: Box<ReadExpr>, id: String },

    /// Terminal: returns the degree of a node as an `i64`.
    Degree { input: Box<ReadExpr> },
}

/// Write operations. Each variant is a self-contained command with all its
/// arguments upfront — no composition, no wrapping.
///
/// Remaining per-node/per-edge variants added incrementally.
pub enum WriteOp {
    // On the graph
    AddNode(AddNode),
    CreateNode(CreateNode),
    AddEdge(AddEdge),
    AddGraphProperty(AddGraphProperty),
    AddGraphMetadata(AddGraphMetadata),
    UpdateGraphMetadata(UpdateGraphMetadata),
    DeleteEdge(DeleteEdge),
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
