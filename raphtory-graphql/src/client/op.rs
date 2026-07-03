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
/// Starting with `AddNode` only. Remaining mutations (`create_node`, `add_edge`,
/// `add_property`, `add_metadata`, `update_metadata`, `delete_edge`, and the
/// per-node/per-edge variants) added incrementally.
pub enum WriteOp {
    AddNode(AddNode),
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
