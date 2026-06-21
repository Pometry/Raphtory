//! The runtime [`Value`] pushed on the execution stack.
//!
//! `Value` is an `enum`, not a `Box<dyn …>`, so it lives on the stack with no
//! per-value heap allocation. Its heavyweight variants wrap Arc-backed Raphtory
//! handles (the *same* handles the `async-graphql` resolvers use), so moving a
//! `Value` between stack slots is a pointer copy / refcount bump, never a data
//! copy — and output parity with the existing engine is structural.

use crate::model::graph::{
    history::GqlHistory, node::GqlNode, nodes::GqlNodes, path_from_node::GqlPathFromNode,
};
use raphtory::db::api::view::DynamicGraph;
use raphtory_api::core::storage::timeindex::EventTime;

/// A value produced during execution and held on the stack.
pub enum Value {
    /// A graph view — the pre-loaded root, or a derived view (`window`, …).
    Graph(DynamicGraph),
    /// A node collection (`graph.nodes`).
    Nodes(GqlNodes),
    /// A path of nodes reachable from a node (`node.neighbours`).
    Path(GqlPathFromNode),
    /// A node view (`node(name:)`, `after(time:)`, a neighbour, …).
    Node(GqlNode),
    /// A history handle (`node.history`).
    History(GqlHistory),
    /// A single history entry — the item produced while iterating `history.list`.
    EventTime(EventTime),
}
