//! The execution plan: a tree of pre-resolved [`Op`]s.
//!
//! A GraphQL request is a *tree*, not a line — a field's selection set can have
//! several children, each run against the same receiver (e.g. `after { history …
//! neighbours … }`). So the plan is a tree of `Op`s walked depth-first by
//! [`exec`](super::exec), with the `Vec<Value>` stack as the environment.
//!
//! Everything here is resolved **once**, at plan time: field names become typed
//! [`Nav`] / [`IterKind`] / [`LeafKind`] enum variants (dispatch is a `match`,
//! i.e. a jump table — no string compares), and arguments are parsed into typed
//! values (`after(time: 500)` → an [`EventTime`]). Execution does zero string
//! lookups and zero argument decoding.

use crate::model::graph::node_id::GqlNodeId;
use raphtory_api::core::storage::timeindex::EventTime;

/// A compiled query: the selection set under the root `graph(path:)` field
/// (which is resolved/loaded asynchronously before execution), plus the response
/// key to emit it under (normally `"graph"`).
pub struct Plan {
    pub root_key: Box<str>,
    pub children: Box<[Op]>,
}

/// One node in the plan tree. Each carries its **response key** (the output JSON
/// key — alias or field name) and what to do.
pub enum Op {
    /// Produce one new receiver from the top of the stack, push it, run
    /// `children` as a JSON object, then pop. `nullable` fields that resolve to
    /// nothing (e.g. a missing `node`) emit `null` and skip their children.
    Navigate {
        key: Box<str>,
        nav: Nav,
        nullable: bool,
        children: Box<[Op]>,
    },
    /// Take an iterable receiver from the top of the stack and emit a JSON
    /// array; for each item push it, run `children` as an object, pop.
    List {
        key: Box<str>,
        iter: IterKind,
        children: Box<[Op]>,
    },
    /// Read a scalar from the top-of-stack receiver and write it to the sink.
    ///
    /// (Every leaf in the POC subset reads the *current* receiver; the design
    /// leaves room for an explicit ancestor stack-slot later.)
    Leaf { key: Box<str>, leaf: LeafKind },
}

/// A navigation step that turns one receiver into another (single) receiver.
/// One variant per supported field; arguments are pre-parsed into typed values.
pub enum Nav {
    /// `graph.node(name:)` — `Graph` → `Node?`
    Node(GqlNodeId),
    /// `node.history` — `Node` → `History`
    History,
    /// `node.after(time:)` — `Node` → `Node`
    After(EventTime),
    /// `node.before(time:)` — `Node` → `Node`
    Before(EventTime),
    /// `node.window(start:, end:)` — `Node` → `Node`
    Window { start: EventTime, end: EventTime },
}

/// An iteration step that turns a receiver into a sequence of item receivers.
pub enum IterKind {
    /// `history.list` — iterate a `History`, item per `EventTime`.
    HistoryList,
}

/// A scalar leaf read from the current receiver.
pub enum LeafKind {
    /// `eventTime.timestamp` — `Int`
    Timestamp,
    /// `eventTime.eventId` — `Int`
    EventId,
    /// `node.name` — `String`
    Name,
}
