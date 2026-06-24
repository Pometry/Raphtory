//! The runtime [`Value`] pushed on the execution stack.
//!
//! `Value` is an `enum`, not a `Box<dyn …>`, so it lives on the stack with no
//! per-value heap allocation. Its heavyweight variants wrap Arc-backed Raphtory
//! handles (the *same* handles the `async-graphql` resolvers use), so moving a
//! `Value` between stack slots is a pointer copy / refcount bump, never a data
//! copy — and output parity with the existing engine is structural.

use crate::model::graph::{
    edge::GqlEdge,
    edges::GqlEdges,
    history::{GqlHistory, GqlHistoryDateTime, GqlHistoryEventId, GqlHistoryTimestamp},
    node::GqlNode,
    nodes::GqlNodes,
    path_from_node::GqlPathFromNode,
    property::{GqlMetadata, GqlProperties, GqlProperty, GqlTemporalProperties, GqlTemporalProperty},
    timeindex::GqlEventTime,
};
use raphtory::db::api::view::DynamicGraph;

/// A value produced during execution and held on the stack.
pub enum Value {
    /// A graph view — the pre-loaded root, or a derived view (`window`, `layer`, …).
    Graph(DynamicGraph),
    /// A node collection (`graph.nodes`).
    Nodes(GqlNodes),
    /// A path of nodes reachable from a node (`node.neighbours`).
    Path(GqlPathFromNode),
    /// A node view (`node(name:)`, `after(time:)`, a neighbour, `edge.src`, …).
    Node(GqlNode),
    /// An edge collection (`graph.edges`).
    Edges(GqlEdges),
    /// An edge view (`graph.edge(src:, dst:)`).
    Edge(GqlEdge),
    /// A history handle (`node.history` / `edge.history`).
    History(GqlHistory),
    /// A point in time as a (nullable) `EventTime` object — a `history.list`
    /// item, or a time field like `earliestTime` / `start` / edge `time`. Wraps
    /// `Option<EventTime>` so the `timestamp` / `eventId` / `datetime` leaves can
    /// emit `null` when the time is absent.
    EventTime(GqlEventTime),
    /// The timestamp projection of a history (`history.timestamps`).
    HistoryTimestamp(GqlHistoryTimestamp),
    /// The event-id projection of a history (`history.eventId`).
    HistoryEventId(GqlHistoryEventId),
    /// The datetime projection of a history (`history.datetimes`).
    HistoryDateTime(GqlHistoryDateTime),
    /// A property bag (`node.properties` / `edge.properties`).
    Properties(GqlProperties),
    /// The temporal-only view of a property bag (`properties.temporal`).
    TemporalProperties(GqlTemporalProperties),
    /// A metadata bag (`node.metadata` / `edge.metadata`).
    Metadata(GqlMetadata),
    /// A single `{key, value}` reading — item of `properties.values` / `metadata.values`.
    Property(GqlProperty),
    /// A single property timeline — item of `temporal.values`.
    TemporalProperty(GqlTemporalProperty),
}
