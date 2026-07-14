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
    /// Latest state — no args. Composes.
    Latest { input: Box<ReadExpr> },
    /// Snapshot at the latest time. Composes.
    SnapshotLatest { input: Box<ReadExpr> },
    /// Snapshot at a specific time. Composes.
    SnapshotAt { input: Box<ReadExpr>, time: i64 },
    /// Exclude a specific layer.
    ExcludeLayer { input: Box<ReadExpr>, name: String },
    /// Shrink both start and end of the window.
    ShrinkWindow {
        input: Box<ReadExpr>,
        start: i64,
        end: i64,
    },
    /// Shrink the start of the window.
    ShrinkStart { input: Box<ReadExpr>, start: i64 },
    /// Shrink the end of the window.
    ShrinkEnd { input: Box<ReadExpr>, end: i64 },
    /// Restrict to the "valid" subgraph (event-graph filter). No args. Composes.
    Valid { input: Box<ReadExpr> },
    /// Restrict to the default layer. No args. Composes.
    DefaultLayer { input: Box<ReadExpr> },
    /// Restrict to a specific set of layers.
    Layers {
        input: Box<ReadExpr>,
        names: Vec<String>,
    },
    /// Exclude a specific set of layers.
    ExcludeLayers {
        input: Box<ReadExpr>,
        names: Vec<String>,
    },
    /// Restrict to a subgraph induced by the given node ids.
    Subgraph {
        input: Box<ReadExpr>,
        nodes: Vec<String>,
    },
    /// Restrict to nodes matching one of the given node types.
    SubgraphNodeTypes {
        input: Box<ReadExpr>,
        node_types: Vec<String>,
    },
    /// Exclude the given nodes from the view.
    ExcludeNodes {
        input: Box<ReadExpr>,
        nodes: Vec<String>,
    },
    /// Restrict a `RemoteNodes` collection to members with one of the given
    /// node types. Unlike view ops, this actually filters membership — the
    /// returned collection has fewer members. Server field: `typeFilter`.
    TypeFilter {
        input: Box<ReadExpr>,
        node_types: Vec<String>,
    },

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
    /// Navigate to an edge's "other end" node. Edge → Node.
    /// Context-sensitive: on an out-edge yields the destination; on an
    /// in-edge yields the source. Server field: `nbr`.
    Nbr { input: Box<ReadExpr> },
    /// Navigate to the event history of a node or edge. Node/Edge → History.
    /// Container-selection: the resulting `RemoteHistory` handle exposes
    /// terminals like `.count()`, `.list()`, plus sub-container accessors
    /// (`.timestamps`, `.intervals`, etc.).
    History { input: Box<ReadExpr> },
    /// Navigate to the deletion history of an edge. Edge → History.
    /// Same shape as `History` but reads the `deletions` server field
    /// instead of `history` — deletions are edge-only.
    Deletions { input: Box<ReadExpr> },
    /// Graph → the collection of all nodes in the (view-restricted) graph.
    Nodes { input: Box<ReadExpr> },
    /// Node → the collection of the node's neighbours (both directions).
    Neighbours { input: Box<ReadExpr> },
    /// Node → the collection of the node's in-neighbours.
    InNeighbours { input: Box<ReadExpr> },
    /// Node → the collection of the node's out-neighbours.
    OutNeighbours { input: Box<ReadExpr> },
    /// Graph → the collection of all edges in the (view-restricted) graph.
    Edges { input: Box<ReadExpr> },
    /// Node → the collection of the node's edges (both directions).
    NodeEdges { input: Box<ReadExpr> },
    /// Node → the collection of the node's incoming edges.
    InEdges { input: Box<ReadExpr> },
    /// Node → the collection of the node's outgoing edges.
    OutEdges { input: Box<ReadExpr> },
    /// Node → the collection of nodes reachable *into* this node via incoming
    /// edges (i.e., the node's ancestors in the directed graph). Server
    /// field: `inComponent`.
    InComponent { input: Box<ReadExpr> },
    /// Node → the collection of nodes reachable *out from* this node via
    /// outgoing edges (i.e., the node's descendants). Server field: `outComponent`.
    OutComponent { input: Box<ReadExpr> },

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
    /// Terminal: earliest edge event time under this view — `Option<i64>`. Graph only.
    EarliestEdgeTime { input: Box<ReadExpr> },
    /// Terminal: latest edge event time under this view — `Option<i64>`. Graph only.
    LatestEdgeTime { input: Box<ReadExpr> },
    /// Terminal: first update time on this node — `Option<i64>`. Node only.
    FirstUpdate { input: Box<ReadExpr> },
    /// Terminal: last update time on this node — `Option<i64>`. Node only.
    LastUpdate { input: Box<ReadExpr> },
    /// Terminal: the time an edge event occurred — `Option<i64>`. Edge only.
    /// Server field is `Result<GqlEventTime, GraphError>`; the client treats
    /// server-side errors as `ClientError::GraphQLErrors`.
    Time { input: Box<ReadExpr> },

    // ============ Graph scalar terminals ============
    /// Terminal: check if a node with `id` exists in the view — `bool`.
    HasNode { input: Box<ReadExpr>, id: String },
    /// Terminal: check if an edge with `(src, dst)` exists in the view — `bool`.
    HasEdge {
        input: Box<ReadExpr>,
        src: String,
        dst: String,
    },
    /// Terminal: total count of temporal edges (edge updates) — `i64`.
    CountTemporalEdges { input: Box<ReadExpr> },
    /// Terminal: graph path — `String`.
    Path { input: Box<ReadExpr> },
    /// Terminal: parent namespace of the graph path — `String`.
    Namespace { input: Box<ReadExpr> },
    /// Terminal: graph creation timestamp — `i64` (metadata, always set).
    Created { input: Box<ReadExpr> },
    /// Terminal: graph last-opened timestamp — `i64` (metadata, always set).
    LastOpened { input: Box<ReadExpr> },
    /// Terminal: graph last-updated timestamp — `i64` (metadata, always set).
    LastUpdated { input: Box<ReadExpr> },
    /// Terminal: layer names present in this graph — `Vec<String>`.
    UniqueLayers { input: Box<ReadExpr> },

    // ============ Collection terminals (on Nodes/Edges collections) ============
    /// Terminal on a Nodes collection: list of member ids — `Vec<String>`.
    Ids { input: Box<ReadExpr> },
    /// Terminal on a collection: number of members — `i64`.
    /// Distinct from `CountNodes`/`CountEdges` (which are Graph-scope); this
    /// fires against the collection's `count` field. Also polymorphic on
    /// `RemoteHistory` — same server field name (`count`).
    Count { input: Box<ReadExpr> },
    /// Terminal on a `RemoteHistory` container: whether the history is empty
    /// — `bool`. Server field name is `isEmpty`.
    IsEmpty { input: Box<ReadExpr> },
    /// Terminal on a `RemoteHistory` container: list all events in ascending
    /// order — `Vec<RemoteEventTime>`. Server field is `list`; queries the
    /// compound sub-fields `timestamp`, `dt`, `eventId` per record.
    HistoryList { input: Box<ReadExpr> },
    /// Terminal on a `RemoteHistory` container: list all events in descending
    /// order — `Vec<RemoteEventTime>`. Server field is `listRev`.
    HistoryListRev { input: Box<ReadExpr> },
    /// Terminal on a `RemoteHistory` container: paginated list of events in
    /// ascending order — `Vec<RemoteEventTime>`. `offset` and `page_index`
    /// are optional; each defaults to 0 server-side.
    HistoryPage {
        input: Box<ReadExpr>,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    },
    /// Terminal on a `RemoteHistory` container: paginated list of events in
    /// descending order — `Vec<RemoteEventTime>`. Same args as `HistoryPage`.
    HistoryPageRev {
        input: Box<ReadExpr>,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    },

    // ============ RemoteHistory sub-container selection ============
    /// Navigate to the timestamps view of a history. History → HistoryTimestamps.
    /// Server field: `timestamps`.
    HistoryTimestamps { input: Box<ReadExpr> },
    /// Navigate to the event-id view of a history. History → HistoryEventIds.
    /// Server field: `eventId`.
    HistoryEventIds { input: Box<ReadExpr> },
    /// Navigate to the datetime view of a history. History → HistoryDateTimes.
    /// Server field: `datetimes` (no format arg — server default RFC 3339).
    HistoryDateTimes { input: Box<ReadExpr> },
    /// Navigate to the intervals view of a history — inter-event gaps.
    /// History → HistoryIntervals. Server field: `intervals`.
    HistoryIntervals { input: Box<ReadExpr> },

    // ============ Sub-container list/page terminals (polymorphic) ============
    // These four variants render as `list` / `listRev` / `page(...)` /
    // `pageRev(...)` on the underlying sub-container. Return type is
    // determined by `parse_read` based on the parent selection variant:
    // int list for Timestamps/EventIds/Intervals, string list for DateTimes.
    /// Terminal on any sub-container: list in ascending order.
    SubList { input: Box<ReadExpr> },
    /// Terminal on any sub-container: list in descending order.
    SubListRev { input: Box<ReadExpr> },
    /// Terminal on any sub-container: paginated ascending list.
    SubPage {
        input: Box<ReadExpr>,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    },
    /// Terminal on any sub-container: paginated descending list.
    SubPageRev {
        input: Box<ReadExpr>,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    },

    // ============ Intervals scalar stats ============
    /// Terminal on `HistoryIntervals`: mean of inter-event gaps. `Option<f64>`.
    IntervalsMean { input: Box<ReadExpr> },
    /// Terminal on `HistoryIntervals`: median of inter-event gaps. `Option<i64>`.
    IntervalsMedian { input: Box<ReadExpr> },
    /// Terminal on `HistoryIntervals`: max inter-event gap. `Option<i64>`.
    IntervalsMax { input: Box<ReadExpr> },
    /// Terminal on `HistoryIntervals`: min inter-event gap. `Option<i64>`.
    IntervalsMin { input: Box<ReadExpr> },
    /// Terminal on an Edges collection: list of (src, dst) pairs.
    /// Returned as `Prop::List(Prop::List(Prop::Str, Prop::Str), ...)` on the
    /// wire — each outer element is a 2-element inner list `[src, dst]`.
    /// Distinct from `Ids` (nodes) because edges have no single-string id;
    /// they're identified by the pair.
    EdgesList { input: Box<ReadExpr> },

    // ============ Node scalar terminals ============
    /// Terminal: node id — `String` (server may return int-like GID; treated as string).
    Id { input: Box<ReadExpr> },
    /// Terminal: node type — `Option<String>` (null if not set).
    NodeType { input: Box<ReadExpr> },
    /// Terminal: whether the node has any events in the current view — `bool`.
    /// Also polymorphic on Edge — same server field name.
    IsActive { input: Box<ReadExpr> },
    /// Terminal: count of temporal edge events on this node — `i64`.
    EdgeHistoryCount { input: Box<ReadExpr> },

    // ============ Edge scalar terminals ============
    /// Terminal: edge id — pair of endpoint ids as `Vec<String>` of length 2.
    /// Distinct from Node's `Id` (single string): server field is the same
    /// name (`id`) but returns `Vec<GqlNodeId>` for edges.
    EdgeIdPair { input: Box<ReadExpr> },
    /// Terminal: layer names the edge is present in — `Vec<String>`.
    LayerNames { input: Box<ReadExpr> },
    /// Terminal: single layer name for a layer-restricted edge view — `String`.
    /// Server field is `Result<String, GraphError>`; server-side error surfaces
    /// as `ClientError::GraphQLErrors`.
    LayerName { input: Box<ReadExpr> },
    /// Terminal: whether the edge is valid at the current time — `bool`.
    IsValid { input: Box<ReadExpr> },
    /// Terminal: whether the edge has been deleted at the current time — `bool`.
    IsDeleted { input: Box<ReadExpr> },
    /// Terminal: whether the edge's `src == dst` — `bool`.
    IsSelfLoop { input: Box<ReadExpr> },
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
