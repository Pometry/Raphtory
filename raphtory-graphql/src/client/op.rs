//! Client-side operation types shipped to a `Transport` for execution.
//!
//! Every method on `RemoteGraph`/`RemoteNode`/`RemoteEdge` builds an `Op` and
//! hands it to the transport. This module is the single source of truth for
//! what "an operation" means on the wire.

use crate::{
    client::properties_to_input,
    model::graph::filtering::{GqlEdgeFilter, GqlFilter, GqlNodeFilter},
};
use raphtory_api::core::entities::properties::prop::Prop;
// Re-exported so the client transport wrappers import the op tree's time type
// from one place (`op::InputTime`), same as `ReadExpr`/`WriteOp`.
pub use raphtory_api::core::utils::time::InputTime;
use serde::{ser::SerializeStruct, Serialize, Serializer};
use std::{collections::HashMap, sync::Arc};

// View-op and write times are `InputTime` (`Simple(t)` / `Indexed(t, id)`)
// directly — the Python bindings extract that shape, preserving whether an
// event id was provided (a plain timestamp → `Simple`, a `(t, id)` tuple →
// `Indexed`). Rendering/serializing to the wire lives in the transport, not
// here (see `render_input_time` / `input_time_var` in `graphql_transport.rs`).

/// Build an `InputTime` from a timestamp plus an optional explicit event id.
/// Used by the write path, where `event_id` arrives as a separate kwarg (not a
/// tuple): `Some(id)` locks the secondary index, `None` lets the server
/// auto-increment it.
pub fn input_time_from_parts(timestamp: i64, event_id: Option<usize>) -> InputTime {
    match event_id {
        Some(id) => InputTime::Indexed(timestamp, id),
        None => InputTime::Simple(timestamp),
    }
}

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
#[derive(Clone, Debug)]
pub enum ReadExpr {
    /// Start of every read tree — names the graph.
    Root { path: String },

    // ============ View chaining (Graph → Graph) ============
    /// A composable graph-view operation (window / layer / at / …) applied
    /// to the input. The op itself is data (`ViewOp`); the transport renders
    /// it to the same-named server field. One variant covers the entire
    /// shared view vocabulary — see `ViewOp`.
    View { input: Arc<ReadExpr>, op: ViewOp },
    /// Restrict to the "valid" subgraph (event-graph filter). No args. Composes.
    Valid { input: Arc<ReadExpr> },
    /// Restrict to a subgraph induced by the given node ids.
    Subgraph {
        input: Arc<ReadExpr>,
        nodes: Arc<[String]>,
    },
    /// Restrict to nodes matching one of the given node types.
    SubgraphNodeTypes {
        input: Arc<ReadExpr>,
        node_types: Arc<[String]>,
    },
    /// Exclude the given nodes from the view.
    ExcludeNodes {
        input: Arc<ReadExpr>,
        nodes: Arc<[String]>,
    },
    /// Restrict a `RemoteNodes` collection to members with one of the given
    /// node types. Unlike view ops, this actually filters membership — the
    /// returned collection has fewer members. Server field: `typeFilter`.
    TypeFilter {
        input: Arc<ReadExpr>,
        node_types: Arc<[String]>,
    },

    // ============ Selection ============
    /// Narrow to a single node by id. Graph → Node.
    Node { input: Arc<ReadExpr>, id: String },
    /// Narrow to a single edge by (src, dst). Graph → Edge.
    Edge {
        input: Arc<ReadExpr>,
        src: String,
        dst: String,
    },
    /// Navigate to a source node. Polymorphic on the endpoint's collection
    /// kind — the server field is `src` in every case, so one variant covers
    /// all of them: Edge → Node, `Edges` → `PathFromNode`, `NestedEdges` →
    /// `PathFromGraph`. The downstream terminal decides how the result is read.
    Src { input: Arc<ReadExpr> },
    /// Navigate to a destination node. Polymorphic like `Src` (server field
    /// `dst`): Edge → Node, `Edges` → `PathFromNode`, `NestedEdges` →
    /// `PathFromGraph`.
    Dst { input: Arc<ReadExpr> },
    /// Navigate to the "other end" node. Polymorphic like `Src` (server field
    /// `nbr`): Edge → Node, `Edges` → `PathFromNode`, `NestedEdges` →
    /// `PathFromGraph`. Context-sensitive per edge: on an out-edge yields the
    /// destination; on an in-edge yields the source.
    Nbr { input: Arc<ReadExpr> },
    /// Navigate to the event history of a node or edge. Node/Edge → History.
    /// Container-selection: the resulting `RemoteHistory` handle exposes
    /// terminals like `.count()`, `.collect()`, plus sub-container accessors
    /// (`.timestamps`, `.intervals`, etc.).
    History { input: Arc<ReadExpr> },
    /// Navigate to the combined event history of a `PathFromNode` /
    /// `PathFromGraph` collection — a single `History` container merging the
    /// time entries of all members. Container-selection like `History`.
    /// Server field: `combinedHistory`.
    CombinedHistory { input: Arc<ReadExpr> },
    /// Navigate to the reversed view of a `RemoteHistory` container — a new
    /// `History` whose iteration order is flipped. Container-selection like
    /// `History`. Server field: `reverse`.
    HistoryReverse { input: Arc<ReadExpr> },
    /// Navigate to the deletion history of an edge. Edge → History.
    /// Same shape as `History` but reads the `deletions` server field
    /// instead of `history` — deletions are edge-only.
    Deletions { input: Arc<ReadExpr> },
    /// Graph → the collection of all nodes in the (view-restricted) graph.
    Nodes { input: Arc<ReadExpr> },
    /// Node → the collection of the node's neighbours (both directions).
    Neighbours { input: Arc<ReadExpr> },
    /// Node → the collection of the node's in-neighbours.
    InNeighbours { input: Arc<ReadExpr> },
    /// Node → the collection of the node's out-neighbours.
    OutNeighbours { input: Arc<ReadExpr> },
    /// Graph → the collection of all edges in the (view-restricted) graph.
    Edges { input: Arc<ReadExpr> },
    /// Node → the collection of the node's edges (both directions).
    NodeEdges { input: Arc<ReadExpr> },
    /// Node → the collection of the node's incoming edges.
    InEdges { input: Arc<ReadExpr> },
    /// Node → the collection of the node's outgoing edges.
    OutEdges { input: Arc<ReadExpr> },
    /// Node → the collection of nodes reachable *into* this node via incoming
    /// edges (i.e., the node's ancestors in the directed graph). Server
    /// field: `inComponent`.
    InComponent { input: Arc<ReadExpr> },
    /// Node → the collection of nodes reachable *out from* this node via
    /// outgoing edges (i.e., the node's descendants). Server field: `outComponent`.
    OutComponent { input: Arc<ReadExpr> },
    /// Fan out an edge / edge collection into one instance per event.
    /// Polymorphic: on a single `Edge` produces an `Edges` collection with
    /// one entry per event; on an `Edges` collection produces an `Edges`
    /// collection with all events across all members. Server field: `explode`.
    Explode { input: Arc<ReadExpr> },
    /// Fan out an edge / edge collection into one instance per layer.
    /// Polymorphic on `Edge` and `Edges`. Server field: `explodeLayers`.
    ExplodeLayers { input: Arc<ReadExpr> },
    /// Reorder a `Nodes` collection by an ordered list of sort keys applied
    /// lexicographically. Returns a `Nodes` — chainable with any downstream
    /// terminal (`.collect`, `.count`, `.ids`, …). Server field:
    /// `sorted(sortBys: [NodeSortBy!]!)`.
    SortedNodes {
        input: Arc<ReadExpr>,
        sort_bys: Vec<NodeSortBy>,
    },
    /// Reorder an `Edges` collection. Same shape as `SortedNodes` with the
    /// edge-specific sort key set (adds `src` / `dst`). Server field:
    /// `sorted(sortBys: [EdgeSortBy!]!)`.
    SortedEdges {
        input: Arc<ReadExpr>,
        sort_bys: Vec<EdgeSortBy>,
    },
    /// Filter this view by a general filter expression (node/edge predicates,
    /// graph views, and/or/not combinations). The restriction propagates to
    /// downstream traversals. One variant serves Graph, Node, Edge, and every
    /// collection — they all expose the same `filter(expr: GqlFilter!)` field.
    Filtered {
        input: Arc<ReadExpr>,
        filter: Arc<GqlFilter>,
    },
    /// Narrow a `Nodes` collection's membership by a filter expression.
    /// Returns `Nodes`. Server field: `select(expr: NodeFilter!)` on
    /// `Nodes`.
    ///
    /// Applies the filter only to this step; downstream traversals from
    /// the matching nodes see the unfiltered graph.
    SelectNodes {
        input: Arc<ReadExpr>,
        filter: Arc<GqlNodeFilter>,
    },
    /// Narrow an `Edges` collection's membership by a filter expression.
    /// Returns `Edges`. Server field: `select(expr: EdgeFilter!)` on
    /// `Edges`.
    ///
    /// Applies the filter only to this step; downstream traversals from
    /// the matching edges see the unfiltered graph.
    SelectEdges {
        input: Arc<ReadExpr>,
        filter: Arc<GqlEdgeFilter>,
    },
    /// Pin a single `Edge` handle to one event — the exploded instance at
    /// exactly `(time, event_id)`, optionally restricted to `layer`.
    /// Returns an `Edge` that answers `time` / `layerName` like a member of
    /// `explode`. Server field: `event(time: TimeInput!, layer: String)` on
    /// `Edge` — `event_id: Some(_)` renders the exact `{timestamp, eventId}`
    /// object form. Used by `collect()` on exploded collections.
    EdgeEvent {
        input: Arc<ReadExpr>,
        time: i64,
        event_id: Option<i64>,
        layer: Option<String>,
    },

    /// Pin a single `Edge` handle to one layer-exploded instance by layer name
    /// — the analogue of `EdgeEvent` for `explodeLayers`. Returns an `Edge` that
    /// answers `layerName` like a member of `explodeLayers` (`time` is
    /// unavailable, matching local). Server field: `eventLayer(name: String!)`
    /// on `Edge`. Used by `collect()` on layer-exploded collections.
    EdgeLayerEvent { input: Arc<ReadExpr>, layer: String },

    // ============ Properties / Metadata containers ============
    /// Navigate to the non-temporal metadata container. Polymorphic:
    /// Graph/Node/Edge → Metadata. Server field: `metadata`.
    Metadata { input: Arc<ReadExpr> },
    /// Navigate to the full properties container (temporal + non-temporal).
    /// Polymorphic: Graph/Node/Edge → Properties. Server field: `properties`.
    Properties { input: Arc<ReadExpr> },
    /// Terminal on a properties/metadata container: fetch a single property
    /// value by key — `Option<Prop>`. Only `{ value }` is selected (the caller
    /// already knows the key). The server returns `null` when the key isn't
    /// present, decoded to `None` client-side rather than raising `NotFound`
    /// (see nullable-intermediate handling in `parse_read`). Server field:
    /// `get(key: String!)`.
    PropertyGet { input: Arc<ReadExpr>, key: String },
    /// Terminal on a properties/metadata container: `bool` — does a
    /// property with this key exist? Server field: `contains(key: String!)`.
    PropertyContains { input: Arc<ReadExpr>, key: String },
    /// Terminal on a properties/metadata container: `Vec<String>` — all
    /// property keys. Server field: `keys`.
    PropertyKeys { input: Arc<ReadExpr> },
    /// Terminal on a properties container: the data-type of the property's
    /// latest value by key — `Option<String>`. `None` when the key isn't
    /// present. The string is the `PropType` display form (e.g. `"I64"`,
    /// `"Str"`, `"List<F64>"`). Server field: `getDtypeOf(key: String!)`.
    PropertyGetDtypeOf { input: Arc<ReadExpr>, key: String },
    /// Terminal on a properties/metadata container: `Vec<Prop>` — the property
    /// values only (`{ value }` selected per record; keys aren't fetched —
    /// use `PropertyItems` when pairs are needed). Optional `keys` whitelist
    /// filters the returned set. Server field: `values(keys: [String!])`.
    PropertyValues {
        input: Arc<ReadExpr>,
        keys: Option<Vec<String>>,
    },
    /// Terminal on a properties/metadata container: `Vec<(String, Prop)>` —
    /// full `(key, value)` pairs (`{ key value }` selected per record). The
    /// pair-fetching sibling of `PropertyValues`; backs `.items()`. Optional
    /// `keys` whitelist filters the returned set. Server field:
    /// `values(keys: [String!])`.
    PropertyItems {
        input: Arc<ReadExpr>,
        keys: Option<Vec<String>>,
    },
    /// Navigate to the temporal-only view of a properties container.
    /// Properties → TemporalProperties. Server field: `temporal`.
    TemporalProperties { input: Arc<ReadExpr> },
    /// Select a single temporal property by key. TemporalProperties →
    /// TemporalProperty. Server field: `get(key)` — but rendered without
    /// inner sub-selection so downstream terminals nest their own.
    TemporalPropertyByKey { input: Arc<ReadExpr>, key: String },
    /// Terminal on a TemporalProperties container: `Vec<String>` — the keys
    /// of each temporal property, optionally filtered. Server field:
    /// `values(keys) { key }` — we extract just the key from each record.
    TemporalPropertyList {
        input: Arc<ReadExpr>,
        keys: Option<Vec<String>>,
    },
    /// Terminal on a TemporalProperty: `Vec<Prop>` — all values this
    /// property has ever taken, in temporal order. Server field: `values`.
    TemporalPropertyValueList { input: Arc<ReadExpr> },
    /// Terminal on a TemporalProperty: value at or before the given time.
    /// Server field: `at(t)`. Nullable — returns `None` if no update
    /// exists on or before `t`.
    TemporalPropertyAt { input: Arc<ReadExpr>, time: i64 },
    /// Terminal on a TemporalProperty: the most recent value. Server field:
    /// `latest`. Nullable — `None` if the property has no updates in view.
    TemporalPropertyLatest { input: Arc<ReadExpr> },
    /// Terminal on a TemporalProperty: the set of distinct values (order
    /// not guaranteed). Server field: `unique`.
    TemporalPropertyUnique { input: Arc<ReadExpr> },
    /// Terminal on a TemporalProperty: collapse consecutive-equal updates
    /// into a single `(time, value)` pair. `latest_time = true` picks the
    /// last timestamp of each run; `false` picks the first. Server field:
    /// `orderedDedupe(latestTime: bool)`.
    TemporalPropertyOrderedDedupe {
        input: Arc<ReadExpr>,
        latest_time: bool,
    },
    /// Terminal on a TemporalProperty: sum of all updates. Server field:
    /// `sum`. Nullable.
    TemporalPropertySum { input: Arc<ReadExpr> },
    /// Terminal on a TemporalProperty: mean of all updates. Server field:
    /// `mean`. Nullable.
    TemporalPropertyMean { input: Arc<ReadExpr> },
    /// Terminal on a TemporalProperty: mean (alias). Server field: `average`.
    /// Nullable.
    TemporalPropertyAverage { input: Arc<ReadExpr> },
    /// Terminal on a TemporalProperty: minimum `(time, value)` pair.
    /// Server field: `min`. Nullable.
    TemporalPropertyMin { input: Arc<ReadExpr> },
    /// Terminal on a TemporalProperty: maximum `(time, value)` pair.
    /// Server field: `max`. Nullable.
    TemporalPropertyMax { input: Arc<ReadExpr> },
    /// Terminal on a TemporalProperty: median `(time, value)` pair.
    /// Server field: `median`. Nullable.
    TemporalPropertyMedian { input: Arc<ReadExpr> },

    /// Terminal on Graph: the full schema tree (node types + edge layers +
    /// their property schemas). Compound-structured — the entire nested
    /// tree is fetched in one RPC and materialized as plain data structs.
    /// Server field: `schema`.
    Schema { input: Arc<ReadExpr> },

    /// Terminal on Graph: given a set of node ids, return the nodes that
    /// are common neighbours of *all* of them (set intersection). Empty
    /// result if any input id doesn't exist or the list is empty. Returns
    /// `Vec<String>` of names — clients wrap each in a `RemoteNode`.
    /// Server field: `sharedNeighbours(selectedNodes: [NodeId!]!)`.
    SharedNeighbours {
        input: Arc<ReadExpr>,
        ids: Vec<String>,
    },

    /// Terminal on Graph: the nodes whose latest property values match every
    /// `(name, value)` entry in `properties`. Returns `Vec<String>` of node
    /// names — clients wrap each in a `RemoteNode`. Server field:
    /// `findNodes(propertiesDict: [PropertyInput!]!)`.
    FindNodes {
        input: Arc<ReadExpr>,
        properties: HashMap<String, Prop>,
    },
    /// Terminal on Graph: the edges whose latest property values match every
    /// `(name, value)` entry in `properties`. Returns `Vec<(String, String)>`
    /// of `(src, dst)` name pairs — clients wrap each in a `RemoteEdge`.
    /// Server field: `findEdges(propertiesDict: [PropertyInput!]!)`.
    FindEdges {
        input: Arc<ReadExpr>,
        properties: HashMap<String, Prop>,
    },
    /// Terminal on Graph: all node types present in the graph — `Vec<String>`.
    /// Server field: `getAllNodeTypes`.
    GetAllNodeTypes { input: Arc<ReadExpr> },

    // ============ Scalar terminals on Graph ============
    /// Terminal: total node count under the current view — `i64`.
    CountNodes { input: Arc<ReadExpr> },
    /// Terminal: total edge count under the current view — `i64`.
    CountEdges { input: Arc<ReadExpr> },

    // ============ Scalar terminals on Node ============
    /// Terminal: node degree — `i64`.
    Degree { input: Arc<ReadExpr> },
    /// Terminal: in-degree — `i64`.
    InDegree { input: Arc<ReadExpr> },
    /// Terminal: out-degree — `i64`.
    OutDegree { input: Arc<ReadExpr> },
    /// Terminal: node name — `String`.
    Name { input: Arc<ReadExpr> },

    // ============ Compound terminals on Graph or Node → Option<i64> ============
    // Server returns an `EventTime` object; we query `<field> { timestamp }`
    // and unwrap the (possibly-null) `timestamp` field.
    /// Terminal: earliest event time — `Option<i64>`. Works on Graph and Node.
    EarliestTime { input: Arc<ReadExpr> },
    /// Terminal: latest event time — `Option<i64>`. Works on Graph and Node.
    LatestTime { input: Arc<ReadExpr> },
    /// Terminal: view start bound — `Option<i64>`. Works on Graph and Node.
    Start { input: Arc<ReadExpr> },
    /// Terminal: view end bound — `Option<i64>`. Works on Graph and Node.
    End { input: Arc<ReadExpr> },
    /// Terminal: earliest edge event time under this view — `Option<i64>`. Graph only.
    EarliestEdgeTime { input: Arc<ReadExpr> },
    /// Terminal: latest edge event time under this view — `Option<i64>`. Graph only.
    LatestEdgeTime { input: Arc<ReadExpr> },
    /// Terminal: first update time on this node — `Option<i64>`. Node only.
    FirstUpdate { input: Arc<ReadExpr> },
    /// Terminal: last update time on this node — `Option<i64>`. Node only.
    LastUpdate { input: Arc<ReadExpr> },
    /// Terminal: the time an edge event occurred — `Option<i64>`. Edge only.
    /// Server field is `Result<GqlEventTime, GraphError>`; the client treats
    /// server-side errors as `ClientError::GraphQLErrors`.
    Time { input: Arc<ReadExpr> },

    // ============ Graph scalar terminals ============
    /// Terminal: check if a node with `id` exists in the view — `bool`.
    HasNode { input: Arc<ReadExpr>, id: String },
    /// Terminal: check if an edge with `(src, dst)` exists in the view — `bool`.
    HasEdge {
        input: Arc<ReadExpr>,
        src: String,
        dst: String,
    },
    /// Terminal: total count of temporal edges (edge updates) — `i64`.
    CountTemporalEdges { input: Arc<ReadExpr> },
    /// Terminal: graph path — `String`.
    Path { input: Arc<ReadExpr> },
    /// Terminal: parent namespace of the graph path — `String`.
    Namespace { input: Arc<ReadExpr> },
    /// Terminal: graph creation timestamp — `i64` (metadata, always set).
    Created { input: Arc<ReadExpr> },
    /// Terminal: graph last-opened timestamp — `i64` (metadata, always set).
    LastOpened { input: Arc<ReadExpr> },
    /// Terminal: graph last-updated timestamp — `i64` (metadata, always set).
    LastUpdated { input: Arc<ReadExpr> },
    /// Terminal: layer names present in this graph — `Vec<String>`.
    UniqueLayers { input: Arc<ReadExpr> },
    /// Terminal: does this view contain a layer with the given `name`? — `bool`.
    /// Polymorphic across Graph/Node/Edge and the node/edge collections.
    /// Server field: `hasLayer(name: String!)`.
    HasLayer { input: Arc<ReadExpr>, name: String },
    /// Terminal: the size of the window covered by this view (`end - start`),
    /// or `None` for an unbounded view — `Option<i64>`. Polymorphic across
    /// Graph/Node/Edge and the node/edge collections. Server field: `windowSize`.
    WindowSize { input: Arc<ReadExpr> },

    // ============ Collection terminals (on Nodes/Edges collections) ============
    /// Terminal on a Nodes collection: list of member ids — `Vec<String>`.
    Ids { input: Arc<ReadExpr> },
    /// Terminal on a `PathFromGraph` collection: the nested list of member ids
    /// — `Vec<Vec<String>>` (one inner list per source node). Renders the
    /// columnar `ids` field (whole nested result in one server-side compute,
    /// not one per source). Parsed as `Prop::List(Prop::List(Prop::Str))`
    /// (outer = per source, inner = ids).
    NestedIds { input: Arc<ReadExpr> },
    /// Terminal on a `PathFromGraph` collection: the ids of the SOURCE nodes the
    /// paths hang off — `Vec<String>`, one per source, aligned with `NestedIds`'
    /// outer index. Server field: `sourceIds`. Lets a client pair each source
    /// with its own path in one RPC (see `HandleCtx::path_handle_expr`) instead
    /// of one RPC per source.
    SourceIds { input: Arc<ReadExpr> },
    /// Terminal on a `Nodes`/`PathFromNode` collection: the per-node degree
    /// (number of incident edges) as a FLAT list — `Vec<i64>`. Renders
    /// `degree`. Distinct from the scalar `Degree` (single node); this parses a
    /// JSON int array via `expect_i64_list`.
    CollectionDegree { input: Arc<ReadExpr> },
    /// Terminal on a `Nodes`/`PathFromNode` collection: per-node in-degree as a
    /// FLAT list — `Vec<i64>`. Renders `inDegree`.
    CollectionInDegree { input: Arc<ReadExpr> },
    /// Terminal on a `Nodes`/`PathFromNode` collection: per-node out-degree as a
    /// FLAT list — `Vec<i64>`. Renders `outDegree`.
    CollectionOutDegree { input: Arc<ReadExpr> },
    /// Terminal on a `Nodes`/`PathFromNode` collection: per-node count of
    /// incident edge updates as a FLAT list — `Vec<i64>`. Renders
    /// `edgeHistoryCount`.
    CollectionEdgeHistoryCount { input: Arc<ReadExpr> },
    /// Terminal on a `PathFromGraph` collection: the NESTED per-node degree —
    /// `Vec<Vec<i64>>` (one inner list per source node). Renders the columnar
    /// `degree` field. Mirrors `NestedIds`. Parsed via
    /// `expect_nested_i64_list`.
    NestedDegree { input: Arc<ReadExpr> },
    /// Terminal on a `PathFromGraph` collection: the NESTED per-node in-degree —
    /// `Vec<Vec<i64>>`. Renders the columnar `inDegree` field. Mirrors
    /// `NestedDegree`.
    NestedInDegree { input: Arc<ReadExpr> },
    /// Terminal on a `PathFromGraph` collection: the NESTED per-node out-degree —
    /// `Vec<Vec<i64>>`. Renders the columnar `outDegree` field. Mirrors
    /// `NestedDegree`.
    NestedOutDegree { input: Arc<ReadExpr> },
    /// Terminal on a `PathFromGraph` collection: the NESTED per-node count of
    /// incident edge updates — `Vec<Vec<i64>>`. Renders `list { edgeHistoryCount }`
    /// (per-source `PathFromNode` records — no columnar `edgeHistoryCount` field
    /// exists on the server's `PathFromGraph`). Mirrors `NestedDegree` in shape.
    NestedEdgeHistoryCount { input: Arc<ReadExpr> },
    /// Terminal on a collection: number of members — `i64`.
    /// Distinct from `CountNodes`/`CountEdges` (which are Graph-scope); this
    /// fires against the collection's `count` field. Also polymorphic on
    /// `RemoteHistory` — same server field name (`count`).
    Count { input: Arc<ReadExpr> },
    /// Terminal on a `RemoteHistory` container: whether the history is empty
    /// — `bool`. Server field name is `isEmpty`.
    IsEmpty { input: Arc<ReadExpr> },
    /// Terminal on a `RemoteHistory` container: list all events in ascending
    /// order — `Vec<RemoteEventTime>`. Server field is `list`; queries the
    /// compound sub-fields `timestamp`, `dt`, `eventId` per record.
    HistoryList { input: Arc<ReadExpr> },
    /// Terminal on a `RemoteHistory` container: list all events in descending
    /// order — `Vec<RemoteEventTime>`. Server field is `listRev`.
    HistoryListRev { input: Arc<ReadExpr> },
    /// Terminal on a `RemoteHistory` container: paginated list of events in
    /// ascending order — `Vec<RemoteEventTime>`. `offset` and `page_index`
    /// are optional; each defaults to 0 server-side.
    HistoryPage {
        input: Arc<ReadExpr>,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    },
    /// Terminal on a `RemoteHistory` container: paginated list of events in
    /// descending order — `Vec<RemoteEventTime>`. Same args as `HistoryPage`.
    HistoryPageRev {
        input: Arc<ReadExpr>,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    },

    // ============ RemoteHistory sub-container selection ============
    /// Navigate to the timestamps view of a history. History → HistoryTimestamps.
    /// Server field: `timestamps`.
    HistoryTimestamps { input: Arc<ReadExpr> },
    /// Navigate to the event-id view of a history. History → HistoryEventIds.
    /// Server field: `eventId`.
    HistoryEventIds { input: Arc<ReadExpr> },
    /// Navigate to the datetime view of a history. History → HistoryDateTimes.
    /// Server field: `datetimes` (no format arg — server default RFC 3339).
    HistoryDateTimes { input: Arc<ReadExpr> },
    /// Navigate to the intervals view of a history — inter-event gaps.
    /// History → HistoryIntervals. Server field: `intervals`.
    HistoryIntervals { input: Arc<ReadExpr> },

    // ============ Sub-container list/page terminals (polymorphic) ============
    // These four variants render as `list` / `listRev` / `page(...)` /
    // `pageRev(...)` on the underlying sub-container. Return type is
    // determined by `parse_read` based on the parent selection variant:
    // int list for Timestamps/EventIds/Intervals, string list for DateTimes.
    /// Terminal on any sub-container: list in ascending order.
    SubList { input: Arc<ReadExpr> },
    /// Terminal on any sub-container: list in descending order.
    SubListRev { input: Arc<ReadExpr> },
    /// Terminal on any sub-container: paginated ascending list.
    SubPage {
        input: Arc<ReadExpr>,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    },
    /// Terminal on any sub-container: paginated descending list.
    SubPageRev {
        input: Arc<ReadExpr>,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    },

    // ============ Intervals scalar stats ============
    /// Terminal on `HistoryIntervals`: mean of inter-event gaps. `Option<f64>`.
    IntervalsMean { input: Arc<ReadExpr> },
    /// Terminal on `HistoryIntervals`: median of inter-event gaps. `Option<i64>`.
    IntervalsMedian { input: Arc<ReadExpr> },
    /// Terminal on `HistoryIntervals`: max inter-event gap. `Option<i64>`.
    IntervalsMax { input: Arc<ReadExpr> },
    /// Terminal on `HistoryIntervals`: min inter-event gap. `Option<i64>`.
    IntervalsMin { input: Arc<ReadExpr> },
    /// Terminal on an Edges collection: list of (src, dst) pairs.
    /// Returned as `Prop::List(Prop::List(Prop::Str, Prop::Str), ...)` on the
    /// wire — each outer element is a 2-element inner list `[src, dst]`.
    /// Distinct from `Ids` (nodes) because edges have no single-string id;
    /// they're identified by the pair.
    EdgesList { input: Arc<ReadExpr> },
    /// Terminal on a `NestedEdges` collection: the nested list of (src, dst)
    /// pairs — one inner list per source node. Renders
    /// `list { list { src { name } dst { name } } }`: `NestedEdges.list` is
    /// `[Edges!]!`, and each per-source `Edges` yields its own flat edge list.
    /// Parsed as `Prop::List(Prop::List(Prop::List(Prop::Str, Prop::Str)))`
    /// (outer = per source, middle = that source's edges, inner = `[src, dst]`).
    /// Mirrors `EdgesList`, one level deeper.
    NestedEdgesList { input: Arc<ReadExpr> },
    /// Terminal on an *exploded* `Edges` collection: each member's full event
    /// identity, fetched in ONE RPC so the handle pins can't skew against a
    /// concurrent write. Renders
    /// `list { src { name } dst { name } time { timestamp eventId } layerName }`.
    /// Parsed as an outer `Prop::List` with one 5-element inner list per
    /// member: `[src, dst, timestamp, event_id, layer_name]` (`Str, Str, I64,
    /// I64, Str`). Used by `collect()` on exploded collections to build
    /// `EdgeEvent`-pinned handles.
    ExplodedEdgesList { input: Arc<ReadExpr> },
    /// Terminal on an exploded `NestedEdges` collection: the nested variant of
    /// `ExplodedEdgesList` — one inner list per source node. Renders
    /// `list { list { src { name } dst { name } time { timestamp eventId } layerName } }`.
    NestedExplodedEdgesList { input: Arc<ReadExpr> },
    /// Terminal on a layer-exploded `Edges` collection: one `(src, dst, layer)`
    /// per member. Renders `list { src { name } dst { name } layerName }`. Used
    /// by `collect()` to pin each layer instance (no time — `explodeLayers`
    /// members have a layer but not a single event time).
    ExplodedLayersEdgesList { input: Arc<ReadExpr> },
    /// Nested variant of `ExplodedLayersEdgesList` — one inner list per source
    /// node. Renders `list { list { src { name } dst { name } layerName } }`.
    NestedExplodedLayersEdgesList { input: Arc<ReadExpr> },

    // ============ Columnar accessors on collections (via `list { field }`) ============
    // Each renders `list { <field> }` on a flat collection (`Nodes` /
    // `PathFromNode` / `Edges`) and reads the per-element scalar back into a
    // flat `Prop::List`. The `Nested*` variants render `list { list { <field> } }`
    // on a nested collection (`PathFromGraph` / `NestedEdges`) and produce a
    // per-source `Prop::List(Prop::List(..))`. All open ONE net brace (the
    // outer `list`); inner groups are self-balanced. Optional scalars use the
    // `Prop::List` wrapper convention: `[]` = None, `[x]` = Some(x).
    /// FLAT: per-node `name` — `Vec<String>`. Renders `list { name }`.
    CollectionNames { input: Arc<ReadExpr> },
    /// FLAT: per-node `nodeType` — `Vec<Option<String>>`. Renders `list { nodeType }`.
    CollectionNodeTypes { input: Arc<ReadExpr> },
    /// FLAT: per-edge `layerNames` — `Vec<Vec<String>>`. Renders `list { layerNames }`.
    CollectionLayerNames { input: Arc<ReadExpr> },
    /// FLAT: per-edge `layerName` — `Vec<String>` (exploded edges only; the
    /// server field is `Result`, surfacing as a GraphQL error otherwise).
    /// Renders `list { layerName }`.
    CollectionLayerName { input: Arc<ReadExpr> },
    /// FLAT: per-edge `earliestTime` — `Vec<Option<EventTime>>`. Renders
    /// `list { earliestTime { timestamp datetime eventId } }`.
    CollectionEarliestTime { input: Arc<ReadExpr> },
    /// FLAT: per-edge `latestTime` — `Vec<Option<EventTime>>`.
    CollectionLatestTime { input: Arc<ReadExpr> },
    /// FLAT: per-edge `time` — `Vec<Option<EventTime>>` (exploded edges only).
    CollectionTime { input: Arc<ReadExpr> },
    /// NESTED: per-source per-node `name` — `Vec<Vec<String>>`. Renders
    /// `list { list { name } }`.
    NestedNames { input: Arc<ReadExpr> },
    /// NESTED: per-source per-node `nodeType` — `Vec<Vec<Option<String>>>`.
    NestedNodeTypes { input: Arc<ReadExpr> },
    /// NESTED: per-source per-edge `layerNames` — `Vec<Vec<Vec<String>>>`.
    NestedLayerNames { input: Arc<ReadExpr> },
    /// NESTED: per-source per-edge `layerName` — `Vec<Vec<String>>` (exploded only).
    NestedLayerName { input: Arc<ReadExpr> },
    /// NESTED: per-source per-edge `earliestTime` — `Vec<Vec<Option<EventTime>>>`.
    NestedEarliestTime { input: Arc<ReadExpr> },
    /// NESTED: per-source per-edge `latestTime` — `Vec<Vec<Option<EventTime>>>`.
    NestedLatestTime { input: Arc<ReadExpr> },
    /// NESTED: per-source per-edge `time` — `Vec<Vec<Option<EventTime>>>` (exploded only).
    NestedTime { input: Arc<ReadExpr> },
    /// FLAT: per-edge `isActive` — `Vec<bool>`. Renders `list { isActive }`.
    CollectionIsActive { input: Arc<ReadExpr> },
    /// FLAT: per-edge `isValid` — `Vec<bool>`. Renders `list { isValid }`.
    CollectionIsValid { input: Arc<ReadExpr> },
    /// FLAT: per-edge `isDeleted` — `Vec<bool>`. Renders `list { isDeleted }`.
    CollectionIsDeleted { input: Arc<ReadExpr> },
    /// FLAT: per-edge `isSelfLoop` — `Vec<bool>`. Renders `list { isSelfLoop }`.
    CollectionIsSelfLoop { input: Arc<ReadExpr> },
    /// NESTED: per-source per-edge `isActive` — `Vec<Vec<bool>>`. Renders
    /// `list { list { isActive } }`.
    NestedIsActive { input: Arc<ReadExpr> },
    /// NESTED: per-source per-edge `isValid` — `Vec<Vec<bool>>`.
    NestedIsValid { input: Arc<ReadExpr> },
    /// NESTED: per-source per-edge `isDeleted` — `Vec<Vec<bool>>`.
    NestedIsDeleted { input: Arc<ReadExpr> },
    /// NESTED: per-source per-edge `isSelfLoop` — `Vec<Vec<bool>>`.
    NestedIsSelfLoop { input: Arc<ReadExpr> },

    // ============ Columnar property / metadata containers on collections ============
    // These descend into each collection member's `metadata` / `properties`
    // container and fetch all `{key, value}` entries, so the client can pivot
    // them into per-key columns (one value per member, `None` where a member
    // lacks the key). FLAT variants render `list { <container> { values { key
    // value } } }` on `Nodes` / `Edges` / `PathFromNode`; NESTED variants render
    // `list { list { <container> { values { key value } } } }` on `PathFromGraph`
    // / `NestedEdges`. Each opens ONE net brace (the outer `list`); inner groups
    // self-balance. For `properties`, temporal values collapse to their latest
    // under the current view — matching the local columnar property views.
    /// FLAT: each member's metadata entries — one `[{key, value}]` per member.
    CollectionMetadataValues { input: Arc<ReadExpr> },
    /// FLAT: each member's property entries (temporal → latest).
    CollectionPropertiesValues { input: Arc<ReadExpr> },
    /// NESTED: per-source per-member metadata entries.
    NestedMetadataValues { input: Arc<ReadExpr> },
    /// NESTED: per-source per-member property entries (temporal → latest).
    NestedPropertiesValues { input: Arc<ReadExpr> },

    // ============ Node scalar terminals ============
    /// Terminal: node id — `String` (server may return int-like GID; treated as string).
    Id { input: Arc<ReadExpr> },
    /// Terminal: node type — `Option<String>` (null if not set).
    NodeType { input: Arc<ReadExpr> },
    /// Terminal: whether the node has any events in the current view — `bool`.
    /// Also polymorphic on Edge — same server field name.
    IsActive { input: Arc<ReadExpr> },
    /// Terminal: count of temporal edge events on this node — `i64`.
    EdgeHistoryCount { input: Arc<ReadExpr> },

    // ============ Edge scalar terminals ============
    /// Terminal: edge id — pair of endpoint ids as `Vec<String>` of length 2.
    /// Distinct from Node's `Id` (single string): server field is the same
    /// name (`id`) but returns `Vec<GqlNodeId>` for edges.
    EdgeIdPair { input: Arc<ReadExpr> },
    /// Terminal: layer names the edge is present in — `Vec<String>`.
    LayerNames { input: Arc<ReadExpr> },
    /// Terminal: single layer name for a layer-restricted edge view — `String`.
    /// Server field is `Result<String, GraphError>`; server-side error surfaces
    /// as `ClientError::GraphQLErrors`.
    LayerName { input: Arc<ReadExpr> },
    /// Terminal: whether the edge is valid at the current time — `bool`.
    IsValid { input: Arc<ReadExpr> },
    /// Terminal: whether the edge has been deleted at the current time — `bool`.
    IsDeleted { input: Arc<ReadExpr> },
    /// Terminal: whether the edge's `src == dst` — `bool`.
    IsSelfLoop { input: Arc<ReadExpr> },
}

/// How a collection has been fanned out into per-instance members, if at all.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Fanout {
    /// One member per event (`explode`). Members are re-addressable as
    /// handles via the server's `Edge.event` field.
    Events,
    /// One member per layer (`explodeLayers`). Members are re-addressable as
    /// handles via the server's `Edge.eventLayer` field (pinned by layer name).
    Layers,
}

/// The per-member pin `collect()` substitutes at the `Fanout` marker when
/// materializing an exploded edge handle.
#[derive(Clone, Debug)]
pub enum EdgePin {
    /// A time-exploded instance (`explode`) — pinned by `(time, event_id?)`,
    /// optionally within `layer`. Renders `EdgeEvent`.
    Event {
        time: i64,
        event_id: Option<i64>,
        layer: Option<String>,
    },
    /// A layer-exploded instance (`explodeLayers`) — pinned by layer name.
    /// Renders `EdgeLayerEvent`.
    Layer { layer: String },
}

/// One collection-level operation deferred for replay onto materialized
/// entity handles. `collect()` rebuilds each member as a fresh entity
/// selection (`node(id)` / `edge(src, dst)`) anchored on the parent graph
/// view, then replays these ops in application order, so the handle
/// evaluates under the same composed view as collection-level reads.
///
/// Order is load-bearing: filters capture the view they were created on, so
/// `.filter(f).window(w)` and `.window(w).filter(f)` differ for temporal
/// property filters — replay must preserve the user's call order.
#[derive(Clone, Debug)]
pub enum HandleOp {
    /// A pure view op (window / layer / at / …). Stores the op as data, so
    /// replay applies the same `ReadExpr` node the collection applied to its
    /// own `expr` — definitionally identical, and inspectable in tests. Every
    /// collection view op has a same-named server field on `Node` and `Edge`,
    /// so replay always renders.
    View(ViewOp),
    /// An anchor-relative filter. Replays as the unified `filter(expr:)`
    /// field on both node and edge handles.
    Filter(Arc<GqlFilter>),
    /// Positional marker recording where `explode` / `explodeLayers` was
    /// applied in the op chain. Ops before the marker shape the view the
    /// instances were enumerated from; ops after it wrap the pinned handle.
    /// `collect()` substitutes each member's `EdgeEvent` pin at this position.
    Fanout(Fanout),
}

/// The view-op vocabulary shared by every remote collection/entity handle —
/// the data form of `window`/`layer`/`at`/…, stored in `HandleCtx` so
/// `collect()` can replay the exact chain per member. Being data (not a
/// closure), a recorded chain can be printed, compared, and asserted on.
#[derive(Clone, Debug, PartialEq)]
pub enum ViewOp {
    Window { start: InputTime, end: InputTime },
    At { time: InputTime },
    Before { time: InputTime },
    After { time: InputTime },
    Latest,
    SnapshotLatest,
    SnapshotAt { time: InputTime },
    ShrinkWindow { start: InputTime, end: InputTime },
    ShrinkStart { start: InputTime },
    ShrinkEnd { end: InputTime },
    Layer { name: String },
    ExcludeLayer { name: String },
    Layers { names: Arc<[String]> },
    ExcludeLayers { names: Arc<[String]> },
    ValidLayers { names: Arc<[String]> },
    ExcludeValidLayer { name: String },
    ExcludeValidLayers { names: Arc<[String]> },
    DefaultLayer,
}

impl ViewOp {
    /// Wrap `input` in this op's `ReadExpr` node.
    pub fn apply(&self, input: Arc<ReadExpr>) -> ReadExpr {
        ReadExpr::View {
            input,
            op: self.clone(),
        }
    }
}

/// Materialization context carried by every remote collection and entity
/// handle. `graph` is the view chain accumulated *before* entering the
/// collection (graph-level ops); `ops` are the collection-level ops applied
/// *after* it, replayed per member by `collect()`. Flows down unchanged into
/// child collections (`.neighbours()`, `.edges()`, …) so filters keep
/// propagating to descendants exactly like the local one-hop semantics.
#[derive(Clone, Debug)]
pub struct HandleCtx {
    /// The parent graph view under which the collection lives.
    pub graph: Arc<ReadExpr>,
    /// Ordered entity-level ops to replay when materializing handles.
    pub ops: Vec<HandleOp>,
}

impl HandleCtx {
    pub fn new(graph: impl Into<Arc<ReadExpr>>) -> Self {
        Self {
            graph: graph.into(),
            ops: Vec::new(),
        }
    }

    /// A copy of this context with one more op appended.
    pub fn with_op(&self, op: HandleOp) -> Self {
        let mut ops = self.ops.clone();
        ops.push(op);
        Self {
            graph: self.graph.clone(),
            ops,
        }
    }

    /// The first fanout marker in the op chain, if any. Later markers are
    /// no-ops server-side (exploding an already-pinned instance yields
    /// itself), so only the first decides how `collect()` materializes.
    pub fn fanout(&self) -> Option<Fanout> {
        self.ops.iter().find_map(|op| match op {
            HandleOp::Fanout(f) => Some(*f),
            _ => None,
        })
    }

    /// Replay the op chain onto a single-node anchor. Fanout markers never
    /// occur in node collections and are ignored.
    pub fn node_handle_expr(&self, id: String) -> ReadExpr {
        let mut expr = ReadExpr::Node {
            input: self.graph.clone(),
            id,
        };
        for op in &self.ops {
            expr = match op {
                HandleOp::View(op) => op.apply(Arc::new(expr)),
                HandleOp::Filter(filter) => ReadExpr::Filtered {
                    input: Arc::new(expr),
                    filter: filter.clone(),
                },
                HandleOp::Fanout(_) => expr,
            };
        }
        expr
    }

    /// Replay the op chain onto a single-edge anchor, optionally pinning an
    /// event at the position of the first fanout marker. `event` is
    /// `(time, event_id, layer)` as fetched by `ExplodedEdgesList`; callers
    /// materializing a non-exploded collection pass `None`.
    pub fn edge_handle_expr(&self, src: String, dst: String, pin: Option<EdgePin>) -> ReadExpr {
        let mut expr = ReadExpr::Edge {
            input: self.graph.clone(),
            src,
            dst,
        };
        let mut pin = pin;
        for op in &self.ops {
            expr = match op {
                HandleOp::View(op) => op.apply(Arc::new(expr)),
                HandleOp::Filter(filter) => ReadExpr::Filtered {
                    input: Arc::new(expr),
                    filter: filter.clone(),
                },
                HandleOp::Fanout(_) => match pin.take() {
                    Some(EdgePin::Event {
                        time,
                        event_id,
                        layer,
                    }) => ReadExpr::EdgeEvent {
                        input: Arc::new(expr),
                        time,
                        event_id,
                        layer,
                    },
                    Some(EdgePin::Layer { layer }) => ReadExpr::EdgeLayerEvent {
                        input: Arc::new(expr),
                        layer,
                    },
                    None => expr,
                },
            };
        }
        expr
    }

    /// Re-root a *nested* collection's read expression at ONE of its source
    /// nodes, yielding the flat per-source expression — the remote analogue of
    /// the path half of local `PathFromGraph`'s `(source, path)` iteration.
    ///
    /// Where `node_handle_expr` re-anchors at a single node and replays the
    /// recorded `ops`, this re-anchors the collection's own `expr` tree. That
    /// tree is the only record of *where* the traversal sits in the op chain —
    /// `ops` flattens pre- and post-traversal ops into one list (the same
    /// positional problem `HandleOp::Fanout` solves for exploded edges). The
    /// node collection the chain starts from (`nodes`, `inComponent`,
    /// `outComponent`) is swapped for a `Node(id)` selection on this context's
    /// graph view — the very anchor `node_handle_expr` uses — so every op above
    /// it lands on the single source, and each nested server type degrades to
    /// its flat sibling (`PathFromGraph` → `PathFromNode`, `NestedEdges` →
    /// `Edges`) under the same field names.
    ///
    /// Ops that only decide WHICH sources the chain starts from (`typeFilter` /
    /// `select` / `sorted`, below the first traversal) are dropped, exactly as
    /// `node_handle_expr` drops them: the source is already pinned, and a single
    /// `Node` has no such field. The same ops *above* the traversal narrow the
    /// path itself and are kept.
    ///
    /// `None` when the chain contains a step with no single-source counterpart —
    /// callers surface that as an error rather than pair up wrong data.
    pub fn path_handle_expr(&self, expr: &ReadExpr, id: &str) -> Option<Arc<ReadExpr>> {
        rebase_at_source(expr, &self.graph, id).map(|(rebased, _)| rebased)
    }
}

/// Rebuild one link of a collection chain around a replacement input.
type Rebuild<'a> = Box<dyn Fn(Arc<ReadExpr>) -> ReadExpr + 'a>;

/// Worker for `HandleCtx::path_handle_expr`. Returns the re-rooted expression
/// plus whether the result is still inside the *source-selection* segment —
/// the part of the chain below the first traversal, where membership ops are
/// dropped because a single source is already pinned.
fn rebase_at_source(
    expr: &ReadExpr,
    anchor: &Arc<ReadExpr>,
    id: &str,
) -> Option<(Arc<ReadExpr>, bool)> {
    use ReadExpr as E;

    // Bottom of the chain: a node-collection producer. Everything below it is
    // the graph view, which `anchor` already carries.
    if matches!(
        expr,
        E::Nodes { .. } | E::InComponent { .. } | E::OutComponent { .. }
    ) {
        return Some((
            Arc::new(E::Node {
                input: anchor.clone(),
                id: id.to_string(),
            }),
            true,
        ));
    }

    // `(input, is_traversal, source_only, rebuild)`. `is_traversal` ends the
    // source-selection segment; `source_only` marks a membership op that is
    // dropped while still inside it.
    let (input, is_traversal, source_only, rebuild): (_, _, _, Rebuild) = match expr {
        // Traversals — polymorphic server fields that yield the flat sibling
        // type once the input is a single node / flat collection.
        E::Neighbours { input } => (
            input,
            true,
            false,
            Box::new(|input| E::Neighbours { input }),
        ),
        E::InNeighbours { input } => (
            input,
            true,
            false,
            Box::new(|input| E::InNeighbours { input }),
        ),
        E::OutNeighbours { input } => (
            input,
            true,
            false,
            Box::new(|input| E::OutNeighbours { input }),
        ),
        E::NodeEdges { input } => (input, true, false, Box::new(|input| E::NodeEdges { input })),
        E::InEdges { input } => (input, true, false, Box::new(|input| E::InEdges { input })),
        E::OutEdges { input } => (input, true, false, Box::new(|input| E::OutEdges { input })),
        E::Src { input } => (input, true, false, Box::new(|input| E::Src { input })),
        E::Dst { input } => (input, true, false, Box::new(|input| E::Dst { input })),
        E::Nbr { input } => (input, true, false, Box::new(|input| E::Nbr { input })),
        // Membership ops on a node collection.
        E::TypeFilter { input, node_types } => (
            input,
            false,
            true,
            Box::new(|input| E::TypeFilter {
                input,
                node_types: node_types.clone(),
            }),
        ),
        E::SelectNodes { input, filter } => (
            input,
            false,
            true,
            Box::new(|input| E::SelectNodes {
                input,
                filter: filter.clone(),
            }),
        ),
        E::SortedNodes { input, sort_bys } => (
            input,
            false,
            true,
            Box::new(|input| E::SortedNodes {
                input,
                sort_bys: sort_bys.clone(),
            }),
        ),
        // View / filter ops, plus the edge-collection ops that can only appear
        // above a traversal — all carried over unchanged.
        E::View { input, op } => (
            input,
            false,
            false,
            Box::new(|input| E::View {
                input,
                op: op.clone(),
            }),
        ),
        E::Filtered { input, filter } => (
            input,
            false,
            false,
            Box::new(|input| E::Filtered {
                input,
                filter: filter.clone(),
            }),
        ),
        E::Valid { input } => (input, false, false, Box::new(|input| E::Valid { input })),
        E::Explode { input } => (input, false, false, Box::new(|input| E::Explode { input })),
        E::ExplodeLayers { input } => (
            input,
            false,
            false,
            Box::new(|input| E::ExplodeLayers { input }),
        ),
        E::SelectEdges { input, filter } => (
            input,
            false,
            false,
            Box::new(|input| E::SelectEdges {
                input,
                filter: filter.clone(),
            }),
        ),
        E::SortedEdges { input, sort_bys } => (
            input,
            false,
            false,
            Box::new(|input| E::SortedEdges {
                input,
                sort_bys: sort_bys.clone(),
            }),
        ),
        _ => return None,
    };

    let (inner, in_source_segment) = rebase_at_source(input, anchor, id)?;
    if source_only && in_source_segment {
        return Some((inner, in_source_segment));
    }
    Some((Arc::new(rebuild(inner)), in_source_segment && !is_traversal))
}

/// Sort keys for `SortedNodes`/`SortedEdges` are the server's own input types,
/// re-exported so callers keep using `op::{NodeSortBy, EdgeSortBy, SortByTime}`
/// and a new sort key is defined in exactly one place (`model::sorting`).
pub use crate::model::sorting::{EdgeSortBy, NodeSortBy, SortByTime};

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

/// Arguments for `RemoteGraph::add_node`. `event_id` locks the secondary index
/// explicitly (sent as the `{timestamp, eventId}` time-input object); `None`
/// lets the server auto-increment.
pub struct AddNode {
    pub path: String,
    pub time: InputTime,
    pub id: String,
    pub properties: Option<HashMap<String, Prop>>,
    pub node_type: Option<String>,
    pub layer: Option<String>,
}

/// Arguments for `RemoteGraph::create_node` — maps to the server's `createNode`
/// mutation which fails if the node already exists (vs `addNode` which is
/// upsert-like). `event_id` as in `AddNode`.
pub struct CreateNode {
    pub path: String,
    pub time: InputTime,
    pub id: String,
    pub properties: Option<HashMap<String, Prop>>,
    pub node_type: Option<String>,
    pub layer: Option<String>,
}

/// Arguments for `RemoteGraph::add_edge`. `event_id` as in `AddNode`.
pub struct AddEdge {
    pub path: String,
    pub time: InputTime,
    pub src: String,
    pub dst: String,
    pub properties: Option<HashMap<String, Prop>>,
    pub layer: Option<String>,
}

/// Arguments for `RemoteGraph::add_properties` — adds temporal properties on
/// the graph itself (not on a node/edge). `event_id` locks the secondary index
/// explicitly (sent as the `{timestamp, eventId}` time-input object); `None`
/// lets the server auto-increment.
pub struct AddGraphProperty {
    pub path: String,
    pub time: InputTime,
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
/// given time (optionally on a specific layer). `event_id` as in `AddNode`.
pub struct DeleteEdge {
    pub path: String,
    pub time: InputTime,
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
    pub time: InputTime,
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
    pub time: InputTime,
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
    pub time: InputTime,
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
// These serialize to the schema input shapes (`NodeAddition`/`EdgeAddition`/
// `TemporalPropertyInput`) as JSON variables — camelCase field names, and
// `metadata`/`properties` as `[{key, value}]` where `value` is the `Value`
// @oneOf JSON (produced by `Value`'s own serializer, which rejects non-finite
// floats). Absent optionals serialize as JSON `null`, which GraphQL treats the
// same as omitted for an optional input field.

impl Serialize for TemporalUpdate {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let properties = self
            .properties
            .as_ref()
            .map(properties_to_input)
            .transpose()
            .map_err(serde::ser::Error::custom)?;
        let mut state = serializer.serialize_struct("TemporalPropertyInput", 2)?;
        state.serialize_field("time", &self.time)?;
        state.serialize_field("properties", &properties)?;
        state.end()
    }
}

impl Serialize for NodeAddition {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let metadata = self
            .metadata
            .as_ref()
            .map(properties_to_input)
            .transpose()
            .map_err(serde::ser::Error::custom)?;
        let mut state = serializer.serialize_struct("NodeAddition", 4)?;
        state.serialize_field("name", &self.name)?;
        state.serialize_field("nodeType", &self.node_type)?;
        state.serialize_field("metadata", &metadata)?;
        state.serialize_field("updates", &self.updates)?;
        state.end()
    }
}

impl Serialize for EdgeAddition {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let metadata = self
            .metadata
            .as_ref()
            .map(properties_to_input)
            .transpose()
            .map_err(serde::ser::Error::custom)?;
        let mut state = serializer.serialize_struct("EdgeAddition", 5)?;
        state.serialize_field("src", &self.src)?;
        state.serialize_field("dst", &self.dst)?;
        state.serialize_field("layer", &self.layer)?;
        state.serialize_field("metadata", &metadata)?;
        state.serialize_field("updates", &self.updates)?;
        state.end()
    }
}

#[cfg(test)]
mod handle_ctx_tests {
    use super::*;

    // `ViewOp` is data, not a closure: a recorded chain can be pattern-matched,
    // compared, and printed — and replay applies it in recorded order.
    #[test]
    fn recorded_view_ops_are_inspectable_and_replay_in_order() {
        let ctx = HandleCtx::new(ReadExpr::Root { path: "g".into() })
            .with_op(HandleOp::View(ViewOp::Window {
                start: InputTime::Simple(0),
                end: InputTime::Simple(10),
            }))
            .with_op(HandleOp::View(ViewOp::Layer { name: "a".into() }));

        assert_eq!(ctx.ops.len(), 2);
        assert!(matches!(&ctx.ops[0], HandleOp::View(ViewOp::Window { .. })));
        let HandleOp::View(second) = &ctx.ops[1] else {
            panic!("second op should be a view op");
        };
        assert_eq!(*second, ViewOp::Layer { name: "a".into() });

        // Replay onto a member anchor: ops wrap outward in recorded order, so
        // the LAST-applied op is the outermost tree node.
        let expr = ctx.node_handle_expr("n".into());
        let ReadExpr::View { input, op } = expr else {
            panic!("outermost node should be the last-applied op");
        };
        assert_eq!(op, ViewOp::Layer { name: "a".into() });
        assert!(matches!(
            &*input,
            ReadExpr::View {
                op: ViewOp::Window { .. },
                ..
            }
        ));
    }

    fn ctx() -> HandleCtx {
        HandleCtx::new(ReadExpr::Root { path: "g".into() })
    }

    fn nodes() -> Arc<ReadExpr> {
        Arc::new(ReadExpr::Nodes {
            input: Arc::new(ReadExpr::Root { path: "g".into() }),
        })
    }

    // The source collection becomes a single-node anchor, so the traversal above
    // it yields that one source's own path instead of the whole nested result.
    #[test]
    fn path_handle_expr_reroots_the_traversal_at_one_source() {
        let expr = ReadExpr::Neighbours { input: nodes() };
        let rebased = ctx().path_handle_expr(&expr, "a").expect("re-rootable");

        let ReadExpr::Neighbours { input } = &*rebased else {
            panic!("traversal should be preserved as the outermost node");
        };
        let ReadExpr::Node { id, input } = &**input else {
            panic!("the nodes collection should become a single-node anchor");
        };
        assert_eq!(id, "a");
        assert!(matches!(&**input, ReadExpr::Root { .. }));
    }

    // Op order is load-bearing: a view op applied BEFORE the traversal must stay
    // below it, one applied after must stay above.
    #[test]
    fn path_handle_expr_keeps_ops_on_their_side_of_the_traversal() {
        let window = ViewOp::Window {
            start: InputTime::Simple(0),
            end: InputTime::Simple(10),
        };
        let layer = ViewOp::Layer {
            name: "knows".into(),
        };
        // g.nodes.window(0, 10).neighbours.layer("knows")
        let expr = layer.apply(Arc::new(ReadExpr::Neighbours {
            input: Arc::new(window.apply(nodes())),
        }));
        let rebased = ctx().path_handle_expr(&expr, "a").expect("re-rootable");

        let ReadExpr::View { input, op } = &*rebased else {
            panic!("post-traversal view op should stay outermost");
        };
        assert_eq!(*op, layer);
        let ReadExpr::Neighbours { input } = &**input else {
            panic!("traversal should sit between the two view ops");
        };
        let ReadExpr::View { input, op } = &**input else {
            panic!("pre-traversal view op should stay below the traversal");
        };
        assert_eq!(*op, window);
        assert!(matches!(&**input, ReadExpr::Node { .. }));
    }

    // `typeFilter` below the traversal only decides WHICH sources exist — once a
    // source is pinned it is meaningless, and a single `Node` has no such field.
    // Above the traversal it narrows the path itself, so it is kept.
    #[test]
    fn path_handle_expr_drops_source_membership_ops_only_below_the_traversal() {
        let below = ReadExpr::Neighbours {
            input: Arc::new(ReadExpr::TypeFilter {
                input: nodes(),
                node_types: vec!["ant".to_string()].into(),
            }),
        };
        let rebased = ctx().path_handle_expr(&below, "a").expect("re-rootable");
        let ReadExpr::Neighbours { input } = &*rebased else {
            panic!("traversal should be preserved");
        };
        assert!(
            matches!(&**input, ReadExpr::Node { .. }),
            "typeFilter on the source collection should be dropped"
        );

        let above = ReadExpr::TypeFilter {
            input: Arc::new(ReadExpr::Neighbours { input: nodes() }),
            node_types: vec!["ant".to_string()].into(),
        };
        let rebased = ctx().path_handle_expr(&above, "a").expect("re-rootable");
        assert!(
            matches!(&*rebased, ReadExpr::TypeFilter { .. }),
            "typeFilter on the path should be kept"
        );
    }

    // A chain with no single-source counterpart is refused rather than silently
    // re-rooted onto the wrong thing.
    #[test]
    fn path_handle_expr_refuses_a_chain_it_cannot_reroot() {
        let expr = ReadExpr::Neighbours {
            input: Arc::new(ReadExpr::Edges {
                input: Arc::new(ReadExpr::Root { path: "g".into() }),
            }),
        };
        assert!(ctx().path_handle_expr(&expr, "a").is_none());
    }
}
