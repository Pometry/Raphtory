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

use crate::model::{
    graph::node_id::GqlNodeId,
    sorting::{EdgeSortBy, NodeSortBy},
};
use raphtory::db::graph::views::filter::model::{
    edge_filter::CompositeEdgeFilter, node_filter::CompositeNodeFilter,
};
use raphtory_api::core::storage::timeindex::EventTime;

/// Pagination arguments for `page(limit, offset, pageIndex)`. The first item is
/// `pageIndex * limit + offset`.
#[derive(Debug)]
pub struct Page {
    pub limit: usize,
    pub offset: usize,
    pub page_index: usize,
}

impl Page {
    /// Index of the first item to emit.
    pub fn start(&self) -> usize {
        self.page_index * self.limit + self.offset
    }
}

/// A compiled query: the selection set under the root `graph(path:)` field
/// (which is resolved/loaded asynchronously before execution), plus the response
/// key to emit it under (normally `"graph"`).
#[derive(Debug)]
pub struct Plan {
    pub root_key: Box<str>,
    pub children: Box<[Op]>,
}

/// One node in the plan tree. Each carries its **response key** (the output JSON
/// key — alias or field name) and what to do.
#[derive(Debug)]
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
#[derive(Debug)]
pub enum Nav {
    /// `graph.nodes(select:)` — `Graph` → `Nodes` (optional pushed-down filter)
    Nodes(Option<CompositeNodeFilter>),
    /// `graph.node(name:)` — `Graph` → `Node?`
    Node(GqlNodeId),
    /// `graph.edges(select:)` / `node.edges(select:)` — → `Edges` (optional filter)
    Edges(Option<CompositeEdgeFilter>),
    /// `graph.edge(src:, dst:)` — `Graph` → `Edge?`
    Edge { src: GqlNodeId, dst: GqlNodeId },
    /// `graph.filterNodes(expr:)` — `Graph` → `Graph`
    FilterNodes(CompositeNodeFilter),
    /// `graph.filterEdges(expr:)` — `Graph` → `Graph`
    FilterEdges(CompositeEdgeFilter),
    /// `node.filter` / `nodes.filter`+`select` / `path.filter`+`select` — applies a
    /// node filter to a `Node`/`Nodes`/`PathFromNode`, keeping the same type.
    /// `select` chooses `select(..)` (one-hop) over `filter(..)` (sticky).
    ApplyNodeFilter { filter: CompositeNodeFilter, select: bool },
    /// `edges.filter`+`select` — applies an edge filter to an `Edges`.
    ApplyEdgeFilter { filter: CompositeEdgeFilter, select: bool },
    /// `edge.src` — `Edge` → `Node`
    Src,
    /// `edge.dst` — `Edge` → `Node`
    Dst,
    /// `edge.nbr` — `Edge` → `Node`
    Nbr,
    /// `edge.explode` — `Edge` → `Edges`
    Explode,
    /// `edge.explodeLayers` — `Edge` → `Edges`
    ExplodeLayers,
    /// `edge.deletions` — `Edge` → `History`
    Deletions,
    /// `node.inEdges(select:)` — `Node` → `Edges`
    InEdges(Option<CompositeEdgeFilter>),
    /// `node.outEdges(select:)` — `Node` → `Edges`
    OutEdges(Option<CompositeEdgeFilter>),
    /// `node.inNeighbours(select:)` — `Node` → `PathFromNode`
    InNeighbours(Option<CompositeNodeFilter>),
    /// `node.outNeighbours(select:)` — `Node` → `PathFromNode`
    OutNeighbours(Option<CompositeNodeFilter>),
    /// `node.inComponent` — `Node` → `Nodes`
    InComponent,
    /// `node.outComponent` — `Node` → `Nodes`
    OutComponent,
    /// `earliestTime` — `Graph`/`Node`/`Edge` → `EventTime`
    EarliestTime,
    /// `latestTime` — `Graph`/`Node`/`Edge` → `EventTime`
    LatestTime,
    /// `start` — `Graph`/`Node`/`Edge` → `EventTime`
    Start,
    /// `end` — `Graph`/`Node`/`Edge` → `EventTime`
    End,
    /// `firstUpdate` — `Node`/`Edge` → `EventTime`
    FirstUpdate,
    /// `lastUpdate` — `Node`/`Edge` → `EventTime`
    LastUpdate,
    /// `history` — `Node`/`Edge`/`TemporalProperty` → `History`
    History,
    /// `node.neighbours(select:)` — `Node` → `PathFromNode`
    Neighbours(Option<CompositeNodeFilter>),
    /// `nodes.sorted(sortBys:)` — `Nodes` → `Nodes`
    SortedNodes(Vec<NodeSortBy>),
    /// `edges.sorted(sortBys:)` — `Edges` → `Edges`
    SortedEdges(Vec<EdgeSortBy>),
    /// `properties` — `Node`/`Edge` → `Properties`
    Properties,
    /// `metadata` — `Node`/`Edge` → `Metadata`
    Metadata,
    /// `properties.temporal` — `Properties` → `TemporalProperties`
    Temporal,
    /// `properties.get(key:)` / `metadata.get(key:)` — → `Property?`
    PropGet(Box<str>),
    /// `temporal.get(key:)` — `TemporalProperties` → `TemporalProperty?`
    TemporalGet(Box<str>),
    /// `temporalProperty.min` / `max` / `median` — `TemporalProperty` → `PropertyTuple?`
    TemporalMin,
    TemporalMax,
    TemporalMedian,
    /// `propertyTuple.time` — `PropertyTuple` → `EventTime`
    TupleTime,
    /// A view transform that maps a receiver to the **same** type
    /// (`window`/`at`/`layer`/`subgraph`/…). Dispatched per receiver type in
    /// `exec`. See [`ViewKind`].
    View(ViewKind),
    /// `history.timestamps` — `History` → `HistoryTimestamp`
    Timestamps,
    /// `history.eventId` — `History` → `HistoryEventId`
    EventIds,
    /// `history.datetimes(formatString:)` — `History` → `HistoryDateTime`
    /// (the format string is pre-validated at plan time; `None` means default).
    DateTimes(Option<Box<str>>),
}

/// A same-type view transform. One variant per view op; the planner gates which
/// variant is emitted for which receiver type, and `exec` dispatches per type
/// (each arm calling the matching raphtory view op). Arguments are pre-parsed.
#[derive(Debug)]
pub enum ViewKind {
    // ── time scoping (Graph/Node/Edge) ──
    Window { start: EventTime, end: EventTime },
    At(EventTime),
    Before(EventTime),
    After(EventTime),
    Latest,
    SnapshotAt(EventTime),
    SnapshotLatest,
    ShrinkWindow { start: EventTime, end: EventTime },
    ShrinkStart(EventTime),
    ShrinkEnd(EventTime),
    // ── layer scoping (Graph/Node/Edge) ──
    DefaultLayer,
    Layer(Box<str>),
    Layers(Box<[String]>),
    ExcludeLayer(Box<str>),
    ExcludeLayers(Box<[String]>),
    // ── node-collection-only ──
    /// `typeFilter(nodeTypes:)` — `Nodes`/`PathFromNode` → same type
    TypeFilter(Box<[String]>),
    // ── graph-only structural views ──
    Valid,
    Subgraph(Vec<GqlNodeId>),
    SubgraphNodeTypes(Box<[String]>),
    ExcludeNodes(Vec<GqlNodeId>),
}

/// An iteration step that turns a receiver into a sequence of item receivers.
#[derive(Debug)]
pub enum IterKind {
    /// `nodes.list` — iterate a `Nodes`, item per `Node`.
    NodesList,
    /// `neighbours.list` — iterate a `PathFromNode`, item per `Node`.
    NeighboursList,
    /// `edges.list` — iterate an `Edges`, item per `Edge`.
    EdgesList,
    /// `nodes.page(...)` — a paginated window of `Nodes`.
    NodesPage(Page),
    /// `edges.page(...)` — a paginated window of `Edges`.
    EdgesPage(Page),
    /// `neighbours.page(...)` — a paginated window of `PathFromNode`.
    NeighboursPage(Page),
    /// `history.list` — iterate a `History`, item per `EventTime`.
    HistoryList,
    /// `properties.values(keys:)` — item per `Property` (optional key whitelist).
    PropertiesValues(Option<Box<[String]>>),
    /// `metadata.values(keys:)` — item per `Property` (optional key whitelist).
    MetadataValues(Option<Box<[String]>>),
    /// `temporal.values(keys:)` — item per `TemporalProperty` (optional key whitelist).
    TemporalValues(Option<Box<[String]>>),
    /// `temporalProperty.orderedDedupe(latestTime:)` — item per `PropertyTuple`.
    OrderedDedupe(bool),
}

/// A scalar leaf read from the current receiver.
#[derive(Debug)]
pub enum LeafKind {
    /// `nodes`/`edges`/`neighbours` `.count` — `Int`
    Count,
    /// `node.id` — `NodeId` (string or non-negative int)
    Id,
    /// `node.name` — `String`
    Name,
    /// `edge.id` — `[NodeId!]!` (the `[src, dst]` id pair)
    EdgeId,
    /// `eventTime.timestamp` — `Int`
    Timestamp,
    /// `eventTime.eventId` — `Int`
    EventId,
    /// `property.key` / `temporalProperty.key` — `String`
    Key,
    /// `property.asString` — `String`
    AsString,
    /// `property.value` / `propertyTuple.value` — `PropertyOutput` (a typed scalar)
    Value,
    /// `temporalProperty.values` / `unique` — `[PropertyOutput!]!` (flat scalar array)
    TemporalValueList,
    TemporalUniqueList,
    /// `temporalProperty.latest` / `sum` / `mean` / `average` — nullable `PropertyOutput`
    TemporalLatest,
    TemporalSum,
    TemporalMean,
    TemporalAverage,
    /// `temporalProperty.at(t:)` — nullable `PropertyOutput`
    TemporalAt(EventTime),
    /// `temporalProperty.count` — `Int`
    TemporalCount,
    /// `historyTimestamp.list` — `[Int!]!` (a flat array of timestamps)
    TimestampList,
    /// `historyEventId.list` — `[Int!]!` (a flat array of event ids)
    EventIdList,
    /// `historyDateTime.list` — `[String!]!` (a flat array of formatted datetimes)
    DateTimeList,
    /// `eventTime.datetime(formatString:)` — `String` (format pre-validated at plan time)
    DateTime(Box<str>),
    /// `node.nodeType` — `String` (nullable)
    NodeType,
    /// `node.degree` / `inDegree` / `outDegree` / `edgeHistoryCount` — `Int`
    Degree,
    InDegree,
    OutDegree,
    EdgeHistoryCount,
    /// `isActive` (Node/Edge) / `isValid` / `isDeleted` / `isSelfLoop` (Edge) — `Boolean`
    IsActive,
    IsValid,
    IsDeleted,
    IsSelfLoop,
    /// `edge.layerNames` / `graph.uniqueLayers` — `[String!]!`
    LayerNames,
    UniqueLayers,
    /// `graph.countNodes` / `countEdges` / `countTemporalEdges` — `Int`
    CountNodes,
    CountEdges,
    CountTemporalEdges,
    /// `graph.hasNode(name:)` — `Boolean`
    HasNode(GqlNodeId),
    /// `graph.hasEdge(src:, dst:, layer:)` — `Boolean`
    HasEdge {
        src: GqlNodeId,
        dst: GqlNodeId,
        layer: Option<Box<str>>,
    },
}
