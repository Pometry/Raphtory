use crate::model::graph::{property::Value, timeindex::GqlTimeInput};
use async_graphql::dynamic::ValueAccessor;
use dynamic_graphql::{
    internal::{
        FromValue, GetInputTypeRef, InputTypeName, InputValueResult, Register, Registry, TypeName,
    },
    Enum, InputObject, OneOfInput,
};
use raphtory::{
    db::graph::views::filter::model::{
        edge_filter::{CompositeEdgeFilter, EdgeFilter},
        filter::{Filter, FilterValue},
        filter_operator::FilterOperator,
        graph_filter::GraphFilter,
        is_active_edge_filter::IsActiveEdge,
        is_active_node_filter::IsActiveNode,
        is_deleted_filter::IsDeletedEdge,
        is_self_loop_filter::IsSelfLoopEdge,
        is_valid_filter::IsValidEdge,
        latest_filter::Latest as LatestWrap,
        layered_filter::Layered,
        node_filter::{CompositeNodeFilter, NodeFilter},
        property_filter::{Op, PropertyFilter, PropertyFilterValue, PropertyRef},
        snapshot_filter::{SnapshotAt as SnapshotAtWrap, SnapshotLatest as SnapshotLatestWrap},
        windowed_filter::Windowed,
        DynView, ViewWrapOps,
    },
    errors::GraphError,
};
use raphtory_api::core::{
    entities::{properties::prop::Prop, Layer, GID},
    storage::timeindex::{AsTime, EventTime},
};
use std::{
    borrow::Cow,
    collections::HashSet,
    fmt,
    fmt::{Display, Formatter},
    ops::Deref,
    sync::Arc,
};

#[derive(InputObject, Clone, Debug)]
pub struct Window {
    /// Window start time.
    pub start: GqlTimeInput,
    /// Window end time.
    pub end: GqlTimeInput,
}

#[derive(OneOfInput, Clone, Debug)]
pub enum GraphViewCollection {
    /// Contains only the default layer.
    DefaultLayer(bool),
    /// List of included layers.
    Layers(Vec<String>),
    /// List of excluded layers.
    ExcludeLayers(Vec<String>),
    /// Single excluded layer.
    ExcludeLayer(String),
    /// Subgraph nodes.
    Subgraph(Vec<String>),
    /// Subgraph node types.
    SubgraphNodeTypes(Vec<String>),
    /// List of excluded nodes.
    ExcludeNodes(Vec<String>),
    /// Valid state.
    Valid(bool),
    /// Window between a start and end time.
    Window(Window),
    /// View at a specified time.
    At(GqlTimeInput),
    /// View at the latest time.
    Latest(bool),
    /// Snapshot at specified time.
    SnapshotAt(GqlTimeInput),
    /// Snapshot at latest time.
    SnapshotLatest(bool),
    /// View before a specified time (end exclusive).
    Before(GqlTimeInput),
    /// View after a specified time (start exclusive).
    After(GqlTimeInput),
    /// Shrink a Window to a specified start and end time.
    ShrinkWindow(Window),
    /// Set the window start to a specified time.
    ShrinkStart(GqlTimeInput),
    /// Set the window end to a specified time.
    ShrinkEnd(GqlTimeInput),
    /// Node filter.
    NodeFilter(GqlNodeFilter),
    /// Edge filter.
    EdgeFilter(GqlEdgeFilter),
}

#[derive(OneOfInput, Clone, Debug)]
pub enum NodesViewCollection {
    /// Contains only the default layer.
    DefaultLayer(bool),
    /// View at the latest time.
    Latest(bool),
    /// Snapshot at latest time.
    SnapshotLatest(bool),
    /// List of included layers.
    Layers(Vec<String>),
    /// List of excluded layers.
    ExcludeLayers(Vec<String>),
    /// Single excluded layer.
    ExcludeLayer(String),
    /// Window between a start and end time.
    Window(Window),
    /// View at a specified time.
    At(GqlTimeInput),
    /// Snapshot at specified time.
    SnapshotAt(GqlTimeInput),
    /// View before a specified time (end exclusive).
    Before(GqlTimeInput),
    /// View after a specified time (start exclusive).
    After(GqlTimeInput),
    /// Shrink a Window to a specified start and end time.
    ShrinkWindow(Window),
    /// Set the window start to a specified time.
    ShrinkStart(GqlTimeInput),
    /// Set the window end to a specified time.
    ShrinkEnd(GqlTimeInput),
    /// Node filter.
    NodeFilter(GqlNodeFilter),
    /// List of types.
    TypeFilter(Vec<String>),
}

#[derive(OneOfInput, Clone, Debug)]
pub enum NodeViewCollection {
    /// Contains only the default layer.
    DefaultLayer(bool),
    /// View at the latest time.
    Latest(bool),
    /// Snapshot at latest time.
    SnapshotLatest(bool),
    /// Snapshot at specified time.
    SnapshotAt(GqlTimeInput),
    /// List of included layers.
    Layers(Vec<String>),
    /// List of excluded layers.
    ExcludeLayers(Vec<String>),
    /// Single excluded layer.
    ExcludeLayer(String),
    /// Window between a start and end time.
    Window(Window),
    /// View at a specified time.
    At(GqlTimeInput),
    /// View before a specified time (end exclusive).
    Before(GqlTimeInput),
    /// View after a specified time (start exclusive).
    After(GqlTimeInput),
    /// Shrink a Window to a specified start and end time.
    ShrinkWindow(Window),
    /// Set the window start to a specified time.
    ShrinkStart(GqlTimeInput),
    /// Set the window end to a specified time.
    ShrinkEnd(GqlTimeInput),
    /// Node filter.
    NodeFilter(GqlNodeFilter),
}

#[derive(OneOfInput, Clone, Debug)]
pub enum EdgesViewCollection {
    /// Contains only the default layer.
    DefaultLayer(bool),
    /// Latest time.
    Latest(bool),
    /// Snapshot at latest time.
    SnapshotLatest(bool),
    /// Snapshot at specified time.
    SnapshotAt(GqlTimeInput),
    /// List of included layers.
    Layers(Vec<String>),
    /// List of excluded layers.
    ExcludeLayers(Vec<String>),
    /// Single excluded layer.
    ExcludeLayer(String),
    /// Window between a start and end time.
    Window(Window),
    /// View at a specified time.
    At(GqlTimeInput),
    /// View before a specified time (end exclusive).
    Before(GqlTimeInput),
    /// View after a specified time (start exclusive).
    After(GqlTimeInput),
    /// Shrink a Window to a specified start and end time.
    ShrinkWindow(Window),
    /// Set the window start to a specified time.
    ShrinkStart(GqlTimeInput),
    /// Set the window end to a specified time.
    ShrinkEnd(GqlTimeInput),
    /// Edge filter
    EdgeFilter(GqlEdgeFilter),
}

#[derive(OneOfInput, Clone, Debug)]
pub enum EdgeViewCollection {
    /// Contains only the default layer.
    DefaultLayer(bool),
    /// Latest time.
    Latest(bool),
    /// Snapshot at latest time.
    SnapshotLatest(bool),
    /// Snapshot at specified time.
    SnapshotAt(GqlTimeInput),
    /// List of included layers.
    Layers(Vec<String>),
    /// List of excluded layers.
    ExcludeLayers(Vec<String>),
    /// Single excluded layer.
    ExcludeLayer(String),
    /// Window between a start and end time.
    Window(Window),
    /// View at a specified time.
    At(GqlTimeInput),
    /// View before a specified time (end exclusive).
    Before(GqlTimeInput),
    /// View after a specified time (start exclusive).
    After(GqlTimeInput),
    /// Shrink a Window to a specified start and end time.
    ShrinkWindow(Window),
    /// Set the window start to a specified time.
    ShrinkStart(GqlTimeInput),
    /// Set the window end to a specified time.
    ShrinkEnd(GqlTimeInput),
    /// Edge filter
    EdgeFilter(GqlEdgeFilter),
}

#[derive(OneOfInput, Clone, Debug)]
pub enum PathFromNodeViewCollection {
    /// Latest time.
    Latest(bool),
    /// Latest snapshot.
    SnapshotLatest(bool),
    /// Time.
    SnapshotAt(GqlTimeInput),
    /// List of layers.
    Layers(Vec<String>),
    /// List of excluded layers.
    ExcludeLayers(Vec<String>),
    /// Single layer to exclude.
    ExcludeLayer(String),
    /// Window between a start and end time.
    Window(Window),
    /// View at a specified time.
    At(GqlTimeInput),
    /// View before a specified time (end exclusive).
    Before(GqlTimeInput),
    /// View after a specified time (start exclusive).
    After(GqlTimeInput),
    /// Shrink a Window to a specified start and end time.
    ShrinkWindow(Window),
    /// Set the window start to a specified time.
    ShrinkStart(GqlTimeInput),
    /// Set the window end to a specified time.
    ShrinkEnd(GqlTimeInput),
}

#[derive(Enum, Copy, Clone, Debug)]
pub enum NodeField {
    /// Node ID field.
    ///
    /// Represents the graph’s node identifier (numeric or string-backed in the API).
    NodeId,
    /// Node name field.
    ///
    /// Represents the human-readable node name (string).
    NodeName,
    /// Node type field.
    ///
    /// Represents the optional node type assigned at node creation (string).
    NodeType,
}

impl Display for NodeField {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                NodeField::NodeId => "node_id",
                NodeField::NodeName => "node_name",
                NodeField::NodeType => "node_type",
            }
        )
    }
}

/// Filters an entity property or metadata field by name and condition.
///
/// This input is used by both node and edge filters when targeting
/// a specific property key (or metadata key) and applying a `PropCondition`.
///
/// Fields:
/// - `name`: The property key to query.
/// - `where_`: The condition to apply to that property’s value.
///
/// Example (GraphQL):
/// ```graphql
/// { Property: { name: "weight", where: { Gt: 0.5 } } }
/// ```
#[derive(InputObject, Clone, Debug)]
pub struct PropertyFilterNew {
    /// Property (or metadata) key.
    pub name: String,
    /// Condition applied to the property value.
    ///
    /// Exposed as `where` in GraphQL.
    #[graphql(name = "where")]
    pub where_: PropCondition,
}

/// Boolean expression over a property value.
///
/// `PropCondition` is used inside `PropertyFilterNew.where` to describe
/// how a property’s value should be matched.
///
/// It supports:
/// - comparisons (`Eq`, `Gt`, `Le`, …),
/// - string predicates (`Contains`, `StartsWith`, …),
/// - set membership (`IsIn`, `IsNotIn`),
/// - presence checks (`IsSome`, `IsNone`),
/// - boolean composition (`And`, `Or`, `Not`),
/// - and list/aggregate qualifiers (`First`, `Sum`, `Len`, …).
///
/// Notes:
/// - `Value` is interpreted according to the property’s type.
/// - Aggregators/qualifiers like `Sum` and `Len` apply when the underlying
///   property is list-like or aggregatable (depending on your engine rules).
#[derive(OneOfInput, Clone, Debug)]
pub enum PropCondition {
    /// Equality: property value equals the given value.
    Eq(Value),
    /// Inequality: property value does not equal the given value.
    Ne(Value),
    /// Greater-than: property value is greater than the given value.
    Gt(Value),
    /// Greater-than-or-equal: property value is >= the given value.
    Ge(Value),
    /// Less-than: property value is less than the given value.
    Lt(Value),
    /// Less-than-or-equal: property value is <= the given value.
    Le(Value),

    /// String prefix match against the property's string representation.
    StartsWith(Value),
    /// String suffix match against the property's string representation.
    EndsWith(Value),
    /// Substring match against the property's string representation.
    Contains(Value),
    /// Negated substring match against the property's string representation.
    NotContains(Value),

    /// Set membership: property value is contained in the given list of values.
    IsIn(Value),
    /// Negated set membership: property value is not contained in the given list of values.
    IsNotIn(Value),

    /// Presence check: property value is present (not null/missing).
    ///
    /// When set to `true`, requires the property to exist.
    IsSome(bool),
    /// Absence check: property value is missing / null.
    ///
    /// When set to `true`, requires the property to be missing.
    IsNone(bool),

    /// Logical AND over nested conditions.
    And(Vec<PropCondition>),
    /// Logical OR over nested conditions.
    Or(Vec<PropCondition>),
    /// Logical NOT over a nested condition.
    Not(Wrapped<PropCondition>),

    /// Applies the nested condition to the **first** element of a list-like property.
    First(Wrapped<PropCondition>),
    /// Applies the nested condition to the **last** element of a list-like property.
    Last(Wrapped<PropCondition>),
    /// Requires that **any** element of a list-like property matches the nested condition.
    Any(Wrapped<PropCondition>),
    /// Requires that **all** elements of a list-like property match the nested condition.
    All(Wrapped<PropCondition>),

    /// Applies the nested condition to the **sum** of a numeric list-like property.
    Sum(Wrapped<PropCondition>),
    /// Applies the nested condition to the **average** of a numeric list-like property.
    Avg(Wrapped<PropCondition>),
    /// Applies the nested condition to the **minimum** element of a list-like property.
    Min(Wrapped<PropCondition>),
    /// Applies the nested condition to the **maximum** element of a list-like property.
    Max(Wrapped<PropCondition>),
    /// Applies the nested condition to the **length** of a list-like property.
    Len(Wrapped<PropCondition>),
}

impl PropCondition {
    pub fn op_name(&self) -> &'static str {
        use PropCondition::*;
        match self {
            Eq(_) => "eq",
            Ne(_) => "ne",
            Gt(_) => "gt",
            Ge(_) => "ge",
            Lt(_) => "lt",
            Le(_) => "le",

            StartsWith(_) => "startsWith",
            EndsWith(_) => "endsWith",
            Contains(_) => "contains",
            NotContains(_) => "notContains",

            IsIn(_) => "isIn",
            IsNotIn(_) => "isNotIn",

            IsSome(_) => "isSome",
            IsNone(_) => "isNone",

            And(_) => "and",
            Or(_) => "or",
            Not(_) => "not",

            First(_) => "first",
            Last(_) => "last",
            Any(_) => "any",
            All(_) => "all",

            Sum(_) => "sum",
            Avg(_) => "avg",
            Min(_) => "min",
            Max(_) => "max",
            Len(_) => "len",
        }
    }
}

/// Graph view restriction to a time window, optionally chaining another `GraphFilter`.
///
/// Used by `GqlGraphFilter::Window`.
///
/// - `start` and `end` define the window (inclusive start, exclusive end).
/// - `expr` optionally nests another graph filter to apply *within* this window.
///
/// Example (GraphQL):
/// ```graphql
/// { Window: { start: 0, end: 10, expr: { Layers: { names: ["A"] } } } }
/// ```
#[derive(InputObject, Clone, Debug)]
pub struct GraphWindowExpr {
    /// Window start time (inclusive).
    pub start: GqlTimeInput,
    /// Window end time (exclusive).
    pub end: GqlTimeInput,
    /// Optional nested filter applied after the window restriction.
    pub expr: Option<Wrapped<GqlGraphFilter>>,
}

/// Graph view restriction to a single time bound, optionally chaining another `GraphFilter`.
///
/// Used by `At`, `Before`, and `After` graph filters.
///
/// Example:
/// `{ At: { time: 5, expr: { Layers: { names: ["L1"] } } } }`
#[derive(InputObject, Clone, Debug)]
pub struct GraphTimeExpr {
    /// Reference time for the operation.
    pub time: GqlTimeInput,
    /// Optional nested filter applied after the time restriction.
    pub expr: Option<Wrapped<GqlGraphFilter>>,
}

/// Graph view restriction that takes only a nested expression.
///
/// Used for unary view operations like `Latest` and `SnapshotLatest`.
#[derive(InputObject, Clone, Debug)]
pub struct GraphUnaryExpr {
    /// Optional nested filter applied after the unary operation.
    pub expr: Option<Wrapped<GqlGraphFilter>>,
}

/// Graph view restriction by layer membership, optionally chaining another `GraphFilter`.
///
/// Used by `GqlGraphFilter::Layers`.
#[derive(InputObject, Clone, Debug)]
pub struct GraphLayersExpr {
    /// Layer names to include.
    pub names: Vec<String>,
    /// Optional nested filter applied after the layer restriction.
    pub expr: Option<Wrapped<GqlGraphFilter>>,
}

/// GraphQL input type for restricting a graph view.
///
/// `GraphFilter` controls the **evaluation scope** for subsequent node/edge filters:
/// - time windows (`Window`)
/// - time points (`At`)
/// - open-ended ranges (`Before`, `After`)
/// - latest evaluation (`Latest`)
/// - snapshots (`SnapshotAt`, `SnapshotLatest`)
/// - layer membership (`Layers`)
///
/// These filters can be nested via the `expr` field on the corresponding
/// `*Expr` input objects to form pipelines.
#[derive(OneOfInput, Clone, Debug)]
#[graphql(name = "GraphFilter")]
pub enum GqlGraphFilter {
    /// Restrict evaluation to a time window (inclusive start, exclusive end).
    Window(GraphWindowExpr),
    /// Restrict evaluation to a single point in time.
    At(GraphTimeExpr),
    /// Restrict evaluation to times strictly before the given time.
    Before(GraphTimeExpr),
    /// Restrict evaluation to times strictly after the given time.
    After(GraphTimeExpr),

    /// Evaluate against the latest available state.
    Latest(GraphUnaryExpr),
    /// Evaluate against a snapshot of the graph at a given time.
    SnapshotAt(GraphTimeExpr),
    /// Evaluate against the most recent snapshot of the graph.
    SnapshotLatest(GraphUnaryExpr),

    /// Restrict evaluation to one or more layers.
    Layers(GraphLayersExpr),
}

/// Boolean expression over a built-in node field (ID, name, or type).
///
/// This is used by `NodeFieldFilterNew.where_` when filtering a specific
/// `NodeField`.
///
/// Supports comparisons, string predicates, and set membership.
/// (Presence checks and aggregations are handled via property filters instead.)
#[derive(OneOfInput, Clone, Debug)]
pub enum NodeFieldCondition {
    /// Equality.
    Eq(Value),
    /// Inequality.
    Ne(Value),
    /// Greater-than.
    Gt(Value),
    /// Greater-than-or-equal.
    Ge(Value),
    /// Less-than.
    Lt(Value),
    /// Less-than-or-equal.
    Le(Value),

    /// String prefix match.
    StartsWith(Value),
    /// String suffix match.
    EndsWith(Value),
    /// Substring match.
    Contains(Value),
    /// Negated substring match.
    NotContains(Value),

    /// Set membership.
    IsIn(Value),
    /// Negated set membership.
    IsNotIn(Value),
}

impl NodeFieldCondition {
    pub fn op_name(&self) -> &'static str {
        use NodeFieldCondition::*;
        match self {
            Eq(_) => "eq",
            Ne(_) => "ne",
            Gt(_) => "gt",
            Ge(_) => "ge",
            Lt(_) => "lt",
            Le(_) => "le",
            StartsWith(_) => "startsWith",
            EndsWith(_) => "endsWith",
            Contains(_) => "contains",
            NotContains(_) => "notContains",
            IsIn(_) => "isIn",
            IsNotIn(_) => "isNotIn",
        }
    }
}

/// Filters a built-in node field (`id`, `name`, `type`) using a `NodeFieldCondition`.
///
/// Example (GraphQL):
/// ```graphql
/// { Node: { field: NodeName, where: { Contains: "ali" } } }
/// ```
#[derive(InputObject, Clone, Debug)]
pub struct NodeFieldFilterNew {
    /// Which built-in field to filter.
    pub field: NodeField,
    /// Condition applied to the selected field.
    ///
    /// Exposed as `where` in GraphQL.
    #[graphql(name = "where")]
    pub where_: NodeFieldCondition,
}

/// Restricts node evaluation to a time window and applies a nested `NodeFilter`.
///
/// Used by `GqlNodeFilter::Window`.
///
/// The window is inclusive of `start` and exclusive of `end`.
#[derive(InputObject, Clone, Debug)]
pub struct NodeWindowExpr {
    /// Window start time (inclusive).
    pub start: GqlTimeInput,
    /// Window end time (exclusive).
    pub end: GqlTimeInput,
    /// Filter evaluated within the restricted window.
    pub expr: Wrapped<GqlNodeFilter>,
}

/// Restricts node evaluation to a single time bound and applies a nested `NodeFilter`.
///
/// Used by `At`, `Before`, and `After` node filters.
#[derive(InputObject, Clone, Debug)]
pub struct NodeTimeExpr {
    /// Reference time for the operation.
    pub time: GqlTimeInput,
    /// Filter evaluated within the restricted time scope.
    pub expr: Wrapped<GqlNodeFilter>,
}

/// Applies a unary node-view operation and then evaluates a nested `NodeFilter`.
///
/// Used by `Latest` and `SnapshotLatest` node filters.
#[derive(InputObject, Clone, Debug)]
pub struct NodeUnaryExpr {
    /// Filter evaluated after applying the unary operation.
    pub expr: Wrapped<GqlNodeFilter>,
}

/// Restricts node evaluation to one or more layers and applies a nested `NodeFilter`.
///
/// Used by `GqlNodeFilter::Layers`.
#[derive(InputObject, Clone, Debug)]
pub struct NodeLayersExpr {
    /// Layer names to include.
    pub names: Vec<String>,
    /// Filter evaluated within the layer-restricted view.
    pub expr: Wrapped<GqlNodeFilter>,
}

/// GraphQL input type for filtering nodes.
///
/// `NodeFilter` represents a composable boolean expression evaluated
/// against nodes in a graph. Filters can target:
///
/// - built-in node fields (`Node` / `NodeFieldFilterNew`),
/// - node properties and metadata,
/// - temporal properties,
/// - temporal scope (windows, snapshots, latest),
/// - and layer membership,
/// - plus node state predicates (e.g. `IsActive`).
///
/// Filters can be combined recursively using logical operators
/// (`And`, `Or`, `Not`).
#[derive(OneOfInput, Clone, Debug)]
#[graphql(name = "NodeFilter")]
pub enum GqlNodeFilter {
    /// Filters a built-in node field (ID, name, or type).
    Node(NodeFieldFilterNew),

    /// Filters a node property by name and condition.
    Property(PropertyFilterNew),

    /// Filters a node metadata field by name and condition.
    ///
    /// Metadata is shared across all temporal versions of a node.
    Metadata(PropertyFilterNew),

    /// Filters a temporal node property by name and condition.
    ///
    /// Used when the property value varies over time and must be evaluated
    /// within a temporal context.
    TemporalProperty(PropertyFilterNew),

    /// Logical AND over multiple node filters.
    And(Vec<GqlNodeFilter>),

    /// Logical OR over multiple node filters.
    Or(Vec<GqlNodeFilter>),

    /// Logical NOT over a nested node filter.
    Not(Wrapped<GqlNodeFilter>),

    /// Restricts evaluation to a time window (inclusive start, exclusive end).
    Window(NodeWindowExpr),
    /// Restricts evaluation to a single point in time.
    At(NodeTimeExpr),
    /// Restricts evaluation to times strictly before the given time.
    Before(NodeTimeExpr),
    /// Restricts evaluation to times strictly after the given time.
    After(NodeTimeExpr),
    /// Evaluates predicates against the latest available node state.
    Latest(NodeUnaryExpr),
    /// Evaluates predicates against a snapshot of the graph at a given time.
    SnapshotAt(NodeTimeExpr),
    /// Evaluates predicates against the most recent snapshot of the graph.
    SnapshotLatest(NodeUnaryExpr),
    /// Restricts evaluation to nodes belonging to one or more layers.
    Layers(NodeLayersExpr),

    /// Matches nodes that have at least one event in the current view/window.
    ///
    /// When `true`, only active nodes are matched.
    IsActive(bool),
}

/// Restricts edge evaluation to a time window and applies a nested `EdgeFilter`.
///
/// Used by `GqlEdgeFilter::Window`.
///
/// The window is inclusive of `start` and exclusive of `end`.
#[derive(InputObject, Clone, Debug)]
pub struct EdgeWindowExpr {
    /// Window start time (inclusive).
    pub start: GqlTimeInput,
    /// Window end time (exclusive).
    pub end: GqlTimeInput,
    /// Filter evaluated within the restricted window.
    pub expr: Wrapped<GqlEdgeFilter>,
}

/// Restricts edge evaluation to a single time bound and applies a nested `EdgeFilter`.
///
/// Used by `At`, `Before`, and `After` edge filters.
#[derive(InputObject, Clone, Debug)]
pub struct EdgeTimeExpr {
    /// Reference time for the operation.
    pub time: GqlTimeInput,
    /// Filter evaluated within the restricted time scope.
    pub expr: Wrapped<GqlEdgeFilter>,
}

/// Applies a unary edge-view operation and then evaluates a nested `EdgeFilter`.
///
/// Used by `Latest` and `SnapshotLatest` edge filters.
#[derive(InputObject, Clone, Debug)]
pub struct EdgeUnaryExpr {
    /// Filter evaluated after applying the unary operation.
    pub expr: Wrapped<GqlEdgeFilter>,
}

/// Restricts edge evaluation to one or more layers and applies a nested `EdgeFilter`.
///
/// Used by `GqlEdgeFilter::Layers`.
#[derive(InputObject, Clone, Debug)]
pub struct EdgeLayersExpr {
    /// Layer names to include.
    pub names: Vec<String>,
    /// Filter evaluated within the layer-restricted view.
    pub expr: Wrapped<GqlEdgeFilter>,
}

/// GraphQL input type for filtering edges.
///
/// `EdgeFilter` represents a composable boolean expression evaluated
/// against edges in a graph. Filters can target:
///
/// - edge **endpoints** (source / destination nodes),
/// - edge **properties** and **metadata**,
/// - **temporal scope** (windows, snapshots, latest),
/// - **layer membership**,
/// - and **structural edge state** (active, valid, deleted, self-loop).
///
/// Filters can be combined recursively using logical operators
/// (`And`, `Or`, `Not`).
///
/// Examples (GraphQL):
/// ```graphql
/// {
///   edges(filter: {
///     And: [
///       { IsActive: true },
///       { Property: { name: "weight", gt: 0.5 } }
///     ]
///   }) {
///     src
///     dst
///   }
/// }
/// ```
#[derive(OneOfInput, Clone, Debug)]
#[graphql(name = "EdgeFilter")]
pub enum GqlEdgeFilter {
    /// Applies a filter to the **source node** of the edge.
    ///
    /// The nested `NodeFilter` is evaluated against the source endpoint.
    ///
    /// Example:
    /// `{ Src: { Name: { contains: "alice" } } }`
    Src(Wrapped<GqlNodeFilter>),

    /// Applies a filter to the **destination node** of the edge.
    ///
    /// The nested `NodeFilter` is evaluated against the destination endpoint.
    ///
    /// Example:
    /// `{ Dst: { Id: { eq: 42 } } }`
    Dst(Wrapped<GqlNodeFilter>),

    /// Filters an edge **property** by name and value.
    ///
    /// Applies to static or temporal properties depending on context.
    ///
    /// Example:
    /// `{ Property: { name: "weight", gt: 0.5 } }`
    Property(PropertyFilterNew),

    /// Filters an edge **metadata field**.
    ///
    /// Metadata is shared across all temporal versions of an edge.
    ///
    /// Example:
    /// `{ Metadata: { name: "source", eq: "imported" } }`
    Metadata(PropertyFilterNew),

    /// Filters a **temporal edge property**.
    ///
    /// Used when the property value varies over time and must be
    /// evaluated within a temporal context.
    ///
    /// Example:
    /// `{ TemporalProperty: { name: "status", eq: "active" } }`
    TemporalProperty(PropertyFilterNew),

    /// Logical **AND** over multiple edge filters.
    ///
    /// All nested filters must evaluate to `true`.
    ///
    /// Example:
    /// `{ And: [ { IsActive: true }, { IsValid: true } ] }`
    And(Vec<GqlEdgeFilter>),

    /// Logical **OR** over multiple edge filters.
    ///
    /// At least one nested filter must evaluate to `true`.
    ///
    /// Example:
    /// `{ Or: [ { IsDeleted: true }, { IsSelfLoop: true } ] }`
    Or(Vec<GqlEdgeFilter>),

    /// Logical **NOT** over a nested edge filter.
    ///
    /// Negates the result of the wrapped filter.
    ///
    /// Example:
    /// `{ Not: { IsDeleted: true } }`
    Not(Wrapped<GqlEdgeFilter>),

    /// Restricts edge evaluation to a **time window**.
    ///
    /// The window is inclusive of `start` and exclusive of `end`.
    Window(EdgeWindowExpr),

    /// Restricts edge evaluation to a **single point in time**.
    At(EdgeTimeExpr),

    /// Restricts edge evaluation to times **strictly before** a given time.
    Before(EdgeTimeExpr),

    /// Restricts edge evaluation to times **strictly after** a given time.
    After(EdgeTimeExpr),

    /// Evaluates edge predicates against the **latest available state**.
    Latest(EdgeUnaryExpr),

    /// Evaluates edge predicates against a **snapshot** of the graph
    /// at a specific time.
    SnapshotAt(EdgeTimeExpr),

    /// Evaluates edge predicates against the **most recent snapshot**
    /// of the graph.
    SnapshotLatest(EdgeUnaryExpr),

    /// Restricts evaluation to edges belonging to one or more **layers**.
    ///
    /// Example:
    /// `{ Layers: { values: ["fire_nation", "air_nomads"] } }`
    Layers(EdgeLayersExpr),

    /// Matches edges that have at least one event in the current view/window.
    ///
    /// When `true`, only active edges are matched.
    IsActive(bool),

    /// Matches edges that are structurally valid (i.e. not deleted)
    /// in the current view/window.
    IsValid(bool),

    /// Matches edges that have been deleted in the current view/window.
    IsDeleted(bool),

    /// Matches edges that are **self-loops**
    /// (source node == destination node).
    IsSelfLoop(bool),
}

#[derive(Clone, Debug)]
pub struct Wrapped<T>(Box<T>);
impl<T> Deref for Wrapped<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl<T: Register + 'static> Register for Wrapped<T> {
    fn register(registry: Registry) -> Registry {
        registry.register::<T>()
    }
}

impl<T: FromValue + GetInputTypeRef + InputTypeName + 'static> FromValue for Wrapped<T> {
    fn from_value(value: async_graphql::Result<ValueAccessor>) -> InputValueResult<Self> {
        T::from_value(value)
            .map(|v| Wrapped(Box::new(v)))
            .map_err(|e| e.propagate())
    }
}

impl<T: TypeName + 'static> TypeName for Wrapped<T> {
    fn get_type_name() -> Cow<'static, str> {
        T::get_type_name()
    }
}
impl<T: InputTypeName + 'static> InputTypeName for Wrapped<T> {}

fn peel_prop_wrappers_and_collect_ops<'a>(
    cond: &'a PropCondition,
    ops: &mut Vec<Op>,
) -> Option<&'a PropCondition> {
    use PropCondition::*;

    match cond {
        First(inner) => {
            ops.push(Op::First);
            Some(inner.deref())
        }
        Last(inner) => {
            ops.push(Op::Last);
            Some(inner.deref())
        }
        Any(inner) => {
            ops.push(Op::Any);
            Some(inner.deref())
        }
        All(inner) => {
            ops.push(Op::All);
            Some(inner.deref())
        }
        Sum(inner) => {
            ops.push(Op::Sum);
            Some(inner.deref())
        }
        Avg(inner) => {
            ops.push(Op::Avg);
            Some(inner.deref())
        }
        Min(inner) => {
            ops.push(Op::Min);
            Some(inner.deref())
        }
        Max(inner) => {
            ops.push(Op::Max);
            Some(inner.deref())
        }
        Len(inner) => {
            ops.push(Op::Len);
            Some(inner.deref())
        }
        _ => None,
    }
}

fn require_string_value(op: &str, v: &Value) -> Result<String, GraphError> {
    if let Value::Str(s) = v {
        Ok(s.clone())
    } else {
        Err(GraphError::InvalidGqlFilter(format!(
            "{op} requires a string value, got {v}"
        )))
    }
}

fn require_prop_list_value(op: &str, v: &Value) -> Result<PropertyFilterValue, GraphError> {
    if let Value::List(vs) = v {
        let props = vs
            .iter()
            .cloned()
            .map(Prop::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(PropertyFilterValue::Set(Arc::new(
            props.into_iter().collect(),
        )))
    } else {
        Err(GraphError::InvalidGqlFilter(format!(
            "{op} requires a list value, got {v}"
        )))
    }
}

fn require_u64_value(op: &str, v: &Value) -> Result<u64, GraphError> {
    if let Value::U64(i) = v {
        Ok(*i)
    } else {
        Err(GraphError::InvalidGqlFilter(format!(
            "{op} requires a u64 value, got {v}"
        )))
    }
}

fn parse_node_id_scalar(op: &str, v: &Value) -> Result<FilterValue, GraphError> {
    match v {
        Value::U64(i) => Ok(FilterValue::ID(GID::U64(*i))),
        Value::Str(s) => Ok(FilterValue::ID(GID::Str(s.clone()))),
        other => Err(GraphError::InvalidGqlFilter(format!(
            "{op} requires int or str, got {other}"
        ))),
    }
}

fn parse_node_id_list(op: &str, v: &Value) -> Result<FilterValue, GraphError> {
    let Value::List(vs) = v else {
        return Err(GraphError::InvalidGqlFilter(format!(
            "{op} requires a list value, got {v}"
        )));
    };

    let all_u64 = vs.iter().all(|v| matches!(v, Value::U64(_)));
    let all_str = vs.iter().all(|v| matches!(v, Value::Str(_)));
    if !(all_u64 || all_str) {
        return Err(GraphError::InvalidGqlFilter(format!(
            "{op} requires a homogeneous list of ints or strings"
        )));
    }

    let mut set = HashSet::with_capacity(vs.len());
    if all_u64 {
        for v in vs {
            if let Value::U64(i) = v {
                set.insert(GID::U64(*i));
            }
        }
    } else {
        for v in vs {
            if let Value::Str(s) = v {
                set.insert(GID::Str(s.clone()));
            }
        }
    }
    Ok(FilterValue::IDSet(Arc::new(set)))
}

fn parse_string_list(op: &str, v: &Value) -> Result<FilterValue, GraphError> {
    let Value::List(vs) = v else {
        return Err(GraphError::InvalidGqlFilter(format!(
            "{op} requires a list value, got {v}"
        )));
    };

    let strings = vs
        .iter()
        .map(|v| {
            if let Value::Str(s) = v {
                Ok(s.clone())
            } else {
                Err(GraphError::InvalidGqlFilter(format!(
                    "Expected list of strings for {op}, got {v}"
                )))
            }
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(FilterValue::Set(Arc::new(strings.into_iter().collect())))
}

fn translate_node_field_where(
    field: NodeField,
    cond: &NodeFieldCondition,
) -> Result<(String, FilterValue, FilterOperator), GraphError> {
    use FilterOperator as FO;
    use NodeField::*;
    use NodeFieldCondition::*;

    let field_name = field.to_string();
    let op = cond.op_name();

    Ok(match (field, cond) {
        (NodeId, Eq(v)) => (field_name, parse_node_id_scalar(op, v)?, FO::Eq),
        (NodeId, Ne(v)) => (field_name, parse_node_id_scalar(op, v)?, FO::Ne),
        (NodeId, Gt(v)) => (
            field_name,
            FilterValue::ID(GID::U64(require_u64_value(op, v)?)),
            FO::Gt,
        ),
        (NodeId, Ge(v)) => (
            field_name,
            FilterValue::ID(GID::U64(require_u64_value(op, v)?)),
            FO::Ge,
        ),
        (NodeId, Lt(v)) => (
            field_name,
            FilterValue::ID(GID::U64(require_u64_value(op, v)?)),
            FO::Lt,
        ),
        (NodeId, Le(v)) => (
            field_name,
            FilterValue::ID(GID::U64(require_u64_value(op, v)?)),
            FO::Le,
        ),

        (NodeId, StartsWith(v)) => (
            field_name,
            FilterValue::ID(GID::Str(require_string_value(op, v)?)),
            FO::StartsWith,
        ),
        (NodeId, EndsWith(v)) => (
            field_name,
            FilterValue::ID(GID::Str(require_string_value(op, v)?)),
            FO::EndsWith,
        ),
        (NodeId, Contains(v)) => (
            field_name,
            FilterValue::ID(GID::Str(require_string_value(op, v)?)),
            FO::Contains,
        ),
        (NodeId, NotContains(v)) => (
            field_name,
            FilterValue::ID(GID::Str(require_string_value(op, v)?)),
            FO::NotContains,
        ),

        (NodeId, IsIn(v)) => (field_name, parse_node_id_list(op, v)?, FO::IsIn),
        (NodeId, IsNotIn(v)) => (field_name, parse_node_id_list(op, v)?, FO::IsNotIn),

        (NodeName, Eq(v)) => (
            field_name,
            FilterValue::Single(require_string_value(op, v)?),
            FO::Eq,
        ),
        (NodeName, Ne(v)) => (
            field_name,
            FilterValue::Single(require_string_value(op, v)?),
            FO::Ne,
        ),
        (NodeName, Gt(v)) => (
            field_name,
            FilterValue::Single(require_string_value(op, v)?),
            FO::Gt,
        ),
        (NodeName, Ge(v)) => (
            field_name,
            FilterValue::Single(require_string_value(op, v)?),
            FO::Ge,
        ),
        (NodeName, Lt(v)) => (
            field_name,
            FilterValue::Single(require_string_value(op, v)?),
            FO::Lt,
        ),
        (NodeName, Le(v)) => (
            field_name,
            FilterValue::Single(require_string_value(op, v)?),
            FO::Le,
        ),

        (NodeName, StartsWith(v)) => (
            field_name,
            FilterValue::Single(require_string_value(op, v)?),
            FO::StartsWith,
        ),
        (NodeName, EndsWith(v)) => (
            field_name,
            FilterValue::Single(require_string_value(op, v)?),
            FO::EndsWith,
        ),
        (NodeName, Contains(v)) => (
            field_name,
            FilterValue::Single(require_string_value(op, v)?),
            FO::Contains,
        ),
        (NodeName, NotContains(v)) => (
            field_name,
            FilterValue::Single(require_string_value(op, v)?),
            FO::NotContains,
        ),

        (NodeName, IsIn(v)) => (field_name, parse_string_list(op, v)?, FO::IsIn),
        (NodeName, IsNotIn(v)) => (field_name, parse_string_list(op, v)?, FO::IsNotIn),

        (NodeType, Eq(v)) => (
            field_name,
            FilterValue::Single(require_string_value(op, v)?),
            FO::Eq,
        ),
        (NodeType, Ne(v)) => (
            field_name,
            FilterValue::Single(require_string_value(op, v)?),
            FO::Ne,
        ),
        (NodeType, Gt(v)) => (
            field_name,
            FilterValue::Single(require_string_value(op, v)?),
            FO::Gt,
        ),
        (NodeType, Ge(v)) => (
            field_name,
            FilterValue::Single(require_string_value(op, v)?),
            FO::Ge,
        ),
        (NodeType, Lt(v)) => (
            field_name,
            FilterValue::Single(require_string_value(op, v)?),
            FO::Lt,
        ),
        (NodeType, Le(v)) => (
            field_name,
            FilterValue::Single(require_string_value(op, v)?),
            FO::Le,
        ),

        (NodeType, StartsWith(v)) => (
            field_name,
            FilterValue::Single(require_string_value(op, v)?),
            FO::StartsWith,
        ),
        (NodeType, EndsWith(v)) => (
            field_name,
            FilterValue::Single(require_string_value(op, v)?),
            FO::EndsWith,
        ),
        (NodeType, Contains(v)) => (
            field_name,
            FilterValue::Single(require_string_value(op, v)?),
            FO::Contains,
        ),
        (NodeType, NotContains(v)) => (
            field_name,
            FilterValue::Single(require_string_value(op, v)?),
            FO::NotContains,
        ),

        (NodeType, IsIn(v)) => (field_name, parse_string_list(op, v)?, FO::IsIn),
        (NodeType, IsNotIn(v)) => (field_name, parse_string_list(op, v)?, FO::IsNotIn),
    })
}

fn translate_prop_leaf_to_filter(
    name_for_errors: &str,
    cmp: &PropCondition,
) -> Result<(FilterOperator, PropertyFilterValue), GraphError> {
    use FilterOperator as FO;
    use PropCondition::*;

    let single = |v: &Value| -> Result<PropertyFilterValue, GraphError> {
        Ok(PropertyFilterValue::Single(Prop::try_from(v.clone())?))
    };

    Ok(match cmp {
        Eq(v) => (FO::Eq, single(v)?),
        Ne(v) => (FO::Ne, single(v)?),
        Gt(v) => (FO::Gt, single(v)?),
        Ge(v) => (FO::Ge, single(v)?),
        Lt(v) => (FO::Lt, single(v)?),
        Le(v) => (FO::Le, single(v)?),

        StartsWith(v) => (
            FO::StartsWith,
            PropertyFilterValue::Single(Prop::Str(require_string_value(cmp.op_name(), v)?.into())),
        ),
        EndsWith(v) => (
            FO::EndsWith,
            PropertyFilterValue::Single(Prop::Str(require_string_value(cmp.op_name(), v)?.into())),
        ),

        Contains(v) => (FO::Contains, single(v)?),
        NotContains(v) => (FO::NotContains, single(v)?),

        IsIn(v) => (FO::IsIn, require_prop_list_value(cmp.op_name(), v)?),
        IsNotIn(v) => (FO::IsNotIn, require_prop_list_value(cmp.op_name(), v)?),

        IsSome(true) => (FO::IsSome, PropertyFilterValue::None),
        IsNone(true) => (FO::IsNone, PropertyFilterValue::None),

        And(_) | Or(_) | Not(_) | First(_) | Last(_) | Any(_) | All(_) | Sum(_) | Avg(_)
        | Min(_) | Max(_) | Len(_) | IsSome(false) | IsNone(false) => {
            let op = cmp.op_name();
            return Err(GraphError::InvalidGqlFilter(format!(
                "Expected comparison at leaf for {name_for_errors}; got '{op}'"
            )));
        }
    })
}

fn build_property_filter_from_condition_with_entity<M: Clone + Send + Sync + 'static>(
    prop_ref: PropertyRef,
    cond: &PropCondition,
    entity: M,
) -> Result<PropertyFilter<M>, GraphError> {
    let mut ops: Vec<Op> = Vec::new();
    let mut cursor = cond;
    while let Some(inner) = peel_prop_wrappers_and_collect_ops(cursor, &mut ops) {
        cursor = inner;
    }
    let (operator, prop_value) = translate_prop_leaf_to_filter(prop_ref.name(), cursor)?;
    Ok(PropertyFilter {
        prop_ref,
        prop_value,
        operator,
        ops,
        entity,
    })
}

fn build_node_filter_from_prop_condition(
    prop_ref: PropertyRef,
    cond: &PropCondition,
) -> Result<CompositeNodeFilter, GraphError> {
    use PropCondition::*;

    match cond {
        And(list) => {
            let mut it = list.iter();
            let first = it
                .next()
                .ok_or_else(|| GraphError::InvalidGqlFilter("and expects non-empty list".into()))?;
            let mut acc = build_node_filter_from_prop_condition(prop_ref.clone(), first)?;
            for c in it {
                let next = build_node_filter_from_prop_condition(prop_ref.clone(), c)?;
                acc = CompositeNodeFilter::And(Box::new(acc), Box::new(next));
            }
            Ok(acc)
        }
        Or(list) => {
            let mut it = list.iter();
            let first = it
                .next()
                .ok_or_else(|| GraphError::InvalidGqlFilter("or expects non-empty list".into()))?;
            let mut acc = build_node_filter_from_prop_condition(prop_ref.clone(), first)?;
            for c in it {
                let next = build_node_filter_from_prop_condition(prop_ref.clone(), c)?;
                acc = CompositeNodeFilter::Or(Box::new(acc), Box::new(next));
            }
            Ok(acc)
        }
        Not(inner) => {
            let nf = build_node_filter_from_prop_condition(prop_ref, inner)?;
            Ok(CompositeNodeFilter::Not(Box::new(nf)))
        }
        _ => {
            let pf = build_property_filter_from_condition_with_entity::<NodeFilter>(
                prop_ref, cond, NodeFilter,
            )?;
            Ok(CompositeNodeFilter::Property(pf))
        }
    }
}

impl TryFrom<GqlNodeFilter> for CompositeNodeFilter {
    type Error = GraphError;
    fn try_from(filter: GqlNodeFilter) -> Result<Self, Self::Error> {
        match filter {
            GqlNodeFilter::Node(node) => {
                let (field_name, field_value, operator) =
                    translate_node_field_where(node.field, &node.where_)?;
                Ok(CompositeNodeFilter::Node(Filter {
                    field_name,
                    field_value,
                    operator,
                }))
            }
            GqlNodeFilter::Property(prop) => {
                let prop_ref = PropertyRef::Property(prop.name.clone());
                build_node_filter_from_prop_condition(prop_ref, &prop.where_)
            }
            GqlNodeFilter::Metadata(prop) => {
                let prop_ref = PropertyRef::Metadata(prop.name.clone());
                build_node_filter_from_prop_condition(prop_ref, &prop.where_)
            }
            GqlNodeFilter::TemporalProperty(prop) => {
                let prop_ref = PropertyRef::TemporalProperty(prop.name.clone());
                build_node_filter_from_prop_condition(prop_ref, &prop.where_)
            }
            GqlNodeFilter::And(and_filters) => {
                let mut iter = and_filters.into_iter().map(TryInto::try_into);
                let first = iter.next().ok_or_else(|| {
                    GraphError::InvalidGqlFilter("Filter 'and' requires non-empty list".into())
                })??;
                Ok(iter.try_fold(first, |acc, next| {
                    let n = next?;
                    Ok::<_, GraphError>(CompositeNodeFilter::And(Box::new(acc), Box::new(n)))
                })?)
            }
            GqlNodeFilter::Or(or_filters) => {
                let mut iter = or_filters.into_iter().map(TryInto::try_into);
                let first = iter.next().ok_or_else(|| {
                    GraphError::InvalidGqlFilter("Filter 'or' requires non-empty list".into())
                })??;
                Ok(iter.try_fold(first, |acc, next| {
                    let n = next?;
                    Ok::<_, GraphError>(CompositeNodeFilter::Or(Box::new(acc), Box::new(n)))
                })?)
            }
            GqlNodeFilter::Not(not_filters) => {
                let inner = CompositeNodeFilter::try_from(not_filters.deref().clone())?;
                Ok(CompositeNodeFilter::Not(Box::new(inner)))
            }
            GqlNodeFilter::Window(w) => {
                let inner: CompositeNodeFilter = w.expr.deref().clone().try_into()?;
                Ok(CompositeNodeFilter::Windowed(Box::new(Windowed::new(
                    w.start.0, w.end.0, inner,
                ))))
            }

            GqlNodeFilter::At(t) => {
                let inner: CompositeNodeFilter = t.expr.deref().clone().try_into()?;
                let et: EventTime = t.time.0;
                Ok(CompositeNodeFilter::Windowed(Box::new(Windowed::new(
                    et,
                    EventTime::end(et.t().saturating_add(1)),
                    inner,
                ))))
            }

            GqlNodeFilter::Before(t) => {
                let inner: CompositeNodeFilter = t.expr.deref().clone().try_into()?;
                Ok(CompositeNodeFilter::Windowed(Box::new(Windowed::new(
                    EventTime::start(i64::MIN),
                    EventTime::end(t.time.0.t()),
                    inner,
                ))))
            }

            GqlNodeFilter::After(t) => {
                let inner: CompositeNodeFilter = t.expr.deref().clone().try_into()?;
                let start = EventTime::start(t.time.0.t().saturating_add(1));
                Ok(CompositeNodeFilter::Windowed(Box::new(Windowed::new(
                    start,
                    EventTime::end(i64::MAX),
                    inner,
                ))))
            }

            GqlNodeFilter::Latest(u) => {
                let inner: CompositeNodeFilter = u.expr.deref().clone().try_into()?;
                Ok(CompositeNodeFilter::Latest(Box::new(LatestWrap::new(
                    inner,
                ))))
            }

            GqlNodeFilter::SnapshotAt(t) => {
                let inner: CompositeNodeFilter = t.expr.deref().clone().try_into()?;
                Ok(CompositeNodeFilter::SnapshotAt(Box::new(
                    SnapshotAtWrap::new(t.time.0, inner),
                )))
            }

            GqlNodeFilter::SnapshotLatest(u) => {
                let inner: CompositeNodeFilter = u.expr.deref().clone().try_into()?;
                Ok(CompositeNodeFilter::SnapshotLatest(Box::new(
                    SnapshotLatestWrap::new(inner),
                )))
            }

            GqlNodeFilter::Layers(l) => {
                let layer = Layer::from(l.names.clone());
                let inner: CompositeNodeFilter = l.expr.deref().clone().try_into()?;
                Ok(CompositeNodeFilter::Layered(Box::new(Layered::new(
                    layer, inner,
                ))))
            }

            GqlNodeFilter::IsActive(true) => Ok(CompositeNodeFilter::IsActiveNode(IsActiveNode)),
            GqlNodeFilter::IsActive(false) => Ok(CompositeNodeFilter::Not(Box::new(
                CompositeNodeFilter::IsActiveNode(IsActiveNode),
            ))),
        }
    }
}

fn build_edge_filter_from_prop_condition(
    prop_ref: PropertyRef,
    cond: &PropCondition,
) -> Result<CompositeEdgeFilter, GraphError> {
    use PropCondition::*;

    match cond {
        And(list) => {
            let mut it = list.iter();
            let first = it
                .next()
                .ok_or_else(|| GraphError::InvalidGqlFilter("and expects non-empty list".into()))?;
            let mut acc = build_edge_filter_from_prop_condition(prop_ref.clone(), first)?;
            for c in it {
                let next = build_edge_filter_from_prop_condition(prop_ref.clone(), c)?;
                acc = CompositeEdgeFilter::And(Box::new(acc), Box::new(next));
            }
            Ok(acc)
        }
        Or(list) => {
            let mut it = list.iter();
            let first = it
                .next()
                .ok_or_else(|| GraphError::InvalidGqlFilter("or expects non-empty list".into()))?;
            let mut acc = build_edge_filter_from_prop_condition(prop_ref.clone(), first)?;
            for c in it {
                let next = build_edge_filter_from_prop_condition(prop_ref.clone(), c)?;
                acc = CompositeEdgeFilter::Or(Box::new(acc), Box::new(next));
            }
            Ok(acc)
        }
        Not(inner) => {
            let ef = build_edge_filter_from_prop_condition(prop_ref, inner)?;
            Ok(CompositeEdgeFilter::Not(Box::new(ef)))
        }
        _ => {
            let pf = build_property_filter_from_condition_with_entity::<EdgeFilter>(
                prop_ref, cond, EdgeFilter,
            )?;
            Ok(CompositeEdgeFilter::Property(pf))
        }
    }
}

impl TryFrom<GqlEdgeFilter> for CompositeEdgeFilter {
    type Error = GraphError;
    fn try_from(filter: GqlEdgeFilter) -> Result<Self, Self::Error> {
        match filter {
            GqlEdgeFilter::Src(nf) => {
                let nf: CompositeNodeFilter = nf.deref().clone().try_into()?;
                Ok(CompositeEdgeFilter::Src(nf))
            }
            GqlEdgeFilter::Dst(nf) => {
                let nf: CompositeNodeFilter = nf.deref().clone().try_into()?;
                Ok(CompositeEdgeFilter::Dst(nf))
            }
            GqlEdgeFilter::Property(prop) => {
                let prop_ref = PropertyRef::Property(prop.name.clone());
                build_edge_filter_from_prop_condition(prop_ref, &prop.where_)
            }
            GqlEdgeFilter::Metadata(prop) => {
                let prop_ref = PropertyRef::Metadata(prop.name.clone());
                build_edge_filter_from_prop_condition(prop_ref, &prop.where_)
            }
            GqlEdgeFilter::TemporalProperty(prop) => {
                let prop_ref = PropertyRef::TemporalProperty(prop.name.clone());
                build_edge_filter_from_prop_condition(prop_ref, &prop.where_)
            }
            GqlEdgeFilter::And(and_filters) => {
                let mut iter = and_filters.into_iter().map(TryInto::try_into);
                let first = iter.next().ok_or_else(|| {
                    GraphError::InvalidGqlFilter("Filter 'and' requires non-empty list".into())
                })??;
                Ok(iter.try_fold(first, |acc, next| {
                    let n = next?;
                    Ok::<_, GraphError>(CompositeEdgeFilter::And(Box::new(acc), Box::new(n)))
                })?)
            }
            GqlEdgeFilter::Or(or_filters) => {
                let mut iter = or_filters.into_iter().map(TryInto::try_into);
                let first = iter.next().ok_or_else(|| {
                    GraphError::InvalidGqlFilter("Filter 'or' requires non-empty list".into())
                })??;
                Ok(iter.try_fold(first, |acc, next| {
                    let n = next?;
                    Ok::<_, GraphError>(CompositeEdgeFilter::Or(Box::new(acc), Box::new(n)))
                })?)
            }
            GqlEdgeFilter::Not(not_filters) => {
                let inner = CompositeEdgeFilter::try_from(not_filters.deref().clone())?;
                Ok(CompositeEdgeFilter::Not(Box::new(inner)))
            }
            GqlEdgeFilter::Window(w) => {
                let inner: CompositeEdgeFilter = w.expr.deref().clone().try_into()?;
                Ok(CompositeEdgeFilter::Windowed(Box::new(Windowed::new(
                    w.start.0, w.end.0, inner,
                ))))
            }

            GqlEdgeFilter::At(t) => {
                let inner: CompositeEdgeFilter = t.expr.deref().clone().try_into()?;
                let et: EventTime = t.time.0;
                Ok(CompositeEdgeFilter::Windowed(Box::new(Windowed::new(
                    et,
                    EventTime::end(et.t().saturating_add(1)),
                    inner,
                ))))
            }

            GqlEdgeFilter::Before(t) => {
                let inner: CompositeEdgeFilter = t.expr.deref().clone().try_into()?;
                Ok(CompositeEdgeFilter::Windowed(Box::new(Windowed::new(
                    EventTime::start(i64::MIN),
                    EventTime::end(t.time.0.t()),
                    inner,
                ))))
            }

            GqlEdgeFilter::After(t) => {
                let inner: CompositeEdgeFilter = t.expr.deref().clone().try_into()?;
                let start = EventTime::start(t.time.0.t().saturating_add(1));
                Ok(CompositeEdgeFilter::Windowed(Box::new(Windowed::new(
                    start,
                    EventTime::end(i64::MAX),
                    inner,
                ))))
            }

            GqlEdgeFilter::Latest(u) => {
                let inner: CompositeEdgeFilter = u.expr.deref().clone().try_into()?;
                Ok(CompositeEdgeFilter::Latest(Box::new(LatestWrap::new(
                    inner,
                ))))
            }

            GqlEdgeFilter::SnapshotAt(t) => {
                let inner: CompositeEdgeFilter = t.expr.deref().clone().try_into()?;
                Ok(CompositeEdgeFilter::SnapshotAt(Box::new(
                    SnapshotAtWrap::new(t.time.0, inner),
                )))
            }

            GqlEdgeFilter::SnapshotLatest(u) => {
                let inner: CompositeEdgeFilter = u.expr.deref().clone().try_into()?;
                Ok(CompositeEdgeFilter::SnapshotLatest(Box::new(
                    SnapshotLatestWrap::new(inner),
                )))
            }

            GqlEdgeFilter::Layers(l) => {
                let layer = Layer::from(l.names.clone());
                let inner: CompositeEdgeFilter = l.expr.deref().clone().try_into()?;
                Ok(CompositeEdgeFilter::Layered(Box::new(Layered::new(
                    layer, inner,
                ))))
            }

            GqlEdgeFilter::IsActive(true) => Ok(CompositeEdgeFilter::IsActiveEdge(IsActiveEdge)),
            GqlEdgeFilter::IsActive(false) => Ok(CompositeEdgeFilter::Not(Box::new(
                CompositeEdgeFilter::IsActiveEdge(IsActiveEdge),
            ))),

            GqlEdgeFilter::IsValid(true) => Ok(CompositeEdgeFilter::IsValidEdge(IsValidEdge)),
            GqlEdgeFilter::IsValid(false) => Ok(CompositeEdgeFilter::Not(Box::new(
                CompositeEdgeFilter::IsValidEdge(IsValidEdge),
            ))),

            GqlEdgeFilter::IsDeleted(true) => Ok(CompositeEdgeFilter::IsDeletedEdge(IsDeletedEdge)),
            GqlEdgeFilter::IsDeleted(false) => Ok(CompositeEdgeFilter::Not(Box::new(
                CompositeEdgeFilter::IsDeletedEdge(IsDeletedEdge),
            ))),

            GqlEdgeFilter::IsSelfLoop(true) => {
                Ok(CompositeEdgeFilter::IsSelfLoopEdge(IsSelfLoopEdge))
            }
            GqlEdgeFilter::IsSelfLoop(false) => Ok(CompositeEdgeFilter::Not(Box::new(
                CompositeEdgeFilter::IsSelfLoopEdge(IsSelfLoopEdge),
            ))),
        }
    }
}

impl TryFrom<GqlGraphFilter> for DynView {
    type Error = GraphError;

    fn try_from(f: GqlGraphFilter) -> Result<Self, Self::Error> {
        let default_inner: DynView = Arc::new(GraphFilter);

        Ok(match f {
            GqlGraphFilter::Window(w) => {
                let inner: DynView = match w.expr {
                    Some(e) => e.deref().clone().try_into()?,
                    None => default_inner,
                };
                inner.window(w.start, w.end)
            }
            GqlGraphFilter::At(t) => {
                let inner: DynView = match t.expr {
                    Some(e) => e.deref().clone().try_into()?,
                    None => default_inner,
                };
                inner.at(t.time)
            }
            GqlGraphFilter::Before(t) => {
                let inner: DynView = match t.expr {
                    Some(e) => e.deref().clone().try_into()?,
                    None => default_inner,
                };
                inner.before(t.time)
            }
            GqlGraphFilter::After(t) => {
                let inner: DynView = match t.expr {
                    Some(e) => e.deref().clone().try_into()?,
                    None => default_inner,
                };
                inner.after(t.time)
            }
            GqlGraphFilter::Latest(u) => {
                let inner: DynView = match u.expr {
                    Some(e) => e.deref().clone().try_into()?,
                    None => default_inner,
                };
                Arc::new(inner.latest())
            }
            GqlGraphFilter::SnapshotAt(t) => {
                let inner: DynView = match t.expr {
                    Some(e) => e.deref().clone().try_into()?,
                    None => default_inner,
                };
                Arc::new(inner.snapshot_at(t.time))
            }
            GqlGraphFilter::SnapshotLatest(u) => {
                let inner: DynView = match u.expr {
                    Some(e) => e.deref().clone().try_into()?,
                    None => default_inner,
                };
                Arc::new(inner.snapshot_latest())
            }
            GqlGraphFilter::Layers(l) => {
                let inner: DynView = match l.expr {
                    Some(e) => e.deref().clone().try_into()?,
                    None => default_inner,
                };
                Arc::new(inner.layer(l.names))
            }
        })
    }
}
