use crate::model::graph::{
    filter_expr_input::GqlFilterExpr, node_id::GqlNodeId, property::Value, timeindex::GqlTimeInput,
};
use async_graphql::dynamic::ValueAccessor;
use dynamic_graphql::{
    internal::{
        FromValue, GetInputTypeRef, InputTypeName, InputValueResult, Register, Registry, TypeName,
    },
    Enum, InputObject, OneOfInput,
};
use raphtory::{
    db::{
        api::{
            state::NodeOp,
            view::internal::{DynGraphArc, GraphView},
        },
        graph::views::filter::{
            model::{
                filter::{FilterValue, NODE_ID_FIELD, NODE_NAME_FIELD, NODE_TYPE_FIELD},
                filter_operator::FilterOperator,
                property_filter::{Op, PropertyFilter, PropertyFilterValue, PropertyRef},
                tree::{compile_view, FilterExpr, ViewOp},
                DynFilter, DynView,
            },
            CreateFilter,
        },
    },
    errors::GraphError,
};
use raphtory_api::core::{
    entities::{properties::prop::Prop, GID},
    Direction,
};
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    collections::HashSet,
    fmt,
    fmt::{Display, Formatter},
    ops::Deref,
    sync::Arc,
};

#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
    Subgraph(Vec<GqlNodeId>),
    /// Subgraph node types.
    SubgraphNodeTypes(Vec<String>),
    /// List of excluded nodes.
    ExcludeNodes(Vec<GqlNodeId>),
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
    /// Set the window start to a specified time.
    ShrinkStart(GqlTimeInput),
    /// Set the window end to a specified time.
    ShrinkEnd(GqlTimeInput),
}

// The node field a filter targets, as a GraphQL enum value (`NODE_ID`/`NODE_NAME`/`NODE_TYPE`).
#[derive(Enum, Copy, Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
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
                NodeField::NodeId => NODE_ID_FIELD,
                NodeField::NodeName => NODE_NAME_FIELD,
                NodeField::NodeType => NODE_TYPE_FIELD,
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
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PropertyFilterNew {
    /// Property (or metadata) key.
    pub name: String,
    /// Condition applied to the property value.
    ///
    /// Exposed as `where` in GraphQL.
    #[graphql(name = "where")]
    #[serde(rename = "where")]
    pub where_: PropCondition,
}

/// Filters nodes by computed degree with a directional scope.
///
/// `DegreeFilterNew` lets callers filter on:
/// - inbound degree (`IN`),
/// - outbound degree (`OUT`),
/// - or total degree (`BOTH`).
///
/// The selected degree is compared using the `where` condition.
///
/// Example (GraphQL):
/// ```graphql
/// { Degree: { direction: BOTH, where: { Gt: 10 } } }
/// ```

#[derive(Enum, Copy, Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum DegreeDirection {
    In,
    Out,
    Both,
}

impl From<DegreeDirection> for Direction {
    fn from(d: DegreeDirection) -> Self {
        match d {
            DegreeDirection::In => Direction::IN,
            DegreeDirection::Out => Direction::OUT,
            DegreeDirection::Both => Direction::BOTH,
        }
    }
}

impl From<DegreeDirection> for String {
    fn from(d: DegreeDirection) -> Self {
        match d {
            DegreeDirection::In => "in_degree".to_string(),
            DegreeDirection::Out => "out_degree".to_string(),
            DegreeDirection::Both => "degree".to_string(),
        }
    }
}

#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DegreeFilterNew {
    pub direction: DegreeDirection,
    #[graphql(name = "where")]
    #[serde(rename = "where")]
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
#[derive(OneOfInput, Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
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

    /// Fuzzy string match (Levenshtein distance, optional prefix matching).
    FuzzySearch(FuzzySearchExpr),

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
            FuzzySearch(_) => "fuzzySearch",

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
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphTimeExpr {
    /// Reference time for the operation.
    pub time: GqlTimeInput,
    /// Optional nested filter applied after the time restriction.
    pub expr: Option<Wrapped<GqlGraphFilter>>,
}

/// Graph view restriction that takes only a nested expression.
///
/// Used for unary view operations like `Latest` and `SnapshotLatest`.
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphUnaryExpr {
    /// Optional nested filter applied after the unary operation.
    pub expr: Option<Wrapped<GqlGraphFilter>>,
}

/// Graph view restriction by layer membership, optionally chaining another `GraphFilter`.
///
/// Used by `GqlGraphFilter::Layers`.
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[derive(OneOfInput, Clone, Debug, Serialize, Deserialize)]
#[graphql(name = "GraphFilter")]
#[serde(rename_all = "camelCase")]
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

/// A general filter expression — a node filter (`node`), an edge filter (`edge`), a graph/view
/// filter (`graph`, e.g. a layer or window restriction), or an `and`/`or` combination of these
/// (which may mix kinds). Used where an operation accepts any filter, such as scoping a component
/// walk.
#[derive(OneOfInput, Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum GqlFilter {
    /// The filter tree: one grammar for every entity, with expressions on both
    /// sides of a comparison. The other variants are the legacy grammar and
    /// are converted onto it.
    Expr(GqlFilterExpr),
    /// Filter by node properties, fields, or temporal state.
    Node(GqlNodeFilter),
    /// Filter by edge properties, source/destination, or temporal state.
    /// (Persisted filters may use the legacy `edge` key.)
    #[serde(alias = "edge")]
    Edge(GqlEdgeFilter),
    /// Filter exploded edges — per-event edge instances — by properties,
    /// endpoints, or temporal state, evaluated per event.
    ExplodedEdge(GqlExplodedEdgeFilter),
    /// Apply a graph-level view (window, snapshot, layer restriction, …).
    Graph(GqlGraphFilter),
    /// All sub-filters must pass (intersection).
    And(Vec<GqlFilter>),
    /// At least one sub-filter must pass (union).
    /// Cross-type sub-filters (e.g. `node` and `edge` together) produce a
    /// proper graph union: a node is visible if it matches the node filter or
    /// has a visible edge, and an edge is visible if it matches the edge
    /// filter or both its endpoints are visible.
    Or(Vec<GqlFilter>),
    /// Inverts the nested filter.
    Not(Wrapped<GqlFilter>),

    // Flat graph-view spellings — equivalent to wrapping the same expression
    // in `graph: {...}`; kept top-level so pre-existing `Graph.filter`
    // documents (e.g. `filter(expr: {window: ...})`) remain valid.
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

impl TryFrom<GqlNodeFilter> for GqlFilter {
    type Error = GraphError;
    fn try_from(f: GqlNodeFilter) -> Result<Self, Self::Error> {
        Ok(GqlFilter::Node(f))
    }
}

impl TryFrom<GqlEdgeFilter> for GqlFilter {
    type Error = GraphError;
    fn try_from(f: GqlEdgeFilter) -> Result<Self, Self::Error> {
        Ok(GqlFilter::Edge(f))
    }
}

impl TryFrom<GqlExplodedEdgeFilter> for GqlFilter {
    type Error = GraphError;
    fn try_from(f: GqlExplodedEdgeFilter) -> Result<Self, Self::Error> {
        Ok(GqlFilter::ExplodedEdge(f))
    }
}

impl TryFrom<GqlGraphFilter> for GqlFilter {
    type Error = GraphError;
    fn try_from(f: GqlGraphFilter) -> Result<Self, Self::Error> {
        Ok(GqlFilter::Graph(f))
    }
}

impl CreateFilter for GqlFilter {
    type EntityFiltered<'graph, G: GraphView + 'graph, F: GraphView + 'graph>
        = DynGraphArc<'graph>
    where
        Self: 'graph;

    type NodeFilter<'graph, G: GraphView + 'graph, F: GraphView + 'graph> =
        Arc<dyn NodeOp<Output = bool> + 'graph>;

    type FilteredGraph<'graph, G>
        = DynGraphArc<'graph>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::EntityFiltered<'graph, G, F>, GraphError> {
        DynFilter::try_from(self)?.create_filter(graph, filtered)
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError> {
        DynFilter::try_from(self)?.create_node_filter(graph, filtered)
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        DynFilter::try_from(self.clone())?.filter_graph_view(graph)
    }
}

impl TryFrom<FilterExpr> for GqlFilter {
    type Error = GraphError;

    fn try_from(tree: FilterExpr) -> Result<Self, Self::Error> {
        Ok(GqlFilter::Expr(GqlFilterExpr::try_from(&tree)?))
    }
}

impl TryFrom<GqlFilter> for GqlFilterExpr {
    type Error = GraphError;

    fn try_from(value: GqlFilter) -> Result<Self, Self::Error> {
        super::expr_lowering::lower_filter(&value)
    }
}

impl GqlFilter {
    /// The same filter in the tree grammar. A filter already written as a
    /// tree is returned as is; a legacy spelling is converted, constants and
    /// policy placeholders intact.
    pub fn into_tree_grammar(self) -> Result<GqlFilter, GraphError> {
        Ok(match self {
            GqlFilter::Expr(_) => self,
            legacy => GqlFilter::Expr(GqlFilterExpr::try_from(legacy)?),
        })
    }
}

impl TryFrom<GqlFilter> for FilterExpr {
    type Error = GraphError;

    fn try_from(value: GqlFilter) -> Result<Self, Self::Error> {
        FilterExpr::try_from(GqlFilterExpr::try_from(value)?)
    }
}

impl TryFrom<GqlFilter> for DynFilter {
    type Error = GraphError;

    fn try_from(value: GqlFilter) -> Result<Self, Self::Error> {
        FilterExpr::try_from(value)?.compile()
    }
}

/// Boolean expression over a built-in node field (ID, name, or type).
///
/// This is used by `NodeFieldWhere.where_` when filtering a specific
/// built-in field.
///
/// Supports comparisons, string predicates, and set membership.
/// (Presence checks and aggregations are handled via property filters instead.)
#[derive(OneOfInput, Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
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

    /// Fuzzy string match (Levenshtein distance, optional prefix matching).
    FuzzySearch(FuzzySearchExpr),

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
            FuzzySearch(_) => "fuzzySearch",
            IsIn(_) => "isIn",
            IsNotIn(_) => "isNotIn",
        }
    }
}

/// A condition on one specific built-in field — the payload of the per-field
/// filter variants (`{ id: { where: ... } }`, `{ name: { where: ... } }`,
/// `{ nodeType: { where: ... } }`).
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeFieldWhere {
    /// Condition applied to the field.
    ///
    /// Exposed as `where` in GraphQL.
    #[graphql(name = "where")]
    #[serde(rename = "where")]
    pub where_: NodeFieldCondition,
}

/// Restricts node evaluation to a time window and applies a nested `NodeFilter`.
///
/// Used by `GqlNodeFilter::Window`.
///
/// The window is inclusive of `start` and exclusive of `end`.
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeTimeExpr {
    /// Reference time for the operation.
    pub time: GqlTimeInput,
    /// Filter evaluated within the restricted time scope.
    pub expr: Wrapped<GqlNodeFilter>,
}

/// Applies a unary node-view operation and then evaluates a nested `NodeFilter`.
///
/// Used by `Latest` and `SnapshotLatest` node filters.
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeUnaryExpr {
    /// Filter evaluated after applying the unary operation.
    pub expr: Wrapped<GqlNodeFilter>,
}

/// Restricts node evaluation to one or more layers and applies a nested `NodeFilter`.
///
/// Used by `GqlNodeFilter::Layers`.
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
/// - built-in node fields (`Id` / `Name` / `NodeType`),
/// - node properties and metadata,
/// - temporal properties,
/// - temporal scope (windows, snapshots, latest),
/// - and layer membership,
/// - plus node state predicates (e.g. `IsActive`).
///
/// Filters can be combined recursively using logical operators
/// (`And`, `Or`, `Not`).
#[derive(OneOfInput, Clone, Debug, Serialize, Deserialize)]
#[graphql(name = "NodeFilter")]
#[serde(rename_all = "camelCase")]
pub enum GqlNodeFilter {
    /// Filters the node id: `{ id: { where: ... } }`.
    Id(NodeFieldWhere),

    /// Filters the node name: `{ name: { where: ... } }`.
    Name(NodeFieldWhere),

    /// Filters the node type: `{ nodeType: { where: ... } }`.
    NodeType(NodeFieldWhere),

    /// Filters a node property by name and condition.
    Property(PropertyFilterNew),

    /// Filters a node's degree (in, out, or total) by a condition.
    Degree(DegreeFilterNew),

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
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EdgeTimeExpr {
    /// Reference time for the operation.
    pub time: GqlTimeInput,
    /// Filter evaluated within the restricted time scope.
    pub expr: Wrapped<GqlEdgeFilter>,
}

/// Applies a unary edge-view operation and then evaluates a nested `EdgeFilter`.
///
/// Used by `Latest` and `SnapshotLatest` edge filters.
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EdgeUnaryExpr {
    /// Filter evaluated after applying the unary operation.
    pub expr: Wrapped<GqlEdgeFilter>,
}

/// Restricts edge evaluation to one or more layers and applies a nested `EdgeFilter`.
///
/// Used by `GqlEdgeFilter::Layers`.
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[derive(OneOfInput, Clone, Debug, Serialize, Deserialize)]
#[graphql(name = "EdgeFilter")]
#[serde(rename_all = "camelCase")]
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

/// Restricts exploded-edge evaluation to a time window and applies a nested
/// `ExplodedEdgeFilter`.
///
/// Used by `GqlExplodedEdgeFilter::Window`.
///
/// The window is inclusive of `start` and exclusive of `end`.
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExplodedEdgeWindowExpr {
    /// Window start time (inclusive).
    pub start: GqlTimeInput,
    /// Window end time (exclusive).
    pub end: GqlTimeInput,
    /// Filter evaluated within the restricted window.
    pub expr: Wrapped<GqlExplodedEdgeFilter>,
}

/// Restricts exploded-edge evaluation to a single time bound and applies a
/// nested `ExplodedEdgeFilter`.
///
/// Used by `At`, `Before`, `After`, and `SnapshotAt` exploded-edge filters.
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExplodedEdgeTimeExpr {
    /// Reference time for the operation.
    pub time: GqlTimeInput,
    /// Filter evaluated within the restricted time scope.
    pub expr: Wrapped<GqlExplodedEdgeFilter>,
}

/// Applies a unary edge-view operation and then evaluates a nested
/// `ExplodedEdgeFilter`.
///
/// Used by `Latest` and `SnapshotLatest` exploded-edge filters.
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExplodedEdgeUnaryExpr {
    /// Filter evaluated after applying the unary operation.
    pub expr: Wrapped<GqlExplodedEdgeFilter>,
}

/// Restricts exploded-edge evaluation to one or more layers and applies a
/// nested `ExplodedEdgeFilter`.
///
/// Used by `GqlExplodedEdgeFilter::Layers`.
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExplodedEdgeLayersExpr {
    /// Layer names to include.
    pub names: Vec<String>,
    /// Filter evaluated within the layer-restricted view.
    pub expr: Wrapped<GqlExplodedEdgeFilter>,
}

/// GraphQL input type for filtering **exploded edges** — edge views where each
/// temporal event is an individually addressable edge instance, rather than
/// one aggregated edge across time.
///
/// Predicates are evaluated **per event**: a property condition keeps the
/// individual updates that match it (and the edges carrying them), where the
/// plain `EdgeFilter` evaluates one aggregated value per edge.
///
/// Filters can target edge endpoints, properties/metadata, temporal scope,
/// layer membership, and structural edge state, and can be combined
/// recursively with `And`/`Or`/`Not` — mirroring `EdgeFilter`.
#[derive(OneOfInput, Clone, Debug, Serialize, Deserialize)]
#[graphql(name = "ExplodedEdgeFilter")]
#[serde(rename_all = "camelCase")]
pub enum GqlExplodedEdgeFilter {
    /// Applies a filter to the **source node** of the exploded edge.
    Src(Wrapped<GqlNodeFilter>),

    /// Applies a filter to the **destination node** of the exploded edge.
    Dst(Wrapped<GqlNodeFilter>),

    /// Filters an exploded-edge **property** by name and value, evaluated
    /// per event.
    ///
    /// Example:
    /// `{ Property: { name: "weight", gt: 0.5 } }`
    Property(PropertyFilterNew),

    /// Filters an exploded-edge **metadata field**.
    ///
    /// Metadata is shared across all temporal versions of an edge.
    Metadata(PropertyFilterNew),

    /// Filters a **temporal exploded-edge property**, evaluated within a
    /// temporal context per event.
    TemporalProperty(PropertyFilterNew),

    /// Logical **AND** over multiple exploded-edge filters.
    And(Vec<GqlExplodedEdgeFilter>),

    /// Logical **OR** over multiple exploded-edge filters.
    Or(Vec<GqlExplodedEdgeFilter>),

    /// Logical **NOT** over a nested exploded-edge filter.
    Not(Wrapped<GqlExplodedEdgeFilter>),

    /// Restricts exploded-edge evaluation to a **time window**
    /// (inclusive start, exclusive end).
    Window(ExplodedEdgeWindowExpr),

    /// Restricts exploded-edge evaluation to a **single point in time**.
    At(ExplodedEdgeTimeExpr),

    /// Restricts exploded-edge evaluation to times **strictly before** a
    /// given time.
    Before(ExplodedEdgeTimeExpr),

    /// Restricts exploded-edge evaluation to times **strictly after** a
    /// given time.
    After(ExplodedEdgeTimeExpr),

    /// Evaluates exploded-edge predicates against the **latest available
    /// state**.
    Latest(ExplodedEdgeUnaryExpr),

    /// Evaluates exploded-edge predicates against a **snapshot** of the graph
    /// at a specific time.
    SnapshotAt(ExplodedEdgeTimeExpr),

    /// Evaluates exploded-edge predicates against the **most recent
    /// snapshot** of the graph.
    SnapshotLatest(ExplodedEdgeUnaryExpr),

    /// Restricts evaluation to exploded edges belonging to one or more
    /// **layers**.
    Layers(ExplodedEdgeLayersExpr),

    /// Matches exploded edges that have at least one event in the current
    /// view/window.
    IsActive(bool),

    /// Matches exploded edges that are structurally valid (i.e. not deleted)
    /// in the current view/window.
    IsValid(bool),

    /// Matches exploded edges that have been deleted in the current
    /// view/window.
    IsDeleted(bool),

    /// Matches exploded edges that are **self-loops**
    /// (source node == destination node).
    IsSelfLoop(bool),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Wrapped<T>(Box<T>);

impl<T> From<T> for Wrapped<T> {
    fn from(inner: T) -> Self {
        Wrapped(Box::new(inner))
    }
}

impl<T> Deref for Wrapped<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

/// Fuzzy string match: passes when the candidate is within `levenshteinDistance`
/// edits of `value` (optionally also matching by prefix). Mirrors the local
/// `fuzzy_search(value, levenshtein_distance, prefix_match)` builder.
#[derive(InputObject, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct FuzzySearchExpr {
    /// The string to match against.
    pub value: String,
    /// Maximum Levenshtein edit distance for a match.
    pub levenshtein_distance: usize,
    /// Whether a prefix match within the distance also passes.
    pub prefix_match: bool,
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

pub(crate) fn translate_node_field_where(
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
        (NodeId, Gt(v)) => (field_name, parse_node_id_scalar(op, v)?, FO::Gt),
        (NodeId, Ge(v)) => (field_name, parse_node_id_scalar(op, v)?, FO::Ge),
        (NodeId, Lt(v)) => (field_name, parse_node_id_scalar(op, v)?, FO::Lt),
        (NodeId, Le(v)) => (field_name, parse_node_id_scalar(op, v)?, FO::Le),

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

        (NodeId, FuzzySearch(f)) => (
            field_name,
            FilterValue::ID(GID::Str(f.value.clone())),
            FO::FuzzySearch {
                levenshtein_distance: f.levenshtein_distance,
                prefix_match: f.prefix_match,
            },
        ),
        (NodeName, FuzzySearch(f)) | (NodeType, FuzzySearch(f)) => (
            field_name,
            FilterValue::Single(f.value.clone()),
            FO::FuzzySearch {
                levenshtein_distance: f.levenshtein_distance,
                prefix_match: f.prefix_match,
            },
        ),

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

pub(crate) fn translate_prop_leaf_to_filter(
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
        // `isSome: false` is exactly `isNone: true` (and vice versa) — lower
        // to the dual operator instead of rejecting.
        IsSome(false) => (FO::IsNone, PropertyFilterValue::None),
        IsNone(false) => (FO::IsSome, PropertyFilterValue::None),

        FuzzySearch(f) => (
            FO::FuzzySearch {
                levenshtein_distance: f.levenshtein_distance,
                prefix_match: f.prefix_match,
            },
            PropertyFilterValue::Single(Prop::Str(f.value.clone().into())),
        ),

        And(_) | Or(_) | Not(_) | First(_) | Last(_) | Any(_) | All(_) | Sum(_) | Avg(_)
        | Min(_) | Max(_) | Len(_) => {
            let op = cmp.op_name();
            return Err(GraphError::InvalidGqlFilter(format!(
                "Expected comparison at leaf for {name_for_errors}; got '{op}'"
            )));
        }
    })
}

pub(crate) fn build_property_filter_from_condition_with_entity<M: Clone + Send + Sync + 'static>(
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

impl TryFrom<GqlGraphFilter> for DynView {
    type Error = GraphError;

    fn try_from(f: GqlGraphFilter) -> Result<Self, Self::Error> {
        let ops: Vec<ViewOp> = super::expr_lowering::lower_graph_filter(&f)?
            .into_iter()
            .map(ViewOp::from)
            .collect();
        Ok(compile_view(&ops))
    }
}

/// Property/metadata keys to hide per entity type.
#[derive(InputObject, Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HiddenKeys {
    /// Keys to strip from node property/metadata responses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<Vec<String>>,
    /// Keys to strip from edge property/metadata responses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub edge: Option<Vec<String>>,
    /// Keys to strip from graph-own property/metadata responses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph: Option<Vec<String>>,
}

/// Top-level access filter accepted by `grantGraphFilteredReadOnly`.
/// Separates row-level visibility (which entities are returned) from column-level
/// visibility (which property keys appear on returned entities).
#[derive(InputObject, Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GraphAccessFilter {
    /// Row-level filter: which nodes/edges/graph-view are visible.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<GqlFilter>,
    /// Temporal property keys to hide per entity type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hidden_properties: Option<HiddenKeys>,
    /// Metadata keys to hide per entity type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hidden_metadata: Option<HiddenKeys>,
}

// ============ Reverse conversion: engine filter → wire filter ============
//
// Used by the RemoteGraph Python client, which builds filters via the local
// `PyFilterExpr` API (producing a `Composite*Filter`) and sends them as the
// unified `GqlFilter` — the per-kind conversions here are the first half of
// that, with `Composite*Filter -> GqlFilter` wrapping them. The forward path
// (`TryFrom<GqlNodeFilter> for CompositeNodeFilter`, above) already exists.
//
// Not every engine filter has a lossless wire counterpart, and those cases
// surface as `GraphError::InvalidGqlFilter` rather than being silently
// dropped. (A `Layer::All` view op is not one of them: it restricts nothing,
// so it is skipped while the rest of the chain is kept.)

#[cfg(test)]
mod filter_serde_goldens {
    use super::*;

    // The wire format is the single source of truth — async-graphql input
    // coercion, the persisted auth-store `GraphAccessFilter`, and the client
    // all depend on these EXACT shapes. Pin them so a stray `#[serde(rename)]`
    // is caught here, not at e2e time or by an invalidated permission store.
    #[test]
    fn per_field_filter_golden() {
        let f = GqlNodeFilter::Name(NodeFieldWhere {
            where_: NodeFieldCondition::Eq(Value::Str("alice".into())),
        });
        assert_eq!(
            serde_json::to_value(&f).unwrap(),
            serde_json::json!({"name": {"where": {"eq": {"str": "alice"}}}})
        );
    }

    #[test]
    fn property_filter_golden() {
        let f = GqlNodeFilter::Property(PropertyFilterNew {
            name: "score".into(),
            where_: PropCondition::Gt(Value::F64(6.0)),
        });
        assert_eq!(
            serde_json::to_value(&f).unwrap(),
            serde_json::json!({"property": {"name": "score", "where": {"gt": {"f64": 6.0}}}})
        );
    }

    #[test]
    fn logical_and_golden() {
        let f = GqlNodeFilter::And(vec![GqlNodeFilter::IsActive(true)]);
        assert_eq!(
            serde_json::to_value(&f).unwrap(),
            serde_json::json!({"and": [{"isActive": true}]})
        );
    }

    #[test]
    fn datetime_value_golden() {
        // Serialization uses the schema field name `dtime`...
        let v = Value::DTime("2020-01-01T00:00:00Z".into());
        assert_eq!(
            serde_json::to_value(&v).unwrap(),
            serde_json::json!({"dtime": "2020-01-01T00:00:00Z"})
        );
    }
}

#[cfg(test)]
mod empty_combinator_tests {
    use super::*;

    // Empty `and`/`or` lists are rejected in every conversion — critically for
    // `or`, whose previous fallback (match-everything) inverted the caller's
    // intent and was a fail-open where these filters scope access control
    // (`GraphRowFilter` feeds the stored `GraphAccessFilter`).
    #[test]
    fn empty_combinators_are_rejected() {
        for (name, filter) in [
            ("and", GqlFilter::And(vec![])),
            ("or", GqlFilter::Or(vec![])),
        ] {
            let Err(err) = DynFilter::try_from(filter) else {
                panic!("GqlFilter {name}: empty combinator must be rejected");
            };
            assert!(
                err.to_string().contains("requires non-empty list"),
                "GqlFilter {name}: unexpected error {err}"
            );
        }
    }

    // Single-element combinators still convert — the rejection is only about
    // empty lists, not about unary composition.
    #[test]
    fn single_element_combinators_convert() {
        let node_filter = || {
            GqlNodeFilter::Property(PropertyFilterNew {
                name: "x".into(),
                where_: PropCondition::Eq(Value::I64(1)),
            })
        };
        assert!(DynFilter::try_from(GqlFilter::And(vec![GqlFilter::Node(node_filter())])).is_ok());
        assert!(DynFilter::try_from(GqlFilter::Or(vec![GqlFilter::Node(node_filter())])).is_ok());
    }
}

#[cfg(test)]
mod gql_filter_serde_tests {
    use super::*;

    fn node_prop_eq(name: &str, v: i64) -> GqlNodeFilter {
        GqlNodeFilter::Property(PropertyFilterNew {
            name: name.into(),
            where_: PropCondition::Eq(Value::I64(v)),
        })
    }

    // Golden fixtures: `GqlFilter`'s serde output IS the wire contract (GraphQL
    // variables) and the future stored-filter shape — it must match what
    // async-graphql's OneOfInput coercion accepts (externally tagged,
    // camelCase). A rename or tagging change here breaks the wire and any
    // persisted filter; these tests make that a compile-time-adjacent failure
    // instead of a production incident.
    #[test]
    fn serializes_to_the_oneof_wire_shape() {
        let cases = [
            (
                GqlFilter::Node(node_prop_eq("x", 1)),
                r#"{"node":{"property":{"name":"x","where":{"eq":{"i64":1}}}}}"#,
            ),
            (
                GqlFilter::And(vec![GqlFilter::Node(node_prop_eq("x", 1))]),
                r#"{"and":[{"node":{"property":{"name":"x","where":{"eq":{"i64":1}}}}}]}"#,
            ),
            (
                GqlFilter::Or(vec![GqlFilter::Node(node_prop_eq("x", 1))]),
                r#"{"or":[{"node":{"property":{"name":"x","where":{"eq":{"i64":1}}}}}]}"#,
            ),
            (
                GqlFilter::Not(Wrapped::from(GqlFilter::Node(node_prop_eq("x", 1)))),
                r#"{"not":{"node":{"property":{"name":"x","where":{"eq":{"i64":1}}}}}}"#,
            ),
        ];
        for (filter, expected) in cases {
            assert_eq!(serde_json::to_string(&filter).unwrap(), expected);
        }
    }

    #[test]
    fn round_trips_through_serde() {
        let filter = GqlFilter::And(vec![
            GqlFilter::Node(node_prop_eq("a", 1)),
            GqlFilter::Not(Wrapped::from(GqlFilter::Or(vec![GqlFilter::Node(
                node_prop_eq("b", 2),
            )]))),
        ]);
        let json = serde_json::to_string(&filter).unwrap();
        let back: GqlFilter = serde_json::from_str(&json).unwrap();
        assert_eq!(serde_json::to_string(&back).unwrap(), json);
    }

    // `not` composes end-to-end into a core filter.
    #[test]
    fn not_variant_converts_to_dyn_filter() {
        let filter = GqlFilter::Not(Wrapped::from(GqlFilter::Node(node_prop_eq("x", 1))));
        assert!(DynFilter::try_from(filter).is_ok());
    }
}

#[cfg(test)]
mod tree_grammar_tests {
    use super::*;
    use crate::model::graph::filter_expr_input::{GqlExpr, GqlFilterExpr, GqlTarget};

    // A permission grant written in the legacy grammar converts onto the tree
    // grammar with its policy placeholders untouched: `{"var": …}` is a
    // wire-level value, never a Prop, so nothing tries to evaluate it.
    #[test]
    fn legacy_grant_with_placeholders_converts_to_the_tree_grammar() {
        let legacy: GqlFilter = serde_json::from_value(serde_json::json!({
            "and": [
                { "node": { "name": { "where": { "isIn": { "var": "myCompanies" } } } } },
                { "node": { "property": { "name": "risk", "where": { "le": { "claim": "max_risk" } } } } }
            ]
        }))
        .unwrap();
        let GqlFilter::Expr(GqlFilterExpr::And(items)) = legacy.into_tree_grammar().unwrap() else {
            panic!("expected the tree grammar");
        };
        let GqlFilterExpr::IsIn(members) = &items[0] else {
            panic!("expected a membership test, got {:?}", items[0]);
        };
        assert!(matches!(&members.values, Value::Var(name) if name == "myCompanies"));
        assert!(matches!(
            &members.expr,
            GqlExpr::Read(read) if matches!(read.target, GqlTarget::Field(_))
        ));
        let GqlFilterExpr::Le(cmp) = &items[1] else {
            panic!("expected a comparison, got {:?}", items[1]);
        };
        assert!(matches!(&cmp.rhs, GqlExpr::Const(Value::Claim(name)) if name == "max_risk"));
    }

    // A filter already in the tree grammar is returned as it is.
    #[test]
    fn a_tree_grammar_filter_is_already_normal() {
        let tree = GqlFilter::Expr(GqlFilterExpr::View(vec![]));
        let json = serde_json::to_value(&tree).unwrap();
        assert_eq!(
            serde_json::to_value(tree.into_tree_grammar().unwrap()).unwrap(),
            json
        );
    }
}

#[cfg(test)]
mod fuzzy_search_tests {
    use super::*;
    use raphtory::{
        db::graph::views::filter::model::tree::{
            Entity, Expr, Field, FilterExpr, Scope, StrOp, Target,
        },
        prelude::Prop,
    };

    // The wire shape is externally tagged camelCase, like every other condition.
    #[test]
    fn serializes_to_the_wire_shape() {
        let cond = PropCondition::FuzzySearch(FuzzySearchExpr {
            value: "shivam".into(),
            levenshtein_distance: 2,
            prefix_match: false,
        });
        assert_eq!(
            serde_json::to_string(&cond).unwrap(),
            r#"{"fuzzySearch":{"value":"shivam","levenshteinDistance":2,"prefixMatch":false}}"#
        );
    }

    // Wire condition → core (operator, value) and back — the remote client's
    // round-trip for property fuzzy matching.
    #[test]
    // A legacy fuzzy condition lowers onto the tree with its parameters intact.
    #[test]
    fn property_fuzzy_lowers_onto_the_tree() {
        let filter = GqlNodeFilter::Property(PropertyFilterNew {
            name: "bio".into(),
            where_: PropCondition::FuzzySearch(FuzzySearchExpr {
                value: "graph enthusiast".into(),
                levenshtein_distance: 3,
                prefix_match: true,
            }),
        });
        let tree =
            FilterExpr::try_from(super::super::expr_lowering::lower_node_filter(&filter).unwrap())
                .unwrap();
        let FilterExpr::Str { op, rhs, .. } = tree else {
            panic!("expected a string predicate, got {tree:?}");
        };
        assert_eq!(
            op,
            StrOp::FuzzySearch {
                levenshtein_distance: 3,
                prefix_match: true,
            }
        );
        assert_eq!(rhs, Expr::Const(Prop::str("graph enthusiast")));
    }

    // Local node-name filter → wire condition (the reverse conversion the
    // Python remote client rides) preserves the fuzzy parameters.
    #[test]
    // The tree's fuzzy predicate keeps its parameters on the way to the wire.
    #[test]
    fn node_name_fuzzy_reaches_the_wire_intact() {
        let tree = FilterExpr::Str {
            op: StrOp::FuzzySearch {
                levenshtein_distance: 1,
                prefix_match: true,
            },
            lhs: Expr::Read {
                scope: Scope::new(Entity::Node),
                target: Target::Field(Field::Name),
            },
            rhs: Expr::Const(Prop::str("ben")),
        };
        let GqlFilter::Expr(GqlFilterExpr::FuzzySearch(f)) = GqlFilter::try_from(tree).unwrap()
        else {
            panic!("expected the fuzzy variant");
        };
        assert_eq!((f.levenshtein_distance, f.prefix_match), (1, true));
    }
}

#[cfg(test)]
mod conversion_hole_tests {
    use super::*;

    // `isSome: false` lowers to the IsNone operator (and vice versa) instead
    // of erroring — the two spellings are the same predicate.
    #[test]
    fn is_some_false_lowers_to_the_dual_operator() {
        let (op, _) = translate_prop_leaf_to_filter("p", &PropCondition::IsSome(false)).unwrap();
        assert_eq!(op, FilterOperator::IsNone);
        let (op, _) = translate_prop_leaf_to_filter("p", &PropCondition::IsNone(false)).unwrap();
        assert_eq!(op, FilterOperator::IsSome);
    }

    // Node-id ordering comparisons accept string GIDs, matching the local
    // expression api's `V: Into<GID>` bound.
    #[test]
    fn node_id_ordering_accepts_string_gids() {
        let filter = GqlNodeFilter::Id(NodeFieldWhere {
            where_: NodeFieldCondition::Gt(Value::Str("m".into())),
        });
        assert!(super::super::expr_lowering::lower_node_filter(&filter).is_ok());
    }

    // Aggregation ops on a degree filter fail with a clear message — degree is
    // a scalar, so an op chain over it is meaningless.
    #[test]
    fn degree_rejects_aggregation_ops() {
        use raphtory::{db::api::view::Filter as _, prelude::Graph};

        let filter = GqlNodeFilter::Degree(DegreeFilterNew {
            direction: DegreeDirection::Both,
            where_: PropCondition::Sum(Wrapped::from(PropCondition::Eq(Value::I64(3)))),
        });
        let result = super::super::expr_lowering::lower_node_filter(&filter)
            .and_then(FilterExpr::try_from)
            .and_then(|f| Graph::new().filter(f).map(|_| ()));
        let Err(err) = result else {
            panic!("degree with an op chain must be rejected");
        };
        assert!(
            err.to_string().contains("is not valid on a scalar"),
            "unexpected error: {err}"
        );
    }
}

#[cfg(test)]
mod exploded_edge_filter_tests {
    use super::*;
    use raphtory::{
        db::graph::views::filter::model::tree::{CmpOp, Entity, Expr, FilterExpr, Scope, Target},
        prelude::Prop,
    };

    fn exploded_prop_gt(name: &str, v: i64) -> GqlExplodedEdgeFilter {
        GqlExplodedEdgeFilter::Property(PropertyFilterNew {
            name: name.into(),
            where_: PropCondition::Gt(Value::I64(v)),
        })
    }

    // The wire shape follows the OneOfInput convention of every other filter:
    // externally tagged, camelCase.
    #[test]
    fn serializes_to_the_oneof_wire_shape() {
        let cases = [
            (
                GqlFilter::ExplodedEdge(exploded_prop_gt("w", 1)),
                r#"{"explodedEdge":{"property":{"name":"w","where":{"gt":{"i64":1}}}}}"#,
            ),
            (
                GqlFilter::ExplodedEdge(GqlExplodedEdgeFilter::Metadata(PropertyFilterNew {
                    name: "kind".into(),
                    where_: PropCondition::Eq(Value::Str("strong".into())),
                })),
                r#"{"explodedEdge":{"metadata":{"name":"kind","where":{"eq":{"str":"strong"}}}}}"#,
            ),
            (
                GqlFilter::ExplodedEdge(GqlExplodedEdgeFilter::And(vec![
                    exploded_prop_gt("w", 1),
                    GqlExplodedEdgeFilter::IsValid(true),
                ])),
                r#"{"explodedEdge":{"and":[{"property":{"name":"w","where":{"gt":{"i64":1}}}},{"isValid":true}]}}"#,
            ),
        ];
        for (filter, expected) in cases {
            assert_eq!(serde_json::to_string(&filter).unwrap(), expected);
        }
    }

    #[test]
    fn round_trips_through_serde() {
        let filter = GqlFilter::ExplodedEdge(GqlExplodedEdgeFilter::Not(Wrapped::from(
            GqlExplodedEdgeFilter::Or(vec![
                exploded_prop_gt("w", 1),
                GqlExplodedEdgeFilter::TemporalProperty(PropertyFilterNew {
                    name: "w".into(),
                    where_: PropCondition::Any(Wrapped::from(PropCondition::Eq(Value::I64(3)))),
                }),
            ]),
        )));
        let json = serde_json::to_string(&filter).unwrap();
        let back: GqlFilter = serde_json::from_str(&json).unwrap();
        assert_eq!(serde_json::to_string(&back).unwrap(), json);
    }

    // Composite → wire → composite is exact for every variant family the
    // Python builder can produce (property/metadata/temporal, view wrappers,
    // combinators, predicates, endpoints).
    #[test]
    // Every legacy exploded-edge wire form lowers onto a tree that compiles.
    #[test]
    fn legacy_wire_forms_lower_onto_trees_that_compile() {
        let prop = || exploded_prop_gt("w", 1);
        let time = |t: i64| GqlTimeInput::from(t);
        let cases = vec![
            prop(),
            GqlExplodedEdgeFilter::Metadata(PropertyFilterNew {
                name: "kind".into(),
                where_: PropCondition::Eq(Value::Str("strong".into())),
            }),
            GqlExplodedEdgeFilter::Window(ExplodedEdgeWindowExpr {
                start: time(2),
                end: time(4),
                expr: Wrapped::from(prop()),
            }),
            GqlExplodedEdgeFilter::Layers(ExplodedEdgeLayersExpr {
                names: vec!["knows".into()],
                expr: Wrapped::from(prop()),
            }),
            GqlExplodedEdgeFilter::And(vec![prop(), GqlExplodedEdgeFilter::IsValid(true)]),
            GqlExplodedEdgeFilter::Not(Wrapped::from(prop())),
            GqlExplodedEdgeFilter::Src(Wrapped::from(GqlNodeFilter::Name(NodeFieldWhere {
                where_: NodeFieldCondition::Eq(Value::Str("a".into())),
            }))),
        ];
        for original in cases {
            let wire = super::super::expr_lowering::lower_exploded_edge_filter(&original)
                .unwrap_or_else(|e| panic!("wire form does not lower: {original:?}: {e}"));
            let tree = FilterExpr::try_from(wire).unwrap();
            tree.compile()
                .unwrap_or_else(|e| panic!("tree does not compile: {tree}: {e}"));
        }
    }

    // The composite converts to a DynFilter, so the server can evaluate it
    // through the same `graph.filter(...)` machinery as node/edge filters.
    #[test]
    fn converts_to_dyn_filter() {
        let filter = GqlFilter::ExplodedEdge(exploded_prop_gt("w", 1));
        assert!(DynFilter::try_from(filter).is_ok());
    }

    // The exploded FilterTree kind flows into the wire enum — the client's
    // transport path.
    #[test]
    // A tree that mixes node and exploded-edge reads survives the trip to the
    // wire and back with both entities intact.
    #[test]
    fn mixed_trees_round_trip_through_the_wire_variant() {
        let read = |entity, name: &str| Expr::Read {
            scope: Scope::new(entity),
            target: Target::Property(name.into()),
        };
        let tree = FilterExpr::And(vec![
            FilterExpr::Cmp {
                op: CmpOp::Eq,
                lhs: read(Entity::Node, "x"),
                rhs: Expr::Const(Prop::I64(1)),
            },
            FilterExpr::Cmp {
                op: CmpOp::Gt,
                lhs: read(Entity::ExplodedEdge, "w"),
                rhs: Expr::Const(Prop::I64(1)),
            },
        ]);
        let gql = GqlFilter::try_from(tree.clone()).unwrap();
        assert!(
            matches!(gql, GqlFilter::Expr(_)),
            "expected the tree variant"
        );
        assert_eq!(FilterExpr::try_from(gql).unwrap(), tree);
    }

    // Empty combinators are rejected like everywhere else in this module.
    #[test]
    fn empty_combinators_are_rejected() {
        for (name, filter) in [
            ("and", GqlExplodedEdgeFilter::And(vec![])),
            ("or", GqlExplodedEdgeFilter::Or(vec![])),
        ] {
            let Err(err) = super::super::expr_lowering::lower_exploded_edge_filter(&filter) else {
                panic!("ExplodedEdgeFilter {name}: empty combinator must be rejected");
            };
            assert!(
                err.to_string().contains("requires non-empty list"),
                "ExplodedEdgeFilter {name}: unexpected error {err}"
            );
        }
    }
}
