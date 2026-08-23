use crate::model::graph::{node_id::GqlNodeId, property::Value, timeindex::GqlTimeInput};
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
            view::{internal::GraphView, BoxableGraphView},
        },
        graph::views::filter::{
            model::{
                degree_filter::DegreeFilter,
                edge_filter::{CompositeEdgeFilter, EdgeFilter},
                exploded_edge_filter::{CompositeExplodedEdgeFilter, ExplodedEdgeFilter},
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
                snapshot_filter::{
                    SnapshotAt as SnapshotAtWrap, SnapshotLatest as SnapshotLatestWrap,
                },
                windowed_filter::Windowed,
                ComposableFilter, DynFilter, DynView, FilterTree, GraphViewOp, ViewWrapOps,
            },
            CreateFilter,
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use raphtory_api::core::{
    entities::{properties::prop::Prop, Layer, GID},
    storage::timeindex::{AsTime, EventTime},
    utils::time::IntoTime,
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
    type EntityFiltered<'graph, G: GraphViewOps<'graph>>
        = Arc<dyn BoxableGraphView + 'graph>
    where
        Self: 'graph;

    type NodeFilter<'graph, G: GraphView + 'graph> = Arc<dyn NodeOp<Output = bool> + 'graph>;

    type FilteredGraph<'graph, G>
        = Arc<dyn BoxableGraphView + 'graph>
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        DynFilter::try_from(self)?.create_filter(graph)
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        DynFilter::try_from(self)?.create_node_filter(graph)
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        DynFilter::try_from(self.clone())?.filter_graph_view(graph)
    }
}

impl TryFrom<CompositeNodeFilter> for GqlFilter {
    type Error = GraphError;
    fn try_from(f: CompositeNodeFilter) -> Result<Self, Self::Error> {
        Ok(GqlFilter::Node(f.try_into()?))
    }
}

impl TryFrom<CompositeEdgeFilter> for GqlFilter {
    type Error = GraphError;
    fn try_from(f: CompositeEdgeFilter) -> Result<Self, Self::Error> {
        Ok(GqlFilter::Edge(f.try_into()?))
    }
}

impl TryFrom<CompositeExplodedEdgeFilter> for GqlFilter {
    type Error = GraphError;
    fn try_from(f: CompositeExplodedEdgeFilter) -> Result<Self, Self::Error> {
        Ok(GqlFilter::ExplodedEdge(f.try_into()?))
    }
}

/// Build the nested wire form of a graph-view chain (outermost-first ops →
/// nested `expr` fields). `Layer::All` ops restrict nothing and are dropped.
fn view_ops_to_graph_filter(ops: Vec<GraphViewOp>) -> Result<GqlGraphFilter, GraphError> {
    let time_input = |t: EventTime| {
        GqlTimeInput(raphtory_api::core::utils::time::InputTime::Indexed(
            t.t(),
            t.i(),
        ))
    };
    let mut acc: Option<GqlGraphFilter> = None;
    for op in ops.into_iter().rev() {
        let expr = acc.take().map(wrap);
        let next = match op {
            GraphViewOp::Window { start, end } => GqlGraphFilter::Window(GraphWindowExpr {
                start: time_input(start),
                end: time_input(end),
                expr,
            }),
            GraphViewOp::Latest => GqlGraphFilter::Latest(GraphUnaryExpr { expr }),
            GraphViewOp::SnapshotAt(t) => GqlGraphFilter::SnapshotAt(GraphTimeExpr {
                time: time_input(t),
                expr,
            }),
            GraphViewOp::SnapshotLatest => GqlGraphFilter::SnapshotLatest(GraphUnaryExpr { expr }),
            GraphViewOp::Layers(layer) => {
                if matches!(layer, Layer::All) {
                    // No restriction — skip the op, keep the accumulated chain.
                    acc = expr.map(|w| w.deref().clone());
                    continue;
                }
                GqlGraphFilter::Layers(GraphLayersExpr {
                    names: layer_to_names(&layer)?,
                    expr,
                })
            }
        };
        acc = Some(next);
    }
    acc.ok_or_else(|| GraphError::InvalidGqlFilter("graph-view filter with no restrictions".into()))
}

impl TryFrom<FilterTree> for GqlFilter {
    type Error = GraphError;

    fn try_from(tree: FilterTree) -> Result<Self, Self::Error> {
        Ok(match tree {
            FilterTree::Node(f) => GqlFilter::Node(f.try_into()?),
            FilterTree::Edge(f) => GqlFilter::Edge(f.try_into()?),
            FilterTree::ExplodedEdge(f) => GqlFilter::ExplodedEdge(f.try_into()?),
            FilterTree::View(ops) => GqlFilter::Graph(view_ops_to_graph_filter(ops)?),
            FilterTree::And(items) => GqlFilter::And(
                items
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            FilterTree::Or(items) => GqlFilter::Or(
                items
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            FilterTree::Not(inner) => GqlFilter::Not(wrap((*inner).try_into()?)),
        })
    }
}

impl TryFrom<GqlFilter> for DynFilter {
    type Error = GraphError;

    fn try_from(value: GqlFilter) -> Result<Self, Self::Error> {
        let filter = match value {
            GqlFilter::Node(f) => Arc::new(CompositeNodeFilter::try_from(f)?) as DynFilter,
            GqlFilter::Edge(f) => Arc::new(CompositeEdgeFilter::try_from(f)?) as DynFilter,
            GqlFilter::ExplodedEdge(f) => {
                Arc::new(CompositeExplodedEdgeFilter::try_from(f)?) as DynFilter
            }
            GqlFilter::Graph(f) => DynView::try_from(f)?,
            GqlFilter::And(filters) => {
                let mut filters = filters.into_iter().map(DynFilter::try_from);
                // An empty combinator is almost always a caller bug (a filter
                // list built from an empty source). Reject it rather than
                // guessing an identity — for `or` in particular, the previous
                // fallback (match-everything) inverted the caller's intent,
                // which is a fail-open when the filter scopes access control.
                // Matches the composite conversions' convention above.
                let first = filters.next().transpose()?.ok_or_else(|| {
                    GraphError::InvalidGqlFilter("Filter 'and' requires non-empty list".into())
                })?;
                filters.try_fold(first, |combined, filter| {
                    Ok::<_, GraphError>(Arc::new(combined.and(filter?)) as DynFilter)
                })?
            }
            GqlFilter::Or(filters) => {
                let mut filters = filters.into_iter().map(DynFilter::try_from);
                let first = filters.next().transpose()?.ok_or_else(|| {
                    GraphError::InvalidGqlFilter("Filter 'or' requires non-empty list".into())
                })?;
                filters.try_fold(first, |combined, filter| {
                    Ok::<_, GraphError>(Arc::new(combined.or(filter?)) as DynFilter)
                })?
            }
            GqlFilter::Not(inner) => {
                let inner = DynFilter::try_from(inner.deref().clone())?;
                Arc::new(inner.not()) as DynFilter
            }
            // Flat view spellings delegate to the graph-filter conversion.
            GqlFilter::Window(w) => DynView::try_from(GqlGraphFilter::Window(w))?,
            GqlFilter::At(t) => DynView::try_from(GqlGraphFilter::At(t))?,
            GqlFilter::Before(t) => DynView::try_from(GqlGraphFilter::Before(t))?,
            GqlFilter::After(t) => DynView::try_from(GqlGraphFilter::After(t))?,
            GqlFilter::Latest(u) => DynView::try_from(GqlGraphFilter::Latest(u))?,
            GqlFilter::SnapshotAt(t) => DynView::try_from(GqlGraphFilter::SnapshotAt(t))?,
            GqlFilter::SnapshotLatest(u) => DynView::try_from(GqlGraphFilter::SnapshotLatest(u))?,
            GqlFilter::Layers(l) => DynView::try_from(GqlGraphFilter::Layers(l))?,
        };
        Ok(filter)
    }
}

/// Boolean expression over a built-in node field (ID, name, or type).
///
/// This is used by `NodeFieldFilterNew.where_` when filtering a specific
/// `NodeField`.
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

/// Filters a built-in node field (`id`, `name`, `type`) using a `NodeFieldCondition`.
///
/// Example (GraphQL):
/// ```graphql
/// { Node: { field: NodeName, where: { Contains: "ali" } } }
/// ```
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeFieldFilterNew {
    /// Which built-in field to filter.
    pub field: NodeField,
    /// Condition applied to the selected field.
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
/// - built-in node fields (`Node` / `NodeFieldFilterNew`),
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
    /// Filters a built-in node field (ID, name, or type).
    Node(NodeFieldFilterNew),

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
            GqlNodeFilter::Degree(degree) => {
                let core_direction: Direction = degree.direction.into();

                let field_name: String = degree.direction.into();

                let mut ops = Vec::new();
                let mut cursor = &degree.where_;
                while let Some(inner) = peel_prop_wrappers_and_collect_ops(cursor, &mut ops) {
                    cursor = inner;
                }
                // Degree is a scalar — aggregation/selector ops (sum/first/…)
                // have nothing to operate on, and the core filter rejects them
                // at evaluation time. Fail at conversion with a clear message.
                if !ops.is_empty() {
                    return Err(GraphError::InvalidGqlFilter(
                        "degree filters take a plain comparison; aggregation ops are not supported"
                            .into(),
                    ));
                }
                let (operator, value) = translate_prop_leaf_to_filter(&field_name, cursor)?;
                Ok(CompositeNodeFilter::Degree(DegreeFilter {
                    direction: core_direction,
                    operator,
                    value,
                    ops,
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
                    w.start.into_time(),
                    w.end.into_time(),
                    inner,
                ))))
            }

            GqlNodeFilter::At(t) => {
                let inner: CompositeNodeFilter = t.expr.deref().clone().try_into()?;
                let et = t.time.into_time();
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
                    EventTime::end(t.time.t()),
                    inner,
                ))))
            }

            GqlNodeFilter::After(t) => {
                let inner: CompositeNodeFilter = t.expr.deref().clone().try_into()?;
                let start = EventTime::start(t.time.t().saturating_add(1));
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
                    SnapshotAtWrap::new(t.time.into_time(), inner),
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
                    w.start.into_time(),
                    w.end.into_time(),
                    inner,
                ))))
            }

            GqlEdgeFilter::At(t) => {
                let inner: CompositeEdgeFilter = t.expr.deref().clone().try_into()?;
                let et = t.time.into_time();
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
                    EventTime::end(t.time.t()),
                    inner,
                ))))
            }

            GqlEdgeFilter::After(t) => {
                let inner: CompositeEdgeFilter = t.expr.deref().clone().try_into()?;
                let start = EventTime::start(t.time.t().saturating_add(1));
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
                    SnapshotAtWrap::new(t.time.into_time(), inner),
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

fn build_exploded_edge_filter_from_prop_condition(
    prop_ref: PropertyRef,
    cond: &PropCondition,
) -> Result<CompositeExplodedEdgeFilter, GraphError> {
    use PropCondition::*;

    match cond {
        And(list) => {
            let mut it = list.iter();
            let first = it
                .next()
                .ok_or_else(|| GraphError::InvalidGqlFilter("and expects non-empty list".into()))?;
            let mut acc = build_exploded_edge_filter_from_prop_condition(prop_ref.clone(), first)?;
            for c in it {
                let next = build_exploded_edge_filter_from_prop_condition(prop_ref.clone(), c)?;
                acc = CompositeExplodedEdgeFilter::And(Box::new(acc), Box::new(next));
            }
            Ok(acc)
        }
        Or(list) => {
            let mut it = list.iter();
            let first = it
                .next()
                .ok_or_else(|| GraphError::InvalidGqlFilter("or expects non-empty list".into()))?;
            let mut acc = build_exploded_edge_filter_from_prop_condition(prop_ref.clone(), first)?;
            for c in it {
                let next = build_exploded_edge_filter_from_prop_condition(prop_ref.clone(), c)?;
                acc = CompositeExplodedEdgeFilter::Or(Box::new(acc), Box::new(next));
            }
            Ok(acc)
        }
        Not(inner) => {
            let ef = build_exploded_edge_filter_from_prop_condition(prop_ref, inner)?;
            Ok(CompositeExplodedEdgeFilter::Not(Box::new(ef)))
        }
        _ => {
            let pf = build_property_filter_from_condition_with_entity::<ExplodedEdgeFilter>(
                prop_ref,
                cond,
                ExplodedEdgeFilter,
            )?;
            Ok(CompositeExplodedEdgeFilter::Property(pf))
        }
    }
}

impl TryFrom<GqlExplodedEdgeFilter> for CompositeExplodedEdgeFilter {
    type Error = GraphError;
    fn try_from(filter: GqlExplodedEdgeFilter) -> Result<Self, Self::Error> {
        match filter {
            GqlExplodedEdgeFilter::Src(nf) => {
                let nf: CompositeNodeFilter = nf.deref().clone().try_into()?;
                Ok(CompositeExplodedEdgeFilter::Src(nf))
            }
            GqlExplodedEdgeFilter::Dst(nf) => {
                let nf: CompositeNodeFilter = nf.deref().clone().try_into()?;
                Ok(CompositeExplodedEdgeFilter::Dst(nf))
            }
            GqlExplodedEdgeFilter::Property(prop) => {
                let prop_ref = PropertyRef::Property(prop.name.clone());
                build_exploded_edge_filter_from_prop_condition(prop_ref, &prop.where_)
            }
            GqlExplodedEdgeFilter::Metadata(prop) => {
                let prop_ref = PropertyRef::Metadata(prop.name.clone());
                build_exploded_edge_filter_from_prop_condition(prop_ref, &prop.where_)
            }
            GqlExplodedEdgeFilter::TemporalProperty(prop) => {
                let prop_ref = PropertyRef::TemporalProperty(prop.name.clone());
                build_exploded_edge_filter_from_prop_condition(prop_ref, &prop.where_)
            }
            GqlExplodedEdgeFilter::And(and_filters) => {
                let mut iter = and_filters.into_iter().map(TryInto::try_into);
                let first = iter.next().ok_or_else(|| {
                    GraphError::InvalidGqlFilter("Filter 'and' requires non-empty list".into())
                })??;
                Ok(iter.try_fold(first, |acc, next| {
                    let n = next?;
                    Ok::<_, GraphError>(CompositeExplodedEdgeFilter::And(
                        Box::new(acc),
                        Box::new(n),
                    ))
                })?)
            }
            GqlExplodedEdgeFilter::Or(or_filters) => {
                let mut iter = or_filters.into_iter().map(TryInto::try_into);
                let first = iter.next().ok_or_else(|| {
                    GraphError::InvalidGqlFilter("Filter 'or' requires non-empty list".into())
                })??;
                Ok(iter.try_fold(first, |acc, next| {
                    let n = next?;
                    Ok::<_, GraphError>(CompositeExplodedEdgeFilter::Or(Box::new(acc), Box::new(n)))
                })?)
            }
            GqlExplodedEdgeFilter::Not(not_filters) => {
                let inner = CompositeExplodedEdgeFilter::try_from(not_filters.deref().clone())?;
                Ok(CompositeExplodedEdgeFilter::Not(Box::new(inner)))
            }
            GqlExplodedEdgeFilter::Window(w) => {
                let inner: CompositeExplodedEdgeFilter = w.expr.deref().clone().try_into()?;
                Ok(CompositeExplodedEdgeFilter::Windowed(Box::new(
                    Windowed::new(w.start.into_time(), w.end.into_time(), inner),
                )))
            }

            GqlExplodedEdgeFilter::At(t) => {
                let inner: CompositeExplodedEdgeFilter = t.expr.deref().clone().try_into()?;
                let et = t.time.into_time();
                Ok(CompositeExplodedEdgeFilter::Windowed(Box::new(
                    Windowed::new(et, EventTime::end(et.t().saturating_add(1)), inner),
                )))
            }

            GqlExplodedEdgeFilter::Before(t) => {
                let inner: CompositeExplodedEdgeFilter = t.expr.deref().clone().try_into()?;
                Ok(CompositeExplodedEdgeFilter::Windowed(Box::new(
                    Windowed::new(
                        EventTime::start(i64::MIN),
                        EventTime::end(t.time.t()),
                        inner,
                    ),
                )))
            }

            GqlExplodedEdgeFilter::After(t) => {
                let inner: CompositeExplodedEdgeFilter = t.expr.deref().clone().try_into()?;
                let start = EventTime::start(t.time.t().saturating_add(1));
                Ok(CompositeExplodedEdgeFilter::Windowed(Box::new(
                    Windowed::new(start, EventTime::end(i64::MAX), inner),
                )))
            }

            GqlExplodedEdgeFilter::Latest(u) => {
                let inner: CompositeExplodedEdgeFilter = u.expr.deref().clone().try_into()?;
                Ok(CompositeExplodedEdgeFilter::Latest(Box::new(
                    LatestWrap::new(inner),
                )))
            }

            GqlExplodedEdgeFilter::SnapshotAt(t) => {
                let inner: CompositeExplodedEdgeFilter = t.expr.deref().clone().try_into()?;
                Ok(CompositeExplodedEdgeFilter::SnapshotAt(Box::new(
                    SnapshotAtWrap::new(t.time.into_time(), inner),
                )))
            }

            GqlExplodedEdgeFilter::SnapshotLatest(u) => {
                let inner: CompositeExplodedEdgeFilter = u.expr.deref().clone().try_into()?;
                Ok(CompositeExplodedEdgeFilter::SnapshotLatest(Box::new(
                    SnapshotLatestWrap::new(inner),
                )))
            }

            GqlExplodedEdgeFilter::Layers(l) => {
                let layer = Layer::from(l.names.clone());
                let inner: CompositeExplodedEdgeFilter = l.expr.deref().clone().try_into()?;
                Ok(CompositeExplodedEdgeFilter::Layered(Box::new(
                    Layered::new(layer, inner),
                )))
            }

            GqlExplodedEdgeFilter::IsActive(true) => {
                Ok(CompositeExplodedEdgeFilter::IsActiveEdge(IsActiveEdge))
            }
            GqlExplodedEdgeFilter::IsActive(false) => Ok(CompositeExplodedEdgeFilter::Not(
                Box::new(CompositeExplodedEdgeFilter::IsActiveEdge(IsActiveEdge)),
            )),

            GqlExplodedEdgeFilter::IsValid(true) => {
                Ok(CompositeExplodedEdgeFilter::IsValidEdge(IsValidEdge))
            }
            GqlExplodedEdgeFilter::IsValid(false) => Ok(CompositeExplodedEdgeFilter::Not(
                Box::new(CompositeExplodedEdgeFilter::IsValidEdge(IsValidEdge)),
            )),

            GqlExplodedEdgeFilter::IsDeleted(true) => {
                Ok(CompositeExplodedEdgeFilter::IsDeletedEdge(IsDeletedEdge))
            }
            GqlExplodedEdgeFilter::IsDeleted(false) => Ok(CompositeExplodedEdgeFilter::Not(
                Box::new(CompositeExplodedEdgeFilter::IsDeletedEdge(IsDeletedEdge)),
            )),

            GqlExplodedEdgeFilter::IsSelfLoop(true) => {
                Ok(CompositeExplodedEdgeFilter::IsSelfLoopEdge(IsSelfLoopEdge))
            }
            GqlExplodedEdgeFilter::IsSelfLoop(false) => Ok(CompositeExplodedEdgeFilter::Not(
                Box::new(CompositeExplodedEdgeFilter::IsSelfLoopEdge(IsSelfLoopEdge)),
            )),
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
// `PyFilterExpr` API (produces `CompositeNodeFilter`) then converts to
// `GqlNodeFilter` for GraphQL transmission. The forward path
// (`TryFrom<GqlNodeFilter> for CompositeNodeFilter`, above) already exists.
//
// Not all `CompositeNodeFilter` variants have a lossless GQL counterpart —
// for example, `Layer::All` has no single-layer-name representation on the
// wire. Unsupported cases surface as `GraphError::InvalidGqlFilter`.

fn wrap<T>(t: T) -> Wrapped<T> {
    Wrapped(Box::new(t))
}

/// `FilterValue` (used by field filters) → wire `Value`.
fn filter_value_to_value(v: &FilterValue) -> Result<Value, GraphError> {
    Ok(match v {
        FilterValue::Single(s) => Value::Str(s.clone()),
        FilterValue::Set(strs) => {
            // Set semantics — element order is irrelevant on the wire.
            Value::List(strs.iter().map(|s| Value::Str(s.clone())).collect())
        }
        FilterValue::ID(GID::Str(s)) => Value::Str(s.clone()),
        FilterValue::ID(GID::U64(u)) => Value::U64(*u),
        FilterValue::IDSet(gids) => {
            let items: Vec<Value> = gids
                .iter()
                .map(|g| match g {
                    GID::Str(s) => Value::Str(s.clone()),
                    GID::U64(u) => Value::U64(*u),
                })
                .collect();
            Value::List(items)
        }
    })
}

/// `PropertyFilterValue` → wire `Value` — used inside `PropCondition`.
/// For `None` (used only with `IsSome`/`IsNone`) callers should route
/// separately since `PropCondition::IsSome`/`IsNone` take `bool`, not
/// `Value`.
fn prop_filter_value_to_value(v: &PropertyFilterValue) -> Result<Value, GraphError> {
    match v {
        PropertyFilterValue::Single(p) => Value::try_from(p),
        PropertyFilterValue::Set(ps) => {
            // Set semantics — element order is irrelevant on the wire.
            let items: Vec<Value> = ps.iter().map(Value::try_from).collect::<Result<_, _>>()?;
            Ok(Value::List(items))
        }
        PropertyFilterValue::None => Err(GraphError::InvalidGqlFilter(
            "cannot render PropertyFilterValue::None as a wire Value".into(),
        )),
    }
}

/// Build a base `PropCondition` from an operator + value (no `ops` wrapping).
fn build_base_prop_condition(
    operator: FilterOperator,
    value: &PropertyFilterValue,
) -> Result<PropCondition, GraphError> {
    use FilterOperator as FO;
    Ok(match operator {
        FO::Eq => PropCondition::Eq(prop_filter_value_to_value(value)?),
        FO::Ne => PropCondition::Ne(prop_filter_value_to_value(value)?),
        FO::Gt => PropCondition::Gt(prop_filter_value_to_value(value)?),
        FO::Ge => PropCondition::Ge(prop_filter_value_to_value(value)?),
        FO::Lt => PropCondition::Lt(prop_filter_value_to_value(value)?),
        FO::Le => PropCondition::Le(prop_filter_value_to_value(value)?),
        FO::StartsWith => PropCondition::StartsWith(prop_filter_value_to_value(value)?),
        FO::EndsWith => PropCondition::EndsWith(prop_filter_value_to_value(value)?),
        FO::Contains => PropCondition::Contains(prop_filter_value_to_value(value)?),
        FO::NotContains => PropCondition::NotContains(prop_filter_value_to_value(value)?),
        FO::IsIn => PropCondition::IsIn(prop_filter_value_to_value(value)?),
        FO::IsNotIn => PropCondition::IsNotIn(prop_filter_value_to_value(value)?),
        FO::IsSome => PropCondition::IsSome(true),
        FO::IsNone => PropCondition::IsNone(true),
        FO::FuzzySearch {
            levenshtein_distance,
            prefix_match,
        } => {
            let PropertyFilterValue::Single(Prop::Str(v)) = value else {
                return Err(GraphError::InvalidGqlFilter(
                    "fuzzySearch requires a string value".into(),
                ));
            };
            PropCondition::FuzzySearch(FuzzySearchExpr {
                value: v.to_string(),
                levenshtein_distance,
                prefix_match,
            })
        }
    })
}

/// Rebuild the wire tree from `ops`. Both the peel (`peel_prop_wrappers_and_
/// collect_ops`) and core eval (`evaluate.rs`) treat the OUTERMOST tree node
/// as the FIRST-applied op: tree `First(Sum(x))` ⇔ ops `[First, Sum]` ⇔ chain
/// `.first().sum()`. Since folding wraps inside-out (each wrap becomes the new
/// outermost), we iterate `ops` in REVERSE so that `ops[0]` ends up outermost.
///
/// Beware: core's `Display` prints the OPPOSITE nesting (`[First, Sum]` prints
/// as `"sum(first(x))"`) — don't validate this mapping against Display strings.
fn apply_ops_to_condition(base: PropCondition, ops: &[Op]) -> PropCondition {
    // Fold reversed so `ops[0]` becomes the outermost wrapper (see doc comment).
    ops.iter().rev().fold(base, |acc, op| match op {
        Op::First => PropCondition::First(wrap(acc)),
        Op::Last => PropCondition::Last(wrap(acc)),
        Op::Len => PropCondition::Len(wrap(acc)),
        Op::Sum => PropCondition::Sum(wrap(acc)),
        Op::Avg => PropCondition::Avg(wrap(acc)),
        Op::Min => PropCondition::Min(wrap(acc)),
        Op::Max => PropCondition::Max(wrap(acc)),
        Op::Any => PropCondition::Any(wrap(acc)),
        Op::All => PropCondition::All(wrap(acc)),
    })
}

/// Map a `Filter` (built-in node field filter) → wire `NodeFieldFilterNew`.
fn filter_to_node_field(f: Filter) -> Result<NodeFieldFilterNew, GraphError> {
    let field = match f.field_name.as_str() {
        "node_id" => NodeField::NodeId,
        "node_name" => NodeField::NodeName,
        "node_type" => NodeField::NodeType,
        other => {
            return Err(GraphError::InvalidGqlFilter(format!(
                "unknown node field name for wire conversion: {}",
                other
            )))
        }
    };
    let val = filter_value_to_value(&f.field_value)?;
    let where_ = match f.operator {
        FilterOperator::Eq => NodeFieldCondition::Eq(val),
        FilterOperator::Ne => NodeFieldCondition::Ne(val),
        FilterOperator::Gt => NodeFieldCondition::Gt(val),
        FilterOperator::Ge => NodeFieldCondition::Ge(val),
        FilterOperator::Lt => NodeFieldCondition::Lt(val),
        FilterOperator::Le => NodeFieldCondition::Le(val),
        FilterOperator::StartsWith => NodeFieldCondition::StartsWith(val),
        FilterOperator::EndsWith => NodeFieldCondition::EndsWith(val),
        FilterOperator::Contains => NodeFieldCondition::Contains(val),
        FilterOperator::NotContains => NodeFieldCondition::NotContains(val),
        FilterOperator::IsIn => NodeFieldCondition::IsIn(val),
        FilterOperator::IsNotIn => NodeFieldCondition::IsNotIn(val),
        FilterOperator::FuzzySearch {
            levenshtein_distance,
            prefix_match,
        } => {
            let Value::Str(v) = val else {
                return Err(GraphError::InvalidGqlFilter(
                    "fuzzySearch requires a string value".into(),
                ));
            };
            NodeFieldCondition::FuzzySearch(FuzzySearchExpr {
                value: v,
                levenshtein_distance,
                prefix_match,
            })
        }
        other => {
            return Err(GraphError::InvalidGqlFilter(format!(
                "unsupported operator for node field: {:?}",
                other
            )))
        }
    };
    Ok(NodeFieldFilterNew { field, where_ })
}

/// Map a `Layer` (engine) → `Vec<String>` names for the wire.
fn layer_to_names(layer: &Layer) -> Result<Vec<String>, GraphError> {
    match layer {
        Layer::One(name) => Ok(vec![name.to_string()]),
        Layer::Multiple(names) => Ok(names.iter().map(|s| s.to_string()).collect()),
        Layer::Default => Ok(vec!["_default".to_string()]),
        // No layers — the empty name list (`Layer::from_iter([])` maps back
        // to `Layer::None`, so the round-trip is exact).
        Layer::None => Ok(vec![]),
        // All layers is no restriction at all — callers drop the layer
        // wrapper entirely instead of rendering it.
        Layer::All => Err(GraphError::InvalidGqlFilter(
            "Layer::All is no layer restriction — omit the layer wrapper".into(),
        )),
    }
}

impl TryFrom<CompositeNodeFilter> for GqlNodeFilter {
    type Error = GraphError;
    fn try_from(f: CompositeNodeFilter) -> Result<Self, Self::Error> {
        Ok(match f {
            CompositeNodeFilter::Node(filter) => GqlNodeFilter::Node(filter_to_node_field(filter)?),

            CompositeNodeFilter::Property(pf) => {
                let base = build_base_prop_condition(pf.operator, &pf.prop_value)?;
                let where_ = apply_ops_to_condition(base, &pf.ops);
                let name = pf.prop_ref.name().to_string();
                match pf.prop_ref {
                    PropertyRef::Property(_) => {
                        GqlNodeFilter::Property(PropertyFilterNew { name, where_ })
                    }
                    PropertyRef::Metadata(_) => {
                        GqlNodeFilter::Metadata(PropertyFilterNew { name, where_ })
                    }
                    PropertyRef::TemporalProperty(_) => {
                        GqlNodeFilter::TemporalProperty(PropertyFilterNew { name, where_ })
                    }
                }
            }

            CompositeNodeFilter::Degree(df) => {
                let direction = match df.direction {
                    Direction::IN => DegreeDirection::In,
                    Direction::OUT => DegreeDirection::Out,
                    Direction::BOTH => DegreeDirection::Both,
                };
                let base = build_base_prop_condition(df.operator, &df.value)?;
                let where_ = apply_ops_to_condition(base, &df.ops);
                GqlNodeFilter::Degree(DegreeFilterNew { direction, where_ })
            }

            CompositeNodeFilter::IsActiveNode(_) => GqlNodeFilter::IsActive(true),

            CompositeNodeFilter::And(l, r) => {
                GqlNodeFilter::And(vec![(*l).try_into()?, (*r).try_into()?])
            }
            CompositeNodeFilter::Or(l, r) => {
                GqlNodeFilter::Or(vec![(*l).try_into()?, (*r).try_into()?])
            }
            CompositeNodeFilter::Not(inner) => GqlNodeFilter::Not(wrap((*inner).try_into()?)),

            CompositeNodeFilter::Windowed(w) => GqlNodeFilter::Window(NodeWindowExpr {
                start: w.start.t().into(),
                end: w.end.t().into(),
                expr: wrap(w.inner.try_into()?),
            }),

            CompositeNodeFilter::Latest(l) => GqlNodeFilter::Latest(NodeUnaryExpr {
                expr: wrap(l.inner.try_into()?),
            }),

            CompositeNodeFilter::SnapshotAt(s) => GqlNodeFilter::SnapshotAt(NodeTimeExpr {
                time: s.time.t().into(),
                expr: wrap(s.inner.try_into()?),
            }),

            CompositeNodeFilter::SnapshotLatest(s) => {
                GqlNodeFilter::SnapshotLatest(NodeUnaryExpr {
                    expr: wrap(s.inner.try_into()?),
                })
            }

            CompositeNodeFilter::Layered(l) => {
                if matches!(l.layer, Layer::All) {
                    // Restricting to ALL layers restricts nothing — drop the
                    // wrapper and convert the inner filter directly.
                    l.inner.try_into()?
                } else {
                    GqlNodeFilter::Layers(NodeLayersExpr {
                        names: layer_to_names(&l.layer)?,
                        expr: wrap(l.inner.try_into()?),
                    })
                }
            }
        })
    }
}

impl TryFrom<CompositeEdgeFilter> for GqlEdgeFilter {
    type Error = GraphError;
    fn try_from(f: CompositeEdgeFilter) -> Result<Self, Self::Error> {
        Ok(match f {
            // Endpoint filters recurse into the node converter — an edge
            // filter on src/dst wraps a full node filter.
            CompositeEdgeFilter::Src(nf) => GqlEdgeFilter::Src(wrap(nf.try_into()?)),
            CompositeEdgeFilter::Dst(nf) => GqlEdgeFilter::Dst(wrap(nf.try_into()?)),

            CompositeEdgeFilter::Property(pf) => {
                let base = build_base_prop_condition(pf.operator, &pf.prop_value)?;
                let where_ = apply_ops_to_condition(base, &pf.ops);
                let name = pf.prop_ref.name().to_string();
                match pf.prop_ref {
                    PropertyRef::Property(_) => {
                        GqlEdgeFilter::Property(PropertyFilterNew { name, where_ })
                    }
                    PropertyRef::Metadata(_) => {
                        GqlEdgeFilter::Metadata(PropertyFilterNew { name, where_ })
                    }
                    PropertyRef::TemporalProperty(_) => {
                        GqlEdgeFilter::TemporalProperty(PropertyFilterNew { name, where_ })
                    }
                }
            }

            CompositeEdgeFilter::IsActiveEdge(_) => GqlEdgeFilter::IsActive(true),
            CompositeEdgeFilter::IsValidEdge(_) => GqlEdgeFilter::IsValid(true),
            CompositeEdgeFilter::IsDeletedEdge(_) => GqlEdgeFilter::IsDeleted(true),
            CompositeEdgeFilter::IsSelfLoopEdge(_) => GqlEdgeFilter::IsSelfLoop(true),

            CompositeEdgeFilter::And(l, r) => {
                GqlEdgeFilter::And(vec![(*l).try_into()?, (*r).try_into()?])
            }
            CompositeEdgeFilter::Or(l, r) => {
                GqlEdgeFilter::Or(vec![(*l).try_into()?, (*r).try_into()?])
            }
            CompositeEdgeFilter::Not(inner) => GqlEdgeFilter::Not(wrap((*inner).try_into()?)),

            CompositeEdgeFilter::Windowed(w) => GqlEdgeFilter::Window(EdgeWindowExpr {
                start: w.start.t().into(),
                end: w.end.t().into(),
                expr: wrap(w.inner.try_into()?),
            }),

            CompositeEdgeFilter::Latest(l) => GqlEdgeFilter::Latest(EdgeUnaryExpr {
                expr: wrap(l.inner.try_into()?),
            }),

            CompositeEdgeFilter::SnapshotAt(s) => GqlEdgeFilter::SnapshotAt(EdgeTimeExpr {
                time: s.time.t().into(),
                expr: wrap(s.inner.try_into()?),
            }),

            CompositeEdgeFilter::SnapshotLatest(s) => {
                GqlEdgeFilter::SnapshotLatest(EdgeUnaryExpr {
                    expr: wrap(s.inner.try_into()?),
                })
            }

            CompositeEdgeFilter::Layered(l) => {
                if matches!(l.layer, Layer::All) {
                    l.inner.try_into()?
                } else {
                    GqlEdgeFilter::Layers(EdgeLayersExpr {
                        names: layer_to_names(&l.layer)?,
                        expr: wrap(l.inner.try_into()?),
                    })
                }
            }
        })
    }
}

impl TryFrom<CompositeExplodedEdgeFilter> for GqlExplodedEdgeFilter {
    type Error = GraphError;
    fn try_from(f: CompositeExplodedEdgeFilter) -> Result<Self, Self::Error> {
        Ok(match f {
            // Endpoint filters recurse into the node converter — an
            // exploded-edge filter on src/dst wraps a full node filter.
            CompositeExplodedEdgeFilter::Src(nf) => {
                GqlExplodedEdgeFilter::Src(wrap(nf.try_into()?))
            }
            CompositeExplodedEdgeFilter::Dst(nf) => {
                GqlExplodedEdgeFilter::Dst(wrap(nf.try_into()?))
            }

            CompositeExplodedEdgeFilter::Property(pf) => {
                let base = build_base_prop_condition(pf.operator, &pf.prop_value)?;
                let where_ = apply_ops_to_condition(base, &pf.ops);
                let name = pf.prop_ref.name().to_string();
                match pf.prop_ref {
                    PropertyRef::Property(_) => {
                        GqlExplodedEdgeFilter::Property(PropertyFilterNew { name, where_ })
                    }
                    PropertyRef::Metadata(_) => {
                        GqlExplodedEdgeFilter::Metadata(PropertyFilterNew { name, where_ })
                    }
                    PropertyRef::TemporalProperty(_) => {
                        GqlExplodedEdgeFilter::TemporalProperty(PropertyFilterNew { name, where_ })
                    }
                }
            }

            CompositeExplodedEdgeFilter::IsActiveEdge(_) => GqlExplodedEdgeFilter::IsActive(true),
            CompositeExplodedEdgeFilter::IsValidEdge(_) => GqlExplodedEdgeFilter::IsValid(true),
            CompositeExplodedEdgeFilter::IsDeletedEdge(_) => GqlExplodedEdgeFilter::IsDeleted(true),
            CompositeExplodedEdgeFilter::IsSelfLoopEdge(_) => {
                GqlExplodedEdgeFilter::IsSelfLoop(true)
            }

            CompositeExplodedEdgeFilter::And(l, r) => {
                GqlExplodedEdgeFilter::And(vec![(*l).try_into()?, (*r).try_into()?])
            }
            CompositeExplodedEdgeFilter::Or(l, r) => {
                GqlExplodedEdgeFilter::Or(vec![(*l).try_into()?, (*r).try_into()?])
            }
            CompositeExplodedEdgeFilter::Not(inner) => {
                GqlExplodedEdgeFilter::Not(wrap((*inner).try_into()?))
            }

            CompositeExplodedEdgeFilter::Windowed(w) => {
                GqlExplodedEdgeFilter::Window(ExplodedEdgeWindowExpr {
                    start: w.start.t().into(),
                    end: w.end.t().into(),
                    expr: wrap(w.inner.try_into()?),
                })
            }

            CompositeExplodedEdgeFilter::Latest(l) => {
                GqlExplodedEdgeFilter::Latest(ExplodedEdgeUnaryExpr {
                    expr: wrap(l.inner.try_into()?),
                })
            }

            CompositeExplodedEdgeFilter::SnapshotAt(s) => {
                GqlExplodedEdgeFilter::SnapshotAt(ExplodedEdgeTimeExpr {
                    time: s.time.t().into(),
                    expr: wrap(s.inner.try_into()?),
                })
            }

            CompositeExplodedEdgeFilter::SnapshotLatest(s) => {
                GqlExplodedEdgeFilter::SnapshotLatest(ExplodedEdgeUnaryExpr {
                    expr: wrap(s.inner.try_into()?),
                })
            }

            CompositeExplodedEdgeFilter::Layered(l) => {
                if matches!(l.layer, Layer::All) {
                    l.inner.try_into()?
                } else {
                    GqlExplodedEdgeFilter::Layers(ExplodedEdgeLayersExpr {
                        names: layer_to_names(&l.layer)?,
                        expr: wrap(l.inner.try_into()?),
                    })
                }
            }
        })
    }
}

#[cfg(test)]
mod op_chain_tests {
    use super::*;

    #[test]
    fn multi_op_prop_condition_round_trips() {
        // Tree `Sum(First(leaf))`: the OUTERMOST node (Sum) is the first-applied
        // op. Peeling outermost-first yields ops `[Sum, First]`, and core eval
        // runs ops[0] first — so this tree is the chain `.sum().first()`.
        let tree = PropCondition::Sum(wrap(PropCondition::First(wrap(PropCondition::IsSome(
            true,
        )))));

        // Decompose exactly as the wire encoder does — peel outermost-first.
        let mut ops = Vec::new();
        let mut cursor = &tree;
        while let Some(inner) = peel_prop_wrappers_and_collect_ops(cursor, &mut ops) {
            cursor = inner;
        }

        // Reconstruct: with the fold-in-reverse fix this round-trips. Before the
        // fix it produced the inverted `First(Sum(leaf))` (i.e. `.first().sum()`).
        let rebuilt = apply_ops_to_condition(cursor.clone(), &ops);
        assert_eq!(
            format!("{tree:?}"),
            format!("{rebuilt:?}"),
            "op chain did not round-trip — nesting inverted"
        );
    }

    #[test]
    fn apply_ops_pins_explicit_nesting_and_is_direction_sensitive() {
        // A round-trip alone is self-consistent even if decompose+reconstruct
        // were both wrong, so pin the exact tree and assert the two orderings
        // genuinely differ — otherwise a future edit could silently re-invert.
        let leaf = || PropCondition::IsSome(true);

        // ops = [Sum, First] (peeled outermost-first from tree `Sum(First(leaf))`,
        // the chain `.sum().first()`) must reconstruct as `Sum(First(leaf))`, not
        // `First(Sum(leaf))`.
        let rebuilt = apply_ops_to_condition(leaf(), &[Op::Sum, Op::First]);
        let expected = PropCondition::Sum(wrap(PropCondition::First(wrap(leaf()))));
        assert_eq!(format!("{expected:?}"), format!("{rebuilt:?}"));

        // The reverse op order produces a genuinely different tree.
        let reversed = apply_ops_to_condition(leaf(), &[Op::First, Op::Sum]);
        assert_ne!(
            format!("{rebuilt:?}"),
            format!("{reversed:?}"),
            "op ordering must be direction-sensitive"
        );
    }
}

#[cfg(test)]
mod filter_serde_goldens {
    use super::*;

    // The wire format is the single source of truth — async-graphql input
    // coercion, the persisted auth-store `GraphAccessFilter`, and the client
    // all depend on these EXACT shapes. Pin them so a stray `#[serde(rename)]`
    // is caught here, not at e2e time or by an invalidated permission store.

    #[test]
    fn node_field_filter_golden() {
        let f = GqlNodeFilter::Node(NodeFieldFilterNew {
            field: NodeField::NodeName,
            where_: NodeFieldCondition::Eq(Value::Str("alice".into())),
        });
        assert_eq!(
            serde_json::to_value(&f).unwrap(),
            serde_json::json!({"node": {"field": "NODE_NAME", "where": {"eq": {"str": "alice"}}}})
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
        // Serialization uses the schema field name `dtime` — the same on every path.
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
                GqlFilter::Not(wrap(GqlFilter::Node(node_prop_eq("x", 1)))),
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
            GqlFilter::Not(wrap(GqlFilter::Or(vec![GqlFilter::Node(node_prop_eq(
                "b", 2,
            ))]))),
        ]);
        let json = serde_json::to_string(&filter).unwrap();
        let back: GqlFilter = serde_json::from_str(&json).unwrap();
        assert_eq!(serde_json::to_string(&back).unwrap(), json);
    }

    // `not` composes end-to-end into a core filter.
    #[test]
    fn not_variant_converts_to_dyn_filter() {
        let filter = GqlFilter::Not(wrap(GqlFilter::Node(node_prop_eq("x", 1))));
        assert!(DynFilter::try_from(filter).is_ok());
    }
}

#[cfg(test)]
mod fuzzy_search_tests {
    use super::*;
    use raphtory::db::graph::views::filter::model::node_filter::{
        ops::NodeFilterOps, NodeFilter as NodeFilterBuilder,
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
    fn property_fuzzy_round_trips_through_the_conversions() {
        let cond = PropCondition::FuzzySearch(FuzzySearchExpr {
            value: "graph enthusiast".into(),
            levenshtein_distance: 3,
            prefix_match: true,
        });

        let (operator, value) = translate_prop_leaf_to_filter("bio", &cond).unwrap();
        assert_eq!(
            operator,
            FilterOperator::FuzzySearch {
                levenshtein_distance: 3,
                prefix_match: true,
            }
        );

        let back = build_base_prop_condition(operator, &value).unwrap();
        let PropCondition::FuzzySearch(f) = back else {
            panic!("expected fuzzySearch back, got something else");
        };
        assert_eq!(
            (f.value.as_str(), f.levenshtein_distance, f.prefix_match),
            ("graph enthusiast", 3, true)
        );
    }

    // Local node-name builder → wire condition (the reverse conversion the
    // Python remote client rides) preserves the fuzzy parameters.
    #[test]
    fn node_name_fuzzy_round_trips_through_the_wire() {
        let core = NodeFilterBuilder::name().fuzzy_search("ben", 1, true);
        let wire = filter_to_node_field(core.0).unwrap();
        let NodeFieldCondition::FuzzySearch(ref f) = wire.where_ else {
            panic!("expected fuzzySearch condition, got {:?}", wire.where_);
        };
        assert_eq!(
            (f.value.as_str(), f.levenshtein_distance, f.prefix_match),
            ("ben", 1, true)
        );
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
    // builder's `V: Into<GID>` bound.
    #[test]
    fn node_id_ordering_accepts_string_gids() {
        let filter = GqlNodeFilter::Node(NodeFieldFilterNew {
            field: NodeField::NodeId,
            where_: NodeFieldCondition::Gt(Value::Str("m".into())),
        });
        assert!(CompositeNodeFilter::try_from(filter).is_ok());
    }

    // Aggregation ops on a degree filter fail at conversion time with a clear
    // message (previously they slipped through and failed at evaluation).
    #[test]
    fn degree_rejects_aggregation_ops_at_conversion() {
        let filter = GqlNodeFilter::Degree(DegreeFilterNew {
            direction: DegreeDirection::Both,
            where_: PropCondition::Sum(wrap(PropCondition::Eq(Value::I64(3)))),
        });
        let Err(err) = CompositeNodeFilter::try_from(filter) else {
            panic!("degree with an op chain must be rejected at conversion");
        };
        assert!(
            err.to_string()
                .contains("aggregation ops are not supported"),
            "unexpected error: {err}"
        );
    }

    // Layer round-trip semantics: `None` is the empty name list (exact
    // round-trip via `Layer::from_iter([])`); `All` is no restriction, so the
    // reverse conversion drops the wrapper entirely.
    #[test]
    fn layer_none_and_all_normalize() {
        assert_eq!(layer_to_names(&Layer::None).unwrap(), Vec::<String>::new());

        let inner = CompositeNodeFilter::Node(Filter::eq("node_name", "a"));
        let layered = CompositeNodeFilter::Layered(Box::new(Layered {
            layer: Layer::All,
            inner,
        }));
        let gql = GqlNodeFilter::try_from(layered).unwrap();
        assert!(
            matches!(gql, GqlNodeFilter::Node(_)),
            "Layer::All should drop the layer wrapper, got {gql:?}"
        );
    }
}

#[cfg(test)]
mod exploded_edge_filter_tests {
    use super::*;
    use raphtory::db::graph::views::filter::model::{
        property_filter::ops::PropertyFilterOps, ComposableFilter, PropertyFilterFactory,
        TryAsCompositeFilter, ViewWrapOps,
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
        let filter = GqlFilter::ExplodedEdge(GqlExplodedEdgeFilter::Not(wrap(
            GqlExplodedEdgeFilter::Or(vec![
                exploded_prop_gt("w", 1),
                GqlExplodedEdgeFilter::TemporalProperty(PropertyFilterNew {
                    name: "w".into(),
                    where_: PropCondition::Any(wrap(PropCondition::Eq(Value::I64(3)))),
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
    fn composite_round_trips_through_the_wire() {
        let prop = || {
            ExplodedEdgeFilter
                .property("w")
                .gt(1i64)
                .try_as_composite_exploded_edge_filter()
                .unwrap()
        };
        let cases = vec![
            prop(),
            ExplodedEdgeFilter
                .metadata("kind")
                .eq("strong")
                .try_as_composite_exploded_edge_filter()
                .unwrap(),
            ExplodedEdgeFilter
                .window(2i64, 4i64)
                .property("w")
                .gt(1i64)
                .try_as_composite_exploded_edge_filter()
                .unwrap(),
            ExplodedEdgeFilter
                .layer("knows")
                .property("w")
                .gt(1i64)
                .try_as_composite_exploded_edge_filter()
                .unwrap(),
            CompositeExplodedEdgeFilter::And(
                Box::new(prop()),
                Box::new(CompositeExplodedEdgeFilter::IsValidEdge(IsValidEdge)),
            ),
            CompositeExplodedEdgeFilter::Not(Box::new(prop())),
            CompositeExplodedEdgeFilter::Src(CompositeNodeFilter::Node(Filter::eq(
                "node_name",
                "a",
            ))),
        ];
        for original in cases {
            let gql = GqlExplodedEdgeFilter::try_from(original.clone()).unwrap();
            let back = CompositeExplodedEdgeFilter::try_from(gql).unwrap();
            assert_eq!(original, back, "round-trip changed the filter");
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
    fn filter_tree_converts_to_the_wire_variant() {
        let tree = ExplodedEdgeFilter
            .property("w")
            .gt(1i64)
            .try_as_filter_tree()
            .unwrap();
        let gql = GqlFilter::try_from(tree).unwrap();
        assert!(
            matches!(gql, GqlFilter::ExplodedEdge(_)),
            "expected ExplodedEdges, got {gql:?}"
        );

        // A mixed node∧exploded tree keeps both kinds through the conversion.
        let n = raphtory::db::graph::views::filter::model::node_filter::NodeFilter
            .property("x")
            .eq(1i64);
        let x = ExplodedEdgeFilter.property("w").gt(1i64);
        let tree = n.and(x).try_as_filter_tree().unwrap();
        let gql = GqlFilter::try_from(tree).unwrap();
        let GqlFilter::And(items) = gql else {
            panic!("expected GqlFilter::And");
        };
        assert!(matches!(items[0], GqlFilter::Node(_)));
        assert!(matches!(items[1], GqlFilter::ExplodedEdge(_)));
    }

    // Empty combinators are rejected like everywhere else in this module.
    #[test]
    fn empty_combinators_are_rejected() {
        for (name, filter) in [
            ("and", GqlExplodedEdgeFilter::And(vec![])),
            ("or", GqlExplodedEdgeFilter::Or(vec![])),
        ] {
            let Err(err) = CompositeExplodedEdgeFilter::try_from(filter) else {
                panic!("ExplodedEdgeFilter {name}: empty combinator must be rejected");
            };
            assert!(
                err.to_string().contains("requires non-empty list"),
                "ExplodedEdgeFilter {name}: unexpected error {err}"
            );
        }
    }
}

#[cfg(test)]
mod filter_tree_tests {
    use super::*;
    use raphtory::db::graph::views::filter::model::{
        edge_filter::EdgeFilter as EdgeFilterBuilder, graph_filter::GraphFilter,
        node_filter::NodeFilter as NodeFilterBuilder, property_filter::ops::PropertyFilterOps,
        ComposableFilter, PropertyFilterFactory, TryAsCompositeFilter, ViewWrapOps,
    };

    // A same-kind combination stays in composite form — no structural tree.
    #[test]
    fn same_kind_and_exports_as_a_composite() {
        let a = NodeFilterBuilder.property("x").eq(1i64);
        let b = NodeFilterBuilder.property("y").eq(2i64);
        let tree = a.and(b).try_as_filter_tree().unwrap();
        assert!(matches!(tree, FilterTree::Node(_)));
    }

    // A mixed node∧edge combination exports structurally and converts to the
    // wire form — the case the single-kind exports cannot represent.
    #[test]
    fn mixed_and_exports_structurally_and_converts() {
        let n = NodeFilterBuilder.property("x").eq(1i64);
        let e = EdgeFilterBuilder.property("w").eq(2i64);
        let tree = n.and(e).try_as_filter_tree().unwrap();
        let FilterTree::And(ref items) = tree else {
            panic!("expected structural And, got {tree:?}");
        };
        assert!(matches!(items[0], FilterTree::Node(_)));
        assert!(matches!(items[1], FilterTree::Edge(_)));

        let gql = GqlFilter::try_from(tree).unwrap();
        let GqlFilter::And(items) = gql else {
            panic!("expected GqlFilter::And");
        };
        assert!(matches!(items[0], GqlFilter::Node(_)));
        assert!(matches!(items[1], GqlFilter::Edge(_)));
    }

    // A graph-view chain exports outermost-first and converts to the nested
    // wire form.
    #[test]
    fn graph_view_chain_exports_and_converts() {
        let f = GraphFilter.window(1i64, 5i64).layer("x");
        let tree = f.try_as_filter_tree().unwrap();
        let FilterTree::View(ref ops) = tree else {
            panic!("expected View chain, got {tree:?}");
        };
        assert!(matches!(ops[0], GraphViewOp::Layers(_)));
        assert!(matches!(ops[1], GraphViewOp::Window { .. }));

        let gql = GqlFilter::try_from(tree).unwrap();
        let GqlFilter::Graph(GqlGraphFilter::Layers(ref l)) = gql else {
            panic!("expected Graph(Layers), got {gql:?}");
        };
        assert_eq!(l.names, vec!["x"]);
        assert!(matches!(l.expr.as_deref(), Some(GqlGraphFilter::Window(_))));
    }
}
