//! Query → [`Plan`]: parse, validate against the SDL, and pre-resolve every
//! field/argument into a typed [`Op`] tree.
//!
//! This is a single type-directed walk. Starting from the root type
//! (`QueryRoot`), each selected field is:
//!   1. **validated** — it must exist on its parent type in the SDL
//!      ([`SchemaTypes::field`]); otherwise the request is rejected here, before
//!      any execution.
//!   2. **resolved** — `(parent_type, field)` is mapped to a concrete execution
//!      [`OpKind`] via [`resolve_op`] (the hand-written binding table — the
//!      "pre-resolved bindings"), with arguments parsed once into typed values.
//!   3. **recursed** — the field's SDL return type becomes the parent type for
//!      its children.

use super::{
    plan::{IterKind, LeafKind, Nav, Op, Page, Plan, ViewKind, ViewStep},
    schema::SchemaTypes,
    tokens::{arg, field as fld, ty},
};
use crate::model::{
    graph::{
        filtering::{GqlEdgeFilter, GqlNodeFilter},
        node_id::GqlNodeId,
        timeindex::{dt_format_str_is_valid, GqlTimeInput},
    },
    sorting::{EdgeSortBy, NodeSortBy, SortByTime},
};
use raphtory::db::graph::views::filter::model::{
    edge_filter::CompositeEdgeFilter, node_filter::CompositeNodeFilter,
};
use async_graphql::{
    normalize::{iter_fields, resolve_value},
    parser::{
        parse_query,
        types::{
            DocumentOperations, ExecutableDocument, Field, FragmentDefinition, Selection,
            SelectionSet, VariableDefinition,
        },
        Positioned,
    },
    Name, ValidationMode, Value as GqlValue, Variables,
};
use std::{collections::HashMap, sync::OnceLock};
use raphtory_api::core::{entities::GID, storage::timeindex::EventTime};

#[derive(Debug, thiserror::Error)]
pub enum PlanError {
    #[error("failed to parse query: {0}")]
    Parse(String),
    #[error("query has no operation")]
    NoOperation,
    #[error("the root must select exactly one `graph(path:)` field")]
    BadRoot,
    #[error("unknown field `{field}` on type `{ty}`")]
    UnknownField { ty: String, field: String },
    #[error("field `{field}` on type `{ty}` is not supported by the interpreter")]
    Unsupported { ty: String, field: String },
    #[error("missing required argument `{0}`")]
    MissingArgument(&'static str),
    #[error("invalid value for argument `{0}`")]
    BadArgument(&'static str),
    #[error("Invalid datetime format string: '{0}'")]
    InvalidDateTimeFormat(String),
    #[error("invalid filter for argument `{arg}`: {reason}")]
    Filter { arg: &'static str, reason: String },
    #[error("{0}")]
    Variable(String),
    #[error("{0}")]
    Validation(String),
}

/// A planned request: the (async-loaded) root graph path plus the compiled plan
/// for its selection set.
#[derive(Debug)]
pub struct PlannedRequest {
    pub graph_path: String,
    pub plan: Plan,
}

/// Parse, validate, and compile a query string into a [`PlannedRequest`].
/// The async-graphql schema `Registry` used to validate requests. Built once
/// from the same `App` that powers the `/` endpoint (so it is authoritative —
/// it is what generates `schema.graphql`), then cached. No `Data` is needed to
/// build it: data is runtime context, the registry is schema shape only.
fn schema_registry() -> &'static async_graphql::registry::Registry {
    use async_graphql::dynamic::Schema;
    static SCHEMA: OnceLock<Schema> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            crate::model::App::create_schema()
                .finish()
                .expect("failed to build schema for interpreter validation")
        })
        .registry()
}

pub fn plan_request(query: &str, variables: &Variables) -> Result<PlannedRequest, PlanError> {
    let doc = parse_query(query).map_err(|e| PlanError::Parse(e.to_string()))?;

    // Validate against async-graphql's own rules (field/argument existence and
    // types, variable usage, fragment cycles, …) before touching the document —
    // variable references must still be present for the variable rules to run.
    schema_registry()
        .check_query(&doc, Some(variables), None, ValidationMode::Strict)
        .map_err(|errors| {
            PlanError::Validation(
                errors
                    .iter()
                    .map(|e| e.message.clone())
                    .collect::<Vec<_>>()
                    .join("; "),
            )
        })?;

    let ExecutableDocument {
        operations,
        mut fragments,
    } = doc;
    let mut op = match operations {
        DocumentOperations::Single(op) => op.node,
        DocumentOperations::Multiple(ops) => {
            ops.into_iter().next().ok_or(PlanError::NoOperation)?.1.node
        }
    };

    // Resolve variables once, up front: rewrite every argument's `$var` into a
    // concrete value (using `variables`, then operation-level defaults) — both in
    // the operation and in the fragment definitions its spreads pull in. After
    // this, the planner only ever sees variable-free argument values, so the
    // existing `const_arg` path works unchanged.
    substitute_variables(&mut op.selection_set.node, &op.variable_definitions, variables)?;
    for fragment in fragments.values_mut() {
        substitute_variables(
            &mut fragment.node.selection_set.node,
            &op.variable_definitions,
            variables,
        )?;
    }

    let schema = SchemaTypes::get();

    // The root must select exactly one field (after following any fragment), and
    // it must be `graph(path:)`.
    let mut roots = iter_fields(&op.selection_set.node, &fragments);
    let graph_field = roots.next().ok_or(PlanError::BadRoot)?;
    if roots.next().is_some() {
        return Err(PlanError::BadRoot);
    }
    let graph_field = &graph_field.node;
    if graph_field.name.node.as_str() != fld::GRAPH {
        return Err(PlanError::BadRoot);
    }
    let ginfo = schema
        .field(ty::QUERY_ROOT, fld::GRAPH)
        .ok_or_else(|| PlanError::UnknownField {
            ty: ty::QUERY_ROOT.into(),
            field: fld::GRAPH.into(),
        })?;

    let graph_path = string_arg(graph_field, arg::PATH)?;
    let root_key = graph_field.response_key().node.to_string();
    let children =
        plan_selection(&ginfo.return_type, &graph_field.selection_set.node, schema, &fragments)?;

    Ok(PlannedRequest {
        graph_path,
        plan: Plan {
            root_key: root_key.into_boxed_str(),
            children,
        },
    })
}

/// Rewrite every argument value in `sel` (and nested inline fragments), replacing
/// `Value::Variable` with a concrete value via the schema-agnostic resolver in
/// `async_graphql::normalize`. Fragment *definitions* are handled by the caller.
fn substitute_variables(
    sel: &mut SelectionSet,
    defs: &[Positioned<VariableDefinition>],
    variables: &Variables,
) -> Result<(), PlanError> {
    for item in &mut sel.items {
        match &mut item.node {
            Selection::Field(field) => {
                for (_, value) in &mut field.node.arguments {
                    let resolved = resolve_value(&value.node, defs, variables)
                        .map_err(|e| PlanError::Variable(e.message))?;
                    value.node = resolved.into_value();
                }
                substitute_variables(&mut field.node.selection_set.node, defs, variables)?;
            }
            Selection::InlineFragment(inline) => {
                substitute_variables(&mut inline.node.selection_set.node, defs, variables)?;
            }
            Selection::FragmentSpread(_) => {}
        }
    }
    Ok(())
}

/// Compile a selection set whose fields are selected on `parent_type`. Fragment
/// spreads and inline fragments are followed transparently via [`iter_fields`].
fn plan_selection(
    parent_type: &str,
    sel: &SelectionSet,
    schema: &SchemaTypes,
    fragments: &HashMap<Name, Positioned<FragmentDefinition>>,
) -> Result<Box<[Op]>, PlanError> {
    let mut ops = Vec::new();
    for field in iter_fields(sel, fragments) {
        let field = &field.node;
        let name = field.name.node.as_str();
        let key = field.response_key().node.as_str();

        // (1) validate against the SDL.
        let finfo = schema
            .field(parent_type, name)
            .ok_or_else(|| PlanError::UnknownField {
                ty: parent_type.into(),
                field: name.into(),
            })?;

        // (2) resolve to a concrete op; (3) recurse on the SDL return type.
        let op = match resolve_op(parent_type, name, field)? {
            OpKind::Leaf(leaf) => Op::Leaf {
                key: key.into(),
                leaf,
            },
            OpKind::Navigate(nav) => Op::Navigate {
                key: key.into(),
                nav,
                nullable: finfo.nullable,
                children: plan_selection(
                    &finfo.return_type,
                    &field.selection_set.node,
                    schema,
                    fragments,
                )?,
            },
            OpKind::List(iter) => Op::List {
                key: key.into(),
                iter,
                children: plan_selection(
                    &finfo.return_type,
                    &field.selection_set.node,
                    schema,
                    fragments,
                )?,
            },
        };
        ops.push(op);
    }
    Ok(ops.into_boxed_slice())
}

/// How a supported field executes. The SDL validates *existence*; this table
/// supplies *behaviour* (and parses arguments). A field present in the SDL but
/// absent here is a valid-but-unimplemented field → [`PlanError::Unsupported`].
enum OpKind {
    Navigate(Nav),
    List(IterKind),
    Leaf(LeafKind),
}

fn resolve_op(parent_type: &str, field: &str, f: &Field) -> Result<OpKind, PlanError> {
    use IterKind as I;
    use LeafKind as L;
    use Nav as N;
    use OpKind::{Leaf, List, Navigate};
    use ViewKind as VK;

    // A same-type view transform op.
    let view = |vk: ViewKind| Navigate(N::View(vk));

    // Fields shared across types collapse into one arm via or-patterns on the
    // parent type. The parent type is *kept* (not dropped) so a field that the
    // SDL allows on a type the interpreter hasn't wired falls through to the
    // `Unsupported` arm — a clean pre-stream error rather than an exec panic.
    Ok(match (parent_type, field) {
        // ── entry points: graph → collections / lookups (optional `select:` filter) ──
        (ty::GRAPH, fld::NODES) => Navigate(N::Nodes(opt_node_filter(f, arg::SELECT)?)),
        (ty::GRAPH, fld::NODE) => Navigate(N::Node(node_id_arg(f, arg::NAME)?)),
        (ty::GRAPH, fld::EDGE) => Navigate(N::Edge {
            src: node_id_arg(f, arg::SRC)?,
            dst: node_id_arg(f, arg::DST)?,
        }),

        // ── traversal (Node) and edge endpoints; `select:` pushed into raphtory ──
        (ty::GRAPH | ty::NODE, fld::EDGES) => Navigate(N::Edges(opt_edge_filter(f, arg::SELECT)?)),
        (ty::NODE, fld::IN_EDGES) => Navigate(N::InEdges(opt_edge_filter(f, arg::SELECT)?)),
        (ty::NODE, fld::OUT_EDGES) => Navigate(N::OutEdges(opt_edge_filter(f, arg::SELECT)?)),
        (ty::NODE, fld::NEIGHBOURS) => Navigate(N::Neighbours(opt_node_filter(f, arg::SELECT)?)),
        (ty::NODE, fld::IN_NEIGHBOURS) => {
            Navigate(N::InNeighbours(opt_node_filter(f, arg::SELECT)?))
        }
        (ty::NODE, fld::OUT_NEIGHBOURS) => {
            Navigate(N::OutNeighbours(opt_node_filter(f, arg::SELECT)?))
        }
        (ty::NODE, fld::IN_COMPONENT) => Navigate(N::InComponent),
        (ty::NODE, fld::OUT_COMPONENT) => Navigate(N::OutComponent),
        (ty::EDGE, fld::SRC) => Navigate(N::Src),
        (ty::EDGE, fld::DST) => Navigate(N::Dst),
        (ty::EDGE, fld::NBR) => Navigate(N::Nbr),
        (ty::EDGE, fld::EXPLODE) => Navigate(N::Explode),
        (ty::EDGE, fld::EXPLODE_LAYERS) => Navigate(N::ExplodeLayers),
        (ty::EDGE, fld::DELETIONS) => Navigate(N::Deletions),

        // ── view transforms (same type in → out) via Nav::View(ViewKind) ──
        // Time/layer views apply to entities *and* their collections; `is_viewable`
        // gates the parent type (SDL validation already confirmed the field exists).
        (t, fld::WINDOW) if is_viewable(t) => view(VK::Window {
            start: time_arg(f, arg::START)?,
            end: time_arg(f, arg::END)?,
        }),
        (t, fld::AT) if is_viewable(t) => view(VK::At(time_arg(f, arg::TIME)?)),
        (t, fld::BEFORE) if is_viewable(t) => view(VK::Before(time_arg(f, arg::TIME)?)),
        (t, fld::AFTER) if is_viewable(t) => view(VK::After(time_arg(f, arg::TIME)?)),
        (t, fld::LATEST) if is_viewable(t) => view(VK::Latest),
        (t, fld::SNAPSHOT_AT) if is_viewable(t) => view(VK::SnapshotAt(time_arg(f, arg::TIME)?)),
        (t, fld::SNAPSHOT_LATEST) if is_viewable(t) => view(VK::SnapshotLatest),
        (t, fld::SHRINK_WINDOW) if is_viewable(t) => view(VK::ShrinkWindow {
            start: time_arg(f, arg::START)?,
            end: time_arg(f, arg::END)?,
        }),
        (t, fld::SHRINK_START) if is_viewable(t) => view(VK::ShrinkStart(time_arg(f, arg::START)?)),
        (t, fld::SHRINK_END) if is_viewable(t) => view(VK::ShrinkEnd(time_arg(f, arg::END)?)),
        (t, fld::DEFAULT_LAYER) if is_viewable(t) => view(VK::DefaultLayer),
        (t, fld::LAYER) if is_viewable(t) => view(VK::Layer(string_arg(f, arg::NAME)?.into())),
        (t, fld::LAYERS) if is_viewable(t) => view(VK::Layers(strings_arg(f, arg::NAMES)?)),
        (t, fld::EXCLUDE_LAYER) if is_viewable(t) => {
            view(VK::ExcludeLayer(string_arg(f, arg::NAME)?.into()))
        }
        (t, fld::EXCLUDE_LAYERS) if is_viewable(t) => {
            view(VK::ExcludeLayers(strings_arg(f, arg::NAMES)?))
        }
        // node-collection-only
        (ty::NODES | ty::PATH_FROM_NODE, fld::TYPE_FILTER) => {
            view(VK::TypeFilter(strings_arg(f, arg::NODE_TYPES)?))
        }
        // applyViews — fold a [*ViewCollection] list (all viewable types)
        (t, fld::APPLY_VIEWS) if is_viewable(t) => Navigate(N::ApplyViews(view_steps_arg(f)?)),
        // graph-only structural views
        (ty::GRAPH, fld::VALID) => view(VK::Valid),
        (ty::GRAPH, fld::SUBGRAPH) => view(VK::Subgraph(node_ids_arg(f, arg::NODES)?)),
        (ty::GRAPH, fld::SUBGRAPH_NODE_TYPES) => {
            view(VK::SubgraphNodeTypes(strings_arg(f, arg::NODE_TYPES)?))
        }
        (ty::GRAPH, fld::EXCLUDE_NODES) => view(VK::ExcludeNodes(node_ids_arg(f, arg::NODES)?)),

        // ── time fields → EventTime ──
        (ty::GRAPH | ty::NODE | ty::EDGE, fld::EARLIEST_TIME) => Navigate(N::EarliestTime),
        (ty::GRAPH | ty::NODE | ty::EDGE, fld::LATEST_TIME) => Navigate(N::LatestTime),
        (ty::GRAPH | ty::NODE | ty::EDGE, fld::START) => Navigate(N::Start),
        (ty::GRAPH | ty::NODE | ty::EDGE, fld::END) => Navigate(N::End),
        (ty::NODE | ty::EDGE, fld::FIRST_UPDATE) => Navigate(N::FirstUpdate),
        (ty::NODE | ty::EDGE, fld::LAST_UPDATE) => Navigate(N::LastUpdate),

        // ── properties / metadata / history entry ──
        (ty::NODE | ty::EDGE, fld::PROPERTIES) => Navigate(N::Properties),
        (ty::NODE | ty::EDGE, fld::METADATA) => Navigate(N::Metadata),
        (ty::NODE | ty::EDGE | ty::TEMPORAL_PROPERTY, fld::HISTORY) => Navigate(N::History),
        (ty::PROPERTIES, fld::TEMPORAL) => Navigate(N::Temporal),

        // ── history projections ──
        (ty::HISTORY, fld::TIMESTAMPS) => Navigate(N::Timestamps),
        (ty::HISTORY, fld::EVENT_ID) => Navigate(N::EventIds),
        (ty::HISTORY, fld::DATETIMES) => Navigate(N::DateTimes(datetime_format_arg(f)?)),

        // ── filtering (pushed into raphtory) ──
        (ty::GRAPH, fld::FILTER_NODES) => Navigate(N::FilterNodes(node_filter(f, arg::EXPR)?)),
        (ty::GRAPH, fld::FILTER_EDGES) => Navigate(N::FilterEdges(edge_filter(f, arg::EXPR)?)),
        (ty::NODE, fld::FILTER) => Navigate(N::ApplyNodeFilter {
            filter: node_filter(f, arg::EXPR)?,
            select: false,
        }),
        (ty::NODES | ty::PATH_FROM_NODE, fld::FILTER) => Navigate(N::ApplyNodeFilter {
            filter: node_filter(f, arg::EXPR)?,
            select: false,
        }),
        (ty::NODES | ty::PATH_FROM_NODE, fld::SELECT) => Navigate(N::ApplyNodeFilter {
            filter: node_filter(f, arg::EXPR)?,
            select: true,
        }),
        (ty::EDGES, fld::FILTER) => Navigate(N::ApplyEdgeFilter {
            filter: edge_filter(f, arg::EXPR)?,
            select: false,
        }),
        (ty::EDGES, fld::SELECT) => Navigate(N::ApplyEdgeFilter {
            filter: edge_filter(f, arg::EXPR)?,
            select: true,
        }),

        // ── collection sizing / sorting (same type out) ──
        (ty::NODES | ty::EDGES | ty::PATH_FROM_NODE, fld::COUNT) => Leaf(L::Count),
        (ty::NODES, fld::SORTED) => Navigate(N::SortedNodes(node_sort_bys_arg(f)?)),
        (ty::EDGES, fld::SORTED) => Navigate(N::SortedEdges(edge_sort_bys_arg(f)?)),

        // ── lists & pages (parent type disambiguates the item) ──
        (ty::NODES, fld::LIST) => List(I::NodesList),
        (ty::EDGES, fld::LIST) => List(I::EdgesList),
        (ty::PATH_FROM_NODE, fld::LIST) => List(I::NeighboursList),
        (ty::NODES, fld::PAGE) => List(I::NodesPage(page_arg(f)?)),
        (ty::EDGES, fld::PAGE) => List(I::EdgesPage(page_arg(f)?)),
        (ty::PATH_FROM_NODE, fld::PAGE) => List(I::NeighboursPage(page_arg(f)?)),
        (ty::HISTORY, fld::LIST) => List(I::HistoryList),
        (ty::PROPERTIES, fld::VALUES) => List(I::PropertiesValues(keys_arg(f)?)),
        (ty::METADATA, fld::VALUES) => List(I::MetadataValues(keys_arg(f)?)),
        (ty::TEMPORAL_PROPERTIES, fld::VALUES) => List(I::TemporalValues(keys_arg(f)?)),
        (ty::PROPERTIES | ty::METADATA, fld::GET) => {
            Navigate(N::PropGet(string_arg(f, arg::KEY)?.into()))
        }
        (ty::TEMPORAL_PROPERTIES, fld::GET) => {
            Navigate(N::TemporalGet(string_arg(f, arg::KEY)?.into()))
        }
        (ty::HISTORY_TIMESTAMP, fld::LIST) => Leaf(L::TimestampList),
        (ty::HISTORY_EVENT_ID, fld::LIST) => Leaf(L::EventIdList),
        (ty::HISTORY_DATE_TIME, fld::LIST) => Leaf(L::DateTimeList),

        // ── scalar leaves ──
        (ty::NODE, fld::ID) => Leaf(L::Id),
        (ty::EDGE, fld::ID) => Leaf(L::EdgeId),
        (ty::NODE, fld::NAME) => Leaf(L::Name),
        (ty::NODE, fld::NODE_TYPE) => Leaf(L::NodeType),
        (ty::NODE, fld::DEGREE) => Leaf(L::Degree),
        (ty::NODE, fld::IN_DEGREE) => Leaf(L::InDegree),
        (ty::NODE, fld::OUT_DEGREE) => Leaf(L::OutDegree),
        (ty::NODE, fld::EDGE_HISTORY_COUNT) => Leaf(L::EdgeHistoryCount),
        (ty::NODE | ty::EDGE, fld::IS_ACTIVE) => Leaf(L::IsActive),
        (ty::EDGE, fld::IS_VALID) => Leaf(L::IsValid),
        (ty::EDGE, fld::IS_DELETED) => Leaf(L::IsDeleted),
        (ty::EDGE, fld::IS_SELF_LOOP) => Leaf(L::IsSelfLoop),
        (ty::EDGE, fld::LAYER_NAMES) => Leaf(L::LayerNames),
        (ty::GRAPH, fld::COUNT_NODES) => Leaf(L::CountNodes),
        (ty::GRAPH, fld::COUNT_EDGES) => Leaf(L::CountEdges),
        (ty::GRAPH, fld::COUNT_TEMPORAL_EDGES) => Leaf(L::CountTemporalEdges),
        (ty::GRAPH, fld::UNIQUE_LAYERS) => Leaf(L::UniqueLayers),
        (ty::GRAPH, fld::HAS_NODE) => Leaf(L::HasNode(node_id_arg(f, arg::NAME)?)),
        (ty::GRAPH, fld::HAS_EDGE) => Leaf(L::HasEdge {
            src: node_id_arg(f, arg::SRC)?,
            dst: node_id_arg(f, arg::DST)?,
            layer: opt_string_arg(f, arg::LAYER)?,
        }),
        (ty::EVENT_TIME, fld::TIMESTAMP) => Leaf(L::Timestamp),
        (ty::EVENT_TIME, fld::EVENT_ID) => Leaf(L::EventId),
        (ty::EVENT_TIME, fld::DATETIME) => Leaf(L::DateTime(
            datetime_format_arg(f)?.unwrap_or_else(|| "%+".into()),
        )),
        (ty::PROPERTY | ty::TEMPORAL_PROPERTY, fld::KEY) => Leaf(L::Key),
        (ty::PROPERTY | ty::PROPERTY_TUPLE, fld::AS_STRING) => Leaf(L::AsString),
        (ty::PROPERTY | ty::PROPERTY_TUPLE, fld::VALUE) => Leaf(L::Value),

        // ── temporal aggregates (TemporalProperty) ──
        (ty::TEMPORAL_PROPERTY, fld::VALUES) => Leaf(L::TemporalValueList),
        (ty::TEMPORAL_PROPERTY, fld::UNIQUE) => Leaf(L::TemporalUniqueList),
        (ty::TEMPORAL_PROPERTY, fld::LATEST) => Leaf(L::TemporalLatest),
        (ty::TEMPORAL_PROPERTY, fld::SUM) => Leaf(L::TemporalSum),
        (ty::TEMPORAL_PROPERTY, fld::MEAN) => Leaf(L::TemporalMean),
        (ty::TEMPORAL_PROPERTY, fld::AVERAGE) => Leaf(L::TemporalAverage),
        (ty::TEMPORAL_PROPERTY, fld::COUNT) => Leaf(L::TemporalCount),
        (ty::TEMPORAL_PROPERTY, fld::AT) => Leaf(L::TemporalAt(time_arg(f, arg::T)?)),
        (ty::TEMPORAL_PROPERTY, fld::MIN) => Navigate(N::TemporalMin),
        (ty::TEMPORAL_PROPERTY, fld::MAX) => Navigate(N::TemporalMax),
        (ty::TEMPORAL_PROPERTY, fld::MEDIAN) => Navigate(N::TemporalMedian),
        (ty::TEMPORAL_PROPERTY, fld::ORDERED_DEDUPE) => {
            List(I::OrderedDedupe(bool_arg(f, arg::LATEST_TIME)?))
        }
        (ty::PROPERTY_TUPLE, fld::TIME) => Navigate(N::TupleTime),

        _ => {
            return Err(PlanError::Unsupported {
                ty: parent_type.into(),
                field: field.into(),
            })
        }
    })
}

/// Fetch an argument and lower it to a [`GqlValue`] (`ConstValue`). The parser
/// yields the variable-capable `Value`; `into_const` resolves it when no
/// variables are present (the POC does not support variables).
fn const_arg(f: &Field, name: &str) -> Option<GqlValue> {
    f.get_argument(name).and_then(|v| v.node.clone().into_const())
}

fn bool_arg(f: &Field, name: &'static str) -> Result<bool, PlanError> {
    match const_arg(f, name) {
        Some(GqlValue::Boolean(b)) => Ok(b),
        Some(_) => Err(PlanError::BadArgument(name)),
        None => Err(PlanError::MissingArgument(name)),
    }
}

fn string_arg(f: &Field, name: &'static str) -> Result<String, PlanError> {
    match const_arg(f, name) {
        Some(GqlValue::String(s)) => Ok(s),
        Some(_) => Err(PlanError::BadArgument(name)),
        None => Err(PlanError::MissingArgument(name)),
    }
}

/// Parse an optional string argument (absent or null → `None`).
fn opt_string_arg(f: &Field, name: &'static str) -> Result<Option<Box<str>>, PlanError> {
    match const_arg(f, name) {
        None | Some(GqlValue::Null) => Ok(None),
        Some(GqlValue::String(s)) => Ok(Some(s.into_boxed_str())),
        Some(_) => Err(PlanError::BadArgument(name)),
    }
}

/// Parse a `TimeInput` argument into an `EventTime`, via the same scalar parser
/// the resolvers use (`GqlTimeInput::from_value`). Supports all three GraphQL
/// forms: integer/datetime-string/`{timestamp, eventId}` object.
fn time_arg(f: &Field, name: &'static str) -> Result<EventTime, PlanError> {
    use dynamic_graphql::ScalarValue;
    use raphtory_api::core::utils::time::IntoTime;
    let v = const_arg(f, name).ok_or(PlanError::MissingArgument(name))?;
    GqlTimeInput::from_value(v)
        .map(IntoTime::into_time)
        .map_err(|_| PlanError::BadArgument(name))
}

/// Parse and validate the optional `formatString` argument for `datetimes` /
/// `datetime`. Validation happens here, at plan time — before any byte is
/// streamed — so an invalid format becomes a clean pre-stream error.
/// Returns `None` when the argument is absent (caller supplies the default).
fn datetime_format_arg(f: &Field) -> Result<Option<Box<str>>, PlanError> {
    match const_arg(f, arg::FORMAT_STRING) {
        None | Some(GqlValue::Null) => Ok(None),
        Some(GqlValue::String(s)) => {
            if dt_format_str_is_valid(&s) {
                Ok(Some(s.into_boxed_str()))
            } else {
                Err(PlanError::InvalidDateTimeFormat(s))
            }
        }
        Some(_) => Err(PlanError::BadArgument(arg::FORMAT_STRING)),
    }
}

/// Types that accept the shared time/layer view transforms (entities and their
/// collections). Graph-only structural views (`valid`/`subgraph`/…) are matched
/// separately.
fn is_viewable(ty: &str) -> bool {
    matches!(
        ty,
        ty::GRAPH
            | ty::NODE
            | ty::EDGE
            | ty::NODES
            | ty::EDGES
            | ty::PATH_FROM_NODE
    )
}

/// Parse a node-filter argument (`NodeFilter` `@oneOf`) and push it down into a
/// raphtory `CompositeNodeFilter`. Goes via serde — the `Gql*Filter` types derive
/// `Deserialize` with camelCase renames that match the GraphQL JSON — then the
/// existing `TryInto` conversion the resolvers use. Both steps run at plan time,
/// so a malformed filter is a clean pre-stream error.
fn parse_node_filter(v: GqlValue, arg: &'static str) -> Result<CompositeNodeFilter, PlanError> {
    let json = serde_json::to_value(&v).map_err(|e| PlanError::Filter {
        arg,
        reason: e.to_string(),
    })?;
    let gql: GqlNodeFilter = serde_json::from_value(json).map_err(|e| PlanError::Filter {
        arg,
        reason: e.to_string(),
    })?;
    CompositeNodeFilter::try_from(gql).map_err(|e| PlanError::Filter {
        arg,
        reason: e.to_string(),
    })
}

fn parse_edge_filter(v: GqlValue, arg: &'static str) -> Result<CompositeEdgeFilter, PlanError> {
    let json = serde_json::to_value(&v).map_err(|e| PlanError::Filter {
        arg,
        reason: e.to_string(),
    })?;
    let gql: GqlEdgeFilter = serde_json::from_value(json).map_err(|e| PlanError::Filter {
        arg,
        reason: e.to_string(),
    })?;
    CompositeEdgeFilter::try_from(gql).map_err(|e| PlanError::Filter {
        arg,
        reason: e.to_string(),
    })
}

/// Parse the `views:` argument of `applyViews` — a list of `*ViewCollection`
/// `@oneOf` entries — into a sequence of [`ViewStep`]s folded left-to-right at
/// exec time. Boolean flags that are `false` contribute no step.
fn view_steps_arg(f: &Field) -> Result<Vec<ViewStep>, PlanError> {
    let items = match const_arg(f, arg::VIEWS) {
        Some(GqlValue::List(items)) => items,
        Some(_) => return Err(PlanError::BadArgument(arg::VIEWS)),
        None => return Err(PlanError::MissingArgument(arg::VIEWS)),
    };
    let mut steps = Vec::with_capacity(items.len());
    for item in items {
        if let Some(step) = parse_view_step(item)? {
            steps.push(step);
        }
    }
    Ok(steps)
}

/// Parse a single `*ViewCollection` `@oneOf` entry into a [`ViewStep`].
/// Returns `None` for a boolean flag set to `false` (no-op step).
fn parse_view_step(entry: GqlValue) -> Result<Option<ViewStep>, PlanError> {
    use ViewKind as VK;
    let obj = match entry {
        GqlValue::Object(o) => o,
        _ => return Err(PlanError::BadArgument(arg::VIEWS)),
    };
    // `@oneOf`: exactly one key is set.
    let (key, v) = obj
        .into_iter()
        .next()
        .ok_or(PlanError::BadArgument(arg::VIEWS))?;
    let step = match key.as_str() {
        fld::WINDOW => {
            let (start, end) = window_value(v)?;
            ViewStep::View(VK::Window { start, end })
        }
        fld::SHRINK_WINDOW => {
            let (start, end) = window_value(v)?;
            ViewStep::View(VK::ShrinkWindow { start, end })
        }
        fld::AT => ViewStep::View(VK::At(time_value(v)?)),
        fld::BEFORE => ViewStep::View(VK::Before(time_value(v)?)),
        fld::AFTER => ViewStep::View(VK::After(time_value(v)?)),
        fld::SNAPSHOT_AT => ViewStep::View(VK::SnapshotAt(time_value(v)?)),
        fld::SHRINK_START => ViewStep::View(VK::ShrinkStart(time_value(v)?)),
        fld::SHRINK_END => ViewStep::View(VK::ShrinkEnd(time_value(v)?)),
        fld::LATEST => return Ok(bool_value(&v)?.then_some(ViewStep::View(VK::Latest))),
        fld::SNAPSHOT_LATEST => {
            return Ok(bool_value(&v)?.then_some(ViewStep::View(VK::SnapshotLatest)))
        }
        fld::DEFAULT_LAYER => return Ok(bool_value(&v)?.then_some(ViewStep::View(VK::DefaultLayer))),
        fld::VALID => return Ok(bool_value(&v)?.then_some(ViewStep::View(VK::Valid))),
        fld::LAYERS => ViewStep::View(VK::Layers(strings_value(v)?)),
        fld::EXCLUDE_LAYERS => ViewStep::View(VK::ExcludeLayers(strings_value(v)?)),
        fld::EXCLUDE_LAYER => ViewStep::View(VK::ExcludeLayer(string_value(v)?.into_boxed_str())),
        fld::TYPE_FILTER => ViewStep::View(VK::TypeFilter(strings_value(v)?)),
        fld::SUBGRAPH => ViewStep::View(VK::Subgraph(node_ids_value(v)?)),
        fld::SUBGRAPH_NODE_TYPES => ViewStep::View(VK::SubgraphNodeTypes(strings_value(v)?)),
        fld::EXCLUDE_NODES => ViewStep::View(VK::ExcludeNodes(node_ids_value(v)?)),
        fld::NODE_FILTER => ViewStep::NodeFilter(parse_node_filter(v, fld::NODE_FILTER)?),
        fld::EDGE_FILTER => ViewStep::EdgeFilter(parse_edge_filter(v, fld::EDGE_FILTER)?),
        other => {
            return Err(PlanError::Unsupported {
                ty: "ViewCollection".into(),
                field: other.into(),
            })
        }
    };
    Ok(Some(step))
}

/// Parse a `TimeInput` value (already lowered to a `GqlValue`) into an
/// `EventTime`, via the same scalar parser the resolvers use.
fn time_value(v: GqlValue) -> Result<EventTime, PlanError> {
    use dynamic_graphql::ScalarValue;
    use raphtory_api::core::utils::time::IntoTime;
    GqlTimeInput::from_value(v)
        .map(IntoTime::into_time)
        .map_err(|_| PlanError::BadArgument(arg::VIEWS))
}

/// Parse a `Window` value object (`{start, end}`) into a `(start, end)` pair.
fn window_value(v: GqlValue) -> Result<(EventTime, EventTime), PlanError> {
    let start = field_of(&v, arg::START)
        .cloned()
        .ok_or(PlanError::BadArgument(arg::VIEWS))?;
    let end = field_of(&v, arg::END)
        .cloned()
        .ok_or(PlanError::BadArgument(arg::VIEWS))?;
    Ok((time_value(start)?, time_value(end)?))
}

fn bool_value(v: &GqlValue) -> Result<bool, PlanError> {
    match v {
        GqlValue::Boolean(b) => Ok(*b),
        _ => Err(PlanError::BadArgument(arg::VIEWS)),
    }
}

fn string_value(v: GqlValue) -> Result<String, PlanError> {
    match v {
        GqlValue::String(s) => Ok(s),
        _ => Err(PlanError::BadArgument(arg::VIEWS)),
    }
}

fn strings_value(v: GqlValue) -> Result<Box<[String]>, PlanError> {
    match v {
        GqlValue::List(items) => items
            .into_iter()
            .map(|x| match x {
                GqlValue::String(s) => Ok(s),
                _ => Err(PlanError::BadArgument(arg::VIEWS)),
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Vec::into_boxed_slice),
        _ => Err(PlanError::BadArgument(arg::VIEWS)),
    }
}

fn node_ids_value(v: GqlValue) -> Result<Vec<GqlNodeId>, PlanError> {
    match v {
        GqlValue::List(items) => items
            .into_iter()
            .map(|x| match x {
                GqlValue::String(s) => Ok(GqlNodeId(GID::Str(s))),
                GqlValue::Number(n) => n
                    .as_u64()
                    .map(|u| GqlNodeId(GID::U64(u)))
                    .ok_or(PlanError::BadArgument(arg::VIEWS)),
                _ => Err(PlanError::BadArgument(arg::VIEWS)),
            })
            .collect(),
        _ => Err(PlanError::BadArgument(arg::VIEWS)),
    }
}

/// Required node filter (`expr:`).
fn node_filter(f: &Field, arg: &'static str) -> Result<CompositeNodeFilter, PlanError> {
    parse_node_filter(const_arg(f, arg).ok_or(PlanError::MissingArgument(arg))?, arg)
}

/// Required edge filter (`expr:`).
fn edge_filter(f: &Field, arg: &'static str) -> Result<CompositeEdgeFilter, PlanError> {
    parse_edge_filter(const_arg(f, arg).ok_or(PlanError::MissingArgument(arg))?, arg)
}

/// Optional node filter (`select:`), absent/null → `None`.
fn opt_node_filter(
    f: &Field,
    arg: &'static str,
) -> Result<Option<CompositeNodeFilter>, PlanError> {
    match const_arg(f, arg) {
        None | Some(GqlValue::Null) => Ok(None),
        Some(v) => Ok(Some(parse_node_filter(v, arg)?)),
    }
}

/// Optional edge filter (`select:`), absent/null → `None`.
fn opt_edge_filter(
    f: &Field,
    arg: &'static str,
) -> Result<Option<CompositeEdgeFilter>, PlanError> {
    match const_arg(f, arg) {
        None | Some(GqlValue::Null) => Ok(None),
        Some(v) => Ok(Some(parse_edge_filter(v, arg)?)),
    }
}

/// Parse a required `[String!]!` argument into a boxed slice.
fn strings_arg(f: &Field, name: &'static str) -> Result<Box<[String]>, PlanError> {
    let items = match const_arg(f, name) {
        Some(GqlValue::List(items)) => items,
        Some(_) => return Err(PlanError::BadArgument(name)),
        None => return Err(PlanError::MissingArgument(name)),
    };
    items
        .into_iter()
        .map(|v| match v {
            GqlValue::String(s) => Ok(s),
            _ => Err(PlanError::BadArgument(name)),
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Vec::into_boxed_slice)
}

/// Parse a required `[NodeId!]!` argument.
fn node_ids_arg(f: &Field, name: &'static str) -> Result<Vec<GqlNodeId>, PlanError> {
    let items = match const_arg(f, name) {
        Some(GqlValue::List(items)) => items,
        Some(_) => return Err(PlanError::BadArgument(name)),
        None => return Err(PlanError::MissingArgument(name)),
    };
    items
        .into_iter()
        .map(|v| match v {
            GqlValue::String(s) => Ok(GqlNodeId(GID::Str(s))),
            GqlValue::Number(n) => n
                .as_u64()
                .map(|u| GqlNodeId(GID::U64(u)))
                .ok_or(PlanError::BadArgument(name)),
            _ => Err(PlanError::BadArgument(name)),
        })
        .collect()
}

/// Parse the optional `keys: [String!]` whitelist for `values(...)`.
fn keys_arg(f: &Field) -> Result<Option<Box<[String]>>, PlanError> {
    match const_arg(f, arg::KEYS) {
        None | Some(GqlValue::Null) => Ok(None),
        Some(GqlValue::List(items)) => {
            let mut keys = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    GqlValue::String(s) => keys.push(s),
                    _ => return Err(PlanError::BadArgument("keys")),
                }
            }
            Ok(Some(keys.into_boxed_slice()))
        }
        Some(_) => Err(PlanError::BadArgument("keys")),
    }
}

fn node_id_arg(f: &Field, name: &'static str) -> Result<GqlNodeId, PlanError> {
    match const_arg(f, name) {
        Some(GqlValue::String(s)) => Ok(GqlNodeId(GID::Str(s))),
        Some(GqlValue::Number(n)) => n
            .as_u64()
            .map(|u| GqlNodeId(GID::U64(u)))
            .ok_or(PlanError::BadArgument(name)),
        Some(_) => Err(PlanError::BadArgument(name)),
        None => Err(PlanError::MissingArgument(name)),
    }
}

/// Parse `page(limit, offset, pageIndex)` args (`limit` required; the others
/// default to 0).
fn page_arg(f: &Field) -> Result<Page, PlanError> {
    let limit = match const_arg(f, arg::LIMIT) {
        Some(GqlValue::Number(n)) => usize_of(n.as_u64(), arg::LIMIT)?,
        Some(_) => return Err(PlanError::BadArgument(arg::LIMIT)),
        None => return Err(PlanError::MissingArgument(arg::LIMIT)),
    };
    Ok(Page {
        limit,
        offset: opt_usize_arg(f, arg::OFFSET)?.unwrap_or(0),
        page_index: opt_usize_arg(f, arg::PAGE_INDEX)?.unwrap_or(0),
    })
}

fn opt_usize_arg(f: &Field, name: &'static str) -> Result<Option<usize>, PlanError> {
    match const_arg(f, name) {
        None | Some(GqlValue::Null) => Ok(None),
        Some(GqlValue::Number(n)) => Ok(Some(usize_of(n.as_u64(), name)?)),
        Some(_) => Err(PlanError::BadArgument(name)),
    }
}

fn usize_of(v: Option<u64>, name: &'static str) -> Result<usize, PlanError> {
    v.map(|u| u as usize).ok_or(PlanError::BadArgument(name))
}

/// Parse the required `sortBys: [NodeSortBy!]!` list.
fn node_sort_bys_arg(f: &Field) -> Result<Vec<NodeSortBy>, PlanError> {
    sort_bys_list(f)?
        .iter()
        .map(|v| {
            Ok(NodeSortBy {
                reverse: as_opt_bool(field_of(v, "reverse"))?,
                id: as_opt_bool(field_of(v, "id"))?,
                time: as_opt_time(field_of(v, "time"))?,
                property: as_opt_string(field_of(v, "property"))?,
            })
        })
        .collect()
}

/// Parse the required `sortBys: [EdgeSortBy!]!` list.
fn edge_sort_bys_arg(f: &Field) -> Result<Vec<EdgeSortBy>, PlanError> {
    sort_bys_list(f)?
        .iter()
        .map(|v| {
            Ok(EdgeSortBy {
                reverse: as_opt_bool(field_of(v, "reverse"))?,
                src: as_opt_bool(field_of(v, "src"))?,
                dst: as_opt_bool(field_of(v, "dst"))?,
                time: as_opt_time(field_of(v, "time"))?,
                property: as_opt_string(field_of(v, "property"))?,
            })
        })
        .collect()
}

fn sort_bys_list(f: &Field) -> Result<Vec<GqlValue>, PlanError> {
    match const_arg(f, arg::SORT_BYS) {
        Some(GqlValue::List(items)) => Ok(items),
        Some(_) => Err(PlanError::BadArgument(arg::SORT_BYS)),
        None => Err(PlanError::MissingArgument(arg::SORT_BYS)),
    }
}

/// Look up a field inside an input-object `ConstValue`.
fn field_of<'a>(v: &'a GqlValue, key: &str) -> Option<&'a GqlValue> {
    match v {
        GqlValue::Object(obj) => obj.get(key),
        _ => None,
    }
}

fn as_opt_bool(v: Option<&GqlValue>) -> Result<Option<bool>, PlanError> {
    match v {
        None | Some(GqlValue::Null) => Ok(None),
        Some(GqlValue::Boolean(b)) => Ok(Some(*b)),
        Some(_) => Err(PlanError::BadArgument(arg::SORT_BYS)),
    }
}

fn as_opt_string(v: Option<&GqlValue>) -> Result<Option<String>, PlanError> {
    match v {
        None | Some(GqlValue::Null) => Ok(None),
        Some(GqlValue::String(s)) => Ok(Some(s.clone())),
        Some(_) => Err(PlanError::BadArgument(arg::SORT_BYS)),
    }
}

fn as_opt_time(v: Option<&GqlValue>) -> Result<Option<SortByTime>, PlanError> {
    match v {
        None | Some(GqlValue::Null) => Ok(None),
        Some(GqlValue::Enum(name)) => match name.as_str() {
            "LATEST" => Ok(Some(SortByTime::Latest)),
            "EARLIEST" => Ok(Some(SortByTime::Earliest)),
            _ => Err(PlanError::BadArgument(arg::SORT_BYS)),
        },
        Some(_) => Err(PlanError::BadArgument(arg::SORT_BYS)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        client::raphtory_client::RaphtoryGraphQLClient,
        interpreter::{execute, sink::test_collect_json},
        url_encode::url_encode_graph,
        GraphServer,
    };
    use raphtory::{
        db::api::{storage::storage::Config, view::IntoDynamic},
        prelude::{AdditionOps, Graph, GraphViewOps, NO_PROPS},
    };
    use std::{collections::HashMap, time::Duration};
    use tempfile::TempDir;
    use url::Url;

    fn sample_graph() -> Graph {
        let g = Graph::new();
        g.add_edge(1, "ben", "hamza", NO_PROPS, None).unwrap();
        g.add_edge(2, "haaroon", "hamza", NO_PROPS, None).unwrap();
        g.add_edge(3, "ben", "haaroon", NO_PROPS, None).unwrap();
        g
    }

    #[test]
    fn rejects_unknown_field() {
        // `bogus` is not a field on Node in schema.graphql → async-graphql
        // validation rejects it (before our SDL walk would), naming the field.
        let err = plan_request(
            r#"{ graph(path:"g") { nodes { list { bogus } } } }"#,
            &Variables::default(),
        )
        .unwrap_err();
        match err {
            PlanError::Validation(msg) => assert!(msg.contains("bogus"), "{msg}"),
            other => panic!("expected validation error, got {other:?}"),
        }
    }

    #[test]
    fn rejects_unknown_argument() {
        // The SDL type-map walk only checked field existence; async-graphql
        // validation also rejects unknown arguments (KnownArgumentNames).
        let err = plan_request(
            r#"{ graph(path:"g") { nodes(bogusArg: 1) { list { id } } } }"#,
            &Variables::default(),
        )
        .unwrap_err();
        assert!(matches!(err, PlanError::Validation(_)), "{err:?}");
    }

    #[test]
    fn rejects_undefined_variable() {
        // `$missing` is never declared → NoUndefinedVariables.
        let err = plan_request(
            r#"query { graph(path: $missing) { nodes { list { id } } } }"#,
            &Variables::default(),
        )
        .unwrap_err();
        assert!(matches!(err, PlanError::Validation(_)), "{err:?}");
    }

    #[test]
    fn rejects_unimplemented_field() {
        // `name` is a valid Graph field in the SDL, but the interpreter
        // doesn't implement it yet → distinct from a validation failure.
        let err = plan_request(r#"{ graph(path:"g") { name } }"#, &Variables::default()).unwrap_err();
        assert!(matches!(err, PlanError::Unsupported { .. }), "{err:?}");
    }

    #[test]
    fn extracts_graph_path() {
        let p = plan_request(
            r#"{ graph(path:"my/graph") { nodes { list { id } } } }"#,
            &Variables::default(),
        )
        .unwrap();
        assert_eq!(p.graph_path, "my/graph");
    }

    /// async-graphql's *parser* does NOT inline fragments or substitute
    /// variables — it preserves them symbolically in the AST. That resolution is
    /// a separate pass in async-graphql's *execution* layer, which we bypass.
    /// This test pins that behaviour so the gap is documented, not assumed.
    #[test]
    fn parser_preserves_fragments_and_variables() {
        let query = r#"
            query Q($p: String!) {
                graph(path: $p) {
                    nodes { ...NodeFields }
                }
            }
            fragment NodeFields on Nodes { list { id } }
        "#;
        let doc = parse_query(query).unwrap();

        // (1) The fragment is kept as a *definition*, not inlined at the spread.
        assert!(
            doc.fragments.contains_key("NodeFields"),
            "fragment definition should survive parsing"
        );

        let op = match &doc.operations {
            DocumentOperations::Single(op) => &op.node,
            DocumentOperations::Multiple(ops) => &ops.values().next().unwrap().node,
        };
        let graph_field = match &op.selection_set.node.items[0].node {
            Selection::Field(f) => &f.node,
            _ => panic!("expected the graph field"),
        };

        // (2) The `path` argument is still a *variable reference*, not the value.
        let (_, path_val) = graph_field
            .arguments
            .iter()
            .find(|(n, _)| n.node.as_str() == "path")
            .unwrap();
        // It renders as the GraphQL variable `$p` (the variable-capable
        // `async_graphql_value::Value::Variable`), not the literal "g".
        assert_eq!(path_val.node.to_string(), "$p");
        // `into_const()` (what `const_arg` uses) yields None while a variable is
        // unresolved — which is exactly why variable args don't reach the planner.
        assert!(path_val.node.clone().into_const().is_none());

        // (3) Inside `nodes`, the selection is a *spread*, not inlined fields.
        let nodes_field = match &graph_field.selection_set.node.items[0].node {
            Selection::Field(f) => &f.node,
            _ => panic!("expected the nodes field"),
        };
        assert!(
            matches!(
                &nodes_field.selection_set.node.items[0].node,
                Selection::FragmentSpread(_)
            ),
            "spread should remain a FragmentSpread, not be inlined into Fields"
        );
    }

    /// Fragment spreads are now followed transparently during planning (via
    /// `async_graphql::normalize::iter_fields`), so a fragment query plans fine.
    #[test]
    fn plans_through_fragments() {
        let query = r#"
            { graph(path: "g") { nodes { ...NodeFields } } }
            fragment NodeFields on Nodes { list { id } }
        "#;
        assert!(plan_request(query, &Variables::default()).is_ok());
    }

    /// Variables are substituted up front from the provided `variables` map, so
    /// the `path` argument resolves to the variable's value.
    #[test]
    fn substitutes_variable_argument() {
        let query = r#"query Q($p: String!) { graph(path: $p) { nodes { list { id } } } }"#;
        let variables = Variables::from_json(serde_json::json!({ "p": "my/graph" }));
        let planned = plan_request(query, &variables).unwrap();
        assert_eq!(planned.graph_path, "my/graph");
    }

    /// A variable with no provided value falls back to its operation-level default.
    #[test]
    fn substitutes_variable_default() {
        let query =
            r#"query Q($p: String = "default/graph") { graph(path: $p) { nodes { list { id } } } }"#;
        let planned = plan_request(query, &Variables::default()).unwrap();
        assert_eq!(planned.graph_path, "default/graph");
    }

    /// The full vertical slice: a raw query string is parsed, validated against
    /// `schema.graphql`, planned, executed through the streaming sink, and the
    /// result is compared byte-for-byte (as JSON) against the live endpoint.
    #[tokio::test]
    async fn vertical_slice_matches_endpoint() {
        let query = r#"{ graph(path: "g") { nodes { list { id } } } }"#;

        // request -> validate -> plan
        let planned = plan_request(query, &Variables::default()).unwrap();
        assert_eq!(planned.graph_path, "g");

        // stand up the real server (old engine) and send the same graph
        let tempdir = TempDir::new().unwrap();
        let server = GraphServer::new(tempdir.path().to_path_buf(), None, Config::default())
            .await
            .unwrap();
        let port = 43933;
        let _running = server.start_with_port(port).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;

        let client = RaphtoryGraphQLClient::new(
            Url::parse(&format!("http://localhost:{port}/")).unwrap(),
            None,
        );
        let g = sample_graph();
        let encoded = url_encode_graph(g.materialize().unwrap()).unwrap();
        client
            .send_graph(&planned.graph_path, &encoded, true)
            .await
            .unwrap();

        let expected = client.query(query, HashMap::new()).await.unwrap();
        let expected = serde_json::to_value(expected).unwrap(); // {"graph": {...}}

        // execute the plan over the same in-memory graph, with minimal allocation
        let plan = planned.plan;
        let out = test_collect_json(move |sink| execute(&plan, g.into_dynamic(), sink)).await;

        assert_eq!(out["data"], expected);
    }
}
