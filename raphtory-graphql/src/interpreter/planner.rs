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
    plan::{IterKind, LeafKind, Nav, Op, Plan},
    schema::SchemaTypes,
    tokens::{arg, field as fld, ty},
};
use crate::model::graph::{node_id::GqlNodeId, timeindex::dt_format_str_is_valid};
use async_graphql::{
    parser::{
        parse_query,
        types::{DocumentOperations, Field, Selection, SelectionSet},
    },
    Value as GqlValue,
};
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
    #[error("fragments are not supported")]
    Fragments,
}

/// A planned request: the (async-loaded) root graph path plus the compiled plan
/// for its selection set.
#[derive(Debug)]
pub struct PlannedRequest {
    pub graph_path: String,
    pub plan: Plan,
}

/// Parse, validate, and compile a query string into a [`PlannedRequest`].
pub fn plan_request(query: &str) -> Result<PlannedRequest, PlanError> {
    let doc = parse_query(query).map_err(|e| PlanError::Parse(e.to_string()))?;
    let op = match doc.operations {
        DocumentOperations::Single(op) => op.node,
        DocumentOperations::Multiple(ops) => {
            ops.into_iter().next().ok_or(PlanError::NoOperation)?.1.node
        }
    };
    let schema = SchemaTypes::get();

    // The root must select exactly one field, and it must be `graph(path:)`.
    let items = &op.selection_set.node.items;
    if items.len() != 1 {
        return Err(PlanError::BadRoot);
    }
    let graph_field = match &items[0].node {
        Selection::Field(f) => &f.node,
        _ => return Err(PlanError::Fragments),
    };
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
    let children = plan_selection(&ginfo.return_type, &graph_field.selection_set.node, schema)?;

    Ok(PlannedRequest {
        graph_path,
        plan: Plan {
            root_key: root_key.into_boxed_str(),
            children,
        },
    })
}

/// Compile a selection set whose fields are selected on `parent_type`.
fn plan_selection(
    parent_type: &str,
    sel: &SelectionSet,
    schema: &SchemaTypes,
) -> Result<Box<[Op]>, PlanError> {
    let mut ops = Vec::with_capacity(sel.items.len());
    for item in &sel.items {
        let field = match &item.node {
            Selection::Field(f) => &f.node,
            _ => return Err(PlanError::Fragments),
        };
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
                children: plan_selection(&finfo.return_type, &field.selection_set.node, schema)?,
            },
            OpKind::List(iter) => Op::List {
                key: key.into(),
                iter,
                children: plan_selection(&finfo.return_type, &field.selection_set.node, schema)?,
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

    // Fields shared across types collapse into one arm via or-patterns on the
    // parent type. The parent type is *kept* (not dropped) so a field that the
    // SDL allows on a type the interpreter hasn't wired falls through to the
    // `Unsupported` arm — a clean pre-stream error rather than an exec panic.
    Ok(match (parent_type, field) {
        // ── entry points: graph → collections / lookups ──
        (ty::GRAPH, fld::NODES) => Navigate(N::Nodes),
        (ty::GRAPH, fld::NODE) => Navigate(N::Node(node_id_arg(f, arg::NAME)?)),
        (ty::GRAPH, fld::EDGE) => Navigate(N::Edge {
            src: node_id_arg(f, arg::SRC)?,
            dst: node_id_arg(f, arg::DST)?,
        }),

        // ── traversal (Node) and edge endpoints ──
        (ty::GRAPH | ty::NODE, fld::EDGES) => {
            reject_select(f, parent_type, field)?;
            Navigate(N::Edges)
        }
        (ty::NODE, fld::IN_EDGES) => {
            reject_select(f, parent_type, field)?;
            Navigate(N::InEdges)
        }
        (ty::NODE, fld::OUT_EDGES) => {
            reject_select(f, parent_type, field)?;
            Navigate(N::OutEdges)
        }
        (ty::NODE, fld::NEIGHBOURS) => {
            reject_select(f, parent_type, field)?;
            Navigate(N::Neighbours)
        }
        (ty::NODE, fld::IN_NEIGHBOURS) => {
            reject_select(f, parent_type, field)?;
            Navigate(N::InNeighbours)
        }
        (ty::NODE, fld::OUT_NEIGHBOURS) => {
            reject_select(f, parent_type, field)?;
            Navigate(N::OutNeighbours)
        }
        (ty::NODE, fld::IN_COMPONENT) => Navigate(N::InComponent),
        (ty::NODE, fld::OUT_COMPONENT) => Navigate(N::OutComponent),
        (ty::EDGE, fld::SRC) => Navigate(N::Src),
        (ty::EDGE, fld::DST) => Navigate(N::Dst),
        (ty::EDGE, fld::NBR) => Navigate(N::Nbr),
        (ty::EDGE, fld::EXPLODE) => Navigate(N::Explode),
        (ty::EDGE, fld::EXPLODE_LAYERS) => Navigate(N::ExplodeLayers),
        (ty::EDGE, fld::DELETIONS) => Navigate(N::Deletions),

        // ── view transforms (same type in → out) ──
        (ty::GRAPH | ty::NODE | ty::EDGE, fld::LAYER) => {
            Navigate(N::Layer(string_arg(f, arg::NAME)?.into()))
        }
        (ty::GRAPH | ty::NODE | ty::EDGE, fld::WINDOW) => Navigate(N::Window {
            start: time_arg(f, arg::START)?,
            end: time_arg(f, arg::END)?,
        }),
        (ty::NODE | ty::EDGE, fld::AFTER) => Navigate(N::After(time_arg(f, arg::TIME)?)),
        (ty::NODE | ty::EDGE, fld::BEFORE) => Navigate(N::Before(time_arg(f, arg::TIME)?)),

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

        // ── lists (parent type disambiguates the item) ──
        (ty::NODES, fld::LIST) => List(I::NodesList),
        (ty::EDGES, fld::LIST) => List(I::EdgesList),
        (ty::PATH_FROM_NODE, fld::LIST) => List(I::NeighboursList),
        (ty::HISTORY, fld::LIST) => List(I::HistoryList),
        (ty::PROPERTIES, fld::VALUES) => List(I::PropertiesValues(keys_arg(f)?)),
        (ty::METADATA, fld::VALUES) => List(I::MetadataValues(keys_arg(f)?)),
        (ty::TEMPORAL_PROPERTIES, fld::VALUES) => List(I::TemporalValues(keys_arg(f)?)),
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
        (ty::PROPERTY, fld::AS_STRING) => Leaf(L::AsString),
        (ty::PROPERTY, fld::VALUE) => Leaf(L::Value),

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

/// Parse a `TimeInput` argument. The POC supports the integer form (millis
/// since epoch), built into an `EventTime` exactly as the resolvers do
/// (`EventTime::start`). String/object time forms are not yet supported.
fn time_arg(f: &Field, name: &'static str) -> Result<EventTime, PlanError> {
    match const_arg(f, name) {
        Some(GqlValue::Number(n)) => n
            .as_i64()
            .map(EventTime::start)
            .ok_or(PlanError::BadArgument(name)),
        Some(_) => Err(PlanError::BadArgument(name)),
        None => Err(PlanError::MissingArgument(name)),
    }
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

/// Reject a `select:` filter argument on a traversal field — filtering is
/// pushed into raphtory but not wired yet, and silently ignoring it would change
/// the output.
fn reject_select(f: &Field, parent_type: &str, label: &str) -> Result<(), PlanError> {
    if f.get_argument(arg::SELECT).is_some() {
        return Err(PlanError::Unsupported {
            ty: parent_type.into(),
            field: format!("{label}(select:)"),
        });
    }
    Ok(())
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
        // `bogus` is not a field on Node in schema.graphql
        let err = plan_request(r#"{ graph(path:"g") { nodes { list { bogus } } } }"#).unwrap_err();
        assert!(matches!(err, PlanError::UnknownField { .. }), "{err:?}");
    }

    #[test]
    fn rejects_unimplemented_field() {
        // `name` is a valid Graph field in the SDL, but the interpreter
        // doesn't implement it yet → distinct from a validation failure.
        let err = plan_request(r#"{ graph(path:"g") { name } }"#).unwrap_err();
        assert!(matches!(err, PlanError::Unsupported { .. }), "{err:?}");
    }

    #[test]
    fn extracts_graph_path() {
        let p = plan_request(r#"{ graph(path:"my/graph") { nodes { list { id } } } }"#).unwrap();
        assert_eq!(p.graph_path, "my/graph");
    }

    /// The full vertical slice: a raw query string is parsed, validated against
    /// `schema.graphql`, planned, executed through the streaming sink, and the
    /// result is compared byte-for-byte (as JSON) against the live endpoint.
    #[tokio::test]
    async fn vertical_slice_matches_endpoint() {
        let query = r#"{ graph(path: "g") { nodes { list { id } } } }"#;

        // request -> validate -> plan
        let planned = plan_request(query).unwrap();
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
