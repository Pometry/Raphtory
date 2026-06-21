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
};
use crate::model::graph::node_id::GqlNodeId;
use async_graphql::{
    parser::{
        parse_query,
        types::{DocumentOperations, Field, Selection, SelectionSet},
    },
    Value as GqlValue,
};
use raphtory_api::core::{entities::GID, storage::timeindex::EventTime};

/// The root object type in `schema.graphql`.
const ROOT_TYPE: &str = "QueryRoot";

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
    if graph_field.name.node.as_str() != "graph" {
        return Err(PlanError::BadRoot);
    }
    let ginfo = schema
        .field(ROOT_TYPE, "graph")
        .ok_or_else(|| PlanError::UnknownField {
            ty: ROOT_TYPE.into(),
            field: "graph".into(),
        })?;

    let graph_path = string_arg(graph_field, "path")?;
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
    Ok(match (parent_type, field) {
        ("Graph", "nodes") => OpKind::Navigate(Nav::Nodes),
        ("Graph", "node") => OpKind::Navigate(Nav::Node(node_id_arg(f)?)),
        ("Graph", "window") => OpKind::Navigate(Nav::Window {
            start: time_arg(f, "start")?,
            end: time_arg(f, "end")?,
        }),
        ("Nodes", "list") => OpKind::List(IterKind::NodesList),
        ("Node", "id") => OpKind::Leaf(LeafKind::Id),
        ("Node", "name") => OpKind::Leaf(LeafKind::Name),
        ("Node", "history") => OpKind::Navigate(Nav::History),
        ("Node", "after") => OpKind::Navigate(Nav::After(time_arg(f, "time")?)),
        ("Node", "before") => OpKind::Navigate(Nav::Before(time_arg(f, "time")?)),
        ("Node", "window") => OpKind::Navigate(Nav::Window {
            start: time_arg(f, "start")?,
            end: time_arg(f, "end")?,
        }),
        ("Node", "neighbours") => {
            // `select` filters the neighbour set and changes output — refuse it
            // rather than silently ignore it until the interpreter supports it.
            if f.get_argument("select").is_some() {
                return Err(PlanError::Unsupported {
                    ty: parent_type.into(),
                    field: "neighbours(select:)".into(),
                });
            }
            OpKind::Navigate(Nav::Neighbours)
        }
        ("PathFromNode", "list") => OpKind::List(IterKind::NeighboursList),
        ("History", "list") => OpKind::List(IterKind::HistoryList),
        ("EventTime", "timestamp") => OpKind::Leaf(LeafKind::Timestamp),
        ("EventTime", "eventId") => OpKind::Leaf(LeafKind::EventId),
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

fn node_id_arg(f: &Field) -> Result<GqlNodeId, PlanError> {
    match const_arg(f, "name") {
        Some(GqlValue::String(s)) => Ok(GqlNodeId(GID::Str(s))),
        Some(GqlValue::Number(n)) => n
            .as_u64()
            .map(|u| GqlNodeId(GID::U64(u)))
            .ok_or(PlanError::BadArgument("name")),
        Some(_) => Err(PlanError::BadArgument("name")),
        None => Err(PlanError::MissingArgument("name")),
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
        // `uniqueLayers` is a valid Graph field in the SDL, but the interpreter
        // doesn't implement it yet → distinct from a validation failure.
        let err = plan_request(r#"{ graph(path:"g") { uniqueLayers } }"#).unwrap_err();
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
        let server = GraphServer::new(tempdir.path().to_path_buf(), None, None, Config::default())
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
