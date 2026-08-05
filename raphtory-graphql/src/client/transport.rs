//! Wire abstraction for talking to a remote graph server.
//!
//! V1 implementation is `GraphqlTransport`, which renders ops as GraphQL
//! queries against the existing GraphQL server. Future transports (e.g. a
//! gRPC-based one) can be swapped in by implementing this trait — client
//! wrappers won't change.

use crate::client::{op::Op, remote_history::RemoteEventTime, ClientError};
use async_graphql::async_trait;
use raphtory_api::core::entities::properties::prop::Prop;

/// Executes a graph operation against a remote server.
///
/// Return semantics:
/// - `Ok(None)` — write succeeded with no return value.
/// - `Ok(Some(prop))` — read returned a scalar (`Prop::I64` for `degree`,
///   `Prop::Str` for `name`, etc.).
/// - `Err(_)` — RPC or protocol failure.
///
/// If richer return shapes are needed later (Arrow columns, node handles),
/// this signature grows to a purpose-fit `Value` enum. For now, `Option<Prop>`
/// covers everything we ship.
///
/// Implementations are expected to be `Send + Sync` and cheaply cloneable behind
/// an `Arc` — client wrappers hold `Arc<dyn Transport>` and clone the handle when
/// constructing child references (`RemoteGraph::node`, etc.).
#[async_trait::async_trait]
pub trait Transport: Send + Sync {
    async fn execute(&self, op: &Op) -> Result<Option<Prop>, ClientError>;
}

// ============ Result decoding ============
//
// The decode half of the `Transport` contract: every `expect_*` function
// unwraps one documented `Option<Prop>` result shape (see the `ReadExpr`
// terminal docs for which terminal produces which shape). Wrappers call
// these to turn transport results into typed values; a second transport
// implementation must produce exactly the shapes these functions accept.

/// Unwrap a `Transport::execute` result expecting a `Prop::I64` scalar.
/// `context` is used for the error message if the shape doesn't match.
pub(crate) fn expect_i64(v: Option<Prop>, context: &str) -> Result<i64, ClientError> {
    match v {
        Some(Prop::I64(n)) => Ok(n),
        _ => Err(ClientError::InvalidResponse(format!(
            "`{}` returned unexpected value type",
            context
        ))),
    }
}

/// Unwrap a `Transport::execute` result expecting a `Prop::Str` scalar.
pub(crate) fn expect_string(v: Option<Prop>, context: &str) -> Result<String, ClientError> {
    match v {
        Some(Prop::Str(s)) => Ok(s.to_string()),
        _ => Err(ClientError::InvalidResponse(format!(
            "`{}` returned unexpected value type",
            context
        ))),
    }
}

/// Unwrap a `Transport::execute` result expecting a nullable `Prop::I64`
/// scalar. `Ok(None)` from the transport means the server returned JSON null
/// (e.g. earliest_time on an empty graph); `Ok(Some(Prop::I64(n)))` is the
/// happy path. Wrong-type payloads become an error.
pub(crate) fn expect_optional_i64(
    v: Option<Prop>,
    context: &str,
) -> Result<Option<i64>, ClientError> {
    match v {
        None => Ok(None),
        Some(Prop::I64(n)) => Ok(Some(n)),
        Some(_) => Err(ClientError::InvalidResponse(format!(
            "`{}` returned unexpected value type",
            context
        ))),
    }
}

/// Unwrap a `Transport::execute` result expecting a `Prop::Bool` scalar.
pub(crate) fn expect_bool(v: Option<Prop>, context: &str) -> Result<bool, ClientError> {
    match v {
        Some(Prop::Bool(b)) => Ok(b),
        _ => Err(ClientError::InvalidResponse(format!(
            "`{}` returned unexpected value type",
            context
        ))),
    }
}

/// Unwrap a `Transport::execute` result expecting a nullable `Prop::Str`
/// scalar. `Ok(None)` means the server returned JSON null (e.g. `node_type`
/// when the type isn't set); `Ok(Some(Prop::Str(s)))` is the happy path.
pub(crate) fn expect_optional_string(
    v: Option<Prop>,
    context: &str,
) -> Result<Option<String>, ClientError> {
    match v {
        None => Ok(None),
        Some(Prop::Str(s)) => Ok(Some(s.to_string())),
        Some(_) => Err(ClientError::InvalidResponse(format!(
            "`{}` returned unexpected value type",
            context
        ))),
    }
}

/// Unwrap a `Transport::execute` result expecting a `Prop::List` of
/// `Prop::Str`s (e.g. the result of `.ids()` on a collection).
pub(crate) fn expect_string_list(
    v: Option<Prop>,
    context: &str,
) -> Result<Vec<String>, ClientError> {
    match v {
        Some(Prop::List(items)) => items
            .iter()
            .map(|p| match p {
                Prop::Str(s) => Ok(s.to_string()),
                _ => Err(ClientError::InvalidResponse(format!(
                    "`{}` list contains non-string element",
                    context
                ))),
            })
            .collect(),
        _ => Err(ClientError::InvalidResponse(format!(
            "`{}` returned unexpected value type",
            context
        ))),
    }
}

/// Unwrap a `Transport::execute` result expecting a `Prop::List` of
/// `Prop::List` of `Prop::Str` (a nested list of node ids) — e.g. the result
/// of `.ids()` on a `PathFromGraph` collection, where each inner list holds
/// the neighbours of one source node.
pub(crate) fn expect_nested_string_list(
    v: Option<Prop>,
    context: &str,
) -> Result<Vec<Vec<String>>, ClientError> {
    match v {
        Some(Prop::List(rows)) => rows
            .iter()
            .map(|row| match row {
                Prop::List(items) => items
                    .iter()
                    .map(|p| match p {
                        Prop::Str(s) => Ok(s.to_string()),
                        _ => Err(ClientError::InvalidResponse(format!(
                            "`{}` inner list contains non-string element",
                            context
                        ))),
                    })
                    .collect::<Result<Vec<String>, ClientError>>(),
                _ => Err(ClientError::InvalidResponse(format!(
                    "`{}` outer list contains non-list element",
                    context
                ))),
            })
            .collect(),
        _ => Err(ClientError::InvalidResponse(format!(
            "`{}` returned unexpected value type",
            context
        ))),
    }
}

/// Unwrap a `Transport::execute` result expecting a `Prop::List` of
/// `Prop::Bool`s — e.g. the per-edge `is_valid()` / `is_active()` accessors
/// on a flat `Edges` collection.
pub(crate) fn expect_bool_list(v: Option<Prop>, context: &str) -> Result<Vec<bool>, ClientError> {
    match v {
        Some(Prop::List(items)) => items
            .iter()
            .map(|p| match p {
                Prop::Bool(b) => Ok(b),
                _ => Err(ClientError::InvalidResponse(format!(
                    "`{}` list contains non-bool element",
                    context
                ))),
            })
            .collect(),
        _ => Err(ClientError::InvalidResponse(format!(
            "`{}` returned unexpected value type",
            context
        ))),
    }
}

/// Unwrap a `Transport::execute` result expecting a `Prop::List` of
/// `Prop::List` of `Prop::Bool` (a nested list of booleans) — e.g. the
/// per-edge `is_valid()` accessor on a `NestedEdges` collection, where each
/// inner list holds one source node's incident edges.
pub(crate) fn expect_nested_bool_list(
    v: Option<Prop>,
    context: &str,
) -> Result<Vec<Vec<bool>>, ClientError> {
    match v {
        Some(Prop::List(rows)) => rows
            .iter()
            .map(|row| match row {
                Prop::List(items) => items
                    .iter()
                    .map(|p| match p {
                        Prop::Bool(b) => Ok(b),
                        _ => Err(ClientError::InvalidResponse(format!(
                            "`{}` inner list contains non-bool element",
                            context
                        ))),
                    })
                    .collect::<Result<Vec<bool>, ClientError>>(),
                _ => Err(ClientError::InvalidResponse(format!(
                    "`{}` outer list contains non-list element",
                    context
                ))),
            })
            .collect(),
        _ => Err(ClientError::InvalidResponse(format!(
            "`{}` returned unexpected value type",
            context
        ))),
    }
}

/// Unwrap a `Transport::execute` result expecting a `Prop::List` of
/// `Prop::I64`s. Used by sub-container list/page terminals when the parent
/// is `Timestamps`, `EventIds`, or `Intervals`.
pub(crate) fn expect_i64_list(v: Option<Prop>, context: &str) -> Result<Vec<i64>, ClientError> {
    match v {
        Some(Prop::List(items)) => items
            .iter()
            .map(|p| match p {
                Prop::I64(n) => Ok(n),
                _ => Err(ClientError::InvalidResponse(format!(
                    "`{}` list contains non-i64 element",
                    context
                ))),
            })
            .collect(),
        _ => Err(ClientError::InvalidResponse(format!(
            "`{}` returned unexpected value type",
            context
        ))),
    }
}

/// Unwrap a `Transport::execute` result expecting a `Prop::List` of
/// `Prop::List` of `Prop::I64` (a nested list of integers) — e.g. the result
/// of `.degree()` on a `PathFromGraph` collection, where each inner list holds
/// the per-node degrees of one source node's neighbours.
pub(crate) fn expect_nested_i64_list(
    v: Option<Prop>,
    context: &str,
) -> Result<Vec<Vec<i64>>, ClientError> {
    match v {
        Some(Prop::List(rows)) => rows
            .iter()
            .map(|row| match row {
                Prop::List(items) => items
                    .iter()
                    .map(|p| match p {
                        Prop::I64(n) => Ok(n),
                        _ => Err(ClientError::InvalidResponse(format!(
                            "`{}` inner list contains non-i64 element",
                            context
                        ))),
                    })
                    .collect::<Result<Vec<i64>, ClientError>>(),
                _ => Err(ClientError::InvalidResponse(format!(
                    "`{}` outer list contains non-list element",
                    context
                ))),
            })
            .collect(),
        _ => Err(ClientError::InvalidResponse(format!(
            "`{}` returned unexpected value type",
            context
        ))),
    }
}

/// Unwrap a `Transport::execute` result expecting a `Prop::List` of
/// `Prop::Map({key, value})` records — used by `PropertyValues`.
pub(crate) fn expect_property_list(
    v: Option<Prop>,
    context: &str,
) -> Result<Vec<(String, Prop)>, ClientError> {
    match v {
        Some(Prop::List(items)) => items
            .iter()
            .map(|p| match p {
                Prop::Map(map) => extract_key_value_pair(&*map, context),
                _ => Err(ClientError::InvalidResponse(format!(
                    "`{}` element not a Prop::Map",
                    context
                ))),
            })
            .collect(),
        _ => Err(ClientError::InvalidResponse(format!(
            "`{}` returned unexpected value type",
            context
        ))),
    }
}

/// Unwrap a flat columnar property/metadata result — a `Prop::List` where each
/// element is itself a `Prop::List` of `{key, value}` records (one inner list
/// per collection member). Used by the collection-level `RemoteMetadataView` /
/// `RemotePropertiesView` on flat collections.
pub(crate) fn expect_columnar_property_list(
    v: Option<Prop>,
    context: &str,
) -> Result<Vec<Vec<(String, Prop)>>, ClientError> {
    match v {
        Some(Prop::List(members)) => members
            .iter()
            .map(|member| match member {
                Prop::List(pairs) => pairs
                    .iter()
                    .map(|p| match p {
                        Prop::Map(map) => extract_key_value_pair(&*map, context),
                        _ => Err(ClientError::InvalidResponse(format!(
                            "`{}` entry not a Prop::Map",
                            context
                        ))),
                    })
                    .collect(),
                _ => Err(ClientError::InvalidResponse(format!(
                    "`{}` member not a Prop::List",
                    context
                ))),
            })
            .collect(),
        _ => Err(ClientError::InvalidResponse(format!(
            "`{}` returned unexpected value type",
            context
        ))),
    }
}

/// Unwrap a nested columnar property/metadata result — a `Prop::List` of
/// per-source `Prop::List`s, each holding per-member `Prop::List`s of
/// `{key, value}` records. Used by the collection-level views on nested
/// collections (`PathFromGraph` / `NestedEdges`).
pub(crate) fn expect_nested_columnar_property_list(
    v: Option<Prop>,
    context: &str,
) -> Result<Vec<Vec<Vec<(String, Prop)>>>, ClientError> {
    match v {
        Some(Prop::List(sources)) => sources
            .iter()
            .map(|source| expect_columnar_property_list(Some(source.clone()), context))
            .collect(),
        _ => Err(ClientError::InvalidResponse(format!(
            "`{}` returned unexpected value type",
            context
        ))),
    }
}

fn extract_key_value_pair(
    map: &rustc_hash::FxHashMap<raphtory_api::core::storage::arc_str::ArcStr, Prop>,
    context: &str,
) -> Result<(String, Prop), ClientError> {
    let key = match map.get("key") {
        Some(Prop::Str(s)) => s.to_string(),
        _ => {
            return Err(ClientError::InvalidResponse(format!(
                "`{}` record missing `key`",
                context
            )))
        }
    };
    let value = map.get("value").cloned().ok_or_else(|| {
        ClientError::InvalidResponse(format!("`{}` record missing `value`", context))
    })?;
    Ok((key, value))
}

/// Unwrap a `Transport::execute` result expecting a nullable polymorphic
/// `Prop` scalar. Used by TemporalProperty terminals like `at` / `latest`
/// that return an arbitrary property value or null.
pub(crate) fn expect_optional_prop(
    v: Option<Prop>,
    _context: &str,
) -> Result<Option<Prop>, ClientError> {
    Ok(v)
}

/// Unwrap a `Transport::execute` result expecting a nullable property tuple
/// (a `Prop::Map` with `time` and `value` keys). Used by TemporalProperty
/// stats returning an optional `(time, value)` pair.
pub(crate) fn expect_optional_property_tuple(
    v: Option<Prop>,
    context: &str,
) -> Result<Option<(RemoteEventTime, Prop)>, ClientError> {
    match v {
        None => Ok(None),
        Some(Prop::Map(map)) => extract_property_tuple(&*map, context).map(Some),
        Some(_) => Err(ClientError::InvalidResponse(format!(
            "`{}` returned unexpected value type",
            context
        ))),
    }
}

/// Unwrap a list of property tuples (used by `orderedDedupe`).
pub(crate) fn expect_property_tuple_list(
    v: Option<Prop>,
    context: &str,
) -> Result<Vec<(RemoteEventTime, Prop)>, ClientError> {
    match v {
        Some(Prop::List(items)) => items
            .iter()
            .map(|p| match p {
                Prop::Map(map) => extract_property_tuple(&*map, context),
                _ => Err(ClientError::InvalidResponse(format!(
                    "`{}` element not a Prop::Map",
                    context
                ))),
            })
            .collect(),
        _ => Err(ClientError::InvalidResponse(format!(
            "`{}` returned unexpected value type",
            context
        ))),
    }
}

fn extract_property_tuple(
    map: &rustc_hash::FxHashMap<raphtory_api::core::storage::arc_str::ArcStr, Prop>,
    context: &str,
) -> Result<(RemoteEventTime, Prop), ClientError> {
    let time = match map.get("time") {
        Some(Prop::Map(time_map)) => extract_event_time(&*time_map),
        _ => {
            return Err(ClientError::InvalidResponse(format!(
                "`{}` tuple missing `time`",
                context
            )))
        }
    };
    let value = map.get("value").cloned().ok_or_else(|| {
        ClientError::InvalidResponse(format!("`{}` tuple missing `value`", context))
    })?;
    Ok((time, value))
}

fn extract_event_time(
    map: &rustc_hash::FxHashMap<raphtory_api::core::storage::arc_str::ArcStr, Prop>,
) -> RemoteEventTime {
    let timestamp = match map.get("timestamp") {
        Some(Prop::I64(n)) => Some(*n),
        _ => None,
    };
    let dt = match map.get("datetime") {
        Some(Prop::Str(s)) => Some(s.to_string()),
        _ => None,
    };
    let event_id = match map.get("eventId") {
        Some(Prop::I64(n)) => Some(*n),
        _ => None,
    };
    RemoteEventTime {
        timestamp,
        dt,
        event_id,
    }
}

/// Unwrap a `Transport::execute` result expecting a `Prop::List` of
/// arbitrary polymorphic `Prop`s. Used by `TemporalPropertyValueList`.
pub(crate) fn expect_prop_list(v: Option<Prop>, context: &str) -> Result<Vec<Prop>, ClientError> {
    match v {
        Some(Prop::List(items)) => Ok(items.iter().collect()),
        _ => Err(ClientError::InvalidResponse(format!(
            "`{}` returned unexpected value type",
            context
        ))),
    }
}

/// Unwrap a `Transport::execute` result expecting a nullable EventTime
/// terminal (`earliest_time`, `latest_time`, `start`, `end`, `time`). The
/// transport returns `Some(Prop::Map({timestamp, datetime, eventId}))` for a
/// present value, or `None` (JSON null) for an absent one (e.g. empty graph).
pub(crate) fn expect_optional_event_time(
    v: Option<Prop>,
    context: &str,
) -> Result<Option<RemoteEventTime>, ClientError> {
    match v {
        None => Ok(None),
        Some(Prop::Map(map)) => Ok(Some(extract_event_time(&map))),
        Some(_) => Err(ClientError::InvalidResponse(format!(
            "`{}` returned unexpected value type",
            context
        ))),
    }
}

/// Unwrap a `Transport::execute` result expecting a nullable `Prop::F64`
/// scalar. Used by `IntervalsMean`.
pub(crate) fn expect_optional_f64(
    v: Option<Prop>,
    context: &str,
) -> Result<Option<f64>, ClientError> {
    match v {
        None => Ok(None),
        Some(Prop::F64(n)) => Ok(Some(n)),
        Some(_) => Err(ClientError::InvalidResponse(format!(
            "`{}` returned unexpected value type",
            context
        ))),
    }
}

/// Unwrap a `Transport::execute` result expecting a `HistoryList` /
/// `HistoryListRev` terminal — a `Prop::List` of `Prop::Map` records where
/// each map may contain `timestamp` (i64), `dt` (String), and `eventId`
/// (i64). Missing keys decode to `None` on the corresponding field.
pub(crate) fn expect_event_time_list(
    v: Option<Prop>,
    context: &str,
) -> Result<Vec<RemoteEventTime>, ClientError> {
    match v {
        Some(Prop::List(items)) => items
            .iter()
            .map(|p| match p {
                Prop::Map(map) => {
                    let timestamp = match map.get("timestamp") {
                        Some(Prop::I64(n)) => Some(*n),
                        _ => None,
                    };
                    let dt = match map.get("datetime") {
                        Some(Prop::Str(s)) => Some(s.to_string()),
                        _ => None,
                    };
                    let event_id = match map.get("eventId") {
                        Some(Prop::I64(n)) => Some(*n),
                        _ => None,
                    };
                    Ok(RemoteEventTime {
                        timestamp,
                        dt,
                        event_id,
                    })
                }
                _ => Err(ClientError::InvalidResponse(format!(
                    "`{}` element not a Prop::Map",
                    context
                ))),
            })
            .collect(),
        _ => Err(ClientError::InvalidResponse(format!(
            "`{}` returned unexpected value type",
            context
        ))),
    }
}

/// Unwrap a `Transport::execute` result expecting an EdgesList terminal — a
/// `Prop::List` of 2-element `Prop::List([src, dst])` string pairs.
pub(crate) fn expect_edge_list(
    v: Option<Prop>,
    context: &str,
) -> Result<Vec<(String, String)>, ClientError> {
    match v {
        Some(Prop::List(items)) => items
            .iter()
            .map(|p| match p {
                Prop::List(pair) => {
                    let mut it = pair.iter();
                    let src = it.next().ok_or_else(|| {
                        ClientError::InvalidResponse(format!("`{}` element missing src", context))
                    })?;
                    let dst = it.next().ok_or_else(|| {
                        ClientError::InvalidResponse(format!("`{}` element missing dst", context))
                    })?;
                    if it.next().is_some() {
                        return Err(ClientError::InvalidResponse(format!(
                            "`{}` element has more than 2 items",
                            context
                        )));
                    }
                    let src = match src {
                        Prop::Str(s) => s.to_string(),
                        _ => {
                            return Err(ClientError::InvalidResponse(format!(
                                "`{}` src not a string",
                                context
                            )))
                        }
                    };
                    let dst = match dst {
                        Prop::Str(s) => s.to_string(),
                        _ => {
                            return Err(ClientError::InvalidResponse(format!(
                                "`{}` dst not a string",
                                context
                            )))
                        }
                    };
                    Ok((src, dst))
                }
                _ => Err(ClientError::InvalidResponse(format!(
                    "`{}` element not a pair",
                    context
                ))),
            })
            .collect(),
        _ => Err(ClientError::InvalidResponse(format!(
            "`{}` returned unexpected value type",
            context
        ))),
    }
}

/// One member of an exploded-edge fetch: `(src, dst, time, event_id,
/// layer_name)` — everything needed to pin a handle to the event.
pub(crate) type ExplodedEdgeRecord = (String, String, i64, i64, String);

/// Decode one 5-element `[src, dst, timestamp, event_id, layer_name]` inner
/// list produced by the `ExplodedEdgesList` terminals.
fn exploded_edge_record(p: &Prop, context: &str) -> Result<ExplodedEdgeRecord, ClientError> {
    let items: Vec<Prop> = match p {
        Prop::List(items) => items.iter().collect(),
        _ => Vec::new(),
    };
    if items.len() != 5 {
        return Err(ClientError::InvalidResponse(format!(
            "`{}` element not a 5-element exploded-edge record",
            context
        )));
    }
    let str_at = |idx: usize, what: &str| match &items[idx] {
        Prop::Str(s) => Ok(s.to_string()),
        _ => Err(ClientError::InvalidResponse(format!(
            "`{}` {} not a string",
            context, what
        ))),
    };
    let i64_at = |idx: usize, what: &str| match &items[idx] {
        Prop::I64(i) => Ok(*i),
        _ => Err(ClientError::InvalidResponse(format!(
            "`{}` {} not an int",
            context, what
        ))),
    };
    Ok((
        str_at(0, "src")?,
        str_at(1, "dst")?,
        i64_at(2, "timestamp")?,
        i64_at(3, "event_id")?,
        str_at(4, "layer_name")?,
    ))
}

/// Unwrap a `Transport::execute` result expecting a `Prop::List` of exploded
/// edge records. Used by `.collect()` on an exploded `Edges` collection.
pub(crate) fn expect_exploded_edge_list(
    v: Option<Prop>,
    context: &str,
) -> Result<Vec<ExplodedEdgeRecord>, ClientError> {
    match v {
        Some(Prop::List(items)) => items
            .iter()
            .map(|p| exploded_edge_record(&p, context))
            .collect(),
        _ => Err(ClientError::InvalidResponse(format!(
            "`{}` returned unexpected value type",
            context
        ))),
    }
}

/// Decode one `[src, dst, layer]` layer-exploded-edge record.
fn layers_edge_record(p: &Prop, context: &str) -> Result<(String, String, String), ClientError> {
    let items: Vec<Prop> = match p {
        Prop::List(items) => items.iter().collect(),
        _ => Vec::new(),
    };
    if items.len() != 3 {
        return Err(ClientError::InvalidResponse(format!(
            "`{}` element not a 3-element layer-exploded-edge record",
            context
        )));
    }
    let str_at = |idx: usize, what: &str| match &items[idx] {
        Prop::Str(s) => Ok(s.to_string()),
        _ => Err(ClientError::InvalidResponse(format!(
            "`{}` {} not a string",
            context, what
        ))),
    };
    Ok((str_at(0, "src")?, str_at(1, "dst")?, str_at(2, "layer")?))
}

/// Unwrap a `Transport::execute` result for `ExplodedLayersEdgesList` — a
/// `Prop::List` of `[src, dst, layer]` inner lists (no time). Used by
/// `.collect()` on a layer-exploded `Edges` collection.
pub(crate) fn expect_exploded_layers_edge_list(
    v: Option<Prop>,
    context: &str,
) -> Result<Vec<(String, String, String)>, ClientError> {
    match v {
        Some(Prop::List(items)) => items
            .iter()
            .map(|p| layers_edge_record(&p, context))
            .collect(),
        _ => Err(ClientError::InvalidResponse(format!(
            "`{}` returned unexpected value type",
            context
        ))),
    }
}

/// Nested variant — one inner list of `[src, dst, layer]` records per source
/// node. Used by `.collect()` on a layer-exploded `NestedEdges` collection.
pub(crate) fn expect_nested_exploded_layers_edge_list(
    v: Option<Prop>,
    context: &str,
) -> Result<Vec<Vec<(String, String, String)>>, ClientError> {
    match v {
        Some(Prop::List(rows)) => rows
            .iter()
            .map(|row| match row {
                Prop::List(items) => items
                    .iter()
                    .map(|p| layers_edge_record(&p, context))
                    .collect(),
                _ => Err(ClientError::InvalidResponse(format!(
                    "`{}` row not a list",
                    context
                ))),
            })
            .collect(),
        _ => Err(ClientError::InvalidResponse(format!(
            "`{}` returned unexpected value type",
            context
        ))),
    }
}

/// Nested variant of `expect_exploded_edge_list` — one inner list per source
/// node. Used by `.collect()` on an exploded `NestedEdges` collection.
pub(crate) fn expect_nested_exploded_edge_list(
    v: Option<Prop>,
    context: &str,
) -> Result<Vec<Vec<ExplodedEdgeRecord>>, ClientError> {
    match v {
        Some(Prop::List(rows)) => rows
            .iter()
            .map(|row| match row {
                Prop::List(items) => items
                    .iter()
                    .map(|p| exploded_edge_record(&p, context))
                    .collect(),
                _ => Err(ClientError::InvalidResponse(format!(
                    "`{}` row not a list",
                    context
                ))),
            })
            .collect(),
        _ => Err(ClientError::InvalidResponse(format!(
            "`{}` returned unexpected value type",
            context
        ))),
    }
}

/// Unwrap a `Transport::execute` result expecting a `Prop::List` of
/// `Prop::List` of `(src, dst)` pairs — a nested list of edge endpoints, one
/// inner list per source node. Used by `.collect()` on a `NestedEdges`
/// collection. Mirrors `expect_edge_list`, one level deeper.
pub(crate) fn expect_nested_edge_list(
    v: Option<Prop>,
    context: &str,
) -> Result<Vec<Vec<(String, String)>>, ClientError> {
    match v {
        Some(Prop::List(rows)) => rows
            .iter()
            .map(|row| match row {
                Prop::List(items) => items
                    .iter()
                    .map(|p| match p {
                        Prop::List(pair) => {
                            let mut it = pair.iter();
                            let src = it.next().ok_or_else(|| {
                                ClientError::InvalidResponse(format!(
                                    "`{}` element missing src",
                                    context
                                ))
                            })?;
                            let dst = it.next().ok_or_else(|| {
                                ClientError::InvalidResponse(format!(
                                    "`{}` element missing dst",
                                    context
                                ))
                            })?;
                            if it.next().is_some() {
                                return Err(ClientError::InvalidResponse(format!(
                                    "`{}` element has more than 2 items",
                                    context
                                )));
                            }
                            let src = match src {
                                Prop::Str(s) => s.to_string(),
                                _ => {
                                    return Err(ClientError::InvalidResponse(format!(
                                        "`{}` src not a string",
                                        context
                                    )))
                                }
                            };
                            let dst = match dst {
                                Prop::Str(s) => s.to_string(),
                                _ => {
                                    return Err(ClientError::InvalidResponse(format!(
                                        "`{}` dst not a string",
                                        context
                                    )))
                                }
                            };
                            Ok((src, dst))
                        }
                        _ => Err(ClientError::InvalidResponse(format!(
                            "`{}` element not a pair",
                            context
                        ))),
                    })
                    .collect::<Result<Vec<(String, String)>, ClientError>>(),
                _ => Err(ClientError::InvalidResponse(format!(
                    "`{}` outer list contains non-list element",
                    context
                ))),
            })
            .collect(),
        _ => Err(ClientError::InvalidResponse(format!(
            "`{}` returned unexpected value type",
            context
        ))),
    }
}

/// Unwrap a columnar accessor producing `Vec<Option<String>>` — a flat
/// `Prop::List` where each element is a `Prop::List` of 0 (`None`) or 1
/// (`Some`) `Prop::Str`. Used by `Nodes.node_type` / `PathFromNode.node_type`.
pub(crate) fn expect_optional_string_list(
    v: Option<Prop>,
    context: &str,
) -> Result<Vec<Option<String>>, ClientError> {
    match v {
        Some(Prop::List(items)) => items
            .iter()
            .map(|elem| match elem {
                Prop::List(inner) => match inner.iter().next() {
                    None => Ok(None),
                    Some(Prop::Str(s)) => Ok(Some(s.to_string())),
                    Some(_) => Err(ClientError::InvalidResponse(format!(
                        "`{}` element wrapper contains non-string",
                        context
                    ))),
                },
                _ => Err(ClientError::InvalidResponse(format!(
                    "`{}` element not an optional wrapper",
                    context
                ))),
            })
            .collect(),
        _ => Err(ClientError::InvalidResponse(format!(
            "`{}` returned unexpected value type",
            context
        ))),
    }
}

/// Nested form of `expect_optional_string_list` → `Vec<Vec<Option<String>>>`
/// (one inner list per source node). Used by `PathFromGraph.node_type`.
pub(crate) fn expect_nested_optional_string_list(
    v: Option<Prop>,
    context: &str,
) -> Result<Vec<Vec<Option<String>>>, ClientError> {
    match v {
        Some(Prop::List(rows)) => rows
            .iter()
            .map(|row| expect_optional_string_list(Some(row.clone()), context))
            .collect(),
        _ => Err(ClientError::InvalidResponse(format!(
            "`{}` returned unexpected value type",
            context
        ))),
    }
}

/// Unwrap a columnar accessor producing `Vec<Option<RemoteEventTime>>` — a flat
/// `Prop::List` where each element is a `Prop::List` of 0 (`None`) or 1
/// (`Some`) `Prop::Map`. Used by `Edges.earliest_time` / `latest_time` / `time`.
pub(crate) fn expect_optional_event_time_list(
    v: Option<Prop>,
    context: &str,
) -> Result<Vec<Option<RemoteEventTime>>, ClientError> {
    match v {
        Some(Prop::List(items)) => items
            .iter()
            .map(|elem| match elem {
                Prop::List(inner) => match inner.iter().next() {
                    None => Ok(None),
                    Some(Prop::Map(map)) => Ok(Some(extract_event_time(&map))),
                    Some(_) => Err(ClientError::InvalidResponse(format!(
                        "`{}` element wrapper contains non-map",
                        context
                    ))),
                },
                _ => Err(ClientError::InvalidResponse(format!(
                    "`{}` element not an optional wrapper",
                    context
                ))),
            })
            .collect(),
        _ => Err(ClientError::InvalidResponse(format!(
            "`{}` returned unexpected value type",
            context
        ))),
    }
}

/// Nested form of `expect_optional_event_time_list` →
/// `Vec<Vec<Option<RemoteEventTime>>>`. Used by `NestedEdges.earliest_time` etc.
pub(crate) fn expect_nested_optional_event_time_list(
    v: Option<Prop>,
    context: &str,
) -> Result<Vec<Vec<Option<RemoteEventTime>>>, ClientError> {
    match v {
        Some(Prop::List(rows)) => rows
            .iter()
            .map(|row| expect_optional_event_time_list(Some(row.clone()), context))
            .collect(),
        _ => Err(ClientError::InvalidResponse(format!(
            "`{}` returned unexpected value type",
            context
        ))),
    }
}

/// Unwrap a triply-nested string list → `Vec<Vec<Vec<String>>>`. Used by
/// `NestedEdges.layer_names`, where each edge carries a list of layer names,
/// grouped per source node.
pub(crate) fn expect_double_nested_string_list(
    v: Option<Prop>,
    context: &str,
) -> Result<Vec<Vec<Vec<String>>>, ClientError> {
    match v {
        Some(Prop::List(rows)) => rows
            .iter()
            .map(|row| expect_nested_string_list(Some(row.clone()), context))
            .collect(),
        _ => Err(ClientError::InvalidResponse(format!(
            "`{}` returned unexpected value type",
            context
        ))),
    }
}
