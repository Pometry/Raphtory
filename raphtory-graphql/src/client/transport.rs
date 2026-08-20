//! Wire abstraction for talking to a remote graph server.
//!
//! V1 implementation is `GraphqlTransport`, which renders ops as GraphQL
//! queries against the existing GraphQL server. Future transports (e.g. a
//! gRPC-based one) can be swapped in by implementing this trait — client
//! wrappers won't change.

use crate::client::{op::Op, ClientError};
use async_graphql::async_trait;
use raphtory_api::core::{
    entities::{
        properties::prop::{Prop, PropMap, PropUnwrap},
        GID,
    },
    storage::timeindex::EventTime,
};

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

// The uniform decoders below are one-line casts over these combinators: a
// `PropUnwrap` accessor (`into_i64`, `into_bool`, …) names the expected shape,
// and the combinator handles scalar / nullable / list / nested-list plumbing.

/// The shared "shape mismatch" decode error.
fn unexpected(context: &str) -> ClientError {
    ClientError::InvalidResponse(format!("`{}` returned unexpected value type", context))
}

/// Cast a required scalar.
fn cast<T>(
    v: Option<Prop>,
    cast: impl Fn(Prop) -> Option<T>,
    context: &str,
) -> Result<T, ClientError> {
    v.and_then(cast).ok_or_else(|| unexpected(context))
}

/// Cast a nullable scalar: JSON null (`None`) stays `None`; a present value of
/// the wrong type is an error.
fn cast_optional<T>(
    v: Option<Prop>,
    cast: impl Fn(Prop) -> Option<T>,
    context: &str,
) -> Result<Option<T>, ClientError> {
    v.map(|p| cast(p).ok_or_else(|| unexpected(context)))
        .transpose()
}

/// Cast every element of a `Prop::List`.
fn cast_list<T>(
    v: Option<Prop>,
    cast: impl Fn(Prop) -> Option<T>,
    context: &str,
) -> Result<Vec<T>, ClientError> {
    match v {
        Some(Prop::List(items)) => items
            .iter()
            .map(|p| cast(p).ok_or_else(|| unexpected(context)))
            .collect(),
        _ => Err(unexpected(context)),
    }
}

/// Cast a nested list (one inner `Prop::List` per outer element), element-wise.
fn cast_nested_list<T>(
    v: Option<Prop>,
    cast: impl Fn(Prop) -> Option<T> + Copy,
    context: &str,
) -> Result<Vec<Vec<T>>, ClientError> {
    match v {
        Some(Prop::List(rows)) => rows
            .iter()
            .map(|row| cast_list(Some(row), cast, context))
            .collect(),
        _ => Err(unexpected(context)),
    }
}

/// Cast a columnar optional list — each element is a 0-length (`None`) or
/// 1-length (`Some`) `Prop::List` wrapper.
fn cast_optional_wrapper_list<T>(
    v: Option<Prop>,
    cast: impl Fn(Prop) -> Option<T>,
    context: &str,
) -> Result<Vec<Option<T>>, ClientError> {
    match v {
        Some(Prop::List(items)) => items
            .iter()
            .map(|elem| match elem {
                Prop::List(inner) => inner
                    .iter()
                    .next()
                    .map(|p| cast(p).ok_or_else(|| unexpected(context)))
                    .transpose(),
                _ => Err(unexpected(context)),
            })
            .collect(),
        _ => Err(unexpected(context)),
    }
}

/// A `Prop::Str` cast producing an owned `String`.
fn into_string(p: Prop) -> Option<String> {
    p.into_str().map(|s| s.to_string())
}

/// Unwrap a `Transport::execute` result expecting a `Prop::I64` scalar.
/// `context` is used for the error message if the shape doesn't match.
pub(crate) fn expect_i64(v: Option<Prop>, context: &str) -> Result<i64, ClientError> {
    cast(v, PropUnwrap::into_i64, context)
}

/// Unwrap a `Transport::execute` result expecting a `Prop::Str` scalar.
pub(crate) fn expect_string(v: Option<Prop>, context: &str) -> Result<String, ClientError> {
    cast(v, into_string, context)
}

/// Unwrap a `Transport::execute` result expecting a nullable `Prop::I64`
/// scalar. `Ok(None)` from the transport means the server returned JSON null
/// (e.g. earliest_time on an empty graph); `Ok(Some(Prop::I64(n)))` is the
/// happy path. Wrong-type payloads become an error.
pub(crate) fn expect_optional_i64(
    v: Option<Prop>,
    context: &str,
) -> Result<Option<i64>, ClientError> {
    cast_optional(v, PropUnwrap::into_i64, context)
}

/// Unwrap a `Transport::execute` result expecting a `Prop::Bool` scalar.
pub(crate) fn expect_bool(v: Option<Prop>, context: &str) -> Result<bool, ClientError> {
    cast(v, PropUnwrap::into_bool, context)
}

/// Unwrap a `Transport::execute` result expecting a nullable `Prop::Str`
/// scalar. `Ok(None)` means the server returned JSON null (e.g. `node_type`
/// when the type isn't set); `Ok(Some(Prop::Str(s)))` is the happy path.
pub(crate) fn expect_optional_string(
    v: Option<Prop>,
    context: &str,
) -> Result<Option<String>, ClientError> {
    cast_optional(v, into_string, context)
}

/// Unwrap a `Transport::execute` result expecting a `Prop::List` of
/// `Prop::Str`s (e.g. the result of `.ids()` on a collection).
pub(crate) fn expect_string_list(
    v: Option<Prop>,
    context: &str,
) -> Result<Vec<String>, ClientError> {
    cast_list(v, into_string, context)
}

/// A node id decoded from a response `Prop` — string ids arrive as
/// `Prop::Str`, integer ids as `Prop::U64` (see `gid_prop`).
fn into_gid(p: Prop) -> Option<GID> {
    match p {
        Prop::Str(s) => Some(GID::Str(s.to_string())),
        Prop::U64(v) => Some(GID::U64(v)),
        _ => None,
    }
}

/// Unwrap a `Transport::execute` result expecting a single node id.
pub(crate) fn expect_gid(v: Option<Prop>, context: &str) -> Result<GID, ClientError> {
    cast(v, into_gid, context)
}

/// Unwrap a `Transport::execute` result expecting a `Prop::List` of node ids
/// (e.g. the result of `.ids()` on a collection) — typed, not stringified.
pub(crate) fn expect_gid_list(v: Option<Prop>, context: &str) -> Result<Vec<GID>, ClientError> {
    cast_list(v, into_gid, context)
}

/// Nested variant of `expect_gid_list` — e.g. `.ids()` on a `PathFromGraph`
/// collection, where each inner list holds the neighbours of one source node.
pub(crate) fn expect_nested_gid_list(
    v: Option<Prop>,
    context: &str,
) -> Result<Vec<Vec<GID>>, ClientError> {
    cast_nested_list(v, into_gid, context)
}

/// Unwrap a `Transport::execute` result expecting a `Prop::List` of
/// `Prop::List` of `Prop::Str` (a nested list of node ids) — e.g. the result
/// of `.ids()` on a `PathFromGraph` collection, where each inner list holds
/// the neighbours of one source node.
pub(crate) fn expect_nested_string_list(
    v: Option<Prop>,
    context: &str,
) -> Result<Vec<Vec<String>>, ClientError> {
    cast_nested_list(v, into_string, context)
}

/// Unwrap a `Transport::execute` result expecting a `Prop::List` of
/// `Prop::Bool`s — e.g. the per-edge `is_valid()` / `is_active()` accessors
/// on a flat `Edges` collection.
pub(crate) fn expect_bool_list(v: Option<Prop>, context: &str) -> Result<Vec<bool>, ClientError> {
    cast_list(v, PropUnwrap::into_bool, context)
}

/// Unwrap a `Transport::execute` result expecting a `Prop::List` of
/// `Prop::List` of `Prop::Bool` (a nested list of booleans) — e.g. the
/// per-edge `is_valid()` accessor on a `NestedEdges` collection, where each
/// inner list holds one source node's incident edges.
pub(crate) fn expect_nested_bool_list(
    v: Option<Prop>,
    context: &str,
) -> Result<Vec<Vec<bool>>, ClientError> {
    cast_nested_list(v, PropUnwrap::into_bool, context)
}

/// Unwrap a `Transport::execute` result expecting a `Prop::List` of
/// `Prop::I64`s. Used by sub-container list/page terminals when the parent
/// is `Timestamps`, `EventIds`, or `Intervals`.
pub(crate) fn expect_i64_list(v: Option<Prop>, context: &str) -> Result<Vec<i64>, ClientError> {
    cast_list(v, PropUnwrap::into_i64, context)
}

/// Unwrap a `Transport::execute` result expecting a `Prop::List` of
/// `Prop::List` of `Prop::I64` (a nested list of integers) — e.g. the result
/// of `.degree()` on a `PathFromGraph` collection, where each inner list holds
/// the per-node degrees of one source node's neighbours.
pub(crate) fn expect_nested_i64_list(
    v: Option<Prop>,
    context: &str,
) -> Result<Vec<Vec<i64>>, ClientError> {
    cast_nested_list(v, PropUnwrap::into_i64, context)
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

/// Unwrap a columnar property/metadata fetch: a `Prop::List` of columns, each
/// a `Prop::List` of per-member optionals (`[]` absent, `[v]` present).
///
/// The wire carries one aliased field per requested column, so the response is
/// already column-shaped — there is no key to match and nothing to pivot.
pub(crate) fn expect_columnar_property_list(
    v: Option<Prop>,
    context: &str,
) -> Result<Vec<Vec<Option<Prop>>>, ClientError> {
    match v {
        Some(Prop::List(columns)) => columns
            .iter()
            .map(|column| cast_optional_wrapper_list(Some(column), Some, context))
            .collect(),
        _ => Err(unexpected(context)),
    }
}

/// Nested variant: a `Prop::List` of columns, each a `Prop::List` of sources,
/// each a `Prop::List` of per-member optionals.
pub(crate) fn expect_nested_columnar_property_list(
    v: Option<Prop>,
    context: &str,
) -> Result<Vec<Vec<Vec<Option<Prop>>>>, ClientError> {
    match v {
        Some(Prop::List(columns)) => columns
            .iter()
            .map(|column| match column {
                Prop::List(sources) => sources
                    .iter()
                    .map(|source| cast_optional_wrapper_list(Some(source), Some, context))
                    .collect(),
                _ => Err(unexpected(context)),
            })
            .collect(),
        _ => Err(unexpected(context)),
    }
}

fn extract_key_value_pair(map: &PropMap, context: &str) -> Result<(String, Prop), ClientError> {
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
) -> Result<Option<(EventTime, Prop)>, ClientError> {
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
) -> Result<Vec<(EventTime, Prop)>, ClientError> {
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

fn extract_property_tuple(map: &PropMap, context: &str) -> Result<(EventTime, Prop), ClientError> {
    let time = match map.get("time") {
        Some(Prop::Map(time_map)) => extract_event_time(&*time_map).ok_or_else(|| {
            ClientError::InvalidResponse(format!("`{}` tuple `time` has no timestamp", context))
        })?,
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

/// Decode a wire `{timestamp, eventId}` record into an [`EventTime`] — the same
/// type the local API exposes. `None` when there is no timestamp: the server's
/// representation of "no event time" (e.g. `earliest_time` on an empty view),
/// which the local API models as an absent value. A missing `event_id`
/// defaults to `0`; the server only omits it alongside the timestamp.
///
/// The datetime is *not* read from the wire — `EventTime::dt()` derives it from
/// the timestamp locally, so the server never renders one.
fn extract_event_time(map: &PropMap) -> Option<EventTime> {
    let timestamp = match map.get("timestamp") {
        Some(Prop::I64(n)) => *n,
        _ => return None,
    };
    let event_id = match map.get("eventId") {
        Some(Prop::I64(n)) => *n as usize,
        _ => 0,
    };
    Some(EventTime::new(timestamp, event_id))
}

/// Unwrap a `Transport::execute` result expecting a `Prop::List` of
/// arbitrary polymorphic `Prop`s. Used by `TemporalPropertyValueList`.
pub(crate) fn expect_prop_list(v: Option<Prop>, context: &str) -> Result<Vec<Prop>, ClientError> {
    cast_list(v, Some, context)
}

/// Unwrap a `Transport::execute` result expecting a nullable EventTime
/// terminal (`earliest_time`, `latest_time`, `start`, `end`, `time`). The
/// transport returns `Some(Prop::Map({timestamp, datetime, eventId}))` for a
/// present value, or `None` (JSON null) for an absent one (e.g. empty graph).
pub(crate) fn expect_optional_event_time(
    v: Option<Prop>,
    context: &str,
) -> Result<Option<EventTime>, ClientError> {
    match v {
        None => Ok(None),
        Some(Prop::Map(map)) => Ok(extract_event_time(&map)),
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
    cast_optional(v, PropUnwrap::into_f64, context)
}

/// Unwrap a `Transport::execute` result expecting a `HistoryList` /
/// `HistoryListRev` terminal — a `Prop::List` of `Prop::Map` records where
/// each map may contain `timestamp` (i64), `dt` (String), and `eventId`
/// (i64). Missing keys decode to `None` on the corresponding field.
pub(crate) fn expect_event_time_list(
    v: Option<Prop>,
    context: &str,
) -> Result<Vec<EventTime>, ClientError> {
    match v {
        Some(Prop::List(items)) => items
            .iter()
            .map(|p| match p {
                // Every entry in a history list carries a timestamp; one
                // without is a malformed response, surfaced rather than
                // silently dropped from the list.
                Prop::Map(map) => extract_event_time(&map).ok_or_else(|| {
                    ClientError::InvalidResponse(format!("`{}` element has no timestamp", context))
                }),
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
/// `Prop::List` of 2-element `Prop::List([src, dst])` typed-id pairs.
pub(crate) fn expect_edge_list(
    v: Option<Prop>,
    context: &str,
) -> Result<Vec<(GID, GID)>, ClientError> {
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
                    let src = into_gid(src).ok_or_else(|| {
                        ClientError::InvalidResponse(format!("`{}` src not a node id", context))
                    })?;
                    let dst = into_gid(dst).ok_or_else(|| {
                        ClientError::InvalidResponse(format!("`{}` dst not a node id", context))
                    })?;
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
pub(crate) type ExplodedEdgeRecord = (GID, GID, i64, i64, String);

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
    let gid_at = |idx: usize, what: &str| {
        into_gid(items[idx].clone()).ok_or_else(|| {
            ClientError::InvalidResponse(format!("`{}` {} not a node id", context, what))
        })
    };
    Ok((
        gid_at(0, "src")?,
        gid_at(1, "dst")?,
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
fn layers_edge_record(p: &Prop, context: &str) -> Result<(GID, GID, String), ClientError> {
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
    let gid_at = |idx: usize, what: &str| {
        into_gid(items[idx].clone()).ok_or_else(|| {
            ClientError::InvalidResponse(format!("`{}` {} not a node id", context, what))
        })
    };
    Ok((gid_at(0, "src")?, gid_at(1, "dst")?, str_at(2, "layer")?))
}

/// Unwrap a `Transport::execute` result for `ExplodedLayersEdgesList` — a
/// `Prop::List` of `[src, dst, layer]` inner lists (no time). Used by
/// `.collect()` on a layer-exploded `Edges` collection.
pub(crate) fn expect_exploded_layers_edge_list(
    v: Option<Prop>,
    context: &str,
) -> Result<Vec<(GID, GID, String)>, ClientError> {
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
) -> Result<Vec<Vec<(GID, GID, String)>>, ClientError> {
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
) -> Result<Vec<Vec<(GID, GID)>>, ClientError> {
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
                            let src = into_gid(src).ok_or_else(|| {
                                ClientError::InvalidResponse(format!(
                                    "`{}` src not a node id",
                                    context
                                ))
                            })?;
                            let dst = into_gid(dst).ok_or_else(|| {
                                ClientError::InvalidResponse(format!(
                                    "`{}` dst not a node id",
                                    context
                                ))
                            })?;
                            Ok((src, dst))
                        }
                        _ => Err(ClientError::InvalidResponse(format!(
                            "`{}` element not a pair",
                            context
                        ))),
                    })
                    .collect::<Result<Vec<(GID, GID)>, ClientError>>(),
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
    cast_optional_wrapper_list(v, into_string, context)
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
            .map(|row| expect_optional_string_list(Some(row), context))
            .collect(),
        _ => Err(unexpected(context)),
    }
}

/// Unwrap a columnar accessor producing `Vec<Option<EventTime>>` — a flat
/// `Prop::List` where each element is a `Prop::List` of 0 (`None`) or 1
/// (`Some`) `Prop::Map`. Used by `Edges.earliest_time` / `latest_time` / `time`.
pub(crate) fn expect_optional_event_time_list(
    v: Option<Prop>,
    context: &str,
) -> Result<Vec<Option<EventTime>>, ClientError> {
    match v {
        Some(Prop::List(items)) => items
            .iter()
            .map(|elem| match elem {
                Prop::List(inner) => match inner.iter().next() {
                    None => Ok(None),
                    Some(Prop::Map(map)) => Ok(extract_event_time(&map)),
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
/// `Vec<Vec<Option<EventTime>>>`. Used by `NestedEdges.earliest_time` etc.
pub(crate) fn expect_nested_optional_event_time_list(
    v: Option<Prop>,
    context: &str,
) -> Result<Vec<Vec<Option<EventTime>>>, ClientError> {
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
            .map(|row| cast_nested_list(Some(row), into_string, context))
            .collect(),
        _ => Err(unexpected(context)),
    }
}
