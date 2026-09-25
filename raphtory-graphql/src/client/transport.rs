//! Wire abstraction for talking to a remote graph server.
//!
//! V1 implementation is `GraphqlTransport`, which renders ops as GraphQL
//! queries against the existing GraphQL server. Future transports (e.g. a
//! gRPC-based one) can be swapped in by implementing this trait — client
//! wrappers won't change.

use crate::client::{
    graphql_transport::json_to_prop_typed, op::Op, ClientError, RemotePropertyTuple,
};
use async_graphql::async_trait;
use raphtory_api::core::{
    entities::{
        properties::prop::{Prop, PropType},
        GID,
    },
    storage::timeindex::{AsTime, EventTime},
};
use serde::de::DeserializeOwned;
use serde_json::Value as JsonValue;
use std::fmt::Display;

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
    async fn execute(&self, op: &Op) -> Result<JsonValue, ClientError>;
}

// ============ Result decoding ============
//
// The decode half of the `Transport` contract: every `expect_*` function
// unwraps one documented json result shape (see the `ReadExpr`
// terminal docs for which terminal produces which shape). Wrappers call
// these to turn transport results into typed values; a second transport
// implementation must produce exactly the shapes these functions accept.

// The uniform decoders below are one-line casts over these combinators: a
// `PropUnwrap` accessor (`into_i64`, `into_bool`, …) names the expected shape,
// and the combinator handles scalar / nullable / list / nested-list plumbing.

/// The shared "shape mismatch" decode error.
fn unexpected_err(context: &str, err: impl Display) -> ClientError {
    ClientError::InvalidResponse(format!("`{context}` returned unexpected value type: {err}"))
}

/// The shared "shape mismatch" decode error.
fn unexpected_value(context: &str, value: &JsonValue) -> ClientError {
    ClientError::InvalidResponse(format!("`{context}` returned unexpected value: {value}"))
}

/// Cast a required scalar.
pub(crate) fn expect_typed<T: DeserializeOwned>(
    v: JsonValue,
    context: &str,
) -> Result<T, ClientError> {
    T::deserialize(v).map_err(|err| unexpected_err(context, err))
}

// ============ Prop-shaped record decoding ============
//
// The structured terminals (e.g. `schema`) arrive as one `Prop` tree; these
// unwrap its pieces with a context for the error, mirroring the `expect_*`
// family above, which does the same for whole `Transport::execute` results.

fn map_optional<R>(
    result: JsonValue,
    map: impl FnOnce(JsonValue) -> Result<R, ClientError>,
) -> Result<Option<R>, ClientError> {
    match result {
        JsonValue::Null => Ok(None),
        v => Ok(Some(map(v)?)),
    }
}

/// Unwrap a `List` field of a decoded record.
pub(crate) fn expect_list(result: JsonValue, context: &str) -> Result<Vec<JsonValue>, ClientError> {
    match result {
        JsonValue::Array(items) => Ok(items),
        _ => Err(ClientError::InvalidResponse(format!(
            "`{}` expected list",
            context
        ))),
    }
}

/// Look up a required key in a decoded `Prop::Map` record.
pub(crate) fn extract_element(
    map: &mut serde_json::value::Map<String, JsonValue>,
    key: &str,
) -> Result<JsonValue, ClientError> {
    map.remove(key)
        .ok_or_else(|| ClientError::InvalidResponse(format!("record missing `{}`", key)))
}

/// Expect an optional value (helps with type inference)
pub(crate) fn expect_optional_typed<V: DeserializeOwned>(
    v: JsonValue,
    context: &str,
) -> Result<Option<V>, ClientError> {
    expect_typed(v, context)
}

/// Unwrap a `Transport::execute` result expecting a nullable `str`
/// scalar. `Ok(None)` means the server returned JSON null (e.g. `node_type`
/// when the type isn't set).
pub(crate) fn expect_optional_string(
    v: JsonValue,
    context: &str,
) -> Result<Option<String>, ClientError> {
    expect_typed(v, context)
}

/// Unwrap a `Transport::execute` result expecting a list of strings (e.g. the result of `.ids()` on a collection).
pub(crate) fn expect_string_list(v: JsonValue, context: &str) -> Result<Vec<String>, ClientError> {
    expect_typed(v, context)
}

/// Unwrap a `Transport::execute` result expecting a `Prop::List` of node ids
/// (e.g. the result of `.ids()` on a collection) — typed, not stringified.
pub(crate) fn expect_gid_list(v: JsonValue, context: &str) -> Result<Vec<GID>, ClientError> {
    let records = expect_list(v, context)?;
    records
        .into_iter()
        .map(|v| expect_gid(into_element(v, "id", context)?, context))
        .collect()
}

/// Expect an edge id result
pub(crate) fn expect_edge_id(v: JsonValue, context: &str) -> Result<(GID, GID), ClientError> {
    let res = expect_list(v, context)?;
    let mut values = res.into_iter().map(|v| expect_gid(v, context));
    let src = values
        .next()
        .ok_or_else(|| unexpected_err(context, "expected src id"))??;
    let dst = values
        .next()
        .ok_or_else(|| unexpected_err(context, "expected dst id"))??;
    if values.next().is_some() {
        return Err(unexpected_err(context, "too many values for edge id"));
    }
    Ok((src, dst))
}

/// Nested variant of `expect_gid_list` — e.g. `.ids()` on a `PathFromGraph`
/// collection, where each inner list holds the neighbours of one source node.
pub(crate) fn expect_nested_gid_list(
    v: JsonValue,
    context: &str,
) -> Result<Vec<Vec<GID>>, ClientError> {
    let v = expect_list(v, context)?;
    v.into_iter()
        .map(|v| expect_gid_list(into_element(v, "list", context)?, context))
        .collect()
}

/// Unwrap a `Transport::execute` result expecting a `Prop::List` of
/// `Prop::I64`s. Used by sub-container list/page terminals when the parent
/// is `Timestamps`, `EventIds`, or `Intervals`.
pub(crate) fn expect_i64_list(v: JsonValue, context: &str) -> Result<Vec<i64>, ClientError> {
    expect_typed(v, context)
}

pub(crate) fn expect_typed_list<V: DeserializeOwned>(
    v: JsonValue,
    context: &str,
) -> Result<Vec<V>, ClientError> {
    let results = expect_list(v, context)?;
    results
        .into_iter()
        .map(|v| expect_typed(v, context))
        .collect()
}

pub(crate) fn expect_tagged_typed_list<V: DeserializeOwned>(
    v: JsonValue,
    context: &str,
) -> Result<Vec<V>, ClientError> {
    let results = expect_list(v, context)?;
    results
        .into_iter()
        .map(|v| expect_typed(into_element(v, context, context)?, context))
        .collect()
}

/// Unwrap a `Transport::execute` result expecting a `Prop::List` of
/// `Prop::List` of `Prop::I64` (a nested list of integers) — e.g. the result
/// of `.degree()` on a `PathFromGraph` collection, where each inner list holds
/// the per-node degrees of one source node's neighbours.
pub(crate) fn expect_tagged_nested_typed_list<V: DeserializeOwned>(
    v: JsonValue,
    context: &str,
) -> Result<Vec<Vec<V>>, ClientError> {
    let results = expect_list(v, context)?;
    results
        .into_iter()
        .map(|v| expect_typed_list(into_element(v, context, context)?, context))
        .collect()
}

pub(crate) fn expect_tagged_nested_tagged_typed_list<V: DeserializeOwned>(
    v: JsonValue,
    outer_context: &str,
    inner_context: &str,
) -> Result<Vec<Vec<V>>, ClientError> {
    let results = expect_list(v, outer_context)?;
    results
        .into_iter()
        .map(|v| {
            expect_tagged_typed_list(
                into_element(v, outer_context, outer_context)?,
                inner_context,
            )
        })
        .collect()
}

pub(crate) fn expect_nested_typed_list<V: DeserializeOwned>(
    v: JsonValue,
    context: &str,
) -> Result<Vec<Vec<V>>, ClientError> {
    let results = expect_list(v, context)?;
    results
        .into_iter()
        .map(|v| expect_typed_list(v, context))
        .collect()
}

/// Unwrap a `Transport::execute` result expecting a `Prop::List` of
/// `Prop::Map({key, value})` records — used by `PropertyValues`.
pub(crate) fn expect_property_list(
    v: JsonValue,
    context: &str,
) -> Result<Vec<(String, Prop)>, ClientError> {
    let result = expect_list(v, context)?;
    result
        .into_iter()
        .map(|v| expect_prop_key_value_pair(v, context))
        .collect()
}

/// Unwrap a columnar property/metadata fetch: a `Prop::List` of columns, each
/// a `Prop::List` of per-member optionals (`[]` absent, `[v]` present).
///
/// The wire carries one aliased field per requested column, so the response is
/// already column-shaped — there is no key to match and nothing to pivot.
pub(crate) fn expect_columnar_property_list(
    v: JsonValue,
    num_cols: usize,
    context: &str,
) -> Result<Vec<Vec<Option<Prop>>>, ClientError> {
    let result = expect_list(v, context)?;
    let mut columns: Vec<_> = (0..num_cols)
        .map(|_| Vec::with_capacity(result.len()))
        .collect();
    for row in result {
        let properties = expect_map(into_element(row, context, context)?, context)?;
        for (v, column) in properties.into_values().zip(columns.iter_mut()) {
            let prop = expect_optional_prop(v, "value")?;
            column.push(prop);
        }
    }
    Ok(columns)
}

/// Nested variant: a `Prop::List` of columns, each a `Prop::List` of sources,
/// each a `Prop::List` of per-member optionals.
pub(crate) fn expect_nested_columnar_property_list(
    v: JsonValue,
    num_cols: usize,
    context: &str,
) -> Result<Vec<Vec<Vec<Option<Prop>>>>, ClientError> {
    let result = expect_list(v, context)?;
    let mut columns: Vec<_> = (0..num_cols)
        .map(|_| (0..result.len()).map(|_| Vec::new()).collect::<Vec<_>>())
        .collect();
    for (i, outer_row) in result.into_iter().enumerate() {
        let inner_row = expect_list(into_element(outer_row, "list", context)?, context)?;
        for row in inner_row {
            let properties = expect_map(into_element(row, context, context)?, context)?;
            for (v, column) in properties.into_values().zip(columns.iter_mut()) {
                let prop = expect_optional_prop(v, "value")?;
                column[i].push(prop);
            }
        }
    }
    Ok(columns)
}

fn expect_prop_key_value_pair(
    map: JsonValue,
    context: &str,
) -> Result<(String, Prop), ClientError> {
    let mut map = expect_map(map, context)?;
    let dtype = expect_prop_type(extract_element(&mut map, "dtype")?, context)?;
    let name = expect_typed(extract_element(&mut map, "key")?, context)?;
    let value = extract_element(&mut map, "value")?;
    let prop = json_to_prop_typed(&dtype, value)?;
    Ok((name, prop))
}

/// Decode a `{value, dtype?}` record's value, type-directed when the server
/// sent a `dtype` sibling (older servers may not).
pub(crate) fn expect_prop(obj: JsonValue, context: &str) -> Result<Prop, ClientError> {
    let mut map = expect_map(obj, context)?;
    let dtype = match extract_element(&mut map, "dtype") {
        Ok(v) => expect_prop_type(v, context)?,
        Err(_) => PropType::Empty, // untyped conversion
    };
    let value = extract_element(&mut map, context)?;
    json_to_prop_typed(&dtype, value)
}

pub(crate) fn expect_prop_type(v: JsonValue, context: &str) -> Result<PropType, ClientError> {
    expect_typed(v, context)
}

pub(crate) fn expect_optional_prop_type(
    v: JsonValue,
    context: &str,
) -> Result<Option<PropType>, ClientError> {
    map_optional(v, |v| {
        expect_prop_type(into_element(v, "dtype", context)?, context)
    })
}

/// Unwrap a `Transport::execute` result expecting a nullable polymorphic
/// `Prop` scalar. Used by TemporalProperty terminals like `at` / `latest`
/// that return an arbitrary property value or null.
pub(crate) fn expect_optional_prop(
    v: JsonValue,
    context: &str,
) -> Result<Option<Prop>, ClientError> {
    match v {
        JsonValue::Null => Ok(None),
        JsonValue::Object(mut map) => match extract_element(&mut map, context) {
            Ok(v) => {
                if v.is_null() {
                    return Ok(None);
                }
                let dtype = match extract_element(&mut map, "dtype") {
                    Ok(v) => expect_prop_type(v, context)?,
                    Err(_) => PropType::Empty, // untyped conversion
                };
                Ok(Some(json_to_prop_typed(&dtype, v)?))
            }
            Err(_) => Ok(None),
        },
        v => Err(unexpected_value(context, &v)),
    }
}

/// Unwrap a `Transport::execute` result expecting a nullable property tuple
/// (a `Prop::Map` with `time` and `value` keys). Used by TemporalProperty
/// stats returning an optional `(time, value)` pair.
pub(crate) fn expect_optional_property_tuple(
    v: JsonValue,
    context: &str,
) -> Result<Option<(EventTime, Prop)>, ClientError> {
    let mut map = expect_map(v, context)?;
    let dtype = expect_prop_type(extract_element(&mut map, "dtype")?, context)?;
    map_optional(extract_element(&mut map, context)?, |v| {
        expect_property_tuple(v, &dtype, context)
    })
}

/// Unwrap a list of property tuples (used by `orderedDedupe`).
pub(crate) fn expect_property_tuple_list(
    v: JsonValue,
    context: &str,
) -> Result<Vec<RemotePropertyTuple>, ClientError> {
    let mut map = expect_map(v, context)?;
    let dtype = expect_prop_type(extract_element(&mut map, "dtype")?, context)?;
    let results = expect_list(extract_element(&mut map, context)?, context)?;
    results
        .into_iter()
        .map(|v| {
            expect_property_tuple(v, &dtype, "value")
                .map(|(time, value)| RemotePropertyTuple { time, value })
        })
        .collect()
}

fn expect_property_tuple(
    map: JsonValue,
    dtype: &PropType,
    context: &str,
) -> Result<(EventTime, Prop), ClientError> {
    let mut map = expect_map(map, context)?;
    let time = expect_event_time(extract_element(&mut map, "time")?, context)?;
    let value = json_to_prop_typed(dtype, extract_element(&mut map, "value")?)?;
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
fn expect_event_time(map: JsonValue, context: &str) -> Result<EventTime, ClientError> {
    let mut map = expect_map(map, context)?;
    let timestamp = expect_typed(extract_element(&mut map, "timestamp")?, context)?;
    let event_id = expect_typed(extract_element(&mut map, "eventId")?, context)?;
    Ok(EventTime::new(timestamp, event_id))
}

///
pub(crate) fn expect_typed_prop_list(
    v: JsonValue,
    context: &str,
) -> Result<Vec<Prop>, ClientError> {
    let mut map = expect_map(v, context)?;
    let dtype = expect_prop_type(extract_element(&mut map, "dtype")?, context)?;
    let values = expect_list(extract_element(&mut map, context)?, context)?;
    values
        .into_iter()
        .map(|v| json_to_prop_typed(&dtype, v))
        .collect()
}

/// Unwrap a `Transport::execute` result expecting a `Prop::List` of
/// arbitrary polymorphic `Prop`s.
pub(crate) fn expect_prop_list(v: JsonValue, context: &str) -> Result<Vec<Prop>, ClientError> {
    let result = expect_list(v, context)?;
    result
        .into_iter()
        .map(|v| expect_prop(v, "value"))
        .collect()
}

/// Unwrap a `Transport::execute` result expecting a nullable EventTime
/// terminal (`earliest_time`, `latest_time`, `start`, `end`, `time`). The
/// transport returns `Some(Prop::Map({timestamp, datetime, eventId}))` for a
/// present value, or `None` (JSON null) for an absent one (e.g. empty graph).
pub(crate) fn expect_optional_event_time(
    v: JsonValue,
    context: &str,
) -> Result<Option<EventTime>, ClientError> {
    let mut map = expect_map(v, context)?;
    let timestamp = match expect_optional_typed(extract_element(&mut map, "timestamp")?, context)? {
        None => return Ok(None),
        Some(v) => v,
    };

    let event_id = expect_typed(extract_element(&mut map, "eventId")?, context)?;
    Ok(Some(EventTime::new(timestamp, event_id)))
}

/// Unwrap a `Transport::execute` result expecting a `HistoryList` /
/// `HistoryListRev` terminal — a `Prop::List` of `Prop::Map` records where
/// each map may contain `timestamp` (i64), `dt` (String), and `eventId`
/// (i64). Missing keys decode to `None` on the corresponding field.
pub(crate) fn expect_event_time_list(
    v: JsonValue,
    context: &str,
) -> Result<Vec<EventTime>, ClientError> {
    match v {
        JsonValue::Array(items) => items
            .into_iter()
            .map(|p| expect_event_time(p, context))
            .collect(),
        _ => Err(ClientError::InvalidResponse(format!(
            "`{}` returned unexpected value type",
            context
        ))),
    }
}

pub(crate) fn expect_edge_record(v: JsonValue, context: &str) -> Result<(GID, GID), ClientError> {
    let mut map = expect_map(v, context)?;
    let src = expect_gid(
        into_element(extract_element(&mut map, "src")?, "id", context)?,
        context,
    )?;
    let dst = expect_gid(
        into_element(extract_element(&mut map, "dst")?, "id", context)?,
        context,
    )?;
    Ok((src, dst))
}

/// Unwrap a `Transport::execute` result expecting an EdgesList terminal — a
/// `Prop::List` of 2-element `Prop::List([src, dst])` typed-id pairs.
pub(crate) fn expect_edge_list(
    v: JsonValue,
    context: &str,
) -> Result<Vec<(GID, GID)>, ClientError> {
    let records = expect_list(v, context)?;
    records
        .into_iter()
        .map(|v| expect_edge_record(v, context))
        .collect()
}

/// One member of an exploded-edge fetch: `(src, dst, time, event_id,
/// layer_name)` — everything needed to pin a handle to the event.
pub(crate) type ExplodedEdgeRecord = (GID, GID, i64, i64, String);

fn expect_map(
    v: JsonValue,
    context: &str,
) -> Result<serde_json::value::Map<String, JsonValue>, ClientError> {
    match v {
        JsonValue::Object(map) => Ok(map),
        v => Err(unexpected_value(context, &v)),
    }
}

fn into_element(v: JsonValue, key: &str, context: &str) -> Result<JsonValue, ClientError> {
    let mut value = expect_map(v, context)?;
    extract_element(&mut value, key)
}

pub(crate) fn expect_gid(v: JsonValue, context: &str) -> Result<GID, ClientError> {
    let gid = match v {
        JsonValue::Number(number) => GID::U64(
            number
                .as_u64()
                .ok_or_else(|| unexpected_err(context, "invalid numeric id"))?,
        ),
        JsonValue::String(v) => GID::Str(v),
        v => Err(unexpected_value(context, &v))?,
    };
    Ok(gid)
}

fn expect_exploded_edge_record(
    v: JsonValue,
    context: &str,
) -> Result<ExplodedEdgeRecord, ClientError> {
    let mut record = expect_map(v, context)?;
    let src = expect_gid(
        into_element(extract_element(&mut record, "src")?, "id", context)?,
        context,
    )?;
    let dst = expect_gid(
        into_element(extract_element(&mut record, "dst")?, "id", context)?,
        context,
    )?;
    let time = expect_event_time(extract_element(&mut record, "time")?, context)?;
    let layer_name = expect_typed(extract_element(&mut record, "layerName")?, context)?;
    Ok((src, dst, time.t(), time.i() as i64, layer_name))
}

/// Unwrap a `Transport::execute` result expecting a `Prop::List` of exploded
/// edge records. Used by `.collect()` on an exploded `Edges` collection.
pub(crate) fn expect_exploded_edge_list(
    v: JsonValue,
    context: &str,
) -> Result<Vec<ExplodedEdgeRecord>, ClientError> {
    expect_list(v, context)?
        .into_iter()
        .map(|v| expect_exploded_edge_record(v, context))
        .collect()
}

fn expect_layered_edge_record(
    v: JsonValue,
    context: &str,
) -> Result<(GID, GID, String), ClientError> {
    let mut record = expect_map(v, context)?;
    let src = expect_gid(
        into_element(extract_element(&mut record, "src")?, "id", context)?,
        context,
    )?;
    let dst = expect_gid(
        into_element(extract_element(&mut record, "dst")?, "id", context)?,
        context,
    )?;
    let layer_name = expect_typed(extract_element(&mut record, "layerName")?, context)?;
    Ok((src, dst, layer_name))
}

/// Unwrap a `Transport::execute` result for `ExplodedLayersEdgesList` — a
/// `Prop::List` of `[src, dst, layer]` inner lists (no time). Used by
/// `.collect()` on a layer-exploded `Edges` collection.
pub(crate) fn expect_exploded_layers_edge_list(
    v: JsonValue,
    context: &str,
) -> Result<Vec<(GID, GID, String)>, ClientError> {
    let records = expect_list(v, context)?;
    records
        .into_iter()
        .map(|v| expect_layered_edge_record(v, context))
        .collect()
}

/// Nested variant — one inner list of `[src, dst, layer]` records per source
/// node. Used by `.collect()` on a layer-exploded `NestedEdges` collection.
pub(crate) fn expect_nested_exploded_layers_edge_list(
    v: JsonValue,
    context: &str,
) -> Result<Vec<Vec<(GID, GID, String)>>, ClientError> {
    let records = expect_list(v, context)?;
    records
        .into_iter()
        .map(|v| expect_exploded_layers_edge_list(into_element(v, context, context)?, context))
        .collect()
}

/// Nested variant of `expect_exploded_edge_list` — one inner list per source
/// node. Used by `.collect()` on an exploded `NestedEdges` collection.
pub(crate) fn expect_nested_exploded_edge_list(
    v: JsonValue,
    context: &str,
) -> Result<Vec<Vec<ExplodedEdgeRecord>>, ClientError> {
    let records = expect_list(v, context)?;
    records
        .into_iter()
        .map(|v| expect_exploded_edge_list(into_element(v, context, context)?, context))
        .collect()
}

/// Unwrap a `Transport::execute` result expecting a `Prop::List` of
/// `Prop::List` of `(src, dst)` pairs — a nested list of edge endpoints, one
/// inner list per source node. Used by `.collect()` on a `NestedEdges`
/// collection. Mirrors `expect_edge_list`, one level deeper.
pub(crate) fn expect_nested_edge_list(
    v: JsonValue,
    context: &str,
) -> Result<Vec<Vec<(GID, GID)>>, ClientError> {
    let records = expect_list(v, context)?;
    records
        .into_iter()
        .map(|v| expect_edge_list(into_element(v, context, context)?, context))
        .collect()
}

/// Unwrap a columnar accessor producing `Vec<Option<String>>` — a flat
/// `Prop::List` where each element is a `Prop::List` of 0 (`None`) or 1
/// (`Some`) `Prop::Str`. Used by `Nodes.node_type` / `PathFromNode.node_type`.
pub(crate) fn expect_node_type_list(
    v: JsonValue,
    context: &str,
) -> Result<Vec<Option<String>>, ClientError> {
    let records = expect_list(v, context)?;
    records
        .into_iter()
        .map(|v| expect_optional_string(into_element(v, context, context)?, context))
        .collect()
}

/// Unwrap a columnar accessor producing `Vec<Option<EventTime>>` — a flat
/// `Prop::List` where each element is a `Prop::List` of 0 (`None`) or 1
/// (`Some`) `Prop::Map`. Used by `Edges.earliest_time` / `latest_time` / `time`.
pub(crate) fn expect_optional_event_time_list(
    v: JsonValue,
    inner_key: &str,
    context: &str,
) -> Result<Vec<Option<EventTime>>, ClientError> {
    let records = expect_list(v, context)?;
    records
        .into_iter()
        .map(|elem| expect_optional_event_time(into_element(elem, inner_key, context)?, context))
        .collect()
}

/// Nested form of `expect_optional_event_time_list` →
/// `Vec<Vec<Option<EventTime>>>`. Used by `NestedEdges.earliest_time` etc.
pub(crate) fn expect_nested_optional_event_time_list(
    v: JsonValue,
    inner_key: &str,
    context: &str,
) -> Result<Vec<Vec<Option<EventTime>>>, ClientError> {
    let records = expect_list(v, context)?;
    records
        .into_iter()
        .map(|v| {
            expect_optional_event_time_list(into_element(v, "list", context)?, inner_key, context)
        })
        .collect()
}
