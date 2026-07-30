use crate::{
    client::{
        graphql_transport::GraphqlTransport,
        op::{
            AddEdge as AddEdgeOp, AddGraphMetadata as AddGraphMetadataOp,
            AddGraphProperty as AddGraphPropertyOp, AddNode as AddNodeOp,
            CreateNode as CreateNodeOp, DeleteEdge as DeleteEdgeOp, HandleCtx, InputTime, Op,
            ReadExpr, UpdateGraphMetadata as UpdateGraphMetadataOp, WriteOp,
        },
        remote_client::RemoteClient,
        remote_edge::RemoteEdge,
        remote_edges::RemoteEdges,
        remote_history::RemoteEventTime,
        remote_metadata::{RemoteMetadata, RemoteProperties},
        remote_node::RemoteNode,
        remote_nodes::RemoteNodes,
        remote_schema::RemoteGraphSchema,
        transport::Transport,
        ClientError,
    },
    model::graph::filtering::{GqlEdgeFilter, GqlNodeFilter},
};
use raphtory_api::core::{
    entities::{properties::prop::Prop, GID},
    storage::timeindex::{AsTime, EventTime},
    utils::time::IntoTime,
};
use std::{collections::HashMap, sync::Arc};

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

/// Unwrap a `Transport::execute` result expecting a `Prop::Map({key, value})`
/// wrapped in `Option` — used by `PropertyGet`. Returns `None` if the key
/// wasn't present in the container.
pub(crate) fn expect_optional_property(
    v: Option<Prop>,
    context: &str,
) -> Result<Option<(String, Prop)>, ClientError> {
    match v {
        None => Ok(None),
        Some(Prop::Map(map)) => extract_key_value_pair(&*map, context).map(Some),
        Some(_) => Err(ClientError::InvalidResponse(format!(
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
) -> Result<Option<(crate::client::remote_history::RemoteEventTime, Prop)>, ClientError> {
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
) -> Result<Vec<(crate::client::remote_history::RemoteEventTime, Prop)>, ClientError> {
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
) -> Result<(crate::client::remote_history::RemoteEventTime, Prop), ClientError> {
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
) -> crate::client::remote_history::RemoteEventTime {
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
    crate::client::remote_history::RemoteEventTime {
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

/// A handle to a remote graph on the server.
///
/// Holds an accumulating `ReadExpr` for view construction. View-op methods
/// (`.window()`, `.layer()`, `.at()`, ...) append lazily — no RPC. Selection
/// methods `.node()` and `.edge()` fire one RPC each (`hasNode` / `hasEdge`
/// against the current view chain) to validate that the id resolves, raising
/// `ClientError::NotFound` if not. Terminals on the child types (e.g.
/// `RemoteNode::degree`) fire their own RPC evaluating the accumulated
/// expression via the transport.
#[derive(Clone)]
pub struct RemoteGraph {
    pub path: String,
    pub transport: Arc<dyn Transport>,
    /// The read expression built so far. Starts as `Root { path }`.
    pub expr: ReadExpr,
}

impl RemoteGraph {
    /// Construct a `RemoteGraph` handle for the graph at `path`, using the
    /// given `RemoteClient` for transport. Wraps the client in a
    /// `GraphqlTransport`; starts the accumulated read expression at
    /// `Root { path }`.
    pub fn new(path: String, client: RemoteClient) -> Self {
        let transport: Arc<dyn Transport> = Arc::new(GraphqlTransport::new(client));
        let expr = ReadExpr::Root { path: path.clone() };
        Self {
            path,
            transport,
            expr,
        }
    }

    /// Time-window the graph. Lazy — builds up the read expression, no RPC.
    pub fn window(&self, start: InputTime, end: InputTime) -> RemoteGraph {
        self.with_expr(ReadExpr::Window {
            input: Arc::new(self.expr.clone()),
            start,
            end,
        })
    }

    /// Restrict to a single named layer. Lazy — no RPC.
    pub fn layer(&self, name: impl ToString) -> RemoteGraph {
        self.with_expr(ReadExpr::Layer {
            input: Arc::new(self.expr.clone()),
            name: name.to_string(),
        })
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    pub fn at(&self, time: InputTime) -> RemoteGraph {
        self.with_expr(ReadExpr::At {
            input: Arc::new(self.expr.clone()),
            time,
        })
    }

    /// Restrict to events strictly before the given time. Lazy — no RPC.
    pub fn before(&self, time: InputTime) -> RemoteGraph {
        self.with_expr(ReadExpr::Before {
            input: Arc::new(self.expr.clone()),
            time,
        })
    }

    /// Restrict to events strictly after the given time (exclusive). Lazy — no RPC.
    pub fn after(&self, time: InputTime) -> RemoteGraph {
        self.with_expr(ReadExpr::After {
            input: Arc::new(self.expr.clone()),
            time,
        })
    }

    /// Restrict to the latest state — no args. Lazy — no RPC.
    pub fn latest(&self) -> RemoteGraph {
        self.with_expr(ReadExpr::Latest {
            input: Arc::new(self.expr.clone()),
        })
    }

    /// Snapshot at the latest time. Lazy — no RPC.
    pub fn snapshot_latest(&self) -> RemoteGraph {
        self.with_expr(ReadExpr::SnapshotLatest {
            input: Arc::new(self.expr.clone()),
        })
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    pub fn snapshot_at(&self, time: InputTime) -> RemoteGraph {
        self.with_expr(ReadExpr::SnapshotAt {
            input: Arc::new(self.expr.clone()),
            time,
        })
    }

    /// Exclude a specific layer from the view. Lazy — no RPC.
    pub fn exclude_layer(&self, name: impl ToString) -> RemoteGraph {
        self.with_expr(ReadExpr::ExcludeLayer {
            input: Arc::new(self.expr.clone()),
            name: name.to_string(),
        })
    }

    /// Shrink both start and end of the current window (intersection, never widens).
    /// Lazy — no RPC.
    pub fn shrink_window(&self, start: InputTime, end: InputTime) -> RemoteGraph {
        self.with_expr(ReadExpr::ShrinkWindow {
            input: Arc::new(self.expr.clone()),
            start,
            end,
        })
    }

    /// Shrink the start of the current window. Lazy — no RPC.
    pub fn shrink_start(&self, start: InputTime) -> RemoteGraph {
        self.with_expr(ReadExpr::ShrinkStart {
            input: Arc::new(self.expr.clone()),
            start,
        })
    }

    /// Shrink the end of the current window. Lazy — no RPC.
    pub fn shrink_end(&self, end: InputTime) -> RemoteGraph {
        self.with_expr(ReadExpr::ShrinkEnd {
            input: Arc::new(self.expr.clone()),
            end,
        })
    }

    /// Restrict to the "valid" subgraph (event-graph filter). Lazy — no RPC.
    pub fn valid(&self) -> RemoteGraph {
        self.with_expr(ReadExpr::Valid {
            input: Arc::new(self.expr.clone()),
        })
    }

    /// Restrict to the default layer. Lazy — no RPC.
    pub fn default_layer(&self) -> RemoteGraph {
        self.with_expr(ReadExpr::DefaultLayer {
            input: Arc::new(self.expr.clone()),
        })
    }

    /// Restrict to the given set of layers. Lazy — no RPC.
    pub fn layers(&self, names: Vec<String>) -> RemoteGraph {
        self.with_expr(ReadExpr::Layers {
            input: Arc::new(self.expr.clone()),
            names: names.into(),
        })
    }

    /// Exclude the given set of layers from the view. Lazy — no RPC.
    pub fn exclude_layers(&self, names: Vec<String>) -> RemoteGraph {
        self.with_expr(ReadExpr::ExcludeLayers {
            input: Arc::new(self.expr.clone()),
            names: names.into(),
        })
    }

    /// Restrict to the given set of valid layers. Lazy — no RPC.
    pub fn valid_layers(&self, names: Vec<String>) -> RemoteGraph {
        self.with_expr(ReadExpr::ValidLayers {
            input: Arc::new(self.expr.clone()),
            names: names.into(),
        })
    }

    /// Exclude a specific valid layer from the view. Lazy — no RPC.
    pub fn exclude_valid_layer(&self, name: impl ToString) -> RemoteGraph {
        self.with_expr(ReadExpr::ExcludeValidLayer {
            input: Arc::new(self.expr.clone()),
            name: name.to_string(),
        })
    }

    /// Exclude the given set of valid layers from the view. Lazy — no RPC.
    pub fn exclude_valid_layers(&self, names: Vec<String>) -> RemoteGraph {
        self.with_expr(ReadExpr::ExcludeValidLayers {
            input: Arc::new(self.expr.clone()),
            names: names.into(),
        })
    }

    /// Restrict to a subgraph induced by the given node ids. Lazy — no RPC.
    pub fn subgraph(&self, nodes: Vec<String>) -> RemoteGraph {
        self.with_expr(ReadExpr::Subgraph {
            input: Arc::new(self.expr.clone()),
            nodes: nodes.into(),
        })
    }

    /// Restrict to nodes matching one of the given node types. Lazy — no RPC.
    pub fn subgraph_node_types(&self, node_types: Vec<String>) -> RemoteGraph {
        self.with_expr(ReadExpr::SubgraphNodeTypes {
            input: Arc::new(self.expr.clone()),
            node_types: node_types.into(),
        })
    }

    /// Return a filtered graph view keeping nodes that match `filter`; edges
    /// survive only if both endpoints do. The node-filter half of the local
    /// `Graph.filter(FilterExpr)` API. Lazy — no RPC.
    pub fn filter_nodes(&self, filter: GqlNodeFilter) -> RemoteGraph {
        self.with_expr(ReadExpr::FilterGraphNodes {
            input: Arc::new(self.expr.clone()),
            filter: Arc::new(filter),
        })
    }

    /// Return a filtered graph view keeping edges that match `filter`; nodes
    /// remain even if all their edges drop. The edge-filter half of the local
    /// `Graph.filter(FilterExpr)` API. Lazy — no RPC.
    pub fn filter_edges(&self, filter: GqlEdgeFilter) -> RemoteGraph {
        self.with_expr(ReadExpr::FilterGraphEdges {
            input: Arc::new(self.expr.clone()),
            filter: Arc::new(filter),
        })
    }

    /// Exclude the given nodes from the view. Lazy — no RPC.
    pub fn exclude_nodes(&self, nodes: Vec<String>) -> RemoteGraph {
        self.with_expr(ReadExpr::ExcludeNodes {
            input: Arc::new(self.expr.clone()),
            nodes: nodes.into(),
        })
    }

    /// Terminal: count of nodes under the current view. Fires one RPC.
    pub async fn count_nodes(&self) -> Result<i64, ClientError> {
        let op = Op::Read(ReadExpr::CountNodes {
            input: Arc::new(self.expr.clone()),
        });
        expect_i64(self.transport.execute(&op).await?, "countNodes")
    }

    /// Terminal: count of edges under the current view. Fires one RPC.
    pub async fn count_edges(&self) -> Result<i64, ClientError> {
        let op = Op::Read(ReadExpr::CountEdges {
            input: Arc::new(self.expr.clone()),
        });
        expect_i64(self.transport.execute(&op).await?, "countEdges")
    }

    /// Terminal: earliest event timestamp under the current view. Returns
    /// `None` if the view has no events. Fires one RPC.
    pub async fn earliest_time(&self) -> Result<Option<RemoteEventTime>, ClientError> {
        let op = Op::Read(ReadExpr::EarliestTime {
            input: Arc::new(self.expr.clone()),
        });
        expect_optional_event_time(self.transport.execute(&op).await?, "earliestTime")
    }

    /// Terminal: latest event timestamp under the current view. Returns
    /// `None` if the view has no events. Fires one RPC.
    pub async fn latest_time(&self) -> Result<Option<RemoteEventTime>, ClientError> {
        let op = Op::Read(ReadExpr::LatestTime {
            input: Arc::new(self.expr.clone()),
        });
        expect_optional_event_time(self.transport.execute(&op).await?, "latestTime")
    }

    /// Terminal: view start bound. Returns `None` for an unbounded view.
    /// Fires one RPC.
    pub async fn start(&self) -> Result<Option<RemoteEventTime>, ClientError> {
        let op = Op::Read(ReadExpr::Start {
            input: Arc::new(self.expr.clone()),
        });
        expect_optional_event_time(self.transport.execute(&op).await?, "start")
    }

    /// Terminal: view end bound. Returns `None` for an unbounded view.
    /// Fires one RPC.
    pub async fn end(&self) -> Result<Option<RemoteEventTime>, ClientError> {
        let op = Op::Read(ReadExpr::End {
            input: Arc::new(self.expr.clone()),
        });
        expect_optional_event_time(self.transport.execute(&op).await?, "end")
    }

    /// Terminal: does the graph have a node with this id? Fires one RPC.
    pub async fn has_node(&self, id: impl ToString) -> Result<bool, ClientError> {
        let op = Op::Read(ReadExpr::HasNode {
            input: Arc::new(self.expr.clone()),
            id: id.to_string(),
        });
        expect_bool(self.transport.execute(&op).await?, "hasNode")
    }

    /// Terminal: does the graph have an edge `(src, dst)`? Fires one RPC.
    pub async fn has_edge(
        &self,
        src: impl ToString,
        dst: impl ToString,
    ) -> Result<bool, ClientError> {
        let op = Op::Read(ReadExpr::HasEdge {
            input: Arc::new(self.expr.clone()),
            src: src.to_string(),
            dst: dst.to_string(),
        });
        expect_bool(self.transport.execute(&op).await?, "hasEdge")
    }

    /// Terminal: total temporal-edge count (edge updates) under the current view.
    /// Fires one RPC.
    pub async fn count_temporal_edges(&self) -> Result<i64, ClientError> {
        let op = Op::Read(ReadExpr::CountTemporalEdges {
            input: Arc::new(self.expr.clone()),
        });
        expect_i64(self.transport.execute(&op).await?, "countTemporalEdges")
    }

    /// Terminal: graph name. Fires one RPC.
    pub async fn name(&self) -> Result<String, ClientError> {
        let op = Op::Read(ReadExpr::Name {
            input: Arc::new(self.expr.clone()),
        });
        expect_string(self.transport.execute(&op).await?, "name")
    }

    /// Terminal: graph path. Fires one RPC.
    pub async fn path(&self) -> Result<String, ClientError> {
        let op = Op::Read(ReadExpr::Path {
            input: Arc::new(self.expr.clone()),
        });
        expect_string(self.transport.execute(&op).await?, "path")
    }

    /// Terminal: parent namespace of the graph path. Fires one RPC.
    pub async fn namespace(&self) -> Result<String, ClientError> {
        let op = Op::Read(ReadExpr::Namespace {
            input: Arc::new(self.expr.clone()),
        });
        expect_string(self.transport.execute(&op).await?, "namespace")
    }

    /// Terminal: graph creation timestamp — never null (server metadata).
    /// Fires one RPC.
    pub async fn created(&self) -> Result<i64, ClientError> {
        let op = Op::Read(ReadExpr::Created {
            input: Arc::new(self.expr.clone()),
        });
        expect_i64(self.transport.execute(&op).await?, "created")
    }

    /// Terminal: graph last-opened timestamp — never null. Fires one RPC.
    pub async fn last_opened(&self) -> Result<i64, ClientError> {
        let op = Op::Read(ReadExpr::LastOpened {
            input: Arc::new(self.expr.clone()),
        });
        expect_i64(self.transport.execute(&op).await?, "lastOpened")
    }

    /// Terminal: graph last-updated timestamp — never null. Fires one RPC.
    pub async fn last_updated(&self) -> Result<i64, ClientError> {
        let op = Op::Read(ReadExpr::LastUpdated {
            input: Arc::new(self.expr.clone()),
        });
        expect_i64(self.transport.execute(&op).await?, "lastUpdated")
    }

    /// Terminal: list of unique layer names present in this graph. Fires one RPC.
    pub async fn unique_layers(&self) -> Result<Vec<String>, ClientError> {
        let op = Op::Read(ReadExpr::UniqueLayers {
            input: Arc::new(self.expr.clone()),
        });
        expect_string_list(self.transport.execute(&op).await?, "uniqueLayers")
    }

    /// Terminal: whether this view contains a layer named `name`. Fires one RPC.
    pub async fn has_layer(&self, name: impl ToString) -> Result<bool, ClientError> {
        let op = Op::Read(ReadExpr::HasLayer {
            input: Arc::new(self.expr.clone()),
            name: name.to_string(),
        });
        expect_bool(self.transport.execute(&op).await?, "hasLayer")
    }

    /// Terminal: the size of the window covered by this view (`end - start`),
    /// or `None` for an unbounded view. Fires one RPC.
    pub async fn window_size(&self) -> Result<Option<i64>, ClientError> {
        let op = Op::Read(ReadExpr::WindowSize {
            input: Arc::new(self.expr.clone()),
        });
        expect_optional_i64(self.transport.execute(&op).await?, "windowSize")
    }

    /// Terminal: earliest edge event time under the current view. Returns
    /// `None` if the view has no edge events. Fires one RPC.
    pub async fn earliest_edge_time(&self) -> Result<Option<i64>, ClientError> {
        let op = Op::Read(ReadExpr::EarliestEdgeTime {
            input: Arc::new(self.expr.clone()),
        });
        expect_optional_i64(self.transport.execute(&op).await?, "earliestEdgeTime")
    }

    /// Terminal: latest edge event time under the current view. Returns
    /// `None` if the view has no edge events. Fires one RPC.
    pub async fn latest_edge_time(&self) -> Result<Option<i64>, ClientError> {
        let op = Op::Read(ReadExpr::LatestEdgeTime {
            input: Arc::new(self.expr.clone()),
        });
        expect_optional_i64(self.transport.execute(&op).await?, "latestEdgeTime")
    }

    /// Internal helper: clone `self` with a new `expr`. Keeps the view-op
    /// builder methods (`.window`, `.layer`, `.at`, ...) as one-liners.
    fn with_expr(&self, expr: ReadExpr) -> RemoteGraph {
        RemoteGraph {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr,
        }
    }

    /// Returns a remote node reference for the given node id.
    ///
    /// **Fires one RPC** — a `hasNode` check against the current view chain,
    /// raising `ClientError::NotFound` if the node isn't visible under the
    /// current view. This guarantees that any handle you hold refers to a
    /// node the server actually resolved at handle-construction time; race
    /// conditions where the node is deleted between selection and terminal
    /// are still caught downstream via the null-intermediate NotFound path
    /// in `parse_read`.
    ///
    /// Server-returned handles (from `.nodes.collect()`, `.neighbours`, etc.)
    /// bypass this check — those ids came from the server, so we trust them.
    pub async fn node(&self, id: impl ToString) -> Result<Option<RemoteNode>, ClientError> {
        let id_str = id.to_string();
        let check = Op::Read(ReadExpr::HasNode {
            input: Arc::new(self.expr.clone()),
            id: id_str.clone(),
        });
        let exists = expect_bool(self.transport.execute(&check).await?, "hasNode")?;
        if !exists {
            return Ok(None);
        }
        Ok(Some(RemoteNode::with_expr(
            self.path.clone(),
            id_str.clone(),
            self.transport.clone(),
            ReadExpr::Node {
                input: Arc::new(self.expr.clone()),
                id: id_str,
            },
            HandleCtx::new(self.expr.clone()),
        )))
    }

    /// Returns the collection of all nodes in the graph, evaluated under the
    /// current view chain. Lazy — no RPC.
    pub fn nodes(&self) -> RemoteNodes {
        RemoteNodes::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::Nodes {
                input: Arc::new(self.expr.clone()),
            },
            HandleCtx::new(self.expr.clone()),
        )
    }

    /// Returns the metadata container of this graph — non-temporal
    /// properties whose values don't depend on time. Lazy — no RPC.
    pub fn metadata(&self) -> RemoteMetadata {
        RemoteMetadata::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::Metadata {
                input: Arc::new(self.expr.clone()),
            },
            HandleCtx::new(self.expr.clone()),
        )
    }

    /// Terminal: fetch the graph's schema — node types, edge layers, and
    /// their observed property/metadata schemas. Fires one RPC and
    /// materializes the entire tree eagerly (unlike other containers,
    /// which lazy-fetch on demand).
    pub async fn schema(&self) -> Result<RemoteGraphSchema, ClientError> {
        let op = Op::Read(ReadExpr::Schema {
            input: Arc::new(self.expr.clone()),
        });
        let prop = self
            .transport
            .execute(&op)
            .await?
            .ok_or_else(|| ClientError::InvalidResponse("schema returned null".into()))?;
        RemoteGraphSchema::from_prop(prop)
    }

    /// Terminal: the set-intersection of neighbours across the given node
    /// ids. Fires one RPC. Server behaviour:
    /// - empty input list → empty result
    /// - **ids that don't exist in the current view are silently dropped**;
    ///   the intersection is taken over the remaining ids
    /// - if no ids remain after dropping missing ones → empty result
    ///
    /// Each returned handle rebases at `self.expr`, so downstream terminals
    /// inherit the same view chain.
    pub async fn shared_neighbours(
        &self,
        ids: Vec<String>,
    ) -> Result<Vec<RemoteNode>, ClientError> {
        let op = Op::Read(ReadExpr::SharedNeighbours {
            input: Arc::new(self.expr.clone()),
            ids,
        });
        let names = expect_string_list(self.transport.execute(&op).await?, "sharedNeighbours")?;
        Ok(names
            .into_iter()
            .map(|name| {
                RemoteNode::with_expr(
                    self.path.clone(),
                    name.clone(),
                    self.transport.clone(),
                    ReadExpr::Node {
                        input: Arc::new(self.expr.clone()),
                        id: name,
                    },
                    HandleCtx::new(self.expr.clone()),
                )
            })
            .collect())
    }

    /// Terminal: the nodes whose latest property value equals the given value
    /// for **every** `(name, value)` entry in `properties_dict`. Mirrors the
    /// local `Graph.find_nodes`. Fires one RPC. Each returned handle rebases at
    /// `self.expr`, inheriting the current view chain.
    pub async fn find_nodes(
        &self,
        properties_dict: HashMap<String, Prop>,
    ) -> Result<Vec<RemoteNode>, ClientError> {
        let op = Op::Read(ReadExpr::FindNodes {
            input: Arc::new(self.expr.clone()),
            properties: properties_dict,
        });
        let names = expect_string_list(self.transport.execute(&op).await?, "findNodes")?;
        Ok(names
            .into_iter()
            .map(|name| {
                RemoteNode::with_expr(
                    self.path.clone(),
                    name.clone(),
                    self.transport.clone(),
                    ReadExpr::Node {
                        input: Arc::new(self.expr.clone()),
                        id: name,
                    },
                    HandleCtx::new(self.expr.clone()),
                )
            })
            .collect())
    }

    /// Terminal: the edges whose latest property value equals the given value
    /// for **every** `(name, value)` entry in `properties_dict`. Mirrors the
    /// local `Graph.find_edges`. Fires one RPC. Each returned handle rebases at
    /// `self.expr`, inheriting the current view chain.
    pub async fn find_edges(
        &self,
        properties_dict: HashMap<String, Prop>,
    ) -> Result<Vec<RemoteEdge>, ClientError> {
        let op = Op::Read(ReadExpr::FindEdges {
            input: Arc::new(self.expr.clone()),
            properties: properties_dict,
        });
        let pairs = expect_edge_list(self.transport.execute(&op).await?, "findEdges")?;
        Ok(pairs
            .into_iter()
            .map(|(src, dst)| {
                RemoteEdge::with_expr(
                    self.path.clone(),
                    src.clone(),
                    dst.clone(),
                    self.transport.clone(),
                    ReadExpr::Edge {
                        input: Arc::new(self.expr.clone()),
                        src,
                        dst,
                    },
                    HandleCtx::new(self.expr.clone()),
                )
            })
            .collect())
    }

    /// Terminal: all node types present in the graph. Mirrors the local
    /// `Graph.get_all_node_types`. Fires one RPC.
    pub async fn get_all_node_types(&self) -> Result<Vec<String>, ClientError> {
        let op = Op::Read(ReadExpr::GetAllNodeTypes {
            input: Arc::new(self.expr.clone()),
        });
        expect_string_list(self.transport.execute(&op).await?, "getAllNodeTypes")
    }

    /// Returns the full properties container of this graph — includes both
    /// temporal properties and metadata. Lazy — no RPC.
    pub fn properties(&self) -> RemoteProperties {
        RemoteProperties::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::Properties {
                input: Arc::new(self.expr.clone()),
            },
            HandleCtx::new(self.expr.clone()),
        )
    }

    /// Returns the collection of all edges in the graph, evaluated under the
    /// current view chain. Lazy — no RPC.
    pub fn edges(&self) -> RemoteEdges {
        RemoteEdges::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::Edges {
                input: Arc::new(self.expr.clone()),
            },
            HandleCtx::new(self.expr.clone()),
        )
    }

    /// Returns a remote edge reference for the given source and destination node ids.
    ///
    /// **Fires one RPC** — a `hasEdge` check against the current view chain,
    /// raising `ClientError::NotFound` if the edge isn't visible under the
    /// current view. Same guarantee and rationale as `.node()`.
    pub async fn edge(
        &self,
        src: impl ToString,
        dst: impl ToString,
    ) -> Result<Option<RemoteEdge>, ClientError> {
        let src_str = src.to_string();
        let dst_str = dst.to_string();
        let check = Op::Read(ReadExpr::HasEdge {
            input: Arc::new(self.expr.clone()),
            src: src_str.clone(),
            dst: dst_str.clone(),
        });
        let exists = expect_bool(self.transport.execute(&check).await?, "hasEdge")?;
        if !exists {
            return Ok(None);
        }
        Ok(Some(RemoteEdge::with_expr(
            self.path.clone(),
            src_str.clone(),
            dst_str.clone(),
            self.transport.clone(),
            ReadExpr::Edge {
                input: Arc::new(self.expr.clone()),
                src: src_str,
                dst: dst_str,
            },
            HandleCtx::new(self.expr.clone()),
        )))
    }

    /// Add a node to the graph at the given timestamp.
    ///
    /// Upsert-like: if a node with this id already exists, additional updates
    /// are appended at the given time. Use `create_node` for strict-create.
    ///
    /// Fires one RPC. Returns a trusted `RemoteNode` handle for the added
    /// node — no follow-up `hasNode` validation is fired, since the server
    /// just confirmed the write.
    pub async fn add_node<G: Into<GID> + ToString, T: IntoTime>(
        &self,
        timestamp: T,
        id: G,
        properties: Option<HashMap<String, Prop>>,
        node_type: Option<String>,
        layer: Option<String>,
        event_id: Option<usize>,
    ) -> Result<RemoteNode, ClientError> {
        let id_str = id.to_string();
        let op = Op::Write(WriteOp::AddNode(AddNodeOp {
            path: self.path.clone(),
            time: timestamp.into_time().t(),
            event_id,
            id: id_str.clone(),
            properties,
            node_type,
            layer,
        }));
        self.transport.execute(&op).await?;
        Ok(RemoteNode::with_expr(
            self.path.clone(),
            id_str.clone(),
            self.transport.clone(),
            ReadExpr::Node {
                input: Arc::new(self.expr.clone()),
                id: id_str,
            },
            HandleCtx::new(self.expr.clone()),
        ))
    }

    /// Create a new node at the given timestamp. Fails if a node with this
    /// id already exists — use `add_node` for upsert semantics.
    ///
    /// Fires one RPC. Returns a trusted `RemoteNode` handle for the created
    /// node — no follow-up `hasNode` validation is fired.
    pub async fn create_node<G: Into<GID> + ToString, T: IntoTime>(
        &self,
        timestamp: T,
        id: G,
        properties: Option<HashMap<String, Prop>>,
        node_type: Option<String>,
        layer: Option<String>,
        event_id: Option<usize>,
    ) -> Result<RemoteNode, ClientError> {
        let id_str = id.to_string();
        let op = Op::Write(WriteOp::CreateNode(CreateNodeOp {
            path: self.path.clone(),
            time: timestamp.into_time().t(),
            event_id,
            id: id_str.clone(),
            properties,
            node_type,
            layer,
        }));
        self.transport.execute(&op).await?;
        Ok(RemoteNode::with_expr(
            self.path.clone(),
            id_str.clone(),
            self.transport.clone(),
            ReadExpr::Node {
                input: Arc::new(self.expr.clone()),
                id: id_str,
            },
            HandleCtx::new(self.expr.clone()),
        ))
    }

    /// Add an edge to the graph at the given timestamp.
    ///
    /// Upsert-like: if an edge with these endpoints already exists, additional
    /// updates are appended at the given time (optionally on a specific layer).
    ///
    /// Fires one RPC. Returns a trusted `RemoteEdge` handle — no follow-up
    /// `hasEdge` validation is fired, since the server just confirmed the write.
    pub async fn add_edge<G: Into<GID> + ToString, T: IntoTime>(
        &self,
        timestamp: T,
        src: G,
        dst: G,
        properties: Option<HashMap<String, Prop>>,
        layer: Option<String>,
        event_id: Option<usize>,
    ) -> Result<RemoteEdge, ClientError> {
        let src_str = src.to_string();
        let dst_str = dst.to_string();
        let op = Op::Write(WriteOp::AddEdge(AddEdgeOp {
            path: self.path.clone(),
            time: timestamp.into_time().t(),
            event_id,
            src: src_str.clone(),
            dst: dst_str.clone(),
            properties,
            layer,
        }));
        self.transport.execute(&op).await?;
        Ok(RemoteEdge::with_expr(
            self.path.clone(),
            src_str.clone(),
            dst_str.clone(),
            self.transport.clone(),
            ReadExpr::Edge {
                input: Arc::new(self.expr.clone()),
                src: src_str,
                dst: dst_str,
            },
            HandleCtx::new(self.expr.clone()),
        ))
    }

    /// Add temporal properties on the graph itself (not on any node/edge) at
    /// the given timestamp. Distinct from `add_metadata`, which is non-temporal.
    ///
    /// Fires one RPC.
    pub async fn add_properties(
        &self,
        timestamp: EventTime,
        properties: HashMap<String, Prop>,
        event_id: Option<usize>,
    ) -> Result<(), ClientError> {
        let op = Op::Write(WriteOp::AddGraphProperty(AddGraphPropertyOp {
            path: self.path.clone(),
            time: timestamp.into_time().t(),
            event_id,
            properties,
        }));
        self.transport.execute(&op).await?;
        Ok(())
    }

    /// Add non-temporal metadata on the graph itself. Values persist for the
    /// lifetime of the graph and don't depend on any timestamp.
    ///
    /// Fires one RPC.
    pub async fn add_metadata(&self, properties: HashMap<String, Prop>) -> Result<(), ClientError> {
        let op = Op::Write(WriteOp::AddGraphMetadata(AddGraphMetadataOp {
            path: self.path.clone(),
            properties,
        }));
        self.transport.execute(&op).await?;
        Ok(())
    }

    /// Overwrite existing metadata on the graph. Unlike `add_metadata`, this
    /// replaces the value for each supplied key rather than adding.
    ///
    /// Fires one RPC.
    pub async fn update_metadata(
        &self,
        properties: HashMap<String, Prop>,
    ) -> Result<(), ClientError> {
        let op = Op::Write(WriteOp::UpdateGraphMetadata(UpdateGraphMetadataOp {
            path: self.path.clone(),
            properties,
        }));
        self.transport.execute(&op).await?;
        Ok(())
    }

    /// Mark an edge as deleted at the given timestamp (optionally on a
    /// specific layer). The edge remains queryable in earlier views; only
    /// events at or after `timestamp` see it as deleted.
    ///
    /// Fires one RPC. Returns a trusted `RemoteEdge` handle for the deleted
    /// edge — subsequent reads on it observe the deletion.
    pub async fn delete_edge<G: Into<GID> + ToString, T: IntoTime>(
        &self,
        timestamp: T,
        src: G,
        dst: G,
        layer: Option<String>,
    ) -> Result<RemoteEdge, ClientError> {
        let src_str = src.to_string();
        let dst_str = dst.to_string();
        let op = Op::Write(WriteOp::DeleteEdge(DeleteEdgeOp {
            path: self.path.clone(),
            time: timestamp.into_time().t(),
            src: src_str.clone(),
            dst: dst_str.clone(),
            layer,
        }));
        self.transport.execute(&op).await?;
        Ok(RemoteEdge::with_expr(
            self.path.clone(),
            src_str.clone(),
            dst_str.clone(),
            self.transport.clone(),
            ReadExpr::Edge {
                input: Arc::new(self.expr.clone()),
                src: src_str,
                dst: dst_str,
            },
            HandleCtx::new(self.expr.clone()),
        ))
    }
}
