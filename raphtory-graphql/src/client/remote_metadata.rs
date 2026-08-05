use crate::client::{
    op::{HandleCtx, Op, ReadExpr},
    remote_history::{RemoteEventTime, RemoteHistory},
    transport::{
        expect_bool, expect_i64, expect_optional_prop, expect_optional_property_tuple,
        expect_prop_list, expect_property_list, expect_property_tuple_list, expect_string_list,
        Transport,
    },
    ClientError,
};
use raphtory_api::core::entities::properties::prop::Prop;
use std::sync::Arc;

/// A handle to the metadata container of a remote graph, node, or edge —
/// the non-temporal properties whose values don't change over the graph's
/// lifetime.
///
/// Produced by `.metadata` on `RemoteGraph` / `RemoteNode` / `RemoteEdge`.
#[derive(Clone)]
pub struct RemoteMetadata {
    pub path: String,
    pub transport: Arc<dyn Transport>,
    pub expr: Arc<ReadExpr>,
    /// The parent graph view — carried for future propagation into
    /// materialized descendants once the container tree ships more types.
    pub ctx: HandleCtx,
}

impl RemoteMetadata {
    pub fn with_expr(
        path: String,
        transport: Arc<dyn Transport>,
        expr: impl Into<Arc<ReadExpr>>,
        ctx: HandleCtx,
    ) -> Self {
        Self {
            path,
            transport,
            expr: expr.into(),
            ctx,
        }
    }

    /// Terminal: fetch a single metadata value by key. Returns `None` if the
    /// key isn't present. Fires one RPC.
    pub async fn get(&self, key: impl ToString) -> Result<Option<Prop>, ClientError> {
        let op = Op::Read(ReadExpr::PropertyGet {
            input: self.expr.clone(),
            key: key.to_string(),
        });
        expect_optional_prop(self.transport.execute(&op).await?, "get")
    }

    /// Terminal: check whether a metadata entry with this key exists. Fires one RPC.
    pub async fn contains(&self, key: impl ToString) -> Result<bool, ClientError> {
        let op = Op::Read(ReadExpr::PropertyContains {
            input: self.expr.clone(),
            key: key.to_string(),
        });
        expect_bool(self.transport.execute(&op).await?, "contains")
    }

    /// Terminal: all metadata keys present on this entity. Fires one RPC.
    pub async fn keys(&self) -> Result<Vec<String>, ClientError> {
        let op = Op::Read(ReadExpr::PropertyKeys {
            input: self.expr.clone(),
        });
        expect_string_list(self.transport.execute(&op).await?, "keys")
    }

    /// Terminal: all metadata values (no keys — see `items()` for pairs).
    /// If `keys` is `Some`, only values for those names are returned. Fires
    /// one RPC.
    pub async fn values(&self, keys: Option<Vec<String>>) -> Result<Vec<Prop>, ClientError> {
        let op = Op::Read(ReadExpr::PropertyValues {
            input: self.expr.clone(),
            keys,
        });
        expect_prop_list(self.transport.execute(&op).await?, "values")
    }

    /// Terminal: all `(key, value)` entries as pairs. If `keys` is `Some`,
    /// only entries with those names are returned. Unlike `values()`, this
    /// fetches the keys too — use it only when the pairs are needed. Fires
    /// one RPC.
    pub async fn items(
        &self,
        keys: Option<Vec<String>>,
    ) -> Result<Vec<(String, Prop)>, ClientError> {
        let op = Op::Read(ReadExpr::PropertyItems {
            input: self.expr.clone(),
            keys,
        });
        expect_property_list(self.transport.execute(&op).await?, "items")
    }
}

/// A handle to the full properties container of a remote graph, node, or
/// edge — includes both non-temporal metadata and temporal properties.
///
/// Same terminal shape as `RemoteMetadata` (`get`/`contains`/`keys`/`values`),
/// but each value can be temporal. Callers who want to drill into a
/// property's timeline reach for `.temporal()`.
///
/// Produced by `.properties()` on `RemoteGraph` / `RemoteNode` / `RemoteEdge`.
#[derive(Clone)]
pub struct RemoteProperties {
    pub path: String,
    pub transport: Arc<dyn Transport>,
    pub expr: Arc<ReadExpr>,
    pub ctx: HandleCtx,
}

impl RemoteProperties {
    pub fn with_expr(
        path: String,
        transport: Arc<dyn Transport>,
        expr: impl Into<Arc<ReadExpr>>,
        ctx: HandleCtx,
    ) -> Self {
        Self {
            path,
            transport,
            expr: expr.into(),
            ctx,
        }
    }

    /// Terminal: fetch a single property value by key. Returns `None` if
    /// the key isn't present in the current view. For a temporal property,
    /// this yields its most recent value under the view. Fires one RPC.
    pub async fn get(&self, key: impl ToString) -> Result<Option<Prop>, ClientError> {
        let op = Op::Read(ReadExpr::PropertyGet {
            input: self.expr.clone(),
            key: key.to_string(),
        });
        expect_optional_prop(self.transport.execute(&op).await?, "get")
    }

    /// Terminal: whether a property with this key exists. Fires one RPC.
    pub async fn contains(&self, key: impl ToString) -> Result<bool, ClientError> {
        let op = Op::Read(ReadExpr::PropertyContains {
            input: self.expr.clone(),
            key: key.to_string(),
        });
        expect_bool(self.transport.execute(&op).await?, "contains")
    }

    /// Terminal: all property keys in the current view. Does not include
    /// metadata keys — those are exposed separately on `.metadata`. Fires
    /// one RPC.
    pub async fn keys(&self) -> Result<Vec<String>, ClientError> {
        let op = Op::Read(ReadExpr::PropertyKeys {
            input: self.expr.clone(),
        });
        expect_string_list(self.transport.execute(&op).await?, "keys")
    }

    /// Terminal: all property values (no keys — see `items()` for pairs).
    /// If `keys` is `Some`, only values for those names are returned. For
    /// temporal properties, each value is the property's most recent value
    /// under the view. Fires one RPC.
    pub async fn values(&self, keys: Option<Vec<String>>) -> Result<Vec<Prop>, ClientError> {
        let op = Op::Read(ReadExpr::PropertyValues {
            input: self.expr.clone(),
            keys,
        });
        expect_prop_list(self.transport.execute(&op).await?, "values")
    }

    /// Terminal: all `(key, value)` entries as pairs. If `keys` is `Some`,
    /// only entries with those names are returned. Unlike `values()`, this
    /// fetches the keys too — use it only when the pairs are needed. Fires
    /// one RPC.
    pub async fn items(
        &self,
        keys: Option<Vec<String>>,
    ) -> Result<Vec<(String, Prop)>, ClientError> {
        let op = Op::Read(ReadExpr::PropertyItems {
            input: self.expr.clone(),
            keys,
        });
        expect_property_list(self.transport.execute(&op).await?, "items")
    }

    /// Terminal: the data-type of the property's latest value by key, as its
    /// `PropType` display string (e.g. `"I64"`, `"Str"`, `"List<F64>"`).
    /// Returns `None` when the key isn't present. Mirrors the local
    /// `Properties.get_dtype_of`. Fires one RPC.
    pub async fn get_dtype_of(&self, key: impl ToString) -> Result<Option<String>, ClientError> {
        let op = Op::Read(ReadExpr::PropertyGetDtypeOf {
            input: self.expr.clone(),
            key: key.to_string(),
        });
        match self.transport.execute(&op).await? {
            None => Ok(None),
            Some(Prop::Str(s)) => Ok(Some(s.to_string())),
            Some(_) => Err(ClientError::InvalidResponse(
                "getDtypeOf returned unexpected value type".into(),
            )),
        }
    }

    /// Sub-container: the temporal-only view of these properties — excludes
    /// metadata and lets you drill into per-key timelines. Lazy — no RPC.
    pub fn temporal(&self) -> RemoteTemporalProperties {
        RemoteTemporalProperties {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: Arc::new(ReadExpr::TemporalProperties {
                input: self.expr.clone(),
            }),
            ctx: self.ctx.clone(),
        }
    }
}

/// A handle to the temporal-only view of a properties container. Each
/// property in this container has a full history over time — access it
/// via `.get(key)` which returns a `RemoteTemporalProperty` handle.
///
/// Produced by `RemoteProperties.temporal()`.
#[derive(Clone)]
pub struct RemoteTemporalProperties {
    pub path: String,
    pub transport: Arc<dyn Transport>,
    pub expr: Arc<ReadExpr>,
    pub ctx: HandleCtx,
}

impl RemoteTemporalProperties {
    /// Terminal: fetch a temporal property by key. Returns `None` if the
    /// key isn't present. Fires one RPC (`contains` check) to validate
    /// existence — if `Some`, subsequent method calls on the handle fire
    /// their own RPCs.
    pub async fn get(
        &self,
        key: impl ToString,
    ) -> Result<Option<RemoteTemporalProperty>, ClientError> {
        let key_str = key.to_string();
        let op = Op::Read(ReadExpr::PropertyContains {
            input: self.expr.clone(),
            key: key_str.clone(),
        });
        let exists = expect_bool(self.transport.execute(&op).await?, "contains")?;
        if !exists {
            return Ok(None);
        }
        Ok(Some(RemoteTemporalProperty {
            path: self.path.clone(),
            transport: self.transport.clone(),
            key: key_str.clone(),
            expr: Arc::new(ReadExpr::TemporalPropertyByKey {
                input: self.expr.clone(),
                key: key_str,
            }),
            ctx: self.ctx.clone(),
        }))
    }

    /// Terminal: whether a temporal property with this key exists. Fires one RPC.
    pub async fn contains(&self, key: impl ToString) -> Result<bool, ClientError> {
        let op = Op::Read(ReadExpr::PropertyContains {
            input: self.expr.clone(),
            key: key.to_string(),
        });
        expect_bool(self.transport.execute(&op).await?, "contains")
    }

    /// Terminal: all temporal property keys. Fires one RPC.
    pub async fn keys(&self) -> Result<Vec<String>, ClientError> {
        let op = Op::Read(ReadExpr::PropertyKeys {
            input: self.expr.clone(),
        });
        expect_string_list(self.transport.execute(&op).await?, "keys")
    }

    /// Terminal: all temporal properties as `RemoteTemporalProperty` handles.
    /// If `keys` is `Some`, only entries with those names are returned.
    /// Fires one RPC (fetches the key list); each returned handle fires its
    /// own RPC on subsequent method calls.
    pub async fn values(
        &self,
        keys: Option<Vec<String>>,
    ) -> Result<Vec<RemoteTemporalProperty>, ClientError> {
        let op = Op::Read(ReadExpr::TemporalPropertyList {
            input: self.expr.clone(),
            keys,
        });
        let key_list = expect_string_list(self.transport.execute(&op).await?, "values")?;
        Ok(key_list
            .into_iter()
            .map(|key| RemoteTemporalProperty {
                path: self.path.clone(),
                transport: self.transport.clone(),
                key: key.clone(),
                expr: Arc::new(ReadExpr::TemporalPropertyByKey {
                    input: self.expr.clone(),
                    key,
                }),
                ctx: self.ctx.clone(),
            })
            .collect())
    }
}

/// A handle to a single temporal property — one key with its full history
/// of updates across time, plus statistical summaries and time-indexed
/// accessors.
///
/// Produced by `RemoteTemporalProperties.get()` / `.values()`.
#[derive(Clone)]
pub struct RemoteTemporalProperty {
    pub path: String,
    pub transport: Arc<dyn Transport>,
    /// The property name — cached on the handle so callers don't need to
    /// fire an RPC just to recover it.
    pub key: String,
    pub expr: Arc<ReadExpr>,
    pub ctx: HandleCtx,
}

impl RemoteTemporalProperty {
    /// Terminal: event history for this property — one entry per temporal
    /// update, in insertion order. Fires one RPC when a terminal on the
    /// returned `RemoteHistory` is invoked.
    pub fn history(&self) -> RemoteHistory {
        RemoteHistory::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::History {
                input: self.expr.clone(),
            },
            self.ctx.clone(),
        )
    }

    /// Terminal: all values this property has ever taken, in temporal order
    /// (one per update). Fires one RPC.
    pub async fn values(&self) -> Result<Vec<Prop>, ClientError> {
        let op = Op::Read(ReadExpr::TemporalPropertyValueList {
            input: self.expr.clone(),
        });
        expect_prop_list(self.transport.execute(&op).await?, "values")
    }

    /// Terminal: value at or before time `t` (latest update on or before
    /// `t`). Returns `None` if no update exists on or before `t`. Fires one RPC.
    pub async fn at(&self, time: i64) -> Result<Option<Prop>, ClientError> {
        let op = Op::Read(ReadExpr::TemporalPropertyAt {
            input: self.expr.clone(),
            time,
        });
        expect_optional_prop(self.transport.execute(&op).await?, "at")
    }

    /// Terminal: the most recent value, or `None` if the property has never
    /// been set in this view. Fires one RPC.
    pub async fn latest(&self) -> Result<Option<Prop>, ClientError> {
        let op = Op::Read(ReadExpr::TemporalPropertyLatest {
            input: self.expr.clone(),
        });
        expect_optional_prop(self.transport.execute(&op).await?, "latest")
    }

    /// Terminal: number of updates recorded for this property in the current
    /// view. Fires one RPC.
    pub async fn count(&self) -> Result<i64, ClientError> {
        let op = Op::Read(ReadExpr::Count {
            input: self.expr.clone(),
        });
        expect_i64(self.transport.execute(&op).await?, "count")
    }

    /// Terminal: distinct values this property has ever taken. Order is not
    /// guaranteed. Fires one RPC.
    pub async fn unique(&self) -> Result<Vec<Prop>, ClientError> {
        let op = Op::Read(ReadExpr::TemporalPropertyUnique {
            input: self.expr.clone(),
        });
        expect_prop_list(self.transport.execute(&op).await?, "unique")
    }

    /// Terminal: collapse consecutive-equal updates into single `(time,
    /// value)` pairs. `latest_time = true` picks the last timestamp of
    /// each run; `false` picks the first. Fires one RPC.
    pub async fn ordered_dedupe(
        &self,
        latest_time: bool,
    ) -> Result<Vec<RemotePropertyTuple>, ClientError> {
        let op = Op::Read(ReadExpr::TemporalPropertyOrderedDedupe {
            input: self.expr.clone(),
            latest_time,
        });
        let tuples =
            expect_property_tuple_list(self.transport.execute(&op).await?, "orderedDedupe")?;
        Ok(tuples
            .into_iter()
            .map(|(time, value)| RemotePropertyTuple { time, value })
            .collect())
    }

    /// Terminal: sum of all updates. `None` if not additive. Fires one RPC.
    pub async fn sum(&self) -> Result<Option<Prop>, ClientError> {
        let op = Op::Read(ReadExpr::TemporalPropertySum {
            input: self.expr.clone(),
        });
        expect_optional_prop(self.transport.execute(&op).await?, "sum")
    }

    /// Terminal: mean of all updates as `f64`. `None` if not numeric or empty.
    /// Fires one RPC.
    pub async fn mean(&self) -> Result<Option<Prop>, ClientError> {
        let op = Op::Read(ReadExpr::TemporalPropertyMean {
            input: self.expr.clone(),
        });
        expect_optional_prop(self.transport.execute(&op).await?, "mean")
    }

    /// Terminal: alias for `mean`. Fires one RPC.
    pub async fn average(&self) -> Result<Option<Prop>, ClientError> {
        let op = Op::Read(ReadExpr::TemporalPropertyAverage {
            input: self.expr.clone(),
        });
        expect_optional_prop(self.transport.execute(&op).await?, "average")
    }

    /// Terminal: minimum `(time, value)` pair. `None` if not comparable or
    /// empty. Fires one RPC.
    pub async fn min(&self) -> Result<Option<RemotePropertyTuple>, ClientError> {
        let op = Op::Read(ReadExpr::TemporalPropertyMin {
            input: self.expr.clone(),
        });
        expect_optional_property_tuple(self.transport.execute(&op).await?, "min")
            .map(|opt| opt.map(|(time, value)| RemotePropertyTuple { time, value }))
    }

    /// Terminal: maximum `(time, value)` pair. `None` if not comparable or
    /// empty. Fires one RPC.
    pub async fn max(&self) -> Result<Option<RemotePropertyTuple>, ClientError> {
        let op = Op::Read(ReadExpr::TemporalPropertyMax {
            input: self.expr.clone(),
        });
        expect_optional_property_tuple(self.transport.execute(&op).await?, "max")
            .map(|opt| opt.map(|(time, value)| RemotePropertyTuple { time, value }))
    }

    /// Terminal: median `(time, value)` pair (lower median on even-length
    /// inputs). `None` if not comparable or empty. Fires one RPC.
    pub async fn median(&self) -> Result<Option<RemotePropertyTuple>, ClientError> {
        let op = Op::Read(ReadExpr::TemporalPropertyMedian {
            input: self.expr.clone(),
        });
        expect_optional_property_tuple(self.transport.execute(&op).await?, "median")
            .map(|opt| opt.map(|(time, value)| RemotePropertyTuple { time, value }))
    }
}

/// A `(time, value)` snapshot inside a temporal property — the return type
/// of `min` / `max` / `median` (which return a single pair) and each entry
/// of `ordered_dedupe` (which returns a list of pairs).
#[derive(Clone, Debug, PartialEq)]
pub struct RemotePropertyTuple {
    /// The event time at which this value was observed.
    pub time: RemoteEventTime,
    /// The property value at that time.
    pub value: Prop,
}
