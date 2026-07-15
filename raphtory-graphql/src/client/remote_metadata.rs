use crate::client::{
    op::{Op, ReadExpr},
    remote_graph::{expect_bool, expect_optional_property, expect_property_list, expect_string_list},
    transport::Transport,
    ClientError,
};
use raphtory_api::core::entities::properties::prop::Prop;
use std::sync::Arc;

/// A single `(key, value)` property reading. Value can be any polymorphic
/// property type — string, int, float, bool, list, map, datetime, etc.
///
/// Constructed by `RemoteMetadata.get()` / `.values()` (and, in the upcoming
/// Properties batch, by `RemoteProperties.get()` / `.values()`).
#[derive(Clone, Debug, PartialEq)]
pub struct RemoteProperty {
    /// The property name.
    pub key: String,
    /// The property value. `Prop` is raphtory's polymorphic value enum;
    /// PyO3 converts it to a native Python type when returned across the FFI
    /// boundary.
    pub value: Prop,
}

/// A handle to the metadata container of a remote graph, node, or edge —
/// the non-temporal properties whose values don't change over the graph's
/// lifetime.
///
/// Produced by `.metadata` on `RemoteGraph` / `RemoteNode` / `RemoteEdge`.
#[derive(Clone)]
pub struct RemoteMetadata {
    pub path: String,
    pub transport: Arc<dyn Transport>,
    pub expr: ReadExpr,
    /// The parent graph view — carried for future propagation into
    /// materialized descendants once the container tree ships more types.
    pub base_graph: ReadExpr,
}

impl RemoteMetadata {
    pub fn with_expr(
        path: String,
        transport: Arc<dyn Transport>,
        expr: ReadExpr,
        base_graph: ReadExpr,
    ) -> Self {
        Self {
            path,
            transport,
            expr,
            base_graph,
        }
    }

    /// Terminal: fetch a single metadata value by key. Returns `None` if the
    /// key isn't present. Fires one RPC.
    pub async fn get(&self, key: impl ToString) -> Result<Option<RemoteProperty>, ClientError> {
        let op = Op::Read(ReadExpr::PropertyGet {
            input: Box::new(self.expr.clone()),
            key: key.to_string(),
        });
        expect_optional_property(self.transport.execute(&op).await?, "get")
            .map(|opt| opt.map(|(key, value)| RemoteProperty { key, value }))
    }

    /// Terminal: check whether a metadata entry with this key exists. Fires one RPC.
    pub async fn contains(&self, key: impl ToString) -> Result<bool, ClientError> {
        let op = Op::Read(ReadExpr::PropertyContains {
            input: Box::new(self.expr.clone()),
            key: key.to_string(),
        });
        expect_bool(self.transport.execute(&op).await?, "contains")
    }

    /// Terminal: all metadata keys present on this entity. Fires one RPC.
    pub async fn keys(&self) -> Result<Vec<String>, ClientError> {
        let op = Op::Read(ReadExpr::PropertyKeys {
            input: Box::new(self.expr.clone()),
        });
        expect_string_list(self.transport.execute(&op).await?, "keys")
    }

    /// Terminal: all `(key, value)` metadata entries. If `keys` is `Some`,
    /// only entries with those names are returned. Fires one RPC.
    pub async fn values(
        &self,
        keys: Option<Vec<String>>,
    ) -> Result<Vec<RemoteProperty>, ClientError> {
        let op = Op::Read(ReadExpr::PropertyValues {
            input: Box::new(self.expr.clone()),
            keys,
        });
        let pairs = expect_property_list(self.transport.execute(&op).await?, "values")?;
        Ok(pairs
            .into_iter()
            .map(|(key, value)| RemoteProperty { key, value })
            .collect())
    }
}
