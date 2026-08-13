//! Collection-level (columnar) metadata / properties views.
//!
//! Where the single-entity [`RemoteMetadata`](crate::client::remote_metadata)
//! reads one node/edge/graph, these views read a *collection* of members and
//! return one value per member (a "column") for a given key. They mirror the
//! local `MetadataView` / `PropertiesView` (flat collections) and their nested
//! `*ListList` variants (`PathFromGraph` / `NestedEdges`).
//!
//! Reads are lazy, shaped to the question being asked — mirroring the local
//! views, whose `keys()` / `__contains__` are registry lookups on the first
//! member and whose `get()` walks one key:
//! - [`keys`](RemoteMetadataView::keys) / [`contains`](RemoteMetadataView::contains)
//!   fire a `page(limit: 1)` key lookup — one member's key names on the wire.
//! - [`get`](RemoteMetadataView::get) fetches a single column via the server's
//!   `values(keys: [..])` whitelist — never the collection's other properties.
//! - [`fetch_all`](RemoteMetadataView::fetch_all) fetches everything in one
//!   RPC, for the all-columns reads (`values` / `items` / `as_dict`).
//!
//! For `properties`, temporal values collapse to their latest under the
//! current view — matching local.

use crate::client::{
    op::{HandleCtx, Op, ReadExpr},
    transport::{
        expect_columnar_property_list, expect_nested_columnar_property_list, expect_string_list,
        Transport,
    },
    ClientError,
};
use raphtory_api::core::entities::properties::prop::Prop;
use std::sync::Arc;

/// One key's column: a value per member, `None` where a member lacks the key.
/// `Nested` carries one member-list per source node.
#[derive(Clone, Debug)]
pub enum Column {
    /// Flat collection (`Nodes` / `Edges` / `PathFromNode`).
    Flat(Vec<Option<Prop>>),
    /// Nested collection (`PathFromGraph` / `NestedEdges`).
    Nested(Vec<Vec<Option<Prop>>>),
}

/// The pivoted result of a columnar property/metadata fetch. `Flat` carries one
/// entry-list per member; `Nested` carries one member-list per source node.
#[derive(Clone, Debug)]
pub enum ColumnarProps {
    /// Flat collection (`Nodes` / `Edges` / `PathFromNode`): per-member entries.
    Flat(Vec<Vec<(String, Prop)>>),
    /// Nested collection (`PathFromGraph` / `NestedEdges`): per-source,
    /// per-member entries.
    Nested(Vec<Vec<Vec<(String, Prop)>>>),
}

/// Look up `key` in one member's `(key, value)` entries.
fn member_value(entries: &[(String, Prop)], key: &str) -> Option<Prop> {
    entries
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.clone())
}

impl ColumnarProps {
    /// The union of all keys across every fetched entry, in first-seen order.
    fn entry_keys(&self) -> Vec<String> {
        let mut seen: Vec<String> = Vec::new();
        let mut push = |entries: &Vec<(String, Prop)>| {
            for (k, _) in entries {
                if !seen.contains(k) {
                    seen.push(k.clone());
                }
            }
        };
        match self {
            ColumnarProps::Flat(members) => {
                for m in members {
                    push(m);
                }
            }
            ColumnarProps::Nested(sources) => {
                for source in sources {
                    for m in source {
                        push(m);
                    }
                }
            }
        }
        seen
    }

    /// Whether any fetched member carried a value (i.e. the fetch matched at
    /// least one entry).
    fn has_entries(&self) -> bool {
        !self.entry_keys().is_empty()
    }

    /// Pivot out the column for `key`: one value per member, `None` where a
    /// member lacks the key.
    pub fn column(&self, key: &str) -> Column {
        match self {
            ColumnarProps::Flat(members) => {
                Column::Flat(members.iter().map(|m| member_value(m, key)).collect())
            }
            ColumnarProps::Nested(sources) => Column::Nested(
                sources
                    .iter()
                    .map(|source| source.iter().map(|m| member_value(m, key)).collect())
                    .collect(),
            ),
        }
    }
}

// The two views share every accessor; only the ReadExpr variants they fire
// differ. The macro pins the two impls in lockstep.
macro_rules! columnar_view_impl {
    ($ty:ident, $values_flat:ident, $values_nested:ident, $keys_flat:ident, $keys_nested:ident) => {
        impl $ty {
            pub fn with_expr(
                path: String,
                transport: Arc<dyn Transport>,
                expr: impl Into<Arc<ReadExpr>>,
                ctx: HandleCtx,
                nested: bool,
            ) -> Self {
                Self {
                    path,
                    transport,
                    expr: expr.into(),
                    ctx,
                    nested,
                }
            }

            /// Fetch the members' `{key, value}` entries, pivoted into
            /// [`ColumnarProps`]. `keys: Some(..)` renders the server-side
            /// `values(keys: [..])` whitelist so only those columns travel;
            /// `None` fetches every column. One RPC either way.
            async fn fetch_with_keys(
                &self,
                keys: Option<Arc<[String]>>,
            ) -> Result<ColumnarProps, ClientError> {
                if self.nested {
                    let op = Op::Read(ReadExpr::$values_nested {
                        input: self.expr.clone(),
                        keys,
                    });
                    let data = expect_nested_columnar_property_list(
                        self.transport.execute(&op).await?,
                        "values",
                    )?;
                    Ok(ColumnarProps::Nested(data))
                } else {
                    let op = Op::Read(ReadExpr::$values_flat {
                        input: self.expr.clone(),
                        keys,
                    });
                    let data = expect_columnar_property_list(
                        self.transport.execute(&op).await?,
                        "values",
                    )?;
                    Ok(ColumnarProps::Flat(data))
                }
            }

            /// The key set, read from the FIRST member — mirroring the local
            /// views, whose `keys()` reads the first entity's filtered
            /// property registry. One `page(limit: 1)` RPC; only key names
            /// travel. Empty collection → empty list, like local.
            pub async fn keys(&self) -> Result<Vec<String>, ClientError> {
                let op = if self.nested {
                    Op::Read(ReadExpr::$keys_nested {
                        input: self.expr.clone(),
                    })
                } else {
                    Op::Read(ReadExpr::$keys_flat {
                        input: self.expr.clone(),
                    })
                };
                expect_string_list(self.transport.execute(&op).await?, "keys")
            }

            /// Whether the key exists — same first-member registry lookup as
            /// [`keys`](Self::keys), matching the local `__contains__`.
            pub async fn contains(&self, key: &str) -> Result<bool, ClientError> {
                Ok(self.keys().await?.iter().any(|k| k == key))
            }

            /// The single column for `key`, or `None` when the key isn't
            /// registered — matching the local `get()`.
            ///
            /// Fast path: one RPC fetching just this column. Only when the
            /// column comes back entirely empty does a key lookup distinguish
            /// registered-but-absent (`Some` column of `None`s) from
            /// unregistered (`None`).
            pub async fn get(&self, key: &str) -> Result<Option<Column>, ClientError> {
                let whitelist: Arc<[String]> = Arc::from(vec![key.to_string()]);
                let data = self.fetch_with_keys(Some(whitelist)).await?;
                if data.has_entries() || self.contains(key).await? {
                    Ok(Some(data.column(key)))
                } else {
                    Ok(None)
                }
            }

            /// Every column, keyed and ordered by [`keys`](Self::keys) — the
            /// local views drive `values` / `items` / `as_dict` off `keys()`
            /// (`keys().map(get)`), so the remote mirrors that: one key lookup
            /// plus one whitelist-filtered values fetch, shipping exactly the
            /// columns that will be returned.
            pub async fn fetch_all(&self) -> Result<Vec<(String, Column)>, ClientError> {
                let keys = self.keys().await?;
                if keys.is_empty() {
                    return Ok(Vec::new());
                }
                let whitelist: Arc<[String]> = Arc::from(keys.clone());
                let data = self.fetch_with_keys(Some(whitelist)).await?;
                Ok(keys
                    .into_iter()
                    .map(|key| {
                        let col = data.column(&key);
                        (key, col)
                    })
                    .collect())
            }
        }
    };
}

/// A columnar view over the non-temporal metadata of a remote node/edge
/// collection. Produced by `.metadata` on the remote collection handles.
#[derive(Clone)]
pub struct RemoteMetadataView {
    pub path: String,
    pub transport: Arc<dyn Transport>,
    /// The accumulated collection read expression.
    pub expr: Arc<ReadExpr>,
    pub ctx: HandleCtx,
    /// `true` for nested collections (`PathFromGraph` / `NestedEdges`).
    pub nested: bool,
}

columnar_view_impl!(
    RemoteMetadataView,
    CollectionMetadataValues,
    NestedMetadataValues,
    CollectionMetadataKeys,
    NestedMetadataKeys
);

/// A columnar view over the full properties (temporal → latest) of a remote
/// node/edge collection. Produced by `.properties` on the remote collection
/// handles.
#[derive(Clone)]
pub struct RemotePropertiesView {
    pub path: String,
    pub transport: Arc<dyn Transport>,
    pub expr: Arc<ReadExpr>,
    pub ctx: HandleCtx,
    pub nested: bool,
}

columnar_view_impl!(
    RemotePropertiesView,
    CollectionPropertiesValues,
    NestedPropertiesValues,
    CollectionPropertiesKeys,
    NestedPropertiesKeys
);
