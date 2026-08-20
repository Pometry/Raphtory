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

impl Column {
    /// Whether any member has a value — distinguishes a registered key whose
    /// members all lack a value from a key that is not registered at all.
    fn has_values(&self) -> bool {
        match self {
            Column::Flat(members) => members.iter().any(Option::is_some),
            Column::Nested(sources) => sources
                .iter()
                .any(|members| members.iter().any(Option::is_some)),
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

            /// Fetch the given columns in one RPC — one aliased `get` per
            /// key, so only those columns travel and the response arrives
            /// already column-shaped (no key matching, no pivot).
            async fn fetch_columns(&self, keys: Arc<[String]>) -> Result<Vec<Column>, ClientError> {
                if self.nested {
                    let op = Op::Read(ReadExpr::$values_nested {
                        input: self.expr.clone(),
                        keys,
                    });
                    let cols = expect_nested_columnar_property_list(
                        self.transport.execute(&op).await?,
                        "values",
                    )?;
                    Ok(cols.into_iter().map(Column::Nested).collect())
                } else {
                    let op = Op::Read(ReadExpr::$values_flat {
                        input: self.expr.clone(),
                        keys,
                    });
                    let cols = expect_columnar_property_list(
                        self.transport.execute(&op).await?,
                        "values",
                    )?;
                    Ok(cols.into_iter().map(Column::Flat).collect())
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
            /// One RPC fetching just this column. Only when the column is
            /// entirely empty does a key lookup distinguish registered-but-
            /// absent (`Some` column of `None`s) from unregistered (`None`).
            pub async fn get(&self, key: &str) -> Result<Option<Column>, ClientError> {
                let keys: Arc<[String]> = Arc::from(vec![key.to_string()]);
                let column = self.fetch_columns(keys).await?.pop().ok_or_else(|| {
                    ClientError::InvalidResponse("`get` returned no column".into())
                })?;
                if column.has_values() || self.contains(key).await? {
                    Ok(Some(column))
                } else {
                    Ok(None)
                }
            }

            /// Every column, keyed and ordered by [`keys`](Self::keys) — the
            /// local views drive `values` / `items` / `as_dict` off `keys()`
            /// (`keys().map(get)`), so the remote mirrors that: one key lookup
            /// plus one fetch of exactly the columns that will be returned,
            /// zipped back to their keys by position.
            pub async fn fetch_all(&self) -> Result<Vec<(String, Column)>, ClientError> {
                let keys = self.keys().await?;
                if keys.is_empty() {
                    return Ok(Vec::new());
                }
                let columns = self.fetch_columns(Arc::from(keys.clone())).await?;
                Ok(keys.into_iter().zip(columns).collect())
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
