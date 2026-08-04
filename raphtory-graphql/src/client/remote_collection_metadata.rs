//! Collection-level (columnar) metadata / properties views.
//!
//! Where the single-entity [`RemoteMetadata`](crate::client::remote_metadata)
//! reads one node/edge/graph, these views read a *collection* of members and
//! return one value per member (a "column") for a given key. They mirror the
//! local `MetadataView` / `PropertiesView` (flat collections) and their nested
//! `*ListList` variants (`PathFromGraph` / `NestedEdges`).
//!
//! A single RPC fetches every member's `{key, value}` entries; the client then
//! pivots them into per-key columns, so `keys` / `get` / `values` / `items` /
//! `as_dict` all derive from the same fetch shape. For `properties`, temporal
//! values collapse to their latest under the current view — matching local.

use crate::client::{
    op::{HandleCtx, Op, ReadExpr},
    transport::{expect_columnar_property_list, expect_nested_columnar_property_list, Transport},
    ClientError,
};
use raphtory_api::core::entities::properties::prop::Prop;
use std::sync::Arc;

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

impl ColumnarProps {
    /// The union of all keys across every member, in first-seen order —
    /// matching the local view's `keys()` ordering (collection order, then
    /// per-member key order).
    pub fn keys(&self) -> Vec<String> {
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

    /// Whether any member carries this key.
    pub fn contains_key(&self, key: &str) -> bool {
        self.keys().iter().any(|k| k == key)
    }
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

impl RemoteMetadataView {
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

    /// Fetch every member's metadata `{key, value}` entries, pivoted into
    /// [`ColumnarProps`]. Fires one RPC.
    pub async fn fetch(&self) -> Result<ColumnarProps, ClientError> {
        if self.nested {
            let op = Op::Read(ReadExpr::NestedMetadataValues {
                input: self.expr.clone(),
            });
            let data = expect_nested_columnar_property_list(
                self.transport.execute(&op).await?,
                "metadata",
            )?;
            Ok(ColumnarProps::Nested(data))
        } else {
            let op = Op::Read(ReadExpr::CollectionMetadataValues {
                input: self.expr.clone(),
            });
            let data =
                expect_columnar_property_list(self.transport.execute(&op).await?, "metadata")?;
            Ok(ColumnarProps::Flat(data))
        }
    }
}

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

impl RemotePropertiesView {
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

    /// Fetch every member's property `{key, value}` entries (temporal
    /// properties yield their latest value under the current view), pivoted
    /// into [`ColumnarProps`]. Fires one RPC.
    pub async fn fetch(&self) -> Result<ColumnarProps, ClientError> {
        if self.nested {
            let op = Op::Read(ReadExpr::NestedPropertiesValues {
                input: self.expr.clone(),
            });
            let data = expect_nested_columnar_property_list(
                self.transport.execute(&op).await?,
                "properties",
            )?;
            Ok(ColumnarProps::Nested(data))
        } else {
            let op = Op::Read(ReadExpr::CollectionPropertiesValues {
                input: self.expr.clone(),
            });
            let data =
                expect_columnar_property_list(self.transport.execute(&op).await?, "properties")?;
            Ok(ColumnarProps::Flat(data))
        }
    }
}
