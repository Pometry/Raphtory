use super::{node_collection, view_ops::remote_view_ops};
use crate::{
    client::{
        op::{HandleCtx, HandleOp, InputTime, NodeSortBy, Op, ReadExpr, ViewOp},
        remote_collection_metadata::{RemoteMetadataView, RemotePropertiesView},
        remote_nested_edges::RemoteNestedEdges,
        remote_node::RemoteNode,
        remote_path_from_graph::RemotePathFromGraph,
        transport::{
            expect_gid_list, expect_i64_list, expect_node_type_list, expect_optional_event_time,
            expect_optional_event_time_list, expect_optional_typed, expect_tagged_typed_list,
            expect_typed, Transport,
        },
        ClientError,
    },
    model::graph::{
        filtering::{GqlFilter, GqlNodeFilter, NodeFieldCondition, NodeFieldWhere},
        property::Value,
    },
};
use raphtory::errors::GraphError;
use raphtory_api::core::{entities::GID, storage::timeindex::EventTime};
use std::sync::Arc;

/// A handle to a remote collection of nodes on the server.
///
/// Produced by:
/// - `RemoteGraph::nodes()` — all nodes in the current view.
/// - `RemoteNode::neighbours()` / `.in_neighbours()` / `.out_neighbours()` —
///   the neighbours of a specific node.
///
/// Holds the accumulated read expression (`expr`) so terminals like `.id()`
/// and `.count()` evaluate under the full view chain built up on the parent,
/// plus a materialization context (`ctx`) recording the parent graph view and
/// the ordered collection-level ops — used by `.collect()` so materialized
/// `RemoteNode`s evaluate under the same composed view.
#[derive(Clone)]
pub struct RemoteNodes {
    pub path: String,
    pub transport: Arc<dyn Transport>,
    pub expr: Arc<ReadExpr>,
    /// Materialization context: the parent graph view plus the ordered
    /// collection-level ops (view ops, filters) replayed per member by
    /// `.collect()`.
    pub ctx: HandleCtx,
}

impl RemoteNodes {
    /// Construct with an explicit transport, pre-built read expression, and
    /// materialization context.
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

    /// Internal helper: apply a view op to `expr` (narrowing the collection's
    /// own view) and record it in `ctx` in application order, so members
    /// materialized via `.collect()` replay it at the same position relative
    /// to any filters.
    fn with_view_op(&self, op: ViewOp) -> RemoteNodes {
        RemoteNodes {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: Arc::new(op.apply(self.expr.clone())),
            ctx: self.ctx.with_op(HandleOp::View(op)),
        }
    }

    /// Restrict this collection to members whose node type is in the given
    /// list. Unlike view ops (`window`, `layer`, ...), this actually filters
    /// membership — the returned collection has fewer members. Lazy — no RPC.
    ///
    /// Only updates `expr` (the collection's own view), **not** `ctx`
    /// — `typeFilter` is a Nodes-only server operation and applying it to
    /// the parent graph view would be a schema error. Materialized nodes
    /// from `.collect()` don't need the filter propagated because their `id`
    /// already identifies the specific filtered node.
    pub fn type_filter(&self, node_types: Vec<String>) -> RemoteNodes {
        RemoteNodes {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: Arc::new(ReadExpr::TypeFilter {
                input: self.expr.clone(),
                node_types: node_types.into(),
            }),
            ctx: self.ctx.clone(),
        }
    }

    /// Filter this collection by a filter expression. **The filter
    /// propagates**: it applies to the current collection's membership
    /// *and* to downstream traversals from the matching nodes (e.g. their
    /// `.neighbours`, `.edges`). For a narrow-here-only variant, use
    /// `.select(...)`. Recorded in `ctx` so members materialized via
    /// `.collect()` replay it per handle (server field `filter` on `Node`).
    /// Lazy — no RPC.
    pub fn filter(
        &self,
        filter: impl TryInto<GqlFilter, Error = GraphError>,
    ) -> Result<RemoteNodes, ClientError> {
        let filter = Arc::new(filter.try_into()?);
        Ok(RemoteNodes {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: Arc::new(ReadExpr::Filtered {
                input: self.expr.clone(),
                filter: filter.clone(),
            }),
            ctx: self.ctx.with_op(HandleOp::Filter(filter)),
        })
    }

    /// Narrow this collection's membership by a filter expression (node
    /// predicates, graph views, and/or/not combinations — expressions that
    /// test edges are rejected by the server). Unlike `.filter()`, the
    /// filter applies **only at this step** — downstream traversals from
    /// the matching nodes see the unfiltered graph. Lazy — no RPC.
    pub fn select(
        &self,
        filter: impl TryInto<GqlFilter, Error = GraphError>,
    ) -> Result<RemoteNodes, ClientError> {
        let filter = Arc::new(filter.try_into()?);
        Ok(RemoteNodes {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: Arc::new(ReadExpr::SelectNodes {
                input: self.expr.clone(),
                filter,
            }),
            ctx: self.ctx.clone(),
        })
    }

    /// Narrow this collection to the members in positions `start..end` — the
    /// half-open range, as in Python slicing. An `end` at or below `start`
    /// yields an empty collection.
    ///
    /// Unlike every other combinator here this is **not lazy**: a slice is
    /// positional, and only the server knows which members fall in it, so this
    /// fires one RPC to fetch their ids and then pins them by id. The result is
    /// an ordinary `RemoteNodes`, so the columnar reads (`.id()`,
    /// `.properties()`, `.degree()`, …) and traversals all chain off it as
    /// usual, under the same view the slice was taken from.
    ///
    /// Order of application matters, exactly as it does for list slicing:
    /// `.sorted(k).slice(0, n)` is the top `n` by `k`, whereas
    /// `.slice(0, n).sorted(k)` sorts *within* an arbitrary `n`. Likewise
    /// `.filter(f).slice(..)` slices the matching nodes, while
    /// `.slice(..).filter(f)` keeps whichever of the sliced nodes match.
    ///
    /// Slicing is not a snapshot: each call is its own traversal, so concurrent
    /// writes can shift members between slices. Pin a view (`.snapshot_at(..)`,
    /// `.at(..)`) first if that matters.
    pub async fn slice(&self, start: usize, end: usize) -> Result<RemoteNodes, ClientError> {
        // An empty range has no ids to look up, so skip the round trip and go
        // straight to the empty selection.
        let ids = match range_to_page(start, end) {
            None => Vec::new(),
            Some((limit, offset)) => self.id_page(limit, offset, None).await?,
        };
        // Pinned by id rather than position: `select` narrows membership here
        // only, so traversals off the slice still see the whole graph. `ctx` is
        // untouched for the same reason `select` leaves it — a materialized
        // member's own id already identifies it.
        Ok(RemoteNodes {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: Arc::new(ReadExpr::SelectNodes {
                input: self.expr.clone(),
                filter: id_in_filter(ids),
            }),
            ctx: self.ctx.clone(),
        })
    }

    /// Reorder this collection by the given sort keys (lexicographic — ties
    /// on the first key break to the second, etc.). Returns a new
    /// `RemoteNodes` handle carrying the sort; the RPC only fires on a
    /// downstream terminal (`.collect()`, `.count()`, `.id()`, …). Lazy — no
    /// RPC. `ctx` is unchanged: sorting affects only this
    /// collection's iteration order, not the view of materialized nodes.
    pub fn sorted(&self, sort_bys: Vec<NodeSortBy>) -> RemoteNodes {
        RemoteNodes {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: Arc::new(ReadExpr::SortedNodes {
                input: self.expr.clone(),
                sort_bys,
            }),
            ctx: self.ctx.clone(),
        }
    }

    /// Returns the "path from graph" collection of each member's neighbours
    /// (both directions). Lazy — no RPC. Propagates the base graph view so
    /// materialized nodes are correctly rebased.
    ///
    /// Returns a `RemotePathFromGraph` (not `RemoteNodes`) because the server's
    /// `GqlPathFromGraph` type groups results per source node — its terminals
    /// (`ids`, `list`, `count`) return nested / per-source shapes.
    pub fn neighbours(&self) -> RemotePathFromGraph {
        RemotePathFromGraph::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::Neighbours {
                input: self.expr.clone(),
            },
            self.ctx.clone(),
        )
    }

    /// Returns each member's in-neighbours. Lazy — no RPC. See `neighbours`
    /// for why this is a `RemotePathFromGraph`.
    pub fn in_neighbours(&self) -> RemotePathFromGraph {
        RemotePathFromGraph::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::InNeighbours {
                input: self.expr.clone(),
            },
            self.ctx.clone(),
        )
    }

    /// Returns each member's out-neighbours. Lazy — no RPC. See `neighbours`
    /// for why this is a `RemotePathFromGraph`.
    pub fn out_neighbours(&self) -> RemotePathFromGraph {
        RemotePathFromGraph::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::OutNeighbours {
                input: self.expr.clone(),
            },
            self.ctx.clone(),
        )
    }

    /// Returns the nested edges collection of each member's incident edges
    /// (both directions). Lazy — no RPC. Propagates the base graph view so
    /// materialized edges are correctly rebased.
    ///
    /// Returns a `RemoteNestedEdges` (not `RemoteEdges`) because the server's
    /// `GqlNestedEdges` type groups results per source node — its terminals
    /// (`collect`, `list`, `count`) return nested / per-source shapes.
    pub fn edges(&self) -> RemoteNestedEdges {
        RemoteNestedEdges::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::NodeEdges {
                input: self.expr.clone(),
            },
            self.ctx.clone(),
        )
    }

    /// Returns each member's incoming edges. Lazy — no RPC. See `edges` for why
    /// this is a `RemoteNestedEdges`.
    pub fn in_edges(&self) -> RemoteNestedEdges {
        RemoteNestedEdges::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::InEdges {
                input: self.expr.clone(),
            },
            self.ctx.clone(),
        )
    }

    /// Returns each member's outgoing edges. Lazy — no RPC. See `edges` for why
    /// this is a `RemoteNestedEdges`.
    pub fn out_edges(&self) -> RemoteNestedEdges {
        RemoteNestedEdges::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::OutEdges {
                input: self.expr.clone(),
            },
            self.ctx.clone(),
        )
    }

    /// Columnar accessor: each node's id — mirrors the local `Nodes.id`,
    /// including the type: string ids for string-indexed graphs, integers
    /// for integer-indexed ones. Fires one RPC.
    pub async fn id(&self) -> Result<Vec<GID>, ClientError> {
        let op = Op::Read(ReadExpr::Ids {
            input: self.expr.clone(),
            page: None,
        });
        expect_gid_list(self.transport.execute(&op).await?, "id")
    }

    /// The ids of one page of members. Fires one RPC. Internal: `page()` and
    /// `slice()` both need to know which members fall in a range.
    async fn id_page(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Result<Vec<GID>, ClientError> {
        node_collection::id_page(&self.transport, &self.expr, limit, offset, page_index).await
    }

    /// Columnar accessor: each node's name — mirrors the local `Nodes.name`.
    /// Fires one RPC.
    pub async fn name(&self) -> Result<Vec<String>, ClientError> {
        let op = Op::Read(ReadExpr::CollectionNames {
            input: self.expr.clone(),
        });
        expect_tagged_typed_list(self.transport.execute(&op).await?, "name")
    }

    /// Columnar accessor: each node's type (`None` when unset) — mirrors the
    /// local `Nodes.node_type`. Fires one RPC.
    pub async fn node_type(&self) -> Result<Vec<Option<String>>, ClientError> {
        let op = Op::Read(ReadExpr::CollectionNodeTypes {
            input: self.expr.clone(),
        });
        expect_node_type_list(self.transport.execute(&op).await?, "nodeType")
    }

    /// Columnar accessor: each node's earliest event time — mirrors the local
    /// `Nodes.earliest_time`. Fires one RPC.
    pub async fn earliest_time(&self) -> Result<Vec<Option<EventTime>>, ClientError> {
        let op = Op::Read(ReadExpr::CollectionEarliestTime {
            input: self.expr.clone(),
        });
        expect_optional_event_time_list(
            self.transport.execute(&op).await?,
            "earliestTime",
            "earliestTime",
        )
    }

    /// Columnar accessor: each node's latest event time — mirrors the local
    /// `Nodes.latest_time`. Fires one RPC.
    pub async fn latest_time(&self) -> Result<Vec<Option<EventTime>>, ClientError> {
        let op = Op::Read(ReadExpr::CollectionLatestTime {
            input: self.expr.clone(),
        });
        expect_optional_event_time_list(
            self.transport.execute(&op).await?,
            "latestTime",
            "latestTime",
        )
    }

    /// The non-temporal metadata of this collection as a columnar view —
    /// mirrors the local `Nodes.metadata`. Lazy — no RPC (each accessor on the
    /// returned view fires its own RPC).
    pub fn metadata(&self) -> RemoteMetadataView {
        RemoteMetadataView::with_expr(
            self.path.clone(),
            self.transport.clone(),
            self.expr.clone(),
            self.ctx.clone(),
            false,
        )
    }

    /// The properties of this collection as a columnar view — mirrors the local
    /// `Nodes.properties`. Lazy — no RPC.
    pub fn properties(&self) -> RemotePropertiesView {
        RemotePropertiesView::with_expr(
            self.path.clone(),
            self.transport.clone(),
            self.expr.clone(),
            self.ctx.clone(),
            false,
        )
    }

    /// Terminal: the per-node degree (number of incident edges) of every node
    /// in this collection, in order — a flat `Vec<i64>`. Fires one RPC.
    pub async fn degree(&self) -> Result<Vec<i64>, ClientError> {
        let op = Op::Read(ReadExpr::CollectionDegree {
            input: self.expr.clone(),
        });
        expect_i64_list(self.transport.execute(&op).await?, "degree")
    }

    /// Terminal: the per-node in-degree of every node in this collection, in
    /// order — a flat `Vec<i64>`. Fires one RPC.
    pub async fn in_degree(&self) -> Result<Vec<i64>, ClientError> {
        let op = Op::Read(ReadExpr::CollectionInDegree {
            input: self.expr.clone(),
        });
        expect_i64_list(self.transport.execute(&op).await?, "inDegree")
    }

    /// Terminal: the per-node out-degree of every node in this collection, in
    /// order — a flat `Vec<i64>`. Fires one RPC.
    pub async fn out_degree(&self) -> Result<Vec<i64>, ClientError> {
        let op = Op::Read(ReadExpr::CollectionOutDegree {
            input: self.expr.clone(),
        });
        expect_i64_list(self.transport.execute(&op).await?, "outDegree")
    }

    /// Terminal: the per-node count of incident edge updates of every node in
    /// this collection, in order — a flat `Vec<i64>`. Fires one RPC.
    pub async fn edge_history_count(&self) -> Result<Vec<i64>, ClientError> {
        let op = Op::Read(ReadExpr::CollectionEdgeHistoryCount {
            input: self.expr.clone(),
        });
        expect_i64_list(self.transport.execute(&op).await?, "edgeHistoryCount")
    }

    /// Terminal: the number of nodes in this collection. Fires one RPC.
    pub async fn count(&self) -> Result<i64, ClientError> {
        let op = Op::Read(ReadExpr::Count {
            input: self.expr.clone(),
        });
        expect_typed(self.transport.execute(&op).await?, "count")
    }

    /// Terminal: whether this view contains a layer named `name`. Fires one RPC.
    pub async fn has_layer(&self, name: impl ToString) -> Result<bool, ClientError> {
        let op = Op::Read(ReadExpr::HasLayer {
            input: self.expr.clone(),
            name: name.to_string(),
        });
        expect_typed(self.transport.execute(&op).await?, "hasLayer")
    }

    /// Terminal: the size of the window covered by this view (`end - start`),
    /// or `None` for an unbounded view. Fires one RPC.
    pub async fn window_size(&self) -> Result<Option<i64>, ClientError> {
        let op = Op::Read(ReadExpr::WindowSize {
            input: self.expr.clone(),
        });
        expect_optional_typed(self.transport.execute(&op).await?, "windowSize")
    }

    /// Terminal: view start bound for this collection — `None` if unbounded.
    /// Fires one RPC.
    pub async fn start(&self) -> Result<Option<EventTime>, ClientError> {
        let op = Op::Read(ReadExpr::Start {
            input: self.expr.clone(),
        });
        expect_optional_event_time(self.transport.execute(&op).await?, "start")
    }

    /// Terminal: view end bound for this collection — `None` if unbounded.
    /// Fires one RPC.
    pub async fn end(&self) -> Result<Option<EventTime>, ClientError> {
        let op = Op::Read(ReadExpr::End {
            input: self.expr.clone(),
        });
        expect_optional_event_time(self.transport.execute(&op).await?, "end")
    }

    /// Materialize this collection as a `Vec<RemoteNode>`. Fires one RPC to
    /// fetch the ids; each returned node anchors on the parent graph view and
    /// replays the collection-level ops (view ops, filters) in application
    /// order — so terminals on returned nodes evaluate under the same
    /// composed view as collection-level reads.
    pub async fn collect(&self) -> Result<Vec<RemoteNode>, ClientError> {
        let ids = self.id().await?;
        Ok(node_collection::materialize(
            &self.path,
            &self.transport,
            &self.ctx,
            ids,
        ))
    }

    /// Materialize ONE PAGE of this collection as a `Vec<RemoteNode>` — the
    /// bounded counterpart of `collect()`, with the same rebasing. At most
    /// `limit` handles, starting `page_index * limit + offset` members in;
    /// both `offset` and `page_index` default to 0 server-side. Fires one RPC.
    ///
    /// A result shorter than `limit` means the collection is exhausted. Prefer
    /// this to `collect()` on a large collection, and use it where the server
    /// runs with bulk list endpoints disabled — `collect()` is rejected there.
    ///
    /// Returns handles, so a read on each fires its own RPC. For a columnar
    /// read across the page in a single request, use `slice()` instead.
    pub async fn page(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Result<Vec<RemoteNode>, ClientError> {
        let ids = self.id_page(limit, offset, page_index).await?;
        Ok(node_collection::materialize(
            &self.path,
            &self.transport,
            &self.ctx,
            ids,
        ))
    }
}


/// A filter matching exactly the given node ids.
///
/// The server resolves `id.isIn` straight to a VID set (`NodeIdFilterOp::domain`),
/// so on a `Nodes` collection this narrows the iteration domain rather than
/// scanning. Ids keep their JSON type — string ids stay strings, integer-indexed
/// graphs keep integers — so `isIn` compares like against like.
pub(crate) fn id_in_filter(ids: Vec<GID>) -> Arc<GqlFilter> {
    let values = ids
        .into_iter()
        .map(|id| match id {
            GID::U64(n) => Value::U64(n),
            GID::Str(s) => Value::Str(s),
        })
        .collect();
    Arc::new(GqlFilter::Node(GqlNodeFilter::Id(NodeFieldWhere {
        where_: NodeFieldCondition::IsIn(Value::List(values)),
    })))
}

/// The half-open range `start..end` as `(limit, offset)` page arguments, or
/// `None` when the range is empty — the caller can then skip the round trip.
pub(crate) fn range_to_page(start: usize, end: usize) -> Option<(usize, Option<usize>)> {
    let limit = end.saturating_sub(start);
    if limit == 0 {
        None
    } else {
        Some((limit, Some(start)))
    }
}

remote_view_ops!(RemoteNodes);
