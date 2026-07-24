use crate::{
    client::{
        op::{Op, ReadExpr},
        remote_collection_metadata::{RemoteMetadataView, RemotePropertiesView},
        remote_edges::RemoteEdges,
        remote_graph::{
            expect_bool, expect_i64, expect_i64_list, expect_optional_event_time,
            expect_optional_event_time_list, expect_optional_i64, expect_optional_string_list,
            expect_string_list,
        },
        remote_history::{RemoteEventTime, RemoteHistory},
        remote_node::RemoteNode,
        transport::Transport,
        ClientError,
    },
    model::graph::filtering::GqlNodeFilter,
};
use std::sync::Arc;

/// A handle to a "path from node" collection on the server — the nodes
/// reachable one hop from a specific node in a given direction. Produced by:
/// - `RemoteNode::neighbours()` — both directions
/// - `RemoteNode::in_neighbours()`
/// - `RemoteNode::out_neighbours()`
///
/// Distinct from `RemoteNodes` because the server-side type
/// (`GqlPathFromNode`) exposes a **subset** of `GqlNodes`' fields:
/// - **Missing**: `sorted`, `default_layer` — these methods are simply not
///   available; the server has no field for them here.
/// - **Present**: view chain (`window`, `at`, `layer`, ...), `type_filter`,
///   and terminals (`ids`, `count`, `list`, `start`, `end`).
///
/// Structurally identical to `RemoteNodes` — same `expr` + `base_graph`
/// fields, same view-op wiring — but the type distinction is what gives
/// clients compile-time protection from calling unsupported methods.
#[derive(Clone)]
pub struct RemotePathFromNode {
    pub path: String,
    pub transport: Arc<dyn Transport>,
    pub expr: ReadExpr,
    /// The parent graph view — see `RemoteNodes` for details.
    pub base_graph: ReadExpr,
}

impl RemotePathFromNode {
    /// Construct with an explicit transport, pre-built read expression, and
    /// parent graph view.
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

    fn with_view_op<F>(&self, wrap: F) -> RemotePathFromNode
    where
        F: Fn(ReadExpr) -> ReadExpr,
    {
        RemotePathFromNode {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: wrap(self.expr.clone()),
            base_graph: wrap(self.base_graph.clone()),
        }
    }

    /// Time-window this collection. Lazy — no RPC.
    pub fn window(&self, start: i64, end: i64) -> RemotePathFromNode {
        self.with_view_op(|input| ReadExpr::Window {
            input: Box::new(input),
            start,
            end,
        })
    }

    /// Restrict to a single named layer. Lazy — no RPC.
    pub fn layer(&self, name: impl ToString) -> RemotePathFromNode {
        let name = name.to_string();
        self.with_view_op(|input| ReadExpr::Layer {
            input: Box::new(input),
            name: name.clone(),
        })
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    pub fn at(&self, time: i64) -> RemotePathFromNode {
        self.with_view_op(|input| ReadExpr::At {
            input: Box::new(input),
            time,
        })
    }

    /// Restrict to events strictly before the given time. Lazy — no RPC.
    pub fn before(&self, time: i64) -> RemotePathFromNode {
        self.with_view_op(|input| ReadExpr::Before {
            input: Box::new(input),
            time,
        })
    }

    /// Restrict to events strictly after the given time. Lazy — no RPC.
    pub fn after(&self, time: i64) -> RemotePathFromNode {
        self.with_view_op(|input| ReadExpr::After {
            input: Box::new(input),
            time,
        })
    }

    /// Latest state. Lazy — no RPC.
    pub fn latest(&self) -> RemotePathFromNode {
        self.with_view_op(|input| ReadExpr::Latest {
            input: Box::new(input),
        })
    }

    /// Snapshot at the latest time. Lazy — no RPC.
    pub fn snapshot_latest(&self) -> RemotePathFromNode {
        self.with_view_op(|input| ReadExpr::SnapshotLatest {
            input: Box::new(input),
        })
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    pub fn snapshot_at(&self, time: i64) -> RemotePathFromNode {
        self.with_view_op(|input| ReadExpr::SnapshotAt {
            input: Box::new(input),
            time,
        })
    }

    /// Exclude a specific layer. Lazy — no RPC.
    pub fn exclude_layer(&self, name: impl ToString) -> RemotePathFromNode {
        let name = name.to_string();
        self.with_view_op(|input| ReadExpr::ExcludeLayer {
            input: Box::new(input),
            name: name.clone(),
        })
    }

    /// Shrink both start and end of the current window. Lazy — no RPC.
    pub fn shrink_window(&self, start: i64, end: i64) -> RemotePathFromNode {
        self.with_view_op(|input| ReadExpr::ShrinkWindow {
            input: Box::new(input),
            start,
            end,
        })
    }

    /// Shrink the start of the current window. Lazy — no RPC.
    pub fn shrink_start(&self, start: i64) -> RemotePathFromNode {
        self.with_view_op(|input| ReadExpr::ShrinkStart {
            input: Box::new(input),
            start,
        })
    }

    /// Shrink the end of the current window. Lazy — no RPC.
    pub fn shrink_end(&self, end: i64) -> RemotePathFromNode {
        self.with_view_op(|input| ReadExpr::ShrinkEnd {
            input: Box::new(input),
            end,
        })
    }

    /// Restrict to the default layer. Lazy — no RPC.
    pub fn default_layer(&self) -> RemotePathFromNode {
        self.with_view_op(|input| ReadExpr::DefaultLayer {
            input: Box::new(input),
        })
    }

    /// Restrict to the given set of layers. Lazy — no RPC.
    pub fn layers(&self, names: Vec<String>) -> RemotePathFromNode {
        self.with_view_op(|input| ReadExpr::Layers {
            input: Box::new(input),
            names: names.clone(),
        })
    }

    /// Exclude the given set of layers. Lazy — no RPC.
    pub fn exclude_layers(&self, names: Vec<String>) -> RemotePathFromNode {
        self.with_view_op(|input| ReadExpr::ExcludeLayers {
            input: Box::new(input),
            names: names.clone(),
        })
    }

    /// Restrict to the given set of valid layers. Lazy — no RPC.
    pub fn valid_layers(&self, names: Vec<String>) -> RemotePathFromNode {
        self.with_view_op(|input| ReadExpr::ValidLayers {
            input: Box::new(input),
            names: names.clone(),
        })
    }

    /// Exclude a specific valid layer from the view. Lazy — no RPC.
    pub fn exclude_valid_layer(&self, name: impl ToString) -> RemotePathFromNode {
        let name = name.to_string();
        self.with_view_op(|input| ReadExpr::ExcludeValidLayer {
            input: Box::new(input),
            name: name.clone(),
        })
    }

    /// Exclude the given set of valid layers from the view. Lazy — no RPC.
    pub fn exclude_valid_layers(&self, names: Vec<String>) -> RemotePathFromNode {
        self.with_view_op(|input| ReadExpr::ExcludeValidLayers {
            input: Box::new(input),
            names: names.clone(),
        })
    }

    /// Restrict this collection to members whose node type is in the given
    /// list. Filters membership — the returned collection has fewer members.
    /// Lazy — no RPC. Only updates `expr`; see `RemoteNodes::type_filter`
    /// for reasoning.
    pub fn type_filter(&self, node_types: Vec<String>) -> RemotePathFromNode {
        RemotePathFromNode {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: ReadExpr::TypeFilter {
                input: Box::new(self.expr.clone()),
                node_types,
            },
            base_graph: self.base_graph.clone(),
        }
    }

    /// Filter this collection by a node filter. **Propagates** to downstream
    /// traversals from the matching nodes. Mirrors the local
    /// `PathFromNode.filter(FilterExpr)`. Wraps only `expr`. Lazy — no RPC.
    pub fn filter(&self, filter: GqlNodeFilter) -> RemotePathFromNode {
        RemotePathFromNode {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: ReadExpr::FilterNodes {
                input: Box::new(self.expr.clone()),
                filter,
            },
            base_graph: self.base_graph.clone(),
        }
    }

    /// Narrow this collection's membership by a node filter — applies only at
    /// this step; downstream traversals see the unfiltered graph. Server-only
    /// (`select` has no local `PathFromNode` equivalent). Lazy — no RPC.
    pub fn select(&self, filter: GqlNodeFilter) -> RemotePathFromNode {
        RemotePathFromNode {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: ReadExpr::SelectNodes {
                input: Box::new(self.expr.clone()),
                filter,
            },
            base_graph: self.base_graph.clone(),
        }
    }

    /// Traverse one further hop to the neighbours (both directions) of this
    /// path, as a flat `RemotePathFromNode`. Lazy — no RPC.
    pub fn neighbours(&self) -> RemotePathFromNode {
        RemotePathFromNode::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::Neighbours {
                input: Box::new(self.expr.clone()),
            },
            self.base_graph.clone(),
        )
    }

    /// Traverse one further hop to the in-neighbours of this path, as a flat
    /// `RemotePathFromNode`. Lazy — no RPC.
    pub fn in_neighbours(&self) -> RemotePathFromNode {
        RemotePathFromNode::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::InNeighbours {
                input: Box::new(self.expr.clone()),
            },
            self.base_graph.clone(),
        )
    }

    /// Traverse one further hop to the out-neighbours of this path, as a flat
    /// `RemotePathFromNode`. Lazy — no RPC.
    pub fn out_neighbours(&self) -> RemotePathFromNode {
        RemotePathFromNode::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::OutNeighbours {
                input: Box::new(self.expr.clone()),
            },
            self.base_graph.clone(),
        )
    }

    /// Returns the incident edges (both directions) of this path, as a flat
    /// `RemoteEdges` collection. Lazy — no RPC.
    pub fn edges(&self) -> RemoteEdges {
        RemoteEdges::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::NodeEdges {
                input: Box::new(self.expr.clone()),
            },
            self.base_graph.clone(),
        )
    }

    /// Returns the incoming edges of this path, as a flat `RemoteEdges`
    /// collection. Lazy — no RPC.
    pub fn in_edges(&self) -> RemoteEdges {
        RemoteEdges::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::InEdges {
                input: Box::new(self.expr.clone()),
            },
            self.base_graph.clone(),
        )
    }

    /// Returns the outgoing edges of this path, as a flat `RemoteEdges`
    /// collection. Lazy — no RPC.
    pub fn out_edges(&self) -> RemoteEdges {
        RemoteEdges::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::OutEdges {
                input: Box::new(self.expr.clone()),
            },
            self.base_graph.clone(),
        )
    }

    /// Terminal: the list of node ids in this collection. Fires one RPC.
    pub async fn ids(&self) -> Result<Vec<String>, ClientError> {
        let op = Op::Read(ReadExpr::Ids {
            input: Box::new(self.expr.clone()),
        });
        expect_string_list(self.transport.execute(&op).await?, "ids")
    }

    /// Columnar accessor: each node's id — mirrors the local `PathFromNode.id`.
    /// Fires one RPC.
    pub async fn id(&self) -> Result<Vec<String>, ClientError> {
        let op = Op::Read(ReadExpr::Ids {
            input: Box::new(self.expr.clone()),
        });
        expect_string_list(self.transport.execute(&op).await?, "id")
    }

    /// Columnar accessor: each node's name — mirrors the local
    /// `PathFromNode.name`. Fires one RPC.
    pub async fn name(&self) -> Result<Vec<String>, ClientError> {
        let op = Op::Read(ReadExpr::CollectionNames {
            input: Box::new(self.expr.clone()),
        });
        expect_string_list(self.transport.execute(&op).await?, "name")
    }

    /// Columnar accessor: each node's type (`None` when unset) — mirrors the
    /// local `PathFromNode.node_type`. Fires one RPC.
    pub async fn node_type(&self) -> Result<Vec<Option<String>>, ClientError> {
        let op = Op::Read(ReadExpr::CollectionNodeTypes {
            input: Box::new(self.expr.clone()),
        });
        expect_optional_string_list(self.transport.execute(&op).await?, "nodeType")
    }

    /// Columnar accessor: each node's earliest event time — mirrors the local
    /// `PathFromNode.earliest_time`. Fires one RPC.
    pub async fn earliest_time(&self) -> Result<Vec<Option<RemoteEventTime>>, ClientError> {
        let op = Op::Read(ReadExpr::CollectionEarliestTime {
            input: Box::new(self.expr.clone()),
        });
        expect_optional_event_time_list(self.transport.execute(&op).await?, "earliestTime")
    }

    /// Columnar accessor: each node's latest event time — mirrors the local
    /// `PathFromNode.latest_time`. Fires one RPC.
    pub async fn latest_time(&self) -> Result<Vec<Option<RemoteEventTime>>, ClientError> {
        let op = Op::Read(ReadExpr::CollectionLatestTime {
            input: Box::new(self.expr.clone()),
        });
        expect_optional_event_time_list(self.transport.execute(&op).await?, "latestTime")
    }

    /// The non-temporal metadata of this path as a columnar view — mirrors the
    /// local `PathFromNode.metadata`. Lazy — no RPC.
    pub fn metadata(&self) -> RemoteMetadataView {
        RemoteMetadataView::with_expr(
            self.path.clone(),
            self.transport.clone(),
            self.expr.clone(),
            self.base_graph.clone(),
            false,
        )
    }

    /// The properties of this path as a columnar view — mirrors the local
    /// `PathFromNode.properties`. Lazy — no RPC.
    pub fn properties(&self) -> RemotePropertiesView {
        RemotePropertiesView::with_expr(
            self.path.clone(),
            self.transport.clone(),
            self.expr.clone(),
            self.base_graph.clone(),
            false,
        )
    }

    /// Terminal: the per-node degree (number of incident edges) of every node
    /// in this path, in order — a flat `Vec<i64>`. Fires one RPC.
    pub async fn degree(&self) -> Result<Vec<i64>, ClientError> {
        let op = Op::Read(ReadExpr::CollectionDegree {
            input: Box::new(self.expr.clone()),
        });
        expect_i64_list(self.transport.execute(&op).await?, "degree")
    }

    /// Terminal: the per-node in-degree of every node in this path, in order —
    /// a flat `Vec<i64>`. Fires one RPC.
    pub async fn in_degree(&self) -> Result<Vec<i64>, ClientError> {
        let op = Op::Read(ReadExpr::CollectionInDegree {
            input: Box::new(self.expr.clone()),
        });
        expect_i64_list(self.transport.execute(&op).await?, "inDegree")
    }

    /// Terminal: the per-node out-degree of every node in this path, in order —
    /// a flat `Vec<i64>`. Fires one RPC.
    pub async fn out_degree(&self) -> Result<Vec<i64>, ClientError> {
        let op = Op::Read(ReadExpr::CollectionOutDegree {
            input: Box::new(self.expr.clone()),
        });
        expect_i64_list(self.transport.execute(&op).await?, "outDegree")
    }

    /// Terminal: the per-node count of incident edge updates of every node in
    /// this path, in order — a flat `Vec<i64>`. Fires one RPC.
    pub async fn edge_history_count(&self) -> Result<Vec<i64>, ClientError> {
        let op = Op::Read(ReadExpr::CollectionEdgeHistoryCount {
            input: Box::new(self.expr.clone()),
        });
        expect_i64_list(self.transport.execute(&op).await?, "edgeHistoryCount")
    }

    /// Terminal: the number of nodes in this collection. Fires one RPC.
    pub async fn count(&self) -> Result<i64, ClientError> {
        let op = Op::Read(ReadExpr::Count {
            input: Box::new(self.expr.clone()),
        });
        expect_i64(self.transport.execute(&op).await?, "count")
    }

    /// Terminal: whether this view contains a layer named `name`. Fires one RPC.
    pub async fn has_layer(&self, name: impl ToString) -> Result<bool, ClientError> {
        let op = Op::Read(ReadExpr::HasLayer {
            input: Box::new(self.expr.clone()),
            name: name.to_string(),
        });
        expect_bool(self.transport.execute(&op).await?, "hasLayer")
    }

    /// Terminal: the size of the window covered by this view (`end - start`),
    /// or `None` for an unbounded view. Fires one RPC.
    pub async fn window_size(&self) -> Result<Option<i64>, ClientError> {
        let op = Op::Read(ReadExpr::WindowSize {
            input: Box::new(self.expr.clone()),
        });
        expect_optional_i64(self.transport.execute(&op).await?, "windowSize")
    }

    /// Returns a single combined event history for all nodes reachable from
    /// the source in this view — a `RemoteHistory` container. Lazy — no RPC.
    pub fn combined_history(&self) -> RemoteHistory {
        RemoteHistory::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::CombinedHistory {
                input: Box::new(self.expr.clone()),
            },
            self.base_graph.clone(),
        )
    }

    /// Terminal: view start bound for this collection — `None` if unbounded.
    /// Fires one RPC.
    pub async fn start(&self) -> Result<Option<RemoteEventTime>, ClientError> {
        let op = Op::Read(ReadExpr::Start {
            input: Box::new(self.expr.clone()),
        });
        expect_optional_event_time(self.transport.execute(&op).await?, "start")
    }

    /// Terminal: view end bound for this collection — `None` if unbounded.
    /// Fires one RPC.
    pub async fn end(&self) -> Result<Option<RemoteEventTime>, ClientError> {
        let op = Op::Read(ReadExpr::End {
            input: Box::new(self.expr.clone()),
        });
        expect_optional_event_time(self.transport.execute(&op).await?, "end")
    }

    /// Materialize as `Vec<RemoteNode>`. Fires one RPC.
    pub async fn collect(&self) -> Result<Vec<RemoteNode>, ClientError> {
        let ids = self.ids().await?;
        Ok(ids
            .into_iter()
            .map(|id| {
                RemoteNode::with_expr(
                    self.path.clone(),
                    id.clone(),
                    self.transport.clone(),
                    ReadExpr::Node {
                        input: Box::new(self.base_graph.clone()),
                        id,
                    },
                    self.base_graph.clone(),
                )
            })
            .collect())
    }
}
