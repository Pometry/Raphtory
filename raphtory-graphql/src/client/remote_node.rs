use crate::{
    client::{
        op::{
            input_time_from_parts, AddNodeMetadata as AddNodeMetadataOp,
            AddNodeUpdates as AddNodeUpdatesOp, HandleCtx, HandleOp, InputTime, Op, ReadExpr,
            SetNodeType as SetNodeTypeOp, UpdateNodeMetadata as UpdateNodeMetadataOp, WriteOp,
        },
        remote_edges::RemoteEdges,
        remote_graph::{
            expect_bool, expect_i64, expect_optional_event_time, expect_optional_i64,
            expect_optional_string, expect_string,
        },
        remote_history::{RemoteEventTime, RemoteHistory},
        remote_metadata::{RemoteMetadata, RemoteProperties},
        remote_nodes::RemoteNodes,
        remote_path_from_node::RemotePathFromNode,
        transport::Transport,
        ClientError,
    },
    model::graph::filtering::GqlNodeFilter,
};
use raphtory_api::core::{
    entities::properties::prop::Prop, storage::timeindex::AsTime, utils::time::IntoTime,
};
use std::{collections::HashMap, sync::Arc};

/// A handle to a remote node on the server.
///
/// Holds the accumulated read expression (`expr`) so that terminals like
/// `degree()` evaluate under the full view chain built up on the parent
/// `RemoteGraph`, plus a materialization context (`ctx`) recording the parent
/// graph view and the entity-level ops applied to this handle — passed to
/// child collections (`neighbours`, etc.) so members they materialize via
/// `.collect()` replay the same ops and evaluate under the same view chain.
#[derive(Clone)]
pub struct RemoteNode {
    pub path: String,
    pub id: String,
    pub transport: Arc<dyn Transport>,
    pub expr: ReadExpr,
    /// Materialization context — inherited by child collections so their
    /// `.collect()` handles replay this node's ops (view ops, filters).
    pub ctx: HandleCtx,
}

impl RemoteNode {
    /// Construct with an explicit transport, pre-built read expression, and
    /// materialization context.
    pub fn with_expr(
        path: String,
        id: String,
        transport: Arc<dyn Transport>,
        expr: ReadExpr,
        ctx: HandleCtx,
    ) -> Self {
        Self {
            path,
            id,
            transport,
            expr,
            ctx,
        }
    }

    /// Internal helper: apply a view op to `expr` (narrowing the node's own
    /// view) and record it in `ctx` so descendants navigated via
    /// `.neighbours`, `.edges`, etc. replay it when materializing handles.
    fn with_view_op<F>(&self, wrap: F) -> RemoteNode
    where
        F: Fn(ReadExpr) -> ReadExpr + Send + Sync + 'static,
    {
        let wrap = Arc::new(wrap);
        RemoteNode {
            path: self.path.clone(),
            id: self.id.clone(),
            transport: self.transport.clone(),
            expr: wrap(self.expr.clone()),
            ctx: self.ctx.with_op(HandleOp::View(wrap)),
        }
    }

    /// Time-window this node. Lazy — no RPC.
    pub fn window(&self, start: InputTime, end: InputTime) -> RemoteNode {
        self.with_view_op(move |input| ReadExpr::Window {
            input: Box::new(input),
            start,
            end,
        })
    }

    /// Return a filtered view of this node — mirrors the local
    /// `Node.filter(FilterExpr)`. Wraps `expr` (the server field
    /// `filter(expr:)` on `Node`) and records the filter in `ctx` so
    /// descendants materialized through this node replay it. Lazy — no RPC.
    pub fn filter(&self, filter: GqlNodeFilter) -> RemoteNode {
        RemoteNode {
            path: self.path.clone(),
            id: self.id.clone(),
            transport: self.transport.clone(),
            expr: ReadExpr::FilterNodes {
                input: Box::new(self.expr.clone()),
                filter: filter.clone(),
            },
            ctx: self.ctx.with_op(HandleOp::NodeFilter(filter)),
        }
    }

    /// Restrict to a single named layer. Lazy — no RPC.
    pub fn layer(&self, name: impl ToString) -> RemoteNode {
        let name = name.to_string();
        self.with_view_op(move |input| ReadExpr::Layer {
            input: Box::new(input),
            name: name.clone(),
        })
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    pub fn at(&self, time: InputTime) -> RemoteNode {
        self.with_view_op(move |input| ReadExpr::At {
            input: Box::new(input),
            time,
        })
    }

    /// Restrict to events strictly before the given time. Lazy — no RPC.
    pub fn before(&self, time: InputTime) -> RemoteNode {
        self.with_view_op(move |input| ReadExpr::Before {
            input: Box::new(input),
            time,
        })
    }

    /// Restrict to events strictly after the given time (exclusive). Lazy — no RPC.
    pub fn after(&self, time: InputTime) -> RemoteNode {
        self.with_view_op(move |input| ReadExpr::After {
            input: Box::new(input),
            time,
        })
    }

    /// Latest state. Lazy — no RPC.
    pub fn latest(&self) -> RemoteNode {
        self.with_view_op(move |input| ReadExpr::Latest {
            input: Box::new(input),
        })
    }

    /// Snapshot at the latest time. Lazy — no RPC.
    pub fn snapshot_latest(&self) -> RemoteNode {
        self.with_view_op(move |input| ReadExpr::SnapshotLatest {
            input: Box::new(input),
        })
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    pub fn snapshot_at(&self, time: InputTime) -> RemoteNode {
        self.with_view_op(move |input| ReadExpr::SnapshotAt {
            input: Box::new(input),
            time,
        })
    }

    /// Exclude a specific layer from the view. Lazy — no RPC.
    pub fn exclude_layer(&self, name: impl ToString) -> RemoteNode {
        let name = name.to_string();
        self.with_view_op(move |input| ReadExpr::ExcludeLayer {
            input: Box::new(input),
            name: name.clone(),
        })
    }

    /// Shrink both start and end of the current window. Lazy — no RPC.
    pub fn shrink_window(&self, start: InputTime, end: InputTime) -> RemoteNode {
        self.with_view_op(move |input| ReadExpr::ShrinkWindow {
            input: Box::new(input),
            start,
            end,
        })
    }

    /// Shrink the start of the current window. Lazy — no RPC.
    pub fn shrink_start(&self, start: InputTime) -> RemoteNode {
        self.with_view_op(move |input| ReadExpr::ShrinkStart {
            input: Box::new(input),
            start,
        })
    }

    /// Shrink the end of the current window. Lazy — no RPC.
    pub fn shrink_end(&self, end: InputTime) -> RemoteNode {
        self.with_view_op(move |input| ReadExpr::ShrinkEnd {
            input: Box::new(input),
            end,
        })
    }

    /// Restrict to the default layer. Lazy — no RPC.
    pub fn default_layer(&self) -> RemoteNode {
        self.with_view_op(move |input| ReadExpr::DefaultLayer {
            input: Box::new(input),
        })
    }

    /// Restrict to the given set of layers. Lazy — no RPC.
    pub fn layers(&self, names: Vec<String>) -> RemoteNode {
        self.with_view_op(move |input| ReadExpr::Layers {
            input: Box::new(input),
            names: names.clone(),
        })
    }

    /// Exclude the given set of layers from the view. Lazy — no RPC.
    pub fn exclude_layers(&self, names: Vec<String>) -> RemoteNode {
        self.with_view_op(move |input| ReadExpr::ExcludeLayers {
            input: Box::new(input),
            names: names.clone(),
        })
    }

    /// Restrict to the given set of valid layers. Lazy — no RPC.
    pub fn valid_layers(&self, names: Vec<String>) -> RemoteNode {
        self.with_view_op(move |input| ReadExpr::ValidLayers {
            input: Box::new(input),
            names: names.clone(),
        })
    }

    /// Exclude a specific valid layer from the view. Lazy — no RPC.
    pub fn exclude_valid_layer(&self, name: impl ToString) -> RemoteNode {
        let name = name.to_string();
        self.with_view_op(move |input| ReadExpr::ExcludeValidLayer {
            input: Box::new(input),
            name: name.clone(),
        })
    }

    /// Exclude the given set of valid layers from the view. Lazy — no RPC.
    pub fn exclude_valid_layers(&self, names: Vec<String>) -> RemoteNode {
        self.with_view_op(move |input| ReadExpr::ExcludeValidLayers {
            input: Box::new(input),
            names: names.clone(),
        })
    }

    /// Terminal: node degree (in + out, deduplicated). Fires one RPC.
    pub async fn degree(&self) -> Result<i64, ClientError> {
        let op = Op::Read(ReadExpr::Degree {
            input: Box::new(self.expr.clone()),
        });
        expect_i64(self.transport.execute(&op).await?, "degree")
    }

    /// Terminal: node in-degree. Fires one RPC.
    pub async fn in_degree(&self) -> Result<i64, ClientError> {
        let op = Op::Read(ReadExpr::InDegree {
            input: Box::new(self.expr.clone()),
        });
        expect_i64(self.transport.execute(&op).await?, "inDegree")
    }

    /// Terminal: node out-degree. Fires one RPC.
    pub async fn out_degree(&self) -> Result<i64, ClientError> {
        let op = Op::Read(ReadExpr::OutDegree {
            input: Box::new(self.expr.clone()),
        });
        expect_i64(self.transport.execute(&op).await?, "outDegree")
    }

    /// Terminal: node name. Fires one RPC.
    pub async fn name(&self) -> Result<String, ClientError> {
        let op = Op::Read(ReadExpr::Name {
            input: Box::new(self.expr.clone()),
        });
        expect_string(self.transport.execute(&op).await?, "name")
    }

    /// Terminal: earliest event timestamp on this node under the current view.
    /// Returns `None` if the node has no events in the view. Fires one RPC.
    pub async fn earliest_time(&self) -> Result<Option<RemoteEventTime>, ClientError> {
        let op = Op::Read(ReadExpr::EarliestTime {
            input: Box::new(self.expr.clone()),
        });
        expect_optional_event_time(self.transport.execute(&op).await?, "earliestTime")
    }

    /// Terminal: latest event timestamp on this node under the current view.
    /// Fires one RPC.
    pub async fn latest_time(&self) -> Result<Option<RemoteEventTime>, ClientError> {
        let op = Op::Read(ReadExpr::LatestTime {
            input: Box::new(self.expr.clone()),
        });
        expect_optional_event_time(self.transport.execute(&op).await?, "latestTime")
    }

    /// Terminal: view start bound as seen by this node. Fires one RPC.
    pub async fn start(&self) -> Result<Option<RemoteEventTime>, ClientError> {
        let op = Op::Read(ReadExpr::Start {
            input: Box::new(self.expr.clone()),
        });
        expect_optional_event_time(self.transport.execute(&op).await?, "start")
    }

    /// Terminal: view end bound as seen by this node. Fires one RPC.
    pub async fn end(&self) -> Result<Option<RemoteEventTime>, ClientError> {
        let op = Op::Read(ReadExpr::End {
            input: Box::new(self.expr.clone()),
        });
        expect_optional_event_time(self.transport.execute(&op).await?, "end")
    }

    /// Terminal: the node's id (as a string, even if the graph uses int GIDs).
    /// Fires one RPC.
    pub async fn id(&self) -> Result<String, ClientError> {
        let op = Op::Read(ReadExpr::Id {
            input: Box::new(self.expr.clone()),
        });
        expect_string(self.transport.execute(&op).await?, "id")
    }

    /// Terminal: the node's type. `None` if not set. Fires one RPC.
    pub async fn node_type(&self) -> Result<Option<String>, ClientError> {
        let op = Op::Read(ReadExpr::NodeType {
            input: Box::new(self.expr.clone()),
        });
        expect_optional_string(self.transport.execute(&op).await?, "nodeType")
    }

    /// Terminal: whether the node has any events in the current view. Fires one RPC.
    pub async fn is_active(&self) -> Result<bool, ClientError> {
        let op = Op::Read(ReadExpr::IsActive {
            input: Box::new(self.expr.clone()),
        });
        expect_bool(self.transport.execute(&op).await?, "isActive")
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

    /// Terminal: count of temporal edge events on this node. Fires one RPC.
    pub async fn edge_history_count(&self) -> Result<i64, ClientError> {
        let op = Op::Read(ReadExpr::EdgeHistoryCount {
            input: Box::new(self.expr.clone()),
        });
        expect_i64(self.transport.execute(&op).await?, "edgeHistoryCount")
    }

    /// Terminal: first update timestamp on this node under the current view.
    /// Returns `None` if the node has no updates in the view. Fires one RPC.
    pub async fn first_update(&self) -> Result<Option<i64>, ClientError> {
        let op = Op::Read(ReadExpr::FirstUpdate {
            input: Box::new(self.expr.clone()),
        });
        expect_optional_i64(self.transport.execute(&op).await?, "firstUpdate")
    }

    /// Terminal: last update timestamp on this node under the current view.
    /// Returns `None` if the node has no updates in the view. Fires one RPC.
    pub async fn last_update(&self) -> Result<Option<i64>, ClientError> {
        let op = Op::Read(ReadExpr::LastUpdate {
            input: Box::new(self.expr.clone()),
        });
        expect_optional_i64(self.transport.execute(&op).await?, "lastUpdate")
    }

    /// Returns the "path from node" collection of this node's neighbours
    /// (both directions). Lazy — no RPC. Propagates the base graph view so
    /// materialized nodes are correctly rebased.
    ///
    /// Returns a `RemotePathFromNode` (not `RemoteNodes`) because the
    /// server's `GqlPathFromNode` type is a strict subset of `GqlNodes` —
    /// e.g., `sorted` and `default_layer` are not available here.
    pub fn neighbours(&self) -> RemotePathFromNode {
        RemotePathFromNode::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::Neighbours {
                input: Box::new(self.expr.clone()),
            },
            self.ctx.clone(),
        )
    }

    /// Returns this node's in-neighbours. Lazy — no RPC. See `neighbours`
    /// for why this is a `RemotePathFromNode`.
    pub fn in_neighbours(&self) -> RemotePathFromNode {
        RemotePathFromNode::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::InNeighbours {
                input: Box::new(self.expr.clone()),
            },
            self.ctx.clone(),
        )
    }

    /// Returns this node's out-neighbours. Lazy — no RPC. See `neighbours`
    /// for why this is a `RemotePathFromNode`.
    pub fn out_neighbours(&self) -> RemotePathFromNode {
        RemotePathFromNode::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::OutNeighbours {
                input: Box::new(self.expr.clone()),
            },
            self.ctx.clone(),
        )
    }

    /// Returns the in-component of this node — the set of all nodes that can
    /// reach this node via incoming edges (its ancestors in the directed
    /// graph, not including itself). Lazy — no RPC.
    pub fn in_component(&self) -> RemoteNodes {
        RemoteNodes::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::InComponent {
                input: Box::new(self.expr.clone()),
            },
            self.ctx.clone(),
        )
    }

    /// Returns the out-component of this node — the set of all nodes
    /// reachable from this node via outgoing edges (its descendants,
    /// not including itself). Lazy — no RPC.
    pub fn out_component(&self) -> RemoteNodes {
        RemoteNodes::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::OutComponent {
                input: Box::new(self.expr.clone()),
            },
            self.ctx.clone(),
        )
    }

    /// Returns the collection of this node's edges (both directions). Lazy — no RPC.
    /// Propagates the base graph view so materialized edges are correctly rebased.
    pub fn edges(&self) -> RemoteEdges {
        RemoteEdges::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::NodeEdges {
                input: Box::new(self.expr.clone()),
            },
            self.ctx.clone(),
        )
    }

    /// Returns the collection of this node's incoming edges. Lazy — no RPC.
    pub fn in_edges(&self) -> RemoteEdges {
        RemoteEdges::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::InEdges {
                input: Box::new(self.expr.clone()),
            },
            self.ctx.clone(),
        )
    }

    /// Returns the collection of this node's outgoing edges. Lazy — no RPC.
    pub fn out_edges(&self) -> RemoteEdges {
        RemoteEdges::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::OutEdges {
                input: Box::new(self.expr.clone()),
            },
            self.ctx.clone(),
        )
    }

    /// Returns the metadata container of this node — non-temporal properties
    /// whose values don't depend on time. Lazy — no RPC.
    pub fn metadata(&self) -> RemoteMetadata {
        RemoteMetadata::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::Metadata {
                input: Box::new(self.expr.clone()),
            },
            self.ctx.clone(),
        )
    }

    /// Returns the full properties container of this node — includes both
    /// temporal properties and metadata. Lazy — no RPC.
    pub fn properties(&self) -> RemoteProperties {
        RemoteProperties::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::Properties {
                input: Box::new(self.expr.clone()),
            },
            self.ctx.clone(),
        )
    }

    /// Returns the event history of this node — a `RemoteHistory` container
    /// with terminals like `.count()`, `.collect()`, `.earliest_time()`, and
    /// sub-container accessors (`.timestamps`, `.intervals`, etc.). Lazy —
    /// no RPC.
    pub fn history(&self) -> RemoteHistory {
        RemoteHistory::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::History {
                input: Box::new(self.expr.clone()),
            },
            self.ctx.clone(),
        )
    }

    /// Set the type on the node. This only works if the type has not been previously set.
    pub async fn set_node_type(&self, new_type: String) -> Result<(), ClientError> {
        let op = Op::Write(WriteOp::SetNodeType(SetNodeTypeOp {
            path: self.path.clone(),
            id: self.id.clone(),
            new_type,
        }));
        self.transport.execute(&op).await?;
        Ok(())
    }

    /// Add temporal updates to the node at the specified time. `event_id` locks
    /// the secondary index (as on `add_node`); `None` lets the server
    /// auto-increment.
    pub async fn add_updates<T: IntoTime>(
        &self,
        t: T,
        properties: Option<HashMap<String, Prop>>,
        event_id: Option<usize>,
    ) -> Result<(), ClientError> {
        let op = Op::Write(WriteOp::AddNodeUpdates(AddNodeUpdatesOp {
            path: self.path.clone(),
            id: self.id.clone(),
            time: input_time_from_parts(t.into_time().t(), event_id),
            properties,
        }));
        self.transport.execute(&op).await?;
        Ok(())
    }

    /// Add metadata to the node (properties that do not change over time).
    pub async fn add_metadata(&self, properties: HashMap<String, Prop>) -> Result<(), ClientError> {
        let op = Op::Write(WriteOp::AddNodeMetadata(AddNodeMetadataOp {
            path: self.path.clone(),
            id: self.id.clone(),
            properties,
        }));
        self.transport.execute(&op).await?;
        Ok(())
    }

    /// Update metadata of the node, overwriting existing values.
    pub async fn update_metadata(
        &self,
        properties: HashMap<String, Prop>,
    ) -> Result<(), ClientError> {
        let op = Op::Write(WriteOp::UpdateNodeMetadata(UpdateNodeMetadataOp {
            path: self.path.clone(),
            id: self.id.clone(),
            properties,
        }));
        self.transport.execute(&op).await?;
        Ok(())
    }
}
