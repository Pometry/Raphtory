use crate::client::{
    graphql_transport::GraphqlTransport,
    op::{
        AddEdge as AddEdgeOp, AddGraphMetadata as AddGraphMetadataOp,
        AddGraphProperty as AddGraphPropertyOp, AddNode as AddNodeOp, CreateNode as CreateNodeOp,
        DeleteEdge as DeleteEdgeOp, Op, ReadExpr, UpdateGraphMetadata as UpdateGraphMetadataOp,
        WriteOp,
    },
    remote_client::RemoteClient,
    remote_edge::RemoteEdge,
    remote_node::RemoteNode,
    remote_nodes::RemoteNodes,
    transport::Transport,
    ClientError,
};
use minijinja::{Environment, Value};
use raphtory_api::core::{
    entities::{properties::prop::Prop, GID},
    storage::timeindex::{AsTime, EventTime},
    utils::time::IntoTime,
};
use std::{collections::HashMap, sync::Arc};

pub fn build_query(template: &str, context: Value) -> Result<String, ClientError> {
    let mut env = Environment::new();
    env.add_template("template", template)
        .map_err(|e| ClientError::JinjaError(e.to_string()))?;
    let query = env
        .get_template("template")
        .map_err(|e| ClientError::JinjaError(e.to_string()))?
        .render(context)
        .map_err(|e| ClientError::JinjaError(e.to_string()))?;
    Ok(query)
}

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

/// A handle to a remote graph on the server.
///
/// Holds an accumulating `ReadExpr` for lazy view construction — `.window()`,
/// `.node()` etc. append to it without firing an RPC. Terminals on the child
/// types (e.g. `RemoteNode::degree`) evaluate the accumulated expression via
/// the transport.
#[derive(Clone)]
pub struct RemoteGraph {
    pub path: String,
    pub transport: Arc<dyn Transport>,
    /// The read expression built so far. Starts as `Root { path }`.
    pub expr: ReadExpr,
}

impl RemoteGraph {
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
    pub fn window(&self, start: i64, end: i64) -> RemoteGraph {
        self.with_expr(ReadExpr::Window {
            input: Box::new(self.expr.clone()),
            start,
            end,
        })
    }

    /// Restrict to a single named layer. Lazy — no RPC.
    pub fn layer(&self, name: impl ToString) -> RemoteGraph {
        self.with_expr(ReadExpr::Layer {
            input: Box::new(self.expr.clone()),
            name: name.to_string(),
        })
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    pub fn at(&self, time: i64) -> RemoteGraph {
        self.with_expr(ReadExpr::At {
            input: Box::new(self.expr.clone()),
            time,
        })
    }

    /// Restrict to events strictly before the given time. Lazy — no RPC.
    pub fn before(&self, time: i64) -> RemoteGraph {
        self.with_expr(ReadExpr::Before {
            input: Box::new(self.expr.clone()),
            time,
        })
    }

    /// Restrict to events at or after the given time. Lazy — no RPC.
    pub fn after(&self, time: i64) -> RemoteGraph {
        self.with_expr(ReadExpr::After {
            input: Box::new(self.expr.clone()),
            time,
        })
    }

    /// Restrict to the latest state — no args. Lazy — no RPC.
    pub fn latest(&self) -> RemoteGraph {
        self.with_expr(ReadExpr::Latest {
            input: Box::new(self.expr.clone()),
        })
    }

    /// Snapshot at the latest time. Lazy — no RPC.
    pub fn snapshot_latest(&self) -> RemoteGraph {
        self.with_expr(ReadExpr::SnapshotLatest {
            input: Box::new(self.expr.clone()),
        })
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    pub fn snapshot_at(&self, time: i64) -> RemoteGraph {
        self.with_expr(ReadExpr::SnapshotAt {
            input: Box::new(self.expr.clone()),
            time,
        })
    }

    /// Exclude a specific layer from the view. Lazy — no RPC.
    pub fn exclude_layer(&self, name: impl ToString) -> RemoteGraph {
        self.with_expr(ReadExpr::ExcludeLayer {
            input: Box::new(self.expr.clone()),
            name: name.to_string(),
        })
    }

    /// Shrink both start and end of the current window (intersection, never widens).
    /// Lazy — no RPC.
    pub fn shrink_window(&self, start: i64, end: i64) -> RemoteGraph {
        self.with_expr(ReadExpr::ShrinkWindow {
            input: Box::new(self.expr.clone()),
            start,
            end,
        })
    }

    /// Shrink the start of the current window. Lazy — no RPC.
    pub fn shrink_start(&self, start: i64) -> RemoteGraph {
        self.with_expr(ReadExpr::ShrinkStart {
            input: Box::new(self.expr.clone()),
            start,
        })
    }

    /// Shrink the end of the current window. Lazy — no RPC.
    pub fn shrink_end(&self, end: i64) -> RemoteGraph {
        self.with_expr(ReadExpr::ShrinkEnd {
            input: Box::new(self.expr.clone()),
            end,
        })
    }

    /// Restrict to the "valid" subgraph (event-graph filter). Lazy — no RPC.
    pub fn valid(&self) -> RemoteGraph {
        self.with_expr(ReadExpr::Valid {
            input: Box::new(self.expr.clone()),
        })
    }

    /// Restrict to the default layer. Lazy — no RPC.
    pub fn default_layer(&self) -> RemoteGraph {
        self.with_expr(ReadExpr::DefaultLayer {
            input: Box::new(self.expr.clone()),
        })
    }

    /// Restrict to the given set of layers. Lazy — no RPC.
    pub fn layers(&self, names: Vec<String>) -> RemoteGraph {
        self.with_expr(ReadExpr::Layers {
            input: Box::new(self.expr.clone()),
            names,
        })
    }

    /// Exclude the given set of layers from the view. Lazy — no RPC.
    pub fn exclude_layers(&self, names: Vec<String>) -> RemoteGraph {
        self.with_expr(ReadExpr::ExcludeLayers {
            input: Box::new(self.expr.clone()),
            names,
        })
    }

    /// Restrict to a subgraph induced by the given node ids. Lazy — no RPC.
    pub fn subgraph(&self, nodes: Vec<String>) -> RemoteGraph {
        self.with_expr(ReadExpr::Subgraph {
            input: Box::new(self.expr.clone()),
            nodes,
        })
    }

    /// Restrict to nodes matching one of the given node types. Lazy — no RPC.
    pub fn subgraph_node_types(&self, node_types: Vec<String>) -> RemoteGraph {
        self.with_expr(ReadExpr::SubgraphNodeTypes {
            input: Box::new(self.expr.clone()),
            node_types,
        })
    }

    /// Exclude the given nodes from the view. Lazy — no RPC.
    pub fn exclude_nodes(&self, nodes: Vec<String>) -> RemoteGraph {
        self.with_expr(ReadExpr::ExcludeNodes {
            input: Box::new(self.expr.clone()),
            nodes,
        })
    }

    /// Terminal: count of nodes under the current view. Fires one RPC.
    pub async fn count_nodes(&self) -> Result<i64, ClientError> {
        let op = Op::Read(ReadExpr::CountNodes {
            input: Box::new(self.expr.clone()),
        });
        expect_i64(self.transport.execute(&op).await?, "countNodes")
    }

    /// Terminal: count of edges under the current view. Fires one RPC.
    pub async fn count_edges(&self) -> Result<i64, ClientError> {
        let op = Op::Read(ReadExpr::CountEdges {
            input: Box::new(self.expr.clone()),
        });
        expect_i64(self.transport.execute(&op).await?, "countEdges")
    }

    /// Terminal: earliest event timestamp under the current view. Returns
    /// `None` if the view has no events. Fires one RPC.
    pub async fn earliest_time(&self) -> Result<Option<i64>, ClientError> {
        let op = Op::Read(ReadExpr::EarliestTime {
            input: Box::new(self.expr.clone()),
        });
        expect_optional_i64(self.transport.execute(&op).await?, "earliestTime")
    }

    /// Terminal: latest event timestamp under the current view. Returns
    /// `None` if the view has no events. Fires one RPC.
    pub async fn latest_time(&self) -> Result<Option<i64>, ClientError> {
        let op = Op::Read(ReadExpr::LatestTime {
            input: Box::new(self.expr.clone()),
        });
        expect_optional_i64(self.transport.execute(&op).await?, "latestTime")
    }

    /// Terminal: view start bound. Returns `None` for an unbounded view.
    /// Fires one RPC.
    pub async fn start(&self) -> Result<Option<i64>, ClientError> {
        let op = Op::Read(ReadExpr::Start {
            input: Box::new(self.expr.clone()),
        });
        expect_optional_i64(self.transport.execute(&op).await?, "start")
    }

    /// Terminal: view end bound. Returns `None` for an unbounded view.
    /// Fires one RPC.
    pub async fn end(&self) -> Result<Option<i64>, ClientError> {
        let op = Op::Read(ReadExpr::End {
            input: Box::new(self.expr.clone()),
        });
        expect_optional_i64(self.transport.execute(&op).await?, "end")
    }

    /// Terminal: does the graph have a node with this id? Fires one RPC.
    pub async fn has_node(&self, id: impl ToString) -> Result<bool, ClientError> {
        let op = Op::Read(ReadExpr::HasNode {
            input: Box::new(self.expr.clone()),
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
            input: Box::new(self.expr.clone()),
            src: src.to_string(),
            dst: dst.to_string(),
        });
        expect_bool(self.transport.execute(&op).await?, "hasEdge")
    }

    /// Terminal: total temporal-edge count (edge updates) under the current view.
    /// Fires one RPC.
    pub async fn count_temporal_edges(&self) -> Result<i64, ClientError> {
        let op = Op::Read(ReadExpr::CountTemporalEdges {
            input: Box::new(self.expr.clone()),
        });
        expect_i64(self.transport.execute(&op).await?, "countTemporalEdges")
    }

    /// Terminal: graph name. Fires one RPC.
    pub async fn name(&self) -> Result<String, ClientError> {
        let op = Op::Read(ReadExpr::Name {
            input: Box::new(self.expr.clone()),
        });
        expect_string(self.transport.execute(&op).await?, "name")
    }

    /// Terminal: graph path. Fires one RPC.
    pub async fn path(&self) -> Result<String, ClientError> {
        let op = Op::Read(ReadExpr::Path {
            input: Box::new(self.expr.clone()),
        });
        expect_string(self.transport.execute(&op).await?, "path")
    }

    /// Terminal: parent namespace of the graph path. Fires one RPC.
    pub async fn namespace(&self) -> Result<String, ClientError> {
        let op = Op::Read(ReadExpr::Namespace {
            input: Box::new(self.expr.clone()),
        });
        expect_string(self.transport.execute(&op).await?, "namespace")
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
    /// Carries the built-up read expression forward, so subsequent terminals
    /// (e.g. `degree()`) evaluate under the same view chain.
    pub fn node(&self, id: impl ToString) -> RemoteNode {
        let id_str = id.to_string();
        RemoteNode::with_expr(
            self.path.clone(),
            id_str.clone(),
            self.transport.clone(),
            ReadExpr::Node {
                input: Box::new(self.expr.clone()),
                id: id_str,
            },
            self.expr.clone(),
        )
    }

    /// Returns the collection of all nodes in the graph, evaluated under the
    /// current view chain. Lazy — no RPC.
    pub fn nodes(&self) -> RemoteNodes {
        RemoteNodes::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::Nodes {
                input: Box::new(self.expr.clone()),
            },
            self.expr.clone(),
        )
    }

    /// Returns a remote edge reference for the given source and destination node ids.
    /// Carries the built-up read expression forward, so subsequent navigations
    /// (`.src()`, `.dst()`) evaluate under the same view chain.
    pub fn edge(&self, src: impl ToString, dst: impl ToString) -> RemoteEdge {
        let src_str = src.to_string();
        let dst_str = dst.to_string();
        RemoteEdge::with_expr(
            self.path.clone(),
            src_str.clone(),
            dst_str.clone(),
            self.transport.clone(),
            ReadExpr::Edge {
                input: Box::new(self.expr.clone()),
                src: src_str,
                dst: dst_str,
            },
            self.expr.clone(),
        )
    }

    pub async fn add_node<G: Into<GID> + ToString, T: IntoTime>(
        &self,
        timestamp: T,
        id: G,
        properties: Option<HashMap<String, Prop>>,
        node_type: Option<String>,
        layer: Option<String>,
    ) -> Result<RemoteNode, ClientError> {
        let id_str = id.to_string();
        let op = Op::Write(WriteOp::AddNode(AddNodeOp {
            path: self.path.clone(),
            time: timestamp.into_time().t(),
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
                input: Box::new(self.expr.clone()),
                id: id_str,
            },
            self.expr.clone(),
        ))
    }

    /// Create a new node (fails if the node already exists). Uses the createNode mutation.
    pub async fn create_node<G: Into<GID> + ToString, T: IntoTime>(
        &self,
        timestamp: T,
        id: G,
        properties: Option<HashMap<String, Prop>>,
        node_type: Option<String>,
    ) -> Result<RemoteNode, ClientError> {
        let id_str = id.to_string();
        let op = Op::Write(WriteOp::CreateNode(CreateNodeOp {
            path: self.path.clone(),
            time: timestamp.into_time().t(),
            id: id_str.clone(),
            properties,
            node_type,
        }));
        self.transport.execute(&op).await?;
        Ok(RemoteNode::with_expr(
            self.path.clone(),
            id_str.clone(),
            self.transport.clone(),
            ReadExpr::Node {
                input: Box::new(self.expr.clone()),
                id: id_str,
            },
            self.expr.clone(),
        ))
    }

    pub async fn add_edge<G: Into<GID> + ToString, T: IntoTime>(
        &self,
        timestamp: T,
        src: G,
        dst: G,
        properties: Option<HashMap<String, Prop>>,
        layer: Option<String>,
    ) -> Result<RemoteEdge, ClientError> {
        let src_str = src.to_string();
        let dst_str = dst.to_string();
        let op = Op::Write(WriteOp::AddEdge(AddEdgeOp {
            path: self.path.clone(),
            time: timestamp.into_time().t(),
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
                input: Box::new(self.expr.clone()),
                src: src_str,
                dst: dst_str,
            },
            self.expr.clone(),
        ))
    }

    pub async fn add_property(
        &self,
        timestamp: EventTime,
        properties: HashMap<String, Prop>,
    ) -> Result<(), ClientError> {
        let op = Op::Write(WriteOp::AddGraphProperty(AddGraphPropertyOp {
            path: self.path.clone(),
            time: timestamp.into_time().t(),
            properties,
        }));
        self.transport.execute(&op).await?;
        Ok(())
    }

    pub async fn add_metadata(&self, properties: HashMap<String, Prop>) -> Result<(), ClientError> {
        let op = Op::Write(WriteOp::AddGraphMetadata(AddGraphMetadataOp {
            path: self.path.clone(),
            properties,
        }));
        self.transport.execute(&op).await?;
        Ok(())
    }

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

    /// Deletes an edge at the given time, src, dst and optional layer.
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
                input: Box::new(self.expr.clone()),
                src: src_str,
                dst: dst_str,
            },
            self.expr.clone(),
        ))
    }
}
